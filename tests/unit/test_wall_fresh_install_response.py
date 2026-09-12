"""I3 — what a FRESH INSTALL actually answers, driven by the shipped defaults.

🔴 THE GAP, AND IT IS THE CLASS THIS PLAN KEEPS FINDING. `test_wall_endpoint.py`'s
dark-gate tests open with `monkeypatch.setattr(settings, "offerwall_serve_enabled",
False)`. `False` happens to BE the shipped default — but the test **sets** it. So
those tests prove *"with the flag off, the endpoint is invisible"*, which is a
different claim from *"a box nobody has configured is invisible"*. **Flip the
default to True tomorrow and every one of them still passes**, while the sentence
they are cited for silently becomes false.

So the rows here never write a boolean literal into a flag. They read each value
from `Settings.model_fields[...].default` — **what ships** — and drive the real
endpoint with THAT. A default that moves therefore moves the behaviour under
test, and the separate assertions about what the defaults ARE go red to say so.

WHAT A FRESH INSTALL IS, per U1/U2's rulings, stated as a shape rather than as
three unrelated booleans:

  | rail | shipped | consequence on a box nobody armed |
  |---|---|---|
  | `offerwall_serve_enabled` | **False** | `/wall` is invisible — 404, byte-identical to a route that does not exist |
  | `route_preview_enabled`   | **False** | preview codes are not honoured |
  | `wall_tile_honour_enabled`| **True**  | a wall tile WOULD be honoured… |

…and the last row is the one that looks alarming until you follow it: **on a
fresh box no wall tile code can exist at all**, because minting happens only
while serving a wall and rail 1 is dark. Default-ON on rail 3 is inert **by
construction**, not by luck — and `TestTheDefaultONRailIsInertByConstruction`
is that argument made checkable instead of left in a comment.

ON "honoured or *visibly* refused", the box's own words: at shipped defaults the
wall's refusal is deliberately **invisible to a prober** — that is the
no-existence-oracle property, and making it visible would be a regression, not a
fix. The visibility the box is entitled to is the OPERATOR's, and it is covered
one rail down by U2's `OP_WALL_TILE_CODES_UNSIGNED` signal. Recorded here rather
than quietly satisfied, because the two senses of "visible" point in opposite
directions and a later reader will otherwise try to reconcile them.
"""

from __future__ import annotations

import asyncio
from unittest.mock import patch

from app.config import Settings, settings

from tests.unit.test_wall_endpoint import (
    SECRET,
    _fake,
    _post,
    _seed,
)

#: Read from the model, never written as a literal. This is the whole method of
#: this file: the behaviour rows below are parameterised by WHAT SHIPS.
_SHIPPED = {
    name: Settings.model_fields[name].default
    for name in (
        "offerwall_serve_enabled",
        "route_preview_enabled",
        "wall_tile_honour_enabled",
    )
}


def _at_shipped_defaults():
    """Context manager stack putting the process's settings back to FRESH."""
    return [
        patch.object(settings, name, value) for name, value in _SHIPPED.items()
    ] + [patch.object(settings, "tds_secret_key", SECRET)]


def _post_fresh(store, **kw):
    patches = _at_shipped_defaults()
    for p in patches:
        p.start()
    try:
        return _post(store, **kw)
    finally:
        for p in reversed(patches):
            p.stop()


class TestTheShippedCombination:
    """The three as a SET. U2 pins each one; a fresh install is the combination,
    and it is the combination that decides what a new operator sees."""

    def test_serving_a_wall_is_OFF_and_honouring_a_tile_is_ON(self):
        assert _SHIPPED == {
            "offerwall_serve_enabled": False,
            "route_preview_enabled": False,
            "wall_tile_honour_enabled": True,
        }, (
            "the fresh-install shape changed. Every behaviour row in this file "
            "is parameterised by these values, so they now describe a DIFFERENT "
            "fresh install — re-read them before re-pointing this assertion."
        )

    def test_the_two_honour_rails_disagree_which_is_the_point(self):
        # Stated separately because it is the surprising half: preview OFF,
        # wall honour ON. They are different features with different rulings,
        # and a change that re-merges them would pass the row above.
        assert _SHIPPED["route_preview_enabled"] is not _SHIPPED[
            "wall_tile_honour_enabled"]


class TestTheEndpointOnABoxNOBODYConfigured:
    """Driven by `_SHIPPED`, never by a literal — the distinction this file exists for."""

    def test_a_fresh_box_answers_404(self):
        assert _post_fresh(_fake()).status_code == 404

    def test_and_it_is_INDISTINGUISHABLE_from_a_route_that_does_not_exist(self):
        # The no-existence-oracle property, re-asserted HERE because the
        # existing version of it sets the flag by hand and so cannot speak
        # about a default.
        store = _fake()
        off = _post_fresh(store)
        missing = _post_fresh(store, path="/definitely-not-a-route")
        assert off.status_code == missing.status_code == 404
        assert off.json() == missing.json(), (
            f"a fresh box answers {off.json()!r} where a missing route answers "
            f"{missing.json()!r} — that difference IS the oracle"
        )

    def test_a_VALID_key_does_not_reveal_it_either(self):
        # Ordering: the flag is consulted before auth, so a correct credential
        # must not turn the 404 into a 401.
        assert _post_fresh(_fake(), key=SECRET).status_code == 404

    def test_CALIBRATION_arming_rail_one_changes_the_answer(self):
        # Without this the rows above are satisfied by an endpoint that 404s
        # unconditionally — which is exactly what a broken route looks like.
        store = _fake()
        asyncio.run(_seed(store.client()))
        with patch.object(settings, "offerwall_serve_enabled", True), \
                patch.object(settings, "tds_secret_key", SECRET):
            armed_resp = _post(store, key=SECRET)
        assert armed_resp.status_code == 200, armed_resp.text
        assert _post_fresh(store, key=SECRET).status_code == 404


class TestTheDefaultONRailIsInertByConstruction:
    """🔴 Why `wall_tile_honour_enabled = True` on a fresh box is not a hole.

    A tile code can only exist if a wall was served, and serving is dark. So the
    honour switch has nothing to admit — not because it refuses, but because the
    input cannot be produced. That is a stronger property than "it is off", and
    it is the one the fresh-install ruling actually rests on.
    """

    def test_a_fresh_box_serves_NO_tiles_so_no_code_can_be_minted(self):
        store = _fake()
        asyncio.run(_seed(store.client()))     # a wall EXISTS in config…
        r = _post_fresh(store, key=SECRET)     # …and is still not served
        assert r.status_code == 404
        body = r.json()
        assert "tiles" not in str(body), (
            "a fresh box returned tile data — rail 1 is not actually gating the "
            "mint path, and default-ON on rail 3 stops being inert"
        )

    def test_the_CONTRAST_an_armed_box_with_the_same_config_DOES_mint(self):
        # The discriminator for the row above: same store, same seeded wall.
        # If this did not serve tiles, "no tiles on a fresh box" would be a fact
        # about the fixture rather than about the gate.
        store = _fake()
        asyncio.run(_seed(store.client()))
        with patch.object(settings, "offerwall_serve_enabled", True), \
                patch.object(settings, "tds_secret_key", SECRET):
            r = _post(store, key=SECRET)
        assert r.status_code == 200
        assert r.json().get("tiles"), r.json()


class TestWhatThisFileDoesNOTClaim:
    """The honest boundary, asserted so it cannot be quietly widened later."""

    def test_the_fresh_refusal_is_invisible_to_a_PROBER_and_that_is_deliberate(self):
        # `- [ ] I3` asks for "honoured or *visibly* refused". At shipped
        # defaults the refusal is INVISIBLE on purpose — the 404 is
        # indistinguishable from a missing route. This row records that the two
        # senses of "visible" point opposite ways: operator-visible (U2's
        # OP_WALL_TILE_CODES_UNSIGNED, one rail down) versus prober-visible,
        # which must stay absent. Making this refusal visible would be a
        # security regression dressed as closing a box.
        store = _fake()
        assert _post_fresh(store).json() == _post_fresh(
            store, path="/definitely-not-a-route").json()
