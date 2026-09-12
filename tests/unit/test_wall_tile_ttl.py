"""A wall tile link lives on the WALL's clock, not the preview's.

WHY THIS EXISTS. Until 2026-09-12 `_mint_tile_codes` and the route-preview
responder shared ONE setting, `route_code_ttl_seconds`, and the two paths need
opposite windows:

* **preview** is session-scale by design, and its own config comment argues for
  keeping it short — "a longer window only widens the gap between what was
  advertised and what is still true";
* **the wall** publishes its links. The owner, 2026-09-05: «Ми опублікуємо ці
  посилання на якійсь сторінці, користувач її залишить в історії… 30 хвилин –
  це дуже мало… але точно довше 30 хвилин», and he delegated the number. Ruled
  at 7 days (`63-THE-LINE-18-DECIDED.md:112,:186`).

One value could only ever satisfy one of them. It satisfied preview, so the
ruling sat unimplemented for a week while the checklist read complete — which is
the failure this suite is here to make impossible to repeat silently.

🔴 THE TEST THAT MATTERS IS THE DISCRIMINATING ONE. Asserting "the expiry is
about 7 days from now" would pass just as happily if someone set the PREVIEW
value to 7 days and left the wall pointing at it — the numbers would agree and
the bug would be that both paths moved. So the settings are monkeypatched to two
DIFFERENT, recognisable values and the question asked is *which one did the code
carry*. That fails the moment `_mint_tile_codes` is pointed back at the shared
setting, and it cannot be satisfied by coincidence.

Expiry is read back through `route_code.verify()`, never off the response's
`expires_at` alone: the response reports what the endpoint believes, the code
carries what a NODE will actually honour, and it is the second one that decides
a visitor's route.

The harness is imported from the endpoint suite verbatim, for the reason that
suite states: a second, subtly different fixture for one endpoint is how two test
files start describing two different systems.
"""

import asyncio
import time

import pytest

from app import route_code
from app.config import settings

from tests.unit.test_wall_endpoint import (  # noqa: F401  (fixtures are used)
    CAMPAIGN,
    COMPANY,
    WALL_A,
    _fake,
    _post,
    _seed,
    armed,
)

KEY = "wall-tile-ttl-test-key-aaaaaaaaaaaaaaaaaaaaaa"

# Two values that cannot be confused for one another, and neither of which is a
# plausible real setting — so a failure message names the culprit by its number.
WALL_TTL = 600_000      # what the wall must use
PREVIEW_TTL = 300       # what it must NOT use


@pytest.fixture
def coded(armed, monkeypatch):  # noqa: F811
    """`armed`, plus a live ring and two DELIBERATELY different TTLs."""
    monkeypatch.setattr(settings, "route_code_keys", f"1:{KEY}")
    monkeypatch.setattr(settings, "route_code_active_kid", "1")
    monkeypatch.setattr(settings, "wall_tile_ttl_seconds", WALL_TTL)
    monkeypatch.setattr(settings, "route_code_ttl_seconds", PREVIEW_TTL)


def _served(store) -> dict:
    asyncio.run(_seed(store.client()))
    r = _post(store)
    assert r.status_code == 200, r.text
    return r.json()


class TestTheTileCarriesTheWALLsWindow:
    def test_every_tile_code_expires_on_the_wall_clock_not_the_preview_one(self, coded):
        before = int(time.time())
        body = _served(_fake())
        after = int(time.time())

        tiles = body["tiles"]
        assert len(tiles) >= 2, "the discrimination needs a real wall, not one tile"

        for tile in tiles:
            claims = route_code.verify(tile["route_code"])
            assert claims is not None, "a code this node minted must verify here"
            # The window the NODE will honour, derived from the code itself.
            window = claims.expires_at - before
            # Bracketed rather than compared to one number: signing takes a
            # non-zero amount of wall-clock time, so an exact equality would be
            # flaky for a reason that has nothing to do with the property.
            assert WALL_TTL <= window <= WALL_TTL + (after - before) + 2, (
                f"tile expiry is {window}s from now; expected the WALL's "
                f"{WALL_TTL}s. If this reads ~{PREVIEW_TTL}s, _mint_tile_codes "
                f"has been pointed back at route_code_ttl_seconds."
            )
            # And state the discrimination as its own assertion, so a future
            # reader cannot mistake the bracket above for a loose range check.
            assert window > PREVIEW_TTL * 10

    def test_the_response_and_the_code_agree_about_the_window(self, coded):
        # Two independent reporters of one fact. They can disagree — the
        # response is computed at the endpoint, the expiry is baked into each
        # signed blob — and a visitor is routed by the SECOND one, so a drift
        # between them would be invisible to any caller reading only the first.
        body = _served(_fake())
        assert isinstance(body["expires_at"], int)
        for tile in body["tiles"]:
            claims = route_code.verify(tile["route_code"])
            assert claims is not None
            assert abs(claims.expires_at - body["expires_at"]) <= 2


class TestTheShippedDefaultHonoursTheRuling:
    """The settings object, not a monkeypatched one — this is the shipped value."""

    def test_the_wall_default_is_at_least_the_ruled_seven_days(self):
        from app.config import Settings

        shipped = Settings.model_fields["wall_tile_ttl_seconds"].default
        assert shipped >= 604_800, (
            f"shipped wall tile TTL is {shipped}s; the ruling is 7 days = 604800s "
            "and the owner's floor is 'longer than 30 minutes'"
        )

    def test_preview_was_NOT_dragged_along(self):
        # The whole point of splitting was to leave preview alone. If a later
        # change "simplifies" by re-merging them, this is the assertion that
        # notices — from the other side, so both directions are covered.
        from app.config import Settings

        preview = Settings.model_fields["route_code_ttl_seconds"].default
        wall = Settings.model_fields["wall_tile_ttl_seconds"].default
        assert preview < wall, (
            "preview and wall TTLs are no longer distinct. They were one setting "
            "until 2026-09-12 and the two requirements are opposite; re-merging "
            "them silently gives one path a window nobody asked for."
        )
        assert preview == 1800, (
            f"route_code_ttl_seconds is {preview}s, not the session-scale 1800s "
            "its own comment argues for. If this was deliberate, change the "
            "comment in the same edit — otherwise the reason and the value "
            "disagree and the next reader has to guess which is stale."
        )
