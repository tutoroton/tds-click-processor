"""U2 — on a fresh install, a wall tile link must not be silently inert.

WHY THIS EXISTS. Three independent rails have to be up before a published tile
link does anything, and until 2026-09-12 the third one belonged to a DIFFERENT
FEATURE:

  1. ``offerwall_serve_enabled`` — the node answers ``/wall`` at all;
  2. ``route_code_keys`` / ``route_code_active_kid`` — tiles carry a signed code;
  3. the click path HONOURS such a code — which was gated on
     ``route_preview_enabled``, a switch whose name says nothing about walls.

So an operator who armed "the offer wall" got a node that MINTED codes it then
REFUSED: ``_mint_tile_codes`` gates on the keyring alone, while the honour hook
returned None on its first line. The refusal is indistinguishable from a visitor
who carried no code — the click simply routes normally — so the result is a
PUBLISHED link with a seven-day life that is dead on arrival, and nothing
anywhere says why.

It had been named twice and fixed neither time: ADR-0516 consequence 4 ("two
operator switches silently change the outcome, and neither announces itself")
and ``62-PHASE2-FABLE-CRITIC-AND-THE-TWO-DISAGREEMENTS.md`` R9, closed at the
time as "named, not a surprise".

WHAT THESE TESTS PIN, each written so it can only pass for the right reason:

  * the shipped DEFAULTS describe a fresh box, not this developer's env;
  * rail 3 is now the wall's OWN switch — proven by flipping ONLY that switch
    between two otherwise identical runs, so a pass cannot come from some other
    difference;
  * the wall switch does NOT arm route preview — the direction that would be a
    security regression rather than a missing feature;
  * BOTH call sites obey it, because both can be handed a wall claim;
  * rail 2 ANNOUNCES itself when it is down, instead of degrading in silence.

The harnesses are imported verbatim from the suites that already own them. A
second, subtly different fixture for one path is how two test files start
describing two different systems.
"""

from __future__ import annotations

import asyncio
from unittest.mock import patch

from app import action_executor, main, router, sticky as sticky_mod
from app.config import Settings, settings
from app.models import WallTile
from app.telemetry import OP_WALL_TILE_CODES_UNSIGNED

from tests.unit.test_route_code_honoured import (
    _ACTIVE_KID,
    _CAMPAIGN,
    _CODED_TARGET,
    _COMPANY,
    _KEYS,
    _NORMAL_TARGET,
    _OFFER,
    FakeIdentRedis,
    FakeRoutingRedis,
    _click,
    _sign,
)
from tests.unit.test_wall_tile_beats_the_pin import _PINNED_TARGET, _hashes
from tests.unit.test_wall_tile_membership import WALL_ID, _sign_wall


def _resolve(code, *, preview: bool, honour: bool, sticky: bool):
    """Drive the REAL resolver with the two rails set INDEPENDENTLY.

    `sticky` picks WHICH call site is exercised: True reaches the wall-only
    consult that outranks a live pin, False the ordinary one that admits both
    kinds. Everything else is held identical on purpose — the only thing the
    tests below vary is a flag.
    """
    ident = FakeIdentRedis()
    r = FakeRoutingRedis(hashes=_hashes())

    async def _gir():
        return ident

    async def _serve(*a, **k):
        return {
            "url": "https://offer.example/" + _NORMAL_TARGET,
            "offer_id": str(_OFFER),
            "target_id": _NORMAL_TARGET,
            "target_selection_path": "split_weighted",
        }

    async def _get_sticky(*a, **k):
        return _PINNED_TARGET

    async def _runner():
        with patch.object(sticky_mod, "get_identity_redis", _gir), \
                patch.object(sticky_mod, "get_sticky", _get_sticky), \
                patch.object(action_executor, "execute_action", _serve), \
                patch.object(settings, "route_preview_enabled", preview), \
                patch.object(settings, "wall_tile_honour_enabled", honour), \
                patch.object(settings, "route_code_keys", _KEYS), \
                patch.object(settings, "route_code_active_kid", _ACTIVE_KID), \
                patch.object(settings, "returning_uid_ttl_seconds", 1000):
            return await router._resolve_action_with_sticky(
                r, {"action_type": "split"}, _click(code), str(_CAMPAIGN),
                source_mappings={}, campaign_mappings={},
                sticky_active=sticky,
                returning_flow_won=False,
                uid="U", company_id=_COMPANY,
                seen_before=True, returning_visitor=True,
                flow_id="300", allowed_avail=frozenset({"active"}),
                fresh_track=False,
            )

    result, _status = asyncio.run(_runner())
    return result


class TestTheShippedDefaultsDescribeAFreshBox:
    """`Settings` itself, never a monkeypatched copy — this is what ships."""

    def test_honouring_a_tile_is_ON_by_default(self):
        assert Settings.model_fields["wall_tile_honour_enabled"].default is True, (
            "a fresh install must honour the tile links it publishes. The owner "
            "ruled on 2026-09-11, for the sibling delivery flag: «треба, щоб "
            "було включене, і щоб воно працювало», «щоб ми не залежали від цих "
            "env файлів»."
        )

    def test_route_preview_is_still_OFF_by_default(self):
        # The split must not have armed preview as a side effect. If this ever
        # goes green by accident, the test above stops discriminating.
        assert Settings.model_fields["route_preview_enabled"].default is False

    def test_serving_a_wall_is_still_OFF_by_default(self):
        # Rail 1 stays dark, and that is what keeps default-ON on rail 3 inert
        # on a box nobody has armed: no wall served ⇒ no tile code exists ⇒ the
        # honour switch has nothing to admit.
        assert Settings.model_fields["offerwall_serve_enabled"].default is False


class TestRailThreeIsNowTheWallsOwnSwitch:
    """The pair of runs that isolates the cause to ONE flag."""

    def test_a_tile_is_honoured_with_route_preview_OFF(self):
        # THE FIX. Before 2026-09-12 this returned the pinned target, because
        # preview's flag decided a wall's fate.
        result = _resolve(_sign_wall(), preview=False, honour=True, sticky=True)
        assert result["target_id"] == str(_CODED_TARGET), (
            "a wall tile was refused while the wall's own switch is ON. Rail 3 "
            "has been re-coupled to route_preview_enabled."
        )

    def test_the_WALL_switch_OFF_still_makes_the_branch_unreachable(self):
        # The kill switch survives the split — it moved to the flag whose name
        # matches the feature.
        result = _resolve(_sign_wall(), preview=False, honour=False, sticky=True)
        assert result["target_id"] == _PINNED_TARGET

    def test_the_ONLY_difference_between_those_two_runs_is_the_wall_switch(self):
        # Stated as its own assertion so a reader cannot mistake the pair above
        # for two unrelated cases. Same code, same hashes, same call site, same
        # preview flag: if the outcomes differ, `wall_tile_honour_enabled` is
        # what differs, and nothing else can be credited.
        code = _sign_wall()
        on = _resolve(code, preview=False, honour=True, sticky=True)
        off = _resolve(code, preview=False, honour=False, sticky=True)
        assert on["target_id"] != off["target_id"]
        assert (on["target_id"], off["target_id"]) == (
            str(_CODED_TARGET), _PINNED_TARGET)


class TestTheWallSwitchDoesNotArmRoutePreview:
    """The direction that would be a REGRESSION, not a missing feature.

    A v2 preview claim is the anonymous guess ADR-0454 subordinated and
    ADR-0515 refused to promote. Arming the wall must not hand it a door.
    """

    def test_a_v2_PREVIEW_code_is_still_refused_when_preview_is_off(self):
        result = _resolve(_sign(), preview=False, honour=True, sticky=False)
        assert result["target_id"] == _NORMAL_TARGET, (
            "a v2 preview code was honoured while route_preview_enabled is "
            "False. The per-claim gate has been widened to admit both kinds."
        )

    def test_positive_control_the_same_v2_code_IS_honoured_with_preview_on(self):
        # Without this, the refusal above would also pass against a fixture
        # that cannot honour anything at all.
        result = _resolve(_sign(), preview=True, honour=True, sticky=False)
        assert result["target_id"] == str(_CODED_TARGET)


class TestBothCallSitesObeyTheWallSwitch:
    """Why the gate is per-CLAIM and could not be per-CALLER.

    The non-sticky consult passes `wall_only=returning_flow_won`, which is
    False on an ordinary click — so it admits wall claims too. A gate keyed on
    `wall_only` would have armed preview on this path and left the wall unarmed
    on it, which is the confusion the split exists to end.
    """

    def test_the_non_sticky_caller_honours_a_tile_with_preview_OFF(self):
        result = _resolve(_sign_wall(), preview=False, honour=True, sticky=False)
        assert result["target_id"] == str(_CODED_TARGET)

    def test_the_non_sticky_caller_also_obeys_the_wall_switch_OFF(self):
        result = _resolve(_sign_wall(), preview=False, honour=False, sticky=False)
        assert result["target_id"] == _NORMAL_TARGET


def _mint(*, ring: bool, tiles=2):
    """Call the REAL mint with the ring armed or not; capture any op signal."""
    seen: list[tuple] = []

    def _capture(op, dedup_key, message, **extras):
        seen.append((op, dedup_key, message, extras))
        return True

    made = [
        WallTile(offer_id=_OFFER, offer_target_id=_CODED_TARGET + i)
        for i in range(tiles)
    ]
    keys = _KEYS if ring else ""
    kid = _ACTIVE_KID if ring else ""
    with patch.object(main, "capture_op_msg_throttled", _capture), \
            patch.object(settings, "route_code_keys", keys), \
            patch.object(settings, "route_code_active_kid", kid):
        expires = main._mint_tile_codes(
            made, company_id=_COMPANY, campaign_id=_CAMPAIGN, wall_id=WALL_ID,
        )
    return expires, made, seen


class TestRailTwoAnnouncesItselfInsteadOfDegradingSilently:
    def test_a_wall_served_without_a_ring_emits_the_op_signal(self):
        expires, made, seen = _mint(ring=False)
        assert expires is None
        assert all(t.route_code is None for t in made), (
            "all-or-nothing: a partially coded wall is worse than an uncoded one"
        )
        assert [s[0] for s in seen] == [OP_WALL_TILE_CODES_UNSIGNED]
        # The signal must name WHICH campaign and WHICH wall, or an operator
        # reading it learns only that something, somewhere, is unarmed.
        _op, dedup, _msg, extras = seen[0]
        assert dedup == _CAMPAIGN
        assert extras["campaign_id"] == _CAMPAIGN
        assert extras["wall_id"] == WALL_ID
        assert extras["tiles"] == 2

    def test_calibration_with_the_ring_armed_there_is_NO_signal(self):
        # The detector above is only evidence if it can stay quiet. Without
        # this, a capture that fired unconditionally would pass it.
        expires, made, seen = _mint(ring=True)
        assert expires is not None
        assert all(t.route_code for t in made)
        assert seen == []

    def test_an_EMPTY_wall_does_not_cry_about_a_missing_ring(self):
        # Nothing was published, so nothing is dead on arrival. Signalling here
        # would train an operator to ignore the tag.
        expires, _made, seen = _mint(ring=False, tiles=0)
        assert expires is None
        assert seen == []
