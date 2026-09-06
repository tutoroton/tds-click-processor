"""G7.2 — a WALL-kind code outranks a live sticky pin; a PREVIEW code does not.

The owner's D3, quoted rather than paraphrased:

    «Сильніша плитка. Якщо користувач хоче потрапити на конкретний офер, він
     потрапляє на конкретний офер.»
    «…переходи по ось такому форматом URL не діє прив'язка.»

🔴 `ADR-0454-route-code-yields-to-the-sticky-pin-and-never-writes-the-returning-system-outranks-a-guess-about-an-anonymous-visitor` IS NOT SUPERSEDED, and the pair of tests below
is what says so. (The full slug, once, because the NUMBER alone is ambiguous here: `ADR-0454`
also names `…-the-brocard-users-walk-lives-in-process-service-…`, an unrelated finance
decision. The later short references in this file resolve against this line.)
Its title reads *"the returning system outranks a guess about an anonymous
visitor"* — and a tile click is not a guess. A preview code is a PREDICTION the
landing page made; a tile code is a CHOICE the visitor made one click ago, from
a catalogue we showed them. Between a memory and a request, the request is newer
and it is theirs. So the discriminator is the code's KIND, and both sides are
asserted in the SAME fixture: identical pin, identical click, one v3 and one v2.

A test that only showed "the wall code wins" would be satisfied by a change that
simply stopped consulting the pin — which is why the v2 half is not a courtesy.
"""

import asyncio
from unittest.mock import patch

from app import action_executor, route_code, router
from app import sticky as sticky_mod
from app.config import settings

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
    _target_hash,
)
from tests.unit.test_wall_tile_membership import WALL_ID, _sign_wall, _wall

# The offer the visitor is ALREADY pinned to — deliberately NOT the coded one,
# so "the pin lost" and "the code was ignored" cannot look alike.
_PINNED_TARGET = "777"


def _hashes(*, wall_tiles=((_OFFER, _CODED_TARGET),)):
    h = {
        "offer_target:%d" % _CODED_TARGET: _target_hash(),
        "offer_target:%s" % _PINNED_TARGET: {
            "url": "https://offer.example/pinned",
            "availability": "active",
            "offer_id": str(_OFFER),
        },
    }
    if wall_tiles is not None:
        h["flow:%d" % WALL_ID] = _wall(*wall_tiles)
    return h


def _resolve_with_live_pin(code, *, hashes=None, returning_flow_won=False):
    """Drive the REAL resolver with `sticky_active=True` and a pin that HITS."""
    ident = FakeIdentRedis()
    r = FakeRoutingRedis(hashes=hashes if hashes is not None else _hashes())

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
                patch.object(settings, "route_preview_enabled", True), \
                patch.object(settings, "route_code_keys", _KEYS), \
                patch.object(settings, "route_code_active_kid", _ACTIVE_KID), \
                patch.object(settings, "returning_uid_ttl_seconds", 1000):
            return await router._resolve_action_with_sticky(
                r, {"action_type": "split"}, _click(code), str(_CAMPAIGN),
                source_mappings={}, campaign_mappings={},
                sticky_active=True,
                returning_flow_won=returning_flow_won,
                uid="U", company_id=_COMPANY,
                seen_before=True, returning_visitor=True,
                flow_id="300", allowed_avail=frozenset({"active"}),
                fresh_track=False,
            )

    return asyncio.run(_runner())


class TestTheDiscriminatorIsTheKIND:
    def test_a_WALL_code_beats_a_live_pin(self):
        result, _status = _resolve_with_live_pin(_sign_wall())
        assert result["target_id"] == str(_CODED_TARGET), (
            "owner D3: the tile is stronger"
        )
        assert result["target_selection_path"] == "route_code"

    def test_a_PREVIEW_code_in_the_IDENTICAL_fixture_still_yields(self):
        # 🔴 The half that keeps the other honest. Same pin, same click, same
        # everything — only the code's kind differs. Without this, a change that
        # simply stopped consulting the pin would score full marks above.
        result, _status = _resolve_with_live_pin(_sign())
        assert result["target_id"] == _PINNED_TARGET, (
            "ADR-0454 stands for a preview code: the pin wins"
        )

    def test_NO_code_at_all_still_serves_the_pin(self):
        result, _status = _resolve_with_live_pin(None)
        assert result["target_id"] == _PINNED_TARGET


class TestTheGuardsThatMakeItSafe:
    def test_a_returning_flow_still_outranks_the_tile(self):
        # ADR-0454 term 1, evaluated first. ⚠️ Defence rather than a live gate —
        # `sticky_active` is already forced False when a returning flow wins, so
        # this combination cannot arise from the caller today. It is asserted so
        # term 1 survives a change to how `sticky_active` is computed, which is
        # a drift this file's neighbour records as having happened once.
        result, _status = _resolve_with_live_pin(
            _sign_wall(), returning_flow_won=True,
        )
        assert result["target_id"] == _PINNED_TARGET

    def test_a_tile_REMOVED_from_its_wall_does_not_beat_the_pin(self):
        # Membership (G6.3) is what stops this precedence being a blank cheque
        # the wall keeps after the operator changed it.
        result, _status = _resolve_with_live_pin(
            _sign_wall(), hashes=_hashes(wall_tiles=((_OFFER, 999),)),
        )
        assert result["target_id"] == _PINNED_TARGET

    def test_an_UNREADABLE_wall_does_not_beat_the_pin(self):
        result, _status = _resolve_with_live_pin(
            _sign_wall(), hashes=_hashes(wall_tiles=None),
        )
        assert result["target_id"] == _PINNED_TARGET

    def test_the_flag_OFF_makes_the_whole_branch_unreachable(self):
        ident = FakeIdentRedis()
        r = FakeRoutingRedis(hashes=_hashes())

        async def _gir():
            return ident

        async def _serve(*a, **k):
            return {"url": "u", "offer_id": str(_OFFER),
                    "target_id": _NORMAL_TARGET,
                    "target_selection_path": "split_weighted"}

        async def _get_sticky(*a, **k):
            return _PINNED_TARGET

        async def _runner():
            with patch.object(sticky_mod, "get_identity_redis", _gir), \
                    patch.object(sticky_mod, "get_sticky", _get_sticky), \
                    patch.object(action_executor, "execute_action", _serve), \
                    patch.object(settings, "route_preview_enabled", False), \
                    patch.object(settings, "route_code_keys", _KEYS), \
                    patch.object(settings, "route_code_active_kid", _ACTIVE_KID), \
                    patch.object(settings, "returning_uid_ttl_seconds", 1000):
                return await router._resolve_action_with_sticky(
                    r, {"action_type": "split"}, _click(_sign_wall()),
                    str(_CAMPAIGN),
                    source_mappings={}, campaign_mappings={},
                    sticky_active=True, returning_flow_won=False,
                    uid="U", company_id=_COMPANY,
                    seen_before=True, returning_visitor=True,
                    flow_id="300", allowed_avail=frozenset({"active"}),
                    fresh_track=False,
                )

        result, _status = asyncio.run(_runner())
        assert result["target_id"] == _PINNED_TARGET

    def test_a_tile_from_ANOTHER_TENANT_does_not_beat_the_pin(self):
        foreign = _sign_wall_other_company()
        result, _status = _resolve_with_live_pin(foreign)
        assert result["target_id"] == _PINNED_TARGET


def _sign_wall_other_company() -> str:
    with patch.object(settings, "route_code_keys", _KEYS), \
            patch.object(settings, "route_code_active_kid", _ACTIVE_KID):
        return route_code.sign(
            company_id=_COMPANY + 1, campaign_id=_CAMPAIGN, offer_id=_OFFER,
            offer_target_id=_CODED_TARGET, ttl_seconds=600,
            origin_flow_id=WALL_ID,
        )


class TestTheTilePathStillWritesNOTHING:
    def test_beating_the_pin_does_not_repin(self):
        # The pin keeps exactly ONE writer — the ordinary path. D3's third row
        # (the plain URL later returns the visitor to the last offer they were
        # on) depends on that writer being the one that recorded it.
        ident = FakeIdentRedis()
        r = FakeRoutingRedis(hashes=_hashes())

        async def _gir():
            return ident

        async def _serve(*a, **k):
            return {"url": "u", "offer_id": str(_OFFER),
                    "target_id": _NORMAL_TARGET,
                    "target_selection_path": "split_weighted"}

        async def _get_sticky(*a, **k):
            return _PINNED_TARGET

        async def _runner():
            with patch.object(sticky_mod, "get_identity_redis", _gir), \
                    patch.object(sticky_mod, "get_sticky", _get_sticky), \
                    patch.object(action_executor, "execute_action", _serve), \
                    patch.object(settings, "route_preview_enabled", True), \
                    patch.object(settings, "route_code_keys", _KEYS), \
                    patch.object(settings, "route_code_active_kid", _ACTIVE_KID), \
                    patch.object(settings, "returning_uid_ttl_seconds", 1000):
                return await router._resolve_action_with_sticky(
                    r, {"action_type": "split"}, _click(_sign_wall()),
                    str(_CAMPAIGN),
                    source_mappings={}, campaign_mappings={},
                    sticky_active=True, returning_flow_won=False,
                    uid="U", company_id=_COMPANY,
                    seen_before=True, returning_visitor=True,
                    flow_id="300", allowed_avail=frozenset({"active"}),
                    fresh_track=False,
                )

        result, _status = asyncio.run(_runner())
        assert result["target_id"] == str(_CODED_TARGET)
        # BOTH pools, per this service's hazard 9: a detector watching one pool
        # reported zero writes while the other was being written.
        assert r.writes == [], r.writes
        assert ident.set_calls == [], ident.set_calls
