"""ADR-0515 + ADR-0516 — a wall tile outranks a RETURNING FLOW, and under a
STICKY campaign the landing it produces becomes the visitor's last destination.

The owner ruled both on 2026-09-08, quoted rather than paraphrased:

    «Плитка виграє.»

    «Має пам'ятати. … свідомий вибір ось цієї поплитки має закріплювати
     останній маршрут, якщо режим кампанії вибраний стійкий … Якщо в
     налаштуваннях цієї кампанії налаштовано, що вона не закріплює офер, тобто
     є returning visitor fresh режим, то … ми не запам'ятовуємо останній офер.»

🔴 WHY THIS FILE EXISTS SEPARATELY FROM `test_wall_tile_beats_the_pin.py`.

That file drives the resolver with `sticky_active=True`, which is the STICKY
path. Its one returning-flow case says so itself: the combination it asserts
"cannot arise from the caller today". It is defence, and defence cannot
demonstrate this change.

The reason is structural and it is the whole point of this file. `sticky_active`
carries the D35 exclusion (`audience != "returning"`), so a visitor a returning
flow captured has it FALSE — and the non-sticky block ends in an unconditional
`return`. Such a visitor therefore never reaches the sticky-path wall block at
all. They flow through the OTHER site, and that is the site both rulings had to
change. Every test here drives `sticky_active=False`.

🔴 AND WHY THE ASSERTIONS COUNT WRITES RATHER THAN READ VALUES.

A value assertion cannot catch a missing mode gate: under FRESH the B-track
writes the served target and a wall write would write the SAME target, so the
pin's value is identical whether the gate exists or not. Only the NUMBER of
writes discriminates. `FakeIdentRedis.set_calls` is that instrument.
"""

import asyncio
from unittest.mock import patch

from app import action_executor, router
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

_PIN_KEY = "sticky:%d:U:%d" % (_COMPANY, _CAMPAIGN)


# The offer the visitor is ALREADY pinned to in the sticky-path tests below.
# Deliberately neither the coded target nor the flow's normal pick, so "the pin
# won", "the tile won" and "the flow re-picked" are three DISTINGUISHABLE
# outcomes rather than two that can be mistaken for each other.
_PINNED_TARGET = "777"


def _hashes(*, wall_tiles=((_OFFER, _CODED_TARGET),)):
    h = {
        "offer_target:%d" % _CODED_TARGET: _target_hash(),
        # ⚠️ The pinned target MUST be readable and available, or the sticky
        # path treats the pin as INVALID, re-picks and repins - which looks
        # exactly like "the pin lost to the tile" while proving nothing about
        # precedence. Not hypothetical: omitting this row is precisely what the
        # first run of `test_a_PREVIEW_code_on_this_path_neither_wins_nor_writes`
        # did, and it failed for a reason with nothing to do with the code under
        # test. The fixture was the defect, not the subject.
        "offer_target:%s" % _PINNED_TARGET: {
            "url": "https://offer.example/pinned",
            "availability": "active",
            "offer_id": str(_OFFER),
        },
    }
    if wall_tiles is not None:
        h["flow:%d" % WALL_ID] = _wall(*wall_tiles)
    return h


def _resolve(
    code,
    *,
    returning_flow_won=True,
    wall_pin_eligible=True,
    fresh_track=False,
    hashes=None,
    ident=None,
    action_type="split",
    preview_enabled=True,
    honour_enabled=True,
):
    """Drive the REAL resolver down the NON-STICKY path — the one a visitor
    captured by a returning flow actually takes. Returns (result, status, ident)
    so a caller can assert on the WRITES as well as the destination."""
    ident = ident if ident is not None else FakeIdentRedis()
    r = FakeRoutingRedis(hashes=hashes if hashes is not None else _hashes())

    async def _gir():
        return ident

    async def _serve(*a, **k):
        # The returning flow's own pick — deliberately NOT the coded target, so
        # "the tile won" and "the tile was ignored" can never look alike.
        return {
            "url": "https://offer.example/" + _NORMAL_TARGET,
            "offer_id": str(_OFFER),
            "target_id": _NORMAL_TARGET,
            "target_selection_path": "split_weighted",
        }

    async def _runner():
        with patch.object(sticky_mod, "get_identity_redis", _gir), \
                patch.object(action_executor, "execute_action", _serve), \
                patch.object(settings, "route_preview_enabled", preview_enabled), \
                patch.object(settings, "wall_tile_honour_enabled", honour_enabled), \
                patch.object(settings, "route_code_keys", _KEYS), \
                patch.object(settings, "route_code_active_kid", _ACTIVE_KID), \
                patch.object(settings, "returning_uid_ttl_seconds", 1000):
            return await router._resolve_action_with_sticky(
                r, {"action_type": action_type}, _click(code), str(_CAMPAIGN),
                source_mappings={}, campaign_mappings={},
                sticky_active=False,
                returning_flow_won=returning_flow_won,
                uid="U", company_id=_COMPANY,
                seen_before=True, returning_visitor=True,
                flow_id="300", allowed_avail=frozenset({"active"}),
                fresh_track=fresh_track,
                wall_pin_eligible=wall_pin_eligible,
            )

    result, status = asyncio.run(_runner())
    return result, status, ident


class TestRow2TheTileOutranksAReturningFlow:
    def test_a_WALL_tile_wins_over_a_matching_returning_flow(self):
        result, _s, _i = _resolve(_sign_wall())
        assert result["target_id"] == str(_CODED_TARGET), (
            "owner 2026-09-08: «Плитка виграє» — the visitor's explicit choice "
            "outranks our inference about them"
        )
        assert result["target_selection_path"] == "route_code"

    def test_a_PREVIEW_code_in_the_IDENTICAL_fixture_still_YIELDS(self):
        # 🔴 The half that keeps the other honest, and the reason the guard is
        # NARROWED rather than deleted. Deleting it would pass the test above
        # and hand a v2 PREVIEW code the same victory — measured 10/10 on
        # staging 2026-09-03 as a real defect.
        result, _s, _i = _resolve(_sign())
        assert result["target_id"] == _NORMAL_TARGET, (
            "ADR-0454 still stands for a preview code: a landing page's guess "
            "about an anonymous visitor does not displace a returning flow"
        )

    def test_with_NO_returning_winner_a_preview_code_still_works(self):
        # The no-change case. `wall_only` is False here, so this path must be
        # exactly what it was before ADR-0515.
        result, _s, _i = _resolve(_sign(), returning_flow_won=False)
        assert result["target_id"] == str(_CODED_TARGET)

    def test_a_tile_REMOVED_from_its_wall_does_not_win(self):
        result, _s, _i = _resolve(
            _sign_wall(), hashes=_hashes(wall_tiles=((_OFFER, 999),)),
        )
        assert result["target_id"] == _NORMAL_TARGET

    def test_the_WALL_flag_OFF_makes_the_tile_inert(self):
        # 🔴 RE-POINTED 2026-09-12 (U2) — see the same correction in
        # `test_wall_tile_beats_the_pin.py`. The kill switch is now the wall's
        # own; `route_preview_enabled` is held False here as well, so this also
        # shows the two are independent rather than merely renamed.
        result, _s, ident = _resolve(
            _sign_wall(), preview_enabled=False, honour_enabled=False,
        )
        assert result["target_id"] == _NORMAL_TARGET
        assert ident.set_calls == [], "a demoted tile must not pin either"


class TestRow17TheStickyPinIsWritten:
    def test_a_tile_landing_under_STICKY_writes_the_pin(self):
        result, _s, ident = _resolve(_sign_wall())
        assert result["target_id"] == str(_CODED_TARGET)
        assert len(ident.set_calls) == 1, (
            "exactly one write: the tile landing becomes the last destination"
        )
        key, value, nx, _ex = ident.set_calls[0]
        assert key == _PIN_KEY
        assert value == str(_CODED_TARGET), (
            "the pin names the offer the visitor CHOSE, not the flow's pick"
        )
        assert nx is False, (
            "overwrite, not NX — a later tile must be able to replace an "
            "earlier one (owner: «якщо він перейде на інший офер з плитки»)"
        )

    def test_a_PREVIEW_code_NEVER_writes(self):
        # ADR-0516 is a NARROW supersession. This is the line it does not cross.
        _r, _s, ident = _resolve(_sign(), returning_flow_won=False)
        assert ident.set_calls == [], (
            "ADR-0454's no-write rule survives intact for a v2 preview code"
        )

    def test_under_FRESH_the_tile_adds_NO_SECOND_write(self):
        # 🔴 THE COUNT IS THE ASSERTION. Under fresh the B-track writes the
        # served target and a leaked wall write would write the SAME target, so
        # the pin's VALUE is identical either way. Only the number tells them
        # apart — which is why a value check here would be a test that cannot
        # fail for the defect it exists to catch.
        _r, _s, ident = _resolve(
            _sign_wall(), wall_pin_eligible=False, fresh_track=True,
        )
        assert len(ident.set_calls) == 1, (
            "fresh writes exactly once, via the B-track, as it always did"
        )

    def test_a_non_offer_flow_does_not_pin(self):
        # The gate keeps `action_type in (offer, split)` — not copied from its
        # siblings but kept because the pin's only READER carries the same term,
        # so a pin written outside it could never be read as intended.
        _r, _s, ident = _resolve(
            _sign_wall(), wall_pin_eligible=False, action_type="redirect",
        )
        assert ident.set_calls == []

    def test_the_write_is_FAIL_OPEN(self):
        # B11 — proven by a fault probe, never inherited from "the neighbours
        # swallow errors". A persistence fault must not turn a valid explicit
        # choice back into ordinary selection.
        class Exploding(FakeIdentRedis):
            async def set(self, *a, **k):
                raise RuntimeError("redis down")

        result, _s, _i = _resolve(_sign_wall(), ident=Exploding())
        assert result["target_id"] == str(_CODED_TARGET), (
            "the redirect survives a failed pin write"
        )


class TestThePrivateKindFlagNeverEscapes:
    def test_the_result_carries_no_underscore_wall_claim(self):
        # It authorises the write and must not reach the click record, where an
        # unknown key is a silent contract change nobody asked for.
        result, _s, _i = _resolve(_sign_wall())
        assert "_wall_claim" not in result

    def test_a_yielding_preview_result_carries_it_either(self):
        result, _s, _i = _resolve(_sign(), returning_flow_won=False)
        assert "_wall_claim" not in result


class TestTheOtherWriteSiteTheStickyPath:
    """🔴 THIS CLASS EXISTS BECAUSE MUTATION FOUND ITS ABSENCE.

    Every test above drives `sticky_active=False`, which is the path a visitor
    captured by a returning flow takes. A visitor NOT so captured, on the same
    sticky campaign, takes the OTHER path — and deleting that site's write left
    the whole suite green. A green suite that survives the deletion of a write
    is not evidence the write exists.

    Both sites are needed and they are disjoint by construction: reaching the
    non-sticky block means `sticky_active` was False, reaching this one means it
    was True. Neither test can stand in for the other.
    """

    @staticmethod
    def _resolve_sticky(code, *, wall_pin_eligible=True, pinned=_PINNED_TARGET):
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
            return pinned

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
                    returning_flow_won=False,
                    uid="U", company_id=_COMPANY,
                    seen_before=True, returning_visitor=True,
                    flow_id="300", allowed_avail=frozenset({"active"}),
                    fresh_track=False,
                    wall_pin_eligible=wall_pin_eligible,
                )

        result, status = asyncio.run(_runner())
        return result, status, ident

    def test_a_tile_REPLACES_a_live_pin_and_the_new_pin_is_written(self):
        # Owner: «якщо він перейде на інший офер з плитки» — a second tile must
        # be able to replace what the first one pinned. The visitor arrives
        # pinned to 777 and leaves pinned to the tile's target.
        result, _s, ident = self._resolve_sticky(_sign_wall())
        assert result["target_id"] == str(_CODED_TARGET), "the tile beat the pin"
        assert len(ident.set_calls) == 1, "the tile landing was recorded"
        key, value, nx, _ex = ident.set_calls[0]
        assert key == _PIN_KEY
        assert value == str(_CODED_TARGET)
        assert nx is False, "overwrite — NX would refuse to replace the old pin"

    def test_a_PREVIEW_code_on_this_path_neither_wins_nor_writes(self):
        result, _s, ident = self._resolve_sticky(_sign())
        assert result["target_id"] == _PINNED_TARGET, "ADR-0454 stands: the pin wins"
        assert ident.set_calls == [], "and a preview code still writes nothing"

    def test_under_FRESH_settings_this_site_does_not_write(self):
        # `wall_pin_eligible` False stands in for a campaign in fresh mode.
        # The tile still wins the ROUTE (that is D3, unchanged) and simply
        # leaves no memory — «fresh … ми не запам'ятовуємо останній офер».
        result, _s, ident = self._resolve_sticky(
            _sign_wall(), wall_pin_eligible=False,
        )
        assert result["target_id"] == str(_CODED_TARGET)
        assert ident.set_calls == []

    def test_the_private_kind_flag_does_not_escape_here_either(self):
        result, _s, _i = self._resolve_sticky(_sign_wall())
        assert "_wall_claim" not in result
