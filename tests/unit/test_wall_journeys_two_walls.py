"""J2 — TWO walls in ONE campaign, both PRESENT, with DIFFERENT tile sets.

WHY THIS FILE EXISTS, and it is not quite the reason the plan's wording suggests.
Risk **A7** reads: *"two walls in one campaign holding the same offer mint
indistinguishable codes … neither wall-specific membership validation nor origin
attribution is reconstructible"*. The SIGNING half was built — v3 carries a
signed `origin_flow_id` (`route_code.py`), and the trace stamps it
(`test_origin_wall_id_in_routing_trace.py`). **The VALIDATION half has never
been discriminated by a test**, because no fixture anywhere has ever had TWO
wall rows present at once.

MEASURED, not argued (2026-09-12). A mutation making membership WALL-PROMISCUOUS
— "honour the tile if ANY wall of this campaign carries it" — left the entire
wall / route-code selection at **379 passed, 0 failed**. The correct
implementation and the promiscuous one are, to every existing suite, the same
program.

🔴 WHY THE EXISTING NEAR-MISS DOES NOT COVER IT, in its own words.
`test_origin_wall_id_in_routing_trace.py::test_the_fixture_would_notice_a_wrong_wall_id`
signs `origin_flow_id=_OTHER_WALL` and its comment says plainly: *"798's row is
absent from Redis, so membership fails fail-closed"*. That is a true and useful
test of the ABSENT case. It cannot see the promiscuous one, because with only
one wall row present "the origin wall carries it" and "some wall carries it"
cannot disagree. **A refusal produced by a missing fixture row is
indistinguishable from a refusal produced by the rule** — the lesson J1 paid for
with a vacuous preview row, arriving here from the opposite direction.

WHAT IS DELIBERATELY NOT HERE. The plan's J2 box also names *"first-by-position
vs split"*. That is **already built** — `test_wall_delivers_the_click.py`
(`test_the_first_tile_wins_not_an_arbitrary_one`, `TestFirstEligibleNotJustFirst`)
pins first-eligible-by-position against the split's weighted pick. A second copy
would add maintenance cost and no discrimination, so the box's wording is
recorded as overstating what was missing rather than satisfied twice.

THE FIXTURE'S OWN DISCRIMINATION IS ASSERTED FIRST
(`TestTheFixtureItselfDiscriminates`). Without that, every refusal below could
come from a seed that refuses everything.
"""

from __future__ import annotations

import asyncio
import json
from unittest.mock import patch

from app import action_executor, route_code, router, sticky as sticky_mod
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
    _target_hash,
)

#: The two walls. Both are REAL rows in the seed — that is the whole point.
_W1 = 910
_W2 = 911

#: A tile both walls carry. Membership must succeed naming EITHER wall.
_SHARED_TARGET = _CODED_TARGET          # 99

#: A tile only W2 carries. The discriminator: a code naming W1 must be refused
#: even though the target itself is perfectly serveable and W2 does carry it.
_W2_ONLY_TARGET = 98

#: A campaign that is not this click's. `_sign_wall` in the membership suite has
#: no `campaign_id` parameter at all, so the campaign bind has never been
#: exercised on a v3 WALL claim — only on v2 previews
#: (`test_route_code_honoured.py`). The check lives in `verify()`, ahead of the
#: wall branch, so it SHOULD hold; "should" is what a test is for.
_OTHER_CAMPAIGN = _CAMPAIGN + 1


def _wall(*pairs) -> dict:
    """A wall row shaped the way the sync builder publishes it."""
    return {"action_config": json.dumps(
        {"tiles": [{"offer_id": o, "target_id": t} for o, t in pairs]})}


class _TwoWallRedis(FakeRoutingRedis):
    """`FakeRoutingRedis` plus the campaign's WALL LIST.

    🔴 THE LIST IS NOT DECORATION — IT IS WHAT MAKES THE MUTATION RUNNABLE.
    A promiscuous implementation reaches the other walls through
    `campaign:{id}:walls`. On a fake with no `lrange` it would raise, the honour
    hook's fail-open `except` would swallow it, and EVERY case would refuse — so
    the happy-path rows would go red and the discriminator would pass for the
    wrong reason. Seeding the list lets the wrong implementation actually work,
    which is the only state in which refusing proves anything.
    """

    def __init__(self, hashes=None, wall_list=None, **kw):
        super().__init__(hashes=hashes, **kw)
        self._wall_list = [str(w) for w in (wall_list or [])]

    async def lrange(self, key, start, end):
        self.reads.append(key)
        if key != f"campaign:{_CAMPAIGN}:walls":
            return []
        items = self._wall_list
        return items[start:] if end == -1 else items[start:end + 1]


def _seed(*, w1_tiles=((_OFFER, _SHARED_TARGET),),
          w2_tiles=((_OFFER, _SHARED_TARGET), (_OFFER, _W2_ONLY_TARGET))) -> dict:
    """Both targets serveable, both walls present, tile sets DIFFERENT.

    Both `offer_target` rows exist on purpose. If the W2-only target were
    absent, the discriminator below would refuse at the TARGET LOOKUP and prove
    nothing about membership — exactly the vacuous row J1 found in its own
    preview case.
    """
    h = {
        "offer_target:%d" % _SHARED_TARGET: _target_hash(),
        "offer_target:%d" % _W2_ONLY_TARGET: _target_hash(),
    }
    if w1_tiles is not None:
        h["flow:%d" % _W1] = _wall(*w1_tiles)
    if w2_tiles is not None:
        h["flow:%d" % _W2] = _wall(*w2_tiles)
    return h


def _sign_wall(*, origin, target_id=_SHARED_TARGET, campaign_id=_CAMPAIGN,
               offer_id=_OFFER) -> str:
    """A v3 WALL code.

    Unlike the membership suite's helper, the campaign is a PARAMETER here —
    that is the second thing this file exercises.
    """
    with patch.object(settings, "route_code_keys", _KEYS), \
            patch.object(settings, "route_code_active_kid", _ACTIVE_KID):
        return route_code.sign(
            company_id=_COMPANY, campaign_id=campaign_id, offer_id=offer_id,
            offer_target_id=target_id, ttl_seconds=600, origin_flow_id=origin,
        )


def _run(*, code, hashes=None, wall_list=(_W1, _W2)):
    """Drive the REAL resolver, with a trace so origin attribution is readable.

    Mirrors `test_route_code_honoured._resolve` in every other respect; the two
    differences (the wall list and the trace) are exactly what J2 is about.
    """
    r = _TwoWallRedis(hashes=hashes if hashes is not None else _seed(),
                      wall_list=wall_list)
    ident = FakeIdentRedis()
    trace: dict = {}

    async def _gir():
        return ident

    async def _serve(*a, **k):
        return {
            "url": "https://offer.example/" + _NORMAL_TARGET,
            "offer_id": str(_OFFER),
            "target_id": _NORMAL_TARGET,
            "target_selection_path": "split_weighted",
        }

    async def _runner():
        with patch.object(sticky_mod, "get_identity_redis", _gir), \
                patch.object(action_executor, "execute_action", _serve), \
                patch.object(settings, "route_preview_enabled", False), \
                patch.object(settings, "wall_tile_honour_enabled", True), \
                patch.object(settings, "route_code_keys", _KEYS), \
                patch.object(settings, "route_code_active_kid", _ACTIVE_KID), \
                patch.object(settings, "returning_uid_ttl_seconds", 1000):
            return await router._resolve_action_with_sticky(
                r, {"action_type": "split"}, _click(code), str(_CAMPAIGN),
                source_mappings={}, campaign_mappings={},
                sticky_active=False, returning_flow_won=False,
                uid="U", company_id=_COMPANY,
                seen_before=True, returning_visitor=True,
                flow_id="300", allowed_avail=frozenset({"active"}),
                trace=trace, fresh_track=False,
            )

    result, _status = asyncio.run(_runner())
    return result, trace, r


def _served(result) -> str:
    return result["target_id"]


class TestTheFixtureItselfDiscriminates:
    """Before any assertion about the rule, prove the seed can tell them apart.

    J1's lesson applied at the TOP of the file rather than discovered at the
    bottom: a matrix whose inputs have collapsed keeps passing and stops meaning
    anything.
    """

    def test_BOTH_walls_are_present_as_real_rows(self):
        seed = _seed()
        assert "flow:%d" % _W1 in seed
        assert "flow:%d" % _W2 in seed

    def test_the_two_walls_carry_DIFFERENT_tile_sets(self):
        from app import offerwall
        seed = _seed()
        w1, w2 = seed["flow:%d" % _W1], seed["flow:%d" % _W2]
        assert offerwall.wall_contains_tile(w1, _OFFER, _SHARED_TARGET)
        assert offerwall.wall_contains_tile(w2, _OFFER, _SHARED_TARGET)
        # The asymmetry that makes the discriminator possible.
        assert not offerwall.wall_contains_tile(w1, _OFFER, _W2_ONLY_TARGET)
        assert offerwall.wall_contains_tile(w2, _OFFER, _W2_ONLY_TARGET)

    def test_the_W2_ONLY_target_is_itself_perfectly_SERVEABLE(self):
        # 🔴 Without this, the refusal below could come from the target lookup
        # instead of from membership, and the suite would measure nothing.
        # Proven by SERVING it through the real path from the wall that does
        # carry it — an assertion about the seed dict would only restate the seed.
        result, _t, _r = _run(code=_sign_wall(origin=_W2,
                                              target_id=_W2_ONLY_TARGET))
        assert _served(result) == str(_W2_ONLY_TARGET)

    def test_the_wall_LIST_is_reachable_so_a_wrong_implementation_can_run(self):
        # The fake must be able to serve a promiscuous implementation, or the
        # discriminator passes because the wrong code CRASHED, not because the
        # rule refused.
        async def _probe():
            r = _TwoWallRedis(hashes=_seed(), wall_list=(_W1, _W2))
            return await r.lrange("campaign:%d:walls" % _CAMPAIGN, 0, -1)
        assert asyncio.run(_probe()) == [str(_W1), str(_W2)]


class TestMembershipIsWallSPECIFIC:
    """A7's validation half — the work this file exists to do."""

    def test_a_tile_on_BOTH_walls_is_honoured_when_the_code_names_W1(self):
        result, trace, _r = _run(code=_sign_wall(origin=_W1))
        assert _served(result) == str(_SHARED_TARGET)
        assert trace["origin_wall_id"] == _W1

    def test_the_SAME_tile_is_honoured_when_the_code_names_W2(self):
        # Attribution follows the CODE, not the selection — and the two walls
        # are now both real, so this is the first place that claim is
        # falsifiable.
        result, trace, _r = _run(code=_sign_wall(origin=_W2))
        assert _served(result) == str(_SHARED_TARGET)
        assert trace["origin_wall_id"] == _W2

    def test_the_origin_in_the_trace_DIFFERS_between_those_two_runs(self):
        # Stated as its own assertion: the two rows above could both be
        # satisfied by a hook that stamps whatever the code says. What they
        # cannot both survive is a CONSTANT.
        _r1, t1, _ = _run(code=_sign_wall(origin=_W1))
        _r2, t2, _ = _run(code=_sign_wall(origin=_W2))
        assert t1["origin_wall_id"] != t2["origin_wall_id"]

    def test_a_tile_living_ONLY_on_W2_is_REFUSED_when_the_code_names_W1(self):
        # 🔴 THE DISCRIMINATOR OF THE WHOLE FILE.
        # Both walls exist. The target exists and is serveable. W2 carries this
        # tile. The ONLY reason to refuse is that the code's ORIGIN wall does
        # not carry it. A wall-promiscuous implementation honours this and is
        # invisible to every other suite in the repo (measured: 379 passed).
        result, trace, _r = _run(
            code=_sign_wall(origin=_W1, target_id=_W2_ONLY_TARGET))
        assert _served(result) == _NORMAL_TARGET, (
            "a tile was honoured against a wall that does not carry it — "
            "membership is no longer WALL-SPECIFIC (risk A7). The code named "
            "wall %d; only wall %d carries this tile." % (_W1, _W2)
        )
        assert result["target_selection_path"] == "split_weighted"
        # Nothing was attributed either: a claim that did not act must not leave
        # an origin behind.
        assert "origin_wall_id" not in trace, trace

    def test_CONTROL_the_IDENTICAL_fixture_honours_it_when_the_code_names_W2(self):
        # Same seed, same target, same everything — only the signed ORIGIN
        # differs. Without this row the refusal above would also be satisfied by
        # a fixture that refuses this target for some unrelated reason.
        result, trace, _r = _run(
            code=_sign_wall(origin=_W2, target_id=_W2_ONLY_TARGET))
        assert _served(result) == str(_W2_ONLY_TARGET)
        assert trace["origin_wall_id"] == _W2

    def test_the_ORIGIN_wall_is_the_row_actually_READ(self):
        # Mechanism, not just outcome: prove the hook reached W1's row and no
        # other. A pass built on never reading either wall would be a different
        # program with the same answer.
        _result, _trace, r = _run(code=_sign_wall(origin=_W1))
        assert "flow:%d" % _W1 in r.reads
        assert "flow:%d" % _W2 not in r.reads, (
            "the hook read the OTHER wall — membership is consulting more than "
            "the signed origin"
        )


class TestZeroOneAndManyTiles:
    """The box's tile-count axis, through the RESOLVER rather than the helper.

    `test_wall_tile_membership.py` already covers `wall_contains_tile` directly
    at these shapes. What was missing is the same shapes reaching the real
    honour hook with a second wall present to be wrongly consulted.
    """

    def test_ZERO_tiles_on_the_origin_wall_refuses_even_though_W2_has_it(self):
        result, trace, _r = _run(
            code=_sign_wall(origin=_W1),
            hashes=_seed(w1_tiles=()),
        )
        assert _served(result) == _NORMAL_TARGET
        assert "origin_wall_id" not in trace

    def test_ONE_tile_is_enough(self):
        result, _trace, _r = _run(
            code=_sign_wall(origin=_W1),
            hashes=_seed(w1_tiles=((_OFFER, _SHARED_TARGET),)),
        )
        assert _served(result) == str(_SHARED_TARGET)

    def test_MANY_tiles_and_the_match_is_the_LAST_one(self):
        # The wall contract puts the per-wall tile ceiling at 24. Placing the
        # match last is what makes this more than a restatement of the one-tile
        # case: a scan that stopped early, or looked only at position 1 (the
        # DELIVERY rule, which is a different question), would go red here.
        filler = tuple((_OFFER, 700 + i) for i in range(23))
        result, trace, _r = _run(
            code=_sign_wall(origin=_W1),
            hashes=_seed(w1_tiles=filler + ((_OFFER, _SHARED_TARGET),)),
        )
        assert _served(result) == str(_SHARED_TARGET)
        assert trace["origin_wall_id"] == _W1

    def test_a_wall_whose_row_is_MISSING_still_refuses_with_a_sibling_present(self):
        # The absent case the existing trace suite covers — repeated here ONLY
        # because the sibling wall is now present and could be substituted for
        # it. This is the case that used to be the whole of "two walls".
        result, trace, _r = _run(
            code=_sign_wall(origin=_W1),
            hashes=_seed(w1_tiles=None),
        )
        assert _served(result) == _NORMAL_TARGET
        assert "origin_wall_id" not in trace


class TestASecondCampaign:
    """The campaign bind, on a WALL claim — never exercised before.

    `test_route_code_honoured.py` proves a v2 PREVIEW code minted for campaign X
    is refused on campaign Y. A v3 wall code carries an extra claim and takes an
    extra branch; nothing proved the bind still held for it. It does — the check
    is in `verify()`, ahead of the wall branch — and now that is a measurement
    rather than an inference from where the code sits.
    """

    def test_a_WALL_code_minted_for_ANOTHER_campaign_is_refused(self):
        result, trace, _r = _run(
            code=_sign_wall(origin=_W1, campaign_id=_OTHER_CAMPAIGN))
        assert _served(result) == _NORMAL_TARGET, (
            "a wall code minted for campaign %d was honoured on campaign %d — "
            "the v2 campaign bind does not survive the v3 wall branch"
            % (_OTHER_CAMPAIGN, _CAMPAIGN)
        )
        assert "origin_wall_id" not in trace

    def test_CONTROL_the_same_code_on_its_OWN_campaign_is_honoured(self):
        result, _trace, _r = _run(
            code=_sign_wall(origin=_W1, campaign_id=_CAMPAIGN))
        assert _served(result) == str(_SHARED_TARGET)

    def test_the_refusal_happens_BEFORE_any_wall_is_read(self):
        # Mechanism: a cross-campaign code must die at the signature/bind check,
        # not by failing membership afterwards. If it reached the wall read, the
        # bind would be doing no work and a wall that happened to carry the tile
        # would serve it.
        _result, _trace, r = _run(
            code=_sign_wall(origin=_W1, campaign_id=_OTHER_CAMPAIGN))
        assert "flow:%d" % _W1 not in r.reads, r.reads
        assert "flow:%d" % _W2 not in r.reads, r.reads
