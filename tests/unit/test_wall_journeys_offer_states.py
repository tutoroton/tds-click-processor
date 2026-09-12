"""J3 — a real tile link, then the offer changes underneath it.

🔴 THE BOX ASKS FOR FOUR STATES; THE NODE HAS THREE MECHANISMS, AND THAT IS A
CORRECTION, NOT A SHORTCUT. The plan says *"archived · paused · not-accepting ·
removed from the flow — **separately**"*. Read against the code, those four
reach the node as three distinct things, because `status` and `availability` are
**orthogonal axes** (`app/common/availability.py`, ADR-0029/0030):

  * `status`       — `active | paused | archived`  (mechanical)
  * `availability` — `active | draining | closed`  (traffic)

and the sync builder's SQL is
`WHERE ot.status = 'active' AND o.status = 'active'`
(`services/admin-api/app/sync/builders/offer_targets.py`). `status` is the
**PRESENCE filter**; availability is not. So:

  | operator did | what the NODE sees |
  |---|---|
  | archived the offer/target | the `offer_target:` hash is **ABSENT** |
  | paused it | the hash is **ABSENT** — the SAME mechanism |
  | set it not-accepting | hash **PRESENT**, `availability: "closed"` |
  | removed it from the wall | hash present and open, **tile gone from the wall** |

Testing archived and paused "separately" AT THE NODE is therefore testing one
thing twice. What genuinely discriminates them lives one service up, in the
builder — so this file pins the builder's presence filter (below) and then
exercises the three mechanisms the node can actually tell apart.

WHAT WAS ALREADY BUILT, and is deliberately not repeated:
`test_wall_draining_matrix.py::TestT4ATileLinkToAnUnavailableTarget` already
covers **not-accepting** (`closed`) against the WALL's own default, and says so
in its own docstring. `draining` likewise. This file adds the two mechanisms
that had **zero** coverage in the wall frame — an ABSENT row, and a tile removed
from the wall — plus the guard that the expected default is COMPUTED rather than
constant.

THE POINT OF THE BOX, in its own words: *"Rows 8-11 were proven on the OLD
default (an ordinary flow), never against the wall's first-tile default."* Every
assertion here therefore names a TILE of the wall, never `_NORMAL_TARGET`. The
seed is local for the same reason `test_wall_draining_matrix.py`'s is: the
shared fixtures always write both target rows and always list both tiles, which
is precisely the condition that makes these two mechanisms unreachable.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from unittest.mock import patch

import fakeredis.aioredis

from app import identity as identity_mod
from app import router
from app import sticky as sticky_mod
from app.config import settings
from app.models import ClickRequest

from tests.unit.test_route_code_honoured import (
    _ACTIVE_KID,
    _CAMPAIGN,
    _COMPANY,
    _KEYS,
    _OFFER,
    FakeIdentRedis,
)
from tests.unit.test_wall_draining_matrix import TILE_1, TILE_2, _UID, _served
from tests.unit.test_wall_tile_membership import WALL_ID, _sign_wall


async def _seed(r, *, absent=(), wall_tiles=(TILE_1, TILE_2),
                avails=None) -> dict:
    """One offerwall campaign, two tiles, with TWO axes the shared seeds lack.

    `absent`     — target ids whose `offer_target:` hash is NOT written at all,
                   which is exactly what an archived or paused offer looks like
                   from the node's side (the builder never publishes it).
    `wall_tiles` — which tiles the wall's `action_config` lists, so a tile can
                   be REMOVED from the wall while its target stays perfectly
                   serveable. The two are independent, and conflating them is
                   how "removed from the flow" gets mistaken for "unavailable".
    """
    avails = avails or {}
    await r.hset("campaign:%d" % _CAMPAIGN, mapping={
        "company_id": str(_COMPANY),
        "flow_mode": "global",
        "returning_mode": "fresh",
        "returning_routing": "1",
        "flow_family": "offerwall",
        "disable_returning_flows": "1",
    })

    await r.hset("flow:%d" % WALL_ID, mapping={
        "campaign_id": str(_CAMPAIGN), "company_id": str(_COMPANY),
        "scope_type": "company", "scope_id": str(_COMPANY),
        "audience": "offerwall", "action_type": "offerwall",
        "criteria": "[]", "seq_id": "1", "is_default": "0",
        "action_config": json.dumps({"tiles": [
            {"offer_id": _OFFER, "target_id": t} for t in wall_tiles
        ]}),
    })
    await r.rpush("campaign:%d:walls" % _CAMPAIGN, str(WALL_ID))

    for tid in (TILE_1, TILE_2):
        if tid in absent:
            continue
        await r.hset("offer_target:%d" % tid, mapping={
            "url": "https://land/%d" % tid,
            "availability": avails.get(tid, "active"),
            "is_default": "0", "offer_id": str(_OFFER),
            "criteria": "[]", "priority": "0",
        })

    campaign = await r.hgetall("campaign:%d" % _CAMPAIGN)
    campaign["_id"] = str(_CAMPAIGN)
    return campaign


def _run(*, code=None, absent=(), wall_tiles=(TILE_1, TILE_2), avails=None,
         seen_before: bool = True):
    """Drive the REAL composing frame, `_route_via_campaign`.

    Same entry point as the draining matrix, and for the same reason: entering
    lower would be HANDED the fallback instead of watching the wall compute it,
    and the fallback is the whole subject of this box.
    """
    ident = FakeIdentRedis(strings={})

    async def _gir():
        return ident

    async def _stamp(**kw):
        return identity_mod.IdentityResult(
            uid=_UID, is_returning=seen_before, seen_before=seen_before,
            signal_tier="cookie",
        )

    async def _inner():
        r = fakeredis.aioredis.FakeRedis(decode_responses=True)
        campaign = await _seed(r, absent=absent, wall_tiles=wall_tiles,
                               avails=avails)
        req = ClickRequest(
            click_id="wos" + "0" * 18, country="US", user_agent="t/1.0",
            query_params={router.ROUTE_CODE_PARAM: code} if code else {},
        )
        with patch.object(sticky_mod, "get_identity_redis", _gir), \
                patch.object(identity_mod, "resolve_and_stamp", _stamp), \
                patch.object(settings, "returning_resolver_enabled", True), \
                patch.object(settings, "returning_routing_enabled", True), \
                patch.object(settings, "wall_delivery_enabled", True), \
                patch.object(settings, "wall_tile_honour_enabled", True), \
                patch.object(settings, "route_preview_enabled", True), \
                patch.object(settings, "route_code_keys", _KEYS), \
                patch.object(settings, "route_code_active_kid", _ACTIVE_KID), \
                patch.object(settings, "returning_uid_ttl_seconds", 1000):
            return await router._route_via_campaign(
                r, campaign, str(_CAMPAIGN), req, {}, result_label="matched",
            )

    return asyncio.run(_inner())


class TestTheNodeSeesTHREEMechanismsNotFour:
    """The correction, made checkable instead of merely asserted in prose.

    This reaches into admin-api's source ON PURPOSE, and it is the only test
    here that does. The claim *"archived and paused are the same thing at the
    node"* is a claim about the node's INPUTS, and its truth lives in the
    builder's WHERE clause. Without this pin the file below would exercise "an
    absent row" while nothing connected that to the operator action the box
    actually names. If the builder ever starts publishing non-active rows, the
    node gains a fourth mechanism and this goes red — which is the day someone
    must come back and split these cases apart.
    """

    @staticmethod
    def _builder_src() -> str:
        root = Path(__file__).resolve().parents[4]
        p = root / "services/admin-api/app/sync/builders/offer_targets.py"
        assert p.is_file(), (
            "the offer_targets sync builder is not where this test expects it "
            "(%s). It may have moved; re-point this pin rather than deleting "
            "it — the claim it guards is still load-bearing for J3." % p
        )
        return p.read_text(encoding="utf-8")

    def test_status_is_the_PRESENCE_filter_for_both_levels(self):
        src = self._builder_src()
        assert "WHERE ot.status = 'active' AND o.status = 'active'" in src, (
            "the builder's presence filter changed. archived/paused may no "
            "longer collapse to 'row absent' at the node, in which case J3's "
            "three mechanisms have become four."
        )

    def test_availability_is_NOT_a_presence_filter(self):
        # The other half of the same claim, and the one that makes it
        # informative: if availability ALSO filtered presence, a closed target
        # would vanish instead of being excluded-and-re-picked, and the
        # NO-DEAD-END property the wall depends on would be gone.
        src = self._builder_src()
        assert "ot.availability" not in src.split("WHERE")[1].split("ORDER BY")[0], (
            "availability has entered the WHERE clause — a closed target would "
            "now DISAPPEAR from Redis rather than be excluded at the edge"
        )


class TestTheControlComesFirst:
    """Without these, every refusal below is satisfied by a wall that serves
    nobody, or by a code that is never honoured in this fixture."""

    def test_an_UNCHANGED_offer_serves_the_tile_the_link_names(self):
        result = _run(code=_sign_wall(target_id=TILE_1))
        assert result is not None, "the wall served nobody at all"
        assert _served(result) == str(TILE_1)

    def test_and_the_SECOND_tile_too_when_its_link_is_the_one_held(self):
        result = _run(code=_sign_wall(target_id=TILE_2))
        assert result is not None
        assert _served(result) == str(TILE_2)

    def test_with_no_link_at_all_the_wall_serves_its_first_tile(self):
        # The independently computed default, stated once as its own fact: the
        # wall's PUBLICATION order decides, so every expectation below is read
        # off the seed rather than copied from what the code returned.
        result = _run()
        assert result is not None
        assert _served(result) == str(TILE_1)


class TestArchivedOrPausedTheRowIsABSENT:
    """Mechanism 1 — zero coverage in the wall frame before this file.

    An archived or paused offer is not published, so the node holds a tile link
    pointing at a hash that no longer exists. The owner's rule for a stale tile
    (D3-OPEN-3) is *"falls back to the DEFAULT scenario, not an error"* — and on
    a wall campaign that default is the wall's own next eligible tile, never an
    ordinary flow.
    """

    def test_a_link_to_an_ABSENT_target_lands_on_the_walls_next_tile(self):
        result = _run(code=_sign_wall(target_id=TILE_1), absent=(TILE_1,))
        assert result is not None, (
            "an archived/paused tile dead-ended the wall — the visitor got "
            "nothing, which is the one outcome D3-OPEN-3 forbids"
        )
        assert _served(result) == str(TILE_2)

    def test_the_expected_default_is_COMPUTED_not_the_constant_TILE_2(self):
        # 🔴 THE ANTI-CONSTANT GUARD. Every row above expects TILE_2, so all of
        # them would still pass against a wall that always served its second
        # tile. Absent the SECOND tile instead and the answer must move to the
        # FIRST — which can only happen if publication order is really what
        # decides.
        #
        # CALIBRATED, and the calibration is worth recording because the first
        # mutation written for it did NOT fire: appending `tiles[1]` to the walk
        # changes nothing, since the walk already reaches it. What this row does
        # discriminate is a fallback pinned to a FIXED POSITION — proven with
        # `for tile in tiles[-1:]` ("the answer is always the last tile"), which
        # turns it red. Stating the mutation that works, rather than the one
        # that sounded right, is the difference between a guard and a slogan.
        result = _run(code=_sign_wall(target_id=TILE_2), absent=(TILE_2,))
        assert result is not None
        assert _served(result) == str(TILE_1)

    def test_BOTH_tiles_absent_does_not_serve_a_phantom(self):
        # The terminal case. Whatever the campaign answers, it must not be a
        # target whose row does not exist.
        result = _run(code=_sign_wall(target_id=TILE_1),
                      absent=(TILE_1, TILE_2))
        assert _served(result) not in (str(TILE_1), str(TILE_2)), (
            "the wall served a target that has no row in Redis"
        )


class TestRemovedFromTheWall:
    """Mechanism 3 — proven here against the WALL's default, which is the
    correction the box asks for.

    `test_wall_tile_membership.py` already proves a removed tile is refused, but
    its harness stubs `execute_action` and so lands the visitor on
    `_NORMAL_TARGET` — an ORDINARY flow. That is the OLD default the box says
    rows 8-11 were proven against. On a wall campaign the fallback is the wall's
    own next eligible tile, and that is what these rows assert.
    """

    def test_a_link_whose_tile_was_REMOVED_lands_on_the_walls_next_tile(self):
        # TILE_1's target row is untouched and perfectly serveable — only the
        # wall no longer lists it. If the refusal came from availability or from
        # a missing row, this would prove nothing about membership.
        result = _run(code=_sign_wall(target_id=TILE_1),
                      wall_tiles=(TILE_2,))
        assert result is not None
        assert _served(result) == str(TILE_2)

    def test_the_removed_target_is_still_SERVEABLE_which_is_what_makes_it_a_test(self):
        # The discriminator for the row above: the same TILE_1, on a wall that
        # DOES list it, is served. So the refusal is membership and nothing else.
        result = _run(code=_sign_wall(target_id=TILE_1),
                      wall_tiles=(TILE_1, TILE_2))
        assert _served(result) == str(TILE_1)

    def test_removing_the_SECOND_tile_moves_the_answer_the_other_way(self):
        # Anti-constant again, on this mechanism's own axis.
        result = _run(code=_sign_wall(target_id=TILE_2),
                      wall_tiles=(TILE_1,))
        assert result is not None
        assert _served(result) == str(TILE_1)


class TestTheMechanismsDoNotCollapseIntoEachOther:
    """Three states, three different CAUSES, one visible outcome.

    Every refusal above ends at the same place, so a fixture that refused
    everything for one reason would satisfy all of them. These rows hold the
    causes apart by showing that each one's SIBLING condition still serves.
    """

    def test_an_absent_row_refuses_while_the_same_tile_PRESENT_serves(self):
        absent = _run(code=_sign_wall(target_id=TILE_1), absent=(TILE_1,))
        present = _run(code=_sign_wall(target_id=TILE_1))
        assert _served(absent) == str(TILE_2)
        assert _served(present) == str(TILE_1)

    def test_a_removed_tile_refuses_while_the_same_row_ON_the_wall_serves(self):
        off_wall = _run(code=_sign_wall(target_id=TILE_1), wall_tiles=(TILE_2,))
        on_wall = _run(code=_sign_wall(target_id=TILE_1))
        assert _served(off_wall) == str(TILE_2)
        assert _served(on_wall) == str(TILE_1)

    def test_closed_is_a_THIRD_cause_and_still_behaves_as_its_own_suite_pins(self):
        # Not a re-test of `TestT4ATileLinkToAnUnavailableTarget` — a check that
        # the three causes coexist in ONE fixture family rather than each being
        # true only in its own. A change that made `closed` mean "absent" would
        # pass both suites separately and go red here.
        closed = _run(code=_sign_wall(target_id=TILE_1),
                      avails={TILE_1: "closed"})
        assert _served(closed) == str(TILE_2)
        # …and the row is genuinely still THERE, which is what distinguishes
        # this cause from mechanism 1.
        assert _run(code=_sign_wall(target_id=TILE_1),
                    avails={TILE_1: "draining"}) is not None
