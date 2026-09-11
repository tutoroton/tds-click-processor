"""T1-T4 (C9) — `draining` on an offer wall. The gap §3.3a measured and named.

🔴 WHY THIS FILE EXISTS. Measured 2026-09-11 by counting in BOTH directions,
because one absence could mean "not applicable here" while two facing absences
mean nobody wrote the intersection:

    `draining` in the three wall suites                  -> 0
    `draining` in test_c2_availability_delivery.py       -> 8
    `wall` / `offerwall` / `flow_family` in that file    -> 0

So the availability machinery is tested thoroughly and the wall is tested
thoroughly, and the two had never met.

WHAT MAKES THE INTERSECTION DANGEROUS RATHER THAN MERELY UNTESTED. The
availability floor is computed from the visitor class:

    {"active", "draining"} if returning_visitor else {"active"}

and `returning_visitor` is itself False whenever the campaign's returning
partition is suppressed (`disable_returning_flows`). On a wall campaign with
returning disabled there is therefore NO returning class at all, every visitor
is classified fresh, and a `draining` tile is invisible to everyone.

Consequence, and it is the shape PR #4651 already named as the worst one: not a
dead end, which shows up as errors — a SILENT 100% diversion to the campaign
fallback while health stays green and the operator debugs the offer.

⚠️ THIS IS INHERITED ORDINARY-ROUTE BEHAVIOUR, NOT A WALL DEFECT. These tests
PIN the behaviour as it stands; whether it is what the owner wants for a wall is
question 5 of the anchor's §6 and is not ours to answer by changing code.
"""

from __future__ import annotations

import asyncio
import json
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
from tests.unit.test_wall_tile_membership import WALL_ID, _sign_wall

TILE_1 = 111
TILE_2 = 222
_UID = "U"


async def _seed(r, *, disable_returning: bool, avails: dict[int, str]) -> dict:
    """One offerwall campaign, one wall, two tiles with GIVEN availabilities.

    Deliberately a local seed rather than a reused one: every existing wall
    fixture hardcodes `availability: "active"`, which is exactly the condition
    that made this intersection unreachable.
    """
    campaign_hash = {
        "company_id": str(_COMPANY),
        "flow_mode": "global",
        "returning_mode": "fresh",
        "returning_routing": "1",
        "flow_family": "offerwall",
    }
    if disable_returning:
        campaign_hash["disable_returning_flows"] = "1"
    await r.hset("campaign:%d" % _CAMPAIGN, mapping=campaign_hash)

    await r.hset("flow:%d" % WALL_ID, mapping={
        "campaign_id": str(_CAMPAIGN), "company_id": str(_COMPANY),
        "scope_type": "company", "scope_id": str(_COMPANY),
        "audience": "offerwall", "action_type": "offerwall",
        "criteria": "[]", "seq_id": "1", "is_default": "0",
        "action_config": json.dumps({"tiles": [
            {"offer_id": _OFFER, "target_id": TILE_1},
            {"offer_id": _OFFER, "target_id": TILE_2},
        ]}),
    })
    await r.rpush("campaign:%d:walls" % _CAMPAIGN, str(WALL_ID))

    for tid in (TILE_1, TILE_2):
        await r.hset("offer_target:%d" % tid, mapping={
            "url": "https://land/%d" % tid,
            "availability": avails.get(tid, "active"),
            "is_default": "0", "offer_id": str(_OFFER),
            "criteria": "[]", "priority": "0",
        })
    campaign = await r.hgetall("campaign:%d" % _CAMPAIGN)
    campaign["_id"] = str(_CAMPAIGN)
    return campaign


def _run(*, disable_returning: bool, avails: dict[int, str],
         seen_before: bool = True, code: str | None = None):
    """Drive the REAL composing frame, `_route_via_campaign`.

    Entering lower would be handed the availability floor as an argument and so
    could not see it being COMPUTED from the visitor class — which is the whole
    subject here.
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
        campaign = await _seed(r, disable_returning=disable_returning,
                               avails=avails)
        req = ClickRequest(
            click_id="wdm" + "0" * 18, country="US", user_agent="t/1.0",
            query_params={router.ROUTE_CODE_PARAM: code} if code else {},
        )
        with patch.object(sticky_mod, "get_identity_redis", _gir), \
                patch.object(identity_mod, "resolve_and_stamp", _stamp), \
                patch.object(settings, "returning_resolver_enabled", True), \
                patch.object(settings, "returning_routing_enabled", True), \
                patch.object(settings, "wall_delivery_enabled", True), \
                patch.object(settings, "route_preview_enabled", True), \
                patch.object(settings, "route_code_keys", _KEYS), \
                patch.object(settings, "route_code_active_kid", _ACTIVE_KID), \
                patch.object(settings, "returning_uid_ttl_seconds", 1000):
            return await router._route_via_campaign(
                r, campaign, str(_CAMPAIGN), req, {}, result_label="matched",
            )

    return asyncio.run(_inner())


def _served(result) -> str:
    """The target actually served, read from where it LIVES at this frame —
    `result["target_id"]` is None here and would silently compare equal to
    nothing at all."""
    return str(((result or {}).get("attribution") or {}).get("offer_target_id"))


class TestT1DrainingIsServedToAReturningVisitor:
    """The positive direction FIRST. Without it every refusal below is satisfied
    by a wall that serves nobody."""

    def test_a_returning_visitor_gets_the_draining_first_tile(self):
        result = _run(disable_returning=False, avails={TILE_1: "draining"})
        assert result is not None, "the wall served nobody at all"
        assert _served(result) == str(TILE_1)

    def test_a_NEW_visitor_in_the_SAME_fixture_is_walked_past_it(self):
        """The control. Same wall, same tiles, same availabilities — only the
        visitor class differs, so the floor is what moved and nothing else."""
        result = _run(disable_returning=False, avails={TILE_1: "draining"},
                      seen_before=False)
        assert result is not None
        assert _served(result) == str(TILE_2)


class TestT2SuppressingReturningRemovesTheCLASS:
    """🔴 §3.3a — the finding this file was written for.

    `disable_returning_flows` is documented as suppressing the returning
    PARTITION. It also, through `_audience_routing`, removes the returning
    VISITOR CLASS — so the availability floor collapses to {"active"} for
    everyone and a `draining` tile becomes unreachable even for a visitor we
    have seen before. One flag, two effects, and only one of them is named
    where an operator would look.
    """

    def test_a_RETURNING_visitor_no_longer_gets_the_draining_tile(self):
        result = _run(disable_returning=True, avails={TILE_1: "draining"})
        assert result is not None
        assert _served(result) != str(TILE_1), (
            "draining was served although the returning class was suppressed"
        )
        assert _served(result) == str(TILE_2)

    def test_the_SAME_fixture_with_returning_ENABLED_does_serve_it(self):
        """The control that separates 'the flag did it' from 'this fixture
        cannot serve a draining tile at all'."""
        result = _run(disable_returning=False, avails={TILE_1: "draining"})
        assert _served(result) == str(TILE_1)


class TestT3EveryTileDrainingDivertsTheWholeCampaign:
    """The operational shape: not a dead end, a silent 100% diversion.

    🔴 THE FIRST VERSION OF THIS CLASS WAS WEAK AND A MUTATION CAUGHT IT. It
    asserted only `_served(...) not in (TILE_1, TILE_2)` — which is TRUE both
    when the campaign diverts and when the wall is simply broken, so it stayed
    green under a mutation that should have felled it. `_served` returns the
    string `"None"` in the empty case, and `"None" not in (...)` is satisfied by
    anything at all.

    The repair is not a sharper assertion on the same run — it is a SECOND run
    that differs in exactly one input. Measured on unmutated code:

        all tiles draining, returning OFF  ->  served: None
        all tiles draining, returning ON   ->  served: 111

    Same wall, same tiles, same availabilities. Only the class suppression
    moved, so it is the cause.
    """

    def test_no_winner_when_every_tile_is_draining_and_returning_is_off(self):
        result = _run(disable_returning=True,
                      avails={TILE_1: "draining", TILE_2: "draining"})
        assert _served(result) not in (str(TILE_1), str(TILE_2))

    def test_the_SAME_all_draining_wall_DOES_serve_when_the_class_exists(self):
        """🔴 The discriminating control — the one the first version lacked.

        Without it, "nothing was served" is equally explained by a wall that
        cannot serve a draining tile under any circumstances, which would make
        the test above a statement about the fixture rather than about the flag.
        """
        result = _run(disable_returning=False,
                      avails={TILE_1: "draining", TILE_2: "draining"})
        assert result is not None
        assert _served(result) == str(TILE_1)

    def test_ONE_active_tile_is_enough_to_keep_serving(self):
        """The second control: the wall still works in this fixture at all."""
        result = _run(disable_returning=True, avails={TILE_1: "draining"})
        assert result is not None
        assert _served(result) == str(TILE_2)


class TestT4ATileLinkToAnUnavailableTarget:
    """Row 2 of the §3.2 matrix, on a DEDICATED wall campaign.

    The existing coverage of row 2 asserts the visitor reaches `_NORMAL_TARGET`
    — but that fixture keeps an ORDINARY route as the fallback. On a wall
    campaign the fallback is the wall's own next eligible tile, and that
    transition is what had never been exercised.
    """

    def test_a_tile_code_naming_a_CLOSED_target_does_not_win(self):
        result = _run(disable_returning=True, avails={TILE_1: "closed"},
                      code=_sign_wall(target_id=TILE_1))
        assert result is not None
        assert _served(result) != str(TILE_1)
        # …and the visitor lands on the WALL's own default, not nowhere.
        assert _served(result) == str(TILE_2)

    def test_a_tile_code_naming_a_DRAINING_target_does_not_win_either(self):
        result = _run(disable_returning=True, avails={TILE_1: "draining"},
                      code=_sign_wall(target_id=TILE_1))
        assert result is not None
        assert _served(result) != str(TILE_1)
        assert _served(result) == str(TILE_2)

    def test_the_SAME_code_on_an_ACTIVE_target_DOES_win(self):
        """The control. Without it the two refusals above are equally explained
        by a code that is never honoured in this fixture."""
        result = _run(disable_returning=True, avails={},
                      code=_sign_wall(target_id=TILE_2))
        assert result is not None
        assert _served(result) == str(TILE_2)
