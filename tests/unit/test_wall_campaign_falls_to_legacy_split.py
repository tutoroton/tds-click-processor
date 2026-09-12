"""What a WALL campaign serves when its wall matches nobody.

🔴 THIS FILE PINS BEHAVIOUR THAT HAS NEVER BEEN RULED ON. It is deliberately
written as a MEASUREMENT, not as an assertion of what ought to happen: the
owner has not decided what a wall campaign should do when its wall is not
admissible for a visitor, and inventing that answer inside a test would make a
ruling by accident.

THE CHAIN, each link read in the code on 2026-09-12:

  cascade.py:578   on `flow_family == "offerwall"` the admissible set is
                   frozenset({"returning", "offerwall"}) — an ordinary `first`
                   flow is excluded CATEGORICALLY, by family, and lands in
                   `rejected_sink`. The code's own comment names this case.
  router.py:1352   "Stage 7 — legacy fallback (no flow matched)" ->
                   `select_offer(r, campaign_id, rng)`
  splits.py:55     ORDINARY_ROUTING_AUDIENCES = ("first", "returning") — so the
                   `split:{campaign_id}` aggregate CONTAINS the offers of
                   exactly the flows the cascade just excluded, campaign-wide
                   and weighted, with NO criteria attached.
  config.py:900    `wall_delivery_enabled: bool = True` — shipped default.

So a visitor who would NOT have matched that first flow's criteria can still be
served its offer, because the split carries offers and not rules.

🔴 AND THE CONTROL IS THE POINT OF THIS FILE, because without it the finding is
overstated. `TestTheSameHappensOnAStandardCampaign` runs the identical fixture
with NO `flow_family`, where the first flow IS evaluated and simply fails its
criteria. If both arms serve the split offer, then the criteria-free fallback is
PRE-EXISTING and by design — and what the wall changes is not the mechanism but
its FREQUENCY and its CAUSE:

    standard campaign — the fallback is a last resort reached when rules did
                        not match, which is what a fallback is for
    wall campaign     — flows are excluded by FAMILY before any rule is read,
                        so the fallback can become the DEFAULT path for the
                        whole campaign, up to 100% of its traffic

That narrower claim is the one worth putting in front of the owner, and it is
the one this file establishes. A test that asserted "the wall introduced a
criteria bypass" would be asserting something the control refutes.

NOT CHECKED here: the zero-tile door (`action_config: {"tiles": []}`), which
reaches the same Stage 7 by a different route, and the authoring side — nothing
currently refuses `flow_family=offerwall` on a campaign with no active wall.
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
    _CAMPAIGN,
    _COMPANY,
    _OFFER,
    FakeIdentRedis,
)
from tests.unit.test_wall_tile_membership import WALL_ID

FIRST_FLOW = 4242
SPLIT_OFFER = 7777
TILE_1 = 111
_UID = "U"

#: The visitor is in the US; every non-matching rule below keys on RU, so a
#: served offer can only have arrived WITHOUT its rule being satisfied.
VISITOR_COUNTRY = "US"
UNMATCHABLE = [{"type": "geo", "op": "in", "values": ["RU"]}]


async def _seed(r, *, wall_family: bool) -> dict:
    """One campaign, one wall that cannot match, one excluded `first` flow.

    `wall_family` is the ONLY difference between the two arms — everything else
    is byte-identical, so any divergence in the result is attributable to it and
    to nothing else.
    """
    campaign_hash = {
        "company_id": str(_COMPANY),
        "flow_mode": "global",
        "returning_mode": "fresh",
        "returning_routing": "0",
    }
    if wall_family:
        campaign_hash["flow_family"] = "offerwall"
    await r.hset("campaign:%d" % _CAMPAIGN, mapping=campaign_hash)

    # The wall. Its criterion cannot match this visitor, so on the wall arm the
    # admissible pool is present but empty-handed.
    await r.hset("flow:%d" % WALL_ID, mapping={
        "campaign_id": str(_CAMPAIGN), "company_id": str(_COMPANY),
        "scope_type": "company", "scope_id": str(_COMPANY),
        "audience": "offerwall", "action_type": "offerwall",
        "criteria": json.dumps(UNMATCHABLE), "seq_id": "1", "is_default": "0",
        "action_config": json.dumps({"tiles": [
            {"offer_id": _OFFER, "target_id": TILE_1},
        ]}),
    })
    await r.rpush("campaign:%d:walls" % _CAMPAIGN, str(WALL_ID))
    await r.hset("offer_target:%d" % TILE_1, mapping={
        "url": "https://land/tile", "availability": "active",
        "is_default": "0", "offer_id": str(_OFFER),
        "criteria": "[]", "priority": "0",
    })

    # The ORDINARY `first` flow. On the wall arm it never reaches a rule check;
    # on the standard arm it is checked and fails. Its criterion is the same
    # unmatchable one in both, so the two arms differ ONLY by family.
    await r.hset("flow:%d" % FIRST_FLOW, mapping={
        "campaign_id": str(_CAMPAIGN), "company_id": str(_COMPANY),
        "scope_type": "company", "scope_id": str(_COMPANY),
        "audience": "first", "action_type": "offer",
        "criteria": json.dumps(UNMATCHABLE), "seq_id": "2", "is_default": "0",
        "action_config": json.dumps({"offer_id": SPLIT_OFFER}),
    })
    await r.rpush("campaign:%d:flows" % _CAMPAIGN, str(FIRST_FLOW))

    # The legacy split — what `sync/builders/splits.py` would have published for
    # this campaign, since its SQL selects audience IN ('first','returning')
    # regardless of the campaign's family.
    await r.hset("split:%d" % _CAMPAIGN, mapping={str(SPLIT_OFFER): "100"})
    await r.hset("offer:%d" % SPLIT_OFFER, mapping={
        "url": "https://land/legacy-split", "company_id": str(_COMPANY),
    })

    campaign = await r.hgetall("campaign:%d" % _CAMPAIGN)
    campaign["_id"] = str(_CAMPAIGN)
    return campaign


def _run(*, wall_family: bool):
    ident = FakeIdentRedis(strings={})

    async def _gir():
        return ident

    async def _stamp(**kw):
        return identity_mod.IdentityResult(
            uid=_UID, is_returning=False, seen_before=False,
            signal_tier="cookie",
        )

    async def _inner():
        r = fakeredis.aioredis.FakeRedis(decode_responses=True)
        campaign = await _seed(r, wall_family=wall_family)
        req = ClickRequest(
            click_id="wls" + "0" * 18, country=VISITOR_COUNTRY,
            user_agent="t/1.0", query_params={},
        )
        with patch.object(sticky_mod, "get_identity_redis", _gir), \
                patch.object(identity_mod, "resolve_and_stamp", _stamp), \
                patch.object(settings, "returning_resolver_enabled", True), \
                patch.object(settings, "wall_delivery_enabled", True):
            return await router._route_via_campaign(
                r, campaign, str(_CAMPAIGN), req, {}, result_label="matched",
            )

    return asyncio.run(_inner())


def _url(result) -> str:
    return str((result or {}).get("url") or "")


class TestWhatAWallCampaignACTUALLYServes:
    def test_it_reaches_the_legacy_split_and_serves_an_EXCLUDED_flows_offer(self):
        """MEASURED, not desired.

        The first flow's own rule says RU and the visitor is US. If its offer is
        served, it arrived through the split, where offers travel without rules.
        """
        res = _run(wall_family=True)
        assert "legacy-split" in _url(res), (
            "expected the wall campaign to fall through to the legacy split; "
            f"got {_url(res)!r} — if this changed, the ruling changed with it"
        )

    def test_the_tile_was_NOT_served_so_the_wall_really_did_not_match(self):
        """The precondition, asserted rather than assumed.

        Without this the test above could pass on a run where the wall matched
        and something else diverted the click — a green for the wrong reason.
        """
        assert "land/tile" not in _url(_run(wall_family=True))


class TestTheSameHappensOnAStandardCampaign:
    """🔴 THE CONTROL. It is what keeps the finding honest."""

    def test_a_standard_campaign_ALSO_serves_the_split_offer(self):
        """So the criteria-free fallback is PRE-EXISTING, not wall-introduced.

        Here the first flow IS evaluated by the cascade and simply fails its
        rule. Same destination, different reason — and that difference in REASON
        is the entire finding.
        """
        assert "legacy-split" in _url(_run(wall_family=False))

    def test_both_arms_agree_which_is_the_POINT(self):
        """Stated as its own assertion so nobody has to infer it.

        Equality here refutes "the wall introduced a criteria bypass" and
        leaves the narrower, true claim: the wall changes WHY the fallback is
        reached (family exclusion, before any rule) and therefore HOW OFTEN —
        a last resort becomes the default path for the whole campaign.
        """
        assert _url(_run(wall_family=True)) == _url(_run(wall_family=False))
