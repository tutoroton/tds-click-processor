"""R69 regression (2026-06-24): a domain binding that resolves to a campaign
whose `campaign:{id}` HASH is ABSENT must fail CLOSED at binding granularity —
NOT fall through to geo targeting and re-attribute the click to a FOREIGN active
campaign.

A binding key can momentarily outlive its campaign hash: an independent per-row
sync-builder skip (campaigns.py vs domains.py have separate try/except), a
partial/lagged snapshot apply on a node, or a stale-key-cleanup miss. Both
builders gate `c.status='active'` so this is rare in steady state, but reachable
— and today the `domain_campaign_id`-truthy + empty-hash branch sets
`domain_fallthrough=True` → geo broad-eval → foreign poach. The fix mirrors F9
(`domains:disabled`): an absent hash returns the block sentinel
(`{"url": None, "blocked": True}`), the Worker serves its controlled fallback
(NOT a 404 — memory `f9-disabled-router-fallback-not-404`), and the click is
recorded with `routing_result=blocked_dead_binding` (`decision_reason=domain_blocked`).

The present-but-no-route path (CF-OBS-1) is preserved byte-identical: a campaign
whose hash IS present but routes nowhere still falls through to geo (the
legitimate bare-domain catch-all). These drive the REAL `route()` end-to-end with
a shared fakeredis, mirroring `test_cf_obs1_domain_fallthrough_poach.py`.
"""

from __future__ import annotations

import asyncio
import json
from unittest.mock import patch

import fakeredis.aioredis
import pytest

from app import identity, router
from app.main import _decision_reason
from app.models import ClickRequest

pytestmark = pytest.mark.asyncio

# campDead = the campaign the binding points at; its HASH is intentionally NOT
# seeded (archived/deleted/builder-skip window).
# campGeo = the geo winner that WOULD poach if campDead fell through.
CAMP_DEAD = "92"
CAMP_GEO = "42"
GEO_OFFER = "55"
HOST = "r69.test"          # 2-label → non-wildcard host
PARAM_C = "dead-c"         # domain binding selector (?c=dead-c)
CO = 1
BIND_ID = 777
BIND_ALIAS = "deadbind"
FOREIGN_URL = "https://foreign42.poach/{click_id}"


async def _seed_dead_binding(fake) -> None:
    """A param binding (F.31 JSON shape) resolves to campDead, but campDead's
    hash is ABSENT. A geo-eligible foreign campGeo WOULD poach on fall-through."""
    await fake.set(
        f"domain:{HOST}:param:{PARAM_C}",
        json.dumps(
            {"campaign_id": CAMP_DEAD, "binding_id": BIND_ID, "binding_alias": BIND_ALIAS}
        ),
    )
    # NOTE: deliberately NO `campaign:{CAMP_DEAD}` hash — this is the R69 seam.
    # campGeo — geo-eligible (no targeting flags ⇒ matches any), legacy split.
    await fake.sadd("campaigns:active", CAMP_GEO)
    await fake.sadd("geo:US", CAMP_GEO)
    await fake.hset(
        f"campaign:{CAMP_GEO}",
        mapping={"company_id": str(CO), "priority": "0"},
    )
    await fake.sadd(f"campaign:{CAMP_GEO}:offers", GEO_OFFER)
    await fake.hset(
        f"offer:{GEO_OFFER}",
        mapping={"url": FOREIGN_URL, "has_targets": "0"},
    )


def _click(click_id: str) -> ClickRequest:
    return ClickRequest(
        click_id=click_id,
        country="US",
        user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_2)",
        hostname=HOST,
        query_params={"c": PARAM_C, "source": "r69-src"},
    )


async def _route(fake, req: ClickRequest):
    async def _aget():
        return fake

    with patch.object(router, "get_redis", _aget), \
            patch.object(identity, "get_identity_redis", _aget):
        result = await router.route(req)
    for _ in range(5):
        await asyncio.sleep(0)
    return result


class TestR69DeadBinding:
    async def test_domain_binding_absent_campaign_hash_fails_closed(self):
        """The binding → absent-hash campaign returns the block sentinel and does
        NOT set domain_fallthrough (fail closed, not geo)."""
        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        await _seed_dead_binding(fake)

        result = await _route(fake, _click("r69-block-1"))

        assert result is not None
        assert result["blocked"] is True
        assert result["url"] is None
        assert result["campaign_id"] is None  # OD-1 LOCKED — parity with §6
        assert result["timing"]["result"] == "blocked_dead_binding"
        # MUST NOT have fallen through to geo targeting.
        assert "domain_fallthrough" not in result["timing"]

    async def test_binding_dead_does_not_poach_foreign_geo_campaign(self):
        """The precise R69 anti-poach assertion: the foreign campaign's offer URL
        is NEVER present in the result."""
        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        await _seed_dead_binding(fake)

        result = await _route(fake, _click("r69-block-2"))

        assert result is not None
        assert result["blocked"] is True
        assert result["campaign_id"] != CAMP_GEO
        assert (result.get("url") or "").find("foreign42.poach") == -1

    async def test_F31_binding_metadata_carried_on_dead_binding_block(self):
        """Analytics keeps the binding the click ARRIVED through — binding_id /
        binding_alias come straight from the resolution."""
        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        await _seed_dead_binding(fake)

        result = await _route(fake, _click("r69-meta-1"))

        assert result is not None
        assert result["binding_id"] == BIND_ID
        assert result["binding_alias"] == BIND_ALIAS

    async def test_present_campaign_no_route_is_refused_with_its_own_reason(self):
        """A2 (tenant isolation, 2026-08-21) — INVERTED, and it still does the
        job it was written for.

        Its original purpose was to prove R69 did not OVER-block: a campaign
        whose hash is present but routes nowhere must not be mistaken for a
        dead binding. That discrimination is preserved and still asserted —
        the two refusals carry DIFFERENT raw reasons.

        What changed is the disposition of the second case. It used to fall
        through to geo and assert `"foreign42.poach" in result["url"]` — a
        foreign campaign serving a click that arrived on this host. Since the
        geo pool is global, that is the cross-tenant leak. Domain traffic with
        no usable route is now refused at the mouth of the corridor.
        """
        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        await _seed_dead_binding(fake)
        # Now ADD campDead's hash (present), with no flows/offers/split/fallback.
        await fake.hset(
            f"campaign:{CAMP_DEAD}",
            mapping={"company_id": str(CO), "priority": "0"},
        )

        result = await _route(fake, _click("r69-present-1"))

        assert result is not None
        assert result["blocked"] is True
        assert result["url"] is None
        # The foreign campaign never served — the leak this closes.
        assert result["campaign_id"] != CAMP_GEO
        # STRONGER than checking the fall-through flag: `geo_lookup_ms` is
        # only stamped by Stage 2, so its ABSENCE proves the global
        # `campaigns:active` set was never even read. (`domain_fallthrough`
        # is still stamped upstream and stays honest: it marks that the
        # domain path was exhausted — what changed is what follows it.)
        assert "geo_lookup_ms" not in result["timing"]
        # STILL DISCRIMINATING: present-but-no-route is NOT a dead binding.
        # Same refusal, different reason — R69's no-over-block guarantee.
        assert result["timing"]["result"] == "blocked_no_route"
        assert result["timing"]["result"] != "blocked_dead_binding"

    async def test_decision_reason_no_route_maps_to_domain_blocked(self):
        """A2 companion: the mouth-gate refusal tags as the closed
        `domain_blocked` analytics enum, exactly like the other domain-level
        edge blocks. Without this the click would be reported as
        `blocked_by_flow` — a false statement about which mechanism refused
        it, since no flow was ever consulted."""
        reason = _decision_reason(
            {"blocked": True},
            {"result": "blocked_no_route"},
            {},
        )
        assert reason == "domain_blocked"

    async def test_decision_reason_dead_binding_maps_to_domain_blocked(self):
        """main.py companion: a `blocked_dead_binding` result tags as the closed
        `domain_blocked` analytics enum (raw routing_result stays distinct)."""
        reason = _decision_reason(
            {"blocked": True},
            {"result": "blocked_dead_binding"},
            {},
        )
        assert reason == "domain_blocked"
