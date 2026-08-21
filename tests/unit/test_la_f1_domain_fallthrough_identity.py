"""LA-F1 regression (audit-2 HIGH, 2026-06-07): identity must resolve + stamp
EXACTLY ONCE per /decide — against the campaign that ACTUALLY SERVES the click.

The bug: on `domain_fallthrough` (a `?c=` domain-matched campaign that has no
usable route → `_route_via_campaign(..., fall_through_on_no_route=True)` returns
None → router falls through to geo targeting, which routes a DIFFERENT campaign),
`identity.resolve_and_stamp` ran ONCE PER `_route_via_campaign` call → twice.
The FIRST (non-serving) call minted the uid AND wrote the fallen-through campaign
into the uid's campaigns-seen set as a side effect — so a brand-new visitor was
mis-classified `is_unique=False / is_roaming=True` and campaigns-seen was
permanently poisoned.

These tests drive the REAL `route()` end-to-end (the only way to reproduce the
double `_route_via_campaign` call) with a shared fakeredis backing BOTH the
routing keyspace and the identity store (mirrors local dev, where identity reuses
the routing Redis). The resolver is turned ON (env + per-company campaign hash).
"""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import fakeredis.aioredis
import pytest

from app import identity, router
from app.config import settings
from app.identity import _campaigns_key, _sig_key
from app.models import ClickRequest

pytestmark = pytest.mark.asyncio

# campFT = domain-matched campaign with NO route (falls through to geo).
# campGeo = the geo winner that actually serves (legacy split).
CAMP_FT = "86"
CAMP_GEO = "50"
OFFER = "55"
HOST = "geo.test"          # 2-label → non-wildcard host
PARAM_C = "la-ft"          # domain binding selector (?c=la-ft)
CO = 1


async def _seed(fake) -> None:
    """Routing keyspace: campFT domain-binds but routes nowhere; campGeo serves."""
    # Domain binding: ?c=la-ft on geo.test → campFT (86).
    await fake.set(f"domain:{HOST}:param:{PARAM_C}", CAMP_FT)
    # campFT — resolver opted-in, but NO flows / NO offers / NO split → fall through.
    await fake.hset(
        f"campaign:{CAMP_FT}",
        mapping={"company_id": str(CO), "priority": "0", "returning_resolver": "1"},
    )
    # campGeo — geo-eligible (no targeting flags ⇒ matches any), legacy split offer.
    await fake.sadd("campaigns:active", CAMP_GEO)
    await fake.sadd("geo:US", CAMP_GEO)
    await fake.hset(
        f"campaign:{CAMP_GEO}",
        mapping={"company_id": str(CO), "priority": "0", "returning_resolver": "1"},
    )
    await fake.sadd(f"campaign:{CAMP_GEO}:offers", OFFER)
    await fake.hset(
        f"offer:{OFFER}",
        mapping={"url": "https://geo.win/{click_id}", "has_targets": "0"},
    )


def _click(vid: str, click_id: str) -> ClickRequest:
    return ClickRequest(
        click_id=click_id,
        country="US",
        user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_2)",
        hostname=HOST,
        visitor_id=vid,
        query_params={"c": PARAM_C, "source": "la-src-plain", "sub1": "ft2"},
    )


async def _route(fake, req: ClickRequest):
    """Run route() with routing AND identity Redis both pointed at `fake`, then
    flush the fire-and-forget persist/commit tasks so the seen-set is durable."""
    async def _aget():
        return fake

    with patch.object(router, "get_redis", _aget), \
            patch.object(identity, "get_identity_redis", _aget):
        result = await router.route(req)
    # Let the deferred persist (create_task) land before asserting on Redis state.
    for _ in range(5):
        await asyncio.sleep(0)
    return result


@pytest.fixture
def resolver_on(monkeypatch):
    monkeypatch.setattr(settings, "returning_resolver_enabled", True)
    monkeypatch.setattr(settings, "returning_routing_enabled", False)


class TestLAF1DomainFallthrough:
    async def test_refused_click_mints_no_identity_at_all(self, resolver_on):
        """A2 (tenant isolation, 2026-08-21) — INVERTED, and the guarantee it
        carries is now STRICTLY STRONGER than before.

        This test used to assert that when campFT fell through, the SERVING geo
        campaign stamped identity and campaigns-seen held only that geo
        campaign. LA-F1's point was: a campaign that did not serve must never
        write identity.

        The corridor is closed, so there is no geo winner to serve. LA-F1's
        principle therefore reaches its limit case: when NOTHING serves the
        click, NOTHING may be written. A refused click must leave the identity
        store byte-identical — no uid minted, no signal map, no campaigns-seen
        set. Otherwise every refusal would silently poison the identity of a
        visitor we did not serve, which is the LA-F1 bug in a new costume.
        """
        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        await _seed(fake)

        result = await _route(fake, _click("la-vid-ft2", "la-ft-2"))

        # Refused at the mouth of the corridor — the geo campaign never served.
        assert result is not None
        assert result["blocked"] is True
        assert result["url"] is None
        assert result["campaign_id"] != CAMP_GEO
        assert result["timing"]["result"] == "blocked_no_route"
        # STRONGER than checking the fall-through flag: `geo_lookup_ms` is
        # only stamped by Stage 2, so its ABSENCE proves the global
        # `campaigns:active` set was never even read. (`domain_fallthrough`
        # is still stamped upstream and stays honest: it marks that the
        # domain path was exhausted — what changed is what follows it.)
        assert "geo_lookup_ms" not in result["timing"]

        # NOTHING was committed to the identity store — the deferred resolve is
        # only committed for a campaign that actually serves.
        assert await fake.get(_sig_key(CO, "vid", "la-vid-ft2")) is None
        leaked = [
            k async for k in fake.scan_iter(match=f"id:{CO}:uid:*:campaigns")
        ]
        assert leaked == []

    async def test_repeated_refusals_never_accumulate_identity(self, resolver_on):
        """The same visitor refused twice still leaves no identity behind.

        The original test proved recognition survived the LA-F1 fix (second
        click = returning). With the corridor closed there is no serving
        campaign to be returning TO, so the meaningful property becomes:
        refusals do not accumulate state. A refusal that minted on the second
        attempt would be just as poisonous as one that minted on the first,
        and would be far harder to notice.
        """
        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        await _seed(fake)

        first = await _route(fake, _click("la-vid-ft2", "la-ft-2"))
        second = await _route(fake, _click("la-vid-ft2", "la-ft-3"))

        for res in (first, second):
            assert res["blocked"] is True
            assert res["timing"]["result"] == "blocked_no_route"

        assert await fake.get(_sig_key(CO, "vid", "la-vid-ft2")) is None
        leaked = [
            k async for k in fake.scan_iter(match=f"id:{CO}:uid:*:campaigns")
        ]
        assert leaked == []
