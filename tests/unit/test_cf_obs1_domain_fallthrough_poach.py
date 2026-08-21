"""CF-OBS-1 regression (crash-test 2026-06-07): a domain/?c=-bound campaign that
is a dead end (no flow + no legacy offer) must serve its OWN terminal_fallback
when one is configured — NOT fall through to global geo and poach a FOREIGN
campaign's offer + attribution.

Before the fix, `_route_via_campaign(..., fall_through_on_no_route=True)` returned
None on a dead-end bound campaign, the router fell through to geo targeting, and a
DIFFERENT campaign served the click + re-attributed `campaign_id` — the bound
campaign's own `fallback_url` was never consulted. The geo branch already serves
the bound campaign's own terminal_fallback; only the domain fall-through path
poached. The fix mirrors the geo branch: a configured `fallback_url` gates the
poach off; a campaign with NO fallback_url still falls through (the legitimate
bare-domain geo catch-all is preserved — no regression).

These drive the REAL `route()` end-to-end (the only way to exercise the domain →
geo fall-through) with a shared fakeredis backing routing + identity, mirroring
`test_la_f1_domain_fallthrough_identity.py`.
"""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import fakeredis.aioredis
import pytest

from app import identity, router
from app.models import ClickRequest

pytestmark = pytest.mark.asyncio

# campFT = domain-matched campaign that routes nowhere (dead end).
# campGeo = the geo winner that WOULD poach if campFT falls through.
CAMP_FT = "41"
CAMP_GEO = "42"
GEO_OFFER = "55"
HOST = "rtest.test"        # 2-label → non-wildcard host
PARAM_C = "obs-ft"         # domain binding selector (?c=obs-ft)
CO = 1
OWN_FALLBACK = "https://own41.terminal/{click_id}"


async def _seed(fake, *, ft_fallback_url: str | None) -> None:
    """campFT domain-binds but routes nowhere; campGeo is the geo poach target."""
    await fake.set(f"domain:{HOST}:param:{PARAM_C}", CAMP_FT)
    ft_map = {"company_id": str(CO), "priority": "0"}
    if ft_fallback_url is not None:
        ft_map["fallback_url"] = ft_fallback_url
    # campFT — NO flows / NO offers / NO split → dead end → fall through.
    await fake.hset(f"campaign:{CAMP_FT}", mapping=ft_map)
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
        mapping={"url": "https://foreign42.poach/{click_id}", "has_targets": "0"},
    )


def _click(click_id: str) -> ClickRequest:
    return ClickRequest(
        click_id=click_id,
        country="US",
        user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_2)",
        hostname=HOST,
        query_params={"c": PARAM_C, "source": "obs-src"},
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


class TestCFObs1DomainFallthroughPoach:
    async def test_dead_end_with_fallback_serves_own_terminal_no_poach(self):
        """campFT dead-ends but declares its OWN fallback_url → serve THAT, keep
        attribution on campFT, never poach campGeo."""
        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        await _seed(fake, ft_fallback_url=OWN_FALLBACK)

        result = await _route(fake, _click("obs-own-1"))

        assert result is not None
        # Attribution stays on the BOUND campaign — no foreign poach.
        assert result["campaign_id"] == CAMP_FT
        assert result["non_routed"] is True
        # Its OWN terminal_fallback served (macro-resolved), NOT the geo offer.
        assert result["fallback_url"] == "https://own41.terminal/obs-own-1"
        # Did NOT fall through to geo.
        # STRONGER than checking the fall-through flag: `geo_lookup_ms` is
        # only stamped by Stage 2, so its ABSENCE proves the global
        # `campaigns:active` set was never even read. (`domain_fallthrough`
        # is still stamped upstream and stays honest: it marks that the
        # domain path was exhausted — what changed is what follows it.)
        assert "geo_lookup_ms" not in result["timing"]

    async def test_dead_end_no_fallback_is_refused_not_poached(self):
        """A2 (tenant isolation, 2026-08-21) — INVERTED, deliberately.

        This test used to assert that a bound campaign with NO fallback_url
        "still falls through to global geo (the legitimate bare-domain
        catch-all)", and asserted `"foreign42.poach" in result["url"]` — i.e.
        it CODIFIED a foreign campaign serving the click as correct.

        That catch-all is the cross-tenant leak. `campaigns:active` is global,
        so the "catch" can be any tenant's campaign; proven live on staging
        (a click on company 1's domain served by company 38's campaign 304).
        The honest disposition for domain traffic that resolves no route is a
        refusal, so the corridor is now closed at its mouth.

        The ORIGINAL intent of the test is preserved and strengthened: campFT
        must never serve campGeo's offer. It now must not serve anything.
        """
        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        await _seed(fake, ft_fallback_url=None)

        result = await _route(fake, _click("obs-bare-1"))

        assert result is not None
        # Refused at the mouth of the corridor — no geo winner, no URL.
        assert result["blocked"] is True
        assert result["url"] is None
        assert result["timing"]["result"] == "blocked_no_route"
        # The foreign campaign was NOT reached — the point of the original test.
        assert result["campaign_id"] != CAMP_GEO
        # And geo was never consulted, so no fall-through was recorded.
        # STRONGER than checking the fall-through flag: `geo_lookup_ms` is
        # only stamped by Stage 2, so its ABSENCE proves the global
        # `campaigns:active` set was never even read. (`domain_fallthrough`
        # is still stamped upstream and stays honest: it marks that the
        # domain path was exhausted — what changed is what follows it.)
        assert "geo_lookup_ms" not in result["timing"]
