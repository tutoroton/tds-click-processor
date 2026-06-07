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
    async def test_new_visitor_fallthrough_stamps_unique_not_poisoned(self, resolver_on):
        """Brand-new visitor whose domain campaign falls through to geo:
        the SERVING (geo) campaign stamps is_unique=True / is_returning=False /
        is_roaming=False, and campaigns-seen holds ONLY the geo campaign — the
        fallen-through campFT is NEVER minted nor written."""
        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        await _seed(fake)

        result = await _route(fake, _click("la-vid-ft2", "la-ft-2"))

        # The geo campaign served (legacy split), NOT campFT.
        assert result is not None
        assert result["campaign_id"] == CAMP_GEO
        assert result["timing"].get("domain_fallthrough") is True
        attr = result["attribution"]

        # Brand-new visitor — exactly one flag true (segment A).
        assert attr["is_unique"] is True
        assert attr["is_returning"] is False
        assert attr["is_roaming"] is False

        uid = attr["uid"]
        assert uid  # a uid was minted (for the SERVING campaign)

        # The signal map points at the served uid.
        assert await fake.get(_sig_key(CO, "vid", "la-vid-ft2")) == uid

        # campaigns-seen for the uid holds ONLY the SERVING (geo) campaign —
        # the fallen-through campFT (86) must NOT be present (the LA-F1 poison).
        seen = await fake.smembers(_campaigns_key(CO, uid))
        assert seen == {CAMP_GEO}
        assert CAMP_FT not in seen

        # No OTHER uid's campaigns-seen set exists (campFT never minted a uid).
        leaked = [
            k async for k in fake.scan_iter(match=f"id:{CO}:uid:*:campaigns")
        ]
        assert leaked == [_campaigns_key(CO, uid)]

    async def test_second_click_same_vid_is_returning(self, resolver_on):
        """The SAME visitor's second click on the SAME serving campaign is
        recognised as returning — proving the fix didn't break recognition."""
        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        await _seed(fake)

        first = await _route(fake, _click("la-vid-ft2", "la-ft-2"))
        uid1 = first["attribution"]["uid"]

        second = await _route(fake, _click("la-vid-ft2", "la-ft-3"))
        attr2 = second["attribution"]

        assert second["campaign_id"] == CAMP_GEO
        assert attr2["uid"] == uid1            # same canonical identity
        assert attr2["is_unique"] is False
        assert attr2["is_returning"] is True   # return to the SAME campaign (B)
        assert attr2["is_roaming"] is False
        # Still only the geo campaign in the seen-set (campFT never poisons it).
        assert await fake.smembers(_campaigns_key(CO, uid1)) == {CAMP_GEO}
