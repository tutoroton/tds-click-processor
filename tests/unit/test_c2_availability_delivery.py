"""v2 C2 + F-MACRO-1 regression suite — availability honoured at the DELIVERY
boundary (not just the cascade pre-floor), and campaign.fallback_url macro-
resolved on the terminal_fallback path.

C2: cascade excluded a flow on availability OR a matched flow's delivery had
every candidate target/leg excluded → route to terminal_fallback, NEVER
re-serve the drained/closed target via legacy split (the bug).
F-MACRO-1: the served campaign.fallback_url is run through build_url/
safe_substitute → no literal `{macro}` ever leaks.

PRIME DIRECTIVE (byte-identical): with every target `active` (prod dark) OR a
target HASH missing the `availability` field (pre-migration-076), the new floor
excludes nothing → delivery picks exactly as before. Asserted explicitly below.
"""
from __future__ import annotations

import json

import fakeredis.aioredis
import pytest

from app import router
from app.action_executor import execute_action, UNAVAILABLE_RESULT
from app.config import settings
from app.models import ClickRequest

pytestmark = pytest.mark.asyncio

ACTIVE = frozenset({"active"})
RETURNING = frozenset({"active", "draining"})


def _click(**kw) -> ClickRequest:
    base = dict(click_id="c2" + "0" * 20, country="US", user_agent="t/1.0")
    base.update(kw)
    return ClickRequest(**base)


def _bu():
    """Stub build_url_fn mirroring router.build_url signature — emits a marker
    carrying the resolved target_id so tests can assert which target was served."""
    def fn(template, req, campaign_id, offer_id, *, source_mappings,
           campaign_mappings, target_id=None, flow_id=None):
        return f"URL[{template}|oid={offer_id}|tid={target_id}]"
    return fn


async def _r():
    return fakeredis.aioredis.FakeRedis(decode_responses=True)


async def _seed_target(r, tid, *, url, availability="active", is_default="0",
                       offer_id="1", criteria="[]", extra=None):
    m = {"url": url, "availability": availability, "is_default": is_default,
         "offer_id": offer_id, "criteria": criteria, "priority": "0"}
    if extra:
        m.update(extra)
    await r.hset(f"offer_target:{tid}", mapping=m)


def _split_flow(legs):
    return {"_id": "5", "action_type": "split",
            "action_config": json.dumps({"offers": legs})}


def _offer_flow(offer_id, target_id):
    return {"_id": "5", "action_type": "offer",
            "action_config": json.dumps({"offer_id": offer_id, "target_id": target_id})}


async def _exec(r, flow, allowed_avail):
    return await execute_action(
        r, flow, _click(), "1",
        source_mappings=None, campaign_mappings=None,
        build_url_fn=_bu(), allowed_avail=allowed_avail,
    )


# ============================================================
# 1 — split per-leg availability (the documented-but-unimplemented contract)
# ============================================================

class TestSplitPerLeg:
    async def test_closed_leg_excluded_new_visitor(self):
        r = await _r()
        await _seed_target(r, 10, url="https://a", availability="closed")
        await _seed_target(r, 11, url="https://b", availability="active")
        flow = _split_flow([{"offer_id": 1, "target_id": 10, "weight": 50},
                            {"offer_id": 1, "target_id": 11, "weight": 50}])
        # 200 picks — the closed leg (10) must NEVER be chosen.
        for _ in range(200):
            res = await _exec(r, flow, ACTIVE)
            assert res and res["target_id"] == "11"

    async def test_all_legs_closed_returns_unavailable(self):
        r = await _r()
        await _seed_target(r, 10, url="https://a", availability="closed")
        await _seed_target(r, 11, url="https://b", availability="draining")  # draining ∉ new
        flow = _split_flow([{"offer_id": 1, "target_id": 10, "weight": 50},
                            {"offer_id": 1, "target_id": 11, "weight": 50}])
        res = await _exec(r, flow, ACTIVE)
        assert res is UNAVAILABLE_RESULT

    async def test_returning_visitor_allows_draining_leg(self):
        r = await _r()
        await _seed_target(r, 10, url="https://a", availability="closed")
        await _seed_target(r, 11, url="https://b", availability="draining")
        flow = _split_flow([{"offer_id": 1, "target_id": 10, "weight": 50},
                            {"offer_id": 1, "target_id": 11, "weight": 50}])
        res = await _exec(r, flow, RETURNING)
        assert res and res["target_id"] == "11"


# ============================================================
# 2 — offer pinned + offer-default availability
# ============================================================

class TestOfferAvailability:
    async def _offer_with_default(self, r, *, pin_avail, def_avail):
        await _seed_target(r, 10, url="https://pin", availability=pin_avail, offer_id="7")
        await _seed_target(r, 20, url="https://def", availability=def_avail,
                           is_default="1", offer_id="7")
        await r.hset("offer:7", mapping={"has_targets": "1"})
        await r.sadd("offer:7:targets", "10", "20")

    async def test_pinned_closed_falls_to_available_default(self):
        r = await _r()
        await self._offer_with_default(r, pin_avail="closed", def_avail="active")
        res = await _exec(r, _offer_flow(7, 10), ACTIVE)
        assert res and res["target_id"] == "20"
        assert res["target_selection_path"] == "offer_default"

    async def test_pinned_and_default_closed_unavailable(self):
        r = await _r()
        await self._offer_with_default(r, pin_avail="closed", def_avail="closed")
        res = await _exec(r, _offer_flow(7, 10), ACTIVE)
        assert res is UNAVAILABLE_RESULT

    async def test_pinned_active_served_directly(self):
        r = await _r()
        await self._offer_with_default(r, pin_avail="active", def_avail="active")
        res = await _exec(r, _offer_flow(7, 10), ACTIVE)
        assert res and res["target_id"] == "10"
        assert res["target_selection_path"] == "pinned"


# ============================================================
# 3 — legacy resolve_target availability filter (Stage 8)
# ============================================================

class TestLegacyResolveTarget:
    async def _offer(self, r, avail):
        await _seed_target(r, 10, url="https://x", availability=avail,
                           is_default="1", offer_id="7", criteria="[]")
        await r.sadd("offer:7:targets", "10")
        return {"has_targets": "1", "_id": "7"}

    async def test_closed_default_excluded(self):
        r = await _r()
        offer = await self._offer(r, "closed")
        assert await router.resolve_target(r, offer, _click(), ACTIVE) is None

    async def test_draining_served_to_returning(self):
        r = await _r()
        offer = await self._offer(r, "draining")
        assert await router.resolve_target(r, offer, _click(), RETURNING) == "https://x"

    async def test_active_served(self):
        r = await _r()
        offer = await self._offer(r, "active")
        assert await router.resolve_target(r, offer, _click(), ACTIVE) == "https://x"


# ============================================================
# 4 — BYTE-IDENTICAL (prime directive)
# ============================================================

class TestByteIdentical:
    async def test_all_active_split_pick_unchanged(self):
        # weight 100/0 → leg 10 always; availability floor must not perturb it.
        r = await _r()
        await _seed_target(r, 10, url="https://a", availability="active")
        await _seed_target(r, 11, url="https://b", availability="active")
        flow = _split_flow([{"offer_id": 1, "target_id": 10, "weight": 100},
                            {"offer_id": 1, "target_id": 11, "weight": 0}])
        for _ in range(50):
            res = await _exec(r, flow, ACTIVE)
            assert res and res["target_id"] == "10"

    async def test_missing_availability_field_treated_active(self):
        # pre-migration-076 target HASH (no `availability`) → fail-open active.
        r = await _r()
        await r.hset("offer_target:10", mapping={"url": "https://a", "offer_id": "1"})
        flow = _split_flow([{"offer_id": 1, "target_id": 10, "weight": 100}])
        res = await _exec(r, flow, ACTIVE)
        assert res and res["target_id"] == "10"

    async def test_offer_no_availability_field_served(self):
        r = await _r()
        await r.hset("offer_target:10",
                     mapping={"url": "https://a", "offer_id": "7", "is_default": "1"})
        await r.hset("offer:7", mapping={"has_targets": "1"})
        await r.sadd("offer:7:targets", "10")
        res = await _exec(r, _offer_flow(7, 10), ACTIVE)
        assert res and res["target_id"] == "10"


# ============================================================
# 5 — _allowed_availability rule (the single shared gate)
# ============================================================

class TestAllowedAvailabilityRule:
    def test_new_visitor_active_only(self):
        # no uid → not seen_before → {active}
        assert router._allowed_availability(
            {"returning_routing": "1", "returning_mode": "override"},
            {"uid": "", "is_unique": True},
        ) == ACTIVE

    def test_routing_off_active_only(self, monkeypatch):
        monkeypatch.setattr(settings, "returning_routing_enabled", False)
        # even a seen_before returning visitor → {active} when routing OFF
        assert router._allowed_availability(
            {"returning_routing": "1", "returning_mode": "override"},
            {"uid": "u1", "is_unique": False},
        ) == ACTIVE

    def test_returning_visitor_routing_on_active_draining(self, monkeypatch):
        monkeypatch.setattr(settings, "returning_routing_enabled", True)
        assert router._allowed_availability(
            {"returning_routing": "1", "returning_mode": "override"},
            {"uid": "u1", "is_unique": False},
        ) == RETURNING

    def test_mode_fresh_active_only(self, monkeypatch):
        monkeypatch.setattr(settings, "returning_routing_enabled", True)
        # mode fresh disables the partition ⇒ {active} even for a returning uid
        assert router._allowed_availability(
            {"returning_routing": "1", "returning_mode": "fresh"},
            {"uid": "u1", "is_unique": False},
        ) == ACTIVE


# ============================================================
# 6 — F-MACRO-1: campaign.fallback_url macro resolution
# ============================================================

class TestFallbackMacro:
    def test_template_macros_resolved(self):
        url = router._resolve_fallback_template(
            "https://fb.example/?cid={click_id}&camp={campaign_id}&uid={uid}&s1={sub1}",
            _click(click_id="abc123"), "49", None, None,
        )
        assert url is not None
        assert "{" not in url and "}" not in url      # no literal macro leaks
        assert "cid=abc123" in url and "camp=49" in url
        # {uid} (not a macro) + {sub1} (no value) collapse → dropped
        assert "uid=" not in url and "s1=" not in url

    def test_none_template_returns_none(self):
        assert router._resolve_fallback_template(None, _click(), "49", None, None) is None


# ============================================================
# 7 — ROUTER end-to-end: cascade-excluded → terminal_fallback (NOT legacy
#     re-serve) + macro-resolved campaign.fallback_url. The headline C2 repro.
# ============================================================

class TestRouterTerminalFallback:
    async def _seed_campaign(self, r, *, target_avail):
        # campaign 49 (global), one campaign-bound offer flow → a single pinned
        # target. fallback_url carries macros (F-MACRO-1).
        await r.hset("campaign:49", mapping={
            "company_id": "1", "flow_mode": "global", "returning_mode": "fresh",
            "fallback_url": "https://fb.example/?cid={click_id}&camp={campaign_id}&uid={uid}",
        })
        await r.rpush("campaign:49:flows", "319")
        await r.hset("flow:319", mapping={
            "campaign_id": "49", "company_id": "1", "scope_type": "company",
            "scope_id": "1", "audience": "first", "action_type": "offer",
            "action_config": json.dumps({"offer_id": 7, "target_id": 10}),
            "criteria": "[]", "seq_id": "1", "is_default": "0",
        })
        await _seed_target(r, 10, url="https://land/x", availability=target_avail, offer_id="7")

    async def test_closed_target_routes_terminal_fallback_not_legacy(self):
        r = await _r()
        await self._seed_campaign(r, target_avail="closed")
        campaign = await r.hgetall("campaign:49")
        campaign["_id"] = "49"
        timing: dict = {}
        res = await router._route_via_campaign(
            r, campaign, "49", _click(click_id="r2fc2"), timing,
            result_label="matched",
        )
        # NOT a legacy_split re-serve of the closed target.
        assert timing.get("route_via") != "legacy_split"
        assert res is not None
        assert res.get("url") is None and res.get("non_routed") is True
        # terminal_fallback carries the MACRO-RESOLVED campaign.fallback_url.
        fb = res.get("fallback_url")
        assert fb and "{" not in fb and "}" not in fb
        assert "cid=r2fc2" in fb and "camp=49" in fb
        # availability_excluded recorded → main.py would label terminal_fallback.
        assert (res["attribution"].get("routing_trace") or {}).get("availability_excluded")

    async def test_active_target_serves_normally_byte_identical(self):
        # Same campaign but target active → normal flow_cascade serve (no fallback).
        r = await _r()
        await self._seed_campaign(r, target_avail="active")
        campaign = await r.hgetall("campaign:49")
        campaign["_id"] = "49"
        timing: dict = {}
        res = await router._route_via_campaign(
            r, campaign, "49", _click(click_id="r2fok"), timing,
            result_label="matched",
        )
        assert res is not None and res.get("url")
        # real build_url on a macro-less template → the target url verbatim.
        assert res["url"] == "https://land/x"     # served the pinned target
        assert res.get("non_routed") is not True
        assert timing.get("route_via") == "flow_cascade"
