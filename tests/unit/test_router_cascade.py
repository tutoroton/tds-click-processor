"""End-to-end tests for `route()` with flow cascade — Vectors 2.4 + 2.5.

Mocks `app.redis_client.get_redis` to return a hand-built FakeRedis that
serves the routing keyspace populated by the sync builders. Covers the
two integration modes (cascade-hit vs legacy-fallback) and the new
`block` short-circuit. Exercising `route()` end-to-end here is the only
way to catch wiring bugs between cascade.py / action_executor.py /
router.py — pure unit tests on each module would miss them.

Each test crafts a minimal Redis snapshot (campaign + flows + offers +
optional buyer chain), invokes `route(req)`, and asserts on:
  - winner campaign / offer / target IDs,
  - the route_via tag (`flow_cascade` vs `legacy_split`),
  - the URL substituted at the end.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import patch

import pytest

from app import cascade, router
from app.models import ClickRequest


class FakeRedis:
    """Minimal Redis surface used by `route()` and downstream helpers.

    Backed by 4 in-memory dicts (string / hash / set / list keyspaces).
    Pipeline returns results in op order — same contract real
    `redis.asyncio.Redis.pipeline` honors. Exceptions are surfaced raw
    so test failures point at the call site, not a wrapper.
    """

    def __init__(
        self,
        strings: dict[str, str] | None = None,
        hashes: dict[str, dict] | None = None,
        sets: dict[str, set] | None = None,
        lists: dict[str, list] | None = None,
    ):
        self.strings = strings or {}
        self.hashes = hashes or {}
        self.sets = sets or {}
        self.lists = lists or {}

    async def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    async def smembers(self, key):
        return set(self.sets.get(key, set()))

    async def sismember(self, key, member):
        # Mirrors redis SISMEMBER (1/0); resolver wraps in bool().
        return 1 if member in self.sets.get(key, set()) else 0

    async def get(self, key):
        return self.strings.get(key)

    async def set(self, key, value, nx=False, ex=None):
        # v2 Phase S — sticky pin writes (SET NX EX / SET EX). ex is a no-op in
        # the fake (expire() is a no-op too).
        if nx and key in self.strings:
            return None
        self.strings[key] = value
        return True

    async def hget(self, key, field):
        # Single-field HASH read — used by `_effective_source_mappings`
        # to read `campaign:{cid}:source_overrides` (per-link override).
        return self.hashes.get(key, {}).get(field)

    async def incr(self, key):
        cur = int(self.strings.get(key, 0))
        self.strings[key] = str(cur + 1)
        return cur + 1

    async def expire(self, key, seconds):
        return True

    def pipeline(self):
        return _FakePipeline(self)


class _FakePipeline:
    def __init__(self, parent: FakeRedis):
        self.parent = parent
        self.ops: list[tuple] = []

    def hgetall(self, key):
        self.ops.append(("hgetall", key))

    def hget(self, key, field):
        # v2 Phase A — cascade availability pre-selection reads
        # `offer_target:{tid}` `availability` via a pipelined HGET.
        self.ops.append(("hget", key, field))

    def smembers(self, key):
        self.ops.append(("smembers", key))

    def get(self, key):
        self.ops.append(("get", key))

    def lrange(self, key, _start, _end):
        self.ops.append(("lrange", key))

    def exists(self, key):
        self.ops.append(("exists", key))

    def sismember(self, key, member):
        # F9 — the domains:disabled fail-closed check batches SISMEMBERs.
        self.ops.append(("sismember", key, member))

    def incr(self, key):
        self.ops.append(("incr", key))

    def expire(self, key, seconds):
        self.ops.append(("expire", key, seconds))

    async def execute(self):
        out = []
        for op in self.ops:
            kind = op[0]
            key = op[1]
            if kind == "hgetall":
                out.append(dict(self.parent.hashes.get(key, {})))
            elif kind == "hget":
                field = op[2]
                out.append(self.parent.hashes.get(key, {}).get(field))
            elif kind == "smembers":
                out.append(set(self.parent.sets.get(key, set())))
            elif kind == "get":
                out.append(self.parent.strings.get(key))
            elif kind == "lrange":
                out.append(list(self.parent.lists.get(key, [])))
            elif kind == "exists":
                exists = (
                    key in self.parent.hashes
                    or key in self.parent.strings
                    or key in self.parent.sets
                )
                out.append(1 if exists else 0)
            elif kind == "sismember":
                member = op[2]
                out.append(
                    1 if member in self.parent.sets.get(key, set()) else 0
                )
            elif kind == "incr":
                cur = int(self.parent.strings.get(key, 0))
                self.parent.strings[key] = str(cur + 1)
                out.append(cur + 1)
            elif kind == "expire":
                out.append(True)
        return out


def _click(query_params: dict[str, str] | None = None) -> ClickRequest:
    return ClickRequest(
        click_id="test-click-1",
        country="US",
        user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_2)",
        query_params=query_params or {},
    )


def _route_with(redis: FakeRedis, req: ClickRequest):
    """Patch `get_redis` and run `route()`.

    Uses `asyncio.run` (a fresh loop per call) to dodge the deprecated
    `get_event_loop()` path; pytest-asyncio's session-scoped loop is
    overkill for these self-contained synchronous helpers and asyncio.run
    raises if a loop is already running, which here it isn't.
    """
    async def _async_redis():
        return redis

    async def _runner():
        with patch.object(router, "get_redis", _async_redis):
            return await router.route(req)

    import asyncio
    return asyncio.run(_runner())


# ============================================================
# Cascade-hit path (the primary Stage 2 contract)
# ============================================================


class TestCascadeHit:
    def test_redirect_flow_wins(self):
        """Single redirect-action flow at company scope routes the click."""
        flow_id = "100"
        campaign_id = "5"
        redis = FakeRedis(
            sets={
                "geo:US": {campaign_id},
                "device:mobile": {campaign_id},
                "os:ios": {campaign_id},
                "campaigns:active": {campaign_id},
            },
            hashes={
                f"campaign:{campaign_id}": {
                    "company_id": "1",
                    "priority": "0",
                    "weight": "100",
                },
                f"flow:{flow_id}": {
                    "campaign_id": campaign_id,
                    "scope_type": "company",
                    "scope_id": "1",
                    "seq_id": "1",
                    "is_default": "0",
                    "criteria": "[]",
                    "action_type": "redirect",
                    "action_config": json.dumps({
                        "url": "https://lp.example.com/{click_id}",
                    }),
                },
            },
            lists={
                f"campaign:{campaign_id}:flows": [flow_id],
            },
        )

        result = _route_with(redis, _click())
        assert result is not None
        assert result["campaign_id"] == campaign_id
        assert result["url"].startswith("https://lp.example.com/test-click-1")
        assert result["timing"].get("route_via") == "flow_cascade"

    def test_offer_flow_with_pinned_target(self):
        """Offer-action flow loads its pinned target and uses its URL."""
        campaign_id = "10"
        flow_id = "200"
        offer_id = "55"
        target_id = "77"
        redis = FakeRedis(
            sets={
                "geo:US": {campaign_id},
                "device:mobile": {campaign_id},
                "os:ios": {campaign_id},
                "campaigns:active": {campaign_id},
            },
            hashes={
                f"campaign:{campaign_id}": {"company_id": "1", "priority": "0"},
                f"flow:{flow_id}": {
                    "campaign_id": campaign_id,
                    "scope_type": "company",
                    "scope_id": "1",
                    "seq_id": "1",
                    "is_default": "0",
                    "criteria": "[]",
                    "action_type": "offer",
                    "action_config": json.dumps({
                        "offer_id": int(offer_id),
                        "target_id": int(target_id),
                    }),
                },
                f"offer_target:{target_id}": {
                    "url": "https://target.example/path?cid={campaign_id}&oid={offer_id}",
                    "is_default": "0",
                },
            },
            lists={
                f"campaign:{campaign_id}:flows": [flow_id],
            },
        )

        result = _route_with(redis, _click())
        assert result is not None
        assert result["offer_id"] == offer_id
        assert "target.example" in result["url"]
        assert f"cid={campaign_id}" in result["url"]
        assert f"oid={offer_id}" in result["url"]

    def test_block_flow_returns_blocked_result(self):
        """`block` action yields `blocked=True` with no URL."""
        campaign_id = "10"
        flow_id = "300"
        redis = FakeRedis(
            sets={
                "geo:US": {campaign_id},
                "device:mobile": {campaign_id},
                "os:ios": {campaign_id},
                "campaigns:active": {campaign_id},
            },
            hashes={
                f"campaign:{campaign_id}": {"company_id": "1", "priority": "0"},
                f"flow:{flow_id}": {
                    "campaign_id": campaign_id,
                    "scope_type": "company",
                    "scope_id": "1",
                    "seq_id": "1",
                    "is_default": "0",
                    "criteria": "[]",
                    "action_type": "block",
                    "action_config": json.dumps({"code": 404}),
                },
            },
            lists={
                f"campaign:{campaign_id}:flows": [flow_id],
            },
        )
        result = _route_with(redis, _click())
        assert result is not None
        assert result["blocked"] is True
        assert result["url"] is None
        assert result["timing"]["result"] == "blocked_by_flow"

    def test_cross_tenant_buyer_id_drops_chain(self):
        """`?buyer_id=999` for a user in DIFFERENT tenant → chain dropped.

        Security audit 2026-04-28 HIGH-001 amplification: cascade must
        NOT walk a foreign tenant's `flows:scope:*` keyspace just
        because an attacker pointed `?buyer_id=` at someone else's
        user row. Defense is `_resolve_buyer_chain` company assertion.
        """
        campaign_id = "10"
        company_a_flow = "100"  # company A (campaign's tenant)
        company_b_flow = "200"  # company B (attacker target)
        redis = FakeRedis(
            sets={
                "geo:US": {campaign_id},
                "device:mobile": {campaign_id},
                "os:ios": {campaign_id},
                "campaigns:active": {campaign_id},
                f"campaign:{campaign_id}:sources": {"99"},
            },
            hashes={
                # Campaign belongs to company A.
                f"campaign:{campaign_id}": {"company_id": "1", "priority": "0"},
                "source:99": {
                    "slug": "fb",
                    "param_mappings": json.dumps([
                        {"slot": "buyer_id", "key": "buyer_id"},
                    ]),
                },
                # User 999 belongs to company B (NOT campaign's tenant).
                "user:999": {
                    "id": "999",
                    "team_id": "77",
                    "department_id": "33",
                    "custom_group_id": "",
                    "company_id": "2",  # ← different tenant
                    "status": "active",
                },
                # Company A flow at company-scope (legitimate route).
                f"flow:{company_a_flow}": {
                    "campaign_id": campaign_id,
                    "scope_type": "company",
                    "scope_id": "1",
                    "seq_id": "1",
                    "is_default": "0",
                    "criteria": "[]",
                    "action_type": "redirect",
                    "action_config": json.dumps({"url": "https://company-a"}),
                },
                # Company B's team-scoped flow — MUST NOT win even though
                # buyer_id=999 enrichment claims team=77 in company B.
                f"flow:{company_b_flow}": {
                    "campaign_id": "0",
                    "scope_type": "team",
                    "scope_id": "77",
                    "seq_id": "1",
                    "is_default": "0",
                    "criteria": "[]",
                    "action_type": "redirect",
                    "action_config": json.dumps({"url": "https://leaked-from-b"}),
                },
            },
            lists={
                f"campaign:{campaign_id}:flows": [company_a_flow],
                # If chain were not dropped, the cascade would query
                # `flows:scope:2:team:77` and pull company B's flow.
                "flows:scope:2:team:77": [company_b_flow],
                "flows:scope:1:company:1": [],
            },
        )

        result = _route_with(redis, _click({"buyer_id": "999", "source": "fb"}))
        assert result is not None
        assert "company-a" in result["url"]
        assert "leaked-from-b" not in result["url"]

    def test_is_default_company_flow_is_final_catchall(self):
        """No flow matches at any level → `is_default` company flow wins.

        Per `SCOPE-CASCADE.md` step 5: "If no flow matches, walk OUT
        one scope level... `is_default=true` flow is the final catch-all."
        """
        campaign_id = "10"
        explicit_flow = "100"  # buyer-scoped, geo=RU only
        default_flow = "200"   # company-scoped, is_default=true
        redis = FakeRedis(
            sets={
                "geo:US": {campaign_id},
                "device:mobile": {campaign_id},
                "os:ios": {campaign_id},
                "campaigns:active": {campaign_id},
            },
            hashes={
                f"campaign:{campaign_id}": {"company_id": "1", "priority": "0"},
                f"flow:{explicit_flow}": {
                    "campaign_id": campaign_id,
                    "scope_type": "company",
                    "scope_id": "1",
                    "seq_id": "1",
                    "is_default": "0",
                    "criteria": json.dumps([
                        {"type": "geo", "op": "in", "values": ["RU"]},
                    ]),
                    "action_type": "redirect",
                    "action_config": json.dumps({"url": "https://wont-match"}),
                },
                f"flow:{default_flow}": {
                    "campaign_id": campaign_id,
                    "scope_type": "company",
                    "scope_id": "1",
                    "seq_id": "5",
                    "is_default": "1",
                    "criteria": "[]",
                    "action_type": "redirect",
                    "action_config": json.dumps({"url": "https://catchall"}),
                },
            },
            lists={
                f"campaign:{campaign_id}:flows": [explicit_flow, default_flow],
            },
        )
        result = _route_with(redis, _click())
        assert result is not None
        assert "catchall" in result["url"]

    def test_buyer_chain_picks_buyer_scope_flow(self):
        """`?buyer_id=42` resolves a buyer-scoped flow over team-scoped."""
        campaign_id = "10"
        buyer_flow = "100"
        team_flow = "200"
        redis = FakeRedis(
            sets={
                "geo:US": {campaign_id},
                "device:mobile": {campaign_id},
                "os:ios": {campaign_id},
                "campaigns:active": {campaign_id},
            },
            hashes={
                f"campaign:{campaign_id}": {"company_id": "1", "priority": "0"},
                # Source defines `buyer_id` slot aliased from query_params
                "source:99": {
                    "slug": "fb",
                    "param_mappings": json.dumps([
                        {"slot": "buyer_id", "key": "buyer_id"},
                    ]),
                },
                # Buyer 42 → team 7, dept 3, company 1 (active user)
                "user:42": {
                    "id": "42",
                    "team_id": "7",
                    "department_id": "3",
                    "custom_group_id": "",
                    "company_id": "1",
                    "status": "active",
                },
                f"flow:{buyer_flow}": {
                    "campaign_id": campaign_id,
                    "scope_type": "buyer",
                    "scope_id": "42",
                    "seq_id": "1",
                    "is_default": "0",
                    "criteria": "[]",
                    "action_type": "redirect",
                    "action_config": json.dumps({"url": "https://buyer-flow"}),
                },
                f"flow:{team_flow}": {
                    "campaign_id": campaign_id,
                    "scope_type": "team",
                    "scope_id": "7",
                    "seq_id": "1",
                    "is_default": "0",
                    "criteria": "[]",
                    "action_type": "redirect",
                    "action_config": json.dumps({"url": "https://team-flow"}),
                },
            },
            lists={
                f"campaign:{campaign_id}:flows": [buyer_flow, team_flow],
                f"campaign:{campaign_id}:sources": ["99"],
            },
        )
        # Wire `?buyer_id=42&source=fb` → source matches → resolve_slots
        # extracts buyer_id=42 → enrich_buyer → cascade walks buyer scope.
        # `campaign:{cid}:sources` must be a SET, not list. Replace.
        redis.sets[f"campaign:{campaign_id}:sources"] = {"99"}

        result = _route_with(redis, _click({"buyer_id": "42", "source": "fb"}))
        assert result is not None
        assert "buyer-flow" in result["url"]


# ============================================================
# Legacy-fallback path (no flow matches → split:{cid})
# ============================================================


class TestLegacyFallback:
    def test_no_flows_falls_back_to_split(self):
        """Campaign has no flows → legacy `split:{cid}` selects offer."""
        campaign_id = "10"
        offer_id = "55"
        redis = FakeRedis(
            sets={
                "geo:US": {campaign_id},
                "device:mobile": {campaign_id},
                "os:ios": {campaign_id},
                "campaigns:active": {campaign_id},
                f"campaign:{campaign_id}:offers": {offer_id},
            },
            hashes={
                f"campaign:{campaign_id}": {"company_id": "1", "priority": "0"},
                f"offer:{offer_id}": {
                    "url": "https://legacy.offer/{click_id}",
                    "has_targets": "0",
                },
                # No split:{cid} HASH — `select_offer` falls through to
                # campaign:{cid}:offers SET random pick.
            },
            lists={
                f"campaign:{campaign_id}:flows": [],
            },
        )
        result = _route_with(redis, _click())
        assert result is not None
        assert result["campaign_id"] == campaign_id
        assert result["offer_id"] == offer_id
        assert "legacy.offer" in result["url"]
        assert result["timing"].get("route_via") == "legacy_split"

    def test_flow_present_but_no_match_falls_back(self):
        """Flow exists but criteria exclude the click → legacy fallback."""
        campaign_id = "10"
        flow_id = "100"
        offer_id = "55"
        redis = FakeRedis(
            sets={
                "geo:US": {campaign_id},
                "device:mobile": {campaign_id},
                "os:ios": {campaign_id},
                "campaigns:active": {campaign_id},
                f"campaign:{campaign_id}:offers": {offer_id},
            },
            hashes={
                f"campaign:{campaign_id}": {"company_id": "1", "priority": "0"},
                f"flow:{flow_id}": {
                    "campaign_id": campaign_id,
                    "scope_type": "company",
                    "scope_id": "1",
                    "seq_id": "1",
                    "is_default": "0",
                    # Geo=RU only → US click excluded.
                    "criteria": json.dumps([
                        {"type": "geo", "op": "in", "values": ["RU"]},
                    ]),
                    "action_type": "redirect",
                    "action_config": json.dumps({"url": "https://wont-fire"}),
                },
                f"offer:{offer_id}": {
                    "url": "https://legacy.offer",
                    "has_targets": "0",
                },
            },
            lists={
                f"campaign:{campaign_id}:flows": [flow_id],
            },
        )
        result = _route_with(redis, _click())
        assert result is not None
        assert "legacy.offer" in result["url"]
        assert result["timing"]["route_via"] == "legacy_split"


# ============================================================
# R68 (ADR-0034) — legacy Stage-8 now stamps the resolved offer_target_id
# (was always 0), and `resolve_target` stays a byte-identical wrapper over the
# new id-returning `resolve_target_with_id`.
# ============================================================


class TestR68LegacyStamp:
    def test_legacy_split_real_target_stamps_offer_target_id(self):
        """Campaign with no flows → legacy `select_offer` picks offer 55 →
        `resolve_target_with_id` finds target 77 → attribution stamps its id
        (the legacy path recorded offer_target_id=0 before R68)."""
        campaign_id = "10"
        offer_id = "55"
        target_id = "77"
        redis = FakeRedis(
            sets={
                "geo:US": {campaign_id},
                "device:mobile": {campaign_id},
                "os:ios": {campaign_id},
                "campaigns:active": {campaign_id},
                f"campaign:{campaign_id}:offers": {offer_id},
                f"offer:{offer_id}:targets": {target_id},
            },
            hashes={
                f"campaign:{campaign_id}": {"company_id": "1", "priority": "0"},
                f"offer:{offer_id}": {"url": "https://bare", "has_targets": "1"},
                f"offer_target:{target_id}": {
                    "url": "https://t-77/{click_id}",
                    "is_default": "1",
                    "availability": "active",
                    "criteria": "[]",
                    "priority": "0",
                },
            },
            lists={f"campaign:{campaign_id}:flows": []},
        )
        result = _route_with(redis, _click())
        assert result is not None
        assert result["timing"].get("route_via") == "legacy_split"
        assert result["attribution"]["offer_target_id"] == int(target_id)
        assert "t-77" in result["url"]

    def test_resolve_target_wrapper_unchanged(self):
        """Contract — `resolve_target` returns the same `str | None` as
        `resolve_target_with_id(...)[0]` for match / default / no-target /
        all-closed cases (the wrapper must not alter the 11-site signature)."""
        import asyncio

        async def _both(hashes, sets, offer, avail=frozenset({"active"})):
            r = FakeRedis(hashes=hashes, sets=sets)
            wrapped = await router.resolve_target(r, offer, _click(), avail)
            url, _tid = await router.resolve_target_with_id(r, offer, _click(), avail)
            return wrapped, url

        # (a) match — empty-criteria target
        offer = {"_id": "1", "has_targets": "1"}
        w, u = asyncio.run(_both(
            {"offer_target:10": {"url": "https://match", "is_default": "0",
                                 "criteria": "[]", "availability": "active",
                                 "priority": "0"}},
            {"offer:1:targets": {"10"}}, offer))
        assert w == u == "https://match"

        # (b) default — non-matching criteria falls through to the is_default url
        offer = {"_id": "2", "has_targets": "1"}
        w, u = asyncio.run(_both(
            {"offer_target:21": {
                "url": "https://default", "is_default": "1",
                "criteria": json.dumps([{"type": "geo", "op": "in", "values": ["RU"]}]),
                "availability": "active", "priority": "0"}},
            {"offer:2:targets": {"21"}}, offer))
        assert w == u == "https://default"

        # (c) no-target — has_targets=0
        offer = {"_id": "3", "has_targets": "0"}
        w, u = asyncio.run(_both({}, {}, offer))
        assert w is u is None

        # (d) all-closed — every target availability-excluded → None
        offer = {"_id": "4", "has_targets": "1"}
        w, u = asyncio.run(_both(
            {"offer_target:40": {"url": "https://closed", "is_default": "1",
                                 "criteria": "[]", "availability": "closed",
                                 "priority": "0"}},
            {"offer:4:targets": {"40"}}, offer, frozenset({"active"})))
        assert w is u is None


# ============================================================
# Failure modes
# ============================================================


class TestFailureModes:
    def test_no_active_campaigns(self):
        redis = FakeRedis()
        result = _route_with(redis, _click())
        assert result is None

    def test_no_offer_no_flow_returns_non_routed_sentinel(self):
        """Geo-branch campaign matched but has no flow AND no legacy offer.

        G2 (2026-06-02): this is now the TERMINAL geo branch, so instead
        of bare `None` it returns the non-routed sentinel carrying the
        matched campaign + its attribution (so hardcoded defaults persist).
        """
        campaign_id = "10"
        redis = FakeRedis(
            sets={
                "geo:US": {campaign_id},
                "device:mobile": {campaign_id},
                "os:ios": {campaign_id},
                "campaigns:active": {campaign_id},
            },
            hashes={
                f"campaign:{campaign_id}": {"company_id": "1", "priority": "0"},
            },
            lists={f"campaign:{campaign_id}:flows": []},
        )
        result = _route_with(redis, _click())
        assert result is not None
        assert result["non_routed"] is True
        assert result["url"] is None
        assert result["campaign_id"] == campaign_id
        assert result["routing_status"] == "no_offer"
        # Attribution present → fallback record persists slot columns.
        assert "attribution" in result

    def test_g2_campaign_hardcoded_default_persists_on_non_routed(self):
        """G2 — a non-routed (no-flow/no-offer) click whose campaign
        declares a hardcoded `funnel_type` default carries that value in
        the threaded attribution, so the fallback record persists it to
        the column (instead of NULL).
        """
        campaign_id = "10"
        redis = FakeRedis(
            sets={
                "geo:US": {campaign_id},
                "device:mobile": {campaign_id},
                "os:ios": {campaign_id},
                "campaigns:active": {campaign_id},
            },
            hashes={
                f"campaign:{campaign_id}": {
                    "company_id": "1",
                    "priority": "0",
                    # Campaign hardcodes funnel_type — must survive even
                    # though the click never routes.
                    "default_param_mappings": json.dumps([
                        {"slot": "funnel_type", "default_value": "tripwire"},
                    ]),
                },
            },
            lists={f"campaign:{campaign_id}:flows": []},
        )
        result = _route_with(redis, _click())
        assert result["non_routed"] is True
        slots = result["attribution"]["slots"]
        assert slots.get("funnel_type") == "tripwire"
        # And it stamps onto the click record column via the phase-3 helper.
        from app.main import _phase3_attribution_fields
        fields = _phase3_attribution_fields(
            result, _click(), result["timing"],
            "2026-06-02T00:00:00.000Z",
        )
        assert fields["funnel_type"] == "tripwire"

    def test_non_routed_threads_campaign_fallback_url(self):
        """v2 Phase A — the non-routed sentinel carries the campaign's
        `fallback_url` (synced HASH field) so main.py prefers it over the
        node default for the terminal-fallback redirect."""
        campaign_id = "10"
        redis = FakeRedis(
            sets={
                "geo:US": {campaign_id},
                "device:mobile": {campaign_id},
                "os:ios": {campaign_id},
                "campaigns:active": {campaign_id},
            },
            hashes={
                f"campaign:{campaign_id}": {
                    "company_id": "1", "priority": "0",
                    "fallback_url": "https://camp-fallback.example/lp",
                },
            },
            lists={f"campaign:{campaign_id}:flows": []},
        )
        result = _route_with(redis, _click())
        assert result["non_routed"] is True
        assert result["fallback_url"] == "https://camp-fallback.example/lp"

    def test_non_routed_fallback_url_none_when_unset(self):
        """No campaign fallback_url → sentinel carries None → main.py uses the
        node default (byte-identical to pre-v2)."""
        campaign_id = "10"
        redis = FakeRedis(
            sets={
                "geo:US": {campaign_id},
                "device:mobile": {campaign_id},
                "os:ios": {campaign_id},
                "campaigns:active": {campaign_id},
            },
            hashes={f"campaign:{campaign_id}": {"company_id": "1", "priority": "0"}},
            lists={f"campaign:{campaign_id}:flows": []},
        )
        result = _route_with(redis, _click())
        assert result["non_routed"] is True
        assert result["fallback_url"] is None


class TestDomainBranchRouting:
    """Domain-resolved branch routes through the flow cascade.

    Was `TestCapPreCheck`: the campaign-level click-cap / frequency engine
    was removed in returning-users v2 Phase 0 (the cap columns never
    existed on the live DB → the filter was dead code, removal is
    behaviour-preserving). The two cap-blocking cases are gone; this
    remaining case guards that a domain-bound campaign still routes
    normally through the cascade.
    """

    def test_domain_branch_routes(self):
        """Domain-bound campaign → cascade picks the flow and redirects."""
        campaign_id = "10"
        flow_id = "100"
        redis = FakeRedis(
            hashes={
                f"campaign:{campaign_id}": {
                    "company_id": "1",
                    "priority": "0",
                },
                f"flow:{flow_id}": {
                    "campaign_id": campaign_id,
                    "scope_type": "company",
                    "scope_id": "1",
                    "seq_id": "1",
                    "is_default": "0",
                    "criteria": "[]",
                    "action_type": "redirect",
                    "action_config": json.dumps({"url": "https://lp.example"}),
                },
            },
            lists={f"campaign:{campaign_id}:flows": [flow_id]},
        )
        redis.strings["domain:tds.adstudy.dev:root"] = campaign_id
        click = ClickRequest(
            click_id="test-ok",
            country="US",
            user_agent="Mozilla/5.0",
            hostname="tds.adstudy.dev",
            query_params={},
        )
        result = _route_with(redis, click)
        assert result is not None
        assert "lp.example" in result["url"]


class TestWildcardSubdomainFailClosed:
    """§6 (F.30 security): unmatched subdomains of a wildcard-enabled base
    fail closed — no base-key inheritance, no geo fall-through.

    The `domains:wildcard` SET (admin-api sync builder) marks every base
    that has a `*.{base}` wildcard DNS (auto-provisioned when ≥1
    subdomain binding exists). Click-processor decides what is a
    wildcard subdomain by SET membership, not naive label count — so
    multi-label bases (`tds.adstudy.dev`) used directly are never
    mis-classified. Empty / absent SET ⇒ exactly the pre-§6 behaviour.
    """

    def _routable(self, campaign_id, flow_id, url, *, sets=None, strings=None):
        """Minimal campaign+flow snapshot that redirects to `url`."""
        return FakeRedis(
            hashes={
                f"campaign:{campaign_id}": {"company_id": "1", "priority": "0"},
                f"flow:{flow_id}": {
                    "campaign_id": campaign_id,
                    "scope_type": "company",
                    "scope_id": "1",
                    "seq_id": "1",
                    "is_default": "0",
                    "criteria": "[]",
                    "action_type": "redirect",
                    "action_config": json.dumps({"url": url}),
                },
            },
            lists={f"campaign:{campaign_id}:flows": [flow_id]},
            sets=sets or {},
            strings=strings or {},
        )

    def _click(self, hostname, **kw):
        return ClickRequest(
            click_id="t", country="US", user_agent="Mozilla/5.0",
            hostname=hostname, query_params=kw.get("query_params", {}),
        )

    def test_unmatched_subdomain_of_wildcard_base_blocks(self):
        """`evil.adstudy.dev` with no subdomain binding → block, even
        though the base carries a root binding (must NOT inherit it)."""
        redis = FakeRedis(
            sets={"domains:wildcard": {"adstudy.dev"}},
            strings={"domain:adstudy.dev:root": "10"},  # base root — must NOT leak
        )
        result = _route_with(redis, self._click("evil.adstudy.dev"))
        assert result is not None
        assert result["blocked"] is True
        assert result["url"] is None
        assert result["timing"]["result"] == "blocked_unmatched_subdomain"

    def test_unmatched_subdomain_does_not_fall_through_to_geo(self):
        """Wildcard-subdomain miss must NOT route to a geo-matched
        campaign — the strongest §6 guarantee."""
        redis = self._routable(
            "10", "100", "https://geo-lp.example",
            sets={"domains:wildcard": {"adstudy.dev"}},
        )
        # A campaign that WOULD win the geo branch (active, no targeting
        # flags = matches all) if we wrongly fell through.
        redis.sets["campaigns:active"] = {"10"}
        redis.sets["geo:US"] = {"10"}
        result = _route_with(redis, self._click("evil.adstudy.dev"))
        assert result["blocked"] is True
        assert result["url"] is None

    def test_matched_wildcard_subdomain_routes(self):
        """`gambling.adstudy.dev` with a subdomain binding → routes."""
        redis = self._routable(
            "10", "100", "https://sub-lp.example",
            sets={"domains:wildcard": {"adstudy.dev"}},
            strings={"domain:adstudy.dev:subdomain:gambling": "10"},
        )
        result = _route_with(redis, self._click("gambling.adstudy.dev"))
        assert result is not None
        assert result.get("blocked") is not True
        assert "sub-lp.example" in result["url"]
        assert result["timing"]["domain_matched"] is True

    def test_exact_host_root_wins_over_wildcard_subdomain_binding(self):
        """A subdomain that is itself a registered domain (own root
        binding) resolves to its own root, not the parent's subdomain
        binding — exact-host precedence."""
        redis = self._routable(
            "20", "200", "https://own-root.example",
            sets={"domains:wildcard": {"adstudy.dev"}},
            strings={
                "domain:special.adstudy.dev:root": "20",       # exact host wins
                "domain:adstudy.dev:subdomain:special": "10",  # parent binding
            },
        )
        result = _route_with(redis, self._click("special.adstudy.dev"))
        assert result.get("blocked") is not True
        assert "own-root.example" in result["url"]  # campaign 20, not 10

    def test_multilabel_base_used_directly_not_blocked(self):
        """`tds.adstudy.dev` as a root domain — its parent `adstudy.dev`
        is NOT a wildcard base, so it resolves normally and is never
        mis-read as a subdomain of `adstudy.dev`."""
        redis = self._routable(
            "30", "300", "https://multi.example",
            strings={"domain:tds.adstudy.dev:root": "30"},  # no wildcard set
        )
        result = _route_with(redis, self._click("tds.adstudy.dev"))
        assert result.get("blocked") is not True
        assert "multi.example" in result["url"]

    def test_empty_wildcard_set_preserves_legacy_fallthrough(self):
        """No wildcard marker (pre-deploy / absent) → `x.adstudy.dev`
        resolves via the base root exactly as the pre-§6 resolver did
        (3-deploy safety)."""
        redis = self._routable(
            "40", "400", "https://legacy.example",
            strings={"domain:adstudy.dev:root": "40"},  # no domains:wildcard
        )
        result = _route_with(redis, self._click("x.adstudy.dev"))
        assert result.get("blocked") is not True
        assert "legacy.example" in result["url"]

    def test_wildcard_base_itself_resolves_normally(self):
        """The base domain itself (2-label) is never its own subdomain —
        resolves via root even while present in the wildcard set."""
        redis = self._routable(
            "50", "500", "https://base.example",
            sets={"domains:wildcard": {"adstudy.dev"}},
            strings={"domain:adstudy.dev:root": "50"},
        )
        result = _route_with(redis, self._click("adstudy.dev"))
        assert result.get("blocked") is not True
        assert "base.example" in result["url"]

    def test_uppercase_hostname_still_fails_closed(self):
        """§6 must not be case-bypassable: an UPPERCASE unmatched subdomain
        is normalised to lowercase before the wildcard membership check, so
        it still blocks (the wildcard set + keys are stored lowercased)."""
        redis = FakeRedis(
            sets={"domains:wildcard": {"adstudy.dev"}},
            strings={"domain:adstudy.dev:root": "10"},
        )
        result = _route_with(redis, self._click("EVIL.ADSTUDY.DEV"))
        assert result is not None
        assert result["blocked"] is True
        assert result["url"] is None

    def test_trailing_dot_hostname_normalised(self):
        """FQDN trailing-dot form resolves identically (no spurious block)."""
        redis = self._routable(
            "10", "100", "https://sub-lp.example",
            sets={"domains:wildcard": {"adstudy.dev"}},
            strings={"domain:adstudy.dev:subdomain:gambling": "10"},
        )
        result = _route_with(redis, self._click("gambling.adstudy.dev."))
        assert result.get("blocked") is not True
        assert "sub-lp.example" in result["url"]

    def test_corrupt_json_on_wildcard_base_blocks(self):
        """A corrupt JSON binding value on a wildcard subdomain is a MISS →
        fail closed, not route-to-bogus-campaign."""
        redis = FakeRedis(
            sets={"domains:wildcard": {"adstudy.dev"}},
            strings={"domain:adstudy.dev:subdomain:gambling": "{corrupt"},
        )
        result = _route_with(redis, self._click("gambling.adstudy.dev"))
        assert result is not None
        assert result["blocked"] is True
        assert result["url"] is None


class TestParseBindingValue:
    """F.31 B.3: `_parse_binding_value` dual-reads the F.31 JSON payload
    and the legacy bare-scalar campaign_id (3-deploy window)."""

    def test_json_full_payload(self):
        assert router._parse_binding_value(
            json.dumps({"campaign_id": "5", "binding_id": 9, "binding_alias": "x"})
        ) == ("5", 9, "x")

    def test_legacy_scalar(self):
        assert router._parse_binding_value("42") == ("42", 0, None)

    def test_malformed_json_treated_as_miss(self):
        # A value that opens with `{` is unambiguously meant to be the F.31
        # JSON shape; if it's corrupt, return an EMPTY cid (a MISS) so the
        # caller fails closed — never route to a bogus `campaign:{...` id.
        assert router._parse_binding_value("{bad") == ("", 0, None)

    def test_empty_and_none(self):
        assert router._parse_binding_value("") == ("", 0, None)
        assert router._parse_binding_value(None) == ("", 0, None)

    def test_json_null_alias_and_missing_binding_id(self):
        assert router._parse_binding_value(
            json.dumps({"campaign_id": "5", "binding_alias": None})
        ) == ("5", 0, None)

    def test_json_non_numeric_binding_id_coerces_to_zero(self):
        assert router._parse_binding_value(
            json.dumps({"campaign_id": "5", "binding_id": "oops"})
        ) == ("5", 0, None)


class TestBindingMetadataInResult:
    """F.31 B.3: domain-resolved clicks thread binding_id + binding_alias
    into the route() result so the click record can attribute analytics to
    the exact binding the click arrived through."""

    def _routable(self, domain_value):
        return FakeRedis(
            hashes={
                "campaign:10": {"company_id": "1", "priority": "0"},
                "flow:100": {
                    "campaign_id": "10", "scope_type": "company",
                    "scope_id": "1", "seq_id": "1", "is_default": "0",
                    "criteria": "[]", "action_type": "redirect",
                    "action_config": json.dumps({"url": "https://lp.example"}),
                },
            },
            lists={"campaign:10:flows": ["100"]},
            strings={"domain:promo.example:root": domain_value},
        )

    def _click(self):
        return ClickRequest(
            click_id="t", country="US", user_agent="Mozilla/5.0",
            hostname="promo.example", query_params={},
        )

    def test_json_value_carries_binding_metadata(self):
        value = json.dumps(
            {"campaign_id": "10", "binding_id": 77, "binding_alias": "spring"}
        )
        result = _route_with(self._routable(value), self._click())
        assert "lp.example" in result["url"]
        assert result["binding_id"] == 77
        assert result["binding_alias"] == "spring"

    def test_legacy_scalar_value_defaults_metadata(self):
        result = _route_with(self._routable("10"), self._click())
        assert "lp.example" in result["url"]
        assert result["binding_id"] == 0
        assert result["binding_alias"] is None

    def test_geo_resolved_click_has_default_binding(self):
        """A click with no domain binding routes via geo → binding_id 0,
        alias None (the '(default)' analytics bucket)."""
        redis = self._routable("10")
        redis.sets["campaigns:active"] = {"10"}
        redis.sets["geo:US"] = {"10"}
        # No hostname → domain resolution returns no match → geo branch.
        click = ClickRequest(
            click_id="t", country="US", user_agent="Mozilla/5.0",
            hostname="", query_params={},
        )
        result = _route_with(redis, click)
        assert result is not None
        assert "lp.example" in result["url"]
        assert result["binding_id"] == 0
        assert result["binding_alias"] is None


# ============================================================
# P-DEAD — _effective_source_mappings end-to-end Redis-read selection
# ============================================================
#
# The headline 2026-06-02 fix: the per-link override HASH
# `campaign:{cid}:source_overrides` was DEAD at click time. These tests
# drive the REAL read path through `route()` → `_fetch_resolution_context`
# → `_effective_source_mappings` (not the pure `resolve_slots` level — that
# is covered in test_resolution.py). Each asserts on
# `result["attribution"]["slots"]`, the resolved slot bundle the click
# record persists, so the FULL selection matrix is pinned:
#   - per-link override (non-null list)        → override WINS
#   - null / absent override                   → inherit source global
#   - explicit []                              → "no source mappings"
#                                                (campaign fallback)
#   - malformed JSON / non-dict                → defensive source global
# Contract: param-source-campaign-overrides-2026-06-02.md (P-DEAD).


class TestEffectiveSourceOverrideReadPath:
    CID = "5"
    SID = "42"

    def _redis(self, *, source_overrides_field: str | None) -> FakeRedis:
        """Routable redirect-flow campaign + linked source `fbsrc`.

        - source global hardcodes pixel_id = "global_px"
        - campaign hardcodes pixel_id = "cmp_px"
        With SOURCE-WINS, the source layer (global OR per-link override)
        wins per slot unless it contributes nothing for that slot.

        `source_overrides_field`: the raw value stored in the
        `campaign:{cid}:source_overrides` HASH under field `str(SID)`
        (mirrors the admin-api sync builder's `json.dumps({...})`). When
        `None`, the HASH field is ABSENT entirely (no override row).
        """
        cid, sid = self.CID, self.SID
        overrides_hash = {} if source_overrides_field is None else {sid: source_overrides_field}
        return FakeRedis(
            sets={
                "geo:US": {cid},
                "device:mobile": {cid},
                "os:ios": {cid},
                "campaigns:active": {cid},
                f"campaign:{cid}:sources": {sid},
            },
            hashes={
                f"campaign:{cid}": {
                    "company_id": "1",
                    "priority": "0",
                    "weight": "100",
                    "default_param_mappings": json.dumps([
                        {"slot": "pixel_id", "default_value": "cmp_px"},
                    ]),
                },
                f"source:{sid}": {
                    "slug": "fbsrc",
                    "param_mappings": json.dumps([
                        {"slot": "pixel_id", "default_value": "global_px"},
                    ]),
                },
                f"campaign:{cid}:source_overrides": overrides_hash,
                f"flow:300": {
                    "campaign_id": cid,
                    "scope_type": "company",
                    "scope_id": "1",
                    "seq_id": "1",
                    "is_default": "0",
                    "criteria": "[]",
                    "action_type": "redirect",
                    "action_config": json.dumps({
                        "url": "https://lp.example.com/?px={pixel_id}",
                    }),
                },
            },
            lists={f"campaign:{cid}:flows": ["300"]},
        )

    def _slots(self, redis: FakeRedis, query=None):
        qp = {"source": "fbsrc"}
        if query:
            qp.update(query)
        result = _route_with(redis, _click(qp))
        assert result is not None
        assert result["attribution"]["source_id"] == int(self.SID)
        return result

    def test_override_default_wins_over_source_global(self):
        # Non-null params_override hardcoding pixel_id → override WINS over
        # both the source global AND the campaign.
        ov = json.dumps({"params_override": [
            {"slot": "pixel_id", "default_value": "override_px"},
        ]})
        result = self._slots(self._redis(source_overrides_field=ov))
        assert result["attribution"]["slots"]["pixel_id"] == "override_px"
        assert "px=override_px" in result["url"]

    def test_override_alias_wins_and_reads_url(self):
        # Override aliases pixel_id ← "ovpx"; URL carries ?ovpx=fromurl.
        # The override's alias is what gets looked up (URL > override).
        ov = json.dumps({"params_override": [
            {"slot": "pixel_id", "alias": "ovpx"},
        ]})
        result = self._slots(
            self._redis(source_overrides_field=ov), query={"ovpx": "fromurl"},
        )
        assert result["attribution"]["slots"]["pixel_id"] == "fromurl"

    def test_absent_override_inherits_source_global(self):
        # No override row at all → effective source = source global.
        result = self._slots(self._redis(source_overrides_field=None))
        assert result["attribution"]["slots"]["pixel_id"] == "global_px"

    def test_null_params_override_inherits_source_global(self):
        # Override row exists but params_override is JSON null → inherit
        # the source global (the per-link toggle is OFF for params).
        ov = json.dumps({"params_override": None, "postbacks_override": []})
        result = self._slots(self._redis(source_overrides_field=ov))
        assert result["attribution"]["slots"]["pixel_id"] == "global_px"

    def test_empty_list_override_means_no_source_mappings(self):
        # Explicit [] → admin wiped all per-link mappings. Honoured as
        # "the source contributes NOTHING" (NOT a silent global fallback),
        # so the campaign hardcoded default is the fallback that applies.
        ov = json.dumps({"params_override": []})
        result = self._slots(self._redis(source_overrides_field=ov))
        assert result["attribution"]["slots"]["pixel_id"] == "cmp_px"

    def test_malformed_json_override_falls_back_to_source_global(self):
        # Drift / corruption in the HASH field → never blank params,
        # defensively inherit the source global.
        result = self._slots(self._redis(source_overrides_field="{not valid json"))
        assert result["attribution"]["slots"]["pixel_id"] == "global_px"

    def test_non_dict_json_override_falls_back_to_source_global(self):
        # Valid JSON but not the expected dict shape (e.g. a bare list) →
        # defensive source-global fallback.
        result = self._slots(self._redis(source_overrides_field=json.dumps([1, 2])))
        assert result["attribution"]["slots"]["pixel_id"] == "global_px"


# ============================================================
# v2 Phase A FIXUP — availability returning-class is GATED on returning_routing
# ============================================================


class TestAvailabilityClassGating:
    """The availability returning-class (draining serves returning) activates
    TOGETHER with returning routing. routing OFF ⇒ a seen_before visitor is the
    NEW class for availability (draining blocks all) → TOTAL byte-identical."""

    def _capture_returning_visitor(self, *, routing_enabled, company_routing):
        from app import cascade as cascade_mod, identity as identity_mod
        from app.identity import IdentityResult
        from app.config import settings

        captured: dict = {}

        async def _fake_resolve_flow(r, **kw):  # noqa: ANN001
            captured["returning_visitor"] = kw.get("returning_visitor")
            captured["audience_routing"] = kw.get("audience_routing")
            return None  # force non-route — we only assert the passed kwargs

        async def _fake_stamp(**kw):  # seen_before visitor (uid set, not unique)
            return IdentityResult(uid="U", is_unique=False, is_returning=True)

        campaign_id = "10"
        camp = {"company_id": "1", "priority": "0", "returning_resolver": "1"}
        if company_routing:
            camp["returning_routing"] = "1"
            # MODEL V3 — the partition (and thus the returning availability class)
            # is gated by EXISTENCE, not a mode: routing live + the campaign has
            # NOT opted out via `disable_returning_flows`. No mode field needed.
        redis = FakeRedis(
            sets={
                "geo:US": {campaign_id}, "device:mobile": {campaign_id},
                "os:ios": {campaign_id}, "campaigns:active": {campaign_id},
            },
            hashes={f"campaign:{campaign_id}": camp},
            lists={f"campaign:{campaign_id}:flows": []},
        )
        with patch.object(settings, "returning_resolver_enabled", True), \
             patch.object(settings, "returning_routing_enabled", routing_enabled), \
             patch.object(identity_mod, "resolve_and_stamp", _fake_stamp), \
             patch.object(cascade_mod, "resolve_flow", _fake_resolve_flow):
            _route_with(redis, _click())
        return captured

    def test_routing_off_seen_before_treated_as_new_for_availability(self):
        # resolver ON, routing OFF, seen_before visitor → availability class is
        # NEW (returning_visitor False) → a draining target would block it.
        cap = self._capture_returning_visitor(routing_enabled=False, company_routing=False)
        assert cap["audience_routing"] is False
        assert cap["returning_visitor"] is False

    def test_routing_on_seen_before_is_returning_for_availability(self):
        # resolver ON, routing ON (env + company) → returning_visitor True →
        # a draining target serves the returning visitor.
        cap = self._capture_returning_visitor(routing_enabled=True, company_routing=True)
        assert cap["audience_routing"] is True
        assert cap["returning_visitor"] is True


# ============================================================
# MODEL V3 — existence-driven returning partition + disable flag + campaign mode
# ============================================================


class TestReturningModeV3:
    """MODEL V3: the returning partition activates by the EXISTENCE of a
    returning flow in scope, NOT a per-campaign override mode. The partition is
    gated by `audience_routing` = routing live AND the campaign has NOT opted out
    via `disable_returning_flows`. The campaign `returning_mode` (fresh|sticky)
    no longer gates the partition — it only governs the recorded effective mode +
    the Phase-S sticky pin. A 'returning'-audience flow keyed on is_returning is
    the discriminator: it only matches when the partition injects the is_returning
    dim (audience_routing ON). The cascade's empty-returning-pool fallthrough is
    what makes "existence" work — no returning flow ⇒ the returning pass is a
    no-op ⇒ the click routes fresh."""

    def _route(self, *, campaign_returning_mode, routing_enabled,
               disable_returning_flows=False, include_returning_flow=True):
        from app import identity as identity_mod
        from app.identity import IdentityResult
        from app.config import settings

        campaign_id = "10"
        camp = {
            "company_id": "1", "priority": "0",
            "returning_resolver": "1", "returning_routing": "1",
            "returning_mode": campaign_returning_mode,
        }
        if disable_returning_flows:
            camp["disable_returning_flows"] = "1"
        ret_flow = {
            "campaign_id": campaign_id, "scope_type": "company", "scope_id": "1",
            "seq_id": "1", "is_default": "0", "audience": "returning",
            "criteria": json.dumps([{"type": "is_returning", "op": "in", "values": ["true"]}]),
            "action_type": "redirect",
            "action_config": json.dumps({"url": "https://RET/{click_id}"}),
        }
        first_flow = {
            "campaign_id": campaign_id, "scope_type": "company", "scope_id": "1",
            "seq_id": "2", "is_default": "0", "audience": "first", "criteria": "[]",
            "action_type": "redirect",
            "action_config": json.dumps({"url": "https://FIRST/{click_id}"}),
        }
        # MODEL V3 cornerstone: with NO returning flow in scope the returning
        # pass is a no-op (empty pool) and the click falls through to fresh —
        # `include_returning_flow=False` exercises that "activation by existence".
        hashes = {f"campaign:{campaign_id}": camp, "flow:2": first_flow}
        flow_list = ["2"]
        if include_returning_flow:
            hashes["flow:1"] = ret_flow
            flow_list = ["1", "2"]
        redis = FakeRedis(
            sets={
                "geo:US": {campaign_id}, "device:mobile": {campaign_id},
                "os:ios": {campaign_id}, "campaigns:active": {campaign_id},
            },
            hashes=hashes,
            lists={f"campaign:{campaign_id}:flows": flow_list},
        )

        async def _stamp(**kw):
            return IdentityResult(uid="U", is_unique=False, is_returning=True)

        with patch.object(settings, "returning_resolver_enabled", True), \
             patch.object(settings, "returning_routing_enabled", routing_enabled), \
             patch.object(identity_mod, "resolve_and_stamp", _stamp):
            return _route_with(redis, _click())

    def test_fresh_with_returning_flow_activates_partition(self):
        # MODEL V3 (existence-driven, (a)) — campaign_mode=fresh + disable flag
        # UNSET + a returning flow EXISTS in scope ⇒ the partition is ACTIVE and a
        # seen_before visitor routes via the returning pool. This is the core V3
        # flip: under v2 fresh disabled the partition; under V3 only the disable
        # flag does. Recorded mode = the campaign mode (fresh).
        r = self._route(campaign_returning_mode="fresh", routing_enabled=True)
        assert "RET" in r["url"]  # partition ACTIVE (existence-driven)
        assert r["attribution"]["returning_mode"] == "fresh"

    def test_disable_flag_forces_fallthrough(self):
        # MODEL V3 (b) — the per-campaign `disable_returning_flows` flag suppresses
        # the partition even though a returning flow exists ⇒ the seen_before
        # visitor routes through the first/fresh pool (the headline behavioral
        # assertion). The recorded `returning_mode` still reflects the campaign
        # mode in force for a seen_before visitor under live routing (the recorded
        # mode is mode-of-record, independent of whether the partition activated).
        r = self._route(
            campaign_returning_mode="fresh", routing_enabled=True,
            disable_returning_flows=True,
        )
        assert "FIRST" in r["url"]  # partition DISABLED → routed as new
        assert r["attribution"]["returning_mode"] == "fresh"

    def test_no_returning_flow_in_scope_routes_fresh(self):
        # MODEL V3 cornerstone (activation BY EXISTENCE) — routing live + a
        # seen_before visitor + partition NOT disabled, but NO returning-audience
        # flow exists in scope ⇒ the returning pass is a no-op (empty pool) ⇒ the
        # cascade falls through to the first pool ⇒ the visitor routes FRESH.
        # Absent a returning flow an ACTIVE partition changes nothing — this is
        # exactly what makes "existence drives activation" true.
        r = self._route(
            campaign_returning_mode="fresh", routing_enabled=True,
            include_returning_flow=False,
        )
        assert "FIRST" in r["url"]  # empty returning pool → fresh fallthrough

    def test_sticky_mode_routes_via_returning_pool(self):
        # campaign_mode=sticky + returning flow exists ⇒ partition active; the
        # returning flow keeps its own pick (D35 — sticky pin does NOT override a
        # returning winner). Recorded mode = sticky.
        r = self._route(campaign_returning_mode="sticky", routing_enabled=True)
        assert "RET" in r["url"]
        assert r["attribution"]["returning_mode"] == "sticky"

    def test_routing_off_partition_inert_byte_identical(self):
        # routing OFF ⇒ partition off regardless of mode/flag ⇒ routed as new; the
        # recorded mode is "na" (returning routing not live).
        r = self._route(campaign_returning_mode="sticky", routing_enabled=False)
        assert "FIRST" in r["url"]
        assert r["attribution"]["returning_mode"] == "na"

    def test_recorded_mode_is_campaign_mode_no_flow_override(self):
        # MODEL V3 — the per-flow returning_mode override was REMOVED. Even if a
        # (dormant / legacy) returning_mode value rode on the winning flow HASH,
        # the router no longer reads it: the recorded effective mode is ALWAYS the
        # campaign mode (here fresh), never a flow-level value.
        from app import identity as identity_mod
        from app.identity import IdentityResult
        from app.config import settings

        campaign_id = "10"
        camp = {"company_id": "1", "priority": "0", "returning_resolver": "1",
                "returning_routing": "1", "returning_mode": "fresh"}
        ret_flow = {
            "campaign_id": campaign_id, "scope_type": "company", "scope_id": "1",
            "seq_id": "1", "is_default": "0", "audience": "returning",
            # A stale/dormant flow-level value the V3 router MUST ignore.
            "returning_mode": "sticky",
            "criteria": json.dumps([{"type": "is_returning", "op": "in", "values": ["true"]}]),
            "action_type": "redirect",
            "action_config": json.dumps({"url": "https://RET/{click_id}"}),
        }
        redis = FakeRedis(
            sets={"geo:US": {campaign_id}, "device:mobile": {campaign_id},
                  "os:ios": {campaign_id}, "campaigns:active": {campaign_id}},
            hashes={f"campaign:{campaign_id}": camp, "flow:1": ret_flow},
            lists={f"campaign:{campaign_id}:flows": ["1"]},
        )

        async def _stamp(**kw):
            return IdentityResult(uid="U", is_unique=False, is_returning=True)

        with patch.object(settings, "returning_resolver_enabled", True), \
             patch.object(settings, "returning_routing_enabled", True), \
             patch.object(identity_mod, "resolve_and_stamp", _stamp):
            r = _route_with(redis, _click())
        assert "RET" in r["url"]
        # campaign mode (fresh), NOT the flow-level "sticky" — per-flow override gone.
        assert r["attribution"]["returning_mode"] == "fresh"


# ============================================================
# v2 Phase S — sticky binding (uid, campaign) → offer_target pin
# ============================================================


class TestStickyPhaseS:
    """Mint on first / honor on return / re-pin on closed / draining serves /
    cross-company isolation / mode≠sticky byte-identical / fail-open. Sticky
    pins live in the (here shared) identity Redis; offer_target availability is
    read from the routing Redis — both backed by ONE FakeRedis in the test."""

    def _redis(self, *, returning_mode="sticky", flow_target="7", offer_targets=None,
               pins=None):
        campaign_id = "10"
        camp = {
            "company_id": "1", "priority": "0",
            "returning_resolver": "1", "returning_routing": "1",
            "returning_mode": returning_mode,
        }
        flow = {
            "campaign_id": campaign_id, "scope_type": "company", "scope_id": "1",
            "seq_id": "1", "is_default": "0", "audience": "first", "criteria": "[]",
            "action_type": "offer",
            "action_config": json.dumps({"offer_id": 1, "target_id": int(flow_target)}),
        }
        hashes = {f"campaign:{campaign_id}": camp, "flow:1": flow}
        for tid, h in (offer_targets or {}).items():
            hashes[f"offer_target:{tid}"] = h
        return FakeRedis(
            sets={"geo:US": {campaign_id}, "device:mobile": {campaign_id},
                  "os:ios": {campaign_id}, "campaigns:active": {campaign_id}},
            hashes=hashes,
            lists={f"campaign:{campaign_id}:flows": ["1"]},
            strings={**(pins or {})},
        )

    def _run(self, redis, *, is_unique, uid="U", resolver=True, routing=True,
             identity_redis=None):
        from app import identity as identity_mod, sticky as sticky_mod
        from app.identity import IdentityResult
        from app.config import settings
        import asyncio

        async def _stamp(**kw):
            return IdentityResult(uid=uid, is_unique=is_unique, is_returning=not is_unique)

        async def _async_redis():
            return redis

        async def _gir():
            return identity_redis if identity_redis is not None else redis

        async def _runner():
            with patch.object(router, "get_redis", _async_redis), \
                 patch.object(settings, "returning_resolver_enabled", resolver), \
                 patch.object(settings, "returning_routing_enabled", routing), \
                 patch.object(identity_mod, "resolve_and_stamp", _stamp), \
                 patch.object(sticky_mod, "get_identity_redis", _gir):
                return await router.route(_click())

        return asyncio.run(_runner())

    def test_mint_on_first_visit(self):
        redis = self._redis(offer_targets={
            "7": {"url": "https://T7/{click_id}", "availability": "active", "offer_id": "1"},
        })
        result = self._run(redis, is_unique=True)
        assert "T7" in result["url"]
        assert result["attribution"]["sticky_status"] == "minted"
        # pin written for next visit.
        assert redis.strings.get("sticky:1:U:10") == "7"
        assert result["attribution"]["target_selection_path"] == "pinned"

    def test_honor_pin_on_return(self):
        redis = self._redis(
            flow_target="7",
            offer_targets={
                "7": {"url": "https://T7/{click_id}", "availability": "active", "offer_id": "1"},
                "9": {"url": "https://T9/{click_id}", "availability": "active", "offer_id": "1"},
            },
            pins={"sticky:1:U:10": "9"},  # prior pin → target 9 (not the flow's 7)
        )
        result = self._run(redis, is_unique=False)
        assert "T9" in result["url"]  # pin overrides the flow's offer pick
        assert result["attribution"]["sticky_status"] == "hit"
        assert result["attribution"]["target_selection_path"] == "sticky"
        assert result["attribution"]["offer_target_id"] == 9

    def test_draining_pin_serves_returning(self):
        redis = self._redis(
            offer_targets={
                "7": {"url": "https://T7/{click_id}", "availability": "active", "offer_id": "1"},
                "9": {"url": "https://T9/{click_id}", "availability": "draining", "offer_id": "1"},
            },
            pins={"sticky:1:U:10": "9"},
        )
        result = self._run(redis, is_unique=False)
        assert "T9" in result["url"]  # draining still serves a returning visitor
        assert result["attribution"]["sticky_status"] == "hit"

    def test_closed_pin_repins_to_new_target(self):
        redis = self._redis(
            flow_target="7",
            offer_targets={
                "7": {"url": "https://T7/{click_id}", "availability": "active", "offer_id": "1"},
                "9": {"url": "https://T9/{click_id}", "availability": "closed", "offer_id": "1"},
            },
            pins={"sticky:1:U:10": "9"},  # pinned target is CLOSED
        )
        result = self._run(redis, is_unique=False)
        assert "T7" in result["url"]  # re-picked the flow's (available) target
        assert result["attribution"]["sticky_status"] == "invalid_closed"
        assert redis.strings.get("sticky:1:U:10") == "7"  # re-pinned (overwrite)

    def test_missing_pin_target_repins(self):
        redis = self._redis(
            flow_target="7",
            offer_targets={
                "7": {"url": "https://T7/{click_id}", "availability": "active", "offer_id": "1"},
            },
            pins={"sticky:1:U:10": "404"},  # pinned target does not exist
        )
        result = self._run(redis, is_unique=False)
        assert "T7" in result["url"]
        assert result["attribution"]["sticky_status"] == "invalid_closed"
        assert redis.strings.get("sticky:1:U:10") == "7"

    def test_closed_pin_repin_unavailable_is_honest_status(self):
        # C-L-1 (audit-2 2026-06-07): focused on `_resolve_action_with_sticky`.
        # The pin (target 9) is CLOSED and the re-pick (_normal → execute_action)
        # yields UNAVAILABLE — every candidate target drained/closed (reachable
        # when the cascade pre-floor kept the flow, e.g. an offer flow with no
        # pinned target whose offer-default drifted closed). NO re-pin happens,
        # so the status must be the honest "invalid_closed_term", NOT
        # "invalid_closed" (which main.py maps to decision_reason `fresh_repin`).
        import asyncio
        from app import action_executor, sticky as sticky_mod
        from app.config import settings

        routing = FakeRedis(hashes={
            "offer_target:9": {"url": "https://T9", "availability": "closed",
                               "offer_id": "1"},
        })
        ident = FakeRedis(strings={"sticky:1:U:10": "9"})  # stale closed pin

        async def _gir():
            return ident

        async def _unavailable(*a, **k):
            return action_executor.UNAVAILABLE_RESULT

        async def _runner():
            with patch.object(sticky_mod, "get_identity_redis", _gir), \
                 patch.object(action_executor, "execute_action", _unavailable), \
                 patch.object(settings, "returning_uid_ttl_seconds", 1000):
                return await router._resolve_action_with_sticky(
                    routing, {"action_type": "offer"}, _click(), "10",
                    source_mappings={}, campaign_mappings={},
                    sticky_active=True, uid="U", company_id=1,
                    seen_before=True, returning_visitor=True,
                    flow_id="1", allowed_avail=frozenset({"active", "draining"}),
                )

        result, status = asyncio.run(_runner())
        assert result is action_executor.UNAVAILABLE_RESULT
        assert status == "invalid_closed_term"
        assert status != "invalid_closed"  # would falsely map to fresh_repin
        # No re-pin — the stale (closed) pin is left untouched.
        assert ident.strings.get("sticky:1:U:10") == "9"

    def test_closed_pin_genuine_repin_still_invalid_closed(self):
        # C-L-1 guard: when the re-pick DOES yield a routable target, the status
        # stays "invalid_closed" (a genuine re-pin → decision_reason fresh_repin),
        # unchanged by the C-L-1 fix.
        import asyncio
        from app import action_executor, sticky as sticky_mod
        from app.config import settings

        routing = FakeRedis(hashes={
            "offer_target:9": {"url": "https://T9", "availability": "closed",
                               "offer_id": "1"},
        })
        ident = FakeRedis(strings={"sticky:1:U:10": "9"})

        async def _gir():
            return ident

        async def _repick(*a, **k):
            return {"url": "https://T7", "target_id": "7",
                    "target_selection_path": "offer_default"}

        async def _runner():
            with patch.object(sticky_mod, "get_identity_redis", _gir), \
                 patch.object(action_executor, "execute_action", _repick), \
                 patch.object(settings, "returning_uid_ttl_seconds", 1000):
                return await router._resolve_action_with_sticky(
                    routing, {"action_type": "offer"}, _click(), "10",
                    source_mappings={}, campaign_mappings={},
                    sticky_active=True, uid="U", company_id=1,
                    seen_before=True, returning_visitor=True,
                    flow_id="1", allowed_avail=frozenset({"active", "draining"}),
                )

        result, status = asyncio.run(_runner())
        assert status == "invalid_closed"
        assert result["target_id"] == "7"
        # Genuine re-pin overwrote the stale pin to the fresh target.
        assert ident.strings.get("sticky:1:U:10") == "7"

    def test_returning_no_pin_mints_miss(self):
        redis = self._redis(offer_targets={
            "7": {"url": "https://T7/{click_id}", "availability": "active", "offer_id": "1"},
        })
        result = self._run(redis, is_unique=False)  # seen_before, no pin
        assert "T7" in result["url"]
        assert result["attribution"]["sticky_status"] == "miss"
        assert redis.strings.get("sticky:1:U:10") == "7"  # minted now

    def test_cross_company_isolation(self):
        # A pin for company 1 is invisible to company 2's keyspace. The flow's
        # campaign is company 1; a pin keyed under company 2 must NOT be honored.
        redis = self._redis(
            offer_targets={
                "7": {"url": "https://T7/{click_id}", "availability": "active", "offer_id": "1"},
                "9": {"url": "https://T9/{click_id}", "availability": "active", "offer_id": "1"},
            },
            pins={"sticky:2:U:10": "9"},  # company 2 pin — wrong tenant
        )
        result = self._run(redis, is_unique=False)
        assert "T7" in result["url"]  # company-1 click ignores company-2 pin
        assert result["attribution"]["sticky_status"] == "miss"  # no company-1 pin

    def test_mode_fresh_tracks_pin_but_never_serves_it(self):
        # MODEL V3 + B-track (2026-06-10) — mode=fresh → the PIN never gates
        # the pick (byte-identical offer selection, sticky_status "na"), but
        # fresh now TRACKS: the served target overwrites the (uid, campaign)
        # pin so a later flip to sticky freezes the LAST offer the visitor
        # actually received (was: fresh never touched pins → re-enabling
        # sticky resurrected the first-ever pin).
        redis = self._redis(returning_mode="fresh", offer_targets={
            "7": {"url": "https://T7/{click_id}", "availability": "active", "offer_id": "1"},
        })
        result = self._run(redis, is_unique=False)
        assert "T7" in result["url"]
        assert result["attribution"]["sticky_status"] == "na"
        # B-track bookkeeping: pin now equals the just-served target.
        assert redis.strings.get("sticky:1:U:10") == "7"

    def test_d35_returning_flow_winner_not_overridden_by_sticky_pin(self):
        # MODEL V3 / D35 precedence — when the winning flow comes from the
        # RETURNING pool, its OWN offer pick is served; the campaign sticky pin
        # does NOT override it (precedence: returning-flow > sticky pin > fresh).
        # Here a returning-audience offer flow (target 7) wins for a seen_before
        # visitor under mode=sticky, with a stale pin → target 9 PRESENT. The pin
        # must be IGNORED: sticky is suppressed for a returning winner, so the
        # click serves T7 (the returning flow's pick), not T9 (the pin), and
        # sticky_status is "na" (sticky_active was False for this winner).
        campaign_id = "10"
        camp = {
            "company_id": "1", "priority": "0",
            "returning_resolver": "1", "returning_routing": "1",
            "returning_mode": "sticky",
        }
        ret_flow = {
            "campaign_id": campaign_id, "scope_type": "company", "scope_id": "1",
            "seq_id": "1", "is_default": "0", "audience": "returning",
            "criteria": json.dumps([{"type": "is_returning", "op": "in", "values": ["true"]}]),
            "action_type": "offer",
            "action_config": json.dumps({"offer_id": 1, "target_id": 7}),
        }
        redis = FakeRedis(
            sets={"geo:US": {campaign_id}, "device:mobile": {campaign_id},
                  "os:ios": {campaign_id}, "campaigns:active": {campaign_id}},
            hashes={
                f"campaign:{campaign_id}": camp, "flow:1": ret_flow,
                "offer_target:7": {"url": "https://T7/{click_id}",
                                   "availability": "active", "offer_id": "1"},
                "offer_target:9": {"url": "https://T9/{click_id}",
                                   "availability": "active", "offer_id": "1"},
            },
            lists={f"campaign:{campaign_id}:flows": ["1"]},
            strings={"sticky:1:U:10": "9"},  # stale pin → target 9 (MUST be ignored)
        )
        result = self._run(redis, is_unique=False)  # seen_before → returning pool
        assert "T7" in result["url"]  # returning flow's OWN pick, NOT the pin
        assert "T9" not in result["url"]
        assert result["attribution"]["sticky_status"] == "na"  # sticky suppressed
        assert result["attribution"]["audience_pool"] == "returning"

    def test_routing_off_byte_identical(self):
        redis = self._redis(offer_targets={
            "7": {"url": "https://T7/{click_id}", "availability": "active", "offer_id": "1"},
        })
        result = self._run(redis, is_unique=False, routing=False)
        assert "T7" in result["url"]
        assert result["attribution"]["sticky_status"] == "na"
        assert redis.strings.get("sticky:1:U:10") is None

    def test_fail_open_on_sticky_redis_error(self):
        # Identity/sticky Redis errors ⇒ click still routes (lose no click).
        class _Boom:
            async def get(self, *a, **k):
                raise RuntimeError("identity redis down")
            async def set(self, *a, **k):
                raise RuntimeError("identity redis down")
            async def expire(self, *a, **k):
                raise RuntimeError("identity redis down")
        redis = self._redis(offer_targets={
            "7": {"url": "https://T7/{click_id}", "availability": "active", "offer_id": "1"},
        })
        result = self._run(redis, is_unique=False, identity_redis=_Boom())
        assert result is not None and "T7" in result["url"]  # routed despite error
        # status reflects the lookup miss (get failed → None → miss); no crash.
        assert result["attribution"]["sticky_status"] == "miss"


# ============================================================
# v2 LD-F2 — X-Test-Id heavy/light gating wired through route():
# get_test_id() (bound by the /decide middleware from a VALID X-Test-Id)
# flips the cascade trace to Mode-B. Pins the audit-2 MED remediation
# end-to-end (router → cascade → attribution.routing_trace).
# ============================================================


class TestLDF2TraceGatingE2E:
    CAMPAIGN = "70"

    def _redis(self) -> FakeRedis:
        # Winner flow 700 (geo in [US]) + 5 rejected flows (geo in [CA]) so the
        # compact cap (3) vs diagnostic (uncapped) difference is observable.
        hashes = {
            f"campaign:{self.CAMPAIGN}": {"company_id": "1", "priority": "0"},
            "flow:700": {
                "campaign_id": self.CAMPAIGN, "scope_type": "company",
                "scope_id": "1", "seq_id": "1", "is_default": "0",
                "criteria": json.dumps([{"type": "geo", "op": "in", "values": ["US"]}]),
                "action_type": "redirect",
                "action_config": json.dumps({"url": "https://lp/{click_id}"}),
            },
        }
        flow_ids = ["700"]
        for n in range(701, 706):
            hashes[f"flow:{n}"] = {
                "campaign_id": self.CAMPAIGN, "scope_type": "company",
                "scope_id": "1", "seq_id": str(n), "is_default": "0",
                "criteria": json.dumps([{"type": "geo", "op": "in", "values": ["CA"]}]),
                "action_type": "redirect",
                "action_config": json.dumps({"url": "https://no/{click_id}"}),
            }
            flow_ids.append(str(n))
        return FakeRedis(
            sets={
                "geo:US": {self.CAMPAIGN}, "device:mobile": {self.CAMPAIGN},
                "os:ios": {self.CAMPAIGN}, "campaigns:active": {self.CAMPAIGN},
            },
            hashes=hashes,
            lists={f"campaign:{self.CAMPAIGN}:flows": flow_ids},
        )

    def _trace(self, result: dict) -> dict:
        return result["attribution"]["routing_trace"]

    def test_no_test_id_light_trace(self):
        """No X-Test-Id bound → compact trace: criteria present, rejected
        capped at 3, no per-flow criteria detail."""
        from app.diag import set_test_id

        set_test_id("")  # explicit: no test id bound
        try:
            result = _route_with(self._redis(), _click())
        finally:
            set_test_id("")  # tear down for the next test
        assert result is not None
        crit = self._trace(result)["criteria"]
        assert crit["winner_matched"] == ["geo in [US]"]
        assert len(crit["rejected"]) == 3
        assert crit["rejected_truncated"] == 2
        assert all("criteria" not in e for e in crit["rejected"])

    def test_valid_test_id_heavy_trace(self):
        """A VALID X-Test-Id bound → Mode-B trace: rejected uncapped + each
        rejected entry carries full criteria descriptors."""
        from app.diag import set_test_id

        set_test_id("11111111-2222-3333-4444-555555555555")
        try:
            result = _route_with(self._redis(), _click())
        finally:
            set_test_id("")  # tear down so other tests see no test id
        assert result is not None
        crit = self._trace(result)["criteria"]
        assert len(crit["rejected"]) == 5  # cap lifted
        assert "rejected_truncated" not in crit
        assert crit["rejected"][0]["criteria"] == ["geo in [CA]"]


class TestF9DisabledDomainFailClosed:
    """F9 (2026-06-19): a click on an ARCHIVED campaign-router base (a member of
    `domains:disabled`) fails CLOSED (404). The lingering CF Worker route still
    delivers the click to the edge, but it must NOT fall through to geo
    targeting and broad-eval match a FOREIGN campaign. Scoped to the exact host."""

    def test_disabled_apex_click_blocks_even_when_a_foreign_campaign_would_match(self):
        # An active campaign WOULD match this click by geo if we fell through —
        # the block must win, proving the archived host leaks to nobody.
        redis = FakeRedis(
            sets={
                "domains:disabled": {"archived.xyz"},
                "campaigns:active": {"99"},
                "geo:US": {"99"},
            },
            hashes={"campaign:99": {"name": "foreign"}},
        )
        req = ClickRequest(
            click_id="f9-apex", country="US", user_agent="Mozilla/5.0",
            hostname="archived.xyz", path="/",
        )
        result = _route_with(redis, req)
        assert result is not None
        assert result.get("blocked") is True
        assert result.get("url") is None
        assert result.get("campaign_id") is None

    def test_disabled_subdomain_click_is_blocked(self):
        redis = FakeRedis(sets={"domains:disabled": {"archived.xyz"}})
        req = ClickRequest(
            click_id="f9-sub", country="US", user_agent="Mozilla/5.0",
            hostname="go.archived.xyz", path="/",
        )
        result = _route_with(redis, req)
        assert (result or {}).get("blocked") is True

    def test_non_disabled_host_is_not_blocked(self):
        # Control: a host NOT in domains:disabled falls through (no 404). Other
        # domains of the same campaign are untouched by another domain's archive.
        redis = FakeRedis(sets={"domains:disabled": {"archived.xyz"}})
        req = ClickRequest(
            click_id="f9-ctrl", country="US", user_agent="Mozilla/5.0",
            hostname="live.xyz", path="/",
        )
        result = _route_with(redis, req)
        assert not (result or {}).get("blocked")


# ============================================================
# GTD-R135 Phase 3 (G4) + Phase 4 (G5) — structural + identifier filters
# ============================================================
#
# End-to-end through `route()` — the only way to catch a wiring bug between
# `_resolve_buyer_chain` (int chain) / `resolve_slots` (str|None slots) and
# the matcher's string-only comparison contract; a pure `_first_failing_
# criterion` unit test would miss the router.py cast/merge step entirely.


class TestStructuralFilters:
    def test_str_cast_regression_buyer_id_filter_matches_int_chain(self):
        """Unknown 5a — `_resolve_buyer_chain` returns `buyer_id` as a Python
        `int`; the matcher compares strings. Without `str(v)` at the
        click_attrs merge point, `42 in frozenset({"42"})` is FALSE (int
        never `==` a same-digit str) — the exact CF-3 bug class reproduced
        inside the fix meant to prevent it. This is RED without the cast in
        router.py (revert the `str(_v) if _v is not None else ""` line to
        prove it), GREEN with it."""
        campaign_id = "10"
        filter_flow = "100"
        redis = FakeRedis(
            sets={
                "geo:US": {campaign_id},
                "device:mobile": {campaign_id},
                "os:ios": {campaign_id},
                "campaigns:active": {campaign_id},
            },
            hashes={
                f"campaign:{campaign_id}": {"company_id": "1", "priority": "0"},
                "source:99": {
                    "slug": "fb",
                    "param_mappings": json.dumps([
                        {"slot": "buyer_id", "alias": "buyer_id"},
                    ]),
                },
                "user:42": {
                    "id": "42", "team_id": "7", "department_id": "3",
                    "custom_group_id": "", "company_id": "1", "status": "active",
                },
                f"flow:{filter_flow}": {
                    "campaign_id": campaign_id,
                    "scope_type": "company",
                    "scope_id": "1",
                    "seq_id": "1",
                    "is_default": "0",
                    "criteria": json.dumps([
                        {"type": "buyer_id", "op": "in", "values": ["42"]},
                    ]),
                    "action_type": "redirect",
                    "action_config": json.dumps({"url": "https://buyer-filter-hit"}),
                },
            },
            lists={f"campaign:{campaign_id}:flows": [filter_flow]},
        )
        redis.sets[f"campaign:{campaign_id}:sources"] = {"99"}
        result = _route_with(redis, _click({"buyer_id": "42", "source": "fb"}))
        assert result is not None
        assert "buyer-filter-hit" in result["url"]

    def test_non_matching_buyer_falls_through_to_default(self):
        """The dual of the str-cast test — a DIFFERENT buyer (99) must NOT
        match the `buyer_id in [42]` filter; the click falls through to the
        company catch-all. Proves the filter genuinely discriminates, not a
        vacuous always-match."""
        campaign_id = "10"
        filter_flow = "100"
        default_flow = "101"
        redis = FakeRedis(
            sets={
                "geo:US": {campaign_id},
                "device:mobile": {campaign_id},
                "os:ios": {campaign_id},
                "campaigns:active": {campaign_id},
            },
            hashes={
                f"campaign:{campaign_id}": {"company_id": "1", "priority": "0"},
                "source:99": {
                    "slug": "fb",
                    "param_mappings": json.dumps([
                        {"slot": "buyer_id", "alias": "buyer_id"},
                    ]),
                },
                "user:99": {
                    "id": "99", "team_id": "7", "department_id": "3",
                    "custom_group_id": "", "company_id": "1", "status": "active",
                },
                f"flow:{filter_flow}": {
                    "campaign_id": campaign_id,
                    "scope_type": "company",
                    "scope_id": "1",
                    "seq_id": "1",
                    "is_default": "0",
                    "criteria": json.dumps([
                        {"type": "buyer_id", "op": "in", "values": ["42"]},
                    ]),
                    "action_type": "redirect",
                    "action_config": json.dumps({"url": "https://buyer-filter-hit"}),
                },
                f"flow:{default_flow}": {
                    "campaign_id": campaign_id,
                    "scope_type": "company",
                    "scope_id": "1",
                    "seq_id": "5",
                    "is_default": "1",
                    "criteria": "[]",
                    "action_type": "redirect",
                    "action_config": json.dumps({"url": "https://catchall"}),
                },
            },
            lists={f"campaign:{campaign_id}:flows": [filter_flow, default_flow]},
        )
        redis.sets[f"campaign:{campaign_id}:sources"] = {"99"}
        result = _route_with(redis, _click({"buyer_id": "99", "source": "fb"}))
        assert result is not None
        assert "catchall" in result["url"]

    def test_unresolved_buyer_chain_fails_closed_not_crashes(self):
        """No `buyer_id` in the click at all → `_resolve_buyer_chain` returns
        an empty chain (`buyer_id=None`) → the router's `str(_v) if _v is not
        None else ""` cast maps it to `""`, which never matches any saved
        (non-empty) criterion value — fails closed, falls through, does NOT
        crash on a None→str comparison."""
        campaign_id = "10"
        filter_flow = "100"
        default_flow = "101"
        redis = FakeRedis(
            sets={
                "geo:US": {campaign_id},
                "device:mobile": {campaign_id},
                "os:ios": {campaign_id},
                "campaigns:active": {campaign_id},
            },
            hashes={
                f"campaign:{campaign_id}": {"company_id": "1", "priority": "0"},
                f"flow:{filter_flow}": {
                    "campaign_id": campaign_id,
                    "scope_type": "company",
                    "scope_id": "1",
                    "seq_id": "1",
                    "is_default": "0",
                    "criteria": json.dumps([
                        {"type": "buyer_id", "op": "in", "values": ["42"]},
                    ]),
                    "action_type": "redirect",
                    "action_config": json.dumps({"url": "https://buyer-filter-hit"}),
                },
                f"flow:{default_flow}": {
                    "campaign_id": campaign_id,
                    "scope_type": "company",
                    "scope_id": "1",
                    "seq_id": "5",
                    "is_default": "1",
                    "criteria": "[]",
                    "action_type": "redirect",
                    "action_config": json.dumps({"url": "https://catchall"}),
                },
            },
            lists={f"campaign:{campaign_id}:flows": [filter_flow, default_flow]},
        )
        result = _route_with(redis, _click())  # no buyer_id/source at all
        assert result is not None
        assert "catchall" in result["url"]


class TestIdentifierFilters:
    def test_byte_exact_identifier_filter_matches(self):
        """`creative_id` is a canonical RESERVED slot — it auto-binds from a
        same-named GET key with NO mapping needed. A `param:creative_id`
        criterion matches a click carrying the exact wire value."""
        campaign_id = "10"
        filter_flow = "100"
        redis = FakeRedis(
            sets={
                "geo:US": {campaign_id},
                "device:mobile": {campaign_id},
                "os:ios": {campaign_id},
                "campaigns:active": {campaign_id},
            },
            hashes={
                f"campaign:{campaign_id}": {"company_id": "1", "priority": "0"},
                f"flow:{filter_flow}": {
                    "campaign_id": campaign_id,
                    "scope_type": "company",
                    "scope_id": "1",
                    "seq_id": "1",
                    "is_default": "0",
                    "criteria": json.dumps([
                        {"type": "param:creative_id", "op": "in", "values": ["AdVariant_A"]},
                    ]),
                    "action_type": "redirect",
                    "action_config": json.dumps({"url": "https://creative-hit"}),
                },
            },
            lists={f"campaign:{campaign_id}:flows": [filter_flow]},
        )
        result = _route_with(redis, _click({"creative_id": "AdVariant_A"}))
        assert result is not None
        assert "creative-hit" in result["url"]

    def test_byte_exact_identifier_filter_rejects_case_mismatch(self):
        """The sacred rule — wire-format byte-exact. A saved 'AdVariant_A'
        must NOT match a differently-cased click value; identifier dims are
        case-preserve (like geo/region/browser/language), NOT lowercased."""
        campaign_id = "10"
        filter_flow = "100"
        default_flow = "101"
        redis = FakeRedis(
            sets={
                "geo:US": {campaign_id},
                "device:mobile": {campaign_id},
                "os:ios": {campaign_id},
                "campaigns:active": {campaign_id},
            },
            hashes={
                f"campaign:{campaign_id}": {"company_id": "1", "priority": "0"},
                f"flow:{filter_flow}": {
                    "campaign_id": campaign_id,
                    "scope_type": "company",
                    "scope_id": "1",
                    "seq_id": "1",
                    "is_default": "0",
                    "criteria": json.dumps([
                        {"type": "param:creative_id", "op": "in", "values": ["AdVariant_A"]},
                    ]),
                    "action_type": "redirect",
                    "action_config": json.dumps({"url": "https://creative-hit"}),
                },
                f"flow:{default_flow}": {
                    "campaign_id": campaign_id,
                    "scope_type": "company",
                    "scope_id": "1",
                    "seq_id": "5",
                    "is_default": "1",
                    "criteria": "[]",
                    "action_type": "redirect",
                    "action_config": json.dumps({"url": "https://catchall"}),
                },
            },
            lists={f"campaign:{campaign_id}:flows": [filter_flow, default_flow]},
        )
        result = _route_with(redis, _click({"creative_id": "advariant_a"}))
        assert result is not None
        assert "catchall" in result["url"]

    def test_none_slot_regression_explicitly_mapped_unresolved_fails_closed(self):
        """Unknown 6 — `resolve_slots` returns `dict[str, str | None]`. When a
        slot is EXPLICITLY mapped (source `param_mappings` entry) but the
        click carries no value for it AND no default_value is configured,
        `slots["creative_id"]` is `None` (present, not absent). The router's
        `.get(slot) or ""` (NOT `.get(slot, "")`) must map this to `""` —
        proven here by a full route() call that must NOT crash and must
        fail closed (falls through to the catch-all), never silently
        matching an `in` criterion on a stray `None`."""
        campaign_id = "10"
        filter_flow = "100"
        default_flow = "101"
        redis = FakeRedis(
            sets={
                "geo:US": {campaign_id},
                "device:mobile": {campaign_id},
                "os:ios": {campaign_id},
                "campaigns:active": {campaign_id},
            },
            hashes={
                f"campaign:{campaign_id}": {"company_id": "1", "priority": "0"},
                # Explicitly maps creative_id from a NON-canonical alias, with
                # no default_value — the click below never supplies "cr", so
                # `slots["creative_id"]` resolves to None (explicitly-mapped,
                # unresolved), NOT omitted from the dict.
                "source:99": {
                    "slug": "fb",
                    "param_mappings": json.dumps([
                        {"slot": "creative_id", "alias": "cr"},
                    ]),
                },
                f"flow:{filter_flow}": {
                    "campaign_id": campaign_id,
                    "scope_type": "company",
                    "scope_id": "1",
                    "seq_id": "1",
                    "is_default": "0",
                    "criteria": json.dumps([
                        {"type": "param:creative_id", "op": "in", "values": ["AdVariant_A"]},
                    ]),
                    "action_type": "redirect",
                    "action_config": json.dumps({"url": "https://creative-hit"}),
                },
                f"flow:{default_flow}": {
                    "campaign_id": campaign_id,
                    "scope_type": "company",
                    "scope_id": "1",
                    "seq_id": "5",
                    "is_default": "1",
                    "criteria": "[]",
                    "action_type": "redirect",
                    "action_config": json.dumps({"url": "https://catchall"}),
                },
            },
            lists={f"campaign:{campaign_id}:flows": [filter_flow, default_flow]},
        )
        redis.sets[f"campaign:{campaign_id}:sources"] = {"99"}
        # source=fb selects the mapping; NO "cr" param → creative_id
        # explicitly-mapped-but-unresolved (None).
        result = _route_with(redis, _click({"source": "fb"}))
        assert result is not None
        assert "catchall" in result["url"]

    def test_none_slot_cast_produces_string_not_none_in_click_attrs(self):
        """Honest companion to the test above — in THIS matcher, a stray
        `None` and `""` happen to produce the SAME match verdict (`None in
        frozenset({...})` is False, same as `"" in frozenset({...})`), so
        the previous test's route()-level assertion can't actually
        DISCRIMINATE the fixed cast from the naive `.get(slot, "")` bug (both
        pass). This test closes that gap by spying on the exact
        `cascade.resolve_flow` call and asserting the click_attrs VALUE TYPE
        directly — `.get(slot) or ""` must produce a `str`, never a `None`,
        regardless of whether today's evaluator happens to tolerate it. This
        is the real regression guard: a future evaluator change (e.g. a
        dim-specific `.lower()`/`.split()` applied unconditionally) would
        crash on a `None` that slipped through — this test fails FIRST,
        at the type boundary, before that can ever happen."""
        campaign_id = "10"
        filter_flow = "100"
        redis = FakeRedis(
            sets={
                "geo:US": {campaign_id},
                "device:mobile": {campaign_id},
                "os:ios": {campaign_id},
                "campaigns:active": {campaign_id},
            },
            hashes={
                f"campaign:{campaign_id}": {"company_id": "1", "priority": "0"},
                "source:99": {
                    "slug": "fb",
                    "param_mappings": json.dumps([
                        {"slot": "creative_id", "alias": "cr"},
                    ]),
                },
                f"flow:{filter_flow}": {
                    "campaign_id": campaign_id,
                    "scope_type": "company",
                    "scope_id": "1",
                    "seq_id": "1",
                    "is_default": "0",
                    "criteria": "[]",
                    "action_type": "redirect",
                    "action_config": json.dumps({"url": "https://hit"}),
                },
            },
            lists={f"campaign:{campaign_id}:flows": [filter_flow]},
        )
        redis.sets[f"campaign:{campaign_id}:sources"] = {"99"}

        captured: dict = {}
        real_resolve_flow = cascade.resolve_flow

        async def _spy(*args, **kwargs):
            captured["click_attrs"] = kwargs.get("click_attrs")
            return await real_resolve_flow(*args, **kwargs)

        with patch.object(cascade, "resolve_flow", _spy):
            result = _route_with(redis, _click({"source": "fb"}))

        assert result is not None
        assert "click_attrs" in captured
        assert captured["click_attrs"]["param:creative_id"] == ""
        assert isinstance(captured["click_attrs"]["param:creative_id"], str)
        # Every identifier + structural dim must be a str — never None —
        # regardless of whether the underlying source resolved a value.
        for slot in cascade.IDENTIFIER_SLOTS:
            assert isinstance(captured["click_attrs"][f"param:{slot}"], str)
        for dim in cascade.STRUCTURAL_CRITERION_DIMS:
            assert isinstance(captured["click_attrs"][dim], str)

    def test_structural_and_identifier_combined_in_one_flow(self):
        """Both new dim families on ONE flow, both must hold (AND semantics)
        — proves they compose correctly through the same click_attrs dict."""
        campaign_id = "10"
        filter_flow = "100"
        redis = FakeRedis(
            sets={
                "geo:US": {campaign_id},
                "device:mobile": {campaign_id},
                "os:ios": {campaign_id},
                "campaigns:active": {campaign_id},
            },
            hashes={
                f"campaign:{campaign_id}": {"company_id": "1", "priority": "0"},
                "source:99": {
                    "slug": "fb",
                    "param_mappings": json.dumps([
                        {"slot": "buyer_id", "alias": "buyer_id"},
                    ]),
                },
                "user:42": {
                    "id": "42", "team_id": "7", "department_id": "3",
                    "custom_group_id": "", "company_id": "1", "status": "active",
                },
                f"flow:{filter_flow}": {
                    "campaign_id": campaign_id,
                    "scope_type": "company",
                    "scope_id": "1",
                    "seq_id": "1",
                    "is_default": "0",
                    "criteria": json.dumps([
                        {"type": "buyer_id", "op": "in", "values": ["42"]},
                        {"type": "param:creative_id", "op": "in", "values": ["AdVariant_A"]},
                    ]),
                    "action_type": "redirect",
                    "action_config": json.dumps({"url": "https://combo-hit"}),
                },
            },
            lists={f"campaign:{campaign_id}:flows": [filter_flow]},
        )
        redis.sets[f"campaign:{campaign_id}:sources"] = {"99"}
        result = _route_with(
            redis,
            _click({"buyer_id": "42", "source": "fb", "creative_id": "AdVariant_A"}),
        )
        assert result is not None
        assert "combo-hit" in result["url"]

    def test_param_source_mixed_case_wire_value_matches_byte_exact(self):
        """FIX (post-merge adversarial review) — `param:source` is a
        canonical RESERVED slot: `resolve_slots` auto-binds it from the
        raw `?source=` query param VERBATIM (no `.lower()` on that path),
        even though the SEPARATE source-MATCHING lookup (which selects
        WHICH `source:{id}` record applies) lowers its own local copy for
        slug comparison only — that lowering never touches `slots`. A
        mixed-case wire value ("FacebookAds") must byte-match a
        criterion saved with the SAME casing (case-preserve, like geo/
        region/browser/language), and must NOT match a differently-cased
        one — proving both the source-matching lookup (case-insensitive,
        unaffected) and the `param:source` filter (case-sensitive) work
        correctly side-by-side on the same click."""
        campaign_id = "10"
        hit_flow = "100"
        default_flow = "101"
        redis = FakeRedis(
            sets={
                "geo:US": {campaign_id},
                "device:mobile": {campaign_id},
                "os:ios": {campaign_id},
                "campaigns:active": {campaign_id},
            },
            hashes={
                f"campaign:{campaign_id}": {"company_id": "1", "priority": "0"},
                # Source slug is lowercase ("facebookads") — source-MATCHING
                # is case-insensitive, so `?source=FacebookAds` still finds
                # this source record. `param:source` filtering is separate.
                "source:99": {"slug": "facebookads"},
                f"flow:{hit_flow}": {
                    "campaign_id": campaign_id,
                    "scope_type": "company",
                    "scope_id": "1",
                    "seq_id": "1",
                    "is_default": "0",
                    "criteria": json.dumps([
                        {"type": "param:source", "op": "in", "values": ["FacebookAds"]},
                    ]),
                    "action_type": "redirect",
                    "action_config": json.dumps({"url": "https://source-hit"}),
                },
                f"flow:{default_flow}": {
                    "campaign_id": campaign_id,
                    "scope_type": "company",
                    "scope_id": "1",
                    "seq_id": "5",
                    "is_default": "1",
                    "criteria": "[]",
                    "action_type": "redirect",
                    "action_config": json.dumps({"url": "https://catchall"}),
                },
            },
            lists={f"campaign:{campaign_id}:flows": [hit_flow, default_flow]},
        )
        redis.sets[f"campaign:{campaign_id}:sources"] = {"99"}

        # Wire value matches the saved criterion's casing exactly -> hit.
        result = _route_with(redis, _click({"source": "FacebookAds"}))
        assert result is not None
        assert "source-hit" in result["url"]

        # Source-MATCHING still resolves this source case-insensitively
        # (lowercase wire value), but `param:source`'s saved value is
        # "FacebookAds" -> byte-exact mismatch -> falls through.
        result2 = _route_with(redis, _click({"source": "facebookads"}))
        assert result2 is not None
        assert "catchall" in result2["url"]
