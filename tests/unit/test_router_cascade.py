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

from app import router
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

    def smembers(self, key):
        self.ops.append(("smembers", key))

    def get(self, key):
        self.ops.append(("get", key))

    def lrange(self, key, _start, _end):
        self.ops.append(("lrange", key))

    def exists(self, key):
        self.ops.append(("exists", key))

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
