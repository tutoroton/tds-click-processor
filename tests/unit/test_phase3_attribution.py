"""Stage 3 · 03 — enrichment & column population (producer side).

Two concerns:
  1. `_phase3_attribution_fields` (main.py) — the click_record column
     additions: org chain (company_id campaign-anchored, NEVER buyer),
     routing ids, reserved slots, infra columns.
  2. HIGH-001 — `enrich_buyer` tenant-scoped Redis key: the same
     `buyer_id` in two tenants resolves DISTINCTLY; legacy fallback
     works during the migration overlap.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.enrichment import enrich_buyer
from app.main import (
    _CLICK_SCHEMA_VERSION,
    _RESERVED_SLOT_COLUMNS,
    _phase3_attribution_fields,
)
from app.models import ClickRequest
from app.router import parse_accept_language


def _keyed_redis(store: dict[str, dict]) -> MagicMock:
    """Mock Redis whose hgetall returns the per-KEY hash (key-aware)."""
    redis = MagicMock()

    async def _hgetall(key):
        return store.get(key, {})

    redis.hgetall = AsyncMock(side_effect=_hgetall)
    return redis


# ============================================================
# _phase3_attribution_fields — click_record column additions
# ============================================================


class TestPhase3AttributionFields:
    def _result(self):
        return {
            "attribution": {
                "buyer_id": 7, "team_id": 3, "department_id": 2,
                "custom_group_id": 9, "company_id": 42,
                "source_id": 5, "flow_id": 11,
                "offer_target_id": 4, "traffic_target_id": 8,
                "slots": {
                    "source": "fb", "ad_id": "a1", "app_id": "com.x",
                    "sub9": "x9", "sub20": "x20", "buyer_id": "7",
                },
            }
        }

    def test_org_chain_and_routing_ids(self):
        f = _phase3_attribution_fields(
            self._result(),
            ClickRequest(click_id="c1"),
            {"result": "matched"},
        )
        assert f["company_id"] == 42      # campaign-anchored
        assert f["buyer_id"] == 7
        assert f["team_id"] == 3
        assert f["department_id"] == 2
        assert f["custom_group_id"] == 9
        assert f["source_id"] == 5
        assert f["flow_id"] == 11
        assert f["offer_target_id"] == 4
        assert f["traffic_target_id"] == 8
        assert f["routing_result"] == "matched"

    def test_reserved_slots_stamped_from_resolved_slots(self):
        f = _phase3_attribution_fields(
            self._result(), ClickRequest(click_id="c1"), {},
        )
        assert f["source"] == "fb"
        assert f["ad_id"] == "a1"
        assert f["app_id"] == "com.x"
        # A reserved slot the click never carried → None (CH default '').
        assert f["keyword"] is None

    def test_buyer_id_is_not_a_reserved_string_column(self):
        # buyer_id resolves to the org-chain INT column, never a reserved
        # free-text column — guards against double-stamping.
        assert "buyer_id" not in _RESERVED_SLOT_COLUMNS

    def test_sub9_to_20_from_slots_sub1_8_untouched(self):
        f = _phase3_attribution_fields(
            self._result(), ClickRequest(click_id="c1"), {},
        )
        assert f["sub9"] == "x9"
        assert f["sub20"] == "x20"
        # sub1..8 are NOT emitted by this helper (kept as the handler's
        # legacy hardcoded mapping for live-PG continuity).
        assert "sub1" not in f
        assert "sub8" not in f

    def test_infra_and_cost_from_request(self):
        req = ClickRequest(
            click_id="c1", colo="FRA", tls_version="TLSv1.3",
            http_protocol="HTTP/2", hostname="lp.x.test", path="/go",
            accept_language="en-US", query_params={"cost": "0.5"},
        )
        f = _phase3_attribution_fields({"attribution": {}}, req, {})
        assert f["worker_colo"] == "FRA"
        assert f["tls_version"] == "TLSv1.3"
        assert f["http_protocol"] == "HTTP/2"
        assert f["hostname"] == "lp.x.test"
        assert f["path"] == "/go"
        assert f["language"] == parse_accept_language("en-US")
        assert f["cost"] == "0.5"
        assert f["routing_decision_ts"]  # ISO string set at record build

    def test_fallback_path_empty_attribution_no_crash(self):
        # route() returned None → handler builds a synthetic result with
        # NO attribution. Helper must not crash; org/routing fields NULL.
        f = _phase3_attribution_fields({}, ClickRequest(click_id="c1"), {})
        assert f["company_id"] is None
        assert f["buyer_id"] is None
        assert f["source"] is None
        assert f["routing_result"] == ""
        assert f["cost"] == 0

    def test_schema_version_constant_is_2(self):
        assert _CLICK_SCHEMA_VERSION == 2


# ============================================================
# HIGH-001 — tenant-scoped Redis key (cross-tenant prevention)
# ============================================================


class TestHigh001TenantScopedKey:
    @pytest.mark.asyncio
    async def test_scoped_key_preferred(self):
        store = {
            "user:7:42": {
                "id": "42", "company_id": "7", "team_id": "3",
                "department_id": "", "custom_group_id": "", "status": "active",
            },
        }
        redis = _keyed_redis(store)
        result = await enrich_buyer(redis, "42", company_id=7)
        assert result["company_id"] == "7"
        assert result["team_id"] == "3"
        # The scoped key was tried (and hit) FIRST.
        redis.hgetall.assert_awaited_with("user:7:42")

    @pytest.mark.asyncio
    async def test_same_buyer_id_two_tenants_resolves_distinctly(self):
        # The crux of HIGH-001: buyer_id 42 exists in tenant 7 AND tenant 9.
        store = {
            "user:7:42": {"id": "42", "company_id": "7", "team_id": "70",
                          "department_id": "", "custom_group_id": "",
                          "status": "active"},
            "user:9:42": {"id": "42", "company_id": "9", "team_id": "90",
                          "department_id": "", "custom_group_id": "",
                          "status": "active"},
        }
        redis = _keyed_redis(store)
        as_7 = await enrich_buyer(redis, "42", company_id=7)
        as_9 = await enrich_buyer(redis, "42", company_id=9)
        assert as_7["company_id"] == "7"
        assert as_7["team_id"] == "70"
        assert as_9["company_id"] == "9"
        assert as_9["team_id"] == "90"

    @pytest.mark.asyncio
    async def test_legacy_global_key_fallback_during_overlap(self):
        # Builder not yet updated: only the legacy `user:42` key exists.
        # Consumer falls back so routing/attribution keep working.
        store = {
            "user:42": {"id": "42", "company_id": "7", "team_id": "3",
                        "department_id": "", "custom_group_id": "",
                        "status": "active"},
        }
        redis = _keyed_redis(store)
        result = await enrich_buyer(redis, "42", company_id=7)
        assert result["company_id"] == "7"

    @pytest.mark.asyncio
    async def test_other_tenant_buyer_does_not_resolve_when_isolated(self):
        # Post-cleanup state (legacy key retired): only the OTHER tenant's
        # scoped key exists. A campaign in tenant 7 must NOT resolve a
        # buyer that belongs to tenant 9 — structural prevention.
        store = {
            "user:9:42": {"id": "42", "company_id": "9", "team_id": "90",
                          "status": "active"},
        }
        redis = _keyed_redis(store)
        result = await enrich_buyer(redis, "42", company_id=7)
        assert result["company_id"] is None  # EMPTY_ENRICHMENT

    @pytest.mark.asyncio
    async def test_no_company_id_uses_legacy_key(self):
        # Backward-compat: callers/tests that omit company_id resolve the
        # global key exactly as before HIGH-001.
        store = {
            "user:42": {"id": "42", "company_id": "7", "status": "active"},
        }
        redis = _keyed_redis(store)
        result = await enrich_buyer(redis, "42")
        assert result["company_id"] == "7"
        redis.hgetall.assert_awaited_with("user:42")
