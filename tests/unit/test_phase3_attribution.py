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

from app.enrichment import EMPTY_ENRICHMENT, enrich_buyer
from app.main import (
    _CLICK_SCHEMA_VERSION,
    _RESERVED_SLOT_COLUMNS,
    _phase3_attribution_fields,
    _utc_now_ms_iso,
)
from app.models import ClickRequest
from app.parameters import RESERVED_SLOTS
from app.router import parse_accept_language

# A fixed routing_decision_ts the caller would pass in (helper is now pure).
_RDT = "2026-06-01T10:00:00.250Z"


def _keyed_redis(store: dict[str, dict]) -> MagicMock:
    """Mock Redis whose hgetall returns the per-KEY hash (key-aware)."""
    redis = MagicMock()

    async def _hgetall(key):
        return store.get(key, {})

    redis.hgetall = AsyncMock(side_effect=_hgetall)
    return redis


def _raising_redis(exc: Exception) -> MagicMock:
    redis = MagicMock()
    redis.hgetall = AsyncMock(side_effect=exc)
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
                "source_id": 5, "flow_id": 11, "flow_version_id": 33,
                "offer_target_id": 4, "traffic_target_id": 8,
                "slots": {
                    "source": "fb", "ad_id": "a1", "app_id": "com.x",
                    "sub9": "x9", "sub20": "x20", "buyer_id": "7",
                },
            }
        }

    def test_org_chain_and_routing_ids(self):
        f = _phase3_attribution_fields(
            self._result(), ClickRequest(click_id="c1"),
            {"result": "matched"}, _RDT,
        )
        assert f["company_id"] == 42      # campaign-anchored
        assert f["buyer_id"] == 7
        assert f["team_id"] == 3
        assert f["department_id"] == 2
        assert f["custom_group_id"] == 9
        assert f["source_id"] == 5
        assert f["flow_id"] == 11
        assert f["flow_version_id"] == 33    # S1 — flow's current version
        assert f["offer_target_id"] == 4
        assert f["traffic_target_id"] == 8
        assert f["routing_result"] == "matched"

    def test_company_id_is_campaign_anchor_never_buyer(self):
        # FIX-2 (rule multi-tenant-isolation "Test discipline") — pin the
        # invariant at the STAMPING site, not only the CH mapper. The
        # attribution chain's company_id (campaign anchor, 42) is used
        # verbatim even though the buyer enrichment would have resolved a
        # DIFFERENT tenant (company 9) — _resolve_buyer_chain already
        # forced company_id to the campaign, and this helper must not
        # second-guess it from any buyer field.
        result = {
            "attribution": {
                "company_id": 42,            # campaign anchor (wins)
                "buyer_id": 999,             # a buyer that lives in tenant 9
                "team_id": 90, "department_id": 91, "custom_group_id": 92,
                "slots": {"buyer_id": "999"},
            }
        }
        f = _phase3_attribution_fields(
            result, ClickRequest(click_id="c1"), {}, _RDT)
        assert f["company_id"] == 42        # NEVER the buyer's tenant
        assert f["buyer_id"] == 999         # buyer id itself still recorded

    def test_reserved_slots_stamped_from_resolved_slots(self):
        f = _phase3_attribution_fields(
            self._result(), ClickRequest(click_id="c1"), {}, _RDT)
        assert f["source"] == "fb"
        assert f["ad_id"] == "a1"
        assert f["app_id"] == "com.x"
        # A reserved slot the click never carried → None (CH default '').
        assert f["keyword"] is None

    def test_all_18_reserved_slot_columns_stamped(self):
        # FIX-6 — full loop (symmetry with the collector mapper test), not a
        # 3/18 spot check. buyer_id is the ONLY reserved slot that is NOT a
        # column here (it's the org-chain int).
        slots = {c: f"v-{c}" for c in _RESERVED_SLOT_COLUMNS}
        f = _phase3_attribution_fields(
            {"attribution": {"slots": slots}}, ClickRequest(click_id="c1"),
            {}, _RDT)
        for c in _RESERVED_SLOT_COLUMNS:
            assert f[c] == f"v-{c}", c
        # The column set is exactly RESERVED_SLOTS minus buyer_id.
        assert set(_RESERVED_SLOT_COLUMNS) == (RESERVED_SLOTS - {"buyer_id"})

    def test_buyer_id_is_not_a_reserved_string_column(self):
        assert "buyer_id" not in _RESERVED_SLOT_COLUMNS

    def test_all_subs_9_to_20_from_slots_sub1_8_untouched(self):
        # FIX-6 — loop the FULL sub9..20 range (catches off-by-one), not
        # just sub9/sub20.
        slots = {f"sub{i}": f"s{i}" for i in range(9, 21)}
        f = _phase3_attribution_fields(
            {"attribution": {"slots": slots}}, ClickRequest(click_id="c1"),
            {}, _RDT)
        for i in range(9, 21):
            assert f[f"sub{i}"] == f"s{i}", i
        for i in range(1, 9):
            assert f"sub{i}" not in f  # legacy hardcoded mapping owns 1..8

    def test_infra_and_cost_from_request(self):
        req = ClickRequest(
            click_id="c1", colo="FRA", tls_version="TLSv1.3",
            http_protocol="HTTP/2", hostname="lp.x.test", path="/go",
            accept_language="en-US", query_params={"cost": "0.5"},
        )
        f = _phase3_attribution_fields({"attribution": {}}, req, {}, _RDT)
        assert f["worker_colo"] == "FRA"
        assert f["tls_version"] == "TLSv1.3"
        assert f["http_protocol"] == "HTTP/2"
        assert f["hostname"] == "lp.x.test"
        assert f["path"] == "/go"
        assert f["language"] == parse_accept_language("en-US")
        assert f["cost"] == "0.5"
        # FIX-1 — exact passed value (helper is pure; no internal now()).
        assert f["routing_decision_ts"] == _RDT

    def test_fallback_path_empty_attribution_no_crash(self):
        # route() returned None → handler builds a synthetic result with
        # NO attribution. Helper must not crash; org/routing fields NULL.
        f = _phase3_attribution_fields({}, ClickRequest(click_id="c1"), {}, _RDT)
        assert f["company_id"] is None
        assert f["buyer_id"] is None
        assert f["flow_version_id"] is None   # S1 — absent → CH default 0
        assert f["source"] is None
        assert f["routing_result"] == ""
        assert f["cost"] == 0

    def test_schema_version_constant(self):
        # v3 — Phase 4 S1 added flow_version_id to the producer contract.
        assert _CLICK_SCHEMA_VERSION == 3

    def test_utc_now_ms_iso_format(self):
        # FIX-1 — millisecond precision (3 digits), trailing Z, NOT the
        # 6-digit microseconds .isoformat() produces.
        ts = _utc_now_ms_iso()
        assert ts.endswith("Z")
        ms = ts.split(".")[1][:-1]
        assert len(ms) == 3 and ms.isdigit()


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

    @pytest.mark.asyncio
    async def test_fail_open_on_redis_error(self):
        # FIX-6 — enrichment is best-effort: a Redis fault returns
        # EMPTY_ENRICHMENT (routing never blocks on attribution).
        redis = _raising_redis(ConnectionError("boom"))
        result = await enrich_buyer(redis, "42", company_id=7)
        assert result == dict(EMPTY_ENRICHMENT)

    @pytest.mark.asyncio
    async def test_non_digit_buyer_id_short_circuits_no_redis(self):
        # FIX-6 — the injection guard: a non-numeric buyer_id never reaches
        # Redis (no exotic key construction) and yields EMPTY.
        redis = _keyed_redis({"user:7:42": {"company_id": "7", "status": "active"}})
        result = await enrich_buyer(redis, "4'2; DROP", company_id=7)
        assert result == dict(EMPTY_ENRICHMENT)
        redis.hgetall.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_empty_or_nondigit_company_id_no_malformed_key(self):
        # FIX-6 — company_id "" / non-digit must NOT build "user::42" or
        # "user:abc:42" as a scoped key; it falls straight to the legacy
        # global key. (A digit-but-zero company_id is a valid scoped key.)
        store = {"user:42": {"id": "42", "company_id": "7", "status": "active"}}
        for bad_cid in ("", "abc", None):
            redis = _keyed_redis(store)
            result = await enrich_buyer(redis, "42", company_id=bad_cid)
            assert result["company_id"] == "7"           # resolved via legacy
            keys_tried = [c.args[0] for c in redis.hgetall.await_args_list]
            assert keys_tried == ["user:42"]             # no malformed scoped key

    @pytest.mark.asyncio
    async def test_zero_company_id_builds_valid_scoped_key(self):
        # FIX-6 — company_id=0 is a falsy int but a VALID digit → a real
        # scoped key "user:0:42" (not malformed), legacy as fallback.
        store = {"user:0:42": {"id": "42", "company_id": "0", "status": "active"}}
        redis = _keyed_redis(store)
        result = await enrich_buyer(redis, "42", company_id=0)
        assert result["company_id"] == "0"
        redis.hgetall.assert_any_await("user:0:42")
