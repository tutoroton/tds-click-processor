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
    _build_extra_params,
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
                "offer_target_id": 4,
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

    def test_all_subs_1_to_20_from_slots(self):
        # C-1 (2026-06-02): sub1..20 are ALL free per-Source containers,
        # populated canonically from `?subN=` (+ alias) via resolve_slots.
        # The legacy sub1..8 hardcode (sub1=?source, sub2=?creative, …) is
        # GONE — every sub column now comes from the resolved slots dict.
        slots = {f"sub{i}": f"s{i}" for i in range(1, 21)}
        f = _phase3_attribution_fields(
            {"attribution": {"slots": slots}}, ClickRequest(click_id="c1"),
            {}, _RDT)
        for i in range(1, 21):
            assert f[f"sub{i}"] == f"s{i}", i

    def test_sub1_8_no_longer_read_legacy_get_keys(self):
        # Regression pin for the user's pain ("only sub1 filled"): a click
        # carrying the OLD Keitaro keys (?creative/?buyer/?adgroup) must NOT
        # leak them into sub2/sub3/sub5 — those columns are now driven ONLY
        # by the canonical ?sub2/?sub3/?sub5 (here absent → None/CH '').
        slots = {"source": "fb"}   # ?source bound its reserved slot, not sub1
        f = _phase3_attribution_fields(
            {"attribution": {"slots": slots}}, ClickRequest(click_id="c1"),
            {}, _RDT)
        for i in range(1, 21):
            assert f[f"sub{i}"] is None, i   # no legacy hijack

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
        # A2 (audit 2026-06-03) — cost is now strict-coerced to a float
        # (was the raw string "0.5" pre-fix).
        assert f["cost"] == 0.5
        # FIX-1 — exact passed value (helper is pure; no internal now()).
        assert f["routing_decision_ts"] == _RDT

    def test_cost_non_numeric_injection_stored_as_zero(self):
        # A2: an attacker-controllable non-numeric ?cost= must NOT land
        # verbatim in the numeric cost column (would risk a collector
        # insert failure — the C1 poison-pill class) and must never be
        # reflected. Strict gate → stored 0.
        req = ClickRequest(
            click_id="c1", query_params={"cost": "abc'inj<script>"},
        )
        f = _phase3_attribution_fields({"attribution": {}}, req, {}, _RDT)
        assert f["cost"] == 0
        assert f["cost"] != "abc'inj<script>"

    def test_cost_negative_stored_as_zero(self):
        # Negative per-click cost is nonsensical → dropped to 0.
        req = ClickRequest(click_id="c1", query_params={"cost": "-5"})
        f = _phase3_attribution_fields({"attribution": {}}, req, {}, _RDT)
        assert f["cost"] == 0

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
        # v5 — Returning-users v2 added the provenance tail (uid / is_returning
        # / is_roaming / signal_tier / decision_reason / audience_pool /
        # returning_mode / sticky_status / flags_semantics_version / …) to the
        # producer contract. Lockstep with collector KNOWN_CLICK_SCHEMA_VERSION=5
        # (64af87f) — an OLD producer (v4) against the v5 consumer is tolerated
        # by the §4b skew detector (absent cols → CH defaults).
        assert _CLICK_SCHEMA_VERSION == 5

    def test_is_unique_derived_from_visitor_cookie(self):
        # S2 — is_unique is a click-processor derivation: a NEW visitor
        # (no cookie → is_returning False) is unique; a returning one isn't.
        new = _phase3_attribution_fields(
            {"attribution": {}}, ClickRequest(click_id="c1", is_returning=False),
            {}, _RDT)
        ret = _phase3_attribution_fields(
            {"attribution": {}}, ClickRequest(click_id="c1", is_returning=True),
            {}, _RDT)
        assert new["is_unique"] is True
        assert ret["is_unique"] is False

    def test_edge_quality_and_correlation_from_request(self):
        # S2 — is_bot/is_proxy/cf_ray/request_id/arrival_ts come straight
        # off the (worker-populated) request.
        req = ClickRequest(
            click_id="c1", is_bot=True, is_proxy=True,
            cf_ray="8abc-FRA", request_id="11111111-2222-3333-4444-555555555555",
            arrival_ts="2026-06-01T10:00:00.100Z",
        )
        f = _phase3_attribution_fields({"attribution": {}}, req, {}, _RDT)
        assert f["is_bot"] is True
        assert f["is_proxy"] is True
        assert f["cf_ray"] == "8abc-FRA"
        assert f["request_id"] == "11111111-2222-3333-4444-555555555555"
        assert f["arrival_ts"] == "2026-06-01T10:00:00.100Z"

    def test_edge_signals_default_when_absent(self):
        # S2 fail-open: an old worker (no edge signals) → safe defaults,
        # never crashes. arrival_ts None → collector NULL (not now()).
        f = _phase3_attribution_fields(
            {"attribution": {}}, ClickRequest(click_id="c1"), {}, _RDT)
        assert f["is_bot"] is False
        assert f["is_proxy"] is False
        assert f["is_unique"] is True        # absent cookie → new visitor
        assert f["cf_ray"] == ""
        assert f["request_id"] == ""
        assert f["arrival_ts"] is None

    def test_utc_now_ms_iso_format(self):
        # FIX-1 — millisecond precision (3 digits), trailing Z, NOT the
        # 6-digit microseconds .isoformat() produces.
        ts = _utc_now_ms_iso()
        assert ts.endswith("Z")
        ms = ts.split(".")[1][:-1]
        assert len(ms) == 3 and ms.isdigit()


# ============================================================
# C-1 — _build_extra_params (no column duplication; no-match capture)
# ============================================================


class TestBuildExtraParams:
    def test_matched_uses_resolver_extras_no_column_duplication(self):
        # Matched click: the resolver already excluded every key that bound
        # to a reserved/sub column, so extra_params == its `extras` set —
        # a column-bound value (pixel_id) is NOT duplicated here.
        attribution = {
            "slots": {"pixel_id": "PIX", "sub3": "S3"},
            "extras": {"fbclid": "abc", "step": "1"},
        }
        qp = {"pixel_id": "PIX", "sub3": "S3", "fbclid": "abc", "step": "1"}
        extra = _build_extra_params(attribution, qp)
        assert extra == {"fbclid": "abc", "step": "1"}
        assert "pixel_id" not in extra   # lives in its column, not extras
        assert "sub3" not in extra

    def test_debug_flag_dropped_from_matched(self):
        attribution = {"slots": {}, "extras": {"debug": "1", "x": "y"}}
        extra = _build_extra_params(attribution, {"debug": "1", "x": "y"})
        assert extra == {"x": "y"}

    def test_no_match_captures_every_param(self):
        # No attribution (pre-campaign / no-match path never ran the
        # resolver) → capture all advertiser params so nothing is lost,
        # str-coerced, minus the internal debug flag.
        qp = {"source": "fb", "sub2": "B", "cost": 5, "debug": "1"}
        extra = _build_extra_params(None, qp)
        assert extra == {"source": "fb", "sub2": "B", "cost": "5"}
        assert "debug" not in extra

    def test_no_match_empty_attribution_dict_falls_back(self):
        # An attribution dict WITHOUT an `extras` key (defensive) also
        # falls back to full-qp capture rather than silently dropping.
        qp = {"a": "1", "b": "2"}
        assert _build_extra_params({"slots": {}}, qp) == {"a": "1", "b": "2"}

    def test_empty_inputs(self):
        assert _build_extra_params(None, {}) == {}
        assert _build_extra_params({"extras": {}}, {"x": "1"}) == {}


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


# ============================================================
# v2 Phase A2 — base routing provenance + decision_reason
# ============================================================


from app.main import _decision_reason  # noqa: E402


class TestDecisionReason:
    def test_matched_flow(self):
        assert _decision_reason({}, {"route_via": "flow_cascade"}, {}) == "matched_flow"

    def test_matched_legacy_split(self):
        assert _decision_reason({}, {"route_via": "legacy_split"}, {}) == "matched_legacy_split"

    def test_blocked_by_flow(self):
        assert _decision_reason({}, {"route_via": "flow_cascade_block"}, {}) == "blocked_by_flow"
        assert _decision_reason({"routing_status": "blocked"}, {}, {}) == "blocked_by_flow"

    def test_no_campaign_match(self):
        assert _decision_reason({"routing_status": "no_match"}, {}, {}) == "no_campaign_match"

    def test_no_flow_no_offer(self):
        r = _decision_reason({"routing_status": "no_offer"}, {}, {"routing_trace": {}})
        assert r == "no_flow_no_offer"

    def test_terminal_fallback_on_availability_exclusion(self):
        r = _decision_reason(
            {"routing_status": "no_offer"}, {},
            {"routing_trace": {"availability_excluded": 2}},
        )
        assert r == "terminal_fallback"

    def test_always_set_defensive_default(self):
        assert _decision_reason({}, {}, {}) == "no_campaign_match"

    # ── v2 F-REASON-V2: returning-specific refinements of a flow_cascade match
    def test_sticky_pin_hit(self):
        r = _decision_reason(
            {}, {"route_via": "flow_cascade"},
            {"sticky_status": "hit", "audience_pool": "returning"},
        )
        assert r == "sticky_pin_hit"

    def test_fresh_repin(self):
        r = _decision_reason(
            {}, {"route_via": "flow_cascade"},
            {"sticky_status": "invalid_closed", "audience_pool": "returning"},
        )
        assert r == "fresh_repin"

    def test_override_returning_flow(self):
        # returning pool won, no sticky pin involved (miss/minted/na) →
        # override_returning_flow.
        r = _decision_reason(
            {}, {"route_via": "flow_cascade"},
            {"sticky_status": "miss", "audience_pool": "returning"},
        )
        assert r == "override_returning_flow"

    def test_first_pool_stays_matched_flow_byte_identical(self):
        # DARK / new / first-pool click (sticky 'na', audience 'first') is
        # UNCHANGED — the prime-directive byte-identical case.
        assert _decision_reason(
            {}, {"route_via": "flow_cascade"},
            {"sticky_status": "na", "audience_pool": "first"},
        ) == "matched_flow"
        # also unchanged when attr carries no returning keys at all
        assert _decision_reason({}, {"route_via": "flow_cascade"}, {}) == "matched_flow"

    # ── v2 F-DOMAIN-BLOCKED: edge subdomain block ≠ flow block
    def test_domain_blocked(self):
        r = _decision_reason(
            {"blocked": True}, {"result": "blocked_unmatched_subdomain"}, {},
        )
        assert r == "domain_blocked"

    def test_flow_block_unchanged_not_domain_blocked(self):
        # a flow-authored block stays blocked_by_flow (no subdomain marker).
        assert _decision_reason(
            {"blocked": True}, {"route_via": "flow_cascade_block"}, {},
        ) == "blocked_by_flow"


class TestA2ProvenanceFields:
    def _result(self, attr_extra=None):
        attr = {"company_id": 1, "slots": {}}
        if attr_extra:
            attr.update(attr_extra)
        return {"attribution": attr}

    def test_provenance_emitted_from_attribution(self):
        import json
        result = self._result({
            "action_type": "offer", "winning_scope_type": "buyer",
            "winning_scope_id": 5, "audience_pool": "returning",
            "target_selection_path": "pinned",
            "routing_trace": {"candidates": 3, "availability_excluded": 1},
        })
        f = _phase3_attribution_fields(
            result, ClickRequest(click_id="c1"), {"route_via": "flow_cascade"}, _RDT,
        )
        # v2 F-REASON-V2 — audience_pool='returning' (returning pool won, no
        # sticky pin) refines the flow match to `override_returning_flow`.
        assert f["decision_reason"] == "override_returning_flow"
        assert f["winning_scope_type"] == "buyer"
        assert f["winning_scope_id"] == 5
        assert f["audience_pool"] == "returning"
        assert f["action_type"] == "offer"
        assert f["target_selection_path"] == "pinned"
        trace = json.loads(f["routing_trace"])
        assert trace["candidates"] == 3
        assert trace["decision_reason"] == "override_returning_flow"  # folded into trace

    def test_defaults_when_absent(self):
        f = _phase3_attribution_fields(
            self._result(), ClickRequest(click_id="c1"), {}, _RDT,
        )
        assert f["decision_reason"] == "no_campaign_match"
        assert f["winning_scope_type"] == ""
        assert f["audience_pool"] == "none"
        assert f["action_type"] == ""
        assert f["target_selection_path"] == ""
        # routing_result is KEPT alongside decision_reason (both present).
        assert "routing_result" in f
