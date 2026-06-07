"""Unit tests for `app/action_executor.py` — Stage 2 / Vectors 2.4 + 2.5.

Mock Redis via `AsyncMock`, inject `build_url_fn` to avoid coupling
to router internals. Each branch of `execute_action` (redirect / offer
/ split / block) gets happy-path + edge-case coverage.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.action_executor import (
    BLOCK_RESULT,
    _is_positive_int,
    _parse_action_config,
    execute_action,
)
from app.models import ClickRequest


def _click() -> ClickRequest:
    return ClickRequest(
        click_id="abc-123",
        country="US",
        user_agent="Mozilla/5.0 (iPhone)",
        query_params={},
    )


def _flow(action_type: str, action_config: dict) -> dict:
    return {
        "_id": "1",
        "action_type": action_type,
        "action_config": json.dumps(action_config),
    }


def _stub_build_url():
    """Return a stub `build_url_fn` that captures inputs + emits a marker URL.

    Signature MUST mirror `router.build_url` — including the T2.5
    additions `target_id` + `flow_id` (kwargs). When the production
    signature gains a new param, this stub must be updated too;
    otherwise action_executor's call shape breaks here loudly.
    """
    calls: list[tuple] = []

    def fn(template, req, campaign_id, offer_id, *,
           source_mappings, campaign_mappings,
           target_id=None, flow_id=None):
        calls.append((template, campaign_id, offer_id, target_id, flow_id))
        return f"FINAL[{template}|cid={campaign_id}|oid={offer_id}]"

    fn.calls = calls  # type: ignore[attr-defined]
    return fn


def _redis_with_hashes(hashes: dict[str, dict], sets: dict[str, set] | None = None) -> MagicMock:
    sets = sets or {}

    class FakePipeline:
        def __init__(self):
            self._ops: list[tuple] = []

        def hgetall(self, key):
            self._ops.append(("hgetall", key, None))

        def hget(self, key, field):
            # v2 C2 — split per-leg availability read. Absent field → None →
            # action_executor._avail_ok treats it as 'active' (fail-open).
            self._ops.append(("hget", key, field))

        async def execute(self):
            out = []
            for op, key, field in self._ops:
                if op == "hget":
                    out.append(hashes.get(key, {}).get(field))
                else:
                    out.append(dict(hashes.get(key, {})))
            return out

    redis = MagicMock()
    redis.pipeline = lambda: FakePipeline()
    redis.hgetall = AsyncMock(side_effect=lambda key: dict(hashes.get(key, {})))
    redis.hget = AsyncMock(
        side_effect=lambda key, field: dict(hashes.get(key, {})).get(field)
    )
    redis.smembers = AsyncMock(side_effect=lambda key: set(sets.get(key, set())))
    return redis


# ============================================================
# Redirect
# ============================================================


class TestRedirect:
    @pytest.mark.asyncio
    async def test_redirect_substitutes_url(self):
        flow = _flow("redirect", {"url": "https://lp.example.com/{click_id}"})
        build_url = _stub_build_url()
        r = _redis_with_hashes({})

        result = await execute_action(
            r, flow, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=build_url,
        )
        assert result is not None
        assert result["url"].startswith("FINAL[https://lp.example.com")
        assert result["offer_id"] is None
        assert result["target_id"] is None
        assert build_url.calls[0][0] == "https://lp.example.com/{click_id}"

    @pytest.mark.asyncio
    async def test_redirect_missing_url_returns_none(self):
        flow = _flow("redirect", {})
        result = await execute_action(
            _redis_with_hashes({}), flow, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=_stub_build_url(),
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_redirect_with_non_string_url_returns_none(self):
        flow = _flow("redirect", {"url": 12345})
        result = await execute_action(
            _redis_with_hashes({}), flow, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=_stub_build_url(),
        )
        assert result is None


# ============================================================
# Offer
# ============================================================


class TestOffer:
    @pytest.mark.asyncio
    async def test_offer_pinned_target(self):
        flow = _flow("offer", {"offer_id": 5, "target_id": 7})
        r = _redis_with_hashes({
            "offer_target:7": {"url": "https://t.example/path", "is_default": "0"},
        })
        build_url = _stub_build_url()
        result = await execute_action(
            r, flow, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=build_url,
        )
        assert result is not None
        assert result["offer_id"] == "5"
        assert result["target_id"] == "7"
        assert build_url.calls[0][0] == "https://t.example/path"

    @pytest.mark.asyncio
    async def test_offer_target_missing_falls_back_to_default(self):
        flow = _flow("offer", {"offer_id": 5, "target_id": 999})  # 999 absent
        r = _redis_with_hashes(
            {
                "offer:5": {"has_targets": "1", "url": "https://offer.bare/x"},
                "offer_target:7": {"url": "https://t-default", "is_default": "1"},
            },
            sets={"offer:5:targets": {"7"}},
        )
        build_url = _stub_build_url()
        result = await execute_action(
            r, flow, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=build_url,
        )
        assert result is not None
        assert result["offer_id"] == "5"
        assert result["target_id"] == "7"  # default target picked
        assert build_url.calls[0][0] == "https://t-default"

    @pytest.mark.asyncio
    async def test_offer_no_target_no_default_uses_offer_url(self):
        flow = _flow("offer", {"offer_id": 5})  # No target_id at all
        r = _redis_with_hashes({
            "offer:5": {"has_targets": "0", "url": "https://offer.fallback/p"},
        })
        build_url = _stub_build_url()
        result = await execute_action(
            r, flow, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=build_url,
        )
        assert result is not None
        assert result["offer_id"] == "5"
        assert result["target_id"] is None
        assert build_url.calls[0][0] == "https://offer.fallback/p"

    @pytest.mark.asyncio
    async def test_offer_missing_offer_id_returns_none(self):
        flow = _flow("offer", {})
        result = await execute_action(
            _redis_with_hashes({}), flow, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=_stub_build_url(),
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_offer_string_offer_id_rejected(self):
        """Numeric strings drifting through sync — refuse defensively."""
        flow = _flow("offer", {"offer_id": "5"})
        result = await execute_action(
            _redis_with_hashes({}), flow, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=_stub_build_url(),
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_offer_completely_missing_in_redis(self):
        flow = _flow("offer", {"offer_id": 999})
        r = _redis_with_hashes({})
        result = await execute_action(
            r, flow, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=_stub_build_url(),
        )
        assert result is None


# ============================================================
# Split
# ============================================================


class TestSplit:
    @pytest.mark.asyncio
    async def test_split_picks_one_offer_by_weight(self):
        flow = _flow("split", {
            "offers": [
                {"offer_id": 5, "target_id": 7, "weight": 100},
                # weight=0 should be skipped, but list must contain ≥1 valid
            ],
        })
        r = _redis_with_hashes({
            "offer_target:7": {"url": "https://t-7", "is_default": "0"},
        })
        result = await execute_action(
            r, flow, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=_stub_build_url(),
        )
        assert result is not None
        assert result["offer_id"] == "5"
        assert result["target_id"] == "7"

    @pytest.mark.asyncio
    async def test_split_zero_weights_returns_none(self):
        flow = _flow("split", {
            "offers": [
                {"offer_id": 5, "target_id": 7, "weight": 0},
                {"offer_id": 6, "target_id": 8, "weight": 0},
            ],
        })
        result = await execute_action(
            _redis_with_hashes({}), flow, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=_stub_build_url(),
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_split_empty_offers_returns_none(self):
        flow = _flow("split", {"offers": []})
        result = await execute_action(
            _redis_with_hashes({}), flow, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=_stub_build_url(),
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_split_skips_invalid_entries(self):
        flow = _flow("split", {
            "offers": [
                "not-a-dict",
                {"offer_id": -1, "weight": 100},  # bad offer_id
                {"offer_id": 5, "target_id": 7, "weight": -5},  # bad weight
                {"offer_id": 8, "target_id": 9, "weight": 50},  # only valid one
            ],
        })
        r = _redis_with_hashes({
            "offer_target:9": {"url": "https://only-valid", "is_default": "0"},
        })
        result = await execute_action(
            r, flow, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=_stub_build_url(),
        )
        assert result is not None
        assert result["offer_id"] == "8"

    @pytest.mark.asyncio
    async def test_split_no_offers_key_returns_none(self):
        flow = _flow("split", {})
        result = await execute_action(
            _redis_with_hashes({}), flow, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=_stub_build_url(),
        )
        assert result is None


# ============================================================
# Block
# ============================================================


class TestBlock:
    @pytest.mark.asyncio
    async def test_block_returns_sentinel(self):
        flow = _flow("block", {})
        result = await execute_action(
            _redis_with_hashes({}), flow, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=_stub_build_url(),
        )
        assert result == BLOCK_RESULT
        assert result["action"] == "block"
        assert result["url"] is None

    @pytest.mark.asyncio
    async def test_block_with_alert_subconfig(self):
        """Alert sub-config doesn't change execution — Stage 6 handles it."""
        flow = _flow("block", {
            "code": 404,
            "alert": {"severity": "high", "message": "lost!", "_origin": "auto-cascade"},
        })
        result = await execute_action(
            _redis_with_hashes({}), flow, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=_stub_build_url(),
        )
        assert result == BLOCK_RESULT


# ============================================================
# Unknown / corrupt actions
# ============================================================


class TestEdge:
    @pytest.mark.asyncio
    async def test_unknown_action_type_returns_none(self):
        flow = _flow("teleport", {})
        result = await execute_action(
            _redis_with_hashes({}), flow, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=_stub_build_url(),
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_corrupt_action_config_treated_as_empty(self):
        flow = {"_id": "1", "action_type": "redirect", "action_config": "{not json"}
        result = await execute_action(
            _redis_with_hashes({}), flow, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=_stub_build_url(),
        )
        # Empty config → no `url` → returns None.
        assert result is None


# ============================================================
# Pure helpers
# ============================================================


class TestHelpers:
    def test_parse_action_config_dict_passthrough(self):
        assert _parse_action_config({"x": 1}) == {"x": 1}

    def test_parse_action_config_string_json(self):
        assert _parse_action_config('{"x": 1}') == {"x": 1}

    def test_parse_action_config_bad_json(self):
        assert _parse_action_config("{bad") == {}

    def test_parse_action_config_none(self):
        assert _parse_action_config(None) == {}

    def test_parse_action_config_list_returns_empty(self):
        """JSON could parse to a list — we want a dict, refuse."""
        assert _parse_action_config('[1,2,3]') == {}

    def test_is_positive_int_strict(self):
        assert _is_positive_int(1) is True
        assert _is_positive_int(0) is False
        assert _is_positive_int(-1) is False
        assert _is_positive_int("1") is False  # string rejected
        assert _is_positive_int(1.0) is False  # float rejected
        assert _is_positive_int(None) is False
        # Bool explicitly rejected even though `isinstance(True, int)`.
        # JSON-parsed `{"offer_id": true}` must NEVER be treated as id=1.
        assert _is_positive_int(True) is False
        assert _is_positive_int(False) is False


# ============================================================
# Audit 2026-06-03 regression fence — decision-engine gaps
# ============================================================
#
# Pins behaviours surfaced by the routing-reliability audit
# (`docs/development/audit-2026-06-03-routing-reliability.md`). Each
# test is mutation-checked: reintroducing the underlying defect makes
# the test fail (see P2-results.md mutation-check log).


class TestOfferDefaultTargetGap:
    """B4 / Agent-C GAP1 — offer with targets but no usable default.

    The KNOWN Stage-1 gap "offers required-default-target validation"
    (admin-api does NOT enforce that an offer with `has_targets=1`
    carries an `is_default=1` target). At click time the executor's
    offer-fallback (`_offer_default_template`) ONLY promotes an
    `is_default=1` target — a non-default target is NEVER auto-selected
    as the offer fallback. With no default and no bare `offer.url`, the
    action yields `None` and the click silently routes to the legacy
    split fallback. These tests pin that contract so a future change
    that starts auto-selecting an arbitrary target (or that drops the
    is_default gate) fails loudly.
    """

    @pytest.mark.asyncio
    async def test_has_targets_but_no_default_and_no_bare_url_returns_none(self):
        # offer:5 has targets, but target 7 is NOT is_default and the
        # offer carries no bare url → no usable destination → None.
        flow = _flow("offer", {"offer_id": 5})
        r = _redis_with_hashes(
            {
                "offer:5": {"has_targets": "1"},  # no `url`
                "offer_target:7": {"url": "https://non-default", "is_default": "0"},
            },
            sets={"offer:5:targets": {"7"}},
        )
        result = await execute_action(
            r, flow, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=_stub_build_url(),
        )
        # A non-default target must NOT be promoted as the offer fallback.
        assert result is None

    @pytest.mark.asyncio
    async def test_has_targets_empty_set_and_no_bare_url_returns_none(self):
        # has_targets=1 but the targets SET is empty (sync drift) and no
        # bare url → None (cannot fabricate a destination).
        flow = _flow("offer", {"offer_id": 5})
        r = _redis_with_hashes(
            {"offer:5": {"has_targets": "1"}},
            sets={"offer:5:targets": set()},
        )
        result = await execute_action(
            r, flow, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=_stub_build_url(),
        )
        assert result is None


class TestSplitFloatWeightTruncation:
    """B2 / Agent-C GAP2 — float weights are int()-truncated.

    admin-api `flows/validation.py:344` accepts `weight` as
    `(int, float)` and only requires the sum to be positive, so a split
    configured with fractional weights (e.g. 0.5 / 0.5, or someone
    typing percentages as 0.3 / 0.7) PASSES write-time validation. But
    `_execute_split` (action_executor.py:211) AND the sync builder
    (`admin-api/app/sync/builders/splits.py:86,135`) both do
    `int(weight)`, so every fractional weight < 1 truncates to 0.

    CHARACTERIZATION tests — they pin the CURRENT (buggy) truncation so
    the remediation phase (which will round/scale or reject floats at
    validation) flips them with an explicit cascade update. Mutation
    check: replacing `int(weight)` with `weight` makes both tests fail.
    """

    @pytest.mark.asyncio
    async def test_fractional_weights_below_one_truncate_to_zero_falls_back(self):
        # 0.5 + 0.5 sum to 1.0 (passes admin-api validation) but
        # int(0.5)=0 for both → sum of int weights == 0 → no usable
        # offers → None → legacy fallback. The split silently never runs.
        flow = _flow("split", {
            "offers": [
                {"offer_id": 5, "target_id": 7, "weight": 0.5},
                {"offer_id": 6, "target_id": 8, "weight": 0.5},
            ],
        })
        # Populate the targets so offer-URL resolution WOULD succeed if a
        # branch were ever picked — this isolates the truncation: the
        # None result is caused by int()-truncated weights summing to 0,
        # NOT by a missing offer/target row.
        r = _redis_with_hashes({
            "offer_target:7": {"url": "https://t-7", "is_default": "0"},
            "offer_target:8": {"url": "https://t-8", "is_default": "0"},
        })
        result = await execute_action(
            r, flow, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=_stub_build_url(),
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_sub_unit_weight_branch_never_selected_after_truncation(self):
        # Mixed: 2.9 → int 2, 0.9 → int 0. The 0.9 branch (offer 6)
        # becomes weight 0 and is therefore UNREACHABLE — random.choices
        # never selects a zero-weight element when others are positive.
        # So 100/100 picks land on offer 5, distorting the operator's
        # intended ~2.9 : 0.9 (≈76/24) split into 100/0.
        flow = _flow("split", {
            "offers": [
                {"offer_id": 5, "target_id": 7, "weight": 2.9},
                {"offer_id": 6, "target_id": 8, "weight": 0.9},
            ],
        })
        r = _redis_with_hashes({
            "offer_target:7": {"url": "https://t-7", "is_default": "0"},
            "offer_target:8": {"url": "https://t-8", "is_default": "0"},
        })
        chosen = set()
        for _ in range(100):
            result = await execute_action(
                r, flow, _click(), "1",
                source_mappings=None, campaign_mappings=None,
                build_url_fn=_stub_build_url(),
            )
            assert result is not None
            chosen.add(result["offer_id"])
        # offer 6's 0.9 weight truncated to 0 → never chosen.
        assert chosen == {"5"}


# ============================================================
# v2 Phase A2 — target_selection_path provenance
# ============================================================


class TestTargetSelectionPath:
    @pytest.mark.asyncio
    async def test_redirect_is_bare_url(self):
        flow = _flow("redirect", {"url": "https://lp.example/{click_id}"})
        result = await execute_action(
            _redis_with_hashes({}), flow, _click(), "1",
            source_mappings=None, campaign_mappings=None, build_url_fn=_stub_build_url(),
        )
        assert result["target_selection_path"] == "bare_url"

    @pytest.mark.asyncio
    async def test_offer_pinned_is_pinned(self):
        flow = _flow("offer", {"offer_id": 5, "target_id": 7})
        r = _redis_with_hashes({"offer_target:7": {"url": "https://t.example", "is_default": "0"}})
        result = await execute_action(
            r, flow, _click(), "1",
            source_mappings=None, campaign_mappings=None, build_url_fn=_stub_build_url(),
        )
        assert result["target_selection_path"] == "pinned"

    @pytest.mark.asyncio
    async def test_offer_default_fallback_is_offer_default(self):
        # No pinned target → resolves the offer's is_default target.
        flow = _flow("offer", {"offer_id": 5})
        r = _redis_with_hashes(
            {
                "offer:5": {"has_targets": "1"},
                "offer_target:9": {"url": "https://d.example", "is_default": "1"},
            },
            sets={"offer:5:targets": {"9"}},
        )
        result = await execute_action(
            r, flow, _click(), "1",
            source_mappings=None, campaign_mappings=None, build_url_fn=_stub_build_url(),
        )
        assert result is not None
        assert result["target_selection_path"] == "offer_default"

    @pytest.mark.asyncio
    async def test_offer_bare_url_is_bare_url(self):
        # Offer with a bare url, no targets → bare_url path.
        flow = _flow("offer", {"offer_id": 5})
        r = _redis_with_hashes({"offer:5": {"url": "https://bare.example"}})
        result = await execute_action(
            r, flow, _click(), "1",
            source_mappings=None, campaign_mappings=None, build_url_fn=_stub_build_url(),
        )
        assert result is not None
        assert result["target_selection_path"] == "bare_url"

    @pytest.mark.asyncio
    async def test_split_is_split_weighted(self):
        flow = _flow("split", {"offers": [{"offer_id": 5, "target_id": 7, "weight": 100}]})
        r = _redis_with_hashes({"offer_target:7": {"url": "https://t.example", "is_default": "0"}})
        result = await execute_action(
            r, flow, _click(), "1",
            source_mappings=None, campaign_mappings=None, build_url_fn=_stub_build_url(),
        )
        assert result is not None
        assert result["target_selection_path"] == "split_weighted"


# ============================================================
# v2 LD-F2 — routing_trace.split (D22 / §05 Tier-3): weights + picked,
# plus per-leg availability exclusions merged into trace.availability.
# ============================================================


class TestLDF2SplitTrace:
    @pytest.mark.asyncio
    async def test_split_records_weights_and_picked(self):
        """(b) split action with a trace dict → trace.split carries the
        surviving legs' weights, the total, and the picked target."""
        flow = _flow("split", {
            "offers": [
                {"offer_id": 5, "target_id": 7, "weight": 70},
                {"offer_id": 6, "target_id": 9, "weight": 30},
            ],
        })
        r = _redis_with_hashes({
            "offer_target:7": {"url": "https://t-7", "is_default": "0", "availability": "active"},
            "offer_target:9": {"url": "https://t-9", "is_default": "0", "availability": "active"},
        })
        trace: dict = {}
        result = await execute_action(
            r, flow, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=_stub_build_url(), trace=trace,
        )
        assert result is not None
        split = trace["split"]
        # both surviving legs recorded with their weights; total = sum.
        assert split["weights"] == [{"target_id": 7, "w": 70}, {"target_id": 9, "w": 30}]
        assert split["total"] == 100
        # picked = whichever leg the weighted draw chose (== the served target).
        assert split["picked"] in (7, 9)
        assert split["picked"] == int(result["target_id"])

    @pytest.mark.asyncio
    async def test_split_closed_leg_recorded_in_availability(self):
        """A split leg whose target is closed is excluded AND its target_id
        lands in trace.availability.excluded_target_ids — the per-leg blind
        spot the cascade flow-level counter never saw (LD-F2 camp-85 evidence)."""
        flow = _flow("split", {
            "offers": [
                {"offer_id": 5, "target_id": 7, "weight": 50},   # closed → excluded
                {"offer_id": 6, "target_id": 9, "weight": 50},   # active → served
            ],
        })
        r = _redis_with_hashes({
            "offer_target:7": {"url": "https://t-7", "is_default": "0", "availability": "closed"},
            "offer_target:9": {"url": "https://t-9", "is_default": "0", "availability": "active"},
        })
        trace: dict = {}
        result = await execute_action(
            r, flow, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=_stub_build_url(),
            allowed_avail=frozenset({"active"}), trace=trace,
        )
        assert result is not None and result["target_id"] == "9"
        assert trace["availability"]["excluded_target_ids"] == [7]
        assert trace["availability"]["reason"] == "closed"
        # the served leg is the picked one in the split sub-object
        assert trace["split"]["picked"] == 9

    @pytest.mark.asyncio
    async def test_no_trace_split_byte_identical(self):
        """No trace dict → split executes exactly as before (no exception)."""
        flow = _flow("split", {"offers": [{"offer_id": 5, "target_id": 7, "weight": 100}]})
        r = _redis_with_hashes({"offer_target:7": {"url": "https://t-7", "is_default": "0"}})
        result = await execute_action(
            r, flow, _click(), "1",
            source_mappings=None, campaign_mappings=None, build_url_fn=_stub_build_url(),
        )
        assert result is not None and result["target_id"] == "7"

    @pytest.mark.asyncio
    async def test_offer_action_no_split_subobject(self):
        """A non-split action never writes trace.split (byte-identical)."""
        flow = _flow("offer", {"offer_id": 5, "target_id": 7})
        r = _redis_with_hashes({"offer_target:7": {"url": "https://t-7", "is_default": "0"}})
        trace: dict = {}
        result = await execute_action(
            r, flow, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=_stub_build_url(), trace=trace,
        )
        assert result is not None
        assert "split" not in trace
