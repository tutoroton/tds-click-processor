"""P3 observability (audit 2026-06-03) — Sentry signal on silent
routing-decision skip/fallback paths (the user's #1 concern).

Each previously-silent defensive skip now emits a THROTTLED Sentry
capture (or, for caps, a consecutive-failure-counted capture) so an
operator sees the misroute instead of clicks quietly going elsewhere.
These tests pin that the capture fires on each path, and that the
throttle / counter prevents per-click spam. All mutation-checked.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app import action_executor, cascade, resolution, router, telemetry


@pytest.fixture(autouse=True)
def _reset_throttle():
    telemetry._reset_throttle_for_tests()
    router._cap_failures_consecutive = 0
    yield
    telemetry._reset_throttle_for_tests()
    router._cap_failures_consecutive = 0


# ---------------------------------------------------------------------------
# Throttle helper itself
# ---------------------------------------------------------------------------


class TestThrottle:
    def test_first_fires_second_throttled(self):
        with patch.object(telemetry, "capture_op_msg") as cap:
            r1 = telemetry.capture_op_msg_throttled("op_x", "key1", "msg")
            r2 = telemetry.capture_op_msg_throttled("op_x", "key1", "msg")
        assert r1 is True
        assert r2 is False
        cap.assert_called_once()  # only the first reached Sentry

    def test_distinct_keys_each_fire(self):
        with patch.object(telemetry, "capture_op_msg") as cap:
            telemetry.capture_op_msg_throttled("op_x", "keyA", "m")
            telemetry.capture_op_msg_throttled("op_x", "keyB", "m")
        assert cap.call_count == 2

    def test_window_zero_always_fires(self):
        with patch.object(telemetry, "capture_op_msg") as cap:
            telemetry.capture_op_msg_throttled("op_x", "k", "m", window_sec=0)
            telemetry.capture_op_msg_throttled("op_x", "k", "m", window_sec=0)
        assert cap.call_count == 2


# ---------------------------------------------------------------------------
# B12 — cascade malformed criteria skip
# ---------------------------------------------------------------------------


def test_b12_malformed_criteria_emits_signal():
    flow = {"_id": "77", "criteria": "{not valid json"}
    with patch.object(cascade, "capture_op_msg_throttled") as cap:
        survivors = cascade._filter_by_criteria([flow], {"geo": "US"})
    assert survivors == []  # behavior unchanged: skipped
    cap.assert_called_once()
    assert cap.call_args.args[0] == cascade.OP_CRITERIA_SKIP
    assert cap.call_args.args[1] == "77"  # dedup key = flow id


# ---------------------------------------------------------------------------
# D4 — cascade flow-load drift (candidates exist, no hashes) → None
# ---------------------------------------------------------------------------


def _redis_lists_hashes(lists, hashes):
    class FakePipeline:
        def __init__(self):
            self._ops = []

        def lrange(self, key, _s, _e):
            self._ops.append(("lrange", key))

        def hgetall(self, key):
            self._ops.append(("hgetall", key))

        async def execute(self):
            out = []
            for op, key in self._ops:
                out.append(list(lists.get(key, [])) if op == "lrange"
                           else dict(hashes.get(key, {})))
            return out

    redis = MagicMock()
    redis.pipeline = lambda: FakePipeline()
    return redis


@pytest.mark.asyncio
async def test_d4_flow_load_drift_emits_signal():
    # campaign lists a flow id but its HASH is missing (sync drift).
    redis = _redis_lists_hashes(
        {"campaign:1:flows": ["999"], "flows:scope:1:company:1": []},
        {},  # flow:999 absent
    )
    with patch.object(cascade, "capture_op_msg_throttled") as cap:
        winner = await cascade.resolve_flow(
            redis, campaign_id="1", company_id=1, buyer_id=None,
            team_id=None, department_id=None, custom_group_id=None,
            click_attrs={"geo": "US", "os": "ios", "device_type": "mobile"},
        )
    assert winner is None
    cap.assert_called_once()
    assert cap.call_args.args[0] == cascade.OP_FLOW_LOAD


# ---------------------------------------------------------------------------
# B3 — split with no usable offers → fallback signal
# ---------------------------------------------------------------------------


def _ae_click():
    from app.models import ClickRequest
    return ClickRequest(click_id="c1", query_params={})


@pytest.mark.asyncio
async def test_b3_split_fallback_emits_signal():
    flow = {"_id": "55", "action_type": "split",
            "action_config": json.dumps({"offers": [
                {"offer_id": 1, "target_id": 2, "weight": 0},
                {"offer_id": 3, "target_id": 4, "weight": 0},
            ]})}
    redis = MagicMock()
    with patch.object(action_executor, "capture_op_msg_throttled") as cap:
        result = await action_executor.execute_action(
            redis, flow, _ae_click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=lambda *a, **k: "x",
        )
    assert result is None
    cap.assert_called_once()
    assert cap.call_args.args[0] == action_executor.OP_SPLIT_FALLBACK
    assert cap.call_args.args[1] == "55"  # flow id


# ---------------------------------------------------------------------------
# D3 — offer row missing → fallback signal
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_d3_offer_not_found_emits_signal():
    flow = {"_id": "9", "action_type": "offer",
            "action_config": json.dumps({"offer_id": 42})}
    redis = MagicMock()
    redis.hgetall = AsyncMock(return_value={})  # offer:42 absent
    with patch.object(action_executor, "capture_op_msg_throttled") as cap:
        result = await action_executor.execute_action(
            redis, flow, _ae_click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=lambda *a, **k: "x",
        )
    assert result is None
    assert cap.call_args.args[0] == action_executor.OP_OFFER_RESOLVE
    assert cap.call_args.args[1] == "42"  # offer id


# ---------------------------------------------------------------------------
# D10 — param_mappings parse failure signal
# ---------------------------------------------------------------------------


def test_d10_param_parse_failure_emits_signal():
    with patch.object(resolution, "capture_op_msg_throttled") as cap:
        out = resolution.parse_param_mappings("{not valid json")
    assert out == []  # behavior unchanged: treated as no mapping
    cap.assert_called_once()
    assert cap.call_args.args[0] == resolution.OP_PARAM_PARSE


# ---------------------------------------------------------------------------
# D1/D2 — caps/counters sustained-failure counter
# ---------------------------------------------------------------------------


class TestCapFailureCounter:
    def test_fires_only_at_threshold(self):
        exc = RuntimeError("redis down")
        with patch.object(router, "capture_op_msg") as cap:
            for _ in range(router._CAP_FAILURE_ALERT_AFTER - 1):
                router._record_cap_failure("cap_check", exc)
            cap.assert_not_called()  # below threshold — no per-click spam
            router._record_cap_failure("cap_check", exc)  # Nth
            cap.assert_called_once()
            assert cap.call_args.args[0] == router.OP_CAP_COUNTER

    def test_success_resets_the_window(self):
        exc = RuntimeError("blip")
        with patch.object(router, "capture_op_msg") as cap:
            for _ in range(router._CAP_FAILURE_ALERT_AFTER - 1):
                router._record_cap_failure("cap_check", exc)
            router._record_cap_success()  # Redis recovered
            # Counter reset → the next failure is #1, not the threshold.
            router._record_cap_failure("cap_check", exc)
            cap.assert_not_called()
