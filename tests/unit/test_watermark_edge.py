"""P2 c3 (D5, LOSSFIX 2026-07-07) — edge watermark tests.

Ported ~1:1 from the collector's `test_watermark_backpressure.py`
(LOSSFIX P1a) for the state-machine/hysteresis/fail-open/staleness
coverage (`WatermarkState` behaviour is identical); the divergence is
`/decide` integration — the edge SPILLS to the disk-segment fallback
(never a 503 on its own) instead of the collector's whole-batch 503,
because the routing cache and `stream:clicks` share one Redis instance.

Coverage (brief OBSERVABLE DONE item 8 + the ported state-machine
tests):

  1. `should_spill()` state machine — hysteresis enter/exit, fail-open
     on a stale/never-sampled signal (with bounded boot grace), the
     two-home stale-alert (sampler loop + request path) sharing one
     gate.
  2. `/decide` integration — spill mode diverts a real click to the
     disk fallback (never XADD's, never 503's on its own); healthy
     mode is unaffected; a stale signal fails open to XADD.
  3. Spill-mode `/decide` p99 under the CF Worker's 2000ms AbortSignal
     budget at a realistic concurrent load (item 12).
"""

from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from fastapi.testclient import TestClient

from app import main as click_main
from app import watermark as watermark_module
from app.telemetry import OP_WATERMARK_SIGNAL_STALE, OP_WATERMARK_SPILL
from app.watermark import WatermarkState


@pytest.fixture(autouse=True)
def _reset_watermark():
    """The watermark state is a process-wide singleton — reset it
    around every test so spill/staleness set by one case can't leak
    into the next (mirrors the collector's identical fixture)."""
    click_main.watermark_state.reset_for_tests()
    try:
        yield
    finally:
        click_main.watermark_state.reset_for_tests()


@pytest.fixture
def client():
    return TestClient(click_main.app)


@pytest.fixture
def patched_auth():
    with patch("app.main._check_tds_key", new_callable=AsyncMock):
        yield


def _payload(click_id: str = "019e5be83c8179896a0859dd") -> dict:
    return {
        "click_id": click_id,
        "ip": "1.2.3.4",
        "country": "DE",
        "user_agent": "geo-tds-test/1.0",
    }


_MATCHED = {
    "url": "https://offer.example.com/track?cid=1",
    "campaign_id": "camp-9",
    "offer_id": "offer-9",
    "binding_id": 0,
    "binding_alias": None,
    "timing": {"result": "flow_cascade"},
}


def _fake_redis(xadd_side_effect=None) -> MagicMock:
    r = MagicMock()
    r.set = AsyncMock(return_value=True)
    if xadd_side_effect is not None:
        r.xadd = AsyncMock(side_effect=xadd_side_effect)
    else:
        r.xadd = AsyncMock(return_value="1-0")
    return r


def _capturing_op_msg(sink: list):
    def _capture(op_name, message, level="warning", **extras):
        sink.append((op_name, level, extras))
    return _capture


# ---------------------------------------------------------------------------
# 1 — hysteresis state machine (ported from collector, renamed shed->spill)
# ---------------------------------------------------------------------------


def test_hysteresis_enters_at_shed_holds_flat_then_exits_at_resume():
    state = WatermarkState()
    assert state.spill_mode is False

    state.record_sample(85.0)
    assert state.spill_mode is True

    state.record_sample(80.0)  # inside the hysteresis gap
    assert state.spill_mode is True, "must not flap back to healthy in the gap"

    state.record_sample(69.0)
    assert state.spill_mode is False

    state.record_sample(86.0)
    assert state.spill_mode is True


# ---------------------------------------------------------------------------
# 2 — F2 fail-open on a stale sample, even mid-spill
# ---------------------------------------------------------------------------


def test_stale_signal_fails_open_from_active_spill_and_alerts_once(monkeypatch):
    monkeypatch.setattr(watermark_module.settings, "watermark_staleness_sec", 0.01)
    state = WatermarkState()
    state.record_sample(95.0)  # enters spill
    assert state.spill_mode is True

    time.sleep(0.05)
    assert state.is_stale() is True

    alerts: list[str] = []
    monkeypatch.setattr(
        watermark_module, "capture_op_msg",
        lambda op_name, message, level="warning", **extras: alerts.append(op_name),
    )

    assert state.should_spill() is False, "fail-open must NOT spill"
    assert state.should_spill() is False
    assert alerts == [OP_WATERMARK_SIGNAL_STALE]


def test_fresh_sample_rearms_the_stale_alert(monkeypatch):
    state = WatermarkState()
    alerted: list[str] = []
    monkeypatch.setattr(
        watermark_module, "capture_op_msg",
        lambda op_name, message, level="warning", **extras: alerted.append(op_name),
    )

    state.record_sample(95.0)
    state._last_sampled_monotonic -= 1_000

    assert state.should_spill() is False
    assert state.should_spill() is False
    assert alerted == [OP_WATERMARK_SIGNAL_STALE]

    state.record_sample(95.0)
    state._last_sampled_monotonic -= 1_000
    assert state.should_spill() is False
    assert alerted == [OP_WATERMARK_SIGNAL_STALE, OP_WATERMARK_SIGNAL_STALE]


def test_stale_alert_fires_once_when_healthy_sampler_goes_stale(monkeypatch):
    state = WatermarkState()
    alerted: list[str] = []
    monkeypatch.setattr(
        watermark_module, "capture_op_msg",
        lambda op_name, message, level="warning", **extras: alerted.append(op_name),
    )

    state.record_sample(10.0)
    assert state.spill_mode is False
    state._last_sampled_monotonic -= 1_000
    assert state.is_stale() is True

    assert state.should_spill() is False
    assert state.should_spill() is False
    assert alerted == [OP_WATERMARK_SIGNAL_STALE]


def test_inside_boot_grace_never_sampled_stays_quiet(monkeypatch):
    state = WatermarkState()
    alerted: list[str] = []
    monkeypatch.setattr(
        watermark_module, "capture_op_msg",
        lambda op_name, message, level="warning", **extras: alerted.append(op_name),
    )

    assert state._last_sampled_monotonic is None
    assert state.is_stale() is True

    for _ in range(3):
        assert state.should_spill() is False

    assert alerted == []


def test_never_sampled_past_boot_grace_alerts(monkeypatch):
    state = WatermarkState()
    monkeypatch.setattr(watermark_module.settings, "watermark_boot_grace_sec", 0.01)
    alerted: list[str] = []
    monkeypatch.setattr(
        watermark_module, "capture_op_msg",
        lambda op_name, message, level="warning", **extras: alerted.append(op_name),
    )

    assert state._last_sampled_monotonic is None
    state._created_monotonic -= 1_000

    assert state.should_spill() is False
    assert state.should_spill() is False
    assert alerted == [OP_WATERMARK_SIGNAL_STALE]


def test_sampler_loop_consecutive_failures_self_alerts_with_zero_requests(monkeypatch):
    state = WatermarkState()
    monkeypatch.setattr(watermark_module.settings, "watermark_staleness_sec", 5.0)
    monkeypatch.setattr(watermark_module.settings, "watermark_sample_interval_sec", 1.0)
    alerted: list[str] = []
    monkeypatch.setattr(
        watermark_module, "capture_op_msg",
        lambda op_name, message, level="warning", **extras: alerted.append(op_name),
    )

    for _ in range(4):
        state.record_sample_failure()
    assert alerted == []

    state.record_sample_failure()
    assert alerted == [OP_WATERMARK_SIGNAL_STALE]

    state.record_sample_failure()
    assert alerted == [OP_WATERMARK_SIGNAL_STALE]


def test_sampler_loop_self_alert_and_request_path_share_one_gate(monkeypatch):
    state = WatermarkState()
    monkeypatch.setattr(watermark_module.settings, "watermark_staleness_sec", 5.0)
    monkeypatch.setattr(watermark_module.settings, "watermark_sample_interval_sec", 1.0)
    alerted: list[str] = []
    monkeypatch.setattr(
        watermark_module, "capture_op_msg",
        lambda op_name, message, level="warning", **extras: alerted.append(op_name),
    )

    state.record_sample(10.0)
    state._last_sampled_monotonic -= 1_000

    for _ in range(5):
        state.record_sample_failure()
    assert alerted == [OP_WATERMARK_SIGNAL_STALE]

    assert state.should_spill() is False
    assert alerted == [OP_WATERMARK_SIGNAL_STALE], "home 2 must not double-fire"

    state.record_sample(10.0)
    state._last_sampled_monotonic -= 1_000
    for _ in range(5):
        state.record_sample_failure()
    assert alerted == [OP_WATERMARK_SIGNAL_STALE, OP_WATERMARK_SIGNAL_STALE]


# ---------------------------------------------------------------------------
# 2b — sampler
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sampler_reads_info_memory_and_records_sample():
    redis = MagicMock()
    redis.info = AsyncMock(return_value={"used_memory": 900, "maxmemory": 1000})

    from app.watermark import _sample_used_memory_pct
    pct = await _sample_used_memory_pct(redis)
    assert pct == 90.0


@pytest.mark.asyncio
async def test_sampler_maxmemory_unset_returns_none():
    redis = MagicMock()
    redis.info = AsyncMock(return_value={"used_memory": 900, "maxmemory": 0})
    from app.watermark import _sample_used_memory_pct
    assert await _sample_used_memory_pct(redis) is None


@pytest.mark.asyncio
async def test_sampler_info_failure_returns_none():
    redis = MagicMock()
    redis.info = AsyncMock(side_effect=RuntimeError("redis down"))
    from app.watermark import _sample_used_memory_pct
    assert await _sample_used_memory_pct(redis) is None


@pytest.mark.asyncio
async def test_run_sampler_cancellable():
    from app.watermark import run_watermark_sampler
    redis = MagicMock()
    redis.info = AsyncMock(return_value={"used_memory": 0, "maxmemory": 1000})
    task = asyncio.create_task(run_watermark_sampler(redis, interval=10))
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


# ---------------------------------------------------------------------------
# 3 — /decide integration: spill diverts to disk, never a 503 on its own
# ---------------------------------------------------------------------------


def test_spill_mode_diverts_real_click_to_disk_not_xadd(client, patched_auth):
    click_main.watermark_state.record_sample(90.0)
    assert click_main.watermark_state.spill_mode is True

    fake_redis = _fake_redis()  # would succeed if XADD were even attempted
    fake_route = AsyncMock(return_value=_MATCHED)
    fake_enqueue = AsyncMock(return_value=True)
    captured: list = []

    with patch("app.main.get_redis", new=AsyncMock(return_value=fake_redis)), \
         patch("app.main.route", new=fake_route), \
         patch("app.main.enqueue_click_to_disk", new=fake_enqueue), \
         patch("app.main.capture_op_msg", new=_capturing_op_msg(captured)):
        r = client.post("/decide", json=_payload(), headers={"X-TDS-Key": "x"})

    assert r.status_code == 200
    assert r.json()["status"] == 302
    fake_redis.xadd.assert_not_awaited()
    fake_enqueue.assert_awaited_once()
    ops = [c[0] for c in captured]
    assert OP_WATERMARK_SPILL in ops


def test_spill_mode_and_disk_also_fails_returns_503_not_a_spill_503(client, patched_auth):
    """Spill itself is never a 503 — but if the disk fallback it
    diverted to ALSO fails, the existing L1 uncaptured-click 503 still
    fires (same terminal path as any other stream-write failure)."""
    click_main.watermark_state.record_sample(95.0)

    fake_redis = _fake_redis()
    fake_route = AsyncMock(return_value=_MATCHED)
    fake_enqueue = AsyncMock(return_value=False)

    with patch("app.main.get_redis", new=AsyncMock(return_value=fake_redis)), \
         patch("app.main.route", new=fake_route), \
         patch("app.main.enqueue_click_to_disk", new=fake_enqueue), \
         patch("app.main.check_disk_pressure", return_value=(False, 10**9)):
        r = client.post("/decide", json=_payload(), headers={"X-TDS-Key": "x"})

    assert r.status_code == 503
    assert r.json()["detail"] == "click_uncaptured"
    fake_redis.xadd.assert_not_awaited()


def test_healthy_mode_unaffected_xadds_normally(client, patched_auth):
    click_main.watermark_state.record_sample(10.0)
    assert click_main.watermark_state.spill_mode is False

    fake_redis = _fake_redis()
    fake_route = AsyncMock(return_value=_MATCHED)

    with patch("app.main.get_redis", new=AsyncMock(return_value=fake_redis)), \
         patch("app.main.route", new=fake_route):
        r = client.post("/decide", json=_payload(), headers={"X-TDS-Key": "x"})

    assert r.status_code == 200
    assert r.json()["status"] == 302
    fake_redis.xadd.assert_awaited_once()


def test_stale_watermark_signal_fails_open_to_xadd(client, patched_auth, monkeypatch):
    """A wedged sampler must never wedge the hot path shut — a stale
    sample fails open to the normal XADD attempt exactly like the M1
    entry-count gate does."""
    monkeypatch.setattr(watermark_module.settings, "watermark_staleness_sec", 0.01)
    click_main.watermark_state.record_sample(95.0)  # enters spill
    time.sleep(0.05)
    assert click_main.watermark_state.is_stale() is True

    fake_redis = _fake_redis()
    fake_route = AsyncMock(return_value=_MATCHED)

    with patch("app.main.get_redis", new=AsyncMock(return_value=fake_redis)), \
         patch("app.main.route", new=fake_route):
        r = client.post("/decide", json=_payload(), headers={"X-TDS-Key": "x"})

    assert r.status_code == 200
    assert r.json()["status"] == 302
    fake_redis.xadd.assert_awaited_once()


def test_m1_entry_count_gate_takes_priority_over_spill(client, patched_auth):
    """When BOTH the M1 entry-count gate and the watermark are tripped,
    the click still diverts exactly once (no double-processing) — M1
    is checked first in main.py's real-click branch."""
    from app.config import settings as app_settings

    click_main.watermark_state.record_sample(95.0)
    fake_redis = _fake_redis()
    fake_route = AsyncMock(return_value=_MATCHED)
    fake_enqueue = AsyncMock(return_value=True)

    with patch("app.main.get_redis", new=AsyncMock(return_value=fake_redis)), \
         patch("app.main.route", new=fake_route), \
         patch("app.main.enqueue_click_to_disk", new=fake_enqueue), \
         patch(
             "app.main.get_cached_stream_clicks_length",
             return_value=app_settings.stream_clicks_maxlen,
         ):
        r = client.post("/decide", json=_payload(), headers={"X-TDS-Key": "x"})

    assert r.status_code == 200
    fake_redis.xadd.assert_not_awaited()
    fake_enqueue.assert_awaited_once()


# ---------------------------------------------------------------------------
# 12 — spill-mode /decide p99 under the CF Worker's 2000ms AbortSignal
#      budget, at a realistic concurrent spill load (>=300 in flight).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_spill_mode_decide_p99_under_2000ms_at_300_concurrent(
    monkeypatch, tmp_path,
):
    """Exercises the REAL disk-segment writer (group-commit fsync,
    not mocked) under 300 concurrent /decide calls while spill mode is
    active, proving the group-commit design keeps redirect latency
    bounded under a realistic concurrent spill load."""
    from app import disk_queue

    with patch("app.main._check_tds_key", new_callable=AsyncMock):
        click_main.watermark_state.record_sample(95.0)
        assert click_main.watermark_state.spill_mode is True

        monkeypatch.setattr(
            disk_queue.settings, "disk_queue_root", str(tmp_path / "spill-queue"),
        )
        monkeypatch.setattr(disk_queue.settings, "disk_segment_group_commit_ms", 20.0)
        disk_queue._reset_state_for_tests()

        fake_redis = _fake_redis()
        fake_route = AsyncMock(return_value=_MATCHED)

        n = 300
        transport = httpx.ASGITransport(app=click_main.app)
        latencies_ms: list[float] = []

        async def _one(i: int):
            t0 = time.perf_counter()
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
                with patch("app.main.get_redis", new=AsyncMock(return_value=fake_redis)), \
                     patch("app.main.route", new=fake_route):
                    r = await ac.post(
                        "/decide", json=_payload(f"spill-load-{i}"),
                        headers={"X-TDS-Key": "x"},
                    )
            latencies_ms.append((time.perf_counter() - t0) * 1000)
            return r

        results = await asyncio.gather(*[_one(i) for i in range(n)])

        assert all(r.status_code == 200 for r in results)
        latencies_ms.sort()
        p99 = latencies_ms[int(0.99 * (len(latencies_ms) - 1))]
        assert p99 < 2000, (
            f"spill-mode /decide p99={p99:.1f}ms at {n} concurrent exceeds "
            "the CF Worker's 2000ms AbortSignal race deadline"
        )

        # Force-finalize so the still-open `.wip` segment (300 tiny
        # records rarely cross the 2MB/1s rotation threshold on their
        # own) becomes visible to the stats scan — every click landed
        # durably on disk either way (group-commit fsync already ran
        # per-batch; finalize is just the rename+dir-fsync boundary).
        disk_queue._get_writer().force_finalize_for_tests()
        stats = disk_queue._scan_queue_stats_sync()
        assert stats["segments"] >= 1

        disk_queue._reset_state_for_tests()
