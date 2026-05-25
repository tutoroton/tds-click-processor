"""Tests for the observability emission layer (T2.6 partial).

Closes the alert@50% deferred from T2.1 and the visibility gap
from T2.2. Without these emissions, the operator has no signal
that the zero-loss defenses are engaged — only that they failed.

Coverage:

  * `emit_stream_clicks_length`:
      - sub-threshold non-zero: DEBUG log only, no Sentry capture
      - at-threshold: WARN log + Sentry capture
      - over-threshold: WARN + Sentry
      - XLEN failure: WARN log, returns -1, no crash
      - cap=0 fallback (defense in depth): no divide-by-zero

  * `emit_disk_queue_size`:
      - empty queue: no log, no Sentry
      - non-empty sub-threshold: INFO log, no Sentry
      - at-threshold: WARN log + Sentry
      - cap=0 (unbounded mode): logs INFO when non-zero, no
        threshold check, no Sentry capture

  * `run_observability_loop`:
      - cancellable on lifespan shutdown
      - one metric raising doesn't suppress the next
      - per-iteration error doesn't kill the loop

  * Source-pin on lifespan integration.

Reference: rule `sync-protocol`, action-items.md T2.6.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app import disk_queue, observability


@pytest.fixture(autouse=True)
def _reset_settings(monkeypatch, tmp_path):
    """Per-test isolation. Disk-queue size counter must be reset
    too — observability calls `get_queue_size()`."""
    monkeypatch.setattr(
        observability.settings, "stream_clicks_maxlen", 1000,
    )
    monkeypatch.setattr(
        observability.settings, "disk_queue_max_files", 1000,
    )
    monkeypatch.setattr(
        disk_queue.settings, "disk_queue_root", str(tmp_path / "click-queue"),
    )
    monkeypatch.setattr(
        disk_queue.settings, "disk_queue_max_files", 1000,
    )
    disk_queue._reset_state_for_tests()
    yield
    disk_queue._reset_state_for_tests()


# ---------------------------------------------------------------------------
# emit_stream_clicks_length
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stream_length_below_threshold_no_warn(caplog):
    """A small non-zero value (well under 50%) emits DEBUG, not
    WARN. Steady-state stream length is ≤10k (shipper trims), so
    "below threshold but non-zero" is normal."""
    redis = AsyncMock()
    redis.xlen = AsyncMock(return_value=100)  # 100 / 1000 = 10%

    with patch("sentry_sdk.capture_message") as mock_capture:
        with caplog.at_level(logging.WARNING, logger="tds.observability"):
            length = await observability.emit_stream_clicks_length(redis)

    assert length == 100
    # No WARN-level record at threshold capture
    warns = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert warns == []
    # No Sentry breadcrumb either
    mock_capture.assert_not_called()


@pytest.mark.asyncio
async def test_stream_length_at_threshold_warns(caplog):
    """At exactly 50% — the boundary case must trigger the alert.
    Off-by-one would mean operators get the alert one second
    later than they should."""
    redis = AsyncMock()
    redis.xlen = AsyncMock(return_value=500)  # 500 / 1000 = 50%

    with patch("sentry_sdk.capture_message") as mock_capture:
        with caplog.at_level(logging.WARNING, logger="tds.observability"):
            length = await observability.emit_stream_clicks_length(redis)

    assert length == 500
    warns = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warns) == 1
    assert "stream.clicks.length=500" in warns[0].getMessage()
    assert "50%" in warns[0].getMessage()
    mock_capture.assert_called_once()
    # Sentry call shape — level + message contain the metric.
    call = mock_capture.call_args
    assert "stream:clicks at 500/1000" in call.args[0]
    assert call.kwargs.get("level") == "warning"


@pytest.mark.asyncio
async def test_stream_length_far_over_threshold_warns(caplog):
    """At 95% the warn fires once (no exponential noise — same
    as at-threshold). Sentry rule on the message handles
    rate-limiting / grouping at the platform layer."""
    redis = AsyncMock()
    redis.xlen = AsyncMock(return_value=950)

    with patch("sentry_sdk.capture_message") as mock_capture:
        with caplog.at_level(logging.WARNING, logger="tds.observability"):
            await observability.emit_stream_clicks_length(redis)

    warns = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warns) == 1
    mock_capture.assert_called_once()


@pytest.mark.asyncio
async def test_stream_length_xlen_failure_returns_minus_one(caplog):
    """Redis impairment is exactly the case we monitor — but the
    sample call ITSELF can fail. Return -1 + log, never crash."""
    redis = AsyncMock()
    redis.xlen = AsyncMock(side_effect=ConnectionError("redis down"))

    with patch("sentry_sdk.capture_message") as mock_capture:
        with caplog.at_level(logging.WARNING, logger="tds.observability"):
            length = await observability.emit_stream_clicks_length(redis)

    assert length == -1
    # We log the sampler failure but DON'T re-emit a Sentry
    # threshold-breach event (we have no number).
    mock_capture.assert_not_called()


@pytest.mark.asyncio
async def test_stream_length_zero_no_log(caplog):
    """Empty stream — perfectly normal in standalone / dev mode.
    No log, no breadcrumb, no noise."""
    redis = AsyncMock()
    redis.xlen = AsyncMock(return_value=0)

    with patch("sentry_sdk.capture_message") as mock_capture:
        with caplog.at_level(logging.DEBUG, logger="tds.observability"):
            length = await observability.emit_stream_clicks_length(redis)

    assert length == 0
    mock_capture.assert_not_called()


@pytest.mark.asyncio
async def test_stream_length_cap_zero_no_divide_by_zero(monkeypatch, caplog):
    """Defense in depth — even if `stream_clicks_maxlen=0` slips
    through Pydantic validation, the runtime guard prevents
    division-by-zero in the percentage calc."""
    monkeypatch.setattr(observability.settings, "stream_clicks_maxlen", 0)

    redis = AsyncMock()
    redis.xlen = AsyncMock(return_value=5)

    # Should not raise.
    length = await observability.emit_stream_clicks_length(redis)
    assert length == 5


# ---------------------------------------------------------------------------
# emit_disk_queue_size
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_disk_queue_empty_no_log(caplog):
    """Steady state — disk queue is empty when Redis healthy. No
    log noise, no Sentry breadcrumb."""
    with patch("sentry_sdk.capture_message") as mock_capture:
        with caplog.at_level(logging.DEBUG, logger="tds.observability"):
            size = await observability.emit_disk_queue_size()

    assert size == 0
    info_records = [r for r in caplog.records if r.levelno == logging.INFO]
    assert info_records == []
    mock_capture.assert_not_called()


@pytest.mark.asyncio
async def test_disk_queue_below_threshold_info_only(caplog, monkeypatch):
    """Outage just started — non-zero but well under 50%. INFO
    log so the operator sees the trend, no Sentry breadcrumb yet."""
    # Pre-seed 100 files in the queue — 10% of cap (1000).
    for i in range(100):
        await disk_queue.enqueue_click({"i": i})

    with patch("sentry_sdk.capture_message") as mock_capture:
        with caplog.at_level(logging.INFO, logger="tds.observability"):
            size = await observability.emit_disk_queue_size()

    assert size == 100
    # INFO record present, WARN not.
    info_records = [r for r in caplog.records if r.levelno == logging.INFO]
    assert any("disk_queue.size=100" in r.getMessage() for r in info_records)
    warn_records = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert warn_records == []
    mock_capture.assert_not_called()


@pytest.mark.asyncio
async def test_disk_queue_at_threshold_warns(caplog, monkeypatch):
    """50% of cap → WARN + Sentry. Lead time before cap-rejection
    cuts in (which would start dropping clicks)."""
    monkeypatch.setattr(disk_queue.settings, "disk_queue_max_files", 10)
    monkeypatch.setattr(observability.settings, "disk_queue_max_files", 10)

    for i in range(5):
        await disk_queue.enqueue_click({"i": i})

    with patch("sentry_sdk.capture_message") as mock_capture:
        with caplog.at_level(logging.WARNING, logger="tds.observability"):
            size = await observability.emit_disk_queue_size()

    assert size == 5
    warns = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warns) == 1
    assert "disk_queue.size=5" in warns[0].getMessage()
    assert "50%" in warns[0].getMessage()
    mock_capture.assert_called_once()
    assert "disk_queue at 5/10" in mock_capture.call_args.args[0]


@pytest.mark.asyncio
async def test_disk_queue_unbounded_mode_no_threshold(caplog, monkeypatch):
    """Operator opted into unbounded mode (cap=0). Skip threshold
    check entirely — there's no "50% of unbounded". Still log
    INFO when non-zero so the trend is visible."""
    monkeypatch.setattr(disk_queue.settings, "disk_queue_max_files", 0)
    monkeypatch.setattr(observability.settings, "disk_queue_max_files", 0)

    for i in range(50):
        await disk_queue.enqueue_click({"i": i})

    with patch("sentry_sdk.capture_message") as mock_capture:
        with caplog.at_level(logging.INFO, logger="tds.observability"):
            size = await observability.emit_disk_queue_size()

    assert size == 50
    # INFO log present (with "unbounded mode" hint), WARN not.
    info_records = [r for r in caplog.records if r.levelno == logging.INFO]
    assert any("unbounded mode" in r.getMessage() for r in info_records)
    warn_records = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert warn_records == []
    mock_capture.assert_not_called()


# ---------------------------------------------------------------------------
# run_observability_loop
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_loop_cancellable():
    """Lifespan shutdown cancels the task — verify CancelledError
    propagates cleanly (no swallow)."""
    redis = AsyncMock()
    redis.xlen = AsyncMock(return_value=0)
    task = asyncio.create_task(
        observability.run_observability_loop(redis, interval=10),
    )
    await asyncio.sleep(0)  # let the loop enter sleep
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_run_loop_one_metric_raising_does_not_kill_other():
    """If `emit_stream_clicks_length` raises (Redis blip), the
    next call to `emit_disk_queue_size` still happens. Per-metric
    isolation."""
    redis = AsyncMock()
    redis.xlen = AsyncMock(side_effect=RuntimeError("xlen broke"))

    disk_called = {"n": 0}
    real_disk_emit = observability.emit_disk_queue_size

    async def counting_disk_emit():
        disk_called["n"] += 1
        return await real_disk_emit()

    with patch.object(observability, "emit_disk_queue_size", counting_disk_emit):
        task = asyncio.create_task(
            observability.run_observability_loop(redis, interval=0),
        )
        # Let the loop cycle a few times.
        for _ in range(5):
            await asyncio.sleep(0.005)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    # Even though stream metric kept raising, disk metric was
    # called multiple times — proves per-metric isolation.
    assert disk_called["n"] >= 2


@pytest.mark.asyncio
async def test_run_loop_survives_iteration_error():
    """The outer try/except catches catastrophic loop failures
    (something OTHER than per-metric error) and continues. We
    inject by patching `asyncio.sleep` to raise once."""
    redis = AsyncMock()
    redis.xlen = AsyncMock(return_value=0)

    sleep_calls = {"n": 0}
    real_sleep = asyncio.sleep

    async def flaky_sleep(t):
        sleep_calls["n"] += 1
        if sleep_calls["n"] == 2:
            raise RuntimeError("transient sleep failure")
        await real_sleep(t)

    with patch("app.observability.asyncio.sleep", flaky_sleep):
        task = asyncio.create_task(
            observability.run_observability_loop(redis, interval=0),
        )
        for _ in range(5):
            await real_sleep(0.005)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    # We hit at least 3 sleep calls — first succeeded, second
    # raised, third (and beyond) succeeded again. Loop survived.
    assert sleep_calls["n"] >= 3


# ---------------------------------------------------------------------------
# Source-pin: lifespan integration
# ---------------------------------------------------------------------------


class TestLifespanIntegration:
    def test_observability_loop_started_in_lifespan(self):
        from app import main as click_main

        source = inspect.getsource(click_main.lifespan)
        assert "run_observability_loop" in source, (
            "FastAPI lifespan must start run_observability_loop "
            "(T2.6 partial). Without it, the metrics for T2.1 and "
            "T2.2 are emitted nowhere — defenses become invisible."
        )

    def test_observability_task_cancelled_on_shutdown(self):
        from app import main as click_main

        source = inspect.getsource(click_main.lifespan)
        assert "observability_task.cancel()" in source, (
            "Lifespan must cancel observability_task on shutdown — "
            "otherwise the task leaks and blocks clean exit."
        )


# ---------------------------------------------------------------------------
# F.29 Sprint 4.1 — emit_shipper_health (independent shipper watchdog)
# ---------------------------------------------------------------------------


@pytest.fixture
def _fresh_shipper_metrics():
    """Reset the shipper-metrics singleton around each watchdog test."""
    from app.shipper_metrics import _reset_for_tests
    _reset_for_tests()
    yield observability.shipper_metrics
    _reset_for_tests()


def _set_env(monkeypatch, *, environment="production", central_url="https://c:8200"):
    monkeypatch.setattr(observability.settings, "environment", environment)
    monkeypatch.setattr(observability.settings, "central_url", central_url)
    monkeypatch.setattr(observability.settings, "shipper_lag_alert_seconds", 300)
    monkeypatch.setattr(observability.settings, "shipper_success_ratio_alert_min", 0.95)
    monkeypatch.setattr(observability.settings, "shipper_success_ratio_alert_min_sample", 20)


@pytest.mark.asyncio
async def test_shipper_health_not_running_pages(monkeypatch, _fresh_shipper_metrics):
    """The blackout detector: non-local env + central_url set + shipper
    NOT running → ERROR capture_message (page). This is the
    audit-2026-05-16 pathology the independent watchdog exists to catch."""
    _set_env(monkeypatch)
    _fresh_shipper_metrics.running = False
    fake_sentry = MagicMock()
    with patch("app.observability.sentry_sdk", fake_sentry):
        await observability.emit_shipper_health()
    fake_sentry.capture_message.assert_called_once()
    assert fake_sentry.capture_message.call_args.kwargs["level"] == "error"
    assert "not running" in fake_sentry.capture_message.call_args.args[0].lower()


@pytest.mark.asyncio
async def test_shipper_health_local_env_not_running_silent(monkeypatch, _fresh_shipper_metrics):
    """A local/standalone node legitimately runs no shipper → no signal."""
    _set_env(monkeypatch, environment="local")
    _fresh_shipper_metrics.running = False
    fake_sentry = MagicMock()
    with patch("app.observability.sentry_sdk", fake_sentry):
        await observability.emit_shipper_health()
    fake_sentry.capture_message.assert_not_called()


@pytest.mark.asyncio
async def test_shipper_health_no_central_url_not_running_silent(monkeypatch, _fresh_shipper_metrics):
    """No central_url configured (standalone) → not-running is expected."""
    _set_env(monkeypatch, central_url="")
    _fresh_shipper_metrics.running = False
    fake_sentry = MagicMock()
    with patch("app.observability.sentry_sdk", fake_sentry):
        await observability.emit_shipper_health()
    fake_sentry.capture_message.assert_not_called()


@pytest.mark.asyncio
async def test_shipper_health_running_healthy_silent(monkeypatch, _fresh_shipper_metrics):
    """Running, just-shipped, no outcomes → no signal."""
    import time as _t
    _set_env(monkeypatch)
    m = _fresh_shipper_metrics
    m.running = True
    m.last_ship_at = _t.time()  # lag ~0
    fake_sentry = MagicMock()
    with patch("app.observability.sentry_sdk", fake_sentry):
        await observability.emit_shipper_health()
    fake_sentry.capture_message.assert_not_called()


@pytest.mark.asyncio
async def test_shipper_health_lag_pages(monkeypatch, _fresh_shipper_metrics):
    """Running, last ship > threshold ago, AND a real backlog → ERROR
    (page) for stall. Passes a redis with backlog>0 so this exercises
    the GATE (not the no-redis fail-open path, which is covered
    separately) — i.e. it would catch a regression that removed the
    gate's page-on-backlog behaviour."""
    import time as _t
    _set_env(monkeypatch)
    m = _fresh_shipper_metrics
    m.running = True
    m.last_ship_at = _t.time() - 400  # > 300s threshold
    redis = _redis_with_group(pending=3, lag=0)
    fake_sentry = MagicMock()
    with patch("app.observability.sentry_sdk", fake_sentry):
        await observability.emit_shipper_health(redis)
    fake_sentry.capture_message.assert_called_once()
    assert fake_sentry.capture_message.call_args.kwargs["level"] == "error"
    assert "lag" in fake_sentry.capture_message.call_args.args[0].lower()


# F-3 (audit 2026-05-25) — lag-alert de-noise: gate on actual backlog.

def _redis_with_group(pending=0, lag=0, *, raise_exc=None, groups=None):
    """AsyncMock redis whose xinfo_groups returns the shipper group with
    the given pending/lag (or a custom groups list, or raises)."""
    r = AsyncMock()
    if raise_exc is not None:
        r.xinfo_groups = AsyncMock(side_effect=raise_exc)
    elif groups is not None:
        r.xinfo_groups = AsyncMock(return_value=groups)
    else:
        r.xinfo_groups = AsyncMock(return_value=[
            {"name": observability._SHIPPER_GROUP, "pending": pending, "lag": lag},
        ])
    return r


@pytest.mark.asyncio
async def test_shipper_health_lag_drained_suppressed(monkeypatch, _fresh_shipper_metrics):
    """THE fix: lag over threshold but the shipper group is fully drained
    (pending=0, lag=0) → idle, not a stall → NO page. This is the
    low-traffic false-alarm the audit found (2498 false pages/13h)."""
    import time as _t
    _set_env(monkeypatch)
    m = _fresh_shipper_metrics
    m.running = True
    m.last_ship_at = _t.time() - 400  # > 300s threshold
    redis = _redis_with_group(pending=0, lag=0)
    fake_sentry = MagicMock()
    with patch("app.observability.sentry_sdk", fake_sentry):
        await observability.emit_shipper_health(redis)
    fake_sentry.capture_message.assert_not_called()


@pytest.mark.asyncio
async def test_shipper_health_lag_pending_backlog_pages(monkeypatch, _fresh_shipper_metrics):
    """Lag over threshold AND clicks delivered-but-unacked (PEL>0) → a
    genuine in-flight stall → page."""
    import time as _t
    _set_env(monkeypatch)
    m = _fresh_shipper_metrics
    m.running = True
    m.last_ship_at = _t.time() - 400
    redis = _redis_with_group(pending=5, lag=0)
    fake_sentry = MagicMock()
    with patch("app.observability.sentry_sdk", fake_sentry):
        await observability.emit_shipper_health(redis)
    fake_sentry.capture_message.assert_called_once()
    assert fake_sentry.capture_message.call_args.kwargs["level"] == "error"
    assert "backlog=5" in fake_sentry.capture_message.call_args.args[0]


@pytest.mark.asyncio
async def test_shipper_health_lag_undelivered_backlog_pages(monkeypatch, _fresh_shipper_metrics):
    """Lag over threshold AND unread entries (group lag>0) → page."""
    import time as _t
    _set_env(monkeypatch)
    m = _fresh_shipper_metrics
    m.running = True
    m.last_ship_at = _t.time() - 400
    redis = _redis_with_group(pending=0, lag=7)
    fake_sentry = MagicMock()
    with patch("app.observability.sentry_sdk", fake_sentry):
        await observability.emit_shipper_health(redis)
    fake_sentry.capture_message.assert_called_once()
    assert "backlog=7" in fake_sentry.capture_message.call_args.args[0]


@pytest.mark.asyncio
async def test_shipper_health_lag_redis_error_fails_open(monkeypatch, _fresh_shipper_metrics):
    """Backlog UNKNOWN (xinfo_groups raises) → FAIL OPEN: the legacy
    lag alert still fires so a real stall is never silenced by the gate."""
    import time as _t
    _set_env(monkeypatch)
    m = _fresh_shipper_metrics
    m.running = True
    m.last_ship_at = _t.time() - 400
    redis = _redis_with_group(raise_exc=ConnectionError("redis down"))
    fake_sentry = MagicMock()
    with patch("app.observability.sentry_sdk", fake_sentry):
        await observability.emit_shipper_health(redis)
    fake_sentry.capture_message.assert_called_once()
    assert "backlog=unknown" in fake_sentry.capture_message.call_args.args[0]


@pytest.mark.asyncio
async def test_shipper_health_lag_group_absent_fails_open(monkeypatch, _fresh_shipper_metrics):
    """Group not present in xinfo_groups → backlog unknown → fail open."""
    import time as _t
    _set_env(monkeypatch)
    m = _fresh_shipper_metrics
    m.running = True
    m.last_ship_at = _t.time() - 400
    redis = _redis_with_group(groups=[{"name": "other", "pending": 9, "lag": 9}])
    fake_sentry = MagicMock()
    with patch("app.observability.sentry_sdk", fake_sentry):
        await observability.emit_shipper_health(redis)
    fake_sentry.capture_message.assert_called_once()


@pytest.mark.asyncio
async def test_shipper_health_lag_no_redis_fails_open(monkeypatch, _fresh_shipper_metrics):
    """No redis handle passed (legacy caller) → no gate → legacy page."""
    import time as _t
    _set_env(monkeypatch)
    m = _fresh_shipper_metrics
    m.running = True
    m.last_ship_at = _t.time() - 400
    fake_sentry = MagicMock()
    with patch("app.observability.sentry_sdk", fake_sentry):
        await observability.emit_shipper_health()  # no redis
    fake_sentry.capture_message.assert_called_once()


@pytest.mark.asyncio
async def test_shipper_health_lag_below_threshold_with_backlog_silent(monkeypatch, _fresh_shipper_metrics):
    """Backlog>0 but lag under threshold → still NO page (the threshold
    is the first gate; backlog only refines the over-threshold case)."""
    import time as _t
    _set_env(monkeypatch)
    m = _fresh_shipper_metrics
    m.running = True
    m.last_ship_at = _t.time() - 10  # well under 300s
    redis = _redis_with_group(pending=100, lag=100)
    fake_sentry = MagicMock()
    with patch("app.observability.sentry_sdk", fake_sentry):
        await observability.emit_shipper_health(redis)
    fake_sentry.capture_message.assert_not_called()


class TestShipperBacklogHelper:
    @pytest.mark.asyncio
    async def test_sums_pending_and_lag(self):
        redis = _redis_with_group(pending=3, lag=4)
        assert await observability._shipper_backlog(redis) == 7

    @pytest.mark.asyncio
    async def test_lag_none_treated_as_zero(self):
        redis = _redis_with_group(groups=[
            {"name": observability._SHIPPER_GROUP, "pending": 2, "lag": None},
        ])
        assert await observability._shipper_backlog(redis) == 2

    @pytest.mark.asyncio
    async def test_zero_backlog(self):
        redis = _redis_with_group(pending=0, lag=0)
        assert await observability._shipper_backlog(redis) == 0

    @pytest.mark.asyncio
    async def test_group_absent_returns_none(self):
        redis = _redis_with_group(groups=[{"name": "other", "pending": 1, "lag": 1}])
        assert await observability._shipper_backlog(redis) is None

    @pytest.mark.asyncio
    async def test_redis_error_returns_none(self):
        redis = _redis_with_group(raise_exc=RuntimeError("boom"))
        assert await observability._shipper_backlog(redis) is None

    @pytest.mark.asyncio
    async def test_decodes_bytes_group_name(self):
        redis = _redis_with_group(groups=[
            {"name": observability._SHIPPER_GROUP.encode(), "pending": 1, "lag": 2},
        ])
        assert await observability._shipper_backlog(redis) == 3

    @pytest.mark.asyncio
    async def test_decodes_bytes_lag_value(self):
        # Some redis-py configs return numeric fields as bytes — the
        # helper must normalise `lag` like it does the group name.
        redis = _redis_with_group(groups=[
            {"name": observability._SHIPPER_GROUP, "pending": 2, "lag": b"5"},
        ])
        assert await observability._shipper_backlog(redis) == 7


def test_observability_constants_match_shipper():
    """Parity guard — the mirrored stream/group constants MUST track
    shipper.py (they are duplicated to avoid a circular import)."""
    from app import shipper
    assert observability._STREAM_KEY == shipper.STREAM_KEY
    assert observability._SHIPPER_GROUP == shipper.GROUP_NAME


@pytest.mark.asyncio
async def test_shipper_health_low_success_ratio_warns(monkeypatch, _fresh_shipper_metrics):
    """Running, fresh ship, but success_ratio_5m < 0.95 with a meaningful
    sample → WARNING."""
    import time as _t
    _set_env(monkeypatch)
    m = _fresh_shipper_metrics
    m.running = True
    m.last_ship_at = _t.time()
    m.record_outcome(accepted=15, rejected=10)  # ratio 0.6, sample 25
    fake_sentry = MagicMock()
    with patch("app.observability.sentry_sdk", fake_sentry):
        await observability.emit_shipper_health()
    fake_sentry.capture_message.assert_called_once()
    assert fake_sentry.capture_message.call_args.kwargs["level"] == "warning"
    assert "success_ratio" in fake_sentry.capture_message.call_args.args[0].lower()


@pytest.mark.asyncio
async def test_shipper_health_low_ratio_small_sample_silent(monkeypatch, _fresh_shipper_metrics):
    """A lone rejected click (ratio 0.0 but sample < min) must NOT page —
    that's statistical noise, not an outage."""
    import time as _t
    _set_env(monkeypatch)
    m = _fresh_shipper_metrics
    m.running = True
    m.last_ship_at = _t.time()
    m.record_outcome(accepted=0, rejected=1)  # ratio 0.0, sample 1 < 20
    fake_sentry = MagicMock()
    with patch("app.observability.sentry_sdk", fake_sentry):
        await observability.emit_shipper_health()
    fake_sentry.capture_message.assert_not_called()


def test_window_sample_size_property():
    """ShipperMetrics.window_sample_size sums accepted+rejected over the
    rolling window."""
    from app.shipper_metrics import ShipperMetrics
    m = ShipperMetrics()
    assert m.window_sample_size == 0
    m.record_outcome(accepted=10, rejected=5)
    m.record_outcome(accepted=3, rejected=2)
    assert m.window_sample_size == 20


def test_read_side_prunes_stale_outcomes():
    """F.29 Sprint 4.1 validation-cycle-2 — outcomes older than the window
    are pruned on READ even when record_outcome is NOT called again (idle
    shipper). Without this the watchdog (which reads on its own task) would
    alert on a stale, over-counted window."""
    import time as _t
    from app.shipper_metrics import ShipperMetrics, _SUCCESS_RATIO_WINDOW_SECONDS
    m = ShipperMetrics()
    stale_ts = _t.time() - _SUCCESS_RATIO_WINDOW_SECONDS - 100
    # Record a stale low-ratio outcome; no further record_outcome fires.
    m.record_outcome(accepted=0, rejected=30, _now=stale_ts)
    # Read-side properties must prune it → no false "ratio 0.0" signal.
    assert m.window_sample_size == 0
    assert m.success_ratio_5m is None


class TestShipperHealthLifespanWiring:
    def test_loop_invokes_emit_shipper_health(self):
        """The observability loop body must call emit_shipper_health —
        otherwise the watchdog never runs."""
        source = inspect.getsource(observability.run_observability_loop)
        assert "emit_shipper_health" in source
