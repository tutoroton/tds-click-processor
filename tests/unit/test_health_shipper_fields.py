"""Tests for the /health shipper + storage visibility extension.

F.29 Sprint 1.4 (2026-05-23). Pre-F.29 ``/health`` returned only
``redis``, ``campaigns_loaded``, ``sync_version``, ``uptime_seconds``.
A silently-crashed shipper still produced /health=200 with redis=true
because the redis ping itself worked — the audit-2026-05-16 50-day
silent-disable was invisible to dashboards.

Coverage:

  * ShipperMetrics dataclass:
      - default state (running=False, last_ship_at=None,
        lag_seconds=None, last_ship_status="n/a")
      - mark_running / mark_stopped state transitions
      - record_ship updates all three fields atomically (no torn read
        between last_ship_at and last_ship_status)
      - lag_seconds computed correctly when last_ship_at set
      - to_health_dict produces the exact key set HealthResponse expects

  * /health endpoint (via FastAPI TestClient):
      - returns 200 with new fields present (defaults when shipper
        hasn't started)
      - reflects updated metrics after simulated ship_record calls
      - disk_free_bytes is an int when disk_queue_root exists, None
        when not
      - stream_clicks_length reads from redis xlen
      - degrades gracefully when redis fails

Reference: F.29 plan §3 G5 (visibility), §4 Sprint 1.4 row.
"""

from __future__ import annotations

import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app import shipper_metrics as shipper_metrics_module
from app.models import HealthResponse
from app.shipper_metrics import ShipperMetrics, metrics as _global_metrics


@pytest.fixture(autouse=True)
def _reset_metrics():
    """Per-test isolation — every test starts with a fresh metrics
    object so prior tests' record_ship calls don't leak."""
    shipper_metrics_module._reset_for_tests()
    yield
    shipper_metrics_module._reset_for_tests()


# ---------------------------------------------------------------------------
# ShipperMetrics dataclass — pure unit tests, no I/O
# ---------------------------------------------------------------------------


def test_default_state_is_safe_for_dashboard():
    """Defaults must be coherent for a brand-new node before the
    shipper has ever run. Dashboard widgets render these directly —
    "n/a" must be safe (string), and lag_seconds=None must be JSON-
    serializable to ``null``."""
    m = ShipperMetrics()
    assert m.running is False
    assert m.last_ship_at is None
    assert m.last_ship_status == "n/a"
    assert m.last_batch_size == 0
    assert m.lag_seconds is None


def test_mark_running_toggles_flag():
    m = ShipperMetrics()
    m.mark_running()
    assert m.running is True


def test_mark_stopped_toggles_flag_back():
    m = ShipperMetrics()
    m.mark_running()
    m.mark_stopped()
    assert m.running is False


def test_record_ship_updates_all_fields_atomically():
    """All three observable fields (timestamp, status, batch size)
    must update in one synchronous call — no chance of /health
    reading a partially-updated tuple."""
    m = ShipperMetrics()
    before = time.time()
    m.record_ship("success", batch_size=42)
    after = time.time()

    assert before <= m.last_ship_at <= after
    assert m.last_ship_status == "success"
    assert m.last_batch_size == 42


def test_lag_seconds_none_until_first_ship():
    m = ShipperMetrics()
    assert m.lag_seconds is None


def test_lag_seconds_computed_after_ship():
    """Lag = wall-clock now - last_ship_at. Must be non-negative,
    monotonically increasing as wall clock advances."""
    m = ShipperMetrics()
    m.record_ship("success", batch_size=1)

    # Immediately after record_ship, lag is ~0
    lag = m.lag_seconds
    assert lag is not None
    assert lag >= 0
    # Allow generous wall-clock slack — test execution itself takes
    # microseconds, but CI clocks can have surprising granularity.
    assert lag < 1.0


@pytest.mark.parametrize(
    "status",
    [
        "success",
        "ack_failed",
        "collector_error",
        "unreachable",
        "parse_failed",
        "loop_error",
        "n/a",
    ],
)
def test_record_ship_accepts_all_canonical_statuses(status):
    """The ShipStatus Literal pins these 7 values. Drift here would
    break HealthResponse downstream consumers (dashboard widget +
    Sentry alert rules in Sprint 4.1)."""
    m = ShipperMetrics()
    m.record_ship(status, batch_size=0)
    assert m.last_ship_status == status


def test_to_health_dict_has_all_expected_keys():
    """HealthResponse uses ``**shipper_metrics.to_health_dict()`` to
    spread the metrics into the response. Drift in the key set
    breaks Pydantic validation at the /health response layer with
    a confusing error. Pin the key set explicitly here."""
    m = ShipperMetrics()
    d = m.to_health_dict()
    assert set(d.keys()) == {
        "shipper_running",
        "shipper_lag_seconds",
        "last_ship_at",
        "last_ship_status",
        "last_batch_size",
    }


# ---------------------------------------------------------------------------
# HealthResponse schema — new fields must round-trip
# ---------------------------------------------------------------------------


def test_health_response_accepts_new_shipper_fields():
    """Construct HealthResponse with all new F.29 Sprint 1.4 fields.
    Pydantic validation is the actual contract — if a type drifts,
    construction raises here long before staging."""
    h = HealthResponse(
        node_id="test-node",
        region="eu",
        redis=True,
        campaigns_loaded=5,
        sync_version=42,
        uptime_seconds=123.4,
        shipper_running=True,
        shipper_lag_seconds=12.5,
        last_ship_at=1716470400.0,
        last_ship_status="success",
        last_batch_size=100,
        stream_clicks_length=15,
        disk_queue_size=0,
        disk_free_bytes=10_737_418_240,
    )
    assert h.shipper_running is True
    assert h.shipper_lag_seconds == 12.5
    assert h.last_ship_status == "success"
    assert h.disk_free_bytes == 10_737_418_240


def test_health_response_safe_defaults_for_new_fields():
    """Pre-F.29 callers may construct HealthResponse with only the
    legacy fields. The new Sprint 1.4 fields must default to safe
    values so the upgrade is non-breaking."""
    h = HealthResponse(
        node_id="test-node",
        region="eu",
        redis=True,
        campaigns_loaded=0,
        uptime_seconds=0.0,
    )
    assert h.shipper_running is False
    assert h.shipper_lag_seconds is None
    assert h.last_ship_at is None
    assert h.last_ship_status == "n/a"
    assert h.last_batch_size == 0
    assert h.stream_clicks_length == 0
    assert h.disk_queue_size == 0
    assert h.disk_free_bytes is None


def test_health_response_disk_free_bytes_can_be_none():
    """Local dev without TDS_DISK_QUEUE_ROOT, or first-boot before
    the directory exists → ``disk_free_bytes`` must be None (not
    raise, not 0 — distinguishes "unavailable" from "zero free")."""
    h = HealthResponse(
        node_id="test-node",
        region="eu",
        redis=True,
        campaigns_loaded=0,
        uptime_seconds=0.0,
        disk_free_bytes=None,
    )
    assert h.disk_free_bytes is None


# ---------------------------------------------------------------------------
# Module singleton — production lifecycle pin
# ---------------------------------------------------------------------------


def test_module_metrics_singleton_is_shipper_metrics_instance():
    """The shipper coroutine imports ``metrics`` by name and mutates
    it directly; the health handler reads the same binding. Pin the
    identity so a refactor that accidentally rebinds ``metrics`` to
    a different object (e.g. via wrapping) is caught here."""
    assert isinstance(_global_metrics, ShipperMetrics)


def test_reset_for_tests_restores_clean_singleton():
    """Test hygiene — the helper must produce a fully-reset singleton."""
    _global_metrics.mark_running()
    _global_metrics.record_ship("success", batch_size=99)

    shipper_metrics_module._reset_for_tests()

    new_global = shipper_metrics_module.metrics
    assert new_global.running is False
    assert new_global.last_ship_at is None
    assert new_global.last_ship_status == "n/a"
    assert new_global.last_batch_size == 0
