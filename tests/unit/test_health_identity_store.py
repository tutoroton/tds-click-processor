"""CAP-1 (2026-06-10) — /health identity-store saturation visibility.

The identity Redis is noeviction (D30): when FULL, identity writes fail
loud-but-swallowed — clicks still route, but returning recognition degrades
to "new" and sticky pins stop updating. That failure mode must never arrive
silently. /health now carries identity_store_{used,max,pct} on every probe
(docker healthcheck = every 10s ⇒ free cadence) and fires THROTTLED Sentry
signals: ≥80% warning, ≥95% error (once/hour per threshold per process).

Pins:
  * fields populated from INFO memory (used/max → pct);
  * pct None + NO alert when maxmemory=0 (unlimited — local dev);
  * ≥80% → warning, ≥95% → error (and only the higher one);
  * identity store unreachable → fields None, /health still 200;
  * HealthResponse schema round-trips the new fields.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.models import HealthResponse


@pytest.fixture
def client():
    from app.main import app
    return TestClient(app)


def _routing_redis():
    r = MagicMock()
    r.ping = AsyncMock(return_value=True)
    r.scard = AsyncMock(return_value=3)
    r.get = AsyncMock(return_value="7")
    r.xlen = AsyncMock(return_value=0)
    return r


def _identity_redis(used: int, maxmem: int):
    ir = MagicMock()
    ir.info = AsyncMock(return_value={"used_memory": used, "maxmemory": maxmem})
    return ir


def _get_health(client, used, maxmem):
    with patch("app.main.get_redis", new=AsyncMock(return_value=_routing_redis())), \
         patch("app.main.get_identity_redis",
               new=AsyncMock(return_value=_identity_redis(used, maxmem))), \
         patch("app.main.capture_op_msg_throttled") as cap:
        r = client.get("/health")
    return r, cap


def test_health_reports_identity_store_usage(client):
    r, cap = _get_health(client, used=10_000_000, maxmem=100_000_000)
    assert r.status_code == 200
    body = r.json()
    assert body["identity_store_used_bytes"] == 10_000_000
    assert body["identity_store_max_bytes"] == 100_000_000
    assert body["identity_store_used_pct"] == 10.0
    cap.assert_not_called()  # well under thresholds


def test_unlimited_maxmemory_yields_no_pct_and_no_alert(client):
    r, cap = _get_health(client, used=10_000_000, maxmem=0)
    body = r.json()
    assert body["identity_store_used_bytes"] == 10_000_000
    assert body["identity_store_max_bytes"] == 0
    assert body["identity_store_used_pct"] is None
    cap.assert_not_called()


def test_80_pct_fires_throttled_warning(client):
    r, cap = _get_health(client, used=85_000_000, maxmem=100_000_000)
    assert r.json()["identity_store_used_pct"] == 85.0
    cap.assert_called_once()
    args, kwargs = cap.call_args
    assert args[1] == "p80"
    assert kwargs["level"] == "warning"
    assert kwargs["window_sec"] == 3600.0


def test_95_pct_fires_error_not_warning(client):
    r, cap = _get_health(client, used=97_000_000, maxmem=100_000_000)
    assert r.json()["identity_store_used_pct"] == 97.0
    cap.assert_called_once()
    args, kwargs = cap.call_args
    assert args[1] == "p95"
    assert kwargs["level"] == "error"


def test_identity_store_unreachable_degrades_to_none_not_500(client):
    with patch("app.main.get_redis", new=AsyncMock(return_value=_routing_redis())), \
         patch("app.main.get_identity_redis",
               new=AsyncMock(side_effect=RuntimeError("identity down"))), \
         patch("app.main.capture_op_msg_throttled") as cap:
        r = client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["identity_store_used_bytes"] is None
    assert body["identity_store_max_bytes"] is None
    assert body["identity_store_used_pct"] is None
    cap.assert_not_called()


def test_health_response_schema_accepts_identity_fields():
    h = HealthResponse(
        node_id="n", region="eu", redis=True, campaigns_loaded=1,
        sync_version=1, uptime_seconds=1.0,
        identity_store_used_bytes=123,
        identity_store_max_bytes=456,
        identity_store_used_pct=27.0,
    )
    assert h.identity_store_used_pct == 27.0
    # Defaults stay None — legacy consumers unaffected.
    assert HealthResponse(
        node_id="n", region="eu", redis=True, campaigns_loaded=1,
        sync_version=1, uptime_seconds=1.0,
    ).identity_store_used_bytes is None
