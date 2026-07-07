"""LOSSFIX P3 (2026-07-07) — L6: /health depth + backpressure emit.

Exposes watermark state, the M1 stream-backpressure decision, and the
live dedup TTL so an operator (and the P4 abort-guard) can read a
node's spill/backpressure posture from ONE /health call. Every new
field is a CACHED read — never a new Redis round-trip on /health's
hot polling path (docker healthcheck ~10s cadence).

Pins:
  * JSON shape — all six new fields present and correctly typed.
  * Hot-path-safety — /health issues the SAME Redis calls as before
    this phase (no new round-trip introduced for the new fields).
  * watermark_sample_age_seconds is None (not `Infinity`, invalid
    JSON) when the sampler never landed a first reading.
  * stream_backpressure_active reflects the CACHED M1 signal exactly
    (reuses `_check_stream_backpressure()`, not a fresh live XLEN).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.models import HealthResponse
from app.watermark import watermark_state


@pytest.fixture
def client():
    from app.main import app
    return TestClient(app)


@pytest.fixture(autouse=True)
def _reset_watermark():
    watermark_state.reset_for_tests()
    yield
    watermark_state.reset_for_tests()


def _routing_redis():
    r = MagicMock()
    r.ping = AsyncMock(return_value=True)
    r.scard = AsyncMock(return_value=3)
    r.get = AsyncMock(return_value="7")
    r.xlen = AsyncMock(return_value=0)
    return r


def _get_health(client, cached_stream_length=None):
    with patch("app.main.get_redis", new=AsyncMock(return_value=_routing_redis())), \
         patch("app.main.get_identity_redis", new=AsyncMock(side_effect=RuntimeError())), \
         patch("app.main.get_cached_stream_clicks_length", return_value=cached_stream_length):
        return client.get("/health")


def test_health_reports_watermark_and_backpressure_shape(client, monkeypatch):
    monkeypatch.setattr(watermark_state, "spill_mode", True)
    monkeypatch.setattr(watermark_state, "used_memory_pct", 91.2)
    watermark_state.record_sample(91.2)  # lands a real sample -> finite age

    r = _get_health(client)
    assert r.status_code == 200
    body = r.json()
    assert body["watermark_spill_mode"] is True
    assert body["watermark_used_memory_pct"] == 91.2
    assert isinstance(body["watermark_sample_age_seconds"], float)
    assert body["watermark_sample_age_seconds"] >= 0.0


def test_never_sampled_watermark_age_is_none_not_infinity(client):
    """`float('inf')` would serialize to the non-standard JSON literal
    `Infinity` — must degrade to `null` instead."""
    r = _get_health(client)
    body = r.json()
    assert body["watermark_sample_age_seconds"] is None
    assert body["watermark_spill_mode"] is False


def test_stream_backpressure_active_reflects_cached_m1_signal(client):
    from app.config import settings

    r = _get_health(client, cached_stream_length=settings.stream_clicks_maxlen)
    body = r.json()
    assert body["stream_clicks_reject_threshold"] == settings.stream_clicks_maxlen
    assert body["stream_backpressure_active"] is True


def test_stream_backpressure_inactive_below_threshold(client):
    r = _get_health(client, cached_stream_length=1)
    assert r.json()["stream_backpressure_active"] is False


def test_stream_backpressure_fails_open_when_never_sampled(client):
    r = _get_health(client, cached_stream_length=None)
    assert r.json()["stream_backpressure_active"] is False


def test_health_reports_live_dedup_ttl(client):
    from app.config import settings

    r = _get_health(client)
    assert r.json()["click_dedup_ttl_seconds"] == settings.click_dedup_ttl_seconds


def test_health_new_fields_issue_no_extra_redis_calls(client):
    """Hot-path-safety (L6 MUST): the new fields are pure in-process
    reads (watermark_state attributes, a cached-length lookup) — they
    must not add a SINGLE extra call to the routing Redis mock beyond
    what /health already issued pre-P3."""
    redis = _routing_redis()
    with patch("app.main.get_redis", new=AsyncMock(return_value=redis)), \
         patch("app.main.get_identity_redis", new=AsyncMock(side_effect=RuntimeError())), \
         patch("app.main.get_cached_stream_clicks_length", return_value=100):
        r = client.get("/health")
    assert r.status_code == 200
    # Pre-existing /health calls only: ping, scard, get(sync:version),
    # xlen(stream_length). Nothing from the new L6 block.
    assert redis.ping.await_count == 1
    assert redis.scard.await_count == 1
    assert redis.get.await_count == 1
    assert redis.xlen.await_count == 1


def test_health_response_schema_accepts_l6_fields():
    h = HealthResponse(
        node_id="n", region="eu", redis=True, campaigns_loaded=1,
        sync_version=1, uptime_seconds=1.0,
        watermark_spill_mode=True,
        watermark_used_memory_pct=42.0,
        watermark_sample_age_seconds=3.5,
        stream_clicks_reject_threshold=300_000,
        stream_backpressure_active=True,
        click_dedup_ttl_seconds=600,
    )
    assert h.watermark_spill_mode is True
    assert h.stream_backpressure_active is True
    # Defaults stay sane — legacy consumers unaffected.
    defaults = HealthResponse(
        node_id="n", region="eu", redis=True, campaigns_loaded=1,
        sync_version=1, uptime_seconds=1.0,
    )
    assert defaults.watermark_spill_mode is False
    assert defaults.watermark_sample_age_seconds is None
    assert defaults.stream_backpressure_active is False
