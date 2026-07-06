"""GTD-R75 / ADR-0055 — /health reports the EFFECTIVE capacity config.

The honest capacity-verification loop: admin-api must never trust its own
pure computation of what config a node received — it has to ASK the node.
/health carries `web_concurrency` (read straight from this process's own
`WEB_CONCURRENCY` env — the same variable + default the Dockerfile CMD used
to pick the worker count) and `redis_max_connections` (the pydantic-resolved
`settings.redis_max_connections`, already reflecting whatever env/default
actually took effect). Pure presentation — no F4 pool/flow-read logic
touched.

Pins:
  * web_concurrency reflects the process's actual WEB_CONCURRENCY env var;
  * absent env ⇒ the Dockerfile's own default (2), never invented;
  * redis_max_connections reflects settings.redis_max_connections verbatim;
  * HealthResponse schema round-trips both fields with safe defaults.
"""

from __future__ import annotations

import os
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


def _get_health(client):
    with patch("app.main.get_redis", new=AsyncMock(return_value=_routing_redis())), \
         patch("app.main.get_identity_redis",
               new=AsyncMock(side_effect=RuntimeError("identity off in this test"))):
        return client.get("/health")


def test_health_reports_web_concurrency_from_env(client, monkeypatch):
    monkeypatch.setenv("WEB_CONCURRENCY", "8")
    body = _get_health(client).json()
    assert body["web_concurrency"] == 8


def test_health_web_concurrency_defaults_to_2_when_env_unset(client, monkeypatch):
    monkeypatch.delenv("WEB_CONCURRENCY", raising=False)
    body = _get_health(client).json()
    # Same default the Dockerfile CMD's ${WEB_CONCURRENCY:-2} would pick.
    assert body["web_concurrency"] == 2


def test_health_reports_settings_redis_max_connections(client):
    body = _get_health(client).json()
    from app.config import settings
    assert body["redis_max_connections"] == settings.redis_max_connections


def test_health_response_schema_accepts_capacity_fields():
    h = HealthResponse(
        node_id="n", region="eu", redis=True, campaigns_loaded=1,
        sync_version=1, uptime_seconds=1.0,
        web_concurrency=8, redis_max_connections=256,
    )
    assert h.web_concurrency == 8
    assert h.redis_max_connections == 256
    # Safe defaults for legacy construction (no consumer breaks).
    defaults = HealthResponse(
        node_id="n", region="eu", redis=True, campaigns_loaded=1,
        sync_version=1, uptime_seconds=1.0,
    )
    assert defaults.web_concurrency == 2
    assert defaults.redis_max_connections == 128
