"""F-4 MEDIUM (audit 2026-05-25) — /stats auth gate + smoke-probe startup alert.

`/stats` leaked node identity + redis size to any reachable caller. It is
now gated in non-local: the deploy `health.sh` probe curls it from LOOPBACK
(allowed unauthenticated, zero-config), any other caller must present the
node's X-TDS-Key. Local/dev stays open.

The smoke-probe authenticator gets a one-time startup alert (log + Sentry)
when unset in non-local — visibility without changing the deliberate
graceful-degradation (no boot guard) design.
"""

from __future__ import annotations

import inspect
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient


_SECRET = "k" * 40


@pytest.fixture
def client():
    from app.main import app
    return TestClient(app, raise_server_exceptions=False)


def _fake_redis() -> MagicMock:
    r = MagicMock()
    r.info = AsyncMock(return_value={"used_memory": 1024 * 1024})
    r.scard = AsyncMock(return_value=3)
    r.dbsize = AsyncMock(return_value=42)
    return r


def _patch_redis():
    return patch("app.main.get_redis", new=AsyncMock(return_value=_fake_redis()))


class TestStatsAuthGate:
    def test_local_env_open_without_key(self, client, monkeypatch):
        from app.config import settings
        monkeypatch.setattr(settings, "environment", "development")
        with _patch_redis():
            r = client.get("/stats")
        assert r.status_code == 200
        assert r.json()["redis_keys"] == 42

    def test_loopback_set_covers_v4_v6_and_mapped(self):
        """All loopback peer forms uvicorn can report (incl. the
        IPv4-mapped-IPv6 dual-stack form) must be in the carve-out set,
        else a dual-stack node would falsely gate the health probe."""
        from app.main import _LOOPBACK_HOSTS
        assert {"127.0.0.1", "::1", "::ffff:127.0.0.1"} <= _LOOPBACK_HOSTS

    def test_nonlocal_non_loopback_no_key_rejected(self, client, monkeypatch):
        from app.config import settings
        monkeypatch.setattr(settings, "environment", "production")
        monkeypatch.setattr(settings, "tds_secret_key", _SECRET)
        # TestClient's client host is "testclient" (non-loopback) → auth
        # required. This same branch covers request.client == None (→ "").
        r = client.get("/stats")  # no X-TDS-Key
        assert r.status_code == 403

    def test_nonlocal_valid_key_ok(self, client, monkeypatch):
        from app.config import settings
        monkeypatch.setattr(settings, "environment", "production")
        monkeypatch.setattr(settings, "tds_secret_key", _SECRET)
        with _patch_redis():
            r = client.get("/stats", headers={"X-TDS-Key": _SECRET})
        assert r.status_code == 200

    def test_nonlocal_loopback_open_without_key(self, client, monkeypatch):
        """The health.sh probe curls from loopback → allowed with no key.
        TestClient's host is "testclient"; treat it as loopback for this
        test to exercise the loopback-bypass branch."""
        from app import main
        from app.config import settings
        monkeypatch.setattr(settings, "environment", "production")
        monkeypatch.setattr(settings, "tds_secret_key", _SECRET)
        monkeypatch.setattr(
            main, "_LOOPBACK_HOSTS", frozenset({"127.0.0.1", "::1", "localhost", "testclient"}),
        )
        with _patch_redis():
            r = client.get("/stats")  # no key, but "loopback"
        assert r.status_code == 200


class TestSmokeProbeStartupAlert:
    def test_lifespan_warns_on_unset_smoke_secret_nonlocal(self):
        from app.main import lifespan
        source = inspect.getsource(lifespan)
        assert "TDS_SMOKE_PROBE_SECRET is empty" in source, (
            "lifespan must surface an unset smoke-probe secret in non-local."
        )
        assert "smoke_probe_secret" in source
        # It must be an ALERT, not a boot guard — no raise on this path.
        assert "smoke-probe secret unset in non-local" in source
