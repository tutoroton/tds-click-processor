"""Tests for M8 fix — /admin/seed environment gate.

Was `if settings.environment == "production": raise 403` — only the
exact string "production" was blocked. A `TDS_ENVIRONMENT=staging`,
or any typo like `TDS_ENVIRONMENT=Production` (capital P), silently
passed the gate. Combined with H6 (auth fail-open when
`tds_secret_key=""`), unauthenticated callers could overwrite
routing Redis with hardcoded placeholder campaigns.

M8 fix: use the `_LOCAL_ENVIRONMENTS` frozenset (already maintained
in config.py for the `_enforce_secret_presence` guard) as the single
source of truth. Allow seed ONLY in `{"local", "development"}`.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client():
    """FastAPI test client — uses real `app` instance.

    We intentionally avoid mocking the `decide` path; only the
    `/admin/seed` endpoint is exercised here.
    """
    from app.main import app

    return TestClient(app)


# ---------------------------------------------------------------------------
# Environment gate behaviour
# ---------------------------------------------------------------------------


class TestSeedEnvGate:
    """The gate must allow ONLY local/development environments."""

    def test_local_does_not_403_on_env_gate(self, client):
        """`environment=local` short-circuits the env gate — the
        seed handler proceeds past the env check. We don't
        validate the Redis-pipeline path itself (separate concern,
        covered by integration tests); only that the env gate is
        no longer the blocker."""
        from app import main

        with patch.object(main.settings, "environment", "local"):
            r = client.post("/admin/seed")
        # Critical assertion: response is NOT the env-gate 403.
        # It MAY 500 due to Redis being unavailable in test env,
        # but the env gate has passed.
        if r.status_code == 403:
            assert "Seed disabled" not in r.text, (
                f"local env should bypass the seed env gate; got 403 with: {r.text}"
            )

    def test_development_does_not_403_on_env_gate(self, client):
        from app import main

        with patch.object(main.settings, "environment", "development"):
            r = client.post("/admin/seed")
        if r.status_code == 403:
            assert "Seed disabled" not in r.text

    def test_staging_blocked(self, client):
        """`environment=staging` MUST 403 — was the silent bypass."""
        from app import main

        with patch.object(main.settings, "environment", "staging"):
            r = client.post("/admin/seed")
        assert r.status_code == 403
        assert "Seed disabled" in r.text
        assert "local/development" in r.text

    def test_production_blocked(self, client):
        """`environment=production` MUST 403 (was already blocked
        pre-fix; assert that the new gate preserves the behaviour)."""
        from app import main

        with patch.object(main.settings, "environment", "production"):
            r = client.post("/admin/seed")
        assert r.status_code == 403
        assert "Seed disabled" in r.text

    def test_typo_blocked(self, client):
        """`TDS_ENVIRONMENT=Production` (capital P, common typo)
        previously slipped through `== "production"`. Now MUST 403
        because it's not in `_LOCAL_ENVIRONMENTS = {local, development}`."""
        from app import main

        for typo in ("Production", "PRODUCTION", "prod", "staging-2", "prd"):
            with patch.object(main.settings, "environment", typo):
                r = client.post("/admin/seed")
            assert r.status_code == 403, (
                f"environment={typo!r} should be 403 (only "
                f"local/development allow seed); got {r.status_code}"
            )
            assert "Seed disabled" in r.text


# ---------------------------------------------------------------------------
# Source-pin
# ---------------------------------------------------------------------------


class TestSourcePin:
    def test_seed_handler_uses_local_environments_frozenset(self):
        """A refactor that drops the frozenset back to `==` is
        exactly the regression we're closing. Pin the source."""
        import inspect
        from app import main

        src = inspect.getsource(main.seed_data)
        assert "_LOCAL_ENVIRONMENTS" in src, (
            "seed_data MUST gate on _LOCAL_ENVIRONMENTS membership, "
            "NOT a string comparison — M8 fix design."
        )
        assert 'environment == "production"' not in src, (
            "Old `environment == \"production\"` pattern must not "
            "return — it's the bypass that M8 closed."
        )
