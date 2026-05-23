"""Tests for the F.29 Sprint 3.6 smoke-test click_id prefix bypass.

The admin-api ``EdgeNodeService._run_smoke_test`` (Sprint 3.2) emits
synthetic clicks with ``click_id`` prefixed ``smoke-test-`` to verify
the edge → shipper → central pipeline end-to-end. The /decide handler
short-circuits these clicks BEFORE the routing pipeline so they don't
pollute analytics OR consume routing CPU:

  * Skip campaign matching / Redis lookups / postback queue / dedup.
  * XADD a minimal ``smoke_test=True`` payload to ``stream:clicks`` so
    the shipper sends it to central as usual.
  * Return a benign 302 to the fallback URL.

Auth (X-TDS-Key) is still enforced upstream — smoke clicks come from
operator-invoked tooling against a legitimately deployed edge node with
the per-Worker secret available.

These tests pin:
  * Prefix matching is exact (``smoke-test-`` only — case-sensitive).
  * Real clicks (``click_id`` NOT prefixed) bypass the bypass and
    follow the normal routing path.
  * The XADD payload shape (``click_id``, ``node_id``, ``created_at_ms``,
    ``smoke_test=True``) — the admin-api smoke gate keys off the
    ``click_id`` field to detect arrival in central
    ``stream:clicks-incoming``.
  * The XADD failure is logged + Sentry-captured but does NOT raise
    (the smoke gate's 30s timeout is the upstream safety net).

Reference: F.29 plan-doc §13 Sprint 3 row 6.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client():
    """FastAPI test client — uses real `app` instance.

    We override the X-TDS-Key check at the module level (settings.tds_secret_key
    empty in local env makes _check_tds_key fail open via the fail-closed
    branch). Tests run with environment=local so the auth check is a no-op
    for empty-secret scenarios.
    """
    from app.main import app
    return TestClient(app)


@pytest.fixture
def patched_auth():
    """Bypass the X-TDS-Key auth check for smoke-bypass tests — the
    bypass behavior is orthogonal to auth, which is exercised by
    test_admin_auth_timing_safe.py. We patch the helper so the test
    doesn't depend on the auth path.
    """
    with patch("app.main._check_tds_key", new_callable=AsyncMock):
        yield


def _smoke_payload(click_id: str = "smoke-test-deadbeef") -> dict:
    """Minimal ClickRequest payload — just the fields needed for /decide
    to validate the body. Real fields like geo/UA are accepted by
    Pydantic's defaults."""
    return {
        "click_id": click_id,
        "ip": "127.0.0.1",
        "country": "ZZ",
        "user_agent": "geo-tds-smoke-test/1.0",
    }


# ---------------------------------------------------------------------------
# Smoke bypass: short-circuits routing + XADDs minimal record
# ---------------------------------------------------------------------------


def test_smoke_test_prefix_bypasses_routing_and_xadds(client, patched_auth):
    """Happy path: ``smoke-test-deadbeef`` click_id → bypass → XADD to
    ``stream:clicks`` with the canonical payload shape → 302 to fallback.
    The route() function MUST NOT be called for the bypass path."""
    fake_redis = MagicMock()
    fake_redis.xadd = AsyncMock(return_value="1-0")
    fake_route = AsyncMock(return_value={"url": "https://example.com"})

    with patch("app.main.get_redis", new=AsyncMock(return_value=fake_redis)), \
         patch("app.main.route", new=fake_route):
        r = client.post(
            "/decide",
            json=_smoke_payload("smoke-test-deadbeef"),
            headers={"X-TDS-Key": "ignored-by-patched-auth"},
        )

    assert r.status_code == 200
    body = r.json()
    assert body["status"] == 302
    assert body["url"].startswith("http")
    assert "smoke_test" in body["url"]  # reason=smoke_test in URL
    assert "click_id=smoke-test-deadbeef" in body["url"]

    # Routing pipeline MUST NOT have fired.
    fake_route.assert_not_awaited()

    # XADD was called with the canonical payload shape.
    fake_redis.xadd.assert_awaited_once()
    args, kwargs = fake_redis.xadd.await_args
    assert args[0] == "stream:clicks"
    field_dict = args[1]
    data = json.loads(field_dict["data"])
    assert data["click_id"] == "smoke-test-deadbeef"
    assert data["smoke_test"] is True
    assert "node_id" in data
    assert "created_at_ms" in data
    assert isinstance(data["created_at_ms"], int)


def test_non_smoke_click_takes_normal_route_path(client, patched_auth):
    """Defensive: a ``click_id`` that does NOT start with ``smoke-test-``
    MUST traverse the normal routing path (route() is called)."""
    fake_redis = MagicMock()
    fake_redis.xadd = AsyncMock(return_value="1-0")
    fake_route = AsyncMock(return_value={"url": "https://target.com", "status": 302})

    with patch("app.main.get_redis", new=AsyncMock(return_value=fake_redis)), \
         patch("app.main.route", new=fake_route):
        r = client.post(
            "/decide",
            json=_smoke_payload("real-click-xyz-123"),
            headers={"X-TDS-Key": "ignored-by-patched-auth"},
        )

    assert r.status_code == 200
    # Normal route path fired.
    fake_route.assert_awaited_once()
    # The smoke XADD path did NOT fire (route's internal XADDs are
    # separate and orchestrated elsewhere — they wouldn't go through
    # the patched fake_redis.xadd unless route() invokes our patched
    # get_redis, which it does NOT in this test scope).


def test_smoke_prefix_match_is_case_sensitive(client, patched_auth):
    """``Smoke-Test-...`` or ``SMOKE-TEST-...`` are NOT bypassed — only
    lowercase ``smoke-test-`` triggers the short-circuit. This avoids
    ambiguity if a real campaign were to legitimately use a similar-
    looking prefix in a different case."""
    fake_redis = MagicMock()
    fake_redis.xadd = AsyncMock(return_value="1-0")
    fake_route = AsyncMock(return_value=None)  # no match path

    with patch("app.main.get_redis", new=AsyncMock(return_value=fake_redis)), \
         patch("app.main.route", new=fake_route):
        # Mixed case — must NOT trigger the bypass.
        r = client.post(
            "/decide",
            json=_smoke_payload("Smoke-Test-deadbeef"),
            headers={"X-TDS-Key": "ignored-by-patched-auth"},
        )

    assert r.status_code == 200
    fake_route.assert_awaited_once()  # took normal path


def test_smoke_xadd_failure_does_not_raise(client, patched_auth):
    """If the smoke XADD itself fails (Redis impairment), the handler
    must NOT propagate the exception — it logs + Sentry-captures and
    still returns 302. The admin-api smoke gate's 30s timeout is the
    upstream safety net that surfaces the failure as ``smoke_testing``
    + ``error_message``."""
    fake_redis = MagicMock()
    fake_redis.xadd = AsyncMock(side_effect=RuntimeError("redis impairment"))
    fake_route = AsyncMock()
    fake_sentry = MagicMock()

    with patch("app.main.get_redis", new=AsyncMock(return_value=fake_redis)), \
         patch("app.main.route", new=fake_route), \
         patch("app.main.sentry_sdk", fake_sentry):
        # MUST NOT raise — even with Redis broken.
        r = client.post(
            "/decide",
            json=_smoke_payload("smoke-test-x"),
            headers={"X-TDS-Key": "ignored-by-patched-auth"},
        )

    assert r.status_code == 200
    body = r.json()
    assert body["status"] == 302
    # Sentry capture fires.
    fake_sentry.capture_exception.assert_called_once()
    # Route path NOT taken — smoke bypass short-circuited before raising.
    fake_route.assert_not_awaited()
