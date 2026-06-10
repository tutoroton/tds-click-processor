"""Tests for the F.29 Sprint 3.6 smoke-test click_id prefix bypass.

The admin-api ``EdgeNodeService._run_smoke_test`` (Sprint 3.2) emits
synthetic clicks with ``click_id`` prefixed ``smoke-test-`` to verify
the edge → shipper → central pipeline end-to-end. The /decide handler
short-circuits these clicks BEFORE the routing pipeline so they don't
pollute analytics OR consume routing CPU:

  * Skip campaign matching / Redis lookups / postback queue / dedup.
  * XADD a minimal ``smoke_test=True`` payload to ``stream:clicks`` so
    the shipper sends it to central as usual.
  * Return a benign worker-fallback 302 signal (F-2: the node
    carries no fallback URL; the Worker owns the destination).

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
    ``stream:clicks`` with the canonical payload shape → worker-fallback 302.
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
    # F-2: the node carries no fallback URL — the smoke bypass answers the
    # worker-fallback signal (the smoke gate only inspects the central
    # stream, never this body).
    assert body["url"] == ""
    assert body["fallback"] is True
    assert body["fallback_reason"] == "smoke_test"

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


# ---------------------------------------------------------------------------
# F.29 Sprint 4.1 (TD-13) — HMAC smoke-probe enforcement
# ---------------------------------------------------------------------------
#
# When ``settings.smoke_probe_secret`` is configured, the smoke bypass
# REQUIRES a valid ``X-TDS-Smoke-Probe: <issued_at>.<sig>`` header. These
# tests pin the fail-closed contract: valid → bypass, anything else → 403
# (no XADD, no routing). The default empty secret (covered by the tests
# above) preserves the pre-Sprint-4.1 fallback behaviour.

import hashlib
import hmac
import time

from app.config import settings


_PROBE_SECRET = "smoke-probe-secret-distinct-from-x-tds-key-0001"


def _make_probe(secret: str, click_id: str, issued_at: int | None = None) -> str:
    """Construct an ``X-TDS-Smoke-Probe`` header value (mirrors the
    admin-api signing in ``EdgeNodeService._run_smoke_test``)."""
    issued_at = int(time.time()) if issued_at is None else issued_at
    sig = hmac.new(
        secret.encode(), f"{click_id}.{issued_at}".encode(), hashlib.sha256,
    ).hexdigest()
    return f"{issued_at}.{sig}"


@pytest.fixture
def enforce_probe():
    """Enable TD-13 enforce mode by configuring the probe secret."""
    with patch.object(settings, "smoke_probe_secret", _PROBE_SECRET):
        yield


def test_smoke_probe_valid_allows_bypass(client, patched_auth, enforce_probe):
    """Valid HMAC probe → bypass proceeds (XADD + 302), exactly as the
    unauthenticated path did before TD-13."""
    fake_redis = MagicMock()
    fake_redis.xadd = AsyncMock(return_value="1-0")
    fake_route = AsyncMock()
    click_id = "smoke-test-99-deadbeef00112233"

    with patch("app.main.get_redis", new=AsyncMock(return_value=fake_redis)), \
         patch("app.main.route", new=fake_route):
        r = client.post(
            "/decide",
            json=_smoke_payload(click_id),
            headers={
                "X-TDS-Key": "ignored-by-patched-auth",
                "X-TDS-Smoke-Probe": _make_probe(_PROBE_SECRET, click_id),
            },
        )

    assert r.status_code == 200
    assert r.json()["status"] == 302
    fake_redis.xadd.assert_awaited_once()
    fake_route.assert_not_awaited()


def test_smoke_probe_missing_refused(client, patched_auth, enforce_probe):
    """Enforce mode + no probe header → 403, NO XADD, NO routing."""
    fake_redis = MagicMock()
    fake_redis.xadd = AsyncMock(return_value="1-0")
    fake_route = AsyncMock()

    with patch("app.main.get_redis", new=AsyncMock(return_value=fake_redis)), \
         patch("app.main.route", new=fake_route):
        r = client.post(
            "/decide",
            json=_smoke_payload("smoke-test-99-deadbeef00112233"),
            headers={"X-TDS-Key": "ignored-by-patched-auth"},
        )

    assert r.status_code == 403
    assert "probe" in r.json()["detail"].lower()
    fake_redis.xadd.assert_not_awaited()
    fake_route.assert_not_awaited()


def test_smoke_probe_invalid_signature_refused(client, patched_auth, enforce_probe):
    """A probe signed with the WRONG secret → 403 (signature mismatch)."""
    fake_redis = MagicMock()
    fake_redis.xadd = AsyncMock(return_value="1-0")
    click_id = "smoke-test-99-deadbeef00112233"

    with patch("app.main.get_redis", new=AsyncMock(return_value=fake_redis)), \
         patch("app.main.route", new=AsyncMock()):
        r = client.post(
            "/decide",
            json=_smoke_payload(click_id),
            headers={
                "X-TDS-Key": "ignored-by-patched-auth",
                "X-TDS-Smoke-Probe": _make_probe("the-wrong-secret-xxxxxxxxxxxxxxx", click_id),
            },
        )

    assert r.status_code == 403
    fake_redis.xadd.assert_not_awaited()


def test_smoke_probe_expired_refused(client, patched_auth, enforce_probe):
    """A probe whose ``issued_at`` is outside the freshness window → 403.
    Bounds replay of a captured (smoke_id, sig) pair."""
    fake_redis = MagicMock()
    fake_redis.xadd = AsyncMock(return_value="1-0")
    click_id = "smoke-test-99-deadbeef00112233"
    stale = int(time.time()) - 9999  # well past the 120s window

    with patch("app.main.get_redis", new=AsyncMock(return_value=fake_redis)), \
         patch("app.main.route", new=AsyncMock()):
        r = client.post(
            "/decide",
            json=_smoke_payload(click_id),
            headers={
                "X-TDS-Key": "ignored-by-patched-auth",
                "X-TDS-Smoke-Probe": _make_probe(_PROBE_SECRET, click_id, issued_at=stale),
            },
        )

    assert r.status_code == 403
    assert "expired" in r.json()["detail"].lower()
    fake_redis.xadd.assert_not_awaited()


def test_smoke_probe_tampered_click_id_refused(client, patched_auth, enforce_probe):
    """A probe signed for a DIFFERENT click_id → 403. Prevents retargeting
    a captured header (which binds the node_id-embedding smoke_id) to a
    victim node."""
    fake_redis = MagicMock()
    fake_redis.xadd = AsyncMock(return_value="1-0")
    # Probe signed for node 42, but the request targets node 99.
    probe_for_other = _make_probe(_PROBE_SECRET, "smoke-test-42-aaaaaaaaaaaaaaaa")

    with patch("app.main.get_redis", new=AsyncMock(return_value=fake_redis)), \
         patch("app.main.route", new=AsyncMock()):
        r = client.post(
            "/decide",
            json=_smoke_payload("smoke-test-99-deadbeef00112233"),
            headers={
                "X-TDS-Key": "ignored-by-patched-auth",
                "X-TDS-Smoke-Probe": probe_for_other,
            },
        )

    assert r.status_code == 403
    fake_redis.xadd.assert_not_awaited()


def test_smoke_probe_future_dated_refused(client, patched_auth, enforce_probe):
    """F.29 validation-cycle-2 — a probe dated far in the FUTURE is rejected
    (asymmetric freshness). An abs()-based window would have accepted it for
    up to 2× the freshness window."""
    fake_redis = MagicMock()
    fake_redis.xadd = AsyncMock(return_value="1-0")
    click_id = "smoke-test-99-deadbeef00112233"
    future = int(time.time()) + 9999

    with patch("app.main.get_redis", new=AsyncMock(return_value=fake_redis)), \
         patch("app.main.route", new=AsyncMock()):
        r = client.post(
            "/decide",
            json=_smoke_payload(click_id),
            headers={
                "X-TDS-Key": "ignored-by-patched-auth",
                "X-TDS-Smoke-Probe": _make_probe(_PROBE_SECRET, click_id, issued_at=future),
            },
        )

    assert r.status_code == 403
    assert "future" in r.json()["detail"].lower()
    fake_redis.xadd.assert_not_awaited()


def test_smoke_probe_malformed_header_refused(client, patched_auth, enforce_probe):
    """A header without the ``<issued_at>.<sig>`` shape → 403 (malformed)."""
    fake_redis = MagicMock()
    fake_redis.xadd = AsyncMock(return_value="1-0")

    with patch("app.main.get_redis", new=AsyncMock(return_value=fake_redis)), \
         patch("app.main.route", new=AsyncMock()):
        r = client.post(
            "/decide",
            json=_smoke_payload("smoke-test-99-deadbeef00112233"),
            headers={
                "X-TDS-Key": "ignored-by-patched-auth",
                "X-TDS-Smoke-Probe": "garbage-no-dot",
            },
        )

    assert r.status_code == 403
    fake_redis.xadd.assert_not_awaited()


# ---------------------------------------------------------------------------
# _verify_smoke_probe — direct unit tests (no HTTP layer)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# F.33 (2026-05-24) — smoke probe authenticates a FRESH node (chicken-and-egg)
# ---------------------------------------------------------------------------
#
# The activation smoke gate runs BEFORE seed, so a freshly-provisioned node's
# per-Worker `worker_secret_hash` index is EMPTY → `_check_tds_key` 403s every
# X-TDS-Key. The fix: a smoke click authenticates via the X-TDS-Smoke-Probe
# HMAC and SKIPS `_check_tds_key` entirely. These pin that the per-Worker
# check is bypassed when a valid probe is present, and still enforced
# otherwise.


def test_valid_probe_skips_per_worker_check_on_fresh_node():
    """THE chicken-and-egg fix: with the probe secret set + a valid probe, the
    smoke click succeeds EVEN WHEN `_check_tds_key` would 403 (a fresh node's
    empty worker index). Proven by making `_check_tds_key` raise 403 — the
    request must still return 200 because the probe path skips it."""
    from fastapi import HTTPException
    from app.main import app
    client = TestClient(app)
    click_id = "smoke-test-99-deadbeef00112233"
    fake_redis = MagicMock()
    fake_redis.xadd = AsyncMock(return_value="1-0")
    # Simulate a FRESH node: the per-Worker check fail-closes (403).
    fresh_node_403 = AsyncMock(side_effect=HTTPException(status_code=403, detail="Invalid TDS key"))

    with patch.object(settings, "smoke_probe_secret", _PROBE_SECRET), \
         patch("app.main._check_tds_key", new=fresh_node_403), \
         patch("app.main.get_redis", new=AsyncMock(return_value=fake_redis)), \
         patch("app.main.route", new=AsyncMock()):
        r = client.post(
            "/decide",
            json=_smoke_payload(click_id),
            headers={"X-TDS-Smoke-Probe": _make_probe(_PROBE_SECRET, click_id)},
        )

    assert r.status_code == 200, r.text
    assert r.json()["status"] == 302
    # The per-Worker check was NOT consulted — the probe is the auth.
    fresh_node_403.assert_not_awaited()
    fake_redis.xadd.assert_awaited_once()


def test_invalid_probe_does_not_fall_through_to_per_worker_check():
    """Enforce mode + a smoke click with a BAD probe → 403 from the probe
    check; it must NOT fall through to `_check_tds_key` (which could
    accidentally authenticate on a seeded node and mask the probe failure)."""
    from fastapi import HTTPException
    from app.main import app
    client = TestClient(app)
    click_id = "smoke-test-99-deadbeef00112233"
    would_pass = AsyncMock(return_value=7)  # if reached, would auth — must NOT be reached

    with patch.object(settings, "smoke_probe_secret", _PROBE_SECRET), \
         patch("app.main._check_tds_key", new=would_pass), \
         patch("app.main.get_redis", new=AsyncMock(return_value=MagicMock(xadd=AsyncMock()))), \
         patch("app.main.route", new=AsyncMock()):
        r = client.post(
            "/decide",
            json=_smoke_payload(click_id),
            headers={"X-TDS-Smoke-Probe": _make_probe("wrong-secret-xxxxxxxxxxxxxxxxx", click_id)},
        )

    assert r.status_code == 403
    would_pass.assert_not_awaited()  # probe failure is terminal, no fall-through


def test_smoke_without_probe_secret_still_uses_per_worker_check():
    """Backward-compat: when the probe secret is UNSET, a smoke click is NOT
    probe-authed — it goes through `_check_tds_key` like any click (legacy /
    pre-rollout). On a fresh node that 403s (the documented skip_smoke case);
    here we assert the check IS consulted."""
    from app.main import app
    client = TestClient(app)
    check = AsyncMock(return_value=1)  # seeded node — passes

    # smoke_probe_secret defaults empty in the test env; assert behaviour.
    with patch.object(settings, "smoke_probe_secret", ""), \
         patch("app.main._check_tds_key", new=check), \
         patch("app.main.get_redis", new=AsyncMock(return_value=MagicMock(xadd=AsyncMock(return_value="1-0")))), \
         patch("app.main.route", new=AsyncMock()):
        r = client.post(
            "/decide",
            json=_smoke_payload("smoke-test-1-aaaaaaaaaaaaaaaa"),
            headers={"X-TDS-Key": "global-secret"},
        )

    assert r.status_code == 200
    check.assert_awaited_once()  # legacy path still consults the per-Worker auth


def test_verify_smoke_probe_helper_paths():
    """Direct coverage of the verifier's branches with the secret set."""
    from app.main import _verify_smoke_probe

    with patch.object(settings, "smoke_probe_secret", _PROBE_SECRET):
        cid = "smoke-test-1-abcabcabcabcabca"
        ok, _ = _verify_smoke_probe(cid, _make_probe(_PROBE_SECRET, cid))
        assert ok is True

        assert _verify_smoke_probe(cid, "")[0] is False             # missing
        assert _verify_smoke_probe(cid, "nodot")[0] is False         # malformed
        assert _verify_smoke_probe(cid, "notanint.deadbeef")[0] is False  # bad ts
        # Non-hex/short sig of the right shape → mismatch (not a crash).
        assert _verify_smoke_probe(cid, f"{int(time.time())}.zz")[0] is False
