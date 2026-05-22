"""Tests for F.29 Sprint 2.3 shipper central-deadletter forwarding.

Sprint 2.2 made the shipper write deadletters to a local edge ring
buffer (``stream:clicks-deadletter``). Sprint 2.3 additionally POSTs
the record to the central collector's ``/api/clicks/deadletter``
endpoint so the operator dashboard sees deadletters across the whole
fleet.

Forward is best-effort: failures are logged + Sentry-captured but
never propagate. The click is already preserved at the edge — the
central forward is observability redundancy, not durability.

Coverage:
  * Local XADD + central forward both invoked when http_client provided
  * Central forward NOT attempted when http_client=None
  * Central forward swallows HTTP exceptions
  * Central forward returns False on non-202 status
  * Central forward respects central_url + central_api_key settings
  * Empty central_url skips forwarding (standalone mode)

Reference: F.29 plan §4 Sprint 2.3 row.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from app import shipper
from app.shipper import (
    DEADLETTER_STREAM_KEY,
    _deadletter_click,
    _forward_deadletter_to_central,
)


@pytest.fixture(autouse=True)
def _shipper_settings(monkeypatch):
    monkeypatch.setattr(shipper.settings, "node_id", "test-edge-AU")
    monkeypatch.setattr(shipper.settings, "central_url", "http://central:8200")
    monkeypatch.setattr(shipper.settings, "central_api_key", "test-key")


# ---------------------------------------------------------------------------
# _forward_deadletter_to_central — happy path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_forward_returns_true_on_202():
    """Successful forward → True. Caller does not need to take any
    further action; the central dashboard now sees the deadletter."""
    http_mock = AsyncMock(spec=httpx.AsyncClient)
    response_mock = MagicMock(spec=httpx.Response)
    response_mock.status_code = 202
    response_mock.text = '{"status":"accepted"}'
    http_mock.post = AsyncMock(return_value=response_mock)

    record = {
        "click_id": "dead-1",
        "data": '{"click_id":"dead-1"}',
        "attempt_count": "5",
        "last_rejection_reason": "queue_failure",
        "deadlettered_at": "1234.5",
        "node_id": "test-edge-AU",
    }
    ok = await _forward_deadletter_to_central(http_mock, record)
    assert ok is True

    # Verify the POST shape matches the collector's DeadletterRecord
    # schema — must round-trip through Pydantic at the collector.
    http_mock.post.assert_awaited_once()
    args, kwargs = http_mock.post.call_args
    assert args[0] == "http://central:8200/api/clicks/deadletter"
    assert kwargs["headers"]["X-Node-Key"] == "test-key"
    payload = kwargs["json"]
    assert payload["click_id"] == "dead-1"
    assert payload["attempt_count"] == 5  # coerced to int
    assert payload["deadlettered_at"] == 1234.5  # coerced to float


@pytest.mark.asyncio
async def test_forward_returns_false_on_non_202():
    """Non-202 response → False. The collector might have been busy
    or auth-failed; logged but flow continues."""
    http_mock = AsyncMock(spec=httpx.AsyncClient)
    response_mock = MagicMock()
    response_mock.status_code = 503
    response_mock.text = "queue unavailable"
    http_mock.post = AsyncMock(return_value=response_mock)

    record = {
        "click_id": "dead-1",
        "data": "{}",
        "attempt_count": "1",
        "last_rejection_reason": "r",
        "deadlettered_at": "1.0",
        "node_id": "x",
    }
    ok = await _forward_deadletter_to_central(http_mock, record)
    assert ok is False


@pytest.mark.asyncio
async def test_forward_returns_false_on_exception():
    """httpx exceptions (timeout, connection error) are swallowed —
    return False, log + Sentry capture, but never propagate. The
    click is already preserved at the edge."""
    http_mock = AsyncMock(spec=httpx.AsyncClient)
    http_mock.post = AsyncMock(side_effect=httpx.RequestError("timeout"))

    record = {
        "click_id": "dead-1",
        "data": "{}",
        "attempt_count": "1",
        "last_rejection_reason": "r",
        "deadlettered_at": "1.0",
        "node_id": "x",
    }
    with patch("app.shipper._capture_op_exc") as mock_cap:
        ok = await _forward_deadletter_to_central(http_mock, record)
        assert ok is False
        mock_cap.assert_called_once()


@pytest.mark.asyncio
async def test_forward_skips_when_central_url_empty(monkeypatch):
    """Standalone-mode shipper (no central URL configured) skips
    the forward entirely. Local deadletter still works."""
    monkeypatch.setattr(shipper.settings, "central_url", "")

    http_mock = AsyncMock(spec=httpx.AsyncClient)
    record = {"click_id": "x", "data": "{}", "attempt_count": "1",
              "last_rejection_reason": "r", "deadlettered_at": "1.0",
              "node_id": "x"}
    ok = await _forward_deadletter_to_central(http_mock, record)
    assert ok is False
    http_mock.post.assert_not_called()


# ---------------------------------------------------------------------------
# _deadletter_click — local XADD + central forward integration
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_deadletter_click_does_local_xadd_and_central_forward():
    """When called with both redis_pool + http_client, the function
    XADDs locally AND forwards to central. Both paths are exercised
    every deadletter event for redundancy."""
    redis_mock = AsyncMock()
    redis_mock.xadd = AsyncMock(return_value=b"1234-0")
    http_mock = AsyncMock(spec=httpx.AsyncClient)
    response_mock = MagicMock()
    response_mock.status_code = 202
    response_mock.text = '{"status":"accepted"}'
    http_mock.post = AsyncMock(return_value=response_mock)

    click = {"click_id": "dead-x", "ip": "1.1.1.1"}
    await _deadletter_click(
        redis_mock, click, attempt=5, reason="queue_failure",
        http_client=http_mock,
    )

    # Local XADD was called.
    redis_mock.xadd.assert_awaited_once()
    assert redis_mock.xadd.call_args.args[0] == DEADLETTER_STREAM_KEY
    # Central forward was called.
    http_mock.post.assert_awaited_once()


@pytest.mark.asyncio
async def test_deadletter_click_skips_forward_when_http_client_none():
    """When http_client=None (e.g. early unit tests or a config where
    central forwarding is disabled), local XADD still happens but
    central forward is skipped — no httpx is used at all."""
    redis_mock = AsyncMock()
    redis_mock.xadd = AsyncMock()

    click = {"click_id": "dead-y"}
    # http_client defaults to None.
    await _deadletter_click(
        redis_mock, click, attempt=5, reason="r",
    )

    redis_mock.xadd.assert_awaited_once()
    # No http_client to assert against; we just verify local path worked.


@pytest.mark.asyncio
async def test_deadletter_click_central_forward_failure_still_completes():
    """Central forward failure doesn't propagate — caller's flow
    continues. The local XADD is the durability primitive."""
    redis_mock = AsyncMock()
    redis_mock.xadd = AsyncMock()
    http_mock = AsyncMock(spec=httpx.AsyncClient)
    http_mock.post = AsyncMock(side_effect=httpx.RequestError("down"))

    click = {"click_id": "dead-z"}
    # Must not raise.
    await _deadletter_click(
        redis_mock, click, attempt=5, reason="r", http_client=http_mock,
    )

    # Local XADD still succeeded.
    redis_mock.xadd.assert_awaited_once()
    # Forward was attempted but failed — caller doesn't see the exception.
    http_mock.post.assert_awaited_once()
