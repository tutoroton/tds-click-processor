"""Tests for the F.29 Sprint 2.2 shipper response-parsing helpers.

Sprint 2.1 introduced the collector response shape ``{accepted,
rejected, duplicates}``. Sprint 2.2 adds shipper-side helpers that:

  * ``_parse_collector_response`` — robust JSON parsing with shim
    detection. Distinguishes new (Sprint 2.1+) vs legacy (pre-F.29)
    vs unknown (corrupt / non-JSON) bodies so the caller can branch.
  * ``_handle_rejected_click`` — per-click retry counter +
    deadletter on max-attempts.
  * ``_retry_click`` — re-XADD to ``stream:clicks``.
  * ``_deadletter_click`` — XADD to local
    ``stream:clicks-deadletter`` with attempt history.

These tests pin the helper contracts. Full main-loop integration
test lives in Sprint 2.6 (``test_partial_ack.py``).

Reference: F.29 plan §3 G2-HIGH, §4 Sprint 2.2 + 2.5 rows.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import pytest

from app import shipper
from app.shipper import (
    _parse_collector_response,
    _deadletter_click,
    _handle_rejected_click,
    _retry_click,
    DEADLETTER_STREAM_KEY,
    DEADLETTER_STREAM_MAXLEN,
    STREAM_KEY,
)


@pytest.fixture(autouse=True)
def _reset_settings(monkeypatch):
    """Per-test isolation. Each test sets the values it cares about."""
    monkeypatch.setattr(shipper.settings, "node_id", "test-node-AU")
    monkeypatch.setattr(shipper.settings, "shipper_max_retry_attempts", 5)
    monkeypatch.setattr(shipper.settings, "shipper_retry_ttl_seconds", 86400)
    monkeypatch.setattr(shipper.settings, "stream_clicks_maxlen", 1_000_000)


# ---------------------------------------------------------------------------
# _parse_collector_response — shape detection
# ---------------------------------------------------------------------------


def test_parse_new_shape_returns_new_with_body():
    """Sprint 2.1 shape has the ``accepted`` key. Even if other
    fields are absent, presence of ``accepted`` is the canonical
    indicator."""
    body_json = json.dumps({
        "received": 3,
        "queued": 2,
        "accepted": ["a", "b"],
        "rejected": [{"click_id": "c", "reason": "queue_failure"}],
        "duplicates": [],
    })
    shape, parsed = _parse_collector_response(body_json)
    assert shape == "new"
    assert parsed["accepted"] == ["a", "b"]
    assert parsed["rejected"][0]["click_id"] == "c"


def test_parse_legacy_shape_returns_legacy():
    """Pre-F.29 shape: {received, queued, stream_id} with NO
    ``accepted`` key. Sprint 2.5 shim relies on this distinction."""
    body_json = json.dumps({
        "received": 5,
        "queued": 5,
        "stream_id": "1234-0",
    })
    shape, parsed = _parse_collector_response(body_json)
    assert shape == "legacy"
    assert parsed["received"] == 5
    assert "accepted" not in parsed


def test_parse_empty_body_is_unknown():
    """Empty response body — neither legacy nor new shape. Caller
    falls back to status-code-only decision."""
    shape, parsed = _parse_collector_response("")
    assert shape == "unknown"
    assert parsed is None


def test_parse_invalid_json_is_unknown():
    """Corrupt JSON — defensive fallback. No raised exception, just
    unknown verdict."""
    shape, parsed = _parse_collector_response("not json at all {[}")
    assert shape == "unknown"
    assert parsed is None


def test_parse_non_dict_body_is_unknown():
    """JSON that parses but isn't a dict (e.g. an array or scalar).
    Defensive: we expect dict body."""
    shape, parsed = _parse_collector_response("[1, 2, 3]")
    assert shape == "unknown"
    assert parsed is None


def test_parse_dict_without_recognized_keys_is_unknown():
    """A dict with no ``accepted``/``received``/``queued`` keys.
    Could be a future schema or an unrelated service responding —
    treat as unknown."""
    body_json = json.dumps({"foo": "bar"})
    shape, parsed = _parse_collector_response(body_json)
    assert shape == "unknown"
    # Body is still returned (not None) so caller has it for logging.
    assert parsed == {"foo": "bar"}


def test_parse_only_queued_key_is_legacy():
    """A body with only ``queued`` (no ``received``) also counts as
    legacy — defensive against minor pre-F.29 variants."""
    body_json = json.dumps({"queued": 10, "stream_id": "x"})
    shape, _ = _parse_collector_response(body_json)
    assert shape == "legacy"


# ---------------------------------------------------------------------------
# _retry_click — re-XADD to stream:clicks
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_retry_click_xadds_to_stream_with_maxlen_cap():
    """Re-XADDs the click payload to ``stream:clicks`` (NOT the
    deadletter stream). Uses the same MAXLEN cap as the original
    ingest path — bounds stream growth under retry storms."""
    redis_mock = AsyncMock()
    click = {"click_id": "abc", "ip": "1.2.3.4"}

    await _retry_click(redis_mock, click)

    redis_mock.xadd.assert_awaited_once()
    args, kwargs = redis_mock.xadd.call_args
    assert args[0] == STREAM_KEY
    # Payload is JSON-serialised under the "data" key (mirrors
    # main.py /decide XADD shape).
    assert "data" in args[1]
    assert json.loads(args[1]["data"])["click_id"] == "abc"
    # MAXLEN cap enforced — defensive against retry storms.
    assert kwargs["maxlen"] == 1_000_000
    assert kwargs["approximate"] is True


# ---------------------------------------------------------------------------
# _deadletter_click — XADD to local deadletter stream
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_deadletter_click_xadds_full_record():
    """The deadletter record carries enough metadata for operator
    inspection: original click JSON + attempt count + last rejection
    reason + timestamp + node_id."""
    redis_mock = AsyncMock()
    click = {"click_id": "dead-click", "campaign_id": 42}

    await _deadletter_click(
        redis_mock, click, attempt=5, reason="queue_failure",
    )

    redis_mock.xadd.assert_awaited_once()
    args, kwargs = redis_mock.xadd.call_args
    assert args[0] == DEADLETTER_STREAM_KEY
    record = args[1]
    assert json.loads(record["data"])["click_id"] == "dead-click"
    assert json.loads(record["data"])["campaign_id"] == 42
    assert record["attempt_count"] == "5"
    assert record["last_rejection_reason"] == "queue_failure"
    assert record["node_id"] == "test-node-AU"
    assert "deadlettered_at" in record
    # MAXLEN cap on deadletter stream — ring buffer for operator
    # inspection, not unbounded storage.
    assert kwargs["maxlen"] == DEADLETTER_STREAM_MAXLEN
    assert kwargs["approximate"] is True


@pytest.mark.asyncio
async def test_deadletter_truncates_long_reason():
    """The reason string is capped at 64 chars to bound the
    deadletter record size + match RejectedClick.reason cap in the
    collector schema."""
    redis_mock = AsyncMock()
    click = {"click_id": "x"}

    long_reason = "a" * 200
    await _deadletter_click(redis_mock, click, attempt=5, reason=long_reason)

    record = redis_mock.xadd.call_args.args[1]
    assert len(record["last_rejection_reason"]) == 64


@pytest.mark.asyncio
async def test_deadletter_swallows_xadd_failure():
    """If the deadletter XADD itself fails (Redis impaired), we log
    + capture but do NOT propagate — the caller's flow continues."""
    redis_mock = AsyncMock()
    redis_mock.xadd = AsyncMock(side_effect=ConnectionError("redis down"))
    click = {"click_id": "x"}

    with patch("app.shipper._capture_op_exc") as mock_cap:
        # Must not raise.
        await _deadletter_click(redis_mock, click, attempt=5, reason="r")
        mock_cap.assert_called_once()


# ---------------------------------------------------------------------------
# _handle_rejected_click — retry counter + max-attempts deadletter
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_handle_rejected_retries_when_under_max(monkeypatch):
    """First few rejections increment counter + re-XADD. Returns
    True (caller should ACK the old msg_id; new attempt is in
    queue)."""
    monkeypatch.setattr(shipper.settings, "shipper_max_retry_attempts", 5)

    redis_mock = AsyncMock()
    redis_mock.incr = AsyncMock(return_value=3)  # 3 < 5
    redis_mock.expire = AsyncMock()
    redis_mock.xadd = AsyncMock()
    click = {"click_id": "x"}

    retried = await _handle_rejected_click(redis_mock, click, "queue_failure")

    assert retried is True
    redis_mock.incr.assert_awaited_once_with("click:retry:x")
    redis_mock.expire.assert_awaited_once_with("click:retry:x", 86400)
    redis_mock.xadd.assert_awaited_once()
    # The XADD goes to stream:clicks (retry), NOT the deadletter stream.
    assert redis_mock.xadd.call_args.args[0] == STREAM_KEY


@pytest.mark.asyncio
async def test_handle_rejected_deadletters_at_max(monkeypatch):
    """When the counter reaches max, deadletter instead of retry.
    Returns False (caller should ACK the old msg_id; click is in
    deadletter)."""
    monkeypatch.setattr(shipper.settings, "shipper_max_retry_attempts", 5)

    redis_mock = AsyncMock()
    redis_mock.incr = AsyncMock(return_value=5)  # == max
    redis_mock.expire = AsyncMock()
    redis_mock.xadd = AsyncMock()
    redis_mock.delete = AsyncMock()
    click = {"click_id": "x"}

    retried = await _handle_rejected_click(redis_mock, click, "queue_failure")

    assert retried is False
    redis_mock.xadd.assert_awaited_once()
    # XADD must target the deadletter stream, not the retry stream.
    assert redis_mock.xadd.call_args.args[0] == DEADLETTER_STREAM_KEY
    # The retry counter is cleaned up — click is out of the loop.
    redis_mock.delete.assert_awaited_once_with("click:retry:x")


@pytest.mark.asyncio
async def test_handle_rejected_missing_click_id_deadletters_immediately():
    """A click with no click_id (pathological) — cannot maintain a
    counter, so deadletter immediately. Defensive guard."""
    redis_mock = AsyncMock()
    redis_mock.xadd = AsyncMock()
    click = {}  # no click_id

    retried = await _handle_rejected_click(redis_mock, click, "missing_click_id")

    assert retried is False
    redis_mock.xadd.assert_awaited_once()
    assert redis_mock.xadd.call_args.args[0] == DEADLETTER_STREAM_KEY


@pytest.mark.asyncio
async def test_handle_rejected_counter_failure_deadletters(monkeypatch):
    """Redis INCR failure during counter increment — conservatively
    deadletter rather than infinite-retry. Logged + captured but
    flow continues."""
    redis_mock = AsyncMock()
    redis_mock.incr = AsyncMock(side_effect=ConnectionError("redis down"))
    redis_mock.xadd = AsyncMock()
    click = {"click_id": "x"}

    retried = await _handle_rejected_click(redis_mock, click, "queue_failure")

    assert retried is False
    # Deadletter XADD called even though counter failed.
    redis_mock.xadd.assert_awaited_once()
    assert redis_mock.xadd.call_args.args[0] == DEADLETTER_STREAM_KEY


@pytest.mark.asyncio
async def test_handle_rejected_requeue_failure_deadletters(monkeypatch):
    """If the re-XADD itself fails (Redis impaired after counter
    succeeded), conservatively deadletter so the click isn't lost
    silently."""
    monkeypatch.setattr(shipper.settings, "shipper_max_retry_attempts", 5)

    redis_mock = AsyncMock()
    redis_mock.incr = AsyncMock(return_value=2)
    redis_mock.expire = AsyncMock()
    # First xadd call is _retry_click (raises); second would be
    # deadletter. We use side_effect with two values.
    redis_mock.xadd = AsyncMock(side_effect=[
        ConnectionError("redis down on retry"),
        None,  # deadletter xadd succeeds
    ])
    click = {"click_id": "x"}

    retried = await _handle_rejected_click(redis_mock, click, "queue_failure")

    assert retried is False
    # Two xadd calls — first retry attempt failed, second is deadletter.
    assert redis_mock.xadd.await_count == 2
    # Second call (deadletter) targets the right stream.
    assert redis_mock.xadd.call_args_list[1].args[0] == DEADLETTER_STREAM_KEY
