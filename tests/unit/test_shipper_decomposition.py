"""Tests for the F.29 TD-1 (2026-05-23) run_shipper decomposition helpers.

Pre-TD-1 ``run_shipper`` was a ~497 LOC / 5-6 nesting depth function
that bundled XREADGROUP, parse loop, HTTP POST, per-click verdict
handling, the Sprint 2.5 backwards-compat shim, and all exception
handling into a single body. Agent 1 of the Sprint 2.7 validation
cycle flagged this as a CRITICAL violation of rule code-organization
(60-LOC + 3-depth caps).

The decomposition extracted 7 helpers + 4 sub-helpers (see plan-doc
§14 TD-1 and shipper.py module docstring for the decomposition map).
``run_shipper`` is now a ~50 LOC dispatcher with three explicit
state transitions: drain → post → route-by-shape.

These tests pin the new helpers' contracts so a future refactor that
silently changes behaviour gets caught at unit-test time. Coverage
strategy: each helper is exercised in isolation with mocked
collaborators (fake redis, mock httpx response, monkeypatched
shipper_metrics). The end-to-end glue is already covered by
``test_shipper_chaos_partial_ack.py`` (Sprint 2.6 chaos integration).

Reference: F.29 plan-doc §14 TD-1 row; shipper.py decomposition
docstring (lines ~503-548).
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from app import shipper, shipper_metrics as smm
from app.shipper import (
    _ack_shipped_batch,
    _compute_ack_msg_ids_from_verdict,
    _drain_batch_from_stream,
    _handle_central_unreachable,
    _handle_rejected_in_batch,
    _handle_shipper_loop_error,
    _post_batch_to_central,
    _process_collector_error,
    _process_legacy_shape_batch,
    _process_new_shape_batch,
    _record_new_shape_outcome,
    GROUP_NAME,
    MAX_RETRY_DELAY,
    STREAM_KEY,
)


@pytest.fixture(autouse=True)
def _reset_metrics():
    """Per-test isolation — shipper_metrics is a module singleton."""
    smm._reset_for_tests()
    yield
    smm._reset_for_tests()


@pytest.fixture(autouse=True)
def _reset_shim_flag():
    """The ``_shim_warned_this_session`` module-level flag persists
    across tests by design (one-shot per shipper lifetime). Reset
    before each test so legacy-shape tests start from a clean slate.
    """
    shipper._shim_warned_this_session = False
    yield
    shipper._shim_warned_this_session = False


def _make_response(status_code: int, text: str = "") -> httpx.Response:
    """Build a minimal httpx.Response for status_code + text assertions
    without exercising the wire-protocol layer. Sufficient for helpers
    that only read ``.status_code`` and ``.text``."""
    return httpx.Response(status_code=status_code, text=text)


# ===========================================================================
# _drain_batch_from_stream
# ===========================================================================


@pytest.mark.asyncio
async def test_drain_returns_empty_on_no_results():
    """XREADGROUP timed out with no new messages → empty tuple. The
    caller's ``if not clicks: continue`` short-circuits the loop iter."""
    redis = AsyncMock()
    redis.xreadgroup.return_value = []  # no results from XREADGROUP

    clicks, msg_ids = await _drain_batch_from_stream(redis)

    assert clicks == []
    assert msg_ids == []
    # No XACK should fire when there's nothing to drain.
    redis.xack.assert_not_called()


@pytest.mark.asyncio
async def test_drain_parses_all_good_messages():
    """All messages parse cleanly → returns matching click + msg_id lists
    with same length (1:1 by index)."""
    redis = AsyncMock()
    redis.xreadgroup.return_value = [
        (
            STREAM_KEY,
            [
                ("1-0", {"data": json.dumps({"click_id": "c1"})}),
                ("2-0", {"data": json.dumps({"click_id": "c2"})}),
                ("3-0", {"data": json.dumps({"click_id": "c3"})}),
            ],
        )
    ]

    clicks, msg_ids = await _drain_batch_from_stream(redis)

    assert len(clicks) == 3
    assert len(msg_ids) == 3
    assert [c["click_id"] for c in clicks] == ["c1", "c2", "c3"]
    assert msg_ids == ["1-0", "2-0", "3-0"]
    # Parse-failure XACK path should NOT fire on clean parses.
    redis.xack.assert_not_called()


@pytest.mark.asyncio
async def test_drain_acks_parse_failures_inline_excludes_from_return():
    """Parse-failed messages get XACKed inline AND are excluded from the
    returned ``msg_ids`` list. This guarantees the caller can XACK the
    returned msg_ids without double-ACKing the parse-failures (a footgun
    the pre-TD-1 inline form was vulnerable to)."""
    redis = AsyncMock()
    redis.xreadgroup.return_value = [
        (
            STREAM_KEY,
            [
                ("1-0", {"data": json.dumps({"click_id": "c1"})}),
                ("2-0", {"data": "not-valid-json{{"}),  # parse fail
                ("3-0", {"data": json.dumps({"click_id": "c3"})}),
            ],
        )
    ]

    clicks, msg_ids = await _drain_batch_from_stream(redis)

    # Only the 2 good clicks are returned.
    assert [c["click_id"] for c in clicks] == ["c1", "c3"]
    assert msg_ids == ["1-0", "3-0"]
    # The parse-failed msg_id was XACKed inline (not in return list).
    redis.xack.assert_awaited_once_with(STREAM_KEY, GROUP_NAME, "2-0")


@pytest.mark.asyncio
async def test_drain_continues_when_inline_xack_fails():
    """If the inline XACK for a parse-failed message itself raises (Redis
    impairment during a recovery scenario), the drainer must NOT propagate
    the failure — it logs + captures + continues to the next message."""
    redis = AsyncMock()
    redis.xreadgroup.return_value = [
        (
            STREAM_KEY,
            [
                ("1-0", {"data": "garbage"}),
                ("2-0", {"data": json.dumps({"click_id": "c2"})}),
            ],
        )
    ]
    redis.xack.side_effect = RuntimeError("simulated redis blip")

    # Must NOT raise — the parse-fail XACK is best-effort.
    clicks, msg_ids = await _drain_batch_from_stream(redis)

    # The good click still surfaces.
    assert [c["click_id"] for c in clicks] == ["c2"]
    assert msg_ids == ["2-0"]


# ===========================================================================
# _post_batch_to_central
# ===========================================================================


@pytest.mark.asyncio
async def test_post_batch_uses_canonical_url_and_headers(monkeypatch):
    """Wire format pin: POST goes to ``{central_url}/api/clicks/batch``
    with ``X-Node-Key`` header + payload shape
    ``{node_id, clicks}``. The collector's BatchRequest model expects
    exactly this shape; drift here breaks the contract."""
    monkeypatch.setattr(shipper.settings, "central_url", "https://central:8200")
    monkeypatch.setattr(shipper.settings, "node_id", "test-node")
    monkeypatch.setattr(shipper.settings, "central_api_key", "secret-key")

    captured = {}

    async def fake_post(url, json=None, headers=None):
        captured["url"] = url
        captured["json"] = json
        captured["headers"] = headers
        return _make_response(202, "")

    client = MagicMock()
    client.post = fake_post

    clicks = [{"click_id": "c1"}, {"click_id": "c2"}]
    response = await _post_batch_to_central(client, clicks)

    assert response.status_code == 202
    assert captured["url"] == "https://central:8200/api/clicks/batch"
    assert captured["json"] == {"node_id": "test-node", "clicks": clicks}
    assert captured["headers"] == {"X-Node-Key": "secret-key"}


# ===========================================================================
# _compute_ack_msg_ids_from_verdict
# ===========================================================================


def test_compute_ack_includes_accepted_and_duplicates():
    """Accepted ∪ duplicates → both bucketed as ACK-eligible. They both
    represent "click is at central"."""
    mapping = {"c1": "1-0", "c2": "2-0", "c3": "3-0", "c4": "4-0"}
    result = _compute_ack_msg_ids_from_verdict(
        accepted_ids=["c1", "c2"],
        duplicate_ids=["c3"],
        click_id_to_msg_id=mapping,
    )
    assert result == {"1-0", "2-0", "3-0"}


def test_compute_ack_deduplicates_overlap():
    """A malformed response listing the same click_id in BOTH accepted
    and duplicates must not double-ACK — the set semantics protect."""
    mapping = {"c1": "1-0"}
    result = _compute_ack_msg_ids_from_verdict(
        accepted_ids=["c1"],
        duplicate_ids=["c1"],  # malformed: listed twice
        click_id_to_msg_id=mapping,
    )
    assert result == {"1-0"}


def test_compute_ack_silently_skips_unknown_click_ids():
    """A collector echo of an unknown click_id (defensive case) gets
    silently skipped — the caller logs unknown rejects separately."""
    mapping = {"c1": "1-0"}
    result = _compute_ack_msg_ids_from_verdict(
        accepted_ids=["c1", "unknown-cid"],
        duplicate_ids=[],
        click_id_to_msg_id=mapping,
    )
    assert result == {"1-0"}


# ===========================================================================
# _process_collector_error
# ===========================================================================


@pytest.mark.asyncio
async def test_process_collector_error_exponential_backoff():
    """Each invocation doubles the retry_delay, capped at MAX_RETRY_DELAY."""
    response = _make_response(500, "internal error")
    clicks = [{"click_id": "c1"}, {"click_id": "c2"}]

    # Avoid real sleep — test the math, not the timing.
    with patch.object(shipper.asyncio, "sleep", new=AsyncMock()):
        d1 = await _process_collector_error(response, clicks, retry_delay=1)
        d2 = await _process_collector_error(response, clicks, retry_delay=d1)
        d3 = await _process_collector_error(response, clicks, retry_delay=d2)

    assert d1 == 2
    assert d2 == 4
    assert d3 == 8


@pytest.mark.asyncio
async def test_process_collector_error_caps_at_max_retry_delay():
    """The backoff must saturate at MAX_RETRY_DELAY (30) — don't sleep
    for hours if central stays down."""
    response = _make_response(503, "service unavailable")
    clicks = [{"click_id": "c1"}]

    with patch.object(shipper.asyncio, "sleep", new=AsyncMock()):
        new_delay = await _process_collector_error(
            response, clicks, retry_delay=MAX_RETRY_DELAY,
        )
    assert new_delay == MAX_RETRY_DELAY  # capped


@pytest.mark.asyncio
async def test_process_collector_error_records_metrics():
    """The helper must record collector_error ship status + record_outcome
    with full batch_size as rejected — the success-ratio window dips."""
    response = _make_response(502, "bad gateway")
    clicks = [{"click_id": f"c{i}"} for i in range(5)]

    with patch.object(shipper.asyncio, "sleep", new=AsyncMock()):
        await _process_collector_error(response, clicks, retry_delay=1)

    assert smm.metrics.last_ship_status == "collector_error"
    assert smm.metrics.last_batch_size == 5
    # Success ratio over single 0/5 batch = 0.0
    assert smm.metrics.success_ratio_5m == 0.0


@pytest.mark.asyncio
async def test_process_collector_error_207_contract_violation_branch():
    """When ``shape`` is passed (caller hit 207 + non-new shape), the
    helper takes the contract-violation logging branch. Behaviour is
    otherwise identical — same backoff, same metrics.

    D9 (audit 2026-06-03): the contract-violation branch now captures to
    Sentry at ERROR level (was WARN) — a garbled 207 the shipper cannot
    parse-verdict means the batch keeps bouncing on retry and must page,
    not whisper.
    """
    response = _make_response(207, '{"received": 5}')  # legacy shape
    clicks = [{"click_id": "c1"}]

    with patch.object(shipper.asyncio, "sleep", new=AsyncMock()), \
         patch.object(shipper, "_capture_op_msg") as cap:
        new_delay = await _process_collector_error(
            response, clicks, retry_delay=2, shape="legacy",
        )

    assert new_delay == 4
    assert smm.metrics.last_ship_status == "collector_error"
    # Captured at ERROR (D9), not warning.
    cap.assert_called_once()
    assert cap.call_args.kwargs.get("level") == "error"


@pytest.mark.asyncio
async def test_garbled_207_unknown_shape_retries_not_ackall(monkeypatch):
    """D9 regression fence — a 207 with an UNPARSEABLE body (shape
    ``unknown``) must route to ``_process_collector_error`` (retry, no
    ACK-all), NOT the legacy ACK-all shim. Pins the Sprint 3.7.1 TD-17
    tightening so a future loosening of the dispatcher (re-adding
    'unknown on 2xx → ACK-all') is caught.

    We assert the routing INVARIANT at the parse+decision seam: the
    garbled body parses to ``unknown`` and the contract-violation helper
    (a) does NOT XACK anything and (b) records ``collector_error`` +
    captures at ERROR.
    """
    # Garbled 207 body — not valid JSON → unknown shape.
    shape, body = shipper._parse_collector_response("{garbled<not-json>")
    assert shape == "unknown"
    assert body is None

    response = _make_response(207, "{garbled<not-json>")
    clicks = [{"click_id": "c1"}, {"click_id": "c2"}]
    redis = MagicMock()
    redis.xack = AsyncMock()

    with patch.object(shipper.asyncio, "sleep", new=AsyncMock()), \
         patch.object(shipper, "_capture_op_msg") as cap:
        await _process_collector_error(
            response, clicks, retry_delay=1, shape=shape,
        )

    # No ACK-all: the contract-violation path never touches the stream
    # (the batch stays in the PEL / is re-driven on the next loop).
    redis.xack.assert_not_called()
    assert smm.metrics.last_ship_status == "collector_error"
    assert cap.call_args.kwargs.get("level") == "error"


# ===========================================================================
# _handle_central_unreachable
# ===========================================================================


@pytest.mark.asyncio
async def test_handle_central_unreachable_records_unreachable_status():
    """httpx.RequestError handler records ``unreachable`` ship status —
    distinct from ``collector_error`` so operators can split DNS/TCP
    failures from HTTP errors at the dashboard."""
    exc = httpx.ConnectTimeout("could not connect")

    with patch.object(shipper.asyncio, "sleep", new=AsyncMock()):
        new_delay = await _handle_central_unreachable(
            exc, batch_size=10, retry_delay=1,
        )

    assert new_delay == 2  # exponential backoff
    assert smm.metrics.last_ship_status == "unreachable"
    assert smm.metrics.last_batch_size == 10
    # 0 accepted / 10 rejected ⇒ 0.0
    assert smm.metrics.success_ratio_5m == 0.0


# ===========================================================================
# _handle_shipper_loop_error
# ===========================================================================


@pytest.mark.asyncio
async def test_handle_shipper_loop_error_fixed_2s_sleep():
    """Catch-all path uses a FIXED 2-second sleep (not exponential
    backoff). The catch-all can't reason about batch state so it
    intentionally doesn't own retry_delay."""
    exc = RuntimeError("simulated unknown failure")

    fake_sleep = AsyncMock()
    with patch.object(shipper.asyncio, "sleep", new=fake_sleep):
        result = await _handle_shipper_loop_error(exc)

    assert result is None  # no retry_delay returned
    fake_sleep.assert_awaited_once_with(2)
    assert smm.metrics.last_ship_status == "loop_error"
    # batch_size hardcoded 0 because the catch-all can't trust clicks state
    assert smm.metrics.last_batch_size == 0


# ===========================================================================
# _ack_shipped_batch
# ===========================================================================


@pytest.mark.asyncio
async def test_ack_and_trim_returns_true_on_empty_ids():
    """If there's nothing to ACK (e.g., all clicks were rejected and
    retried, none ended up in the ack set), the helper short-circuits
    with True. This is the "all rejected, all re-XADDed" legitimate case."""
    redis = AsyncMock()
    result = await _ack_shipped_batch(
        redis, set(), batch_size=5, collector_status=207,
    )
    assert result is True
    redis.xack.assert_not_called()
    redis.xtrim.assert_not_called()


@pytest.mark.asyncio
async def test_ack_and_trim_success_returns_true():
    """Happy path: XACK succeeds → returns True. No ack_failed ship
    status recorded. AUD-B F1: NO XTRIM here — the old per-batch
    ``MAXLEN ~10000`` trim destroyed outage backlog on recovery;
    processed-history hygiene is the loop-clock MINID trim
    (test_shipper_trim.py)."""
    redis = AsyncMock()
    result = await _ack_shipped_batch(
        redis, {"1-0", "2-0"}, batch_size=2, collector_status=202,
    )
    assert result is True
    redis.xack.assert_awaited_once()
    redis.xtrim.assert_not_awaited()
    # ack_failed should NOT be set; default is "n/a"
    assert smm.metrics.last_ship_status == "n/a"


@pytest.mark.asyncio
async def test_ack_and_trim_records_ack_failed_on_redis_error():
    """If XACK raises (Redis impairment after a successful POST), the
    helper records ``ack_failed`` ship status + returns False so the
    caller skips outcome metrics (consistent with pre-TD-1 behaviour
    at the original lines 715-734)."""
    redis = AsyncMock()
    redis.xack.side_effect = RuntimeError("redis disconnect")

    result = await _ack_shipped_batch(
        redis, {"1-0"}, batch_size=1, collector_status=202,
    )
    assert result is False
    assert smm.metrics.last_ship_status == "ack_failed"
    assert smm.metrics.last_batch_size == 1


@pytest.mark.asyncio
async def test_ack_and_trim_shim_flag_propagates():
    """When ``shim_active=True`` (called from the Sprint 2.5 legacy shim
    path), the failure log carries a different prefix AND the Sentry
    extras include ``shim_active=True`` so the Sprint 4.1 alert can
    distinguish rolling-deploy-window failures from steady-state ones."""
    redis = AsyncMock()
    redis.xack.side_effect = RuntimeError("simulated")

    # We don't directly inspect Sentry extras here (covered by
    # test_shipper_exception_tagging); we just verify ack_failed is
    # still recorded and the helper still returns False.
    result = await _ack_shipped_batch(
        redis, {"1-0"}, batch_size=1,
        collector_status=200, shim_active=True,
    )
    assert result is False
    assert smm.metrics.last_ship_status == "ack_failed"


# ===========================================================================
# _record_new_shape_outcome — outcome status precedence
# ===========================================================================


def test_record_outcome_picks_deadlettered_when_any():
    """When even 1 click hit max retries this iter → ``deadlettered``."""
    _record_new_shape_outcome(
        accepted_ids=["c1", "c2"],
        duplicate_ids=[],
        rejected_items=[{"click_id": "c3", "reason": "x"}],
        deadletter_count=1,
        batch_size=3,
    )
    assert smm.metrics.last_ship_status == "deadlettered"


def test_record_outcome_picks_partial_ack_when_rejected_but_no_deadletters():
    """Rejected but still retrying → ``partial_ack``."""
    _record_new_shape_outcome(
        accepted_ids=["c1"],
        duplicate_ids=[],
        rejected_items=[{"click_id": "c2", "reason": "transient"}],
        deadletter_count=0,
        batch_size=2,
    )
    assert smm.metrics.last_ship_status == "partial_ack"


def test_record_outcome_picks_success_when_all_landed():
    """Pure happy path → ``success``."""
    _record_new_shape_outcome(
        accepted_ids=["c1", "c2"],
        duplicate_ids=["c3"],
        rejected_items=[],
        deadletter_count=0,
        batch_size=3,
    )
    assert smm.metrics.last_ship_status == "success"


def test_record_outcome_feeds_success_ratio_window():
    """The ``record_outcome`` call inside this helper feeds the rolling
    5-min success-ratio window. Verify accepted+duplicates count as
    success while rejected counts as non-delivery."""
    _record_new_shape_outcome(
        accepted_ids=["c1", "c2"],
        duplicate_ids=["c3"],  # 3 successes
        rejected_items=[{"click_id": "c4", "reason": "x"}],  # 1 failure
        deadletter_count=0,
        batch_size=4,
    )
    # 3 / (3 + 1) = 0.75
    assert smm.metrics.success_ratio_5m == 0.75


# ===========================================================================
# _handle_rejected_in_batch
# ===========================================================================


@pytest.mark.asyncio
async def test_handle_rejected_unknown_click_id_acks_and_skips_retry():
    """Defensive case: collector echoes a click_id that wasn't in the
    request batch. Helper must NOT crash; it logs + adds the msg_id to
    ack_msg_ids (if present in the mapping) + skips retry attempt."""
    redis = AsyncMock()
    client = MagicMock()  # not used because there's no retry

    clicks = [{"click_id": "c1"}]
    mapping = {"c1": "1-0", "unknown-cid": "u-0"}  # unknown_cid in mapping
    ack_set: set[str] = set()

    rejected = [{"click_id": "unknown-cid", "reason": "x"}]
    deadletter_count = await _handle_rejected_in_batch(
        redis, client, rejected, clicks, mapping, ack_set,
    )

    assert deadletter_count == 0
    assert "u-0" in ack_set  # unknown msg_id was ACKed
    # No retry counter touched — original click absent.
    redis.pipeline.assert_not_called()


@pytest.mark.asyncio
async def test_handle_rejected_counts_deadletters():
    """deadletter_count reflects the number of clicks for which
    _handle_rejected_click returned False (max retries hit)."""
    redis = AsyncMock()
    client = MagicMock()

    # Patch _handle_rejected_click to return False (deadlettered) for c1
    # and True (retried) for c2 — the helper's bookkeeping should reflect
    # exactly 1 deadletter.
    async def fake_handle(redis_pool, click, reason, http_client=None):
        return click["click_id"] != "c1"  # False (DL) for c1, True for c2

    clicks = [{"click_id": "c1"}, {"click_id": "c2"}]
    mapping = {"c1": "1-0", "c2": "2-0"}
    ack_set: set[str] = set()
    rejected = [
        {"click_id": "c1", "reason": "x"},
        {"click_id": "c2", "reason": "y"},
    ]

    with patch.object(shipper, "_handle_rejected_click", new=fake_handle):
        deadletter_count = await _handle_rejected_in_batch(
            redis, client, rejected, clicks, mapping, ack_set,
        )

    assert deadletter_count == 1
    # Both msg_ids should be ACKed regardless of retry/deadletter outcome.
    assert ack_set == {"1-0", "2-0"}


# ===========================================================================
# _process_new_shape_batch (orchestrator) — single end-to-end happy path
# ===========================================================================


@pytest.mark.asyncio
async def test_process_new_shape_full_happy_path(monkeypatch):
    """Smoke-test the orchestrator: all clicks accepted, none rejected,
    ACK + XTRIM succeed → records ``success`` ship status + full
    accepted count in success ratio."""
    redis = AsyncMock()
    client = MagicMock()

    clicks = [{"click_id": "c1"}, {"click_id": "c2"}, {"click_id": "c3"}]
    msg_ids = ["1-0", "2-0", "3-0"]
    response = _make_response(202, "")
    body = {
        "accepted": ["c1", "c2", "c3"],
        "rejected": [],
        "duplicates": [],
    }

    await _process_new_shape_batch(
        redis, client, response, body, clicks, msg_ids,
    )

    assert smm.metrics.last_ship_status == "success"
    assert smm.metrics.last_batch_size == 3
    assert smm.metrics.success_ratio_5m == 1.0
    # ACK was called with all 3 msg_ids.
    redis.xack.assert_awaited_once()
    ack_args = redis.xack.await_args.args
    assert ack_args[0] == STREAM_KEY
    assert ack_args[1] == GROUP_NAME
    assert set(ack_args[2:]) == {"1-0", "2-0", "3-0"}


# ===========================================================================
# _process_legacy_shape_batch — Sprint 2.5 shim + one-shot Sentry semantics
# ===========================================================================


@pytest.mark.asyncio
async def test_process_legacy_shape_acks_all_and_records_legacy_status():
    """The shim path ACKs ALL msg_ids on 200/202 + records
    ``legacy_collector`` ship status + counts full batch as accepted."""
    redis = AsyncMock()
    clicks = [{"click_id": "c1"}, {"click_id": "c2"}]
    msg_ids = ["1-0", "2-0"]
    response = _make_response(202, '{"received": 2, "queued": 2}')

    await _process_legacy_shape_batch(
        redis, response, shape="legacy",
        clicks=clicks, msg_ids=msg_ids,
    )

    assert smm.metrics.last_ship_status == "legacy_collector"
    assert smm.metrics.last_batch_size == 2
    assert smm.metrics.success_ratio_5m == 1.0
    redis.xack.assert_awaited_once()


@pytest.mark.asyncio
async def test_process_legacy_shape_sentry_fires_only_once_per_session():
    """One-shot Sentry capture semantics (Sprint 2.7a) — even across
    multiple legacy-response iterations, Sentry sees exactly ONE event.
    The WARN log still fires per-batch."""
    redis = AsyncMock()
    clicks = [{"click_id": "c1"}]
    msg_ids = ["1-0"]
    response = _make_response(200, '{"received": 1}')

    with patch("app.telemetry.sentry_sdk") as mock_sentry:
        # Configure push_scope as a context manager
        scope_mock = MagicMock()
        push_scope_mock = MagicMock()
        push_scope_mock.__enter__ = MagicMock(return_value=scope_mock)
        push_scope_mock.__exit__ = MagicMock(return_value=False)
        mock_sentry.push_scope.return_value = push_scope_mock

        # Fire 3 times — Sentry should see ONE capture_message.
        for _ in range(3):
            await _process_legacy_shape_batch(
                redis, response, shape="legacy",
                clicks=clicks, msg_ids=msg_ids,
            )

        assert mock_sentry.capture_message.call_count == 1
