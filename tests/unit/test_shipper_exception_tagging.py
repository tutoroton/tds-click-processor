"""Tests for the click-processor shipper exception tagging (F.29 Sprint 1.3).

Pre-F.29 every shipper exception captured under the generic ``Shipper
error`` umbrella (services/click-processor/app/shipper.py:101-103
pre-F.29: ``except Exception as e: ... sentry_sdk.capture_exception(e)``).
JSON-decode failures even ACKed silently with NO Sentry signal
(lines 71-73 pre-F.29). Sprint 1.3 attaches an ``op`` tag to every
exception path so Sentry's issue grouping splits by operation:

  * ``op=xreadgroup`` — local-Redis stream read failure
  * ``op=parse_payload`` — corrupt click JSON in a stream entry
  * ``op=batch_post`` — central collector unreachable / non-2xx
  * ``op=xack`` — XACK race / Redis blip on single-message ACK
  * ``op=xack_batch`` — XACK/XTRIM after a successful POST
  * ``op=loop_iteration`` — catch-all for unknown branches

These tests pin the tag values + the helper interfaces. Sprint 4.1
alert rules in Sentry key off these exact tag strings, so a drift in
the tag value would silently break paging.

Coverage strategy: helpers are pure (synchronous; only side effect is
Sentry SDK), so we test them directly via Sentry SDK mocking. The
helpers' wiring into ``run_shipper`` is exercised indirectly — testing
the full async loop end-to-end requires a real Redis + httpx mock and
is reserved for the Sprint 2 integration test (`test_partial_ack.py`).

Reference: F.29 plan §3 G3, §4 Sprint 1.3 row.
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest

from app import shipper
from app.shipper import (
    OP_BATCH_POST,
    OP_LOOP_ITERATION,
    OP_PARSE_PAYLOAD,
    OP_XACK,
    OP_XACK_BATCH,
    OP_XREADGROUP,
    _capture_op_exc,
    _capture_op_msg,
)


@pytest.fixture(autouse=True)
def _set_node_id(monkeypatch):
    """Pin a known node_id so the shipper.node_id tag is predictable."""
    monkeypatch.setattr(shipper.settings, "node_id", "test-node-AU")


# ---------------------------------------------------------------------------
# Op-constant pinning — Sentry alert rules query by these exact strings
# ---------------------------------------------------------------------------


def test_op_constants_are_stable_strings():
    """Sprint 4.1 alert rules will be created in Sentry with exact
    tag values matching these constants. A typo or refactor that
    changes a value silently breaks paging. Pin them explicitly.

    The values are SLUGS (snake_case, no spaces, no special chars) —
    Sentry tag values are case-sensitive and indexed, so consistency
    here is load-bearing.
    """
    assert OP_XREADGROUP == "xreadgroup"
    assert OP_PARSE_PAYLOAD == "parse_payload"
    assert OP_BATCH_POST == "batch_post"
    assert OP_XACK == "xack"
    assert OP_XACK_BATCH == "xack_batch"
    assert OP_LOOP_ITERATION == "loop_iteration"


# ---------------------------------------------------------------------------
# _capture_op_exc — exception tagging
# ---------------------------------------------------------------------------


def test_capture_op_exc_sets_op_and_node_tags():
    """Both ``op`` and ``shipper.node_id`` must be set on the scope.
    Per-node filtering is how different regions get routed to
    different on-call teams in Sprint 4.1.
    """
    exc = RuntimeError("boom")

    # `push_scope` is a context manager; we have to mock it correctly.
    scope_mock = MagicMock()
    push_scope_mock = MagicMock()
    push_scope_mock.__enter__ = MagicMock(return_value=scope_mock)
    push_scope_mock.__exit__ = MagicMock(return_value=False)

    with patch("app.shipper.sentry_sdk") as mock_sentry:
        mock_sentry.push_scope.return_value = push_scope_mock
        _capture_op_exc(OP_BATCH_POST, exc, batch_size=42)

        mock_sentry.capture_exception.assert_called_once_with(exc)

    scope_mock.set_tag.assert_any_call("op", OP_BATCH_POST)
    scope_mock.set_tag.assert_any_call("shipper.node_id", "test-node-AU")
    scope_mock.set_extra.assert_called_once_with("batch_size", 42)


def test_capture_op_exc_handles_multiple_extras():
    """Multiple extras (msg_id + context + failure_kind) must all
    reach Sentry's extras section. The shipper uses up to 3 extras
    per call site."""
    exc = ConnectionError("redis down")

    scope_mock = MagicMock()
    push_scope_mock = MagicMock()
    push_scope_mock.__enter__ = MagicMock(return_value=scope_mock)
    push_scope_mock.__exit__ = MagicMock(return_value=False)

    with patch("app.shipper.sentry_sdk") as mock_sentry:
        mock_sentry.push_scope.return_value = push_scope_mock
        _capture_op_exc(
            OP_XACK,
            exc,
            msg_id="1234-0",
            context="post-parse-failure-ack",
            failure_kind="ConnectionError",
        )

    extras_calls = scope_mock.set_extra.call_args_list
    extras_kwargs = {call.args[0]: call.args[1] for call in extras_calls}
    assert extras_kwargs == {
        "msg_id": "1234-0",
        "context": "post-parse-failure-ack",
        "failure_kind": "ConnectionError",
    }


# ---------------------------------------------------------------------------
# _capture_op_msg — message tagging (no exception object)
# ---------------------------------------------------------------------------


def test_capture_op_msg_sets_op_tag_and_level():
    """Non-exception signals (non-2xx HTTP responses, parse failures
    where the exception is suppressed) capture via ``capture_message``
    with an explicit level. Sentry alert rules filter by level too —
    "warning" vs "error" vs "fatal" — so the level passes through."""
    scope_mock = MagicMock()
    push_scope_mock = MagicMock()
    push_scope_mock.__enter__ = MagicMock(return_value=scope_mock)
    push_scope_mock.__exit__ = MagicMock(return_value=False)

    with patch("app.shipper.sentry_sdk") as mock_sentry:
        mock_sentry.push_scope.return_value = push_scope_mock
        _capture_op_msg(
            OP_BATCH_POST,
            "Central returned 503",
            level="warning",
            collector_status=503,
        )

        mock_sentry.capture_message.assert_called_once_with(
            "Central returned 503", level="warning",
        )

    scope_mock.set_tag.assert_any_call("op", OP_BATCH_POST)
    scope_mock.set_tag.assert_any_call("shipper.node_id", "test-node-AU")
    scope_mock.set_extra.assert_called_once_with("collector_status", 503)


def test_capture_op_msg_default_level_is_warning():
    """Default level=warning prevents accidentally pageable signals.
    If a Sprint 1.3 caller forgets the level kwarg, the result is
    "noisy but not pageable" — safer default than "silent" or
    "fatal"."""
    scope_mock = MagicMock()
    push_scope_mock = MagicMock()
    push_scope_mock.__enter__ = MagicMock(return_value=scope_mock)
    push_scope_mock.__exit__ = MagicMock(return_value=False)

    with patch("app.shipper.sentry_sdk") as mock_sentry:
        mock_sentry.push_scope.return_value = push_scope_mock
        _capture_op_msg(OP_PARSE_PAYLOAD, "Parse failure")

        # Default level should be "warning"
        mock_sentry.capture_message.assert_called_once_with(
            "Parse failure", level="warning",
        )


# ---------------------------------------------------------------------------
# Source-level pinning — every exception path in run_shipper uses
# a helper. A regression that introduces a bare capture_exception
# would defeat the tagging scheme.
# ---------------------------------------------------------------------------


def test_run_shipper_source_uses_op_helpers_not_bare_capture():
    """Source-level pin. Pre-F.29 ``except Exception as e:
    sentry_sdk.capture_exception(e)`` was untagged. After Sprint 1.3,
    every ``capture_exception`` call inside ``run_shipper`` should
    use the helpers (which guarantee the op tag).

    This is a static check — counts call patterns in the source.
    Catches drift from someone hand-adding a bare capture that skips
    the helper. The ``assert_shipper_ready`` function legitimately
    uses ``sentry_sdk.capture_message`` directly (it's not inside the
    tagged loop), so we only inspect the ``run_shipper`` body.
    """
    import inspect

    src = inspect.getsource(shipper.run_shipper)

    # Inside run_shipper, every Sentry signal must go through the
    # helpers — no bare capture_exception / capture_message calls.
    assert "sentry_sdk.capture_exception(" not in src, (
        "F.29 Sprint 1.3 regression: bare sentry_sdk.capture_exception "
        "call inside run_shipper — must use _capture_op_exc helper to "
        "guarantee the op tag is set."
    )
    assert "sentry_sdk.capture_message(" not in src, (
        "F.29 Sprint 1.3 regression: bare sentry_sdk.capture_message "
        "call inside run_shipper — must use _capture_op_msg helper."
    )

    # Positive pin: each major op-tag constant is referenced in
    # run_shipper. Drift here means a code path lost its tagging.
    assert "OP_PARSE_PAYLOAD" in src
    assert "OP_BATCH_POST" in src
    assert "OP_XACK" in src
    assert "OP_XACK_BATCH" in src
    assert "OP_LOOP_ITERATION" in src
