"""Tests for the shared Sentry telemetry helpers (F.29 Sprint 1.6).

Validation cycle 2026-05-23 caught a DRY violation: main.py:688-725
(disk-pressure 503 block) reinvented the ``push_scope + set_tag("op", ...) +
capture_*`` pattern that Sprint 1.3 had already crystallised in
shipper._capture_op_exc / _capture_op_msg.

Sprint 1.6 extracted the helpers + canonical OP_* constants to
``app.telemetry``. These tests:

  * Pin the OP_* constants used by Sprint 4.1 Sentry alert rules.
  * Verify the helpers wire push_scope + set_tag + capture_* in one
    canonical incantation (no drift between shipper / main / future
    callers).
  * Cover OP_DISK_PRESSURE (new in Sprint 1.6, used by main.py).

Note: ``test_shipper_exception_tagging.py`` already exercises the
helpers in their shipper context via the underscore-prefix aliases
(``_capture_op_exc`` etc.). This file pins the canonical NAMES that
external modules (main.py disk-pressure block; future Sprint 2 + 3
callers) will import.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from app import telemetry
from app.telemetry import (
    OP_BATCH_POST,
    OP_DISK_PRESSURE,
    OP_LOOP_ITERATION,
    OP_PARSE_PAYLOAD,
    OP_XACK,
    OP_XACK_BATCH,
    OP_XREADGROUP,
    capture_op_exc,
    capture_op_msg,
)


@pytest.fixture(autouse=True)
def _set_node_id(monkeypatch):
    """Pin a known node_id so the node_id tag is predictable."""
    monkeypatch.setattr(telemetry.settings, "node_id", "test-node-AU")


# ---------------------------------------------------------------------------
# OP_* constants — pinned exact strings (Sprint 4.1 alert rules)
# ---------------------------------------------------------------------------


def test_canonical_op_constants_are_stable_strings():
    """Sprint 4.1 alert rules will be created in Sentry with these
    exact tag values. A typo or refactor that changes a value
    silently breaks paging."""
    assert OP_XREADGROUP == "xreadgroup"
    assert OP_PARSE_PAYLOAD == "parse_payload"
    assert OP_BATCH_POST == "batch_post"
    assert OP_XACK == "xack"
    assert OP_XACK_BATCH == "xack_batch"
    assert OP_LOOP_ITERATION == "loop_iteration"
    assert OP_DISK_PRESSURE == "disk_pressure"


def test_op_disk_pressure_is_the_canonical_main_py_value():
    """Pre-Sprint-1.6 main.py:701 used the inline string
    ``"disk_pressure"`` for its op tag, while the shipper had the
    OP_* constants centralised. Drift between the two was a real
    risk. Sprint 1.6 unified them. Pin the value so a future rename
    of OP_DISK_PRESSURE forces an explicit decision rather than a
    silent break of Sentry alert binding."""
    assert OP_DISK_PRESSURE == "disk_pressure"


# ---------------------------------------------------------------------------
# capture_op_exc — exception tagging
# ---------------------------------------------------------------------------


def test_capture_op_exc_sets_op_and_node_id_tags():
    """Both ``op`` and ``node_id`` (the simpler unified tag, NOT
    ``shipper.node_id`` which was Sprint 1.3 local convention) must
    be set. Sprint 1.6 unified the tag name across shipper +
    disk-pressure + future callers — a single ``node_id`` value
    routes alerts uniformly."""
    exc = RuntimeError("boom")

    scope_mock = MagicMock()
    push_scope_mock = MagicMock()
    push_scope_mock.__enter__ = MagicMock(return_value=scope_mock)
    push_scope_mock.__exit__ = MagicMock(return_value=False)

    with patch("app.telemetry.sentry_sdk") as mock_sentry:
        mock_sentry.push_scope.return_value = push_scope_mock
        capture_op_exc(OP_DISK_PRESSURE, exc, free_bytes=512, click_id="X")

        mock_sentry.capture_exception.assert_called_once_with(exc)

    scope_mock.set_tag.assert_any_call("op", OP_DISK_PRESSURE)
    scope_mock.set_tag.assert_any_call("node_id", "test-node-AU")

    extras_calls = scope_mock.set_extra.call_args_list
    extras_kwargs = {call.args[0]: call.args[1] for call in extras_calls}
    assert extras_kwargs == {"free_bytes": 512, "click_id": "X"}


# ---------------------------------------------------------------------------
# capture_op_msg — message tagging (used by /decide disk-pressure path)
# ---------------------------------------------------------------------------


def test_capture_op_msg_for_disk_pressure():
    """The main.py disk-pressure 503 block calls this with
    OP_DISK_PRESSURE + level="error". Pin the exact contract that
    binding rests on."""
    scope_mock = MagicMock()
    push_scope_mock = MagicMock()
    push_scope_mock.__enter__ = MagicMock(return_value=scope_mock)
    push_scope_mock.__exit__ = MagicMock(return_value=False)

    with patch("app.telemetry.sentry_sdk") as mock_sentry:
        mock_sentry.push_scope.return_value = push_scope_mock
        capture_op_msg(
            OP_DISK_PRESSURE,
            "Disk under pressure: 500 < 1GiB",
            level="error",
            free_bytes=500,
            threshold_bytes=1_073_741_824,
            click_id="abc",
        )

        mock_sentry.capture_message.assert_called_once_with(
            "Disk under pressure: 500 < 1GiB",
            level="error",
        )

    scope_mock.set_tag.assert_any_call("op", OP_DISK_PRESSURE)
    scope_mock.set_tag.assert_any_call("node_id", "test-node-AU")

    extras_calls = scope_mock.set_extra.call_args_list
    extras_kwargs = {call.args[0]: call.args[1] for call in extras_calls}
    assert extras_kwargs == {
        "free_bytes": 500,
        "threshold_bytes": 1_073_741_824,
        "click_id": "abc",
    }


def test_capture_op_msg_default_level_is_warning():
    """Default level=warning prevents accidentally pageable signals.
    Sprint 4.1 alert rules treat ``error`` and above as paging; the
    safer default is ``warning`` which only writes to the issue
    feed."""
    scope_mock = MagicMock()
    push_scope_mock = MagicMock()
    push_scope_mock.__enter__ = MagicMock(return_value=scope_mock)
    push_scope_mock.__exit__ = MagicMock(return_value=False)

    with patch("app.telemetry.sentry_sdk") as mock_sentry:
        mock_sentry.push_scope.return_value = push_scope_mock
        capture_op_msg(OP_BATCH_POST, "central 500")

        mock_sentry.capture_message.assert_called_once_with(
            "central 500", level="warning",
        )


# ---------------------------------------------------------------------------
# Source-level pin — main.py disk-pressure block uses the helper
# (not a re-inlined push_scope incantation)
# ---------------------------------------------------------------------------


def test_main_py_disk_pressure_uses_canonical_helper():
    """Pre-Sprint-1.6 main.py:688-725 reinvented the push_scope +
    set_tag dance, duplicating shipper.py:84-110. Sprint 1.6
    refactored to use the shared helper. Source-level pin guards
    against regression to inline pattern.

    NB: a sentry_sdk.push_scope call ELSEWHERE in main.py is allowed
    (e.g., if a future code path has legitimate reason to bypass the
    helper). We only pin the disk-pressure block specifically — by
    asserting OP_DISK_PRESSURE is referenced via the helper, not via
    an inline string literal.
    """
    from pathlib import Path

    src_path = Path(__file__).parent.parent.parent / "app" / "main.py"
    src = src_path.read_text()

    # The disk_pressure handling block must reference the canonical
    # OP constant, not the inline string.
    assert "capture_op_msg(" in src and "OP_DISK_PRESSURE" in src, (
        "F.29 Sprint 1.6 regression: main.py no longer references the "
        "canonical OP_DISK_PRESSURE via capture_op_msg helper. Did "
        "the disk-pressure block get reinlined?"
    )

    # Inline ``"disk_pressure"`` string in the 503 block would be a
    # drift — search for the literal anywhere except the OP_*
    # constant declaration in telemetry.py (which IS the canonical).
    # In main.py, the only legitimate occurrence is the response
    # detail="disk_pressure" — the user-facing HTTP signal.
    inline_occurrences = src.count('"disk_pressure"')
    # Expected: detail="disk_pressure" (HTTPException) — exactly 1
    # occurrence. More than that suggests inline regression.
    assert inline_occurrences <= 1, (
        f"F.29 Sprint 1.6 regression: main.py contains "
        f"{inline_occurrences} inline 'disk_pressure' string "
        "occurrences. Use OP_DISK_PRESSURE for the op tag; the only "
        "inline literal should be the HTTPException detail field."
    )
