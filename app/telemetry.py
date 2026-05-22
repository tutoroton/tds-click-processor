"""Structured Sentry tagging — shared helpers across click-processor.

F.29 Sprint 1.6 (2026-05-23). Extracted from ``app.shipper`` after
validation cycle caught DRY violation: main.py:688-725 (disk-pressure
503 block) reinvented the ``push_scope + set_tag("op", ...) +
capture_*`` pattern that Sprint 1.3 had already crystallised in
``shipper._capture_op_exc / _capture_op_msg``. Moving the helpers
here gives BOTH modules (shipper + main, and any future caller) a
single canonical implementation.

Why this matters for Sprint 4.1:

  Sprint 4.1 will configure Sentry alert rules that key off exact
  ``op`` tag values. A second copy of the pattern in main.py risks
  drift (different tag spellings, missed ``shipper.node_id`` tag,
  inconsistent extras handling) which would silently break alerting.
  Centralised helpers + canonical OP_* constants eliminate that risk.

Tag schema (canonical):

  * ``op`` — string slug naming the operation that failed. One of
    the ``OP_*`` constants below. Sentry alert rules + dashboard
    widgets MUST use these exact values; never inline strings.
  * ``node_id`` — the edge-node identifier (settings.node_id). Used
    to route on-call paging by region.
  * ``extras`` — arbitrary key/value pairs visible in the issue
    detail. Not searchable as tags (cardinality too high for tag
    indexing); use for batch_size, msg_id, collector_status, etc.

Anti-patterns this module rules out (verified by source-level pin
tests in test_shipper_exception_tagging.py + the new
test_telemetry_helpers.py):

  * Bare ``sentry_sdk.capture_exception(e)`` inside a tagged code path
    — loses the op tag. Use ``capture_op_exc`` instead.
  * Bare ``set_tag("op", ...)`` without ``push_scope()`` — tag
    leaks across captures and corrupts unrelated Sentry events.
"""

from __future__ import annotations

import sentry_sdk

from app.config import settings


# ---------------------------------------------------------------------------
# Canonical operation tags. Centralised so a typo in a string literal
# can't silently shard the same logical operation across two Sentry
# tag values.
# ---------------------------------------------------------------------------

# Shipper-loop exception paths (Sprint 1.3)
OP_XREADGROUP = "xreadgroup"
OP_PARSE_PAYLOAD = "parse_payload"
OP_BATCH_POST = "batch_post"
OP_XACK = "xack"
OP_XACK_BATCH = "xack_batch"
OP_LOOP_ITERATION = "loop_iteration"

# Hot-path /decide failure modes (Sprint 1.5+)
OP_DISK_PRESSURE = "disk_pressure"


def capture_op_exc(op_name: str, exc: BaseException, **extras: object) -> None:
    """Capture an exception to Sentry with the F.29 ``op`` tag scheme.

    Args:
        op_name: One of the ``OP_*`` constants above.
        exc: The exception object to report.
        **extras: Additional context (e.g. ``msg_id``, ``batch_size``).
            Each key becomes a Sentry "extras" entry — visible in the
            issue detail but not searchable as a tag.
    """
    with sentry_sdk.push_scope() as scope:
        scope.set_tag("op", op_name)
        scope.set_tag("node_id", settings.node_id)
        for key, value in extras.items():
            scope.set_extra(key, value)
        sentry_sdk.capture_exception(exc)


def capture_op_msg(
    op_name: str,
    message: str,
    level: str = "warning",
    **extras: object,
) -> None:
    """Capture a Sentry message with the same op-tag scheme.

    Use this for non-exception signals (e.g. non-2xx HTTP responses,
    parse failures where the exception is suppressed by an
    intentional ACK, the disk-pressure 503 path).
    """
    with sentry_sdk.push_scope() as scope:
        scope.set_tag("op", op_name)
        scope.set_tag("node_id", settings.node_id)
        for key, value in extras.items():
            scope.set_extra(key, value)
        sentry_sdk.capture_message(message, level=level)
