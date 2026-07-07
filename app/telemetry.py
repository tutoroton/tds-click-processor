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

import time

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

# LOSSFIX P1b (2026-07-07) — L1: the stream write (real XADD or an M1
# gate-reject) AND the disk fallback BOTH failed to durably capture the
# click. Pre-fix this fell through to a silent 302 (click "genuinely
# lost" per the old comment); now it 503s the Worker instead — this tag
# marks that terminal, non-recoverable-here moment.
OP_CLICK_UNCAPTURED = "click_uncaptured"

# LOSSFIX P1b (2026-07-07) — M1: the cached stream:clicks length is
# at/over the reject threshold. Fired when a real click is diverted to
# the disk fallback pre-emptively (no XADD attempted) and when the
# smoke probe rejects outright (503, reject-only, no disk fallback —
# synthetic click, nothing to preserve).
OP_STREAM_ENTRY_LIMIT = "stream_entry_limit"

# LOSSFIX P2 c3 (2026-07-07) — the edge watermark sampler (app/watermark.py)
# tripped: cached used_memory% of the routing Redis is at/over the shed
# threshold. Fired once per click diverted into the disk-segment spill
# path (NOT a 503 — see app/watermark.py module docstring for why edge
# spills instead of shedding).
OP_WATERMARK_SPILL = "watermark_spill"

# LOSSFIX P2 c3 — mirrors the collector's OP_WATERMARK_SIGNAL_STALE: the
# watermark sampler's cached sample is stale (sampler wedged, dead, or
# never landed a first reading past the boot grace). Fail-open always
# wins (accept/XADD proceeds) — this tag is pure visibility.
OP_WATERMARK_SIGNAL_STALE = "watermark_signal_stale"

# LOSSFIX P2 c1 (2026-07-07) — a recovered `.wip` segment (orphan
# adoption, B1) had a torn tail: the last line was incomplete or failed
# to parse. Loss-free by construction (see disk_queue._truncate_torn_
# tail_sync docstring) — this tag is visibility on the truncation event,
# not an error signal about lost data.
OP_SEGMENT_TORN_TAIL = "segment_torn_tail"

# LOSSFIX P2 c1/c2 — the cached total disk-segment byte usage (own +
# adopted + legacy) is at/over `disk_segment_max_total_bytes`. A new
# click is REJECTED (visible 503 via the existing L1 uncaptured path)
# rather than silently rotating the oldest segment.
OP_SEGMENT_BYTE_CAP = "segment_byte_cap"

# LOSSFIX P2 c2 (B1) — startup orphan adoption claimed at least one dead
# worker's segment prefix. Informational (not an error) — visibility
# that the WC=8 stranding class did NOT go silent.
OP_SEGMENT_ORPHAN_ADOPTED = "segment_orphan_adopted"

# Returning-user identity resolver fail-open (P2, 2026-06-05). Emitted (throttled
# per company) when the resolver raises and the click degrades to legacy flags.
OP_IDENTITY = "identity_resolve"

# G-LOW-1 (SEC-M1) — returning-user WRITE-fail paths that used to swallow a Redis
# error with only a log line (no Sentry). Throttled per company so a persistent
# identity-Redis fault surfaces ONCE per window, not per click. Read failures
# stay silent (benign: a missed pin → normal selection, the click still routes).
OP_IDENTITY_PERSIST = "identity_persist"  # deferred uid/profile/history write failed
OP_STICKY_WRITE = "sticky_write"          # sticky pin SET NX / repin failed
OP_IDENTITY_STORE_PRESSURE = "identity_store_pressure"  # CAP-1 — identity-redis ≥80%/≥95% of maxmemory

# Per-click verdict outcomes (Sprint 2.2+)
OP_DEADLETTER = "deadletter"   # click hit max-attempts after rejections
OP_PARTIAL_ACK = "partial_ack"  # batch had mixed accepted+rejected
OP_LEGACY_COLLECTOR = "legacy_collector"  # shim absorbed pre-F.29 shape

# Routing-decision "silent skip / fallback" paths (audit 2026-06-03 P3
# observability — the user's #1 concern: swallowed conditions that route
# a click somewhere unexpected with NO signal). All emitted via
# `capture_op_msg_throttled` so a single misconfigured entity can't flood
# Sentry on every click.
OP_ROUTE_ERROR = "route_error"          # G-LOW-2 — route() catch-all (GEO-TDS-BACKEND-11)
OP_CRITERIA_SKIP = "criteria_skip"      # B12 — malformed flow criteria JSON skipped
OP_FLOW_LOAD = "flow_load"              # D4  — flow HASH load empty/partial → None
OP_PARAM_PARSE = "param_parse"          # D10 — param_mappings JSON unparseable
OP_PARAM_RULES = "param_rules"          # GTD-R166 W2 — campaign param-rule config/eval fail-open
OP_OFFER_RESOLVE = "offer_resolve"      # D3  — offer/target row missing → fallback
OP_SPLIT_FALLBACK = "split_fallback"    # B3  — split had no usable offers → fallback

# F4 (GTD-R173, 2026-07-05) — routing-Redis read fail-open closure. Throttled
# per campaign (dedup_key) so a persistent Redis fault surfaces ONCE per window,
# not per click.
OP_FLOW_READ_FAILED = "flow_read_failed"  # Layer 2/2b — a flow/offer/availability
#                                           Redis read failed after retry-once →
#                                           RECORDED honest outcome (was a silent
#                                           fail-open pre-F4: []/{}/None).
OP_NO_FLOW_NO_OFFER = "no_flow_no_offer"  # Layer 3 — rate signal on the (already
#                                           correct) genuine no-flow+no-offer
#                                           dead-end, previously invisible (the
#                                           click looked "handled") so a future
#                                           regression pages instead of hiding.


# ---------------------------------------------------------------------------
# Throttled message capture (audit 2026-06-03 P3 observability).
# ---------------------------------------------------------------------------
# The routing-decision skip/fallback paths fire PER CLICK. An unthrottled
# Sentry capture there would flood the issue feed (a single misconfigured
# flow whose criteria JSON is malformed skips on EVERY click hitting it) —
# the exact "Sentry quota / alert-fatigue" failure the shipper's one-shot
# shim guard already learned. So these paths capture at most ONCE per
# (op, dedup_key) per window. The dedup_key is the offending entity id
# (flow/offer/source) so distinct misconfigurations are still each visible,
# but a hot path doesn't self-DoS Sentry. The throttle check is a dict
# lookup + monotonic read — negligible on the 10ms hot-path budget; the
# actual capture only runs on the rare first-occurrence-per-window.

_throttle_state: dict[tuple[str, str], float] = {}
# Bound the dict so an adversarial spray of distinct dedup keys can't grow
# it unbounded. A clear() on overflow just re-opens the throttle window for
# everything — acceptable for an observability throttle (worst case: one
# extra event per key after a flush), and far cheaper than an LRU.
_THROTTLE_MAX_KEYS = 1024


def capture_op_msg_throttled(
    op_name: str,
    dedup_key: object,
    message: str,
    *,
    level: str = "warning",
    window_sec: float = 300.0,
    **extras: object,
) -> bool:
    """Capture a message at most once per ``(op_name, dedup_key)`` per
    ``window_sec``. Returns True if it captured, False if throttled.

    Use for per-click skip/fallback signals; pass the offending entity id
    as ``dedup_key`` so distinct misconfigurations remain individually
    visible while a single one can't spam Sentry on every click.
    """
    now = time.monotonic()
    key = (op_name, str(dedup_key))
    last = _throttle_state.get(key)
    if last is not None and (now - last) < window_sec:
        return False
    if len(_throttle_state) >= _THROTTLE_MAX_KEYS and key not in _throttle_state:
        _throttle_state.clear()
    _throttle_state[key] = now
    capture_op_msg(op_name, message, level=level, **extras)
    return True


def _reset_throttle_for_tests() -> None:
    """Test-only — clear the throttle window between tests."""
    _throttle_state.clear()


def capture_op_exc(
    op_name: str,
    exc: BaseException,
    tags: dict[str, str] | None = None,
    **extras: object,
) -> None:
    """Capture an exception to Sentry with the F.29 ``op`` tag scheme.

    Args:
        op_name: One of the ``OP_*`` constants above.
        exc: The exception object to report.
        tags: LOSSFIX P3 (2026-07-07, alert-rule wiring) — additional
            SEARCHABLE tags (e.g. ``{"failure_kind": type(exc).__name__}``).
            Unlike ``**extras`` below, a Sentry issue-alert rule CAN
            filter on these (the classic "event's tags match" condition
            only sees indexed tags, never `set_extra` context) — use
            this when a caller's alert spec needs to filter/exclude by
            a dynamic value (see ``OP_LOOP_ITERATION``'s
            ``failure_kind != TimeoutError`` filter in
            docs/development/lossfix-p3-2026-07-07/ALERT-RULES.md).
        **extras: Additional context (e.g. ``msg_id``, ``batch_size``).
            Each key becomes a Sentry "extras" entry — visible in the
            issue detail but NOT searchable as a tag (use ``tags=``
            above for anything an alert rule needs to filter on).
    """
    with sentry_sdk.push_scope() as scope:
        scope.set_tag("op", op_name)
        scope.set_tag("node_id", settings.node_id)
        for key, value in (tags or {}).items():
            scope.set_tag(key, value)
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
