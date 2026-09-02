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

# GTD-R183 (2026-07-18) — per-click rejected-item bookkeeping (retry
# counter / re-XADD / deadletter) inside a per-batch verdict response.
# Pre-fix, an exception anywhere in that bookkeeping propagated out of
# the whole batch-response handler BEFORE the accepted/duplicate portion
# was ACKed — stranding the entire batch (including clicks central had
# already confirmed) in the local PEL until the periodic orphaned-PEL
# reclaim (60s idle + 30s interval), observed as the GTD-R177 60-90s
# landing tail. Now this bookkeeping is wrapped locally so a failure
# there can't block the ACK of the rest of the batch.
OP_REJECTED_HANDLING = "rejected_handling"

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

# GTD-D149 — the preview admission cap refused a request: the bulkhead is
# SHEDDING preview load to protect click serving. Working as designed under
# a preview flood (the owner's mandate says preview volume may exceed
# clicks), so this is visibility, not an error — but a SUSTAINED stream of
# these is the signal to look at TDS_PREVIEW_MAX_CONCURRENCY and the D149
# rig before raising anything. Throttled: a flood would otherwise fire it
# per refused request.
OP_PREVIEW_CAPACITY_SHED = "preview_capacity_shed"

# Edge-preview P2.2 (anchor §11) — a /preview presented a key hash, the
# lookup missed, AND the `preview_keys:synced` marker is absent: this node
# has never received the preview_key family at all. Degraded sync, NOT a
# cross-tenant probe — the two answer the IDENTICAL wire shape by design
# (the dead-link `matched:false`), so this op is the only place the
# difference is visible. Throttled: a stuck sync would otherwise fire it
# on every keyed preview.
OP_PREVIEW_KEYS_UNSYNCED = "preview_keys_unsynced"

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

# LOSSFIX-4 (2026-08-15) — the DURABLE deadletter park never drains itself.
# A parked click is safe but NOT delivered, and only an operator running
# `scripts/replay_deadletter_park.py` closes that gap. Unwatched, "durable"
# quietly becomes "forgotten".
OP_DEADLETTER_PARK_DEPTH = "deadletter_park_depth"

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

# [TDSP][E19] root-fix for GTD-R466 (2026-07-27): these sites had no explicit
# capture and relied entirely on Sentry's default LoggingIntegration (now
# disabled via event_level=None in main.py). Each guards a periodic-loop
# watchdog whose own silent crash disables a backstop with no other signal —
# always via capture_op_msg_throttled (never raw), per this service's own
# hard lesson (GTD-R454/PR #617: an unthrottled per-tick capture is how 77%
# of the org's 90-day error volume happened).
OP_OBS_STREAM_EMIT_FAILED = "obs_stream_emit_failed"
OP_OBS_SHIPPER_HEALTH_EMIT_FAILED = "obs_shipper_health_emit_failed"
OP_OBS_LOOP_ITERATION = "obs_loop_iteration"
OP_WATERMARK_SAMPLER_ERROR = "watermark_sampler_error"
OP_DISK_STATS_SAMPLER_ERROR = "disk_stats_sampler_error"
OP_DISK_DRAINER_ERROR = "disk_drainer_error"
OP_DOMAIN_BINDING_PARSE = "domain_binding_parse"
OP_SYNC_PULL_FAILED = "sync_pull_failed"

# A2 (tenant isolation, 2026-08-21) — rate signal on the geo corridor's mouth
# gate: domain traffic refused because it resolved no usable route.
#
# WHY THIS NEEDS ITS OWN SIGNAL, in the same spirit as OP_NO_FLOW_NO_OFFER
# above. Before A2 these clicks were SERVED by a draw over the global campaign
# pool, so a misconfiguration looked like traffic working. Now they are
# refused — correctly — but a refusal is only as good as its visibility, and
# this path is the least visible in the service: measured on staging, all
# 7 330 `domain_blocked` events over 90 days carry `company_id=0`, so no
# tenant-scoped surface can show them to the tenant whose traffic they are.
#
# The refusal is RIGHT; a SPIKE in it is not. A binding that stops resolving
# after a bad config push produces exactly this op at volume, and without the
# signal the first symptom would be a customer asking why their link is dead.
# Throttled per hostname so one broken host cannot drown the others — the
# lesson of GTD-R454, where an unthrottled per-tick capture produced 77% of
# the org's 90-day error volume.
OP_BLOCKED_NO_ROUTE = "blocked_no_route"

# [TDSP][E23] (2026-07-27) — GTD-R454 fixed the shipper loop's DOUBLE
# report (log line + tagged capture -> 1 event); GTD-R466 fixed sites
# that had NO capture at all. Neither touched the surviving gap this
# closes: a handful of exception captures already routed through the
# tagged helper, on a hot/cyclic path, with no throttle — so a GENUINE
# sustained storage outage (not a code bug) still floods Sentry at the
# path's natural tick/click rate for as long as the outage lasts. These
# two are the previously-RAW (untagged AND unthrottled) `/decide`
# hot-path sites found in that audit — every click hits them when local
# Redis is down, which is the single largest volume source of the two.
OP_CLICK_DEDUP_FAILED = "click_dedup_failed"  # SETNX dedup degraded, fail-open
OP_STREAM_WRITE_FAILED = "stream_write_failed"  # primary stream:clicks XADD failed


# ---------------------------------------------------------------------------
# Throttled capture (audit 2026-06-03 P3 observability; exception variant
# added [TDSP][E23] 2026-07-27).
# ---------------------------------------------------------------------------
# The routing-decision skip/fallback paths fire PER CLICK. An unthrottled
# Sentry capture there would flood the issue feed (a single misconfigured
# flow whose criteria JSON is malformed skips on EVERY click hitting it) —
# the exact "Sentry quota / alert-fatigue" failure the shipper's one-shot
# shim guard already learned. So these paths capture at most ONCE per
# (op, dedup_key) per window. The dedup_key is the offending entity id
# (flow/offer/source) or a failure classifier (e.g. exception type) so
# distinct failure modes are still each visible, but a hot/cyclic path
# doesn't self-DoS Sentry when the SAME failure repeats (a sustained
# storage outage is exactly this shape — see OP_LOOP_ITERATION /
# OP_STREAM_WRITE_FAILED / OP_CLICK_DEDUP_FAILED callers). The throttle
# check is a dict lookup + monotonic read — negligible on the 10ms
# hot-path budget; the actual capture only runs on the rare
# first-occurrence-per-window.
#
# A throttled occurrence is not a LOST occurrence: every suppression
# increments a per-key counter, and the next capture that actually fires
# for that key reports it as a ``suppressed_since_last_capture`` extra —
# so "how many did we not see" always survives even though the
# individual suppressed events don't.

_throttle_state: dict[tuple[str, str], float] = {}
_suppressed_count: dict[tuple[str, str], int] = {}
# Bound the dicts so an adversarial spray of distinct dedup keys can't grow
# them unbounded. A clear() on overflow just re-opens the throttle window
# for everything — acceptable for an observability throttle (worst case: one
# extra event per key after a flush, and one under-count of suppressions
# spanning the flush), and far cheaper than an LRU.
_THROTTLE_MAX_KEYS = 1024


def _throttle_gate(op_name: str, dedup_key: object, window_sec: float) -> tuple[bool, int]:
    """Shared throttle decision for the message/exception helpers below.

    Returns ``(should_capture, suppressed_since_last_capture)``. When
    ``should_capture`` is False the caller must do nothing else — the
    occurrence has been counted and folded into the next real capture.
    """
    now = time.monotonic()
    key = (op_name, str(dedup_key))
    last = _throttle_state.get(key)
    if last is not None and (now - last) < window_sec:
        _suppressed_count[key] = _suppressed_count.get(key, 0) + 1
        return False, 0
    if len(_throttle_state) >= _THROTTLE_MAX_KEYS and key not in _throttle_state:
        _throttle_state.clear()
        _suppressed_count.clear()
    _throttle_state[key] = now
    return True, _suppressed_count.pop(key, 0)


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
    should_capture, suppressed = _throttle_gate(op_name, dedup_key, window_sec)
    if not should_capture:
        return False
    if suppressed:
        extras["suppressed_since_last_capture"] = suppressed
    capture_op_msg(op_name, message, level=level, **extras)
    return True


def capture_op_exc_throttled(
    op_name: str,
    dedup_key: object,
    exc: BaseException,
    *,
    tags: dict[str, str] | None = None,
    window_sec: float = 300.0,
    **extras: object,
) -> bool:
    """Capture an exception at most once per ``(op_name, dedup_key)`` per
    ``window_sec``. Returns True if it captured, False if throttled.

    Use for exception paths on a hot request path or a periodic/cyclic
    loop (shipper batches, reclaim ticks, watchdog ticks) where the SAME
    failure can repeat every occurrence/tick for the duration of a real
    outage — pass the failure classifier (typically
    ``type(exc).__name__``, optionally namespaced by call site so
    unrelated loops don't share a window) as ``dedup_key`` so a distinct
    failure_kind remains individually visible while a persistent one
    can't flood Sentry at the loop's natural tick rate.
    """
    should_capture, suppressed = _throttle_gate(op_name, dedup_key, window_sec)
    if not should_capture:
        return False
    if suppressed:
        extras["suppressed_since_last_capture"] = suppressed
    capture_op_exc(op_name, exc, tags=tags, **extras)
    return True


def _reset_throttle_for_tests() -> None:
    """Test-only — clear the throttle window between tests."""
    _throttle_state.clear()
    _suppressed_count.clear()


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
