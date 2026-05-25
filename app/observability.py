"""Periodic metric emission for click-processor's zero-loss layer.

T2.6 partial — gives operators *visibility* into the defenses
shipped by T2.1 (`stream:clicks` MAXLEN cap) and T2.2 (disk
fallback queue). Without these emissions, the defenses are
invisible: a Redis outage that exhausted disk-queue capacity, or a
collector outage approaching the stream-length cap, would only
surface at the moment routing fails. Now the operator gets a
warn-level Sentry breadcrumb at 50% of either cap — actionable
lead time.

Emission strategy: WARN-level structured log + Sentry
``capture_message``. Sentry's metrics API was deprecated in
``sentry-sdk`` 2.x; ``capture_message`` is the supported path that
Sentry alert rules can fire on. The shape (`area` tag,
`metric` tag) lets operators write a single alert rule per metric
across environments.

Two metrics emitted by ``run_observability_loop``:

  * ``stream.clicks.length`` — XLEN of `stream:clicks`. The
    shipper's post-success XTRIM keeps the stream at ~10k in
    steady state, so a sustained value above 10k indicates the
    central collector is unreachable (shipper failing to ack).
    50% of MAXLEN cap = "outage has been ongoing for hours,
    intervene now".

  * ``disk_queue.size`` — count of files awaiting drainer replay.
    Steady-state value is 0 (Redis healthy). Any non-zero value
    means at least one click hit the XADD failure path; growing
    values mean Redis is sustained unhealthy. 50% of cap = "30+
    minutes of outage at typical click rates, escalate".

Three metrics from the original T2.6 plan are deferred:

  * ``sync.push.rtt_p95`` — needs a rolling-percentile accumulator
    (admin-api side). Follow-up.
  * ``click_dedup.eviction_rate`` — needs Redis INFO sampling +
    delta calculation. Follow-up.
  * ``apply_snapshot.elapsed_ms`` — already persisted in
    `edge_nodes.last_sync_elapsed_ms` (T1.4 / migration 048); a
    Sentry mirror is non-essential because the dashboard surfaces
    the same data. Follow-up if dashboard latency shows blind
    spots.

Reference: rule `sync-protocol`, action-items.md T2.6,
open-questions.md G-24.
"""

from __future__ import annotations

import asyncio
import logging

import sentry_sdk

from app.config import _LOCAL_ENVIRONMENTS, settings
from app.disk_queue import get_queue_size
from app.shipper_metrics import metrics as shipper_metrics

logger = logging.getLogger("tds.observability")


# Stream key + shipper consumer-group name — duplicated from
# `shipper.py` to avoid a circular import (shipper imports from
# app.config, observability would end up cross-importing). These are
# stable Redis-protocol constants. A parity test
# (`test_observability.py`) asserts they stay equal to
# `shipper.STREAM_KEY` / `shipper.GROUP_NAME` so drift is caught.
_STREAM_KEY = "stream:clicks"
_SHIPPER_GROUP = "shippers"

# Threshold ratio for the warn-level alert. 0.5 = "at 50% of cap".
# Hardcoded rather than env-configurable because the threshold's
# meaning is "give the operator hours of lead time before the cap
# triggers"; tuning it environment-by-environment would defeat the
# uniform alerting story.
_WARN_THRESHOLD_RATIO = 0.5


async def emit_stream_clicks_length(redis) -> int:
    """Sample XLEN of `stream:clicks`; warn at 50% of MAXLEN cap.

    Returns the sampled length so callers can log it themselves
    (e.g., a /health endpoint that wants to expose it). On XLEN
    failure (Redis impaired), returns -1 and emits a warning —
    same Redis impairment that T2.2's disk fallback handles.

    Lower bound on the cap defends against divide-by-zero when an
    operator misconfigures `TDS_STREAM_CLICKS_MAXLEN=0`. The
    setting itself rejects this in its Pydantic constraint, but
    we keep the runtime guard for defense in depth.
    """
    try:
        length = await redis.xlen(_STREAM_KEY)
    except Exception as exc:  # noqa: BLE001 — Redis impairment is exactly the case we monitor
        logger.warning(
            "stream.clicks.length sample failed: %s",
            exc,
            extra={"area": "observability", "metric": "stream.clicks.length"},
        )
        return -1

    cap = max(1, settings.stream_clicks_maxlen)
    pct = (length * 100) // cap
    extra = {
        "area": "observability",
        "metric": "stream.clicks.length",
        "length": length,
        "cap": cap,
        "pct": pct,
    }

    if length >= int(cap * _WARN_THRESHOLD_RATIO):
        logger.warning(
            "stream.clicks.length=%d at %d%% of cap %d — "
            "central-collector outage suspected, intervene before "
            "MAXLEN cap trims oldest clicks.",
            length, pct, cap,
            extra=extra,
        )
        sentry_sdk.capture_message(
            f"stream:clicks at {length}/{cap} ({pct}%) — "
            f"approaching MAXLEN cap",
            level="warning",
        )
    elif length > 0:
        # Sub-threshold but non-zero — log at DEBUG so operators
        # can see the trend if they enable verbose logging, but
        # don't spam INFO. Steady-state value is 0-10k (shipper
        # XTRIMs to 10k after each successful ship); this branch
        # documents that "small non-zero is normal".
        logger.debug(
            "stream.clicks.length=%d (%d%% of cap)",
            length, pct, extra=extra,
        )
    return length


async def emit_disk_queue_size() -> int:
    """Sample disk-queue file count; warn at 50% of cap.

    Steady-state value is 0 — disk queue fires only on XADD
    failure, and the drainer pulls everything back into Redis on
    recovery. Any sustained non-zero value is a signal that
    Redis is unhealthy AND the disk fallback is doing its job.

    `disk_queue_max_files=0` (operator opt-in for unbounded queue)
    skips the threshold check — there's no "50% of unbounded".
    """
    size = await get_queue_size()
    cap = settings.disk_queue_max_files

    if cap == 0:
        # Unbounded — operator chose this. Just log the size when
        # non-zero so the trend is visible.
        if size > 0:
            logger.info(
                "disk_queue.size=%d (unbounded mode)",
                size,
                extra={
                    "area": "observability",
                    "metric": "disk_queue.size",
                    "size": size,
                    "cap": 0,
                },
            )
        return size

    pct = (size * 100) // max(1, cap)
    extra = {
        "area": "observability",
        "metric": "disk_queue.size",
        "size": size,
        "cap": cap,
        "pct": pct,
    }

    if size >= int(cap * _WARN_THRESHOLD_RATIO):
        logger.warning(
            "disk_queue.size=%d at %d%% of cap %d — Redis outage "
            "ongoing, T2.2 fallback engaged. Investigate redis-server "
            "health and consider raising TDS_DISK_QUEUE_MAX_FILES "
            "if outage will persist.",
            size, pct, cap,
            extra=extra,
        )
        sentry_sdk.capture_message(
            f"disk_queue at {size}/{cap} ({pct}%) — approaching cap",
            level="warning",
        )
    elif size > 0:
        # Non-zero but sub-threshold. INFO so operators see the
        # outage start without the noise of a Sentry breadcrumb
        # for every drain cycle.
        logger.info(
            "disk_queue.size=%d (%d%% of cap)",
            size, pct, extra=extra,
        )
    return size


async def _shipper_backlog(redis) -> int | None:
    """Unshipped-click backlog for the shipper consumer group.

    F-3 (audit 2026-05-25). The shipper consumes ``stream:clicks`` via a
    consumer group (``XREADGROUP {STREAM_KEY: ">"}``). "Work the shipper
    has not delivered" therefore has two parts:
      * ``pending`` — entries delivered to the group but not yet XACKed
        (read but ship+ack failed → genuinely stuck in flight).
      * ``lag`` — entries in the stream the group has not read yet
        (transient when the shipper is alive; the blackout detector in
        :func:`emit_shipper_health` covers a dead shipper).
    ``XINFO GROUPS`` returns both in one round-trip; backlog = their sum.

    Returns the backlog count (``>= 0``) when known, or ``None`` when it
    cannot be determined — group absent, ``lag`` unavailable in a way
    that raises, or Redis impaired. ``None`` means "unknown" and the
    caller FAILS OPEN (keeps the legacy lag-only alert) so the real
    stall signal is never weakened by this gate. Redis ``lag`` may be
    nil after stream trims/deletes; that nil is treated as 0 (pending is
    always exact), so a missing nil-lag degrades to a pending-only gate
    rather than failing open.
    """
    try:
        groups = await redis.xinfo_groups(_STREAM_KEY)
    except Exception:  # noqa: BLE001 — unknown → caller fails open
        return None
    for g in groups:
        name = g.get("name")
        if isinstance(name, bytes):
            name = name.decode("utf-8")
        if name == _SHIPPER_GROUP:
            pending = int(g.get("pending") or 0)
            lag = g.get("lag")
            lag = int(lag) if lag is not None else 0
            return pending + lag
    # Group not created yet → no backlog can be attributed to it; but a
    # not-yet-started shipper is the blackout detector's job. Unknown.
    return None


async def emit_shipper_health(redis=None) -> None:
    """F.29 Sprint 4.1 — emit Sentry signals on shipper degradation.

    Runs on the observability loop's OWN asyncio task, independent of the
    shipper coroutine, so it can detect the shipper being WEDGED or dead —
    the audit-2026-05-16 50-day blackout case the shipper loop itself
    cannot self-report (a dead loop emits nothing). Reads the in-memory
    :data:`shipper_metrics` singleton (no Redis needed) and emits stable-
    message ``capture_message`` events that Sentry issue-alert rules
    page/warn on. The rule configs live in the capacity/alert runbook
    (``docs/development/capacity-validation-1000rps.md``) because the
    Sentry MCP cannot create alert rules.

    Levels: ``error`` = page-worthy (pipeline halted / stalled), ``warning``
    = degraded (success ratio dipping). "Sustained" framing is enforced by
    the Sentry alert rule's evaluation window, not here — this emits the
    per-tick signal; Sentry groups + escalates.

    Emits at most one signal per tick: the "not running" blackout signal
    dominates (lag + ratio are moot when the loop is dead), so it returns
    early. Otherwise lag (page) and success-ratio (warn) are independent.
    """
    m = shipper_metrics
    is_local = settings.environment in _LOCAL_ENVIRONMENTS

    # (1) Blackout detector — shipper NOT running while it MUST be. Only
    # meaningful in a non-local env WITH a central_url configured (a
    # standalone / local node legitimately runs no shipper). This is the
    # exact pathology behind the 50-day silent click loss (audit
    # 2026-05-16): /health=200 (Redis ping fine) while clicks never reach
    # central. The independent task is the only thing that can see it.
    if not is_local and settings.central_url and not m.running:
        logger.error(
            "shipper.health — shipper NOT running (env=%s, central_url set); "
            "clicks are NOT being delivered to central. This is the "
            "audit-2026-05-16 blackout pattern.",
            settings.environment,
            extra={"area": "observability", "metric": "shipper.running"},
        )
        sentry_sdk.capture_message(
            "shipper not running — click delivery to central HALTED",
            level="error",
        )
        return

    # (2) Lag — the click pipeline has stalled. Only meaningful once a
    # batch has been attempted (lag_seconds is None before the first).
    #
    # F-3 (audit 2026-05-25) — de-noise. `lag_seconds` is `now -
    # last_ship_at`, and `last_ship_at` only advances on a real ship
    # (an idle XREADGROUP poll does not refresh it). So in low-traffic
    # staging the lag grows simply because there was nothing to ship —
    # firing ~2498 false pages/13h and burying real stalls. Gate the
    # alert on actual unshipped backlog: page only when lag exceeds the
    # threshold AND the shipper group still holds undelivered/unacked
    # clicks. When the group is fully drained (backlog == 0), idle time
    # is NOT a stall → suppress. Backlog UNKNOWN (no redis handle, group
    # absent, Redis impaired) → fail OPEN, preserving the legacy alert.
    lag = m.lag_seconds
    if lag is not None and lag > settings.shipper_lag_alert_seconds:
        backlog = await _shipper_backlog(redis) if redis is not None else None
        if backlog == 0:
            logger.debug(
                "shipper.health — lag %.0fs over threshold but group fully "
                "drained (backlog=0); idle, not a stall — alert suppressed.",
                lag,
            )
        else:
            backlog_desc = "unknown" if backlog is None else str(backlog)
            logger.error(
                "shipper.health — last ship %.0fs ago exceeds %ds threshold "
                "(status=%s, backlog=%s); central delivery stalled.",
                lag, settings.shipper_lag_alert_seconds, m.last_ship_status,
                backlog_desc,
                extra={"area": "observability", "metric": "shipper.lag_seconds"},
            )
            sentry_sdk.capture_message(
                f"shipper lag {lag:.0f}s exceeds "
                f"{settings.shipper_lag_alert_seconds}s with backlog="
                f"{backlog_desc} — central delivery stalled",
                level="error",
            )

    # (3) Success ratio — warn when delivery is degrading, but only with a
    # meaningful sample (a lone rejected click reads as ratio=0.0).
    ratio = m.success_ratio_5m
    if (
        ratio is not None
        and m.window_sample_size >= settings.shipper_success_ratio_alert_min_sample
        and ratio < settings.shipper_success_ratio_alert_min
    ):
        logger.warning(
            "shipper.health — success_ratio_5m=%.4f below %.2f over %d "
            "clicks; collector rejecting/deadlettering an elevated share.",
            ratio, settings.shipper_success_ratio_alert_min,
            m.window_sample_size,
            extra={"area": "observability", "metric": "shipper.success_ratio_5m"},
        )
        sentry_sdk.capture_message(
            f"shipper success_ratio_5m {ratio:.4f} below "
            f"{settings.shipper_success_ratio_alert_min} "
            f"(sample={m.window_sample_size})",
            level="warning",
        )


async def run_observability_loop(redis, interval: int = 60) -> None:
    """Periodic emission loop. Started in FastAPI lifespan,
    cancelled on shutdown.

    Robust to per-iteration errors — a transient failure (e.g.,
    a one-off Redis blip during XLEN) doesn't kill the loop, and
    each metric is sampled independently so one failing emission
    doesn't suppress the others.

    Default interval 60s mirrors typical Sentry-alert evaluation
    cadences (1-5 minute windows) — finer granularity would emit
    duplicate breadcrumbs without giving operators new signal.
    """
    logger.info(
        "Observability loop started (interval=%ds, threshold=%d%%)",
        interval, int(_WARN_THRESHOLD_RATIO * 100),
    )
    while True:
        try:
            await asyncio.sleep(interval)
            # Each metric is independently caught — failure of one
            # does not suppress the next. Shared try/except below
            # only catches catastrophic loop failures (CancelledError
            # is re-raised so shutdown propagates cleanly).
            try:
                await emit_stream_clicks_length(redis)
            except Exception:  # noqa: BLE001
                logger.exception(
                    "emit_stream_clicks_length raised — continuing",
                )
            try:
                await emit_disk_queue_size()
            except Exception:  # noqa: BLE001
                logger.exception(
                    "emit_disk_queue_size raised — continuing",
                )
            try:
                await emit_shipper_health(redis)
            except Exception:  # noqa: BLE001
                logger.exception(
                    "emit_shipper_health raised — continuing",
                )
        except asyncio.CancelledError:
            logger.info("Observability loop cancelled — shutting down")
            raise
        except Exception:  # noqa: BLE001
            logger.exception("Observability loop iteration failed")
