"""Diagnostic toolkit — test_id propagation + observation stream emitter.

Companion to rule `diagnostic-mode` (toggle discipline + production safety)
and skill `diagnostic-tracing` (HOW to invoke `geotds-traffic trace --test-id`
and interpret the rendered timeline).

## Why this module exists

Traffic simulation produces synthetic probes tagged with a UUID per
request (`X-Test-Id` header). When `TDS_DIAG_OBS_STREAM=true` AND the
header is present, every significant checkpoint along the request
lifecycle is appended to a per-test Redis stream `obs:test:<id>`.
The trace CLI later reads this stream from EVERY edge node + central
admin-api, merges by timestamp, and renders a single chronological
timeline so the operator can see "Worker dispatched → which node
received → cascade resolution → action chosen → macro substituted →
stream XADD → response returned" in one view.

Without this module the operator's only correlation tool is Sentry
breadcrumbs (sample-rate-gated, eventually consistent, no per-request
chronology view). Sentry is great for "what crashed" but useless for
"what happened in this 15-ms window".

## Three orthogonal toggles

The module reads three settings from `app.config.settings`:

| Toggle                   | Effect                                          |
|--------------------------|-------------------------------------------------|
| `diag_traces_boost`      | Sentry tracesSampler returns 1.0 for tagged req |
| `diag_obs_stream`        | Append to Redis `obs:test:<id>` per checkpoint  |
| `diag_verbose_logs`      | Promote checkpoint structured logs DEBUG → INFO |

All three default `False` (production safety). Each gates an INDEPENDENT
behaviour — operator can enable obs_stream alone for offline forensic
without paying Sentry quota, or traces_boost alone for live Sentry
investigation without the Redis write overhead.

## Performance contract

- `get_test_id()` / `set_test_id()`: O(1) context-var read/write, ~50 ns
- `emit_obs(stage, data)`:
  - Zero overhead when `test_id` empty (single context-var read + early
    return). This is the production path.
  - When `test_id` present + `diag_obs_stream` true: O(1) async-queue
    put. Background drain task batches every 100ms.
- Drain task: batched XADD pipeline. Per-event Redis cost amortized
  to <0.05 ms. Bounded queue (10k by default) drops oldest if Redis
  is unreachable rather than blocking the request path.

The /decide handler's <10ms latency budget is preserved unconditionally
— even with all toggles ON, the per-request overhead is <1ms total.

## Test ID lifecycle

```
Worker (CF edge)
    ↓ X-Test-Id header
click-processor middleware (this module)
    ↓ context var
/decide handler logic + cascade resolver + macros + stream XADD
    ↓ each emits checkpoint via emit_obs(stage, data)
local Redis stream `obs:test:<test_id>` (TTL 1h, MAXLEN ~10k)
    ↓ trace CLI XRANGE
geotds-traffic trace --test-id <id>
```

Cross-references:
  - rule `.claude/rules/diagnostic-mode.md` — toggle discipline + prod safety
  - skill `.claude/skills/diagnostic-tracing/SKILL.md` — operator HOW
  - `app/main.py` middleware that calls `set_test_id()`
  - `app/router.py` / `app/cascade.py` / `app/sync_client.py` checkpoint emit sites
"""

from __future__ import annotations

import asyncio
import contextvars
import json
import logging
import re
import time
from typing import Any

import sentry_sdk

from app.config import settings

logger = logging.getLogger("tds.diag")

# X-Test-Id values that pass into Sentry tags + Redis stream keys
# MUST be bounded length and benign character set. The header is
# unauthenticated at the CF edge — a malicious client can craft any
# value. Without this gate (audit closure 2026-05-10, Agent 2 HIGH-1
# / HIGH-2 / HIGH-3):
#  - Redis OOM via attacker-chosen `obs:test:<huge-or-many-keys>`
#  - Sentry tag indexing degradation (200-char limit + control-char
#    rejection)
#  - Log injection via newline-bearing values in `extra={...}`
#
# UUID-shape (RFC 4122) is the canonical generator output and the
# narrowest correct match. We accept dashes + hex chars only, capped
# at 64 (handles uuid4 with optional run-suffix per the traffic
# framework convention). Mirror of admin-api/app/diag.py validator.
_VALID_TEST_ID = re.compile(r"^[0-9a-fA-F-]{8,64}$")


def _is_valid_test_id(test_id: str) -> bool:
    """Whitelist-validate an X-Test-Id value before any propagation.

    Returns True for safely-tagged probes; False for absent / oversized /
    out-of-charset values. Callers MUST treat False as "no test_id" —
    drop the value silently rather than raising (the diag path must
    never fail a request).
    """
    if not test_id:
        return False
    if len(test_id) > 64:
        return False
    return bool(_VALID_TEST_ID.match(test_id))


# Request-scoped test_id. Bound by the middleware (`bind_test_id`)
# at the top of `/decide` and read by every checkpoint emit site.
# Defaults to "" (no test) so emit_obs() can early-return without an
# `if test_id is None`.
_test_id_var: contextvars.ContextVar[str] = contextvars.ContextVar(
    "tds_test_id", default=""
)


def get_test_id() -> str:
    """Return the current request's test_id, or "" when none bound."""
    return _test_id_var.get()


def set_test_id(test_id: str) -> contextvars.Token:
    """Bind test_id to the current request's context.

    Validates with `_is_valid_test_id` — out-of-charset / oversized
    values collapse to "" so emit_obs early-returns + Redis keys can
    never be poisoned. Audit closure 2026-05-10.

    Returns a Token so the caller can `_test_id_var.reset(token)` at
    the end of the request — but in FastAPI middleware the context
    naturally tears down when the request scope ends, so resetting
    is optional.
    """
    if not _is_valid_test_id(test_id or ""):
        return _test_id_var.set("")
    return _test_id_var.set(test_id)


# Bounded async queue for obs-stream events. Items are
# `(test_id, stage, data_dict, ts_iso)` tuples. The background drain
# task batches XADD calls every `diag_obs_drain_interval_ms` ms.
# `maxsize` from settings — full queue drops the oldest event (loud
# warning) so a Redis outage cannot back-pressure the request path.
_obs_queue: asyncio.Queue | None = None


def _get_queue() -> asyncio.Queue:
    """Lazy-init the obs queue. Called from the first `emit_obs`."""
    global _obs_queue
    if _obs_queue is None:
        _obs_queue = asyncio.Queue(maxsize=settings.diag_obs_queue_max)
    return _obs_queue


def emit_obs(stage: str, data: dict[str, Any] | None = None) -> None:
    """Emit a checkpoint event for the current request's test_id.

    Zero overhead when no test_id bound (production fast path) or
    when `diag_obs_stream` toggle is False. When both are true,
    enqueues a single tuple — the background drain task handles
    Redis I/O so the caller never blocks.

    `stage` is a dotted name (e.g., `click.cascade_resolve`). The
    canonical taxonomy is documented in skill `diagnostic-tracing`
    so the trace CLI's gap-detector knows which stages are expected.
    """
    test_id = _test_id_var.get()
    if not test_id:
        return
    if not settings.diag_obs_stream:
        return

    if data is None:
        data = {}

    ts_iso = time.strftime("%Y-%m-%dT%H:%M:%S.", time.gmtime()) + (
        f"{int((time.time() % 1) * 1_000_000):06d}Z"
    )

    queue = _get_queue()
    try:
        queue.put_nowait((test_id, stage, data, ts_iso))
    except asyncio.QueueFull:
        # Drop oldest then push. Loud-warn so the operator knows
        # diagnostic data is being lost (e.g., Redis is down so the
        # drainer can't make progress).
        try:
            queue.get_nowait()
            queue.task_done()
            queue.put_nowait((test_id, stage, data, ts_iso))
            logger.warning(
                "obs queue full (%d) — dropped oldest event",
                settings.diag_obs_queue_max,
            )
        except asyncio.QueueEmpty:
            # Race: another consumer drained while we were trying.
            # Skip this event rather than spinning.
            pass


def emit_log(stage: str, data: dict[str, Any] | None = None, level: str = "info") -> None:
    """Promote a structured log when verbose-logs toggle is on AND a
    test_id is bound. INFO by default; pass `level="debug"` for very
    chatty checkpoints.

    The log line carries `area=diag, stage=..., test_id=...` extras
    so a JSON-aware log shipper can fan them out by tag the same way
    trace CLI does.
    """
    test_id = _test_id_var.get()
    if not test_id:
        return
    if not settings.diag_verbose_logs:
        return
    extra = {
        "area": "diag",
        "stage": stage,
        "test_id": test_id,
    }
    if data:
        extra.update(data)
    msg = f"[diag/{stage}] test_id={test_id[:8]}"
    if level == "debug":
        logger.debug(msg, extra=extra)
    elif level == "warning":
        logger.warning(msg, extra=extra)
    else:
        logger.info(msg, extra=extra)


def emit_checkpoint(stage: str, data: dict[str, Any] | None = None) -> None:
    """Convenience: emit the same checkpoint to BOTH obs stream AND
    structured log. Most call-sites want both — they pay zero overhead
    when toggles are off, but get full coverage when on. Sentry
    breadcrumbs are added separately by Sentry's autoinstrumentation
    when traces_boost is on.
    """
    emit_obs(stage, data)
    emit_log(stage, data)


async def run_obs_drain(redis) -> None:
    """Background task that drains the obs queue into Redis streams.

    Started in FastAPI lifespan, cancelled on shutdown. Robust to
    transient Redis errors — a failed batch is logged + Sentry-captured
    but does not kill the loop. Per-test stream MAXLEN cap protects
    Redis from runaway probe sets. Per-stream TTL keeps the diagnostic
    store from accreting.

    Drain cadence: `diag_obs_drain_interval_ms` (default 100ms). At
    that rate the worst-case operator sees their full timeline ~100ms
    after the last probe response — well within the human-loop trace
    CLI invocation latency.

    Idempotent on shutdown — drains remaining queue items before
    exiting on CancelledError so a late probe is not lost between
    request return and lifespan teardown.
    """
    queue = _get_queue()
    interval_s = settings.diag_obs_drain_interval_ms / 1000.0
    maxlen = settings.diag_obs_stream_maxlen
    ttl = settings.diag_obs_stream_ttl_seconds
    logger.info(
        "Diag obs drain started (interval=%dms, maxlen=%d, ttl=%ds)",
        settings.diag_obs_drain_interval_ms, maxlen, ttl,
    )

    try:
        while True:
            try:
                await asyncio.sleep(interval_s)
                await _drain_batch(redis, queue, maxlen, ttl)
            except asyncio.CancelledError:
                # Final flush before exit.
                logger.info("Diag obs drain cancelled — flushing remaining %d events", queue.qsize())
                try:
                    await _drain_batch(redis, queue, maxlen, ttl)
                except Exception:
                    logger.exception("Final obs drain flush failed")
                raise
            except Exception:
                logger.exception("obs drain iteration failed")
    except asyncio.CancelledError:
        logger.info("Diag obs drain shutting down")
        raise


async def _drain_batch(redis, queue: asyncio.Queue, maxlen: int, ttl: int) -> None:
    """Drain all currently-queued events into a Redis pipeline.

    Per-test grouping: one XADD per event keyed `obs:test:<test_id>`,
    one EXPIRE per unique test_id seen in this batch. Pipelined to a
    single round-trip when redis-py supports it.
    """
    if queue.empty():
        return

    # Snapshot the current queue (not a hard cap — drain whatever was
    # accumulated since last tick).
    batch: list[tuple[str, str, dict, str]] = []
    while not queue.empty():
        try:
            batch.append(queue.get_nowait())
            queue.task_done()
        except asyncio.QueueEmpty:
            break

    if not batch:
        return

    seen_test_ids: set[str] = set()
    try:
        pipe = redis.pipeline()
        for test_id, stage, data, ts_iso in batch:
            key = f"obs:test:{test_id}"
            entry = {
                "ts": ts_iso,
                "service": "click-processor",
                "node_id": settings.node_id,
                "stage": stage,
                "data": json.dumps(data, default=str),
            }
            pipe.xadd(key, entry, maxlen=maxlen, approximate=True)
            seen_test_ids.add(key)
        for key in seen_test_ids:
            pipe.expire(key, ttl)
        await pipe.execute()
    except Exception as exc:
        # Loud warn + Sentry — operator needs to know obs data is
        # dropping. The events themselves are gone (already drained
        # from the queue) — re-queueing would risk infinite retry
        # loops on a structurally broken Redis. Acceptable trade:
        # diag data is best-effort; production routing path is
        # untouched.
        logger.warning(
            "obs drain batch failed (%d events lost): %s",
            len(batch), exc,
        )
        try:
            sentry_sdk.capture_exception(exc)
        except Exception:
            pass


def traces_sampler(sampling_context: dict) -> float:
    """Sentry tracesSampler — boost to 1.0 when X-Test-Id is present
    AND `diag_traces_boost` toggle is on. Falls back to 0.1 baseline
    otherwise.

    Reads the header directly from the WSGI/ASGI envelope so it works
    BEFORE FastAPI middleware has had a chance to bind the context
    var — the sampler runs at request-span creation time which is the
    earliest possible point in the trace.
    """
    if not settings.diag_traces_boost:
        return 0.1
    asgi_scope = sampling_context.get("asgi_scope") or {}
    headers = dict(asgi_scope.get("headers") or [])
    test_id = headers.get(b"x-test-id", b"").decode("ascii", errors="ignore")
    if test_id:
        return 1.0
    return 0.1


def before_send(event: dict, hint: dict) -> dict | None:
    """Sentry beforeSend — redact known-sensitive fields from headers,
    cookies, and query strings before the event leaves the process.

    Defensive backstop in case any code path Sentry-captures a request
    object: the hooks below remove auth secrets even when the
    capturing code didn't think to scrub. Never returns None (we
    always want the event to ship — just sanitized).

    Audit closure 2026-05-10 (Agent 2 MED-1): added x-api-key for
    parity with admin-api twin (same set of secrets crosses both
    services in different code paths). Audit MED-2: expanded query-
    key set to cover common credential-bearing variations
    (api_key/access_token/bearer/etc).
    """
    SENSITIVE_HEADERS = {
        "x-tds-key", "x-tds-body-sig", "authorization",
        "cookie", "x-api-key",
    }
    SENSITIVE_QUERY_KEYS = {
        "debug", "key", "token", "password",
        "api_key", "apikey", "access_token", "refresh_token",
        "bearer", "secret", "passwd", "pwd", "auth", "session",
    }

    request = event.get("request") or {}
    headers = request.get("headers") or {}
    if isinstance(headers, dict):
        for k in list(headers.keys()):
            if k.lower() in SENSITIVE_HEADERS:
                headers[k] = "[redacted]"
    query_string = request.get("query_string") or ""
    if query_string and any(f"{k}=" in query_string for k in SENSITIVE_QUERY_KEYS):
        # Coarse redaction — keep param names visible (so the trace
        # is still meaningful) but blank values.
        parts = []
        for piece in query_string.split("&"):
            if "=" in piece:
                k, _ = piece.split("=", 1)
                if k.lower() in SENSITIVE_QUERY_KEYS:
                    parts.append(f"{k}=[redacted]")
                else:
                    parts.append(piece)
            else:
                parts.append(piece)
        request["query_string"] = "&".join(parts)
    return event
