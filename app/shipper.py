"""Click Shipper — reads clicks from local Redis Stream, sends to central collector.

Runs as a background asyncio task within click-processor.
Guarantees at-least-once delivery:
- Reads from local stream with consumer group
- Batches clicks (by count or time)
- POSTs to central collector
- ACKs only after central confirms receipt
- On failure: retries with backoff, messages stay in stream
"""

import asyncio
import json
import logging
import os
import time

import httpx
import sentry_sdk

# Audit 2026-05-27 — narrow Redis-protocol error type for NOGROUP
# detection in :func:`_drain_batch_from_stream`. Imported via the
# unconditional ``redis`` path (not optional like enrichment.py)
# because the shipper module unconditionally needs the type at
# runtime; the redis dependency is in services/click-processor's
# requirements.txt and is always present.
from redis.exceptions import ResponseError as RedisResponseError

from app.config import _LOCAL_ENVIRONMENTS, settings
from app.shipper_metrics import metrics as shipper_metrics

logger = logging.getLogger("tds.shipper")

STREAM_KEY = "stream:clicks"
GROUP_NAME = "shippers"
CONSUMER_NAME = f"shipper-{settings.node_id}-{os.getpid()}"
BATCH_SIZE = 500
BATCH_TIMEOUT_MS = 2000  # 2 seconds
MAX_RETRY_DELAY = 30

# F.29 Sprint 2.2 (2026-05-23) — local Redis key prefix for per-click
# retry-attempt counters. Each rejected click_id increments
# ``click:retry:{click_id}`` (TTL = settings.shipper_retry_ttl_seconds).
# At settings.shipper_max_retry_attempts the click is deadlettered.
_RETRY_KEY_PREFIX = "click:retry:"

# Local edge deadletter stream. Sprint 2.2 writes here as the
# operator-visible holding pen for clicks that exceeded retry
# attempts. Sprint 2.3 will additionally POST to central
# /api/clicks/deadletter so the central operator dashboard sees it
# too — until then, edge operators can XRANGE this stream + replay
# manually via XADD-to-stream:clicks if needed.
DEADLETTER_STREAM_KEY = "stream:clicks-deadletter"
DEADLETTER_STREAM_MAXLEN = 10_000

# F.29 Sprint 2.7a (2026-05-23) — one-shot flag for Sprint 2.5
# backwards-compat shim warning. Pre-2.7 the shim fired a Sentry
# capture_message on EVERY batch with a legacy-shape response,
# generating ~30 events/min during a sustained rolling-deploy
# window → Sentry quota concern. Now the Sentry event fires only on
# the FIRST shim activation per shipper task lifetime; subsequent
# batches still log at WARN level (operator visibility preserved)
# but skip the Sentry breadcrumb to bound quota.
_shim_warned_this_session: bool = False


# ---------------------------------------------------------------------------
# F.29 Sprint 1.3 — structured Sentry tagging (Sprint 1.6 — extracted)
# ---------------------------------------------------------------------------
# Pre-F.29 every shipper exception captured under the generic
# "Shipper error" umbrella, blending JSON-decode / httpx / XREADGROUP /
# XACK failures into one Sentry issue group. Sprint 1.3 attached an
# ``op`` tag to every exception path so issue grouping splits by
# operation and Sprint 4.1 alert rules can key off specific tags.
#
# Sprint 1.6 (2026-05-23 validation cycle) extracted the helpers +
# constants to ``app.telemetry`` after Agent 1 caught a DRY violation:
# main.py:688-725 (disk-pressure 503 block) reinvented the same
# ``push_scope + set_tag("op", ...) + capture_*`` incantation. The
# shared module ensures Sprint 4.1 alert rules bind to consistent tag
# values across BOTH the shipper loop and the /decide hot-path failures.
#
# We re-export the names (with ``_capture_op_*`` underscore-prefix
# aliases) to keep this module's public surface backwards-compatible
# for the existing test_shipper_exception_tagging.py source-pin tests.

from app.telemetry import (
    OP_BATCH_POST,
    OP_DEADLETTER,
    OP_LEGACY_COLLECTOR,
    OP_LOOP_ITERATION,
    OP_PARSE_PAYLOAD,
    OP_XACK,
    OP_XACK_BATCH,
    OP_XREADGROUP,
    capture_op_exc as _capture_op_exc,
    capture_op_msg as _capture_op_msg,
)


class ShipperDisabledError(RuntimeError):
    """Raised when the shipper refuses to start because ``central_url``
    is empty in a non-local environment AND ``require_central_url``
    is True.

    F.29 Sprint 1.2 (2026-05-23). Bubbles out of :func:`assert_shipper_ready`
    into the FastAPI lifespan startup. Lifespan does NOT catch this — the
    service refuses to come up, exactly the desired behaviour. The
    operator sees a stack trace on ``docker compose up`` and a FATAL
    Sentry event, both pointing to the same misconfig.

    Defense-in-depth versus ``Settings._enforce_central_url_presence``:
    the validator catches the same case at config-construction time. The
    runtime assert ALSO fires if a future code path mutates
    ``settings.central_url`` post-boot OR a test bypasses the validator
    via ``Settings.model_construct(...)``.
    """


def assert_shipper_ready() -> None:
    """Synchronous boot-time validation for the click shipper.

    Called from the FastAPI lifespan BEFORE creating the shipper task,
    AND from :func:`run_shipper` as defense in depth. Centralising the
    policy here ensures both call sites apply identical rules and the
    operator-facing log lines are emitted exactly once per boot
    (lifespan runs first; the run_shipper call is a no-op repeat
    against an unchanged ``settings`` snapshot).

    Policy matrix:

    * ``central_url`` non-empty → return silently (shipper proceeds).
    * ``central_url`` empty + ``environment ∈ _LOCAL_ENVIRONMENTS``
      → INFO log + return. Preserves ``make dev`` + isolated
      standalone test rigs (no central collector intended).
    * ``central_url`` empty + non-local + ``require_central_url=False``
      → WARN log + Sentry breadcrumb + return. Operator escape
      hatch: legacy silent-disable is preserved BUT now visible.
    * ``central_url`` empty + non-local + ``require_central_url=True``
      (default per F.29 plan §7.1) → FATAL log + Sentry capture +
      :class:`ShipperDisabledError`. Service refuses to start —
      exactly the behaviour the 50-day audit-2026-05-16 incident
      demanded.

    Raises:
        ShipperDisabledError: when the fatal branch fires. Re-raised
        by lifespan to fail uvicorn startup.
    """
    if settings.central_url:
        return  # Happy path — shipper will run normally.

    if settings.environment in _LOCAL_ENVIRONMENTS:
        logger.info(
            "No CENTRAL_URL configured in %s env — shipper disabled "
            "(standalone mode, F.29 Sprint 1.2 local-env carve-out).",
            settings.environment,
        )
        return

    if not settings.require_central_url:
        logger.warning(
            "F.29 Sprint 1.2 — Shipper disabled in env=%s because "
            "TDS_CENTRAL_URL is empty AND TDS_REQUIRE_CENTRAL_URL=false "
            "(operator escape hatch). Clicks will accumulate on local "
            "stream:clicks with NO delivery upstream — flip the flag "
            "back to true and configure TDS_CENTRAL_URL ASAP.",
            settings.environment,
        )
        sentry_sdk.capture_message(
            "Shipper disabled by operator escape hatch "
            "(TDS_REQUIRE_CENTRAL_URL=false) — clicks accumulating "
            "locally with no upstream delivery.",
            level="warning",
        )
        return

    # Non-local env + require_central_url=True + empty url. The
    # Settings validator should have refused construction; reaching
    # this branch means an env mutation post-boot OR a test that
    # bypassed validation via ``Settings.model_construct(...)``. Either
    # way the production-safe action is identical: refuse to start and
    # surface the misconfig loud.
    msg = (
        f"F.29 Sprint 1.2 FATAL — Shipper cannot start: "
        f"TDS_CENTRAL_URL is empty in env={settings.environment!r} "
        f"AND TDS_REQUIRE_CENTRAL_URL=True. The "
        f"Settings._enforce_central_url_presence validator should "
        f"have caught this at boot; reaching this branch means a "
        f"post-construction env mutation or a test misconfig. "
        f"Refusing to start to prevent the 50-day click-persistence "
        f"blackout (audit 2026-05-16). Fix: set TDS_CENTRAL_URL or "
        f"flip TDS_REQUIRE_CENTRAL_URL=false."
    )
    logger.critical(msg)
    sentry_sdk.capture_message(msg, level="fatal")
    raise ShipperDisabledError(msg)


# ---------------------------------------------------------------------------
# F.29 Sprint 2.2 (2026-05-23) — per-click verdict handling helpers
# ---------------------------------------------------------------------------
# Sprint 2.1 introduced the collector response shape
# ``{accepted: [...], rejected: [{click_id, reason}], duplicates: [...]}``.
# These helpers consume that shape on the shipper side:
#
#   * :func:`_parse_collector_response` — robust JSON parse with the
#     Sprint 2.5 backwards-compat shim. Returns a tuple
#     ``(shape, parsed)`` where ``shape`` is one of ``"new"`` /
#     ``"legacy"`` / ``"unknown"`` so the caller can branch.
#   * :func:`_handle_rejected_click` — increments the per-click retry
#     counter; on max-attempts hit, XADDs the click to the local
#     deadletter stream + reports the OP_DEADLETTER op tag.
#   * :func:`_retry_click` — re-XADDs a rejected click back to
#     ``stream:clicks`` for the next iteration. The retry counter
#     survives via Redis TTL.
#
# Sprint 2.5 shim semantics: a pre-F.29 collector returns
# ``{"received": N, "queued": N, "stream_id": "X"}`` with NO
# ``accepted`` key. The shim detects this and falls back to the
# legacy ACK-all behavior (status 200/202 = success). One WARN-level
# Sentry capture per shim-trigger keeps the operator aware of the
# rolling-deploy gap without spamming the issue feed.

from typing import Any


def _parse_collector_response(
    response_text: str,
) -> tuple[str, dict[str, Any] | None]:
    """Parse the collector's batch response with shim detection.

    Returns:
        Tuple of ``(shape, body)``:
        * ``("new", body)`` — Sprint 2.1+ shape (has ``accepted`` key).
        * ``("legacy", body)`` — pre-F.29 shape (has ``received`` /
          ``queued`` keys but no ``accepted``). Used during rolling
          deploy when shipper has been updated but collector hasn't.
        * ``("unknown", None)`` — body unparseable as JSON OR has
          no recognised keys.

    Caller routing (run_shipper dispatcher, since Sprint 3.7.1 TD-17 —
    docstring corrected here per D9, audit 2026-06-03):
      * ``new`` on any 2xx → per-click verdict handling.
      * ``legacy`` on 200/202 → ACK-all backwards-compat shim.
      * everything else (``unknown`` on any 2xx, ``legacy``/``unknown``
        on 207) → ``_process_collector_error`` → RETRY, never ACK-all.
        The pre-3.7.1 "fall back to status-code-only ACK-all on 2xx for
        unknown" path was REMOVED — a mangled body must not silently
        drop the batch.
    """
    if not response_text:
        return ("unknown", None)
    try:
        body = json.loads(response_text)
    except (json.JSONDecodeError, ValueError):
        return ("unknown", None)
    if not isinstance(body, dict):
        return ("unknown", None)
    if "accepted" in body:
        return ("new", body)
    if "received" in body or "queued" in body:
        return ("legacy", body)
    return ("unknown", body)


async def _retry_click(
    redis_pool,
    click: dict[str, Any],
) -> None:
    """Re-XADD a rejected click back to ``stream:clicks`` for the
    next iteration.

    The retry counter (``click:retry:{click_id}`` in Redis) survives
    across re-XADDs via TTL — it tracks attempts even though the
    underlying stream entry gets a new msg_id each time. The old
    msg_id of the rejected attempt MUST be ACKed by the caller AFTER
    this re-XADD succeeds (otherwise PEL grows unbounded).

    M1 (LOSSFIX P1b, 2026-07-07) — no ``maxlen`` here, deliberately, and
    with NO new gate either: this is a size-neutral swap (the old
    stream entry is ACKed right after this re-XADD succeeds), and
    gating a retry would starve transient failures straight into
    deadletters instead of giving them another shipper cycle.
    """
    payload = json.dumps(click, default=str)
    await redis_pool.xadd(
        STREAM_KEY,
        {"data": payload},
    )


async def _forward_deadletter_to_central(
    http_client: httpx.AsyncClient,
    record: dict[str, str],
) -> bool:
    """F.29 Sprint 2.3 — forward a deadletter record to the central
    collector so the operator dashboard sees deadletters across the
    whole edge fleet.

    Best-effort: the click is ALREADY preserved in the edge local
    ``stream:clicks-deadletter`` ring buffer (the primary durability
    guarantee). Central forwarding failure is logged but never
    propagated — the caller's flow continues. Sprint 4.1 alert rule
    "central deadletter depth > 100 → warn" surfaces persistent
    central failures separately.

    Returns:
        True on successful central XADD (collector returned 202).
        False on failure (logged + Sentry-captured by the caller via
        the existing op=deadletter helper).
    """
    if not settings.central_url:
        # Standalone-mode shipper has no central URL configured. The
        # local deadletter is sufficient.
        return False
    payload = {
        "click_id": record.get("click_id", ""),
        "data": record.get("data", "{}"),
        "attempt_count": int(record.get("attempt_count", "0")),
        "last_rejection_reason": record.get("last_rejection_reason", ""),
        "deadlettered_at": float(record.get("deadlettered_at", "0")),
        "node_id": record.get("node_id", settings.node_id),
    }
    try:
        response = await http_client.post(
            f"{settings.central_url}/api/clicks/deadletter",
            json=payload,
            headers={"X-Node-Key": settings.central_api_key},
            timeout=5.0,
        )
        if response.status_code == 202:
            return True
        logger.warning(
            "Shipper central deadletter forward returned %s for "
            "click_id=%s: %s",
            response.status_code,
            payload["click_id"],
            response.text[:200],
        )
        return False
    except Exception as exc:  # noqa: BLE001
        # Central forwarding is best-effort. Log + Sentry capture
        # with op=deadletter so operators see the forward-failure
        # rate distinct from the deadletter rate itself.
        logger.warning(
            "Shipper central deadletter forward failed for click_id=%s: "
            "%s. Click is preserved at edge local deadletter stream.",
            payload["click_id"], exc,
        )
        _capture_op_exc(
            "deadletter",
            exc,
            click_id=payload["click_id"],
            stage="central_forward",
        )
        return False


async def _deadletter_click(
    redis_pool,
    click: dict[str, Any],
    attempt: int,
    reason: str,
    http_client: httpx.AsyncClient | None = None,
) -> None:
    """Move a click to the local deadletter stream after max retries.

    F.29 Sprint 2.2 (2026-05-23) introduced the local edge deadletter
    stream. Sprint 2.3 (2026-05-23) added the optional ``http_client``
    parameter so the caller (run_shipper) can additionally forward
    the record to the central collector for fleet-wide visibility.

    The local XADD is the durability primitive — central forwarding
    is best-effort observability. If both fail the click is
    genuinely lost; operators see this via Sentry op=deadletter
    captures.

    The deadletter record carries:
      * ``data`` — original click JSON.
      * ``attempt_count`` — final attempt number that triggered the
        cap (operator can see "this click failed 5 times").
      * ``last_rejection_reason`` — collector's reason string for
        the last attempt (e.g. ``"queue_failure"``,
        ``"validation_failed"``).
      * ``deadlettered_at`` — UNIX timestamp.
      * ``node_id`` — which edge node deadlettered the click. Used
        for per-tenant bucketing at the central dashboard.
    """
    import time as _time
    click_id = click.get("click_id", "<unknown>")
    record = {
        "click_id": str(click_id),
        "data": json.dumps(click, default=str),
        "attempt_count": str(attempt),
        "last_rejection_reason": reason[:64],  # bound reason length
        "deadlettered_at": str(_time.time()),
        "node_id": settings.node_id,
    }

    # Local edge deadletter stream — primary durability path.
    try:
        await redis_pool.xadd(
            DEADLETTER_STREAM_KEY,
            record,
            maxlen=DEADLETTER_STREAM_MAXLEN,
            approximate=True,
        )
    except Exception as exc:  # noqa: BLE001
        # Local deadletter XADD failure is rare (separate stream, low
        # rate) but not catastrophic. Log + Sentry capture; central
        # forwarding (below) is the redundancy.
        logger.error(
            "Shipper local deadletter XADD failed for click_id=%s "
            "(attempt=%d, reason=%s): %s",
            click_id, attempt, reason, exc,
        )
        _capture_op_exc(
            "deadletter",
            exc,
            click_id=click_id,
            attempt=attempt,
            reason=reason,
            stage="local_xadd",
        )

    # F.29 Sprint 2.3 — best-effort central forward. Caller passes
    # the active httpx.AsyncClient when available (inside the main
    # loop). When None (e.g. early helper unit tests), we skip the
    # forward step — local deadletter is sufficient on its own.
    if http_client is not None:
        await _forward_deadletter_to_central(http_client, record)


async def _handle_rejected_click(
    redis_pool,
    click: dict[str, Any],
    reason: str,
    http_client: httpx.AsyncClient | None = None,
) -> bool:
    """Increment retry counter; return True if click should be retried,
    False if it was deadlettered (max attempts hit).

    The counter (``click:retry:{click_id}``) is per-click_id; the
    Sprint 2.1 central dedup gate (``click:central_seen:{click_id}``)
    is at the COLLECTOR side. Retries land at the collector and either:
      * Get accepted (transient collector issue resolved).
      * Get bucketed as ``duplicates`` if a sibling node won the race
        in the meantime (idempotent convergence — shipper ACKs).
      * Get rejected again (counter increments).
    """
    click_id = click.get("click_id")
    if not click_id:
        # Pathological — Sprint 2.1 collector returns
        # missing_click_id reason. Cannot retry without an id; just
        # deadletter immediately.
        await _deadletter_click(
            redis_pool, click, attempt=1, reason=reason,
            http_client=http_client,
        )
        return False

    retry_key = f"{_RETRY_KEY_PREFIX}{click_id}"
    try:
        # F.29 Sprint 2.7a (2026-05-23) — atomic INCR + EXPIRE via
        # pipeline. Pre-2.7 these were two sequential awaits; a
        # shipper crash or Redis disconnect BETWEEN them left the
        # counter without a TTL → permanent key in Redis under
        # ``noeviction`` policy. Pipelining drops the window to a
        # single round-trip (server-side execution sequence is
        # still two ops but client cannot observe a partial state).
        pipe = redis_pool.pipeline()
        pipe.incr(retry_key)
        pipe.expire(retry_key, settings.shipper_retry_ttl_seconds)
        incr_result, _ = await pipe.execute()
        attempt = incr_result
    except Exception as exc:  # noqa: BLE001
        # Counter increment failed — Redis impairment. Default to
        # deadletter conservatively rather than infinite-retry.
        # F.29 Sprint 2.7a — use attempt=0 sentinel (was -1 pre-2.7
        # which violated DeadletterRecord.ge=0 Pydantic constraint
        # on central forward, causing a silent 422 reject).
        logger.warning(
            "Shipper retry counter increment failed for click_id=%s: "
            "%s. Deadlettering conservatively.",
            click_id, exc,
        )
        await _deadletter_click(
            redis_pool, click, attempt=0, reason=f"counter_error:{reason}",
            http_client=http_client,
        )
        return False

    if attempt >= settings.shipper_max_retry_attempts:
        await _deadletter_click(
            redis_pool, click, attempt=attempt, reason=reason,
            http_client=http_client,
        )
        # Clean up the retry counter — click is out of the loop now.
        try:
            await redis_pool.delete(retry_key)
        except Exception:  # noqa: BLE001
            pass  # TTL will eventually clean it up
        return False

    # Re-XADD for next iteration.
    try:
        await _retry_click(redis_pool, click)
    except Exception as exc:  # noqa: BLE001
        # Re-XADD failed (likely Redis impairment). Deadletter so the
        # click isn't lost silently.
        logger.warning(
            "Shipper re-XADD failed for click_id=%s on attempt %d: "
            "%s. Deadlettering.",
            click_id, attempt, exc,
        )
        await _deadletter_click(
            redis_pool, click, attempt=attempt, reason=f"requeue_error:{reason}",
            http_client=http_client,
        )
        return False

    return True


# ---------------------------------------------------------------------------
# F.29 TD-1 (2026-05-23) — run_shipper decomposition helpers
# ---------------------------------------------------------------------------
# Sprint 2.7 validation cycle (Agent 1 CRITICAL #1) flagged the pre-TD-1
# ``run_shipper`` as ~497 LOC / 5-6 nesting depth — violates rule
# code-organization 60-LOC + 3-depth caps and is hard to reason about.
#
# Decomposition follows plan-doc §14 TD-1 prescription:
#   * ``_drain_batch_from_stream`` — XREADGROUP + per-message parse loop
#     with op_tag XACK on parse failure.
#   * ``_post_batch_to_central`` — single POST to the collector.
#   * ``_process_new_shape_batch`` — Sprint 2.1+ per-click verdict
#     handling: ACK accepted ∪ duplicates, retry/deadletter rejected,
#     record success-ratio + ship status.
#   * ``_process_legacy_shape_batch`` — Sprint 2.5 backwards-compat shim
#     for pre-F.29 collectors (ACK-all + one-shot Sentry warn).
#   * ``_process_collector_error`` — non-2xx response OR 207 contract
#     violation. Owns the exponential-backoff sleep + returns the new
#     ``retry_delay`` so the caller's state machine stays single-sourced.
#   * ``_handle_central_unreachable`` — httpx.RequestError handler with
#     the same backoff contract.
#   * ``_handle_shipper_loop_error`` — catch-all Exception with fixed
#     2-second sleep (no backoff).
#
# Sub-helpers narrow the new-shape orchestration further to keep each
# function under ~70 LOC + nesting ≤3:
#   * ``_compute_ack_msg_ids_from_verdict`` — accepted ∪ duplicates set.
#   * ``_handle_rejected_in_batch`` — for-loop over rejected_items.
#   * ``_ack_shipped_batch`` — XACK + ack_failed bookkeeping; returns
#     False on Redis failure so the caller short-circuits the outcome
#     metrics. (Processed-history XTRIM moved to the loop-clock
#     ``_trim_processed_history`` — AUD-B F1.)
#   * ``_record_new_shape_outcome`` — record_outcome + record_ship +
#     summary log for the deadletter / partial_ack / success branches.
#
# Net result: ``run_shipper`` drops from ~497 LOC to ~50 LOC, becomes
# a thin dispatcher whose intent is readable in one screen. Every
# observable behaviour (op tags, sleep durations, retry_delay updates,
# record_ship / record_outcome timings) preserved byte-for-byte vs the
# pre-TD-1 implementation — verified by the existing 655 click-processor
# tests + Sprint 2.6 chaos integration test (test_shipper_chaos_partial_ack).
#
# Naming convention: every helper that mutates Redis state OR makes an
# HTTP call is ``async``; pure logging / metrics helpers are sync. This
# keeps the await graph honest — readers can grep ``await _process_``
# to find the I/O boundary points.


async def _ensure_local_consumer_group(redis_pool) -> None:
    """Idempotently create the local-stream consumer group.

    Audit 2026-05-27 — extracted from the inline block in
    :func:`run_shipper` (lines 1142-1149 pre-refactor) so it can be
    reused by the NOGROUP recovery branch inside
    :func:`_drain_batch_from_stream`. ``mkstream=True`` handles the
    case where the stream itself disappeared; ``BUSYGROUP`` is
    absorbed silently for the racing-creator case.
    """
    try:
        await redis_pool.xgroup_create(
            STREAM_KEY, GROUP_NAME, id="0", mkstream=True,
        )
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            raise


def _is_nogroup_error(exc: BaseException) -> bool:
    """True iff ``exc`` is a Redis ResponseError whose message indicates
    the consumer group (or stream) is missing.

    Audit 2026-05-27 — mirrors the writer's classification helper in
    ``services/collector/app/writer.py``. Narrowed to
    ``RedisResponseError`` to avoid false-positive recovery on
    transient connection/timeout errors that have their own backoff
    semantics in ``_handle_shipper_loop_error``.
    """
    return isinstance(exc, RedisResponseError) and "NOGROUP" in str(exc)


async def _drain_batch_from_stream(
    redis_pool,
) -> tuple[list[dict[str, Any]], list[str]]:
    """XREADGROUP one batch of clicks from the local stream, parse JSON,
    handle per-message parse failures.

    Returns:
        Tuple ``(clicks, msg_ids)``:
          * ``clicks`` — list of parsed click dicts. Empty when the
            stream returned no results OR every message in the batch
            failed to parse (parse-failed messages get ACKed in place).
          * ``msg_ids`` — list of msg_ids for the SUCCESSFULLY parsed
            clicks (1:1 with ``clicks`` by index). Parse-failed msg_ids
            are NOT included because they've already been XACKed and
            the caller must not re-ACK them.

    Implementation notes:
      * The pre-TD-1 inline form (lines 551-612) accumulated ALL msg_ids
        first (parsed + failed) and ACKed failures inline. After
        decomposition we ACK parse failures here AND exclude their
        msg_ids from the returned list — caller's downstream XACK loop
        no longer needs to track them, which removes a class of
        double-ACK footguns.
      * The XREADGROUP timeout (``BATCH_TIMEOUT_MS``) is the natural
        loop heartbeat; empty results → return ``([], [])`` so the
        caller's ``if not clicks: continue`` path keeps working.
      * **NOGROUP auto-heal** (audit 2026-05-27): if the local
        consumer group disappears mid-flight (operator XGROUP DESTROY
        during incident triage, Redis restart with corrupt AOF,
        FLUSHDB), ``XREADGROUP`` raises ``ResponseError: NOGROUP …``.
        Pre-fix that bubbled to :func:`_handle_shipper_loop_error`
        which slept 2s and retried — same infinite NOGROUP loop the
        writer suffered. Now we catch NOGROUP HERE, re-create the
        group via :func:`_ensure_local_consumer_group` (idempotent),
        retry the XREADGROUP exactly ONCE, and on any other failure
        re-raise so the outer ``_handle_shipper_loop_error`` records
        it under the canonical OP_LOOP_ITERATION op tag. Non-NOGROUP
        ResponseErrors propagate unchanged.
    """
    try:
        results = await redis_pool.xreadgroup(
            GROUP_NAME, CONSUMER_NAME,
            {STREAM_KEY: ">"},
            count=BATCH_SIZE,
            block=BATCH_TIMEOUT_MS,
        )
    except RedisResponseError as exc:
        if not _is_nogroup_error(exc):
            raise
        # Audit 2026-05-27 — NOGROUP auto-heal.
        logger.error(
            "Local consumer group missing (op=%s) — recreating '%s' "
            "on '%s': %s",
            OP_XREADGROUP, GROUP_NAME, STREAM_KEY, exc,
        )
        _capture_op_msg(
            OP_XREADGROUP,
            f"Shipper NOGROUP recovery — recreating group '{GROUP_NAME}' "
            f"on '{STREAM_KEY}'",
            level="error",
        )
        await _ensure_local_consumer_group(redis_pool)
        # Retry the XREADGROUP exactly once after recovery. A second
        # NOGROUP would indicate a more fundamental issue (e.g.
        # permissions on xgroup_create silently failed, cluster slot
        # migration mid-recovery); propagate to the outer catch-all
        # for the 2s back-off rather than tight-spinning here.
        results = await redis_pool.xreadgroup(
            GROUP_NAME, CONSUMER_NAME,
            {STREAM_KEY: ">"},
            count=BATCH_SIZE,
            block=BATCH_TIMEOUT_MS,
        )

    if not results:
        return [], []

    _stream_name, messages = results[0]
    # C3 (audit 2026-06-03) — parse loop extracted to a helper so the
    # orphaned-PEL reclaim path (`_reclaim_shipper_pending`) parses
    # XAUTOCLAIM'd messages with the identical poison-handling semantics.
    return await _parse_messages_into_clicks(redis_pool, messages)


async def _parse_messages_into_clicks(
    redis_pool, messages,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Parse a list of ``(msg_id, data)`` stream entries into clicks.

    Shared by :func:`_drain_batch_from_stream` (XREADGROUP `>`) and
    :func:`_reclaim_shipper_pending` (XAUTOCLAIM orphaned PEL). Returns
    ``(clicks, msg_ids)`` 1:1 by index for SUCCESSFULLY-parsed entries.
    A poison (undecodable-JSON) entry is XACKed in place — it can never
    succeed, so leaving it pending would wedge the reclaim cursor — and
    excluded from the returned lists (caller must not re-ACK it).
    """
    clicks: list[dict[str, Any]] = []
    msg_ids: list[str] = []

    for msg_id, data in messages:
        try:
            click = json.loads(data.get("data", "{}"))
        except (json.JSONDecodeError, TypeError) as parse_exc:
            # F.29 Sprint 1.3 — visible parse failure with op_tag so the
            # parse-failure rate is per-node alertable in Sprint 4.1.
            logger.warning(
                "Shipper parse failure (op=%s) for msg=%s: %s. "
                "ACKing to unblock stream — corrupt click payload, "
                "data is lost.",
                OP_PARSE_PAYLOAD, msg_id, parse_exc,
            )
            _capture_op_msg(
                OP_PARSE_PAYLOAD,
                f"Shipper parse failure for msg_id={msg_id}: {parse_exc}",
                level="warning",
                msg_id=str(msg_id),
            )
            # Best-effort XACK so the parse-failed message exits the
            # pending-entry list. Wrapped because Redis impairment is
            # exactly the condition we may be recovering from; an XACK
            # raise here would re-propagate to the outer catch-all and
            # lose the parse-payload tag context.
            try:
                await redis_pool.xack(STREAM_KEY, GROUP_NAME, msg_id)
            except Exception as xack_exc:  # noqa: BLE001
                logger.warning(
                    "Shipper xack failure (op=%s) for msg=%s: %s",
                    OP_XACK, msg_id, xack_exc,
                )
                _capture_op_exc(
                    OP_XACK,
                    xack_exc,
                    msg_id=str(msg_id),
                    context="post-parse-failure-ack",
                )
            continue  # skip — do NOT add to clicks/msg_ids

        clicks.append(click)
        msg_ids.append(msg_id)

    return clicks, msg_ids


async def _reclaim_shipper_pending(redis_pool, http_client) -> dict[str, int]:
    """C3 (audit 2026-06-03) — reclaim orphaned PEL entries and re-ship them.

    The mirror of the central writer's ``_reclaim_pending``
    (``services/collector/app/writer.py``) for the edge shipper. The
    shipper ``CONSUMER_NAME`` embeds ``os.getpid()``; when the process
    crashes/restarts between ``XREADGROUP`` and ship+``XACK``, those read
    messages stay in the DEAD consumer's PEL. The main loop reads only
    ``>`` (new entries), so the orphaned PEL is never re-driven →
    **silent click loss** (audit-2026-06-02 C3 / EDGE-XCLAIM).

    This periodic reclaim ``XAUTOCLAIM``s entries idle past
    ``shipper_reclaim_min_idle_ms`` (so it never races the live consumer)
    and re-drives each batch through the SAME post→verdict→ack path as
    the main loop, so accepted clicks get ACKed (leave the PEL) and
    rejected ones re-queue/deadletter exactly as in steady state.

    Durability contract:
      * On a ship FAILURE (non-2xx, contract-violation 207, or central
        unreachable) the claimed entries are LEFT un-ACKed — they are now
        in THIS live consumer's PEL and get re-driven on a later reclaim
        tick once idle again. Nothing is lost; nothing is dead-lettered
        for a transient outage.
      * Only structurally-poison (undecodable-JSON) entries are ACKed in
        place (a retry cannot help) — via the shared parse helper.

    Never raises — a reclaim fault must not break the main ship loop;
    NOGROUP is self-healed in place (mirrors the drain path).

    Returns a small counts dict (``claimed`` / ``shipped_batches``) for
    observability + tests.
    """
    counts = {"claimed": 0, "shipped_batches": 0}
    try:
        await _ensure_local_consumer_group(redis_pool)

        cursor = "0-0"
        while counts["claimed"] < settings.shipper_reclaim_max_per_cycle:
            # redis-py XAUTOCLAIM → [next_cursor, messages, deleted_ids]
            # (older servers omit deleted_ids — unpack defensively).
            result = await redis_pool.xautoclaim(
                STREAM_KEY,
                GROUP_NAME,
                CONSUMER_NAME,
                min_idle_time=settings.shipper_reclaim_min_idle_ms,
                start_id=cursor,
                count=BATCH_SIZE,
            )
            cursor = result[0]
            messages = result[1] if len(result) > 1 else []
            if not messages:
                break

            clicks, msg_ids = await _parse_messages_into_clicks(
                redis_pool, messages,
            )
            if clicks:
                counts["claimed"] += len(clicks)
                shipped = await _reship_reclaimed_batch(
                    redis_pool, http_client, clicks, msg_ids,
                )
                if not shipped:
                    # Ship failed — claimed entries stay un-ACKed in OUR
                    # PEL and are retried on a later reclaim tick. Stop the
                    # cycle so we don't hammer a down central.
                    logger.warning(
                        "Shipper reclaim: re-ship failed for %d clicks — "
                        "left in PEL for next reclaim cycle (op=%s).",
                        len(clicks), OP_BATCH_POST,
                    )
                    break
                counts["shipped_batches"] += 1

            # XAUTOCLAIM returns "0-0" when the PEL scan is complete.
            if cursor in ("0-0", b"0-0"):
                break

        if counts["claimed"]:
            logger.info(
                "Shipper reclaim cycle: claimed=%d shipped_batches=%d (op=%s)",
                counts["claimed"], counts["shipped_batches"], OP_BATCH_POST,
            )
        return counts
    except RedisResponseError as exc:
        if _is_nogroup_error(exc):
            await _ensure_local_consumer_group(redis_pool)
        else:
            logger.warning("Shipper reclaim ResponseError: %s", exc)
            _capture_op_exc(
                OP_LOOP_ITERATION, exc,
                tags={"failure_kind": type(exc).__name__},
                context="reclaim",
            )
        return counts
    except Exception as exc:  # noqa: BLE001 — never break the main loop
        logger.warning("Shipper reclaim cycle failed: %s", exc)
        _capture_op_exc(
            OP_LOOP_ITERATION, exc,
            tags={"failure_kind": type(exc).__name__},
            context="reclaim",
        )
        return counts


async def _reship_reclaimed_batch(
    redis_pool, http_client, clicks, msg_ids,
) -> bool:
    """Re-drive one reclaimed batch through the post→verdict→ack path.

    Returns True when the batch reached central (per-click verdict or
    legacy ACK-all path ran → accepted clicks ACKed). Returns False on a
    delivery failure (non-2xx, contract-violation 207, or unreachable) so
    the caller leaves the entries pending for the next reclaim tick.

    NB: this intentionally does NOT call ``_process_collector_error`` —
    that helper sleeps + advances the main-loop backoff, which would be
    wrong for the periodic reclaim. A reclaim failure is silent here and
    simply retried next tick.
    """
    try:
        response = await _post_batch_to_central(http_client, clicks)
    except httpx.RequestError as exc:
        logger.warning("Shipper reclaim: central unreachable: %s", exc)
        return False

    if response.status_code not in (200, 202, 207):
        return False

    shape, body = _parse_collector_response(response.text)
    if shape == "new":
        await _process_new_shape_batch(
            redis_pool, http_client, response, body, clicks, msg_ids,
        )
        return True
    if shape == "legacy" and response.status_code in (200, 202):
        await _process_legacy_shape_batch(
            redis_pool, response, shape, clicks, msg_ids,
        )
        return True
    # unknown shape on any 2xx, or legacy/unknown on 207 → contract
    # violation. Do NOT ACK-all (the D9/TD-17 invariant) — leave pending.
    logger.error(
        "Shipper reclaim: collector returned status=%d shape=%s — "
        "leaving %d clicks in PEL for retry (op=%s).",
        response.status_code, shape, len(clicks), OP_BATCH_POST,
    )
    return False


async def _post_batch_to_central(
    http_client: httpx.AsyncClient,
    clicks: list[dict[str, Any]],
) -> httpx.Response:
    """Single POST to the collector's ``/api/clicks/batch`` endpoint.

    Extracted purely for symmetry with :func:`_drain_batch_from_stream`
    and to keep ``run_shipper``'s dispatcher body free of HTTP wire
    details. The wire format (``{node_id, clicks}`` payload + X-Node-Key
    header) is pinned by the collector's :class:`BatchRequest` model
    (services/collector/app/models.py); changing either side requires
    a coordinated update.
    """
    return await http_client.post(
        f"{settings.central_url}/api/clicks/batch",
        json={"node_id": settings.node_id, "clicks": clicks},
        headers={"X-Node-Key": settings.central_api_key},
    )


def _compute_ack_msg_ids_from_verdict(
    accepted_ids: list[str],
    duplicate_ids: list[str],
    click_id_to_msg_id: dict[str, str],
) -> set[str]:
    """Build the set of msg_ids to XACK from a Sprint 2.1+ verdict.

    Accepted ∪ duplicates both represent "click is at central" (just
    via different paths — first-write vs idempotent dedup). The set
    eliminates double-ACK on the same msg_id if a malformed response
    listed the same click_id in both buckets.

    Defensive against unknown click_ids: a click_id present in the
    response but absent from the request batch is simply skipped (the
    caller logs the discrepancy from elsewhere).
    """
    ack_msg_ids: set[str] = set()
    for cid in accepted_ids:
        if cid in click_id_to_msg_id:
            ack_msg_ids.add(click_id_to_msg_id[cid])
    for cid in duplicate_ids:
        if cid in click_id_to_msg_id:
            ack_msg_ids.add(click_id_to_msg_id[cid])
    return ack_msg_ids


async def _handle_rejected_in_batch(
    redis_pool,
    http_client: httpx.AsyncClient,
    rejected_items: list[dict[str, Any]],
    clicks: list[dict[str, Any]],
    click_id_to_msg_id: dict[str, str],
    ack_msg_ids: set[str],
) -> int:
    """Process the ``rejected`` items from a Sprint 2.1+ verdict response.

    For each rejected click:
      1. Look up the original click dict in the batch (defensive — the
         collector should only echo click_ids we sent).
      2. Hand it to :func:`_handle_rejected_click` which increments the
         per-click retry counter and either re-XADDs OR deadletters.
      3. ACK the CURRENT (failed) msg_id regardless of outcome — the
         click either lives under a new msg_id (re-XADD) or in the
         deadletter stream.

    Side-effect: mutates ``ack_msg_ids`` by adding the rejected msg_ids
    (passed as a set ref — caller's view updates in place).

    Returns:
        Count of clicks that hit max retries and were deadlettered
        during this batch. The caller uses this to pick the
        ``deadlettered`` vs ``partial_ack`` outcome status.
    """
    deadletter_count = 0
    for rej in rejected_items:
        cid = rej.get("click_id")
        reason = rej.get("reason", "unknown")
        # Find the original click dict to retry. Could be missing if the
        # collector echoed an unknown click_id — defensive.
        original_click = next(
            (c for c in clicks if c.get("click_id") == cid),
            None,
        )
        if original_click is None:
            logger.warning(
                "Shipper got reject for unknown click_id=%s in batch "
                "(defensive): %s",
                cid, reason,
            )
            if cid in click_id_to_msg_id:
                ack_msg_ids.add(click_id_to_msg_id[cid])
            continue
        retried = await _handle_rejected_click(
            redis_pool, original_click, reason,
            http_client=http_client,
        )
        if not retried:
            deadletter_count += 1
        # ACK the current msg_id either way.
        if cid in click_id_to_msg_id:
            ack_msg_ids.add(click_id_to_msg_id[cid])
    return deadletter_count


async def _trim_processed_history(redis_pool) -> int:
    """XTRIM MINID the entries the shipper group has fully processed.

    AUD-B F1 (2026-06-12) — replaces the post-ack ``XTRIM MAXLEN ~10000``
    that capped the SURVIVABLE outage backlog at 10k: during a central
    outage clicks pile up in the stream/PEL, and the first successful
    batch after recovery used to trim everything older than the newest
    10k — silently destroying the un-shipped backlog (including
    PEL-referenced entries, which XAUTOCLAIM then drops as deleted).

    A1 (LOSSFIX P1b, 2026-07-07) — stale-comment fix: the XADD side no
    longer carries a MAXLEN trim cap at all (M1 repurposed
    ``stream_clicks_maxlen`` into a whole-request REJECT threshold,
    checked against a cached length sample in ``main._check_stream_
    backpressure`` — over-threshold real clicks divert to the disk
    fallback instead of writing the stream). The capacity ceiling is
    therefore that reject threshold, not a trim, and it now applies
    BEFORE entries ever reach the stream rather than after.

    Mirror of the proven process-service pattern
    (``app/events/consumer.py`` ``trim_processed``). Safe point:

      * PEL non-empty → oldest PENDING id. Entries ≥ it survive, so the
        orphaned-PEL reclaim (C3) is untouched — pending is NEVER trimmed.
      * PEL empty → the group's last-delivered-id. Everything strictly
        below it is delivered AND ACKed (an un-ACKed delivery would be
        in the PEL). XTRIM MINID removes only ids < minid, so the
        boundary entry is kept (one-entry conservatism, harmless).

    Best-effort: any failure skips this round — hygiene must never break
    the ship loop. Returns the number of entries removed.
    """
    try:
        minid: str | None = None
        summary = await redis_pool.xpending(STREAM_KEY, GROUP_NAME)
        if summary.get("pending") and summary.get("min"):
            minid = str(summary["min"])
        else:
            for group in await redis_pool.xinfo_groups(STREAM_KEY):
                if group.get("name") == GROUP_NAME:
                    minid = str(group.get("last-delivered-id") or "")
                    break
        if not minid or minid == "0-0":
            return 0  # group missing or nothing delivered yet
        removed = int(
            await redis_pool.xtrim(STREAM_KEY, minid=minid, approximate=True)
        )
        if removed:
            logger.info(
                "Shipper trimmed %s processed entries below %s", removed, minid,
            )
        return removed
    except Exception as exc:  # noqa: BLE001 — hygiene must never kill the loop
        logger.warning("Shipper processed-history trim failed (skipped): %s", exc)
        return 0


async def _ack_shipped_batch(
    redis_pool,
    ack_msg_ids: set[str] | list[str],
    *,
    batch_size: int,
    collector_status: int,
    shim_active: bool = False,
) -> bool:
    """XACK after a successful batch delivery; log + record on failure.

    AUD-B F1: this helper no longer XTRIMs — the blanket
    ``MAXLEN ~10000`` trim destroyed outage backlog on recovery.
    Processed-history hygiene now runs on the loop clock via
    :func:`_trim_processed_history` (MINID-based, never cuts
    undelivered/pending entries).

    Returns:
        True on success — caller proceeds to record final outcome metrics.
        False on Redis impairment — the batch is DELIVERED to central but
        local stream not ACKed, so clicks may be re-delivered (at-least-
        once contract). Caller records ``ack_failed`` ship status and
        skips the outcome window update (the metric would otherwise show
        success while the local state is broken).

    Args:
        shim_active: True when called from the Sprint 2.5 legacy shim
            path. Surfaces as a Sentry tag so the Sprint 4.1 alert
            ``ack_failed during shim`` can distinguish rolling-deploy-
            window failures from steady-state ones.
    """
    if not ack_msg_ids:
        return True  # Nothing to ACK (legitimate: all rejected, all retried).
    try:
        await redis_pool.xack(STREAM_KEY, GROUP_NAME, *ack_msg_ids)
    except Exception as ack_exc:  # noqa: BLE001
        if shim_active:
            log_msg = (
                "Shipper xack failure (op=%s) during shim: %s"
            )
        else:
            log_msg = (
                "Shipper xack failure (op=%s) — batch was "
                "DELIVERED to central but local stream not ACKed. "
                "Clicks may be re-delivered (at-least-once contract): %s"
            )
        logger.error(log_msg, OP_XACK_BATCH, ack_exc)
        capture_extras: dict[str, Any] = {
            "batch_size": batch_size,
            "collector_status": collector_status,
        }
        if shim_active:
            capture_extras["shim_active"] = True
        _capture_op_exc(OP_XACK_BATCH, ack_exc, **capture_extras)
        shipper_metrics.record_ship("ack_failed", batch_size=batch_size)
        return False
    return True


def _record_new_shape_outcome(
    *,
    accepted_ids: list[str],
    duplicate_ids: list[str],
    rejected_items: list[dict[str, Any]],
    deadletter_count: int,
    batch_size: int,
) -> None:
    """Sprint 2.4 — feed the rolling-5min success-ratio window + emit the
    canonical ``record_ship`` status + summary log line.

    Outcome status precedence (matches plan-doc §4 Sprint 2.4 row):
      * ``deadlettered`` — at least one click hit max retries this iter
      * ``partial_ack`` — some clicks accepted, some rejected (still
        retrying)
      * ``success`` — all clicks landed (accepted ∪ duplicates only)

    The ``record_outcome`` call counts accepted+duplicates as success
    and rejected (including the deadlettered ones for this iteration)
    as non-delivery. The Sprint 2.7d Sentry breadcrumb on
    ``record_outcome`` carries the per-batch ratio + window depth so
    the Sprint 4.1 alert rule keys off consistent data.
    """
    shipper_metrics.record_outcome(
        accepted=len(accepted_ids) + len(duplicate_ids),
        rejected=len(rejected_items),
    )

    if deadletter_count > 0:
        logger.warning(
            "Shipped batch with %d/%d clicks deadlettered (op=%s); "
            "accepted=%d, duplicates=%d, rejected=%d",
            deadletter_count, batch_size, OP_BATCH_POST,
            len(accepted_ids), len(duplicate_ids), len(rejected_items),
        )
        shipper_metrics.record_ship("deadlettered", batch_size=batch_size)
    elif rejected_items:
        logger.info(
            "Shipped batch with %d/%d clicks retried (op=%s); "
            "accepted=%d, duplicates=%d",
            len(rejected_items), batch_size, OP_BATCH_POST,
            len(accepted_ids), len(duplicate_ids),
        )
        shipper_metrics.record_ship("partial_ack", batch_size=batch_size)
    else:
        logger.info(
            "Shipped %d clicks to central (op=%s); accepted=%d, duplicates=%d",
            batch_size, OP_BATCH_POST,
            len(accepted_ids), len(duplicate_ids),
        )
        shipper_metrics.record_ship("success", batch_size=batch_size)


async def _process_new_shape_batch(
    redis_pool,
    http_client: httpx.AsyncClient,
    response: httpx.Response,
    body: dict[str, Any],
    clicks: list[dict[str, Any]],
    msg_ids: list[str],
) -> None:
    """Sprint 2.1+ per-click verdict handling — orchestrator.

    Parses the ``{accepted, rejected, duplicates}`` response shape and:
      1. Builds the click_id → msg_id reverse map.
      2. Computes accepted ∪ duplicates → ack set.
      3. Processes rejected items (retry counter + re-XADD OR deadletter).
      4. Performs the XACK (short-circuits on Redis impairment).
      5. Records outcome metrics + ship status + summary log.

    Returns normally on every path; caller continues to the next loop
    iteration. The ack_failed short-circuit at step 4 records the
    ``ack_failed`` ship status and skips outcome metrics (consistent
    with pre-TD-1 behaviour at line 715-734 of the old shipper.py).
    """
    accepted_ids = body.get("accepted", []) or []
    rejected_items = body.get("rejected", []) or []
    duplicate_ids = body.get("duplicates", []) or []
    batch_size = len(clicks)

    click_id_to_msg_id = {
        c.get("click_id"): m
        for c, m in zip(clicks, msg_ids)
        if c.get("click_id")
    }

    ack_msg_ids = _compute_ack_msg_ids_from_verdict(
        accepted_ids, duplicate_ids, click_id_to_msg_id,
    )

    deadletter_count = await _handle_rejected_in_batch(
        redis_pool, http_client, rejected_items,
        clicks, click_id_to_msg_id, ack_msg_ids,
    )

    acked = await _ack_shipped_batch(
        redis_pool, ack_msg_ids,
        batch_size=batch_size,
        collector_status=response.status_code,
    )
    if not acked:
        return  # record_ship("ack_failed") already emitted; skip outcome metrics

    _record_new_shape_outcome(
        accepted_ids=accepted_ids,
        duplicate_ids=duplicate_ids,
        rejected_items=rejected_items,
        deadletter_count=deadletter_count,
        batch_size=batch_size,
    )


async def _process_legacy_shape_batch(
    redis_pool,
    response: httpx.Response,
    shape: str,
    clicks: list[dict[str, Any]],
    msg_ids: list[str],
) -> None:
    """Sprint 2.5 backwards-compat shim — ACK-all on 200/202 for
    pre-F.29 collector responses (no ``accepted`` key).

    Called when ``shape`` is ``"legacy"`` (has ``received`` / ``queued``
    keys) or ``"unknown"`` (JSON parse failed / unrecognised body) AND
    status is 200 or 202. Status 207 with non-new shape is a contract
    violation handled by :func:`_process_collector_error` instead.

    Behaviour:
      * Logs WARN per-batch (operator visibility during rolling deploy).
      * Emits a Sentry breadcrumb ONCE per shipper lifetime (Sprint 2.7a
        one-shot semantics) — burst guard against Sentry quota.
      * ACK-all msg_ids (delegates to :func:`_ack_shipped_batch`).
      * Records ``legacy_collector`` ship status + counts whole batch
        as accepted in the success-ratio window (legacy 2xx = all
        delivered from operator's perspective).
    """
    global _shim_warned_this_session
    batch_size = len(clicks)

    logger.warning(
        "Shipper shim activated (op=%s) — collector returned %s shape "
        "(status=%d). Falling back to ACK-all legacy semantics. This is "
        "expected during rolling deploy and harmless; verify the "
        "collector reaches Sprint 2.1+ soon for per-click verdict "
        "visibility.",
        OP_LEGACY_COLLECTOR, shape, response.status_code,
    )

    # F.29 Sprint 2.7a — one-shot Sentry capture per shipper lifetime.
    # Pre-2.7 fired on every batch during rolling deploy → ~30 events/
    # min → Sentry quota risk. WARN log above still fires per batch
    # (operator visibility); Sentry event only on first activation.
    if not _shim_warned_this_session:
        _shim_warned_this_session = True
        _capture_op_msg(
            OP_LEGACY_COLLECTOR,
            f"Shipper got legacy ({shape}) response shape from collector — "
            f"falling back to ACK-all (status={response.status_code}). "
            "This message fires ONCE per shipper lifetime to bound Sentry "
            "quota during rolling deploys; subsequent batches log WARN-only.",
            level="warning",
            shape=shape,
            collector_status=response.status_code,
            batch_size=batch_size,
        )

    acked = await _ack_shipped_batch(
        redis_pool, msg_ids,
        batch_size=batch_size,
        collector_status=response.status_code,
        shim_active=True,
    )
    if not acked:
        return  # record_ship("ack_failed") already emitted

    shipper_metrics.record_ship("legacy_collector", batch_size=batch_size)
    # Sprint 2.4 — legacy shim path treats the batch as fully accepted
    # (legacy collector returns 2xx = all delivered). Counted in success-
    # ratio window so operator sees consistent ratios during rolling deploy.
    shipper_metrics.record_outcome(accepted=batch_size, rejected=0)


async def _process_collector_error(
    response: httpx.Response,
    clicks: list[dict[str, Any]],
    retry_delay: int,
    shape: str | None = None,
) -> int:
    """Non-2xx collector response OR 207-with-non-new-shape contract violation.

    Two callers:
      * Outer ``else`` branch (status NOT in 200/202/207) → ``shape=None``,
        treated as plain collector error.
      * 207 + non-new shape path (caller verified ``shape != "new"``
        already) → ``shape`` passed through for context.

    Side-effects:
      * WARN log + Sentry capture with ``op=batch_post``.
      * ``record_ship("collector_error")`` + ``record_outcome(0, batch_size)``
        so the rolling success-ratio window dips.
      * Async sleep ``retry_delay`` seconds.

    Returns:
        New ``retry_delay`` after exponential backoff
        (``min(retry_delay * 2, MAX_RETRY_DELAY)``). Caller assigns this
        back to its loop variable so the state machine stays single-sourced.
    """
    batch_size = len(clicks)
    if shape is not None:
        # 207 + non-new shape — contract violation. A pre-F.29 collector
        # never returns 207 (only 200); 207 + legacy/unknown shape means
        # a Sprint 2.1+ collector returned 207 with corrupt JSON OR a
        # malicious proxy injected the status. Retry conservatively.
        #
        # D9 (audit 2026-06-03): escalate this branch from WARN to ERROR.
        # A garbled 207 means a per-click verdict the shipper cannot read
        # — the batch is retried (no silent ACK-all; that loss path was
        # closed in Sprint 3.7.1 TD-17), but a PERSISTENT garbled-207
        # (collector bug / proxy mangling the body) keeps the batch
        # bouncing and must page an operator, not whisper at WARN. The
        # plain non-2xx path below stays WARN (ordinary transient 5xx).
        logger.error(
            "Shipper got status=207 with non-new body shape=%s "
            "(contract violation, op=%s). Treating as collector_error — "
            "retrying (NOT ACK-all). Body: %s",
            shape, OP_BATCH_POST, response.text[:200],
        )
        _capture_op_msg(
            OP_BATCH_POST,
            f"Contract violation: status 207 with shape={shape} — "
            "unreadable per-click verdict, batch retried not ACK-all",
            level="error",
            collector_status=response.status_code,
            response_body=response.text[:500],
            batch_size=batch_size,
            shape=shape,
        )
    else:
        # F.29 Sprint 1.3 — non-2xx was warn-log only pre-F.29. Now tagged
        # op=batch_post so Sprint 4.1 alert "success_ratio<X" has signal.
        logger.warning(
            "Central returned %s (op=%s): %s. Retry in %ss.",
            response.status_code, OP_BATCH_POST,
            response.text[:200], retry_delay,
        )
        _capture_op_msg(
            OP_BATCH_POST,
            f"Central returned {response.status_code}",
            level="warning",
            collector_status=response.status_code,
            response_body=response.text[:500],
            batch_size=batch_size,
        )
    shipper_metrics.record_ship("collector_error", batch_size=batch_size)
    # Sprint 2.4 — count whole batch as rejected in the success-ratio
    # window (clicks didn't land, will be retried after sleep).
    shipper_metrics.record_outcome(accepted=0, rejected=batch_size)
    await asyncio.sleep(retry_delay)
    return min(retry_delay * 2, MAX_RETRY_DELAY)


async def _handle_central_unreachable(
    exc: Exception,
    batch_size: int,
    retry_delay: int,
) -> int:
    """httpx.RequestError handler — central node TCP/TLS unreachable.

    F.29 Sprint 1.3 tagged this op=batch_post (HTTP connectivity, not
    Redis). Pre-F.29 was warn-log only; central-unreachable was
    invisible in Sentry. Now the op tag groups these with HTTP 5xx
    for unified alerting on "batch_post failure rate".

    Returns:
        New ``retry_delay`` after exponential backoff.
    """
    logger.warning(
        "Central unreachable (op=%s): %s. Retry in %ss.",
        OP_BATCH_POST, exc, retry_delay,
    )
    _capture_op_exc(
        OP_BATCH_POST,
        exc,
        batch_size=batch_size,
        failure_kind="httpx.RequestError",
    )
    shipper_metrics.record_ship("unreachable", batch_size=batch_size)
    # Sprint 2.4 — central unreachable: nothing landed.
    shipper_metrics.record_outcome(accepted=0, rejected=batch_size)
    await asyncio.sleep(retry_delay)
    return min(retry_delay * 2, MAX_RETRY_DELAY)


async def _handle_shipper_loop_error(exc: Exception) -> None:
    """Catch-all Exception fallback for the shipper loop.

    F.29 Sprint 1.3 — catch-all gets a distinct ``OP_LOOP_ITERATION``
    tag so operators can split "structured failure mode" (the tagged
    paths) from "unknown loop branch" (this one). XREADGROUP timeouts
    / Redis impairment surface here when the inner try-except above
    didn't absorb them. Pre-F.29 every exception landed in this branch
    under the generic ``Shipper error`` umbrella.

    Fixed 2-second sleep (NOT exponential backoff) — this path doesn't
    own the retry_delay state because we can't reason about the batch
    size or operation type that triggered it.
    """
    logger.error(
        "Shipper loop catch-all (op=%s): %s",
        OP_LOOP_ITERATION, exc,
    )
    # LOSSFIX P3 (2026-07-07, alert-rule wiring) — failure_kind is a
    # searchable TAG (not `**extras`, which Sentry issue-alert rules
    # cannot filter on) so the `op=loop_iteration AND failure_kind !=
    # TimeoutError` alert rule (see ALERT-RULES.md) actually works:
    # the fleet fires TimeoutError-class on every idle gap >1s (F-6,
    # steady-state until T-7 lands) — a naive rule on `op` alone would
    # page permanently on expected traffic.
    _capture_op_exc(
        OP_LOOP_ITERATION,
        exc,
        tags={"failure_kind": type(exc).__name__},
    )
    shipper_metrics.record_ship("loop_error", batch_size=0)
    await asyncio.sleep(2)


async def run_shipper(redis_pool):
    """Main shipper loop — thin dispatcher (F.29 TD-1, 2026-05-23).

    Pre-TD-1 (`522e832` and earlier) this function spanned ~497 LOC
    at 5-6 nesting levels and bundled XREADGROUP, parse, HTTP POST,
    per-click verdict logic, legacy shim, and all exception handling
    into one body. Agent 1 of the Sprint 2.7 validation cycle flagged
    it as CRITICAL violation of rule code-organization.

    After TD-1 the body is a dispatcher: each loop iteration drains
    one batch, posts it, then routes the response to one of the
    ``_process_*`` helpers. Every observable behaviour (op tags,
    sleep timings, record_ship + record_outcome ordering) preserved
    byte-for-byte vs the pre-TD-1 implementation.

    Lifecycle pinning:
      * ``assert_shipper_ready()`` — defense in depth vs the boot-time
        validator. Local-env / escape-hatch paths exit silently.
      * ``shipper_metrics.mark_running()`` flips ON only AFTER the
        consumer group is established.
      * ``try/finally`` guarantees ``mark_stopped()`` fires on any
        exit (graceful cancellation OR mid-loop crash) — closes the
        50-day audit-2026-05-16 silent-disable pathology G5 targeted.
    """
    # F.29 Sprint 1.2 — synchronous boot validation. Replaces the
    # pre-F.29 silent `if not settings.central_url: return` early-exit
    # at line 34-36 (audit 2026-05-16 incident site). Either returns
    # silently (local env / escape hatch) — loop never entered — or
    # raises ShipperDisabledError (non-local + flag=true + empty url)
    # → propagates to lifespan which re-raises to fail boot.
    assert_shipper_ready()

    # Re-check post-validation: in local env or escape-hatch mode the
    # assert returned silently with an empty central_url; we still
    # exit the coroutine here (no work to do without a central).
    if not settings.central_url:
        return

    # Create consumer group (audit 2026-05-27 — extracted to helper
    # so the NOGROUP auto-heal in _drain_batch_from_stream can reuse
    # the same idempotent creation logic).
    await _ensure_local_consumer_group(redis_pool)

    logger.info(f"Shipper started → {settings.central_url}/api/clicks/batch")
    # F.29 Sprint 1.4 — surface "shipper is alive" to /health. Pre-F.29
    # /health returned redis=true even when the shipper task had crashed
    # silently; running=true here flips ON only after the consumer group
    # is established. The matching mark_stopped() runs in the try/finally
    # block below (Sprint 1.6 fix), so cancellation on lifespan shutdown
    # AND unexpected mid-loop crashes both surface as running=False in
    # /health immediately after exit.
    shipper_metrics.mark_running()

    # F.29 Sprint 1.6 (validation cycle) — try/finally guarantees
    # mark_stopped() fires on ANY exit (graceful cancellation OR
    # unexpected crash mid-loop). Without this the running flag stayed
    # True forever after a task crash → /health reported the shipper as
    # alive while it was actually dead, exactly the 50-day audit-2026-05-16
    # silent-disable pathology that G5 was built to surface.
    try:
        retry_delay = 1
        # C3 (audit 2026-06-03) — periodic orphaned-PEL reclaim cadence.
        # Mirrors the central writer's `last_reclaim` heartbeat. The
        # XREADGROUP block timeout (BATCH_TIMEOUT_MS) is the natural loop
        # heartbeat, so this check fires roughly every
        # `shipper_reclaim_interval_sec` even on an idle node.
        last_reclaim = time.monotonic()
        # AUD-B F1 (2026-06-12) — processed-history trim on the same loop
        # clock (MINID-based, never cuts undelivered/pending entries).
        # 0.0 → first pass runs immediately (safe by construction), then
        # every `shipper_trim_interval_sec` — cheap (3 Redis round-trips)
        # vs the old per-batch MAXLEN trim that destroyed outage backlog.
        last_trim = 0.0
        async with httpx.AsyncClient(timeout=15.0) as client:
            while True:
                # Local-iteration default so the httpx.RequestError /
                # catch-all handlers below can always reference
                # ``batch_size`` without a NameError. If
                # ``_drain_batch_from_stream`` raises before clicks bind,
                # the catch-all branch lands in
                # ``_handle_shipper_loop_error`` (no batch_size arg).
                clicks: list[dict[str, Any]] = []
                try:
                    # C3 — reclaim orphaned PEL entries (from a dead
                    # consumer after a crash/restart) BEFORE draining new
                    # ones. Runs every reclaim_interval_sec regardless of
                    # whether the drain below finds new clicks, so an idle
                    # node still recovers its orphans. Self-contained +
                    # never-raises (see `_reclaim_shipper_pending`).
                    now = time.monotonic()
                    if now - last_reclaim >= settings.shipper_reclaim_interval_sec:
                        await _reclaim_shipper_pending(redis_pool, client)
                        last_reclaim = now

                    # AUD-B F1 — periodic processed-history hygiene
                    # (replaces the per-batch MAXLEN trim). Never raises.
                    if now - last_trim >= settings.shipper_trim_interval_sec:
                        last_trim = now
                        await _trim_processed_history(redis_pool)

                    clicks, msg_ids = await _drain_batch_from_stream(redis_pool)
                    if not clicks:
                        continue

                    response = await _post_batch_to_central(client, clicks)

                    if response.status_code in (200, 202, 207):
                        retry_delay = 1  # successful POST resets backoff
                        shape, body = _parse_collector_response(response.text)

                        if shape == "new":
                            # F.29 Sprint 2.1+ per-click verdict path.
                            await _process_new_shape_batch(
                                redis_pool, client, response, body,
                                clicks, msg_ids,
                            )
                        elif shape == "legacy" and response.status_code in (200, 202):
                            # Sprint 2.5 shim: ONLY an explicit pre-F.29
                            # legacy shape (has ``received``/``queued``
                            # keys) on 200/202 → ACK-all backwards-compat.
                            await _process_legacy_shape_batch(
                                redis_pool, response, shape, clicks, msg_ids,
                            )
                        else:
                            # F.29 Sprint 3.7.1 (TD-17 / validation-cycle-2
                            # Agent 4) — tighten the shim. Everything else
                            # routes to collector_error (retry
                            # conservatively, do NOT ACK-all):
                            #   * ``unknown`` shape on ANY 2xx — pre-3.7.1
                            #     this fell into the shim and silent-ACKed
                            #     the whole batch. If collector middleware
                            #     mutates the body (compression strip,
                            #     proxy error page, truncated JSON) the
                            #     shipper would lose the batch silently.
                            #     Now it retries → at-least-once preserved.
                            #   * ``legacy`` shape on 207 — contract
                            #     violation (legacy collectors never emit
                            #     207); retry.
                            #   * ``unknown`` shape on 207 — same.
                            retry_delay = await _process_collector_error(
                                response, clicks, retry_delay, shape=shape,
                            )
                    else:
                        # Non-2xx — collector error path.
                        retry_delay = await _process_collector_error(
                            response, clicks, retry_delay,
                        )
                except httpx.RequestError as e:
                    retry_delay = await _handle_central_unreachable(
                        e, len(clicks), retry_delay,
                    )
                except Exception as e:
                    await _handle_shipper_loop_error(e)
    finally:
        # Mirror of mark_running() above. Wrapped in try/except because
        # shipper_metrics is a module singleton and an AttributeError
        # here (e.g. module being torn down at interpreter exit) must
        # NOT mask the underlying exception that caused the loop exit
        # in the first place.
        try:
            shipper_metrics.mark_stopped()
        except Exception:  # noqa: BLE001 — finalisation must not raise
            pass
