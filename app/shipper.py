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

import httpx
import sentry_sdk

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
          no recognised keys. Caller falls back to status-code-only
          decision (legacy ACK-all on 2xx).
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
    """
    payload = json.dumps(click, default=str)
    await redis_pool.xadd(
        STREAM_KEY,
        {"data": payload},
        maxlen=settings.stream_clicks_maxlen,
        approximate=True,
    )


async def _deadletter_click(
    redis_pool,
    click: dict[str, Any],
    attempt: int,
    reason: str,
) -> None:
    """Move a click to the local deadletter stream after max retries.

    F.29 Sprint 2.2 (2026-05-23). The click sits in
    ``stream:clicks-deadletter`` (local edge Redis, MAXLEN 10k) until
    Sprint 2.3 adds central forwarding. Operator can inspect via
    ``redis-cli XRANGE`` or the (future) ``/api/health/summary``
    deadletter widget.

    The deadletter record carries:
      * ``data`` — original click JSON.
      * ``attempt_count`` — final attempt number that triggered the
        cap (operator can see "this click failed 5 times").
      * ``last_rejection_reason`` — collector's reason string for
        the last attempt (e.g. ``"queue_failure"``,
        ``"validation_failed"``).
      * ``deadlettered_at`` — UNIX timestamp.
      * ``node_id`` — which edge node deadlettered the click. Sprint
        2.3 central forwarding will use this to bucket by tenant.
    """
    import time as _time
    record = {
        "data": json.dumps(click, default=str),
        "attempt_count": str(attempt),
        "last_rejection_reason": reason[:64],  # bound reason length
        "deadlettered_at": str(_time.time()),
        "node_id": settings.node_id,
    }
    try:
        await redis_pool.xadd(
            DEADLETTER_STREAM_KEY,
            record,
            maxlen=DEADLETTER_STREAM_MAXLEN,
            approximate=True,
        )
    except Exception as exc:  # noqa: BLE001
        # Deadletter XADD failure is rare (separate stream, low rate)
        # but not catastrophic — log + Sentry capture. The click is
        # genuinely lost in this corner case; operator visibility is
        # the only mitigation here. Future Sprint 2.3 central path
        # provides redundancy.
        logger.error(
            "Shipper deadletter XADD failed for click_id=%s "
            "(attempt=%d, reason=%s): %s",
            click.get("click_id", "<unknown>"), attempt, reason, exc,
        )
        _capture_op_exc(
            "deadletter",
            exc,
            click_id=click.get("click_id", "<unknown>"),
            attempt=attempt,
            reason=reason,
        )


async def _handle_rejected_click(
    redis_pool,
    click: dict[str, Any],
    reason: str,
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
        )
        return False

    retry_key = f"{_RETRY_KEY_PREFIX}{click_id}"
    try:
        attempt = await redis_pool.incr(retry_key)
        await redis_pool.expire(
            retry_key, settings.shipper_retry_ttl_seconds,
        )
    except Exception as exc:  # noqa: BLE001
        # Counter increment failed — Redis impairment. Default to
        # deadletter conservatively rather than infinite-retry.
        logger.warning(
            "Shipper retry counter increment failed for click_id=%s: "
            "%s. Deadlettering conservatively.",
            click_id, exc,
        )
        await _deadletter_click(
            redis_pool, click, attempt=-1, reason=f"counter_error:{reason}",
        )
        return False

    if attempt >= settings.shipper_max_retry_attempts:
        await _deadletter_click(
            redis_pool, click, attempt=attempt, reason=reason,
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
        )
        return False

    return True


async def run_shipper(redis_pool):
    """Main shipper loop."""
    # F.29 Sprint 1.2 — synchronous boot validation. Replaces the
    # pre-F.29 silent `if not settings.central_url: return` early-exit
    # at line 34-36 (audit 2026-05-16 incident site). The function
    # either:
    #   - returns silently (local env / escape hatch) — loop never
    #     entered, no clicks shipped, but no error either,
    #   - raises ShipperDisabledError (non-local + flag=true + empty
    #     url) — propagates to lifespan which re-raises to fail boot.
    assert_shipper_ready()

    # Re-check post-validation: in local env or escape-hatch mode the
    # assert returned silently with an empty central_url; we still
    # exit the coroutine here (no work to do without a central).
    if not settings.central_url:
        return

    # Create consumer group
    try:
        await redis_pool.xgroup_create(STREAM_KEY, GROUP_NAME, id="0", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            raise

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
    # unexpected crash mid-loop). Without this the running flag
    # stayed True forever after a task crash → /health reported
    # the shipper as alive while it was actually dead, exactly the
    # 50-day audit-2026-05-16 silent-disable pathology that G5 was
    # built to surface. Caught by Agent 3 validation 2026-05-23.
    try:
        retry_delay = 1
        async with httpx.AsyncClient(timeout=15.0) as client:
            while True:
                try:
                    # Read batch from local stream
                    results = await redis_pool.xreadgroup(
                        GROUP_NAME, CONSUMER_NAME,
                        {STREAM_KEY: ">"},
                        count=BATCH_SIZE,
                        block=BATCH_TIMEOUT_MS,
                    )

                    if not results:
                        continue

                    stream_name, messages = results[0]
                    clicks = []
                    msg_ids = []

                    for msg_id, data in messages:
                        msg_ids.append(msg_id)
                        try:
                            click = json.loads(data.get("data", "{}"))
                            clicks.append(click)
                        except (json.JSONDecodeError, TypeError) as parse_exc:
                            # F.29 Sprint 1.3 (2026-05-23) — visible parse
                            # failure. Pre-F.29 the except branch silently
                            # ACKed and continued, hiding corrupt payloads
                            # from Sentry. Now we WARN-log + capture with
                            # ``op=parse_payload`` so the parse-failure rate
                            # is visible per node + alertable in Sprint 4.1.
                            logger.warning(
                                "Shipper parse failure (op=%s) for msg=%s: %s. "
                                "ACKing to unblock stream — corrupt click "
                                "payload, data is lost.",
                                OP_PARSE_PAYLOAD, msg_id, parse_exc,
                            )
                            _capture_op_msg(
                                OP_PARSE_PAYLOAD,
                                f"Shipper parse failure for msg_id={msg_id}: "
                                f"{parse_exc}",
                                level="warning",
                                msg_id=str(msg_id),
                            )
                            # Best-effort XACK. Wrapped because Redis
                            # impairment is exactly the condition we are
                            # recovering from; without the wrap an XACK
                            # failure would propagate to the outer Exception
                            # handler and lose the parse-payload tag. The
                            # parse-failed message stays in the pending
                            # entry list on XACK failure; XREADGROUP with
                            # ``>`` moves past it on the consumer side.
                            try:
                                await redis_pool.xack(
                                    STREAM_KEY, GROUP_NAME, msg_id,
                                )
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

                    if not clicks:
                        continue

                    # Send batch to central collector
                    response = await client.post(
                        f"{settings.central_url}/api/clicks/batch",
                        json={"node_id": settings.node_id, "clicks": clicks},
                        headers={"X-Node-Key": settings.central_api_key},
                    )

                    if response.status_code in (200, 202, 207):
                        # F.29 Sprint 2.2 (2026-05-23) — per-click
                        # verdict processing. Pre-F.29 we ACKed ALL
                        # msg_ids regardless of body. Now we parse
                        # ``{accepted, rejected, duplicates}``, ACK
                        # only successful clicks, and retry / deadletter
                        # the rejected ones. Status 207 is the new
                        # multi-status (some rejected) signal.
                        #
                        # Shim (Sprint 2.5): if response body is the
                        # pre-F.29 shape (no ``accepted`` key), fall
                        # back to legacy ACK-all behavior + WARN log.
                        # Handles the rolling-deploy window where the
                        # shipper has been updated but the collector
                        # hasn't yet.
                        retry_delay = 1  # Reset retry delay
                        shape, body = _parse_collector_response(
                            response.text,
                        )
                        click_id_to_msg_id = {
                            c.get("click_id"): m
                            for c, m in zip(clicks, msg_ids)
                            if c.get("click_id")
                        }

                        if shape == "new":
                            # Per-click verdict path.
                            accepted_ids = body.get("accepted", []) or []
                            rejected_items = body.get("rejected", []) or []
                            duplicate_ids = body.get("duplicates", []) or []

                            # ACK accepted ∪ duplicates — both groups
                            # represent "click is at central" (just via
                            # different paths). Use a set to avoid
                            # double-ACK on the same msg_id if a
                            # malformed response lists the same click
                            # twice.
                            ack_msg_ids = set()
                            for cid in accepted_ids:
                                if cid in click_id_to_msg_id:
                                    ack_msg_ids.add(click_id_to_msg_id[cid])
                            for cid in duplicate_ids:
                                if cid in click_id_to_msg_id:
                                    ack_msg_ids.add(click_id_to_msg_id[cid])

                            # Process rejected: increment retry counter,
                            # re-XADD OR deadletter. The msg_id of the
                            # current (failed) attempt gets ACKed
                            # regardless of outcome — either the click
                            # is now in deadletter or it's been re-
                            # XADDed under a new msg_id.
                            deadletter_count = 0
                            for rej in rejected_items:
                                cid = rej.get("click_id")
                                reason = rej.get("reason", "unknown")
                                # Find the original click dict to retry.
                                # Could be missing if the collector
                                # echoed an unknown click_id — defensive.
                                original_click = next(
                                    (c for c in clicks if c.get("click_id") == cid),
                                    None,
                                )
                                if original_click is None:
                                    logger.warning(
                                        "Shipper got reject for unknown "
                                        "click_id=%s in batch (defensive): %s",
                                        cid, reason,
                                    )
                                    if cid in click_id_to_msg_id:
                                        ack_msg_ids.add(click_id_to_msg_id[cid])
                                    continue
                                retried = await _handle_rejected_click(
                                    redis_pool, original_click, reason,
                                )
                                if not retried:
                                    deadletter_count += 1
                                # ACK the current msg_id either way.
                                if cid in click_id_to_msg_id:
                                    ack_msg_ids.add(click_id_to_msg_id[cid])

                            # ACK all that we decided to ACK.
                            if ack_msg_ids:
                                try:
                                    await redis_pool.xack(
                                        STREAM_KEY, GROUP_NAME, *ack_msg_ids,
                                    )
                                    await redis_pool.xtrim(
                                        STREAM_KEY, maxlen=10000,
                                        approximate=True,
                                    )
                                except Exception as ack_exc:  # noqa: BLE001
                                    logger.error(
                                        "Shipper xack/xtrim failure (op=%s) "
                                        "— batch was DELIVERED to central "
                                        "but local stream not ACKed. "
                                        "Clicks may be re-delivered "
                                        "(at-least-once contract): %s",
                                        OP_XACK_BATCH, ack_exc,
                                    )
                                    _capture_op_exc(
                                        OP_XACK_BATCH,
                                        ack_exc,
                                        batch_size=len(clicks),
                                        collector_status=response.status_code,
                                    )
                                    shipper_metrics.record_ship(
                                        "ack_failed",
                                        batch_size=len(clicks),
                                    )
                                    continue

                            # Determine outcome status. Precedence:
                            # deadlettered > partial_ack > success.
                            if deadletter_count > 0:
                                logger.warning(
                                    "Shipped batch with %d/%d clicks "
                                    "deadlettered (op=%s); accepted=%d, "
                                    "duplicates=%d, rejected=%d",
                                    deadletter_count, len(clicks),
                                    OP_BATCH_POST,
                                    len(accepted_ids),
                                    len(duplicate_ids),
                                    len(rejected_items),
                                )
                                shipper_metrics.record_ship(
                                    "deadlettered",
                                    batch_size=len(clicks),
                                )
                            elif rejected_items:
                                logger.info(
                                    "Shipped batch with %d/%d clicks "
                                    "retried (op=%s); accepted=%d, "
                                    "duplicates=%d",
                                    len(rejected_items), len(clicks),
                                    OP_BATCH_POST,
                                    len(accepted_ids),
                                    len(duplicate_ids),
                                )
                                shipper_metrics.record_ship(
                                    "partial_ack",
                                    batch_size=len(clicks),
                                )
                            else:
                                logger.info(
                                    "Shipped %d clicks to central (op=%s); "
                                    "accepted=%d, duplicates=%d",
                                    len(clicks), OP_BATCH_POST,
                                    len(accepted_ids),
                                    len(duplicate_ids),
                                )
                                shipper_metrics.record_ship(
                                    "success",
                                    batch_size=len(clicks),
                                )
                            continue  # next loop iteration

                        # ── Sprint 2.5 backwards-compat shim ──────
                        # ``shape`` is "legacy" or "unknown". Treat as
                        # pre-F.29 collector — ACK ALL on 200/202 only.
                        # 207 from a legacy collector cannot happen
                        # (legacy never returns 207); if shape=="legacy"
                        # and status==207 there's a contract violation,
                        # fall through to error path.
                        if response.status_code in (200, 202):
                            logger.warning(
                                "Shipper shim activated (op=%s) — collector "
                                "returned %s shape (status=%d). Falling back "
                                "to ACK-all legacy semantics. This is "
                                "expected during rolling deploy and harmless; "
                                "verify the collector reaches Sprint 2.1+ "
                                "soon for per-click verdict visibility.",
                                "legacy_collector", shape,
                                response.status_code,
                            )
                            _capture_op_msg(
                                "legacy_collector",
                                f"Shipper got legacy ({shape}) response "
                                f"shape from collector — falling back to "
                                f"ACK-all (status={response.status_code}).",
                                level="warning",
                                shape=shape,
                                collector_status=response.status_code,
                                batch_size=len(clicks),
                            )
                            try:
                                await redis_pool.xack(
                                    STREAM_KEY, GROUP_NAME, *msg_ids,
                                )
                                await redis_pool.xtrim(
                                    STREAM_KEY, maxlen=10000,
                                    approximate=True,
                                )
                            except Exception as ack_exc:  # noqa: BLE001
                                logger.error(
                                    "Shipper xack/xtrim failure (op=%s) "
                                    "during shim: %s",
                                    OP_XACK_BATCH, ack_exc,
                                )
                                _capture_op_exc(
                                    OP_XACK_BATCH,
                                    ack_exc,
                                    batch_size=len(clicks),
                                    collector_status=response.status_code,
                                    shim_active=True,
                                )
                                shipper_metrics.record_ship(
                                    "ack_failed",
                                    batch_size=len(clicks),
                                )
                                continue
                            shipper_metrics.record_ship(
                                "legacy_collector",
                                batch_size=len(clicks),
                            )
                            continue

                        # Status 207 from a legacy/unknown body shape —
                        # contract violation. A pre-F.29 collector never
                        # returns 207 (only 200), so 207 + legacy shape
                        # means a Sprint 2.1+ collector returned a 207
                        # but body parsing failed (corrupt JSON?) OR a
                        # malicious proxy injected the status. Treat as
                        # collector_error to be safe; the inner retry
                        # loop will re-attempt.
                        logger.warning(
                            "Shipper got status=207 with non-new body shape=%s "
                            "(contract violation, op=%s). Treating as "
                            "collector_error — retrying. Body: %s",
                            shape, OP_BATCH_POST, response.text[:200],
                        )
                        _capture_op_msg(
                            OP_BATCH_POST,
                            f"Contract violation: status 207 with shape={shape}",
                            level="warning",
                            collector_status=response.status_code,
                            response_body=response.text[:500],
                            batch_size=len(clicks),
                            shape=shape,
                        )
                        shipper_metrics.record_ship(
                            "collector_error", batch_size=len(clicks),
                        )
                        await asyncio.sleep(retry_delay)
                        retry_delay = min(retry_delay * 2, MAX_RETRY_DELAY)
                        continue
                    else:
                        # F.29 Sprint 1.3 — non-2xx response was warn-log
                        # only pre-F.29. Now tagged op=batch_post so the
                        # Sprint 4.1 alert "shipper.batch.success_ratio<X"
                        # has a queryable signal.
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
                            batch_size=len(clicks),
                        )
                        shipper_metrics.record_ship(
                            "collector_error", batch_size=len(clicks),
                        )
                        await asyncio.sleep(retry_delay)
                        retry_delay = min(retry_delay * 2, MAX_RETRY_DELAY)

                except httpx.RequestError as e:
                    # F.29 Sprint 1.3 — tagged op=batch_post (HTTP
                    # connectivity, not redis). Pre-F.29 warn-log only;
                    # central-unreachable was invisible in Sentry. Now
                    # the op tag groups these with HTTP 5xx for unified
                    # alerting on "batch_post failure rate".
                    logger.warning(
                        "Central unreachable (op=%s): %s. Retry in %ss.",
                        OP_BATCH_POST, e, retry_delay,
                    )
                    _capture_op_exc(
                        OP_BATCH_POST,
                        e,
                        batch_size=len(clicks),
                        failure_kind="httpx.RequestError",
                    )
                    shipper_metrics.record_ship(
                        "unreachable", batch_size=len(clicks),
                    )
                    await asyncio.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, MAX_RETRY_DELAY)
                except Exception as e:
                    # F.29 Sprint 1.3 — catch-all gets a distinct tag so
                    # operators can split "structured failure mode" (the
                    # tagged paths above) from "unknown loop branch"
                    # (this one). XREADGROUP timeouts / Redis impairment
                    # surface here when the inner try-except above didn't
                    # absorb them. Pre-F.29 every exception landed in this
                    # branch with the generic ``Shipper error`` umbrella.
                    logger.error(
                        "Shipper loop catch-all (op=%s): %s",
                        OP_LOOP_ITERATION, e,
                    )
                    _capture_op_exc(
                        OP_LOOP_ITERATION,
                        e,
                        failure_kind=type(e).__name__,
                    )
                    shipper_metrics.record_ship("loop_error", batch_size=0)
                    await asyncio.sleep(2)
    finally:
        # Mirror of mark_running() above. Wrapped in try/except
        # because shipper_metrics is a module singleton and an
        # AttributeError here (e.g. module being torn down at
        # interpreter exit) must NOT mask the underlying exception
        # that caused the loop exit in the first place.
        try:
            shipper_metrics.mark_stopped()
        except Exception:  # noqa: BLE001 — finalisation must not raise
            pass
