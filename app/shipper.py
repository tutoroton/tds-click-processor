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


# ---------------------------------------------------------------------------
# F.29 Sprint 1.3 — structured Sentry tagging helpers (2026-05-23)
# ---------------------------------------------------------------------------
# Pre-F.29 every shipper exception captured under the generic
# "Shipper error" umbrella (services/click-processor/app/shipper.py:101-103
# pre-F.29: ``except Exception as e: ... sentry_sdk.capture_exception(e)``).
# This blended distinct failure modes — JSON-decode errors, httpx
# request errors, XREADGROUP impairments, XACK race conditions — under
# one Sentry issue group, defeating per-operation alert rules. JSON
# decode failures even ACKed silently with NO Sentry signal at all
# (lines 71-73 pre-F.29).
#
# Sprint 1.3 attaches an ``op`` tag to every exception path so:
#   - Sentry's issue grouping splits by operation (search "tag:op:xreadgroup"
#     vs "tag:op:batch_post"),
#   - the Sprint 4.1 alert rules (`shipper.batch.success_ratio` warn at
#     >10/min) key off ``op=batch_post`` specifically,
#   - operator dashboards filter by ``shipper.node_id`` tag to attribute
#     errors to specific edge nodes.
#
# Helper functions keep ``run_shipper`` itself terse (rule
# ``code-organization`` — function length cap 60 lines; run_shipper
# was already past that pre-F.29 and the helpers prevent further bloat).

# Canonical operation tags. Centralised so a typo in a string literal
# can't silently shard the same logical operation across two Sentry
# tag values.
OP_XREADGROUP = "xreadgroup"
OP_PARSE_PAYLOAD = "parse_payload"
OP_BATCH_POST = "batch_post"
OP_XACK = "xack"
OP_XACK_BATCH = "xack_batch"
OP_LOOP_ITERATION = "loop_iteration"


def _capture_op_exc(op_name: str, exc: BaseException, **extras: object) -> None:
    """Capture an exception to Sentry with the F.29 ``op`` tag scheme.

    Centralises the ``push_scope`` + ``set_tag`` + ``capture_exception``
    incantation so every shipper exception path tags consistently. The
    ``shipper.node_id`` tag enables per-node filtering in Sentry +
    alert-rule routing (different on-call teams for different regions).

    Args:
        op_name: One of the ``OP_*`` constants above.
        exc: The exception object to report.
        **extras: Additional context (e.g. ``msg_id``, ``batch_size``).
            Each key becomes a Sentry "extras" entry — visible in the
            issue detail but not searchable as a tag (cardinality is
            often too high).
    """
    with sentry_sdk.push_scope() as scope:
        scope.set_tag("op", op_name)
        scope.set_tag("shipper.node_id", settings.node_id)
        for key, value in extras.items():
            scope.set_extra(key, value)
        sentry_sdk.capture_exception(exc)


def _capture_op_msg(
    op_name: str,
    message: str,
    level: str = "warning",
    **extras: object,
) -> None:
    """Capture a Sentry message with the same op-tag scheme.

    Use this for non-exception signals (e.g. non-2xx HTTP responses,
    parse failures where the exception is suppressed by an intentional
    ACK). Same tag shape as :func:`_capture_op_exc` so alert rules
    can pivot across both code paths uniformly.
    """
    with sentry_sdk.push_scope() as scope:
        scope.set_tag("op", op_name)
        scope.set_tag("shipper.node_id", settings.node_id)
        for key, value in extras.items():
            scope.set_extra(key, value)
        sentry_sdk.capture_message(message, level=level)


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
    # is established. The matching mark_stopped() lives in the finally
    # block at the end of this function so cancellation on lifespan
    # shutdown surfaces correctly in /health right up to process exit.
    shipper_metrics.mark_running()

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

                if response.status_code in (200, 202):
                    # Success — ACK + XTRIM. Wrapped because a Redis
                    # blip between the successful POST and ACK would
                    # otherwise propagate to the outer ``except
                    # Exception`` and tag this as ``op=loop_iteration``
                    # — losing the fact that the batch DID reach
                    # central. With ``op=xack_batch`` the operator can
                    # see that at-least-once semantics were exercised
                    # (clicks may be re-delivered next iteration via
                    # PEL claim) and that the duplicate is benign.
                    try:
                        await redis_pool.xack(
                            STREAM_KEY, GROUP_NAME, *msg_ids,
                        )
                        await redis_pool.xtrim(
                            STREAM_KEY, maxlen=10000, approximate=True,
                        )
                    except Exception as ack_exc:  # noqa: BLE001
                        logger.error(
                            "Shipper xack/xtrim failure (op=%s) — batch was "
                            "DELIVERED to central but local stream not "
                            "ACKed. Clicks may be re-delivered (at-least-"
                            "once contract — Sprint 2 deadletter will "
                            "bound replays): %s",
                            OP_XACK_BATCH, ack_exc,
                        )
                        _capture_op_exc(
                            OP_XACK_BATCH,
                            ack_exc,
                            batch_size=len(clicks),
                            collector_status=response.status_code,
                        )
                        # F.29 Sprint 1.4 — surface this distinct
                        # outcome to /health so operators see "batch
                        # got there but local ACK didn't" without
                        # having to read logs.
                        shipper_metrics.record_ship(
                            "ack_failed", batch_size=len(clicks),
                        )
                    else:
                        logger.info(
                            "Shipped %d clicks to central (op=%s)",
                            len(clicks), OP_BATCH_POST,
                        )
                        shipper_metrics.record_ship(
                            "success", batch_size=len(clicks),
                        )
                    retry_delay = 1  # Reset retry delay
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
