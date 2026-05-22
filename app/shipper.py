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

logger = logging.getLogger("tds.shipper")

STREAM_KEY = "stream:clicks"
GROUP_NAME = "shippers"
CONSUMER_NAME = f"shipper-{settings.node_id}-{os.getpid()}"
BATCH_SIZE = 500
BATCH_TIMEOUT_MS = 2000  # 2 seconds
MAX_RETRY_DELAY = 30


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
                    except (json.JSONDecodeError, TypeError):
                        # Invalid data — ACK to not block stream
                        await redis_pool.xack(STREAM_KEY, GROUP_NAME, msg_id)

                if not clicks:
                    continue

                # Send batch to central collector
                response = await client.post(
                    f"{settings.central_url}/api/clicks/batch",
                    json={"node_id": settings.node_id, "clicks": clicks},
                    headers={"X-Node-Key": settings.central_api_key},
                )

                if response.status_code in (200, 202):
                    # Success — ACK all messages
                    await redis_pool.xack(STREAM_KEY, GROUP_NAME, *msg_ids)
                    # Trim stream (keep last 10K as buffer)
                    await redis_pool.xtrim(STREAM_KEY, maxlen=10000, approximate=True)
                    logger.info(f"Shipped {len(clicks)} clicks to central")
                    retry_delay = 1  # Reset retry delay
                else:
                    logger.warning(f"Central returned {response.status_code}: {response.text[:200]}")
                    await asyncio.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, MAX_RETRY_DELAY)

            except httpx.RequestError as e:
                logger.warning(f"Central unreachable: {e}. Retry in {retry_delay}s")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, MAX_RETRY_DELAY)
            except Exception as e:
                logger.error(f"Shipper error: {e}")
                sentry_sdk.capture_exception(e)
                await asyncio.sleep(2)
