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

from app.config import settings

logger = logging.getLogger("tds.shipper")

STREAM_KEY = "stream:clicks"
GROUP_NAME = "shippers"
CONSUMER_NAME = f"shipper-{settings.node_id}-{os.getpid()}"
BATCH_SIZE = 500
BATCH_TIMEOUT_MS = 2000  # 2 seconds
MAX_RETRY_DELAY = 30


async def run_shipper(redis_pool):
    """Main shipper loop."""
    if not settings.central_url:
        logger.info("No CENTRAL_URL configured — shipper disabled (standalone mode)")
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
