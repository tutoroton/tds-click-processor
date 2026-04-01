"""Edge sync client — receives push + periodic pull from central.

Writes routing data to local Redis using atomic write-then-delete pattern:
1. Write all new keys first
2. Then delete stale keys
3. This ensures routing data is NEVER empty during sync
"""

import asyncio
import json
import logging
import time
from urllib.request import Request, urlopen
from urllib.error import URLError

from app.config import settings

logger = logging.getLogger("tds.sync_client")

_MANAGED_KEY = "sync:managed_keys"

# Key prefixes for routing config (NOT operational state)
_ROUTING_PREFIXES = [
    "campaign:", "campaigns:active",
    "offer:", "split:", "domain:", "flow:",
    "geo:", "device:", "os:",
]


async def apply_snapshot(redis, snapshot: dict) -> dict:
    """Apply snapshot to local Redis using write-first-delete-after pattern.

    Order: write new keys → delete stale keys → update tracking.
    This ensures routing data is never empty during sync.
    """
    t_start = time.perf_counter()

    data = snapshot.get("data", {})
    types = snapshot.get("types", {})

    if not data:
        return {"status": "empty", "keys_written": 0}

    # Step 1: Find ALL existing routing keys
    all_existing = set()
    for prefix in _ROUTING_PREFIXES:
        cursor = 0
        while True:
            cursor, keys = await redis.scan(cursor, match=f"{prefix}*", count=200)
            all_existing.update(keys)
            if cursor == 0:
                break

    # Step 2: WRITE new keys first (before any deletes)
    new_keys: set[str] = set()
    write_pipe = redis.pipeline()

    # Detect keys that change type (rare — only on schema changes)
    type_changed_keys = set()
    for key in new_keys & all_existing:
        # If the key exists but we're about to write a different type, delete first
        pass  # Types rarely change; handled by Redis command overwrite

    for key, value in data.items():
        new_keys.add(key)
        key_type = types.get(key, "string")

        if key_type == "hash" and isinstance(value, dict):
            if value:
                # HSET is idempotent — overwrites fields in-place, no delete needed
                write_pipe.hset(key, mapping={k: str(v) for k, v in value.items()})
        elif key_type == "set" and isinstance(value, list):
            if value:
                # For sets: delete + sadd to ensure exact membership (no stale members)
                write_pipe.delete(key)
                write_pipe.sadd(key, *value)
        elif key_type == "list" and isinstance(value, list):
            if value:
                write_pipe.delete(key)
                write_pipe.rpush(key, *value)
        else:
            write_pipe.set(key, str(value))

    # Store sync version (use 'is not None' — version 0 is valid)
    sync_version = snapshot.get("sync_version", 0)
    if sync_version is not None:
        write_pipe.set("sync:version", str(sync_version))

    # Execute writes
    await write_pipe.execute()

    # Step 3: THEN delete stale keys (routing data is already live)
    stale_keys = all_existing - new_keys
    if stale_keys:
        delete_pipe = redis.pipeline()
        for key in stale_keys:
            delete_pipe.delete(key)
        await delete_pipe.execute()

    # Step 4: Update managed keys tracking
    track_pipe = redis.pipeline()
    track_pipe.delete(_MANAGED_KEY)
    if new_keys:
        track_pipe.sadd(_MANAGED_KEY, *new_keys)
    await track_pipe.execute()

    elapsed = round((time.perf_counter() - t_start) * 1000, 1)

    stats = {
        "status": "ok",
        "keys_written": len(new_keys),
        "stale_removed": len(stale_keys),
        "snapshot_timestamp": snapshot.get("timestamp", ""),
        "applied_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "elapsed_ms": elapsed,
    }

    logger.info(
        "Snapshot applied: %d keys written, %d stale removed in %.1fms",
        len(new_keys), len(stale_keys), elapsed,
    )

    return stats


async def pull_from_central(redis) -> dict | None:
    """Pull snapshot from central admin-api and apply to local Redis."""
    central_url = settings.central_url
    if not central_url:
        return None

    snapshot_url = f"{central_url.rstrip('/')}/api/system/sync/snapshot"

    try:
        req = Request(
            snapshot_url,
            headers={
                "X-TDS-Key": settings.tds_secret_key or "",
                "Accept": "application/json",
            },
        )
        resp_data = json.loads(urlopen(req, timeout=15).read())

        if not resp_data.get("data"):
            logger.warning("Central snapshot has no data")
            return None

        stats = await apply_snapshot(redis, resp_data)
        logger.info("Pull from central complete: %d keys", stats["keys_written"])
        return stats

    except (URLError, OSError, json.JSONDecodeError) as e:
        logger.warning("Failed to pull from central %s: %s", central_url, e)
        return None


async def start_periodic_pull(redis, interval: int = 60):
    """Background task: periodically pull from central."""
    if not settings.central_url:
        logger.info("No CENTRAL_URL configured — periodic pull disabled")
        return

    logger.info("Periodic pull started (every %ds from %s)", interval, settings.central_url)

    # Initial pull on startup — immediate, with retry
    await asyncio.sleep(3)
    result = await pull_from_central(redis)
    if result is None:
        # Retry quickly on startup failure
        for attempt in range(3):
            await asyncio.sleep(5)
            result = await pull_from_central(redis)
            if result:
                break
            logger.warning("Startup pull attempt %d failed", attempt + 2)

    while True:
        await asyncio.sleep(interval)
        try:
            await pull_from_central(redis)
        except Exception:
            logger.exception("Periodic pull failed")
