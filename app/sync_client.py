"""Edge sync client — receives push + periodic pull from central.

Writes routing data to local Redis using atomic write-then-delete pattern:
1. Write all new keys first
2. Then delete stale keys (delta against the previous managed-keys set)
3. This ensures routing data is NEVER empty during sync

Stale-key discovery (T1.3 / G-21, 2026-05-09)
--------------------------------------------
Previously this module SCAN-ed Redis on every apply across 17 prefix
buckets to find keys we own. SCAN cost grows linearly with the
keyspace AND with the prefix list — at the projected scale of ~10k
keys per prefix bucket the SCAN block dominated apply latency
(~100x the rest of the function).

Since admin-api maintains `_MANAGED_KEY` = "sync:managed_keys" as the
authoritative SET of keys it owns (rebuilt at the end of every apply
on both sides), we can discover the previous set in a single
`SMEMBERS` round-trip — O(N) Redis-side, but no per-prefix iteration
and no client-side prefix matching. New nodes start with an empty
SMEMBERS result, which is correct: a fresh Redis can't have stale
routing data by definition. Upgraded nodes already have
`_MANAGED_KEY` populated from the previous (SCAN-era) apply, so
there's no cold-start migration step — the swap is transparent.

`_ROUTING_PREFIXES` is retained below as documentation / a debug
breadcrumb (e.g., `redis-cli --scan --pattern 'campaign:*'` for a
human operator), but is no longer consulted by `apply_snapshot`.
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

# Documentation-only — see module docstring "Stale-key discovery".
# These are the prefix buckets the central snapshot writes; useful
# for ad-hoc redis-cli inspection. NOT consulted by apply_snapshot.
_ROUTING_PREFIXES = [
    "campaign:", "campaigns:active",
    "source:", "sources:active",
    "offer:", "offer_target:",
    "split:", "domain:", "flow:",
    "flows:scope:",
    "geo:", "device:", "os:",
    # Vector 2.10 — org-hierarchy snapshot for buyer_id enrichment.
    # The user hash carries the pre-resolved attribution chain;
    # team/department/custom_group are auxiliary for stats display.
    "user:", "users:active",
    "team:", "teams:active",
    "department:", "departments:active",
    "custom_group:", "custom_groups:active",
]


async def apply_snapshot(redis, snapshot: dict) -> dict:
    """Apply snapshot to local Redis using write-first-delete-after pattern.

    Order: read previous managed-keys set → write new keys → delete
    stale (previous \\ new) → rewrite managed-keys set. This ensures
    routing data is never empty during sync.

    Stale-key delta is computed against the `_MANAGED_KEY` SET that
    the previous successful apply left behind — see module docstring
    "Stale-key discovery (T1.3 / G-21)" for the rationale.
    """
    t_start = time.perf_counter()

    data = snapshot.get("data", {})
    types = snapshot.get("types", {})

    if not data:
        return {"status": "empty", "keys_written": 0}

    # Step 1: Read the previous managed-keys set (T1.3 / G-21).
    # Empty on a brand-new node — that's correct: nothing to clean.
    # Non-empty on an upgraded node — the previous apply (under SCAN
    # or the new code path; both maintain `_MANAGED_KEY` identically)
    # left the authoritative set. SMEMBERS is a single round-trip
    # regardless of cardinality vs. the previous N-prefix SCAN loop.
    all_existing: set[str] = set(await redis.smembers(_MANAGED_KEY))

    # Step 2: WRITE new keys first (before any deletes).
    #
    # H2 fix (2026-05-11): wrap in `transaction=True` so the entire
    # batch executes as one Redis MULTI/EXEC block. Without this, the
    # `delete + sadd` pair for set keys (and `delete + rpush` for list
    # keys) below is pipelined (single round-trip) but NOT atomic at
    # the server — a concurrent `/decide` reader between the DELETE
    # and SADD applying server-side sees an empty set and falls through
    # to no-match. Under single-process uvicorn (--workers 2 default
    # in the Dockerfile) the race window is microseconds and rarely
    # observable, but it becomes load-bearing if the deployment scales
    # workers horizontally or if a future heavy `/decide` path widens
    # the read-vs-sync race. The MULTI block makes set replacement
    # atomic in the same trip — readers see either the old set or the
    # new set, never an empty intermediate state.
    #
    # Aligns with `sync-protocol` rule's asyncio.Lock central-side
    # guarantee — the sync apply on the click-processor side is now
    # transactional in the same spirit as the producer side.
    new_keys: set[str] = set()
    write_pipe = redis.pipeline(transaction=True)

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
