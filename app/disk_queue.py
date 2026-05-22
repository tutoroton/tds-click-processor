"""Disk fallback queue for click writes (T2.2 / G-23).

Closes the click-loss gap on the `/decide` hot path. The MAXLEN
cap (T2.1) defends Redis from unbounded growth, but cannot help
when Redis itself is unreachable — XADD raises, the click
record vanishes. This module catches that case: the record is
serialised to a local JSON file; a background drainer replays it
into Redis once the outage clears. Drained files are unlinked.

Storage layout:

    var/click-queue/
        <YYYY-MM-DD>/
            <YYYYMMDDTHHMMSS>-<uuid8>.json
            ...

Date-bucketed for human inspection + log-rotation parity. ISO-8601
timestamp prefix gives time-sorted listing (drainer drains oldest
first — first-in-first-out semantics, matching Redis Streams'
natural order). UUID suffix avoids collision when the same second
sees multiple enqueues.

Atomic write contract:

    1. Write to <name>.tmp
    2. fsync the file descriptor
    3. Rename .tmp → <name> (atomic on POSIX)

A crash mid-write leaves a `.tmp` file. The drainer scans only
`*.json` files, so half-written records are never replayed — they
sit on disk until manually inspected (operator may want to
post-mortem). The fsync before rename ensures durable write
ordering: even if power dies between rename and the next loop
iteration, the file is recoverable.

Hot-path performance:

    enqueue_click() runs OFF the asyncio event loop via
    asyncio.to_thread() — file I/O happens in the default thread
    pool, the loop stays free for other requests. Typical write
    latency is sub-millisecond for our payload size; even under
    heavy outage load the budget is dominated by Redis-recovery
    time, not by disk write.

In-memory size counter:

    A naive cap check (rglob + count on every enqueue) would scan
    100k files on every click during a sustained outage —
    pathological. Instead we maintain an in-memory counter,
    initialised once at first call by a single filesystem scan.
    Increment on successful write, decrement on successful drain.
    Exact in single-process click-processor (the only deployment
    today); if the service ever runs multi-worker, the counter
    becomes a per-worker estimate — still acceptable for a cap
    check (worst case: ~N workers go ~N% over cap).

Reference: rule `sync-protocol`, action-items.md T2.2,
open-questions.md G-23.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import time
import uuid
from pathlib import Path

import sentry_sdk

from app.config import _LOCAL_ENVIRONMENTS, settings

logger = logging.getLogger("tds.disk_queue")


# In-memory count of files currently in the queue. Initialised
# lazily on first enqueue from a filesystem scan; mutated on
# every enqueue (+1) and successful drain (-1). Accuracy is
# guaranteed in single-process deployment; multi-worker would
# need a shared counter (per-worker estimate is good enough for
# a cap check, see module docstring).
_queue_size: int = 0
_queue_size_initialized: bool = False
_init_lock: asyncio.Lock | None = None


def _get_init_lock() -> asyncio.Lock:
    """Lazy lock construction — `asyncio.Lock()` requires a running
    event loop, so we defer instantiation to first call. Safe in
    single-loop context (only one event loop ever exists in
    click-processor's lifespan)."""
    global _init_lock
    if _init_lock is None:
        _init_lock = asyncio.Lock()
    return _init_lock


def _queue_root() -> Path:
    """Resolve the queue root path. Resolution is per-call so a
    test that monkeypatches settings.disk_queue_root sees the new
    value without restart."""
    return Path(settings.disk_queue_root)


def _today_dir() -> Path:
    """Today's UTC bucket — keeps a long-running queue from piling
    100k files in a single directory (some filesystems degrade
    badly past ~10k entries per directory)."""
    return _queue_root() / time.strftime("%Y-%m-%d", time.gmtime())


def _new_filename() -> str:
    """Time-sorted, collision-resistant filename. ``YYYYMMDDTHHMMSS``
    prefix sorts lexicographically by time; 8-char UUID4 suffix
    avoids same-second collisions across concurrent enqueues."""
    ts = time.strftime("%Y%m%dT%H%M%S", time.gmtime())
    suffix = uuid.uuid4().hex[:8]
    return f"{ts}-{suffix}.json"


def _count_queue_files_sync() -> int:
    """One-shot filesystem scan — used at first-init to seed the
    in-memory counter. Counts only `*.json` (excludes `.tmp` and
    other ad-hoc files an operator might place there)."""
    root = _queue_root()
    if not root.exists():
        return 0
    return sum(1 for _ in root.rglob("*.json"))


def _list_queue_files_sync() -> list[Path]:
    """Sorted listing of queueable files. Sort is critical — gives
    the drainer FIFO semantics (oldest clicks ship first), matching
    Redis Stream natural order."""
    root = _queue_root()
    if not root.exists():
        return []
    return sorted(root.rglob("*.json"))


def _write_file_sync(path: Path, data: bytes) -> None:
    """Atomic write via `tmp + fsync + rename`.

    POSIX rename is atomic on the same filesystem. Creating the
    parent directory is idempotent (mkdir parents=True,
    exist_ok=True). The file descriptor is closed in `finally` so
    a write error doesn't leak the FD even before the rename
    happens.

    File mode `0o600` (owner read+write only) — the queue contains
    click records with PII (IP, geo, full UA, query_params that
    may carry advertiser-supplied identifiers). World-readable
    `0o644` would let any co-located process / sidecar / shared
    bind-mount read every queued click during a Redis outage.
    Parent directory `0o700` mirrors the same boundary at the dir
    level (a user-listable parent leaks filenames, which encode
    timestamps + UUIDs). Closes Agent 2 HIGH-2 audit finding
    (security review 2026-05-09).
    """
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    # `mkdir(mode=...)` only honours mode on FIRST creation —
    # subsequent calls with exist_ok=True don't tighten an
    # already-loose dir. Defensive chmod ensures the boundary
    # holds even when the directory pre-existed (e.g., previous
    # process ran with a more permissive umask).
    try:
        os.chmod(path.parent, 0o700)
    except OSError as exc:
        # M7 fix (2026-05-11): WAS silent `pass`. The earlier
        # rationale (file itself written at 0o600 still protects
        # the click bytes) holds, BUT operators had no signal when
        # the directory perms drifted — e.g., a hostile co-tenant
        # owning the path. Log + Sentry-capture so the operator
        # sees the misconfig. Still non-fatal: the file write is
        # the load-bearing protection; the dir chmod is hygiene.
        logger.warning(
            "Disk queue: chmod 0o700 failed on %s — directory may be "
            "world-readable. The queued click file is still written at "
            "0o600 so PII bytes stay protected, but verify directory "
            "ownership: %s",
            path.parent, exc,
        )
        sentry_sdk.capture_message(
            f"Disk-queue chmod failed on {path.parent}: {exc}",
            level="warning",
        )
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    fd = os.open(tmp_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.write(fd, data)
        os.fsync(fd)
    finally:
        os.close(fd)
    os.rename(tmp_path, path)


async def _ensure_initialized() -> None:
    """Lazy first-time initialization of the in-memory size counter.

    Single FS scan, guarded by an asyncio.Lock so concurrent
    enqueues during the first outage don't all trigger their own
    rglob (which would defeat the optimization that motivated this
    counter in the first place).
    """
    global _queue_size, _queue_size_initialized
    if _queue_size_initialized:
        return
    async with _get_init_lock():
        if _queue_size_initialized:
            return
        _queue_size = await asyncio.to_thread(_count_queue_files_sync)
        _queue_size_initialized = True
        logger.info(
            "Disk queue initialized — %d existing file(s) at %s",
            _queue_size, _queue_root(),
        )


def _decrement_size() -> None:
    """Decrement counter, never below zero. Counter underflow
    would only happen if drain saw a file that enqueue didn't
    count (e.g., file placed externally) — we don't want to track
    a negative value, so we clamp."""
    global _queue_size
    _queue_size = max(0, _queue_size - 1)


async def get_queue_size() -> int:
    """Return current queue size — safe to call from /health or
    metrics emitters. Triggers lazy init on first call."""
    await _ensure_initialized()
    return _queue_size


def check_disk_pressure() -> tuple[bool, int | None]:
    """Synchronous pre-flight check for disk-queue mountpoint capacity.

    F.29 Sprint 1.5 (2026-05-23). Returns a ``(is_pressured, free_bytes)``
    tuple. The caller (``/decide`` handler in ``main.py``) uses this
    BEFORE attempting :func:`enqueue_click` so a known-saturated mount
    surfaces as a visible 503 rather than getting noticed only after
    multiple write failures.

    Policy:

    * Local env (``environment ∈ _LOCAL_ENVIRONMENTS``) → always
      return ``(False, free_or_None)``. Engineers may have small
      dev partitions and the disk-fallback path isn't exercised in
      dev anyway. Free bytes still returned for /health visibility.
    * ``disk_queue_root`` empty or non-existent →
      ``(False, None)``. Operator disabled disk fallback entirely;
      no pressure to surface, no path to read.
    * Otherwise → ``(free_bytes < disk_queue_min_free_bytes, free_bytes)``.

    Synchronous because ``shutil.disk_usage`` is a single syscall
    (~µs). Wrapping in ``asyncio.to_thread`` would add overhead
    without latency benefit; the call site is the slow path
    (post-XADD-failure), not the hot routing path.

    Returns:
        Tuple of ``(is_pressured, free_bytes)``. ``free_bytes`` is
        ``None`` only when ``disk_queue_root`` cannot be read (path
        absent, permission error). When ``None``, ``is_pressured`` is
        always ``False`` (cannot determine pressure → don't block).
    """
    if not settings.disk_queue_root:
        # Operator opted out of disk fallback (TDS_DISK_QUEUE_ROOT="").
        # No pressure to surface; downstream caller will see
        # enqueue_click return False on the cap path and handle
        # accordingly.
        return False, None

    try:
        usage = shutil.disk_usage(settings.disk_queue_root)
    except (OSError, FileNotFoundError) as exc:
        # Root path doesn't exist yet on first boot (the parent dir
        # gets created lazily by ``_write_file_sync.mkdir``). The
        # absence itself isn't pressure — it just means we cannot
        # measure. Log at DEBUG only (not WARNING) because this is
        # the expected state on a brand-new node before the first
        # disk-fallback fires.
        logger.debug(
            "Disk-queue pressure check: %s unreadable (%s) — "
            "treating as 'cannot measure', no pressure.",
            settings.disk_queue_root, exc,
        )
        return False, None

    free_bytes = usage.free

    # Local env: always report "not pressured" so dev partitions
    # don't trigger 503s on a laptop. Still return the free_bytes so
    # /health surfaces the value (operator might want to see it
    # even in dev).
    if settings.environment in _LOCAL_ENVIRONMENTS:
        return False, free_bytes

    threshold = settings.disk_queue_min_free_bytes
    is_pressured = free_bytes < threshold
    return is_pressured, free_bytes


async def enqueue_click(record: dict) -> bool:
    """Persist a click record to the disk queue.

    Returns True on success (file written, atomic, durable).
    Returns False on cap rejection or write failure (the click is
    LOST in this case — the caller is expected to log CRITICAL +
    Sentry capture so the incident is visible).

    Caller contract: this function is intended for the XADD-failure
    fallback path on `/decide`. It is NOT a primary write path —
    the steady state writes directly to Redis. So the cap is sized
    for outage duration, not click volume × seconds.
    """
    global _queue_size
    await _ensure_initialized()

    cap = settings.disk_queue_max_files
    if cap > 0 and _queue_size >= cap:
        logger.error(
            "Disk queue at cap (%d ≥ %d) — DROPPING click. "
            "Resolve Redis outage or raise TDS_DISK_QUEUE_MAX_FILES.",
            _queue_size, cap,
        )
        sentry_sdk.capture_message(
            f"Disk-queue cap reached ({_queue_size} ≥ {cap}); "
            "click rejected.",
            level="error",
        )
        return False

    try:
        payload = json.dumps(record, default=str).encode("utf-8")
    except (TypeError, ValueError) as exc:
        logger.error("Failed to serialize click for disk queue: %s", exc)
        sentry_sdk.capture_exception(exc)
        return False

    path = _today_dir() / _new_filename()
    try:
        await asyncio.to_thread(_write_file_sync, path, payload)
    except OSError as exc:
        logger.error("Failed to enqueue click to disk: %s", exc)
        sentry_sdk.capture_exception(exc)
        return False

    _queue_size += 1
    return True


async def drain_to_redis(redis) -> dict:
    """Replay queued clicks back into the Redis stream.

    Stops at the FIRST XADD failure — no point pounding a Redis
    that's still impaired. The remaining files stay on disk and
    will be retried by the next drainer iteration. Successful
    drains delete the file and decrement the in-memory counter.

    Returns stats dict for caller logging:
      - drained: number of files successfully replayed + deleted
      - skipped: files that vanished mid-drain (race with another
        drainer or manual cleanup)
      - failed: 1 if the loop broke on Redis error, else 0
      - remaining: best-effort count of files still on disk
    """
    await _ensure_initialized()

    files = await asyncio.to_thread(_list_queue_files_sync)
    drained = 0
    skipped = 0
    failed = 0

    for path in files:
        try:
            data = await asyncio.to_thread(path.read_bytes)
        except FileNotFoundError:
            # Race — another drainer or manual cleanup grabbed it.
            skipped += 1
            continue
        except OSError as exc:
            logger.warning("Failed to read queued click %s: %s", path.name, exc)
            sentry_sdk.capture_exception(exc)
            skipped += 1
            continue

        # H1 fix (2026-05-11): apply the same idempotency gate on the
        # replay path. Without this, a Redis outage followed by recovery
        # would produce a stream entry for every queued file — but if
        # the original /decide also succeeded on a different node (the
        # race that motivated H1 in the first place), the replay
        # introduces a duplicate. The TTL is the same operator-tuned
        # `click_dedup_ttl_seconds`. Failure to extract click_id from
        # the payload (corrupt file) is unlikely (we wrote it ourselves
        # atomically) but we fail-open in that case — better to replay
        # a possibly-duplicate than lose a click in a Redis-outage tail.
        click_id_for_dedup: str | None = None
        if settings.click_dedup_ttl_seconds > 0:
            try:
                parsed = json.loads(data)
                cid = parsed.get("click_id")
                if isinstance(cid, str) and cid:
                    click_id_for_dedup = cid
            except (json.JSONDecodeError, AttributeError) as exc:
                logger.warning(
                    "Drain: failed to parse click_id from %s for dedup: %s — "
                    "proceeding without dedup",
                    path.name, exc,
                )
        if click_id_for_dedup is not None:
            try:
                acquired = await redis.set(
                    f"click:seen:{click_id_for_dedup}",
                    "1",
                    nx=True,
                    ex=settings.click_dedup_ttl_seconds,
                )
                if not acquired:
                    # Duplicate — original /decide already wrote to
                    # stream. Unlink the queued file and skip XADD;
                    # counter still decrements (file is gone, no
                    # accounting drift).
                    #
                    # VF1 fix (2026-05-11 code-review cycle):
                    # increment `skipped`, NOT `drained`. The
                    # docstring of `drain_to_redis` says drained =
                    # "files successfully replayed + deleted" — but
                    # a duplicate is NOT replayed (we INTENTIONALLY
                    # skip the XADD). Bucketing it under `drained`
                    # inflated the success-rate metric and made the
                    # `skipped` counter misrepresent reality during
                    # any post-Redis-outage replay where the
                    # original /decide had also succeeded on a
                    # sibling node.
                    logger.info(
                        "Drain: duplicate click_id %s (already in stream) — "
                        "dropping queued file %s",
                        click_id_for_dedup, path.name,
                    )
                    try:
                        await asyncio.to_thread(path.unlink)
                    except FileNotFoundError:
                        pass
                    except OSError as exc:
                        logger.warning(
                            "Drain: unlink failed after duplicate-skip on %s: %s",
                            path.name, exc,
                        )
                    _decrement_size()
                    skipped += 1
                    continue
            except Exception as exc:  # noqa: BLE001 — Redis impaired
                # Dedup failed → fail-open to legacy behaviour. The XADD
                # below may still raise (Redis impaired); the existing
                # error handling at line ~324 stops the drain loop.
                logger.warning(
                    "Drain: SETNX failed for %s: %s — proceeding without dedup",
                    click_id_for_dedup, exc,
                )

        try:
            await redis.xadd(
                "stream:clicks",
                {"data": data.decode("utf-8")},
                maxlen=settings.stream_clicks_maxlen,
                approximate=True,
            )
        except Exception as exc:  # noqa: BLE001 — broad on purpose
            # XADD raised → Redis still impaired. Stop the drain
            # loop. Don't unlink the file — next iteration retries.
            logger.warning(
                "Drain stopped at %s — Redis still impaired: %s",
                path.name, exc,
            )
            failed = 1
            break

        # XADD succeeded — file is safe to remove. The unlink
        # outcome determines counter accounting:
        #   FileNotFoundError → another drainer/cleanup got there
        #     first; treat as drained (data in Redis, file gone).
        #   OSError (e.g. permission denied, ENOSPC during journal
        #     update on full disk) → the file STAYS on disk; counter
        #     MUST NOT decrement, otherwise it underestimates real
        #     queue depth and the cap check lets through more
        #     enqueues than the disk holds. Skip increment of
        #     `drained` too — the next drain cycle re-attempts
        #     XADD (idempotent if click_id-deduped downstream) and
        #     unlink. Closes Agent 1 HIGH-2 audit finding
        #     (code review 2026-05-09).
        try:
            await asyncio.to_thread(path.unlink)
        except FileNotFoundError:
            _decrement_size()
            drained += 1
        except OSError as exc:
            logger.warning(
                "XADD-OK but unlink failed for %s: %s — file kept "
                "on disk, will be retried by next drainer iteration",
                path.name, exc,
            )
            sentry_sdk.capture_exception(exc)
            # Counter NOT decremented; `drained` NOT incremented.
            # Move on to the next file — don't break the loop, this
            # is a per-file unlink issue, not a Redis impairment.
            continue
        else:
            _decrement_size()
            drained += 1

    return {
        "drained": drained,
        "skipped": skipped,
        "failed": failed,
        "remaining": max(0, len(files) - drained - skipped),
    }


async def run_drainer(redis, interval: int | None = None) -> None:
    """Background task: periodic drain attempt.

    Started in the FastAPI lifespan, cancelled on shutdown. Robust
    to per-iteration errors — a transient failure in one round
    doesn't kill the loop.
    """
    if interval is None:
        interval = settings.disk_queue_drain_interval_seconds
    logger.info("Disk-queue drainer started (interval=%ds)", interval)

    while True:
        try:
            await asyncio.sleep(interval)
            stats = await drain_to_redis(redis)
            if stats["drained"] > 0 or stats["failed"] > 0:
                logger.info(
                    "Disk-queue drain: %d drained, %d skipped, "
                    "%d failed, %d remaining",
                    stats["drained"], stats["skipped"],
                    stats["failed"], stats["remaining"],
                )
        except asyncio.CancelledError:
            logger.info("Disk-queue drainer cancelled — shutting down")
            raise
        except Exception:  # noqa: BLE001
            logger.exception("Disk-queue drainer iteration failed")


# ---------------------------------------------------------------------------
# Test hooks — for unit tests that need to reset module state.
# ---------------------------------------------------------------------------


def _reset_state_for_tests() -> None:
    """Reset module-level state. Called from test fixtures only —
    NOT for production use. Production lifecycle is single-init,
    no reset.
    """
    global _queue_size, _queue_size_initialized, _init_lock
    _queue_size = 0
    _queue_size_initialized = False
    _init_lock = None
