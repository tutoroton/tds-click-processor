"""Disk fallback queue for click writes (T2.2 / G-23), REDESIGNED by P2
(LOSSFIX, 2026-07-07) into a segment engine.

The original design (one atomically-written `.json` file per click) was
fine at a trickle but pathological under a sustained outage: an hour at
even modest click volume produced tens of thousands of files in one
directory (inode exhaustion risk, `rglob` cost climbing with file
count) and ~1 `fsync` per click. This module replaces it with append-
only NDJSON SEGMENTS shared by many clicks, group-commit fsync, and a
per-worker ownership model so `WEB_CONCURRENCY` > 1 workers never race
the same file.

Segment lifecycle
------------------

  1. A worker opens `{boot_epoch}-{pid}-{seq:06d}.ndjson.wip` and
     APPENDS one JSON line per click (`_SegmentWriter.append`).
  2. Concurrent `append()` awaiters within `disk_segment_group_commit_ms`
     share ONE `fsync` (group commit) — the await only resolves once
     that shared fsync lands, so redirect-latency stays bounded even
     though multiple clicks' writes are batched.
  3. On a size/age threshold the segment FINALIZES: fd closed, renamed
     `.wip` -> plain `.ndjson` (this rename is the durability boundary —
     a plain `.ndjson` name is used by NOTHING until it is safe to
     read), then the PARENT DIRECTORY is fsynced (D2) so the rename
     itself survives a power-loss.
  4. The drainer (`drain_to_redis`) only ever globs plain `.ndjson`
     files — a `.wip` (still being written) segment is invisible to it
     by construction, no in-memory writer/drainer coordination needed.
  5. Each segment gets a REPLAY-OFFSET sidecar (`{name}.offset`),
     persisted every `disk_replay_offset_batch_lines` lines (gate-E
     perf fix, 2026-07-07 — was every single line, correct but
     expensive under a large backlog) so a CRASH mid-replay re-does at
     most that many lines, never the whole segment (B3) — a Redis-
     impairment break (process stays alive) still flushes immediately,
     no batching window there. Once every line replays, the segment +
     its sidecar are deleted together.

Orphan adoption (B1)
---------------------

`WEB_CONCURRENCY` > 1 means N sibling worker PROCESSES, each with its
own `(boot_epoch, pid)`. A worker that crashes/restarts gets a NEW pid
-> its old segments belong to nobody -> stranded forever unless someone
adopts them. `adopt_orphan_segments()` runs at worker startup AND on
every periodic drainer cycle (gate-E fix, 2026-07-07 — was one-shot-
at-boot only, which meant a worker that crash-restarted WITHIN the
min-age grace window, or an ADOPTER that itself died before draining
what it just claimed, stayed stranded until some unrelated future
restart — possibly days on a stable node). Any segment prefix that
isn't this worker's own AND is older than
`disk_orphan_adopt_min_age_seconds` (so a same-boot sibling still
starting up is never mistaken for a corpse) is claimed via an atomic
`os.rename` per file — POSIX rename requires the SOURCE to exist, so if
two workers race to adopt the same orphan, exactly one wins (the
other's rename raises `FileNotFoundError` and it moves on). A recovered
`.wip` file (the dead worker's segment was still open at crash time)
gets its torn tail truncated first (B2) before being adopted as a
finalized segment.

RE-ADOPTION (multi-hop) — an adopted segment's new name encodes the
CURRENT owner's prefix + the segment's ORIGINAL identity, e.g.
`{owner}-adopted-{origin_epoch}-{origin_pid}-{seq}.ndjson`. If THAT
owner ALSO dies before draining it, discovery (`_SEGMENT_RE`, extended
to recognise this shape) still finds it — the ORIGIN is what gets
carried forward unchanged on every hop; only the leading owner-prefix
is replaced. This bounds the filename length regardless of how many
times a segment gets re-orphaned and re-adopted during an extended
outage — it does NOT grow with hop count.

Mechanical liveness (gate-E round 2 CRITICAL fix, 2026-07-07)
---------------------------------------------------------------

The age check above (`disk_orphan_adopt_min_age_seconds`) is a cheap
pre-filter, NOT proof of death: the epoch in a segment's NAME is the
writer's BOOT time, not the file's age, so ANY live sibling older than
that floor (i.e. every sibling past its first ~30s of life) looked
"dead" to age alone. Combined with the periodic-retry HIGH fix (round
1), this became CONTINUOUS mass live-sibling theft under sustained
WC=8 spill — including OPEN `.wip` theft, which is silent loss TWICE:
(i) a torn-tail truncate mid-flight on a file the owner is still
actively appending to, dropping an already-fsynced, already-
acknowledged write; (ii) the owner's own subsequent appends (its fd
still open, O_APPEND follows the inode) vanishing into an unlinked
inode once the thief drains + deletes the renamed file.

Every worker refreshes its OWN `{prefix}.alive` heartbeat file's mtime
every `run_drainer` cycle (`_touch_heartbeat_sync`) AND at the moment
it opens its FIRST segment (`_ensure_open_sync`) — so a segment file
for a prefix can never exist on disk before that SAME prefix's
heartbeat file does. `_worker_is_dead_sync` is the sole gate: a
candidate orphan prefix is adopted ONLY if its heartbeat is MISSING (a
pre-this-fix orphan whose segments already cleared the age floor — a
live current-code worker can never be in that state, since its
heartbeat always exists by the time its first segment does) OR its
heartbeat is older than `disk_orphan_heartbeat_stale_multiplier *
disk_queue_drain_interval_seconds`. This survives pid recycling for
free: an unrelated NEW process reusing a dead worker's old pid writes
its OWN fresh prefix + heartbeat, never touching the dead one's file.
The SAME check gates BOTH `.wip` and finalized-segment adoption
uniformly — a live worker's finalized-but-not-yet-drained segments are
never touched either, closing a subtler race against that worker's
OWN concurrent drain cycle.

Idle-tail finalize + rotation-failure containment (gate-E round 2 HIGH
fixes, 2026-07-07)
--------------------------------------------------------------------------

A `.wip` segment used to finalize ONLY as a side effect of a LATER
commit's rotate check (`_maybe_rotate_sync`, called from
`_commit_batch_sync`) — a spill burst followed by quiet left the FINAL
segment open INDEFINITELY: its clicks were fsync'd + acknowledged, but
invisible to the drainer (globs `.ndjson` only) until the next burst
or a restart. `run_drainer`'s cycle now also calls `_SegmentWriter.
finalize_if_stale()`, which finalizes an idle-past-
`disk_segment_max_age_seconds` `.wip` even with zero new appends.
Because this runs from an INDEPENDENT asyncio task (not the writer's
own flush-loop task), it serializes with `_commit_batch_sync` via
`_state_lock` — the "only one `_commit_batch_sync` in flight" note
above no longer holds unmodified once a second call site touches the
same fd/path state.

`_finalize_current_sync` itself now NEVER raises: a rename/dir-fsync
failure during rotation (ENOSPC/EIO) re-opens the SAME `.wip` path in
append mode (`_reopen_after_failed_finalize_sync`) instead of
stranding its already-durable bytes under a cleared fd/path. Before
this, a rotation OSError propagated out of `_commit_batch_sync`,
through `asyncio.to_thread`, and out of `_run_flush_loop` BEFORE it
resolved the batch's futures or reset `_flush_task` — hanging every
pending `/decide` call on the node AND permanently wedging the flush
loop (no future append ever spawns a new one again, since `append()`
only spawns when `_flush_task is None`). `_run_flush_loop` also now
wraps its whole body in try/finally (always resets `_flush_task`) with
a defensive except around the commit call (resolves the batch
`ok=False` rather than hanging forever) as a second, independent line
of defense against any OTHER future exception in this path.

Legacy migration (D1)
----------------------

Any pre-P2 `*.json` per-click file still on disk at upgrade time is
drained by the SAME per-cycle sweep (`_drain_legacy_json_files`) so an
upgrade never strands them — this path naturally self-obsoletes once a
node's legacy backlog empties.

Reference: rule `sync-protocol`, action-items.md T2.2, open-questions.md
G-23; collector's `app/watermark.py` for the sibling P2 c3 pattern.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import shutil
import time
from pathlib import Path

import sentry_sdk

from app.config import _LOCAL_ENVIRONMENTS, settings
from app.telemetry import (
    OP_SEGMENT_BYTE_CAP,
    OP_SEGMENT_ORPHAN_ADOPTED,
    OP_SEGMENT_TORN_TAIL,
    capture_op_msg,
)

logger = logging.getLogger("tds.disk_queue")

# `{epoch}-{pid}-{seq}.ndjson` — the FINALIZED segment name. The active
# (still being written) form carries an extra `.wip` suffix and is never
# matched by this regex or by the drainer's glob.
#
# gate-E CRITICAL fix (2026-07-07): the optional `-adopted-{orig_epoch}-
# {orig_pid}-` infix recognises an ADOPTED segment's name. `epoch`/`pid`
# always identify the CURRENT owner (whoever most recently
# adopted it, or the original writer if never adopted); `orig_epoch`/
# `orig_pid` (present only after at least one adoption hop) identify
# the segment's ORIGINAL writer and are carried forward UNCHANGED on
# every re-adoption hop — only the leading owner-prefix ever changes.
# Without this, an adopted segment's name (which used to embed the
# FULL previous name as a plain, non-numeric infix) never matched this
# regex at all, making it permanently invisible to every FUTURE orphan
# scan the instant its adopter died before draining it — the exact
# stranding class this subsystem exists to prevent.
_SEGMENT_RE = re.compile(
    r"^(?P<epoch>\d+)-(?P<pid>\d+)"
    r"(?:-adopted-(?P<orig_epoch>\d+)-(?P<orig_pid>\d+))?"
    r"-(?P<seq>\d{6})\.ndjson$"
)


def _queue_root() -> Path:
    """Resolve the queue root path. Resolution is per-call so a test
    that monkeypatches settings.disk_queue_root sees the new value
    without restart."""
    return Path(settings.disk_queue_root)


# ---------------------------------------------------------------------------
# Worker identity — `{boot_epoch}-{pid}` naming (B1)
# ---------------------------------------------------------------------------

_boot_epoch: int | None = None


def _get_boot_epoch() -> int:
    """Lazily memoized once per process. Combined with `os.getpid()`
    this disambiguates PID RECYCLING across restarts: a bare pid alone
    collides once the OS reuses it for an unrelated later process; the
    epoch (this process's own start time) makes the pair unique-enough
    without requiring any cross-worker coordination at boot."""
    global _boot_epoch
    if _boot_epoch is None:
        _boot_epoch = int(time.time())
    return _boot_epoch


def _worker_prefix() -> str:
    return f"{_get_boot_epoch()}-{os.getpid()}"


# ---------------------------------------------------------------------------
# Disk-pressure preflight — UNCHANGED by P2 (orthogonal to the segment
# format; still a free-bytes floor check called from main.py before
# `enqueue_click`).
# ---------------------------------------------------------------------------


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
        # gets created lazily by the segment writer). The absence
        # itself isn't pressure — it just means we cannot measure.
        # Log at DEBUG only (not WARNING) because this is the
        # expected state on a brand-new node before the first
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


# ---------------------------------------------------------------------------
# Cached queue stats (byte-cap gate + /health, D3) — a CHEAP periodic scan,
# never a per-append live scan. Affordable because a healthy node holds
# hundreds of segments, not the old design's potential millions of files.
# ---------------------------------------------------------------------------

_cached_queue_stats: dict | None = None


def _scan_queue_stats_sync() -> dict:
    root = _queue_root()
    segments = 0
    total_bytes = 0
    oldest_mtime: float | None = None
    if root.exists():
        for pattern in ("*.ndjson", "*.json"):
            for f in root.rglob(pattern):
                try:
                    st = f.stat()
                except OSError:
                    continue
                segments += 1
                total_bytes += st.st_size
                if oldest_mtime is None or st.st_mtime < oldest_mtime:
                    oldest_mtime = st.st_mtime
    oldest_seconds = (time.time() - oldest_mtime) if oldest_mtime is not None else None
    return {"segments": segments, "bytes": total_bytes, "oldest_seconds": oldest_seconds}


def get_cached_queue_stats() -> dict:
    """Cached `{segments, bytes, oldest_seconds}` for /health (D3) and
    the byte-cap gate. Never-sampled returns a zeroed dict — safe
    default for a fresh boot before the sampler's first tick."""
    if _cached_queue_stats is None:
        return {"segments": 0, "bytes": 0, "oldest_seconds": None}
    return _cached_queue_stats


def _check_byte_cap() -> bool:
    """True = at/over the global byte-cap, new appends must be rejected.

    Fail-open on a never-sampled cache (same discipline as the M1
    stream-length gate — a missing signal must never itself become a
    new failure mode) and when the cap is disabled (<=0)."""
    cap = settings.disk_segment_max_total_bytes
    if cap <= 0:
        return False
    if _cached_queue_stats is None:
        return False
    return _cached_queue_stats["bytes"] >= cap


async def run_queue_stats_sampler(interval: float | None = None) -> None:
    """Periodic full scan of the queue root, caching `{segments, bytes,
    oldest_seconds}`. Started in the FastAPI lifespan, cancelled on
    shutdown — mirrors `run_observability_loop` / the collector's
    `run_watermark_sampler`. A transient scan failure just leaves the
    cache aging (fails open on the byte-cap gate); it never kills the
    loop."""
    interval = interval or settings.disk_queue_stats_scan_interval_seconds
    logger.info("Disk-queue stats sampler started (interval=%ss)", interval)
    global _cached_queue_stats
    while True:
        try:
            await asyncio.sleep(interval)
            _cached_queue_stats = await asyncio.to_thread(_scan_queue_stats_sync)
        except asyncio.CancelledError:
            logger.info("Disk-queue stats sampler cancelled — shutting down")
            raise
        except Exception:  # noqa: BLE001
            logger.exception("Disk-queue stats sampler iteration failed")


async def get_queue_stats() -> dict:
    """Async accessor for /health — returns the cached stats, forcing a
    one-shot scan first if the sampler hasn't ticked yet (so /health
    right after boot isn't stuck reporting zeros for a full interval)."""
    global _cached_queue_stats
    if _cached_queue_stats is None:
        _cached_queue_stats = await asyncio.to_thread(_scan_queue_stats_sync)
    return _cached_queue_stats


# ---------------------------------------------------------------------------
# Segment writer — group-commit append (c1)
# ---------------------------------------------------------------------------


class _SegmentWriter:
    """Owns ONE actively-open NDJSON segment for THIS worker process.

    `append()` is the group-commit entrypoint: concurrent awaiters that
    land while a flush loop is already running share the SAME batch (or
    the next one, if their arrival lost the race) — bounding the number
    of `fsync` calls under load while keeping added latency close to
    `disk_segment_group_commit_ms`. Only ONE `_commit_batch_sync` ever
    runs at a time (the flush loop below never spawns a second
    concurrent thread call), so the file-handle/size/rotation state
    below needs no lock beyond that invariant.
    """

    def __init__(self) -> None:
        self._fd: int | None = None
        self._path: Path | None = None
        self._size = 0
        self._opened_monotonic = 0.0
        self._seq = 0
        self._pending: list[tuple[bytes, asyncio.Future]] = []
        self._flush_task: asyncio.Task | None = None
        # gate-E round 2 HIGH fix (2026-07-07) — serializes
        # `_commit_batch_sync` against `finalize_if_stale()`, which runs
        # from run_drainer's INDEPENDENT asyncio task, not this writer's
        # own flush-loop task. Uncontended in the common case (only
        # `_run_flush_loop` ever holds it during normal operation) — this
        # is intra-process coordination for one writer instance, not the
        # forbidden cross-worker lock on the hot append path.
        self._state_lock = asyncio.Lock()

    @property
    def current_wip_path(self) -> Path | None:
        return self._path

    async def append(self, record: dict) -> bool:
        if _check_byte_cap():
            logger.error(
                "Disk-segment byte-cap reached (>=%d bytes) — DROPPING "
                "click. Resolve the outage or raise "
                "TDS_DISK_SEGMENT_MAX_TOTAL_BYTES.",
                settings.disk_segment_max_total_bytes,
            )
            capture_op_msg(
                OP_SEGMENT_BYTE_CAP,
                f"Disk-segment byte-cap reached "
                f"(>={settings.disk_segment_max_total_bytes} bytes); "
                "click rejected.",
                level="error",
            )
            return False

        try:
            line = json.dumps(record, default=str).encode("utf-8") + b"\n"
        except (TypeError, ValueError) as exc:
            logger.error("Failed to serialize click for disk segment: %s", exc)
            sentry_sdk.capture_exception(exc)
            return False

        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        self._pending.append((line, fut))
        if self._flush_task is None:
            self._flush_task = asyncio.create_task(
                self._run_flush_loop(settings.disk_segment_group_commit_ms / 1000.0)
            )
        return await fut

    async def _run_flush_loop(self, initial_delay: float) -> None:
        if initial_delay > 0:
            await asyncio.sleep(initial_delay)
        try:
            while True:
                batch = self._pending
                self._pending = []
                try:
                    async with self._state_lock:
                        ok, exc = await asyncio.to_thread(self._commit_batch_sync, batch)
                except Exception as loop_exc:  # noqa: BLE001 — gate-E round 2 HIGH
                    # fix (2026-07-07): defense-in-depth. `_commit_batch_
                    # sync` (and `_finalize_current_sync` it calls into via
                    # rotation) is now designed to never raise, but if
                    # something ELSE ever escapes here, the batch's
                    # futures must STILL resolve — a caller `await`-ing its
                    # future must never hang forever just because the
                    # commit machinery had a bug. ok=False is the honest
                    # answer: we cannot prove this batch is durable.
                    logger.exception(
                        "Segment group-commit loop iteration crashed "
                        "unexpectedly — resolving this batch as failed "
                        "rather than leaving its callers hanging forever."
                    )
                    sentry_sdk.capture_exception(loop_exc)
                    ok, exc = False, loop_exc
                for _, fut in batch:
                    if not fut.done():
                        fut.set_result(ok)
                if not ok:
                    logger.error("Segment group-commit failed: %s", exc)
                    sentry_sdk.capture_message(
                        f"Disk-segment group-commit failed: {exc}", level="error",
                    )
                if not self._pending:
                    break
                # More arrived while we were mid-commit — they already
                # waited through this commit; drain them immediately
                # rather than making them wait through ANOTHER full linger.
        finally:
            # gate-E round 2 HIGH fix (2026-07-07): ANY exit from this
            # loop — the normal break above, or an exception escaping the
            # try — MUST reset `_flush_task` so the NEXT append() spawns
            # a fresh flush loop. Before this fix, an exception raised
            # before reaching the (until-now unconditional) tail
            # assignment left `_flush_task` permanently non-None —
            # `append()` only spawns a new loop when `_flush_task is
            # None`, so every future append on this worker queued into
            # `self._pending` with nothing left to ever drain it: a
            # node-wide hang of the entire real-click spill/divert path
            # until restart.
            self._flush_task = None

    def _commit_batch_sync(self, batch: list[tuple[bytes, asyncio.Future]]):
        try:
            self._ensure_open_sync()
            for line, _ in batch:
                os.write(self._fd, line)
                self._size += len(line)
            os.fsync(self._fd)
        except OSError as exc:
            # gate-E MEDIUM fix (2026-07-07) — never leave the fd open
            # for a later commit to keep writing into / eventually
            # fsync-through (see `_abandon_current_on_error_sync`).
            self._abandon_current_on_error_sync()
            return False, exc
        # This batch's writes+fsync above are ALREADY durable — nothing
        # below can un-succeed it. `_maybe_rotate_sync` -> `_finalize_
        # current_sync` is designed to NEVER raise (gate-E round 2 HIGH
        # fix — see that function's docstring), so no try/except is
        # needed here; a rotation hiccup self-heals on the writer's own
        # state without ever risking this batch's callers.
        self._maybe_rotate_sync()
        return True, None

    def _ensure_open_sync(self) -> None:
        if self._fd is not None:
            return
        root = _queue_root()
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        try:
            os.chmod(root, 0o700)
        except OSError as exc:
            # M7-style defensive chmod (2026-05-11 fix, carried over):
            # non-fatal — the file itself is still opened at 0o600
            # below — but a drifted directory permission must not be
            # silently swallowed.
            logger.warning(
                "Disk queue: chmod 0o700 failed on %s — directory may be "
                "world-readable. Segment files are still written at "
                "0o600: %s", root, exc,
            )
            sentry_sdk.capture_message(
                f"Disk-queue chmod failed on {root}: {exc}", level="warning",
            )
        # gate-E round 2 CRITICAL fix (2026-07-07) — establish this
        # worker's proof-of-life BEFORE (same call, so effectively
        # atomically-enough with) its FIRST segment file appears on disk.
        # This closes the boot-race window: a segment file for a prefix
        # can never exist without that SAME prefix's heartbeat file also
        # existing, so a peer's orphan scan can never mistake a brand-new,
        # genuinely-alive worker for a dead one (see `_worker_is_dead_sync`
        # docstring). `run_drainer`'s own periodic touch keeps it fresh
        # afterwards even during a quiet (no-new-segment) stretch.
        _touch_heartbeat_sync()
        self._seq += 1
        name = f"{_worker_prefix()}-{self._seq:06d}.ndjson.wip"
        path = root / name
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        self._fd = fd
        self._path = path
        self._size = 0
        self._opened_monotonic = time.monotonic()

    def _maybe_rotate_sync(self) -> None:
        due = (
            self._size >= settings.disk_segment_max_bytes
            or (time.monotonic() - self._opened_monotonic)
            >= settings.disk_segment_max_age_seconds
        )
        if due:
            self._finalize_current_sync()

    def _finalize_current_sync(self) -> None:
        """Close the active `.wip` fd, rename it to the plain `.ndjson`
        name (the durability boundary the drainer relies on), then
        fsync the PARENT DIRECTORY (D2) so that rename survives a
        power-loss. A file's own fsync guarantees its CONTENT is
        durable; it does NOT guarantee the directory entry (this
        filename existing at all) is durable without ALSO fsyncing the
        directory — this bounds the unsafe window to one group-commit,
        matching the design brief's stated invariant.

        gate-E round 2 HIGH fix (2026-07-07): this method NEVER raises.
        The ORIGINAL shape unconditionally nulled `self._fd`/`self._path`
        in a `finally` regardless of whether the rename/dir-fsync
        succeeded, THEN let the exception propagate — which had two
        compounding problems: (1) the exception escaped `_commit_batch_
        sync` -> `asyncio.to_thread` -> `_run_flush_loop` BEFORE it could
        resolve the in-flight batch's futures or reset `_flush_task`,
        hanging every pending caller and permanently wedging the flush
        loop (see `_run_flush_loop`'s own gate-E round 2 fix); (2) even
        with that contained, nulling `_fd`/`_path` on a FAILED rename
        abandons the segment's already-fsynced, already-acknowledged
        bytes under THIS worker's own (now live-protected, post-round-2
        CRITICAL fix) prefix — neither this worker's own drain (globs
        `.ndjson` only) nor orphan adoption (never touches a live prefix)
        would ever look at it again until this worker eventually
        restarts. Now: a rename/dir-fsync failure re-opens the SAME
        `.wip` path in append mode (`_reopen_after_failed_finalize_sync`)
        so the very next commit keeps writing there and the next
        rotation-eligible commit retries the finalize."""
        if self._fd is None:
            return
        fd = self._fd
        path = self._path
        self._fd = None
        self._path = None
        try:
            os.close(fd)
        except OSError as exc:
            logger.warning(
                "Failed to close fd for %s during finalize (continuing "
                "with the rename attempt regardless): %s", path, exc,
            )

        final_path = path.with_suffix("")  # strips ".wip"
        try:
            os.rename(path, final_path)
            _fsync_dir_sync(final_path.parent)
        except OSError as exc:
            logger.error(
                "Segment rotation/finalize failed for %s — its bytes "
                "are already fsync'd-durable at the .wip path; "
                "re-opening it so this worker keeps appending and "
                "retries rotation on the next eligible commit: %s",
                path, exc,
            )
            sentry_sdk.capture_exception(exc)
            self._reopen_after_failed_finalize_sync(path)

    def _reopen_after_failed_finalize_sync(self, path: Path) -> None:
        """A finalize attempt's rename (or the D2 dir-fsync) failed
        AFTER the fd was already closed. The file's CONTENT is
        untouched and still fully durable at `path` (still named
        `.wip` — `os.rename` is atomic, it either fully succeeds or
        leaves the source exactly as it was); re-opening it in append
        mode restores this worker's ownership so the next `append()`
        resumes writing here and the next rotation-eligible commit
        retries the finalize. `_size`/`_opened_monotonic` are left
        as-is (unchanged since they still describe this exact segment)
        — that also means the very next commit's rotate check sees it
        as already over-age/over-size and retries almost immediately,
        which is the desired behaviour."""
        try:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        except OSError as exc:
            logger.error(
                "Failed to re-open %s after a failed finalize — this "
                "worker will open a FRESH segment on the next append; "
                "the old .wip (already-durable data) needs operator "
                "attention if this worker doesn't restart soon: %s",
                path, exc,
            )
            sentry_sdk.capture_exception(exc)
            return
        self._fd = fd
        self._path = path

    async def finalize_if_stale(self) -> None:
        """gate-E round 2 HIGH fix (2026-07-07) — an idle `.wip`
        segment previously finalized ONLY as a side effect of a LATER
        commit's rotate check (`_maybe_rotate_sync`, called from
        `_commit_batch_sync`). A spill burst followed by quiet left
        the FINAL segment open INDEFINITELY: its clicks were fsync'd +
        302-acknowledged, but invisible to the drainer (globs
        `.ndjson` only) until the next burst (hours/days) or a
        restart — an unbounded delivery delay for already-
        acknowledged clicks. Called once per `run_drainer` cycle so an
        idle tail gets flushed within one drain interval.

        Runs from run_drainer's INDEPENDENT asyncio task, not this
        writer's own flush-loop task — MUST serialize with
        `_commit_batch_sync` via `_state_lock` (the "only one
        `_commit_batch_sync` in flight" invariant no longer protects
        against a concurrent commit once a second call site touches
        the same fd/path state)."""
        if self._path is None:
            return
        async with self._state_lock:
            if self._path is None:
                return
            idle_for = time.monotonic() - self._opened_monotonic
            if idle_for < settings.disk_segment_max_age_seconds:
                return
            await asyncio.to_thread(self._finalize_current_sync)

    def _abandon_current_on_error_sync(self) -> None:
        """gate-E MEDIUM fix (2026-07-07) — called when `_commit_batch_
        sync` hits an OSError. Two distinct problems, one fix:

        1. Leaving `self._fd` open would let the NEXT commit keep
           appending to (and eventually successfully fsync) a fd whose
           LAST write may have failed partway — silently un-failing a
           batch whose callers were just told `ok=False` (503'd), i.e.
           a click reappearing later after being told it was lost.
        2. Simply closing+nulling the fd WITHOUT salvaging its content
           would stash whatever an EARLIER, successfully-fsynced commit
           on this SAME fd had already durably written (real,
           acknowledged clicks) into a `.wip` file under THIS worker's
           OWN prefix — which neither this worker's own drain (globs
           only `.ndjson`) NOR orphan adoption (excludes its own
           prefix) will EVER touch again. That is exactly the
           permanently-stranded-acknowledged-click class this whole
           review round exists to close — reintroducing it via the
           "just close the fd" fix would defeat the point.

        So: truncate any torn tail (B2 — drops only what was never
        acknowledged) and FINALIZE the file under THIS worker's own
        name, exactly like a clean rotation would. Whatever is left
        after truncation re-enters the normal per-worker drain path
        with no orphan-adoption round-trip needed (it's still owned by
        the same worker) — durable data never sits in a `.wip` limbo
        that nothing will ever look at again."""
        path = self._path
        if self._fd is not None:
            try:
                os.close(self._fd)
            except OSError:
                pass
        self._fd = None
        self._path = None
        if path is None or not path.exists():
            return
        try:
            _truncate_torn_tail_sync(path)
            final_path = path.with_suffix("")  # strips ".wip"
            os.rename(path, final_path)
            _fsync_dir_sync(final_path.parent)
        except OSError as exc:
            logger.error(
                "Failed to salvage segment %s after a write error — any "
                "already-fsynced content in it may be stranded as a "
                ".wip file until an operator investigates: %s",
                path, exc,
            )
            sentry_sdk.capture_exception(exc)

    def force_finalize_for_tests(self) -> None:
        """Test-only — finalize whatever is open so a short-lived test
        doesn't leave a `.wip` segment sitting below the rotation
        threshold (and therefore invisible to the drainer) forever."""
        self._finalize_current_sync()


_writer: _SegmentWriter | None = None


def _get_writer() -> _SegmentWriter:
    global _writer
    if _writer is None:
        _writer = _SegmentWriter()
    return _writer


async def enqueue_click(record: dict) -> bool:
    """Persist a click record to the disk-segment queue.

    Returns True on success (durably group-committed to the active
    segment). Returns False on byte-cap rejection or a write failure —
    the caller (main.py) is expected to log CRITICAL + Sentry capture
    (or, on this ALSO failing, the L1 uncaptured-click 503) so the
    incident is visible.

    Caller contract: unchanged from pre-P2 — this is the XADD-failure
    (or M1 pre-emptive-divert) fallback path on `/decide`, not a
    primary write path.
    """
    return await _get_writer().append(record)


# ---------------------------------------------------------------------------
# Orphan adoption (B1, c2) — startup-only, atomic per-file rename claim.
# ---------------------------------------------------------------------------


def _truncate_torn_tail_sync(path: Path) -> int:
    """B2 — a recovered `.wip` segment may end in a TORN (incomplete or
    corrupt) last line: the write for it either never completed or was
    never fsynced before the crash/power-loss. Loss-free by
    construction: NOTHING that was ever acknowledged to a caller is
    discarded here — a group-commit only resolves its awaiters'
    futures AFTER its fsync lands, so if that fsync never happened, the
    `/decide` call for every click in that batch never returned
    success. Any WELL-FORMED (JSON-parseable, newline-terminated) line
    is left untouched and replayed normally — we don't need to (and
    can't) reconstruct group-commit batch boundaries at recovery time,
    only the true tail matters.

    Returns the number of bytes truncated (0 if the file was already
    clean).
    """
    try:
        data = path.read_bytes()
    except OSError:
        return 0
    if not data:
        return 0

    lines = data.split(b"\n")
    if data.endswith(b"\n"):
        lines = lines[:-1]  # trailing split artifact from the final \n

    # Walk back from the end, dropping any line that isn't valid JSON —
    # in practice this is at most the single last line (a mid-batch
    # crash can only corrupt the tail; everything before a completed
    # group-commit was already fsynced by an EARLIER commit).
    keep = len(lines)
    while keep > 0:
        try:
            json.loads(lines[keep - 1])
        except (json.JSONDecodeError, ValueError):
            keep -= 1
            continue
        break

    if keep == len(lines):
        return 0  # already clean

    truncated = b"\n".join(lines[:keep])
    if truncated:
        truncated += b"\n"
    dropped = len(data) - len(truncated)

    with open(path, "r+b") as f:
        f.truncate(len(truncated))
        f.seek(0)
        if truncated:
            f.write(truncated)
        f.flush()
        os.fsync(f.fileno())

    logger.warning(
        "B2: truncated a torn tail on recovered segment %s — dropped %d "
        "byte(s) from an unacknowledged (never-fsynced) write; loss-free "
        "by construction (the click(s) it represented never received a "
        "response).",
        path.name, dropped,
    )
    capture_op_msg(
        OP_SEGMENT_TORN_TAIL,
        f"Truncated torn tail on {path.name}: {dropped} byte(s) dropped "
        "(unacknowledged write, loss-free)",
        level="warning",
        segment=path.name,
        dropped_bytes=dropped,
    )
    return dropped


def _fsync_dir_sync(dir_path: Path) -> None:
    """D2 primitive — fsync a directory so a prior rename's directory-
    entry change survives a power-loss (a file's own fsync covers its
    CONTENT only, never the directory entry). Shared by segment
    finalize and orphan adoption."""
    dir_fd = os.open(dir_path, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


_HEARTBEAT_SUFFIX = ".alive"


def _heartbeat_path(root: Path, prefix: str) -> Path:
    return root / f"{prefix}{_HEARTBEAT_SUFFIX}"


def _touch_heartbeat_sync() -> None:
    """gate-E round 2 CRITICAL fix (2026-07-07) — mechanical proof-of-
    life for orphan-adoption gating. Called every `run_drainer` cycle
    AND at the moment THIS worker's `_SegmentWriter` opens its FIRST
    segment (`_ensure_open_sync`) — so a segment file for a prefix can
    never exist on disk before that SAME prefix's heartbeat file does.
    A sibling scanning for orphans (`_worker_is_dead_sync`) treats a
    heartbeat mtime younger than `disk_orphan_heartbeat_stale_
    multiplier * disk_queue_drain_interval_seconds` as PROOF this
    worker is alive — the file itself, not the boot-epoch encoded in
    segment names (the writer's START time, not a measure of current
    aliveness), is the source of truth. Survives pid recycling for
    free: an unrelated NEW process reusing an old dead worker's pid
    writes its OWN fresh prefix + heartbeat, never touching the dead
    one's file."""
    root = _queue_root()
    try:
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
    except OSError:
        return
    path = _heartbeat_path(root, _worker_prefix())
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT, 0o600)
        os.close(fd)
        os.utime(path, None)  # bump mtime to "now" even if it already existed
    except OSError as exc:
        logger.warning("Failed to refresh worker heartbeat %s: %s", path, exc)


def _worker_is_dead_sync(root: Path, prefix: str, now: float) -> bool:
    """gate-E round 2 CRITICAL fix (2026-07-07) — MECHANICAL liveness
    proof, replacing the age-inferred check that treated ANY live
    sibling older than `disk_orphan_adopt_min_age_seconds` (i.e. every
    sibling past its first ~30s of life) as "dead": the epoch in a
    segment NAME is the writer's BOOT time, not the file's age.
    Combined with the periodic-retry HIGH fix (round 1), age-alone
    turned into CONTINUOUS mass live-sibling theft under sustained
    WC=8 spill — including OPEN `.wip` theft, which is silent loss
    TWICE over (a torn-tail truncate on a file the owner is still
    actively appending to, then the owner's own subsequent appends
    vanishing into an unlinked inode once the thief drains + deletes
    the renamed file).

    A prefix is dead only if:
      * its heartbeat file is MISSING — this can only be a pre-this-
        fix orphan (a live current-code worker's heartbeat always
        exists at least as early as its first segment, so it can never
        reach this branch while genuinely alive), OR
      * its heartbeat is older than `disk_orphan_heartbeat_stale_
        multiplier * disk_queue_drain_interval_seconds`.

    Gates BOTH `.wip` and finalized-segment adoption uniformly — a
    live worker's finalized-but-not-yet-drained segments are never
    touched either, closing a subtler race against that worker's OWN
    concurrent drain cycle."""
    hb = _heartbeat_path(root, prefix)
    try:
        mtime = hb.stat().st_mtime
    except OSError:
        return True
    stale_after = (
        settings.disk_orphan_heartbeat_stale_multiplier
        * settings.disk_queue_drain_interval_seconds
    )
    return (now - mtime) >= stale_after


def _canonical_adopted_name(source_name: str, new_owner_prefix: str) -> str | None:
    """gate-E CRITICAL fix (2026-07-07) — the name a segment (or its
    `.wip` form, WITHOUT the `.wip` suffix — strip it before calling)
    should have after adoption by `new_owner_prefix`.

    The segment's ORIGIN identity (the worker that FIRST wrote it) is
    embedded ONCE and carried forward UNCHANGED across any number of
    re-adoption hops — only the LEADING (current-owner) prefix ever
    changes. This is what makes multi-hop adoption safe: a segment
    that gets re-orphaned (its adopter died before draining it) is
    STILL discoverable by `_SEGMENT_RE` (which recognises this exact
    shape) on the next scan, and the filename never grows with hop
    count — unlike embedding the FULL previous name at each hop, which
    both breaks discovery (a non-numeric `-adopted-` infix in the
    middle of what SEGMENT_RE expects to be all-digits) and grows
    without bound.

    Returns None if `source_name` doesn't match the segment naming
    scheme at all (defensive — callers skip adoption for it).
    """
    m = _SEGMENT_RE.match(source_name)
    if not m:
        return None
    if m.group("orig_epoch") is not None:
        origin = f"{m.group('orig_epoch')}-{m.group('orig_pid')}"
    else:
        origin = f"{m.group('epoch')}-{m.group('pid')}"
    return f"{new_owner_prefix}-adopted-{origin}-{m.group('seq')}.ndjson"


def _adopt_orphan_group_sync(prefix: str, root: Path) -> bool:
    """Claim every file (segment + `.wip` + offset sidecar) under one
    orphan `{epoch}-{pid}` prefix via atomic `os.rename`. Returns True
    if at least one file was successfully claimed (another worker may
    have already won some/all of them — that's fine, POSIX rename on a
    vanished source raises FileNotFoundError and we just move on).

    D2 fix (gate-E review, 2026-07-07): the parent directory is fsynced
    once after all renames in this call so the adoption itself (a
    directory-entry change, same class as segment finalize) survives a
    power-loss — closes a durability gap that compounded the CRITICAL
    stranding bug this function's naming scheme fixes."""
    my_prefix = _worker_prefix()
    claimed_any = False

    for f in sorted(root.glob(f"{prefix}-*.ndjson.wip")):
        _truncate_torn_tail_sync(f)
        base_name = f.name[: -len(".wip")]
        new_name = _canonical_adopted_name(base_name, my_prefix)
        if new_name is None:
            logger.warning(
                "Orphan adoption: %s doesn't match the segment naming "
                "scheme — skipping", f,
            )
            continue
        try:
            os.rename(f, f.parent / new_name)
        except FileNotFoundError:
            continue
        except OSError as exc:
            logger.warning("Orphan adoption rename failed for %s: %s", f, exc)
            continue
        claimed_any = True
        old_offset = f.parent / (base_name + ".offset")
        if old_offset.exists():
            try:
                os.rename(old_offset, f.parent / (new_name + ".offset"))
            except OSError:
                pass

    for f in sorted(root.glob(f"{prefix}-*.ndjson")):
        new_name = _canonical_adopted_name(f.name, my_prefix)
        if new_name is None:
            logger.warning(
                "Orphan adoption: %s doesn't match the segment naming "
                "scheme — skipping", f,
            )
            continue
        try:
            os.rename(f, f.parent / new_name)
        except FileNotFoundError:
            continue
        except OSError as exc:
            logger.warning("Orphan adoption rename failed for %s: %s", f, exc)
            continue
        claimed_any = True
        old_offset = f.parent / (f.name + ".offset")
        if old_offset.exists():
            try:
                os.rename(old_offset, f.parent / (new_name + ".offset"))
            except OSError:
                pass

    if claimed_any:
        try:
            _fsync_dir_sync(root)
        except OSError as exc:
            logger.warning(
                "Orphan adoption: parent-directory fsync failed after "
                "claiming prefix=%s: %s", prefix, exc,
            )
        # Best-effort tidy-up — a stale `.alive` file left behind for a
        # fully-claimed dead prefix is harmless (never matched by
        # `_SEGMENT_RE`, never re-considered since its segments are
        # gone), but removing it keeps the directory listing honest.
        try:
            _heartbeat_path(root, prefix).unlink()
        except OSError:
            pass

    return claimed_any


def _adopt_orphan_segments_sync() -> list[str]:
    root = _queue_root()
    if not root.exists():
        return []
    my_prefix = _worker_prefix()
    # HIGH fix (gate-E review, 2026-07-07) — REAL wall-clock "now", not
    # `_get_boot_epoch()`. This function now runs repeatedly over a
    # long-lived process's life (periodic retry, not one-shot-at-boot),
    # and `_get_boot_epoch()` is MEMOIZED at this process's own boot
    # moment — reusing it as "now" would freeze every candidate's
    # computed age at whatever it was on the FIRST call, silently
    # defeating the retry (a deferred orphan would never age past the
    # min-age floor no matter how many real seconds pass).
    now = int(time.time())
    min_age = settings.disk_orphan_adopt_min_age_seconds

    prefixes: set[str] = set()
    for pattern in ("*.ndjson", "*.ndjson.wip"):
        for f in root.glob(pattern):
            name = f.name
            if name.endswith(".wip"):
                name = name[: -len(".wip")]
            m = _SEGMENT_RE.match(name)
            if not m:
                continue
            prefix = f"{m.group('epoch')}-{m.group('pid')}"
            if prefix == my_prefix:
                continue
            epoch = int(m.group("epoch"))
            if now - epoch < min_age:
                continue  # likely a same-boot sibling still starting up
            prefixes.add(prefix)

    adopted: list[str] = []
    for prefix in sorted(prefixes):
        # gate-E round 2 CRITICAL fix (2026-07-07) — the age check above
        # is only a cheap pre-filter (same-boot-sibling-still-starting-up
        # guard); the REAL liveness gate is mechanical (heartbeat-based),
        # never age-inferred. See `_worker_is_dead_sync`'s docstring for
        # why age alone made every live sibling past ~30s of life look
        # "dead".
        if not _worker_is_dead_sync(root, prefix, time.time()):
            continue
        if _adopt_orphan_group_sync(prefix, root):
            adopted.append(prefix)
            epoch = int(prefix.split("-")[0])
            logger.warning(
                "Orphan adoption: claimed dead worker's segments "
                "prefix=%s (age=%ds) — will drain under %s",
                prefix, now - epoch, my_prefix,
            )
    return adopted


async def adopt_orphan_segments() -> list[str]:
    """Run at worker startup AND on every periodic drainer cycle
    (gate-E fix, 2026-07-07 — was one-shot-at-boot only, see module
    docstring for why that stranded segments). Idempotent — a prefix
    already adopted under this worker's own name is excluded from
    later scans (`prefix == my_prefix`), so calling this repeatedly
    with nothing new to adopt is just a couple of cheap glob scans.

    gate-E round 2 CRITICAL fix (2026-07-07): a candidate prefix is
    only ever claimed once `_worker_is_dead_sync` proves it via its
    heartbeat file — see that function's docstring. `run_drainer`
    refreshes THIS worker's own heartbeat every cycle before calling
    here, so a live worker is never at risk of its own segments
    looking "dead" to a peer."""
    adopted = await asyncio.to_thread(_adopt_orphan_segments_sync)
    if adopted:
        capture_op_msg(
            OP_SEGMENT_ORPHAN_ADOPTED,
            f"Orphan adoption claimed {len(adopted)} dead worker prefix(es): "
            f"{adopted}",
            level="warning",
            adopted_prefixes=adopted,
        )
    return adopted


# ---------------------------------------------------------------------------
# Replay (drain) — segments (own + adopted) + legacy *.json (D1)
# ---------------------------------------------------------------------------


def _offset_path_for(segment_path: Path) -> Path:
    return segment_path.with_name(segment_path.name + ".offset")


def _read_offset_sync(segment_path: Path) -> int:
    p = _offset_path_for(segment_path)
    try:
        return int(p.read_text().strip())
    except (FileNotFoundError, ValueError, OSError):
        return 0


def _write_offset_sync(segment_path: Path, offset: int) -> None:
    p = _offset_path_for(segment_path)
    tmp = p.with_suffix(p.suffix + ".tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.write(fd, str(offset).encode())
        os.fsync(fd)
    finally:
        os.close(fd)
    os.rename(tmp, p)


def _delete_segment_and_sidecar_sync(segment_path: Path) -> None:
    for p in (segment_path, _offset_path_for(segment_path)):
        try:
            p.unlink()
        except FileNotFoundError:
            pass


def _read_complete_lines_sync(path: Path) -> list[bytes]:
    try:
        data = path.read_bytes()
    except FileNotFoundError:
        return []
    if not data:
        return []
    lines = data.split(b"\n")
    if lines and lines[-1] == b"":
        lines = lines[:-1]
    return lines


async def _replay_segment(redis, path: Path) -> dict:
    """Replay one finalized segment (own or adopted). Offset persistence
    (B3) is BATCHED every `disk_replay_offset_batch_lines` lines (gate-E
    perf fix, 2026-07-07 — was every single line: correct but expensive
    under a large backlog) so a CRASH mid-replay re-does at most that
    many lines, never the whole segment — the `click:shipped` check
    below is the backstop for that bounded duplicate, on top of the
    collector's own central dedup. A Redis-IMPAIRMENT break (the
    process stays alive, it just stops) flushes the offset immediately
    on the way out — only a hard crash accepts the batching window.

    LOSSFIX P2 fix (2026-07-07, GTD routing-audit CRITICAL-disk-
    fallback-silent-loss): this used to gate on `click:seen` — the SAME
    key `main._acquire_click_dedup` SETNX's at /decide time, BEFORE the
    stream-vs-disk-fallback decision even runs. That meant EVERY click
    that reached disk (M1 reject, a watermark spill, or a genuine XADD
    exception) ALREADY had its `click:seen` marker planted by its OWN
    /decide call — so this check's SETNX always found the key taken,
    always concluded "duplicate, already in the stream", and deleted
    the segment line WITHOUT ever shipping it. 100%-reproducible silent
    loss for every disk-fallback click, live-confirmed 0/40. `click:
    shipped` is a DIFFERENT key, set ONLY after a CONFIRMED-successful
    XADD (here, or in main.py's direct-write path) — never before one
    is attempted — so a click that never got a chance to ship (which is
    every M1/watermark-diverted click) can NEVER see its own marker
    already set."""
    lines = await asyncio.to_thread(_read_complete_lines_sync, path)
    start_offset = await asyncio.to_thread(_read_offset_sync, path)
    drained = skipped = 0
    failed = 0
    i = start_offset
    n = len(lines)
    persisted_offset = start_offset
    batch_size = max(1, settings.disk_replay_offset_batch_lines)

    async def _maybe_persist_offset() -> None:
        nonlocal persisted_offset
        if i - persisted_offset >= batch_size:
            await asyncio.to_thread(_write_offset_sync, path, i)
            persisted_offset = i

    completed = False
    while i < n:
        line = lines[i]
        try:
            parsed = json.loads(line)
        except (json.JSONDecodeError, ValueError) as exc:
            logger.warning(
                "Segment %s line %d unparseable, skipping: %s",
                path.name, i, exc,
            )
            sentry_sdk.capture_exception(exc)
            skipped += 1
            i += 1
            await _maybe_persist_offset()
            continue

        click_id_for_dedup: str | None = None
        if isinstance(parsed, dict):
            cid = parsed.get("click_id")
            if isinstance(cid, str) and cid:
                click_id_for_dedup = cid

        if click_id_for_dedup is not None and settings.click_dedup_ttl_seconds > 0:
            try:
                already_shipped = await redis.get(f"click:shipped:{click_id_for_dedup}")
                if already_shipped:
                    logger.info(
                        "Segment replay: click_id %s already confirmed "
                        "shipped — skipping line %d of %s",
                        click_id_for_dedup, i, path.name,
                    )
                    skipped += 1
                    i += 1
                    await _maybe_persist_offset()
                    continue
            except Exception as exc:  # noqa: BLE001 — Redis impaired, fail-open to legacy
                logger.warning(
                    "Segment replay: click:shipped GET failed for %s: "
                    "%s — proceeding without dedup",
                    click_id_for_dedup, exc,
                )

        try:
            await redis.xadd(
                "stream:clicks",
                {"data": line.decode("utf-8")},
            )
        except Exception as exc:  # noqa: BLE001 — broad on purpose
            logger.warning(
                "Segment replay stopped at %s line %d — Redis still "
                "impaired: %s",
                path.name, i, exc,
            )
            failed = 1
            break

        if click_id_for_dedup is not None and settings.click_dedup_ttl_seconds > 0:
            try:
                await redis.set(
                    f"click:shipped:{click_id_for_dedup}",
                    "1",
                    ex=settings.click_dedup_ttl_seconds,
                )
            except Exception as exc:  # noqa: BLE001 — non-fatal, best-effort marker
                logger.warning(
                    "Segment replay: click:shipped SET failed for %s: "
                    "%s (non-fatal — click already durably in the "
                    "stream)",
                    click_id_for_dedup, exc,
                )

        drained += 1
        i += 1
        await _maybe_persist_offset()
    else:
        completed = True

    if completed:
        # Every line from start_offset onward is now drained or
        # skipped. Safe to delete the whole segment (atomic per-
        # segment, per the design brief) — no need to flush the
        # batched offset first, the file is gone either way.
        await asyncio.to_thread(_delete_segment_and_sidecar_sync, path)
    elif i > persisted_offset:
        # Broke out early (Redis impairment — the process is still
        # alive, it just stopped). Flush whatever progress happened
        # since the last batched write immediately: a non-crash
        # interruption must not accept the batching window, only a
        # hard crash does.
        await asyncio.to_thread(_write_offset_sync, path, i)
        persisted_offset = i

    remaining = max(0, n - start_offset - drained - skipped)
    return {"drained": drained, "skipped": skipped, "failed": failed, "remaining": remaining}


async def _drain_legacy_json_files(redis) -> dict:
    """D1 — pre-P2 per-click `*.json` files (from the OLD design) are
    drained by the SAME cycle so an upgrade never strands them. Shared
    (race-tolerant) across all workers — unlike segments, legacy files
    are not per-worker-owned, but the read-then-XADD-then-unlink
    sequence already tolerates a race (another worker/cleanup grabbing
    the file first surfaces as FileNotFoundError, handled below exactly
    like the pre-P2 code did). This path self-obsoletes: once a node's
    legacy backlog empties, the glob below finds nothing every cycle."""
    root = _queue_root()
    if not root.exists():
        return {"drained": 0, "skipped": 0, "failed": 0, "remaining": 0}

    files = await asyncio.to_thread(lambda: sorted(root.rglob("*.json")))
    drained = skipped = 0
    failed = 0

    for path in files:
        try:
            data = await asyncio.to_thread(path.read_bytes)
        except FileNotFoundError:
            skipped += 1
            continue
        except OSError as exc:
            logger.warning("Legacy drain: failed to read %s: %s", path.name, exc)
            sentry_sdk.capture_exception(exc)
            skipped += 1
            continue

        click_id_for_dedup: str | None = None
        if settings.click_dedup_ttl_seconds > 0:
            try:
                parsed = json.loads(data)
                cid = parsed.get("click_id")
                if isinstance(cid, str) and cid:
                    click_id_for_dedup = cid
            except (json.JSONDecodeError, AttributeError) as exc:
                logger.warning(
                    "Legacy drain: failed to parse click_id from %s: %s "
                    "— proceeding without dedup",
                    path.name, exc,
                )
        if click_id_for_dedup is not None:
            # LOSSFIX P2 fix (2026-07-07, GTD routing-audit CRITICAL-
            # disk-fallback-silent-loss) — `click:shipped`, NOT
            # `click:seen`. See `_replay_segment`'s docstring for the
            # full bug: `click:seen` is planted at /decide dedup-check
            # time, BEFORE any write decision, so every disk-fallback
            # click already has it set and the old SETNX-on-click:seen
            # check always false-positived as "duplicate" — deleting
            # the file without ever shipping it.
            try:
                already_shipped = await redis.get(f"click:shipped:{click_id_for_dedup}")
                if already_shipped:
                    logger.info(
                        "Legacy drain: click_id %s already confirmed "
                        "shipped — dropping %s",
                        click_id_for_dedup, path.name,
                    )
                    try:
                        await asyncio.to_thread(path.unlink)
                    except FileNotFoundError:
                        pass
                    except OSError as exc:
                        logger.warning(
                            "Legacy drain: unlink failed after "
                            "duplicate-skip on %s: %s", path.name, exc,
                        )
                    skipped += 1
                    continue
            except Exception as exc:  # noqa: BLE001 — Redis impaired
                logger.warning(
                    "Legacy drain: click:shipped GET failed for %s: %s "
                    "— proceeding without dedup", click_id_for_dedup, exc,
                )

        try:
            await redis.xadd("stream:clicks", {"data": data.decode("utf-8")})
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Legacy drain stopped at %s — Redis still impaired: %s",
                path.name, exc,
            )
            failed = 1
            break

        if click_id_for_dedup is not None and settings.click_dedup_ttl_seconds > 0:
            try:
                await redis.set(
                    f"click:shipped:{click_id_for_dedup}",
                    "1",
                    ex=settings.click_dedup_ttl_seconds,
                )
            except Exception as exc:  # noqa: BLE001 — non-fatal, best-effort marker
                logger.warning(
                    "Legacy drain: click:shipped SET failed for %s: %s "
                    "(non-fatal — click already durably in the stream)",
                    click_id_for_dedup, exc,
                )

        try:
            await asyncio.to_thread(path.unlink)
        except FileNotFoundError:
            drained += 1
        except OSError as exc:
            logger.warning(
                "Legacy drain: XADD-OK but unlink failed for %s: %s — "
                "file kept, retried next cycle", path.name, exc,
            )
            sentry_sdk.capture_exception(exc)
            continue
        else:
            drained += 1

    return {
        "drained": drained,
        "skipped": skipped,
        "failed": failed,
        "remaining": max(0, len(files) - drained - skipped),
    }


async def drain_to_redis(redis) -> dict:
    """Replay this worker's own finalized segments + any orphan
    segments it adopted at startup + legacy `*.json` files, back into
    `stream:clicks`. Stops at the FIRST Redis failure (self-limit — no
    point pounding an impaired Redis); remaining segments/files stay on
    disk for the next drainer iteration.

    Returns the SAME stats shape as pre-P2: drained/skipped/failed/
    remaining, summed across every source drained this cycle.
    """
    total = {"drained": 0, "skipped": 0, "failed": 0, "remaining": 0}

    legacy_stats = await _drain_legacy_json_files(redis)
    for k in total:
        total[k] += legacy_stats[k]
    if legacy_stats["failed"]:
        return total

    root = _queue_root()
    if not root.exists():
        return total

    my_prefix = _worker_prefix()
    # Own + adopted segments both match `{my_prefix}-*.ndjson` — adopted
    # ones carry the extra `-adopted-{orig}` infix but share the SAME
    # prefix, so one glob covers both. `.wip` segments never match
    # (different suffix) — the active segment is invisible here by
    # construction, no writer/drainer coordination needed.
    segment_paths = sorted(root.glob(f"{my_prefix}-*.ndjson"))

    for path in segment_paths:
        if total["failed"]:
            break
        stats = await _replay_segment(redis, path)
        for k in ("drained", "skipped", "failed"):
            total[k] += stats[k]
        total["remaining"] += stats["remaining"]

    return total


async def run_drainer(redis, interval: int | None = None) -> None:
    """Background task: periodic orphan-adoption retry + drain attempt.

    Started in the FastAPI lifespan, cancelled on shutdown. Robust
    to per-iteration errors — a transient failure in one round
    doesn't kill the loop.

    HIGH fix (gate-E review, 2026-07-07): orphan adoption now runs
    EVERY cycle here, not just once at boot (see `adopt_orphan_
    segments`'s docstring + the module docstring's "Orphan adoption"
    section) — a deferred (too-young-at-first-check) or re-orphaned
    (its adopter died before draining it) segment gets a real retry
    every `interval` seconds, not "wait for an unrelated future
    restart".

    gate-E round 2 fixes (2026-07-07): each cycle also (1) refreshes
    THIS worker's own heartbeat file BEFORE scanning for orphans — the
    mechanical proof-of-life every peer's `adopt_orphan_segments` call
    relies on to never mistake this worker for dead (CRITICAL fix, see
    `_worker_is_dead_sync`); (2) calls the writer's `finalize_if_stale()`
    so an idle `.wip` tail (a spill burst followed by quiet) surfaces to
    the drainer within one interval instead of sitting open indefinitely
    (HIGH fix, see that method's docstring).
    """
    if interval is None:
        interval = settings.disk_queue_drain_interval_seconds
    logger.info("Disk-queue drainer started (interval=%ds)", interval)

    while True:
        try:
            await asyncio.sleep(interval)
            await asyncio.to_thread(_touch_heartbeat_sync)
            await adopt_orphan_segments()
            await _get_writer().finalize_if_stale()
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
    global _writer, _cached_queue_stats, _boot_epoch
    if _writer is not None:
        try:
            _writer.force_finalize_for_tests()
        except Exception:  # noqa: BLE001 — best-effort cleanup between tests
            pass
    _writer = None
    _cached_queue_stats = None
    _boot_epoch = None
