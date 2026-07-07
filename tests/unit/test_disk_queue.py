"""Tests for the click-processor disk-segment fallback queue (T2.2 / G-23),
REDESIGNED by P2 (LOSSFIX, 2026-07-07) from one-file-per-click into
append-only NDJSON segments with group-commit fsync, per-worker
ownership, orphan adoption, and a global byte-cap.

Coverage layers (mirrors the P2 brief's OBSERVABLE DONE items 1-9, 13):

  1. Segment lifecycle — rotate at size/time, group-commit fsync,
     replay-then-unlink, dir-fsync on finalize (D2).
  2. WC=8 no-race — per-worker prefix naming, no two workers touch the
     same live segment.
  3. Orphan adoption (B1) — a dead worker's segments get claimed by
     exactly one live worker.
  4. Legacy migration (D1) — pre-P2 `*.json` files still drain.
  5. Byte-cap — visible shed (return False), never silent.
  6. Partial-last-line (B2) — a torn `.wip` tail truncates + op-tags,
     never crashes, never drops a well-formed line.
  7. Replay exactly-once (B3) — an offset sidecar bounds a crash to at
     most one re-replayed line.
  8. `/health` depth (D3) — segment count / bytes / oldest-age, via
     `get_queue_stats`.
  9. Crash-recovery E2E — kill mid-write, restart, adopt, replay,
     nothing lost, nothing double-shipped.
  10. Mechanical liveness (gate-E round 2 CRITICAL) — orphan adoption
      NEVER touches a live sibling (heartbeat-gated, not age-inferred),
      proven both at the unit level and against a REAL killable
      subprocess.
  11. Rotation-failure containment + idle-tail finalize (gate-E round 2
      HIGH) — a rotation OSError never hangs the flush loop nor
      strands durable bytes; an idle `.wip` with zero new appends
      still finalizes within one drainer cycle.

Reference: rule `sync-protocol`, action-items.md T2.2, open-questions.md
G-23.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from app import disk_queue


@pytest.fixture(autouse=True)
def _reset_disk_queue_state(tmp_path, monkeypatch):
    """Per-test isolation: rebase the queue root on tmp_path, disable
    the group-commit linger (deterministic, fast tests — each
    `enqueue_click` await resolves as soon as its OWN commit runs), and
    reset all P2 module-level state (writer, cached stats, boot
    epoch)."""
    monkeypatch.setattr(
        disk_queue.settings, "disk_queue_root", str(tmp_path / "click-queue"),
    )
    monkeypatch.setattr(disk_queue.settings, "disk_segment_group_commit_ms", 0.0)
    monkeypatch.setattr(disk_queue.settings, "disk_segment_max_bytes", 2_000_000)
    monkeypatch.setattr(disk_queue.settings, "disk_segment_max_age_seconds", 1_000.0)
    monkeypatch.setattr(disk_queue.settings, "disk_segment_max_total_bytes", 5_000_000_000)
    monkeypatch.setattr(disk_queue.settings, "disk_orphan_adopt_min_age_seconds", 30)
    disk_queue._reset_state_for_tests()
    yield
    disk_queue._reset_state_for_tests()


def _root() -> Path:
    return Path(disk_queue.settings.disk_queue_root)


def _segments() -> list[Path]:
    if not _root().exists():
        return []
    return sorted(_root().rglob("*.ndjson"))


def _wip_segments() -> list[Path]:
    if not _root().exists():
        return []
    return sorted(_root().rglob("*.ndjson.wip"))


def _make_redis_mock(xadd_side_effect=None) -> AsyncMock:
    redis = AsyncMock()
    if xadd_side_effect is not None:
        redis.xadd.side_effect = xadd_side_effect
    # `click:shipped` is a GET-then-SET check (P2 fix, 2026-07-07) — a
    # bare AsyncMock's `.get(...)` return_value defaults to a truthy
    # MagicMock, which would make EVERY replayed line look
    # "already shipped" and never reach XADD. None = "not shipped yet",
    # the correct default for a fresh fake-Redis in these tests.
    redis.get = AsyncMock(return_value=None)
    return redis


def _lines_of(path: Path) -> list[dict]:
    text = path.read_text()
    return [json.loads(line) for line in text.splitlines() if line]


# ---------------------------------------------------------------------------
# 1 — segment lifecycle: append, rotate, group-commit fsync, dir-fsync (D2)
# ---------------------------------------------------------------------------


class TestSegmentLifecycle:
    @pytest.mark.asyncio
    async def test_enqueue_creates_a_wip_segment_then_none_finalized_yet(self):
        assert await disk_queue.enqueue_click({"click_id": "a"}) is True
        assert len(_wip_segments()) == 1
        assert _segments() == []  # not rotated yet — below size/age threshold

    @pytest.mark.asyncio
    async def test_multiple_enqueues_append_to_the_same_open_segment(self):
        for i in range(5):
            assert await disk_queue.enqueue_click({"click_id": f"c{i}"}) is True
        wips = _wip_segments()
        assert len(wips) == 1
        assert _lines_of(wips[0]) == [{"click_id": f"c{i}"} for i in range(5)]

    @pytest.mark.asyncio
    async def test_payload_round_trips_via_json(self):
        record = {"click_id": "abc", "country": "PL", "weight": 0.42}
        assert await disk_queue.enqueue_click(record) is True
        assert _lines_of(_wip_segments()[0]) == [record]

    @pytest.mark.asyncio
    async def test_non_serializable_value_uses_str_default(self):
        """`json.dumps(..., default=str)` keeps the disk-segment payload
        shape identical to /decide's happy-path serialization."""
        from datetime import datetime

        record = {"click_id": "abc", "ts": datetime(2026, 5, 9, 12, 0, 0)}
        assert await disk_queue.enqueue_click(record) is True
        decoded = _lines_of(_wip_segments()[0])[0]
        assert decoded["click_id"] == "abc"
        assert isinstance(decoded["ts"], str)

    @pytest.mark.asyncio
    async def test_rotates_at_size_threshold(self, monkeypatch):
        monkeypatch.setattr(disk_queue.settings, "disk_segment_max_bytes", 10)
        await disk_queue.enqueue_click({"a": "x" * 20})  # one line already exceeds 10 bytes
        # Rotation happens AFTER the commit that crossed the threshold —
        # the segment is finalized (renamed off .wip) once that commit lands.
        assert _wip_segments() == []
        assert len(_segments()) == 1

    @pytest.mark.asyncio
    async def test_rotates_at_age_threshold(self, monkeypatch):
        monkeypatch.setattr(disk_queue.settings, "disk_segment_max_age_seconds", 0.0)
        await disk_queue.enqueue_click({"a": 1})
        await disk_queue.enqueue_click({"a": 2})  # 2nd commit sees age >= 0 -> rotates
        assert len(_segments()) >= 1

    @pytest.mark.asyncio
    async def test_group_commit_batches_concurrent_appends_into_one_fsync(
        self, monkeypatch,
    ):
        """Concurrent awaiters that land within the linger window share
        ONE commit (one `_commit_batch_sync` call) instead of one
        fsync per click — the core fix for the old ~1-fsync/click
        pathology."""
        monkeypatch.setattr(disk_queue.settings, "disk_segment_group_commit_ms", 50.0)
        writer = disk_queue._get_writer()
        commit_calls = []
        original = writer._commit_batch_sync

        def counting_commit(batch):
            commit_calls.append(len(batch))
            return original(batch)

        monkeypatch.setattr(writer, "_commit_batch_sync", counting_commit)

        results = await asyncio.gather(
            *[disk_queue.enqueue_click({"click_id": f"g{i}"}) for i in range(10)]
        )
        assert all(results)
        assert len(commit_calls) == 1, (
            f"Expected all 10 concurrent appends to share ONE group-commit, "
            f"got {len(commit_calls)} separate commits: {commit_calls}"
        )
        assert commit_calls[0] == 10

    @pytest.mark.asyncio
    async def test_finalize_dir_fsyncs_parent(self, monkeypatch):
        """D2 — the parent directory must be fsynced on finalize so the
        rename (directory-entry change) survives a power-loss."""
        monkeypatch.setattr(disk_queue.settings, "disk_segment_max_bytes", 1)
        dir_fsync_calls = []
        original_fsync = os.fsync

        def spying_fsync(fd):
            dir_fsync_calls.append(fd)
            return original_fsync(fd)

        monkeypatch.setattr(disk_queue.os, "fsync", spying_fsync)
        await disk_queue.enqueue_click({"a": 1})  # rotates immediately (max_bytes=1)

        assert len(_segments()) == 1
        # At least 2 fsyncs happened: one for the file content, one for
        # the directory (D2). We can't cheaply distinguish fds here
        # without duplicating internals, so assert the COUNT — file +
        # dir fsync both fired.
        assert len(dir_fsync_calls) >= 2

    @pytest.mark.asyncio
    async def test_commit_error_closes_and_salvages_fd_no_stranding(self, monkeypatch):
        """MEDIUM fix (gate-E review, 2026-07-07) — on an OSError inside
        `_commit_batch_sync`, the fd must be closed+nulled (so a LATER
        successful commit can't silently flush the failed batch's
        bytes through and un-fail a click already told `ok=False`) AND
        whatever an EARLIER successful commit on the SAME fd durably
        wrote must be salvaged (truncated + finalized under this
        worker's own prefix) — not abandoned forever in a `.wip` file
        that neither this worker's own drain (globs only `.ndjson`)
        nor orphan adoption (excludes its own prefix) will ever look
        at again."""
        call_count = {"n": 0}
        original_fsync = os.fsync

        def flaky_fsync(fd):
            call_count["n"] += 1
            if call_count["n"] == 2:
                raise OSError("simulated fsync failure")
            return original_fsync(fd)

        monkeypatch.setattr(disk_queue.os, "fsync", flaky_fsync)

        assert await disk_queue.enqueue_click({"click_id": "a"}) is True
        ok = await disk_queue.enqueue_click({"click_id": "b"})  # 2nd commit's fsync fails
        assert ok is False

        writer = disk_queue._get_writer()
        assert writer.current_wip_path is None, (
            "The fd must be abandoned (closed+nulled), not left open "
            "for the NEXT commit to keep writing/fsyncing through."
        )

        # Whatever WAS durably written (click "a", from the first,
        # SUCCESSFUL commit) must have been salvaged — finalized under
        # THIS worker's own prefix, discoverable by its own drain.
        segments = _segments()
        assert len(segments) == 1
        assert {"click_id": "a"} in _lines_of(segments[0])

        # A later click opens a FRESH segment — not the abandoned fd.
        assert await disk_queue.enqueue_click({"click_id": "c"}) is True
        assert len(_wip_segments()) == 1


# ---------------------------------------------------------------------------
# 11 — rotation-failure containment + idle-tail finalize (gate-E round 2
# HIGH §4 + §5).
# ---------------------------------------------------------------------------


class TestRotationFailureContainment:
    @pytest.mark.asyncio
    async def test_rotation_rename_failure_reopens_segment_no_stranding_no_hang(
        self, monkeypatch,
    ):
        """HIGH fix (gate-E round 2 §4) — a rename failure DURING
        rotation (simulated ENOSPC/EIO) must never hang the caller
        (the write+fsync already succeeded, so ok=True) and must never
        strand the segment's already-durable bytes under a cleared
        fd/path — the writer re-opens the SAME `.wip` path so the data
        stays reachable and rotation retries on the next eligible
        commit."""
        # Just over one line's size (~19 bytes for `{"click_id":"a"}\n`)
        # so the FIRST click alone crosses the threshold and triggers
        # the (simulated-failing) rotate attempt.
        monkeypatch.setattr(disk_queue.settings, "disk_segment_max_bytes", 15)

        call_count = {"n": 0}
        original_rename = os.rename

        def flaky_rename(src, dst):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise OSError("simulated ENOSPC on rename")
            return original_rename(src, dst)

        monkeypatch.setattr(disk_queue.os, "rename", flaky_rename)

        ok = await disk_queue.enqueue_click({"click_id": "a"})
        assert ok is True, (
            "The write+fsync succeeded BEFORE the rotate step ran — "
            "callers must see ok=True regardless of a LATER rotation "
            "failure."
        )

        writer = disk_queue._get_writer()
        assert writer.current_wip_path is not None, (
            "After a failed rename, the writer must re-open the SAME "
            ".wip path rather than losing track of it (which would "
            "strand its already-durable bytes under this worker's own, "
            "never-revisited prefix)."
        )

        # Click "b" lands in the SAME still-open segment (size stays
        # over threshold, since the writer never reset it) — its own
        # commit's rotate check retries the finalize, and this time
        # the rename succeeds (call_count reaches 2), closing the
        # segment with BOTH "a" and "b" durably inside it. No data
        # lost across the retry, and the caller never hung.
        ok2 = await disk_queue.enqueue_click({"click_id": "b"})
        assert ok2 is True
        assert _wip_segments() == []
        segments = _segments()
        assert len(segments) == 1
        assert _lines_of(segments[0]) == [{"click_id": "a"}, {"click_id": "b"}]

        # A later click still succeeds — the writer isn't wedged. (With
        # this tiny max_bytes, even a single fresh line already exceeds
        # it, so "c" rotates into its own segment immediately too —
        # what matters is that it's durably recorded, not the exact
        # segment count.)
        ok3 = await disk_queue.enqueue_click({"click_id": "c"})
        assert ok3 is True
        all_lines = [line for seg in _segments() for line in _lines_of(seg)]
        assert {"click_id": "c"} in all_lines

    @pytest.mark.asyncio
    async def test_unexpected_commit_exception_resolves_futures_and_resets_flush_task(
        self, monkeypatch,
    ):
        """Defense-in-depth (gate-E round 2 §4 ask #2) — even an
        exception this fix didn't specifically anticipate escaping the
        commit call must resolve the batch's futures (ok=False, never
        hang forever) AND reset `_flush_task` so the NEXT append()
        spawns a fresh flush loop rather than queuing into a dead one
        — the node-wide spill-path hang this whole fix-round closes."""
        writer = disk_queue._get_writer()
        original = writer._commit_batch_sync
        call_count = {"n": 0}

        def exploding_commit(batch):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise RuntimeError("unexpected bug")
            return original(batch)

        monkeypatch.setattr(writer, "_commit_batch_sync", exploding_commit)

        ok = await asyncio.wait_for(
            disk_queue.enqueue_click({"click_id": "a"}), timeout=2.0,
        )
        assert ok is False

        assert writer._flush_task is None, (
            "_flush_task must be reset after ANY exit from "
            "_run_flush_loop, including an unexpected exception — "
            "otherwise every future append queues into a dead loop "
            "and never resolves, hanging the entire spill path."
        )

        ok2 = await asyncio.wait_for(
            disk_queue.enqueue_click({"click_id": "b"}), timeout=2.0,
        )
        assert ok2 is True


class TestFinalizeIfStale:
    @pytest.mark.asyncio
    async def test_idle_wip_past_max_age_gets_finalized(self, monkeypatch):
        """HIGH fix (gate-E round 2 §5) — an idle `.wip` with ZERO new
        appends must still finalize once past `disk_segment_max_age_
        seconds`, closing the "spill burst then quiet" unbounded-
        delivery-delay gap (previously rotation only ever ran as a
        side effect of a LATER commit)."""
        monkeypatch.setattr(disk_queue.settings, "disk_segment_max_age_seconds", 0.05)
        await disk_queue.enqueue_click({"click_id": "idle-1"})
        assert len(_wip_segments()) == 1

        await asyncio.sleep(0.1)
        await disk_queue._get_writer().finalize_if_stale()

        assert _wip_segments() == []
        assert len(_segments()) == 1
        assert _lines_of(_segments()[0]) == [{"click_id": "idle-1"}]

    @pytest.mark.asyncio
    async def test_fresh_wip_not_yet_stale_is_left_open(self, monkeypatch):
        monkeypatch.setattr(disk_queue.settings, "disk_segment_max_age_seconds", 1000.0)
        await disk_queue.enqueue_click({"click_id": "fresh-1"})

        await disk_queue._get_writer().finalize_if_stale()

        assert len(_wip_segments()) == 1
        assert _segments() == []

    @pytest.mark.asyncio
    async def test_no_open_segment_is_a_noop(self):
        await disk_queue._get_writer().finalize_if_stale()  # must not raise

    @pytest.mark.asyncio
    async def test_wired_into_run_drainer_cycle(self, monkeypatch):
        """Proves the retrofit is actually wired into `run_drainer`,
        not just callable on demand — an idle `.wip` with NO new
        appends still gets drained by the background loop alone."""
        monkeypatch.setattr(disk_queue.settings, "disk_segment_max_age_seconds", 0.01)
        await disk_queue.enqueue_click({"click_id": "cycle-1"})
        assert len(_wip_segments()) == 1

        redis = _make_redis_mock()
        task = asyncio.create_task(disk_queue.run_drainer(redis, interval=0))
        # Poll for the TERMINAL state (both empty) rather than the
        # intermediate "xadd was called" signal — there are further
        # await points (click:shipped SET, offset persistence, the
        # delete-to-thread call) between the xadd and the segment
        # actually disappearing, so breaking on await_count alone
        # races with those and is flaky.
        for _ in range(200):
            await asyncio.sleep(0.01)
            if not _wip_segments() and not _segments():
                break
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert redis.xadd.await_count == 1
        assert _wip_segments() == []
        assert _segments() == []  # drained + deleted


# ---------------------------------------------------------------------------
# 5 — byte-cap: visible shed, never silent
# ---------------------------------------------------------------------------


class TestByteCap:
    @pytest.mark.asyncio
    async def test_at_cap_enqueue_returns_false(self, monkeypatch):
        await disk_queue.enqueue_click({"a": 1})
        disk_queue._cached_queue_stats = {"segments": 1, "bytes": 999, "oldest_seconds": 0.0}
        monkeypatch.setattr(disk_queue.settings, "disk_segment_max_total_bytes", 999)

        assert await disk_queue.enqueue_click({"a": 2}) is False

    @pytest.mark.asyncio
    async def test_below_cap_enqueue_succeeds(self, monkeypatch):
        disk_queue._cached_queue_stats = {"segments": 1, "bytes": 10, "oldest_seconds": 0.0}
        monkeypatch.setattr(disk_queue.settings, "disk_segment_max_total_bytes", 999)
        assert await disk_queue.enqueue_click({"a": 1}) is True

    @pytest.mark.asyncio
    async def test_disabled_cap_never_rejects(self, monkeypatch):
        disk_queue._cached_queue_stats = {"segments": 1, "bytes": 10**12, "oldest_seconds": 0.0}
        monkeypatch.setattr(disk_queue.settings, "disk_segment_max_total_bytes", 0)
        assert await disk_queue.enqueue_click({"a": 1}) is True

    @pytest.mark.asyncio
    async def test_never_sampled_fails_open(self, monkeypatch):
        """Fail-open discipline (A3-style): a never-sampled cache must
        never itself become a new failure mode."""
        assert disk_queue._cached_queue_stats is None
        monkeypatch.setattr(disk_queue.settings, "disk_segment_max_total_bytes", 1)
        assert await disk_queue.enqueue_click({"a": 1}) is True

    def test_scan_computes_bytes_segments_and_oldest_age(self, tmp_path):
        root = _root()
        root.mkdir(parents=True)
        (root / "1-1-000001.ndjson").write_bytes(b'{"a":1}\n')
        time.sleep(0.05)
        (root / "1-1-000002.ndjson").write_bytes(b'{"a":2}\n{"a":3}\n')

        stats = disk_queue._scan_queue_stats_sync()
        assert stats["segments"] == 2
        assert stats["bytes"] == len(b'{"a":1}\n') + len(b'{"a":2}\n{"a":3}\n')
        assert stats["oldest_seconds"] > 0


# ---------------------------------------------------------------------------
# 8 — /health depth (D3)
# ---------------------------------------------------------------------------


class TestQueueStats:
    @pytest.mark.asyncio
    async def test_get_queue_stats_forces_a_scan_when_never_sampled(self):
        await disk_queue.enqueue_click({"a": 1})
        disk_queue._get_writer().force_finalize_for_tests()

        stats = await disk_queue.get_queue_stats()
        assert stats["segments"] == 1
        assert stats["bytes"] > 0
        assert stats["oldest_seconds"] is not None

    @pytest.mark.asyncio
    async def test_get_queue_stats_prefers_cache_over_live_scan(self):
        disk_queue._cached_queue_stats = {
            "segments": 42, "bytes": 4096, "oldest_seconds": 12.0,
        }
        assert await disk_queue.get_queue_stats() == {
            "segments": 42, "bytes": 4096, "oldest_seconds": 12.0,
        }

    def test_cached_stats_default_when_never_sampled(self):
        assert disk_queue.get_cached_queue_stats() == {
            "segments": 0, "bytes": 0, "oldest_seconds": None,
        }


# ---------------------------------------------------------------------------
# 6 — partial-last-line torn-tail truncation (B2)
# ---------------------------------------------------------------------------


class TestTornTailTruncation:
    def test_incomplete_last_line_is_truncated(self, tmp_path):
        path = tmp_path / "x.ndjson.wip"
        good = b'{"click_id":"a"}\n{"click_id":"b"}\n'
        torn = b'{"click_id":"c"'  # no trailing newline, incomplete JSON
        path.write_bytes(good + torn)

        dropped = disk_queue._truncate_torn_tail_sync(path)

        assert dropped == len(torn)
        assert path.read_bytes() == good

    def test_well_formed_trailing_line_without_newline_is_kept(self, tmp_path):
        """A COMPLETE JSON object with no trailing newline is still a
        legitimate line (e.g. the process was killed right after the
        write() but the buffer had exactly one full record) — B2 only
        drops what fails to parse, never a well-formed record."""
        path = tmp_path / "x.ndjson.wip"
        path.write_bytes(b'{"click_id":"a"}\n{"click_id":"b"}')

        dropped = disk_queue._truncate_torn_tail_sync(path)

        assert dropped == 0
        assert disk_queue._read_complete_lines_sync(path) == [
            b'{"click_id":"a"}', b'{"click_id":"b"}',
        ]

    def test_clean_file_is_never_modified(self, tmp_path):
        path = tmp_path / "x.ndjson.wip"
        content = b'{"click_id":"a"}\n{"click_id":"b"}\n'
        path.write_bytes(content)

        dropped = disk_queue._truncate_torn_tail_sync(path)

        assert dropped == 0
        assert path.read_bytes() == content

    def test_empty_file_is_a_noop(self, tmp_path):
        path = tmp_path / "x.ndjson.wip"
        path.write_bytes(b"")
        assert disk_queue._truncate_torn_tail_sync(path) == 0

    def test_torn_tail_op_tagged(self, tmp_path, monkeypatch):
        path = tmp_path / "x.ndjson.wip"
        path.write_bytes(b'{"click_id":"a"}\nnot-json-and-no-newline')

        captured = {}

        def _capture(op_name, message, level="warning", **extras):
            captured["op"] = op_name
            captured["extras"] = extras

        monkeypatch.setattr(disk_queue, "capture_op_msg", _capture)
        disk_queue._truncate_torn_tail_sync(path)

        assert captured["op"] == disk_queue.OP_SEGMENT_TORN_TAIL
        assert captured["extras"]["dropped_bytes"] == len(b"not-json-and-no-newline")


# ---------------------------------------------------------------------------
# 3 — orphan adoption (B1)
# ---------------------------------------------------------------------------


class TestOrphanAdoption:
    @pytest.mark.asyncio
    async def test_aged_orphan_finalized_segment_is_adopted_and_drained(self, monkeypatch):
        root = _root()
        root.mkdir(parents=True)
        old_epoch = int(time.time()) - 3600  # 1h old — well past the age floor
        orphan = root / f"{old_epoch}-999999-000001.ndjson"
        orphan.write_bytes(b'{"click_id":"orphan-1"}\n')

        adopted = await disk_queue.adopt_orphan_segments()

        assert adopted == [f"{old_epoch}-999999"]
        assert not orphan.exists()  # renamed away under my own prefix
        my_prefix = disk_queue._worker_prefix()
        renamed = sorted(root.glob(f"{my_prefix}-adopted-*.ndjson"))
        assert len(renamed) == 1

        redis = _make_redis_mock()
        stats = await disk_queue.drain_to_redis(redis)
        assert stats["drained"] == 1
        assert redis.xadd.await_count == 1
        assert renamed[0].exists() is False  # drained + deleted

    @pytest.mark.asyncio
    async def test_young_orphan_is_left_for_a_same_boot_sibling(self):
        """A prefix younger than `disk_orphan_adopt_min_age_seconds` is
        assumed to belong to a sibling worker of the SAME boot
        generation still starting up — must NOT be adopted yet."""
        root = _root()
        root.mkdir(parents=True)
        fresh_epoch = int(time.time())
        sibling = root / f"{fresh_epoch}-888888-000001.ndjson"
        sibling.write_bytes(b'{"click_id":"sibling-1"}\n')

        adopted = await disk_queue.adopt_orphan_segments()

        assert adopted == []
        assert sibling.exists()

    @pytest.mark.asyncio
    async def test_torn_wip_orphan_is_truncated_then_adopted(self):
        """A dead worker's segment that was STILL OPEN (`.wip`) at
        crash time may have a torn tail (B2) — adoption must truncate
        it BEFORE claiming it, and the result must still replay
        cleanly."""
        root = _root()
        root.mkdir(parents=True)
        old_epoch = int(time.time()) - 3600
        wip = root / f"{old_epoch}-777777-000001.ndjson.wip"
        wip.write_bytes(b'{"click_id":"a"}\n{"click_id":"b"' )  # torn tail

        adopted = await disk_queue.adopt_orphan_segments()
        assert adopted == [f"{old_epoch}-777777"]

        my_prefix = disk_queue._worker_prefix()
        finalized = sorted(root.glob(f"{my_prefix}-adopted-*.ndjson"))
        assert len(finalized) == 1
        assert disk_queue._read_complete_lines_sync(finalized[0]) == [b'{"click_id":"a"}']

        redis = _make_redis_mock()
        stats = await disk_queue.drain_to_redis(redis)
        assert stats["drained"] == 1

    @pytest.mark.asyncio
    async def test_own_prefix_never_treated_as_orphan(self):
        await disk_queue.enqueue_click({"click_id": "mine"})
        adopted = await disk_queue.adopt_orphan_segments()
        assert adopted == []
        assert len(_wip_segments()) == 1  # untouched, still mine

    @pytest.mark.asyncio
    async def test_offset_sidecar_carried_over_on_adoption(self):
        """An orphan's replay progress (offset sidecar) must survive
        adoption — otherwise re-adopting the same segment across
        restarts would re-replay already-confirmed lines."""
        root = _root()
        root.mkdir(parents=True)
        old_epoch = int(time.time()) - 3600
        orphan = root / f"{old_epoch}-666666-000001.ndjson"
        orphan.write_bytes(b'{"click_id":"a"}\n{"click_id":"b"}\n')
        (root / f"{old_epoch}-666666-000001.ndjson.offset").write_text("1")

        await disk_queue.adopt_orphan_segments()

        my_prefix = disk_queue._worker_prefix()
        finalized = sorted(root.glob(f"{my_prefix}-adopted-*.ndjson"))[0]
        assert disk_queue._read_offset_sync(finalized) == 1

        redis = _make_redis_mock()
        stats = await disk_queue.drain_to_redis(redis)
        # Only the SECOND line (offset already at 1) should replay.
        assert stats["drained"] == 1
        assert redis.xadd.await_count == 1
        assert json.loads(redis.xadd.call_args.args[1]["data"]) == {"click_id": "b"}

    @pytest.mark.asyncio
    async def test_adoption_op_tagged(self, monkeypatch):
        root = _root()
        root.mkdir(parents=True)
        old_epoch = int(time.time()) - 3600
        (root / f"{old_epoch}-555555-000001.ndjson").write_bytes(b'{"a":1}\n')

        captured = {}

        def _capture(op_name, message, level="warning", **extras):
            captured["op"] = op_name

        monkeypatch.setattr(disk_queue, "capture_op_msg", _capture)
        await disk_queue.adopt_orphan_segments()

        assert captured["op"] == disk_queue.OP_SEGMENT_ORPHAN_ADOPTED

    @pytest.mark.asyncio
    async def test_young_orphan_deferred_then_adopted_on_retry(self, monkeypatch):
        """HIGH fix (gate-E review, 2026-07-07) — adoption must RETRY,
        not run once at boot. A deferred (too-young) orphan must become
        adoptable on a LATER call once it's genuinely old enough — this
        is what makes `run_drainer`'s per-cycle retry meaningful rather
        than a no-op repeat of the same boot-time check."""
        root = _root()
        root.mkdir(parents=True)
        fresh_epoch = int(time.time())
        sibling = root / f"{fresh_epoch}-888888-000001.ndjson"
        sibling.write_bytes(b'{"click_id":"sibling-1"}\n')

        adopted = await disk_queue.adopt_orphan_segments()
        assert adopted == []
        assert sibling.exists()

        # Simulate enough real time having passed (equivalently: an
        # operator lowering the grace period) by shrinking the min-age
        # floor rather than sleeping in a test — what matters is that
        # CALLING ADOPT AGAIN, once conditions permit, actually adopts
        # what was previously deferred.
        monkeypatch.setattr(disk_queue.settings, "disk_orphan_adopt_min_age_seconds", 0)
        adopted = await disk_queue.adopt_orphan_segments()
        assert adopted == [f"{fresh_epoch}-888888"]
        assert not sibling.exists()

    @pytest.mark.asyncio
    async def test_two_hop_adoption_x_dies_before_draining_y_reclaims_zero_loss(self):
        """CRITICAL regression (gate-E review, 2026-07-07): the
        adoption rename used to embed the FULL previous filename as a
        plain infix (`{new}-adopted-{old_full_name}`), which broke
        `_SEGMENT_RE` (a non-digit `-adopted-` infix in the middle of
        an all-digits pattern never matches) — so a segment that got
        re-orphaned (its adopter died before draining it) became
        PERMANENTLY invisible to every future orphan scan. This
        simulates exactly that: worker A died, worker X adopted A's
        segment but ALSO died before draining it, and worker Y (this
        test process) must still discover + re-adopt + drain it with
        zero loss."""
        root = _root()
        root.mkdir(parents=True)
        orig_epoch = int(time.time()) - 7200  # A, the ORIGINAL dead worker
        x_epoch = int(time.time()) - 3600     # X adopted A's orphan, then X ALSO died
        x_prefix = f"{x_epoch}-222222"
        # State AFTER hop 1 (X adopted A's orphan) without ever
        # draining it — X's own prefix, A's origin preserved.
        hop1_name = f"{x_prefix}-adopted-{orig_epoch}-111111-000001.ndjson"
        (root / hop1_name).write_bytes(b'{"click_id":"two-hop-1"}\n')

        adopted = await disk_queue.adopt_orphan_segments()
        assert adopted == [x_prefix], (
            "Y must discover X's already-adopted-but-undrained segment "
            "as an orphan — the -adopted- infix must not defeat "
            "discovery on a second hop."
        )

        my_prefix = disk_queue._worker_prefix()
        hop2 = sorted(root.glob(f"{my_prefix}-adopted-*.ndjson"))
        assert len(hop2) == 1, (
            f"Expected exactly one re-adopted segment under Y's "
            f"prefix, found .ndjson files: "
            f"{[p.name for p in root.glob('*.ndjson')]}"
        )
        # The ORIGIN identity (A, the first dead worker) must be
        # preserved verbatim across the hop — never X's (also-dead)
        # prefix, and the filename must not grow with each hop.
        assert hop2[0].name == (
            f"{my_prefix}-adopted-{orig_epoch}-111111-000001.ndjson"
        )

        redis = _make_redis_mock()
        stats = await disk_queue.drain_to_redis(redis)
        assert stats["drained"] == 1
        assert redis.xadd.await_count == 1
        assert json.loads(redis.xadd.call_args.args[1]["data"]) == {
            "click_id": "two-hop-1",
        }
        assert not hop2[0].exists()  # fully drained + deleted

    def test_canonical_adopted_name_preserves_origin_across_hops(self):
        """Unit-level pin on the naming primitive itself: re-adopting
        an ALREADY-adopted name must replace only the leading
        (current-owner) prefix, keeping the embedded origin and seq
        untouched — this is what bounds filename length regardless of
        hop count."""
        plain = "1000-111-000042.ndjson"
        assert disk_queue._canonical_adopted_name(plain, "2000-222") == (
            "2000-222-adopted-1000-111-000042.ndjson"
        )

        already_adopted = "2000-222-adopted-1000-111-000042.ndjson"
        assert disk_queue._canonical_adopted_name(already_adopted, "3000-333") == (
            "3000-333-adopted-1000-111-000042.ndjson"
        )

        assert disk_queue._canonical_adopted_name("not-a-segment.json", "3000-333") is None


# ---------------------------------------------------------------------------
# 10 — mechanical liveness (gate-E round 2 CRITICAL) — orphan adoption must
# NEVER touch a live sibling. Fable's independent adversarial pass found the
# age-inferred check (round 1) treated ANY live sibling older than the
# min-age floor as "dead" — the epoch in a segment's NAME is the writer's
# BOOT time, not the file's age. Combined with round 1's own periodic-retry
# fix, this became CONTINUOUS mass live-sibling theft under sustained WC=8
# spill, including catastrophic OPEN `.wip` theft (silent loss twice over).
# ---------------------------------------------------------------------------


class TestMechanicalLiveness:
    """Fast, deterministic coverage of `_worker_is_dead_sync` — the
    arithmetic the real-process regression below exercises end-to-end."""

    def test_fresh_heartbeat_means_alive_regardless_of_segment_name_age(self):
        """The core bug this fix closes: a segment-name epoch can be
        arbitrarily old for a perfectly live, long-running worker — a
        fresh heartbeat must prove liveness regardless."""
        root = _root()
        root.mkdir(parents=True)
        ancient_prefix = f"{int(time.time()) - 7200}-424242"
        disk_queue._heartbeat_path(root, ancient_prefix).touch()

        assert disk_queue._worker_is_dead_sync(root, ancient_prefix, time.time()) is False

    def test_stale_heartbeat_means_dead(self, monkeypatch):
        monkeypatch.setattr(disk_queue.settings, "disk_queue_drain_interval_seconds", 1)
        monkeypatch.setattr(disk_queue.settings, "disk_orphan_heartbeat_stale_multiplier", 3.0)
        root = _root()
        root.mkdir(parents=True)
        prefix = f"{int(time.time())}-424242"
        hb = disk_queue._heartbeat_path(root, prefix)
        hb.touch()
        stale_mtime = time.time() - 10  # well past 3 * 1s
        os.utime(hb, (stale_mtime, stale_mtime))

        assert disk_queue._worker_is_dead_sync(root, prefix, time.time()) is True

    def test_missing_heartbeat_means_dead(self):
        """Legacy fallback — a pre-this-fix orphan (whose segments
        already cleared the age floor) never had a heartbeat file at
        all; a live current-code worker can never reach this state
        (its heartbeat always exists at least as early as its first
        segment — see `_ensure_open_sync`)."""
        root = _root()
        root.mkdir(parents=True)
        assert disk_queue._worker_is_dead_sync(root, "123-456", time.time()) is True

    @pytest.mark.asyncio
    async def test_live_prefix_with_fresh_heartbeat_is_never_adopted_even_if_name_is_old(
        self, monkeypatch,
    ):
        """End-to-end via the public `adopt_orphan_segments()` entry
        point — a segment whose NAME embeds an ancient epoch (would
        have passed the OLD age-only check) must still be left
        completely alone, `.wip` included, as long as its heartbeat is
        fresh."""
        monkeypatch.setattr(disk_queue.settings, "disk_orphan_adopt_min_age_seconds", 0)
        root = _root()
        root.mkdir(parents=True)
        old_epoch = int(time.time()) - 7200
        live_prefix = f"{old_epoch}-424242"
        segment = root / f"{live_prefix}-000001.ndjson"
        segment.write_bytes(b'{"click_id":"still-live"}\n')
        wip = root / f"{live_prefix}-000002.ndjson.wip"
        wip.write_bytes(b'{"click_id":"still-live-2"}\n')
        disk_queue._heartbeat_path(root, live_prefix).touch()

        adopted = await disk_queue.adopt_orphan_segments()

        assert adopted == [], (
            "A live sibling (fresh heartbeat) must NEVER be adopted, "
            "even when its segment-name epoch is old enough that the "
            "pre-round-2 age-only check would have claimed it."
        )
        assert segment.exists()
        assert wip.exists()

    @pytest.mark.asyncio
    async def test_opening_a_segment_creates_this_workers_heartbeat(self):
        """Closes the boot-race window: a segment file for a prefix
        must never exist on disk before that SAME prefix's heartbeat
        file does — otherwise a peer's orphan scan could catch a
        genuinely-live, just-started worker in a 'segment exists, no
        heartbeat yet' gap."""
        await disk_queue.enqueue_click({"click_id": "a"})
        my_prefix = disk_queue._worker_prefix()
        assert disk_queue._heartbeat_path(_root(), my_prefix).exists()


class TestLiveSiblingProcessTheft:
    """CRITICAL regression (Fable's independent adversarial pass,
    gate-E round 2, 2026-07-07) — a REAL live process (an actually
    killable pid), not a simulated dead one, holding an open `.wip`
    must survive many adoption passes UNTOUCHED; once genuinely
    killed, everything (including its formerly-open `.wip`) must be
    adopted and drained with zero loss. The round-1 two-hop regression
    only ever simulated DEAD owners (fresh synthetic prefixes with no
    process behind them) — nothing exercised a real live sibling."""

    @pytest.mark.asyncio
    async def test_live_sibling_survives_many_passes_then_dead_sibling_fully_adopted(
        self, monkeypatch,
    ):
        monkeypatch.setattr(disk_queue.settings, "disk_orphan_adopt_min_age_seconds", 0)
        monkeypatch.setattr(disk_queue.settings, "disk_queue_drain_interval_seconds", 0.5)
        monkeypatch.setattr(disk_queue.settings, "disk_orphan_heartbeat_stale_multiplier", 3.0)

        root = _root()
        root.mkdir(parents=True)
        service_root = Path(disk_queue.__file__).resolve().parent.parent

        child_script = (
            "import os\n"
            f"os.environ['TDS_DISK_QUEUE_ROOT'] = {str(root)!r}\n"
            "import sys, time\n"
            "from app import disk_queue as dq\n"
            "writer = dq._get_writer()\n"
            "writer._ensure_open_sync()\n"
            'os.write(writer._fd, b\'{"click_id": "live-finalized"}\\n\')\n'
            "os.fsync(writer._fd)\n"
            "writer._size += 200\n"
            "writer._finalize_current_sync()\n"
            "writer._ensure_open_sync()\n"
            'os.write(writer._fd, b\'{"click_id": "live-wip"}\\n\')\n'
            "os.fsync(writer._fd)\n"
            "writer._size += 200\n"
            "sys.stdout.write('READY\\n')\n"
            "sys.stdout.flush()\n"
            "while True:\n"
            "    dq._touch_heartbeat_sync()\n"
            "    time.sleep(0.1)\n"
        )

        proc = subprocess.Popen(
            [sys.executable, "-c", child_script],
            cwd=str(service_root),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            ready_line = await asyncio.wait_for(
                asyncio.to_thread(proc.stdout.readline), timeout=10.0,
            )
            assert ready_line.strip() == "READY", (
                f"Child process failed to start: {proc.stderr.read()}"
            )

            live_prefixes = {p.name.rsplit("-", 1)[0] for p in root.glob("*.ndjson")}
            assert len(live_prefixes) == 1
            live_prefix = next(iter(live_prefixes))

            for _ in range(3):
                adopted = await disk_queue.adopt_orphan_segments()
                assert adopted == [], (
                    "A LIVE sibling's segments (including its open "
                    ".wip) must never be adopted — this would truncate "
                    "a live writer's in-flight tail and/or cause its "
                    "subsequent appends to vanish into an unlinked "
                    "inode once the thief deletes the renamed file."
                )
                await asyncio.sleep(0.7)

            proc.kill()
            proc.wait(timeout=5)

            # Wait past the heartbeat staleness threshold (3 * 0.5s).
            await asyncio.sleep(2.0)

            adopted = await disk_queue.adopt_orphan_segments()
            assert adopted == [live_prefix], (
                f"Expected the now-dead sibling's prefix to be "
                f"adopted, got {adopted}"
            )

            redis = _make_redis_mock()
            stats = await disk_queue.drain_to_redis(redis)
            assert stats["drained"] == 2
            assert stats["failed"] == 0
            shipped = {
                json.loads(c.args[1]["data"])["click_id"]
                for c in redis.xadd.await_args_list
            }
            assert shipped == {"live-finalized", "live-wip"}
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=5)


# ---------------------------------------------------------------------------
# run_drainer — periodic orphan-adoption retry (gate-E HIGH fix) + the
# pre-P2 cancellable/survives-error loop coverage this file had dropped.
# ---------------------------------------------------------------------------


class TestRunDrainerLoop:
    @pytest.mark.asyncio
    async def test_cancellable(self):
        redis = _make_redis_mock()
        task = asyncio.create_task(disk_queue.run_drainer(redis, interval=10))
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    @pytest.mark.asyncio
    async def test_survives_iteration_error(self, monkeypatch):
        call_count = {"n": 0}

        async def flaky_drain(redis):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise RuntimeError("transient")
            return {"drained": 0, "skipped": 0, "failed": 0, "remaining": 0}

        monkeypatch.setattr(disk_queue, "drain_to_redis", flaky_drain)

        redis = _make_redis_mock()
        task = asyncio.create_task(disk_queue.run_drainer(redis, interval=0))
        for _ in range(3):
            await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert call_count["n"] >= 2

    @pytest.mark.asyncio
    async def test_retries_orphan_adoption_every_cycle(self, monkeypatch):
        """HIGH fix (gate-E review, 2026-07-07) — proves the retry
        mechanism is actually wired into the loop, not just callable
        on demand."""
        call_count = {"n": 0}

        async def counting_adopt():
            call_count["n"] += 1
            return []

        monkeypatch.setattr(disk_queue, "adopt_orphan_segments", counting_adopt)

        redis = _make_redis_mock()
        task = asyncio.create_task(disk_queue.run_drainer(redis, interval=0))
        for _ in range(3):
            await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert call_count["n"] >= 2, (
            "run_drainer must call adopt_orphan_segments() every "
            "cycle, not just once at boot."
        )


# ---------------------------------------------------------------------------
# 2 — WC=8 no-race: distinct worker prefixes never collide
# ---------------------------------------------------------------------------


class TestWorkerNaming:
    def test_segment_naming_matches_epoch_pid_seq(self):
        assert disk_queue._SEGMENT_RE.match("1700000000-1234-000001.ndjson")
        assert not disk_queue._SEGMENT_RE.match("1700000000-1234-000001.ndjson.wip")
        assert not disk_queue._SEGMENT_RE.match("not-a-segment.json")

    def test_worker_prefix_is_epoch_dash_pid(self):
        prefix = disk_queue._worker_prefix()
        epoch_str, pid_str = prefix.split("-")
        assert epoch_str.isdigit()
        assert int(pid_str) == os.getpid()

    @pytest.mark.asyncio
    async def test_two_simulated_workers_each_drain_only_their_own_segments(self):
        """Simulates two DIFFERENT (epoch, pid) prefixes writing
        segments into the SAME shared root — the drainer for one
        worker (identified by `_worker_prefix()`) must never touch the
        other's live/finalized files."""
        root = _root()
        root.mkdir(parents=True)
        my_prefix = disk_queue._worker_prefix()
        other_prefix = f"{int(time.time())}-424242"

        (root / f"{my_prefix}-000001.ndjson").write_bytes(b'{"click_id":"mine"}\n')
        (root / f"{other_prefix}-000001.ndjson").write_bytes(b'{"click_id":"other"}\n')

        redis = _make_redis_mock()
        stats = await disk_queue.drain_to_redis(redis)

        assert stats["drained"] == 1
        assert redis.xadd.await_count == 1
        assert json.loads(redis.xadd.call_args.args[1]["data"]) == {"click_id": "mine"}
        # The other worker's (not-yet-orphaned, fresh) segment is untouched.
        assert (root / f"{other_prefix}-000001.ndjson").exists()


# ---------------------------------------------------------------------------
# 7 — replay exactly-once via the offset sidecar (B3)
# ---------------------------------------------------------------------------


class TestReplayExactlyOnce:
    @pytest.mark.asyncio
    async def test_offset_advances_per_line_and_segment_deleted_on_full_drain(self):
        for i in range(3):
            await disk_queue.enqueue_click({"click_id": f"c{i}"})
        disk_queue._get_writer().force_finalize_for_tests()
        seg = _segments()[0]

        redis = _make_redis_mock()
        stats = await disk_queue.drain_to_redis(redis)

        assert stats["drained"] == 3
        assert stats["remaining"] == 0
        assert not seg.exists()
        assert not disk_queue._offset_path_for(seg).exists()

    @pytest.mark.asyncio
    async def test_crash_mid_replay_resumes_from_offset_no_reprocessing_earlier_lines(self):
        for i in range(3):
            await disk_queue.enqueue_click({"click_id": f"c{i}"})
        disk_queue._get_writer().force_finalize_for_tests()
        seg = _segments()[0]

        # Simulate a crash right after line 0's offset was persisted.
        disk_queue._write_offset_sync(seg, 1)

        redis = _make_redis_mock()
        stats = await disk_queue.drain_to_redis(redis)

        assert stats["drained"] == 2  # only lines 1 and 2 replayed
        assert redis.xadd.await_count == 2
        shipped = [json.loads(c.args[1]["data"])["click_id"] for c in redis.xadd.await_args_list]
        assert shipped == ["c1", "c2"]

    @pytest.mark.asyncio
    async def test_redis_failure_persists_offset_up_to_last_success_only(self):
        for i in range(3):
            await disk_queue.enqueue_click({"click_id": f"c{i}"})
        disk_queue._get_writer().force_finalize_for_tests()
        seg = _segments()[0]

        redis = _make_redis_mock()
        redis.xadd.side_effect = [None, ConnectionError("down")]
        stats = await disk_queue.drain_to_redis(redis)

        assert stats["drained"] == 1
        assert stats["failed"] == 1
        assert disk_queue._read_offset_sync(seg) == 1
        assert seg.exists()  # not fully drained — stays for next cycle

    @pytest.mark.asyncio
    async def test_duplicate_click_id_skipped_via_dedup_still_advances_offset(self, monkeypatch):
        """P2 fix (2026-07-07): the dedup check keys on `click:shipped`
        (GET), not `click:seen` (SETNX) — a click already CONFIRMED
        shipped (by this or another path) is skipped without a
        re-XADD."""
        monkeypatch.setattr(disk_queue.settings, "click_dedup_ttl_seconds", 300)
        await disk_queue.enqueue_click({"click_id": "dup-1"})
        disk_queue._get_writer().force_finalize_for_tests()
        seg = _segments()[0]

        redis = _make_redis_mock()
        redis.get = AsyncMock(return_value="1")  # click:shipped already set
        stats = await disk_queue.drain_to_redis(redis)

        assert stats["drained"] == 0
        assert stats["skipped"] == 1
        redis.xadd.assert_not_awaited()
        assert not seg.exists()  # fully "processed" (skipped counts toward completion)

    @pytest.mark.asyncio
    async def test_offset_persisted_in_batches_not_per_line(self, monkeypatch):
        """MEDIUM perf fix (gate-E review, 2026-07-07) — the offset
        sidecar is persisted every `disk_replay_offset_batch_lines`
        lines, not every single line (which is correct but expensive —
        a full open+write+fsync+close+rename per line — under a large
        backlog)."""
        monkeypatch.setattr(disk_queue.settings, "disk_replay_offset_batch_lines", 3)
        for i in range(7):
            await disk_queue.enqueue_click({"click_id": f"c{i}"})
        disk_queue._get_writer().force_finalize_for_tests()

        write_calls: list[int] = []
        original = disk_queue._write_offset_sync

        def counting_write_offset(path, offset):
            write_calls.append(offset)
            return original(path, offset)

        monkeypatch.setattr(disk_queue, "_write_offset_sync", counting_write_offset)

        redis = _make_redis_mock()
        stats = await disk_queue.drain_to_redis(redis)

        assert stats["drained"] == 7
        # Batched every 3 lines: flushed mid-loop at 3 and 6; the final
        # line (index 6, offset 7) doesn't need a flush since the
        # segment is fully drained and deleted outright.
        assert write_calls == [3, 6], (
            f"Expected exactly 2 batched offset writes (at 3 and 6), "
            f"got {write_calls} — per-line would produce 7."
        )

    @pytest.mark.asyncio
    async def test_redis_failure_flushes_offset_immediately_not_batched(self, monkeypatch):
        """A Redis-IMPAIRMENT break (the process stays alive, it just
        stops) must NOT accept the batching window — only a hard crash
        does. Confirms batching didn't regress
        test_redis_failure_persists_offset_up_to_last_success_only's
        guarantee even with a batch size bigger than the backlog."""
        monkeypatch.setattr(disk_queue.settings, "disk_replay_offset_batch_lines", 50)
        for i in range(3):
            await disk_queue.enqueue_click({"click_id": f"c{i}"})
        disk_queue._get_writer().force_finalize_for_tests()
        seg = _segments()[0]

        redis = _make_redis_mock()
        redis.xadd.side_effect = [None, ConnectionError("down")]
        stats = await disk_queue.drain_to_redis(redis)

        assert stats["drained"] == 1
        assert stats["failed"] == 1
        assert disk_queue._read_offset_sync(seg) == 1
        assert seg.exists()


# ---------------------------------------------------------------------------
# CRITICAL regression (GTD routing-audit CRITICAL-disk-fallback-silent-loss,
# 2026-07-07) — replay must key on click:shipped, NEVER click:seen. Every
# disk-fallback click's `click:seen` marker is ALREADY planted by its own
# /decide call (main._acquire_click_dedup runs BEFORE the stream-vs-disk
# decision), so gating replay on that key always false-positived as
# "duplicate" and silently dropped the click — 100%-reproducible, live-
# confirmed 0/40. This test FAILS against the old (click:seen-gated) logic
# and PASSES against the fix.
# ---------------------------------------------------------------------------


class TestClickShippedNotClickSeenRegression:
    @pytest.mark.asyncio
    async def test_pre_planted_click_seen_marker_does_not_block_replay(self):
        """Simulates EXACTLY what happens for every disk-fallback click
        today: its own /decide call already ran `_acquire_click_dedup`
        (SET click:seen NX EX 86400) BEFORE ever reaching the stream-
        write decision. The replayed click must still ship exactly
        once — the pre-planted click:seen marker must have zero effect
        on the (correctly click:shipped-gated) replay path."""
        import fakeredis.aioredis

        redis = fakeredis.aioredis.FakeRedis(decode_responses=True)

        click_id = "spilled-click-1"
        # Exactly what main._acquire_click_dedup does at /decide time,
        # for EVERY click, before the stream-vs-disk-fallback decision.
        await redis.set(f"click:seen:{click_id}", "1", nx=True, ex=86400)

        await disk_queue.enqueue_click({"click_id": click_id})
        disk_queue._get_writer().force_finalize_for_tests()

        stats = await disk_queue.drain_to_redis(redis)

        assert stats["drained"] == 1, (
            "The click MUST ship on replay even though its own /decide "
            "already planted click:seen — gating replay on that key "
            "silently drops every disk-fallback click (0 loss is the "
            "whole point of the disk fallback)."
        )
        assert stats["skipped"] == 0
        assert await redis.xlen("stream:clicks") == 1
        entries = await redis.xrange("stream:clicks")
        shipped = json.loads(entries[0][1]["data"])
        assert shipped["click_id"] == click_id

    @pytest.mark.asyncio
    async def test_click_shipped_marker_prevents_a_genuine_re_ship(self):
        """Sanity counterpart — click:shipped (the CORRECT key, set
        only after a confirmed-successful XADD) DOES suppress a replay
        that would otherwise duplicate an already-shipped click."""
        import fakeredis.aioredis

        redis = fakeredis.aioredis.FakeRedis(decode_responses=True)

        click_id = "already-shipped-1"
        await redis.set(f"click:shipped:{click_id}", "1", ex=86400)

        await disk_queue.enqueue_click({"click_id": click_id})
        disk_queue._get_writer().force_finalize_for_tests()

        stats = await disk_queue.drain_to_redis(redis)

        assert stats["drained"] == 0
        assert stats["skipped"] == 1
        assert await redis.xlen("stream:clicks") == 0


# ---------------------------------------------------------------------------
# 4 — legacy *.json migration (D1)
# ---------------------------------------------------------------------------


class TestLegacyMigration:
    @pytest.mark.asyncio
    async def test_legacy_json_files_still_drain(self):
        root = _root()
        legacy_dir = root / "2026-05-09"
        legacy_dir.mkdir(parents=True)
        (legacy_dir / "a.json").write_text(json.dumps({"click_id": "legacy-a"}))
        (legacy_dir / "b.json").write_text(json.dumps({"click_id": "legacy-b"}))

        redis = _make_redis_mock()
        stats = await disk_queue.drain_to_redis(redis)

        assert stats["drained"] == 2
        assert redis.xadd.await_count == 2
        assert list(legacy_dir.glob("*.json")) == []

    @pytest.mark.asyncio
    async def test_legacy_and_segments_drain_in_the_same_cycle(self):
        root = _root()
        legacy_dir = root / "2026-05-09"
        legacy_dir.mkdir(parents=True)
        (legacy_dir / "a.json").write_text(json.dumps({"click_id": "legacy-a"}))

        await disk_queue.enqueue_click({"click_id": "seg-a"})
        disk_queue._get_writer().force_finalize_for_tests()

        redis = _make_redis_mock()
        stats = await disk_queue.drain_to_redis(redis)

        assert stats["drained"] == 2
        assert redis.xadd.await_count == 2

    @pytest.mark.asyncio
    async def test_legacy_drain_stops_on_first_failure_like_before(self):
        root = _root()
        legacy_dir = root / "2026-05-09"
        legacy_dir.mkdir(parents=True)
        (legacy_dir / "a.json").write_text(json.dumps({"click_id": "legacy-a"}))
        (legacy_dir / "b.json").write_text(json.dumps({"click_id": "legacy-b"}))

        redis = _make_redis_mock(xadd_side_effect=ConnectionError("redis down"))
        stats = await disk_queue.drain_to_redis(redis)

        assert stats["drained"] == 0
        assert stats["failed"] == 1
        assert len(list(legacy_dir.glob("*.json"))) == 2

    @pytest.mark.asyncio
    async def test_legacy_failure_stops_the_whole_cycle_segments_not_attempted(self):
        """A legacy-drain failure must stop the CYCLE (self-limit) —
        segments are left for the next iteration rather than racing an
        impaired Redis further."""
        root = _root()
        legacy_dir = root / "2026-05-09"
        legacy_dir.mkdir(parents=True)
        (legacy_dir / "a.json").write_text(json.dumps({"click_id": "legacy-a"}))

        await disk_queue.enqueue_click({"click_id": "seg-a"})
        disk_queue._get_writer().force_finalize_for_tests()

        redis = _make_redis_mock(xadd_side_effect=ConnectionError("redis down"))
        stats = await disk_queue.drain_to_redis(redis)

        assert stats["failed"] == 1
        assert len(_segments()) == 1  # untouched — legacy failure short-circuited


# ---------------------------------------------------------------------------
# 9 — crash-recovery E2E: kill mid-write -> restart -> adopt -> replay
# ---------------------------------------------------------------------------


class TestCrashRecoveryE2E:
    @pytest.mark.asyncio
    async def test_full_cycle_no_loss_no_double_ship(self):
        """Simulates: worker A writes 2 clicks + crashes mid-write of a
        3rd (torn tail left in a `.wip` file) -> worker B (fresh
        pid/epoch, i.e. THIS test process after a state reset) boots,
        adopts A's orphaned segment, truncates the torn tail, and
        drains everything — exactly the 2 fully-written clicks ship,
        the torn 3rd is dropped loss-free (never acked), nothing ships
        twice."""
        root = _root()
        root.mkdir(parents=True)
        dead_epoch = int(time.time()) - 3600
        crashed_segment = root / f"{dead_epoch}-333333-000001.ndjson.wip"
        crashed_segment.write_bytes(
            b'{"click_id":"e2e-1"}\n{"click_id":"e2e-2"}\n{"click_id":"e2e-3"'
        )

        adopted = await disk_queue.adopt_orphan_segments()
        assert adopted == [f"{dead_epoch}-333333"]

        redis = _make_redis_mock()
        stats = await disk_queue.drain_to_redis(redis)

        assert stats["drained"] == 2
        shipped = [json.loads(c.args[1]["data"])["click_id"] for c in redis.xadd.await_args_list]
        assert shipped == ["e2e-1", "e2e-2"]
        assert _segments() == []  # fully drained + deleted
        assert _wip_segments() == []
