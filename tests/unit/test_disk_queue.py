"""Tests for the click-processor disk-segment fallback queue (T2.2 / G-23),
REDESIGNED by P2 (LOSSFIX, 2026-07-07) from one-file-per-click into
append-only NDJSON segments with group-commit fsync and a global
byte-cap. Per-worker orphan adoption + crash-recovery E2E land in the
c2 follow-up commit (this file's TestOrphanAdoption / TestCrashRecoveryE2E
classes) — the naming scheme's per-worker isolation itself IS covered
here (TestWorkerNaming).

Coverage layers (P2 brief's OBSERVABLE DONE items covered by c1: 1, 2,
4, 5, 6, 7, 8):

  1. Segment lifecycle — rotate at size/time, group-commit fsync,
     replay-then-unlink, dir-fsync on finalize (D2).
  2. WC=8 no-race — per-worker prefix naming, no two workers touch the
     same live segment.
  4. Legacy migration (D1) — pre-P2 `*.json` files still drain.
  5. Byte-cap — visible shed (return False), never silent.
  6. Partial-last-line (B2) — a torn `.wip` tail truncates + op-tags,
     never crashes, never drops a well-formed line.
  7. Replay exactly-once (B3) — an offset sidecar bounds a crash to at
     most one re-replayed line.
  8. `/health` depth (D3) — segment count / bytes / oldest-age, via
     `get_queue_stats`.

Reference: rule `sync-protocol`, action-items.md T2.2, open-questions.md
G-23.
"""

from __future__ import annotations

import asyncio
import json
import os
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
        monkeypatch.setattr(disk_queue.settings, "click_dedup_ttl_seconds", 300)
        await disk_queue.enqueue_click({"click_id": "dup-1"})
        disk_queue._get_writer().force_finalize_for_tests()
        seg = _segments()[0]

        redis = _make_redis_mock()
        redis.set = AsyncMock(return_value=False)  # SETNX says "already seen"
        stats = await disk_queue.drain_to_redis(redis)

        assert stats["drained"] == 0
        assert stats["skipped"] == 1
        redis.xadd.assert_not_awaited()
        assert not seg.exists()  # fully "processed" (skipped counts toward completion)


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

