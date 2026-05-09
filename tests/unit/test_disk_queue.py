"""Tests for the click-processor disk fallback queue (T2.2 / G-23).

Closes the click-loss gap on `/decide` when Redis is unreachable.
This module's behaviour is the contract the operator relies on for
"we don't lose a single click during a Redis outage".

Coverage layers:

  * Atomic-write contract — `_write_file_sync` writes via .tmp +
    fsync + rename so a crash mid-write never produces a half-
    written `.json` file (drainer only reads `.json`).

  * Public API behaviour — `enqueue_click`, `drain_to_redis`,
    `get_queue_size`. These run against a tmp_path-rooted queue
    so the real filesystem stays untouched.

  * Cap rejection — at-cap enqueues fail loud (return False, log
    CRITICAL, Sentry-capture) instead of silently rotating the
    oldest. Operator's choice on outage longevity.

  * Drainer integration — `run_drainer` is wired into the FastAPI
    lifespan with a cancellable asyncio task; source-pinned so a
    refactor can't drop the drainer silently.

  * `/decide` fallback — on XADD failure, the click is enqueued
    to disk; source-pinned by inspecting the handler.

Reference: rule `sync-protocol`, action-items.md T2.2,
open-questions.md G-23.
"""

from __future__ import annotations

import asyncio
import inspect
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from app import disk_queue


@pytest.fixture(autouse=True)
def _reset_disk_queue_state(tmp_path, monkeypatch):
    """Per-test isolation: rebase the queue root on tmp_path and
    reset the in-memory size counter. Without this, tests would
    pollute each other's state because the module-level counter
    persists across tests."""
    monkeypatch.setattr(
        disk_queue.settings, "disk_queue_root", str(tmp_path / "click-queue"),
    )
    # Default cap big enough not to trip in normal tests; override
    # per-test where needed.
    monkeypatch.setattr(
        disk_queue.settings, "disk_queue_max_files", 10_000,
    )
    monkeypatch.setattr(
        disk_queue.settings, "stream_clicks_maxlen", 1_000_000,
    )
    disk_queue._reset_state_for_tests()
    yield
    disk_queue._reset_state_for_tests()


# ---------------------------------------------------------------------------
# Atomic write
# ---------------------------------------------------------------------------


class TestAtomicWrite:
    def test_write_creates_target_file(self, tmp_path):
        target = tmp_path / "out" / "x.json"
        disk_queue._write_file_sync(target, b'{"k": 1}')
        assert target.read_bytes() == b'{"k": 1}'

    def test_write_creates_parent_dir(self, tmp_path):
        target = tmp_path / "deep" / "nested" / "out.json"
        disk_queue._write_file_sync(target, b"x")
        assert target.exists()
        assert target.parent.is_dir()

    def test_no_tmp_file_after_success(self, tmp_path):
        """The .tmp intermediate must be renamed away — not left
        behind. Drainer ignores .tmp files; a stale .tmp would
        accumulate forever otherwise."""
        target = tmp_path / "x.json"
        disk_queue._write_file_sync(target, b"data")

        tmp_files = list(tmp_path.glob("*.tmp"))
        assert tmp_files == [], (
            f"Expected no .tmp residue; found {tmp_files!r}."
        )

    def test_overwrites_existing_file(self, tmp_path):
        """Same path → overwrite, not append. UUID suffix prevents
        same-second collisions, but defensive overwrite semantics
        mean a buggy retry that hits the same path doesn't double
        the data."""
        target = tmp_path / "x.json"
        disk_queue._write_file_sync(target, b"first")
        disk_queue._write_file_sync(target, b"second")
        assert target.read_bytes() == b"second"

    def test_file_mode_is_owner_read_write_only(self, tmp_path):
        """Audit fix (Agent 2 HIGH-2, 2026-05-09): click records
        contain PII (IP, geo, full UA, advertiser-supplied
        identifiers in query_params). Files MUST be 0o600 — owner
        read+write only. World-readable 0o644 would let any
        co-located process / shared bind-mount read every queued
        click during a Redis outage.
        """
        target = tmp_path / "queue" / "x.json"
        disk_queue._write_file_sync(target, b'{"k": 1}')

        # Stat the file mode — mask out the file-type bits, keep
        # only the permission bits.
        import os as _os
        mode = _os.stat(target).st_mode & 0o777
        assert mode == 0o600, (
            f"Expected 0o600 (owner rw only) for PII-bearing click "
            f"records; got {oct(mode)}. World-readable mode leaks "
            f"every queued click to any local process."
        )

    def test_parent_dir_mode_is_owner_only(self, tmp_path):
        """Audit fix (Agent 2 HIGH-2, 2026-05-09): the parent
        directory listing leaks queue depth + filenames (which
        encode timestamps + UUIDs — useful for an attacker
        timing-correlating clicks). Mode 0o700 — owner-only
        access at the directory level too.
        """
        target = tmp_path / "queue" / "today" / "x.json"
        disk_queue._write_file_sync(target, b'{"k": 1}')

        import os as _os
        mode = _os.stat(target.parent).st_mode & 0o777
        # We require <= 0o700 — `0o700` is the goal but if the
        # process umask is more restrictive (rare), accept that
        # as still secure.
        assert mode & 0o077 == 0, (
            f"Parent dir {target.parent} leaks read access to "
            f"group/other (mode {oct(mode)}). Click queue must "
            f"be owner-only at the directory level."
        )


# ---------------------------------------------------------------------------
# enqueue_click
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_enqueue_creates_one_file_per_call():
    record = {"click_id": "abc", "campaign_id": 1, "ts": "2026-05-09T12:00:00Z"}

    assert await disk_queue.enqueue_click(record) is True
    assert await disk_queue.enqueue_click(record) is True

    files = list(Path(disk_queue.settings.disk_queue_root).rglob("*.json"))
    assert len(files) == 2


@pytest.mark.asyncio
async def test_enqueue_payload_round_trips_via_json():
    record = {"click_id": "abc", "country": "PL", "weight": 0.42}

    assert await disk_queue.enqueue_click(record) is True

    files = list(Path(disk_queue.settings.disk_queue_root).rglob("*.json"))
    assert len(files) == 1
    decoded = json.loads(files[0].read_text())
    assert decoded == record


@pytest.mark.asyncio
async def test_enqueue_handles_non_serializable_via_str_default():
    """`json.dumps(..., default=str)` is the same shape used by
    /decide on the happy path — keeps the disk-queue payload
    bit-for-bit identical to what would have gone to Redis. Pin
    this so a refactor that changes the serializer doesn't break
    drainer's XADD shape."""
    from datetime import datetime

    record = {"click_id": "abc", "ts": datetime(2026, 5, 9, 12, 0, 0)}
    assert await disk_queue.enqueue_click(record) is True

    files = list(Path(disk_queue.settings.disk_queue_root).rglob("*.json"))
    assert len(files) == 1
    # Datetime serialised via str(...) → ISO-ish string, never raises.
    decoded = json.loads(files[0].read_text())
    assert decoded["click_id"] == "abc"
    assert isinstance(decoded["ts"], str)


@pytest.mark.asyncio
async def test_enqueue_returns_false_on_cap(monkeypatch):
    """At cap, enqueue REJECTS rather than silently rotating
    oldest. The operator must see the loud failure to know an
    outage is exceeding capacity."""
    monkeypatch.setattr(disk_queue.settings, "disk_queue_max_files", 2)

    assert await disk_queue.enqueue_click({"a": 1}) is True
    assert await disk_queue.enqueue_click({"a": 2}) is True
    # Third hits the cap.
    assert await disk_queue.enqueue_click({"a": 3}) is False

    # Files on disk reflect what was accepted, not what was tried.
    files = list(Path(disk_queue.settings.disk_queue_root).rglob("*.json"))
    assert len(files) == 2


@pytest.mark.asyncio
async def test_enqueue_unbounded_cap_zero(monkeypatch):
    """`disk_queue_max_files=0` disables the cap (operator opt-in
    for unbounded — strongly discouraged but documented)."""
    monkeypatch.setattr(disk_queue.settings, "disk_queue_max_files", 0)

    for i in range(5):
        assert await disk_queue.enqueue_click({"i": i}) is True

    files = list(Path(disk_queue.settings.disk_queue_root).rglob("*.json"))
    assert len(files) == 5


# ---------------------------------------------------------------------------
# get_queue_size
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_queue_size_starts_at_zero_on_fresh():
    assert await disk_queue.get_queue_size() == 0


@pytest.mark.asyncio
async def test_get_queue_size_tracks_enqueues():
    await disk_queue.enqueue_click({"a": 1})
    await disk_queue.enqueue_click({"b": 2})
    await disk_queue.enqueue_click({"c": 3})

    assert await disk_queue.get_queue_size() == 3


@pytest.mark.asyncio
async def test_get_queue_size_reflects_pre_existing_files():
    """A click-processor restart should pick up pre-existing files
    from the previous run — `init_queue_size_once` does a single
    rglob to seed the counter."""
    # Pre-create files BEFORE first call to enqueue/get_queue_size.
    root = Path(disk_queue.settings.disk_queue_root)
    today = root / "2026-05-09"
    today.mkdir(parents=True)
    (today / "a.json").write_text('{"existing": 1}')
    (today / "b.json").write_text('{"existing": 2}')

    # Reset state so init runs fresh.
    disk_queue._reset_state_for_tests()

    assert await disk_queue.get_queue_size() == 2


# ---------------------------------------------------------------------------
# drain_to_redis
# ---------------------------------------------------------------------------


def _make_redis_mock(xadd_side_effect=None) -> AsyncMock:
    """AsyncMock standing in for the asyncio Redis client. By
    default xadd() succeeds; pass `xadd_side_effect=Exception(...)`
    to simulate Redis still impaired."""
    redis = AsyncMock()
    if xadd_side_effect is not None:
        redis.xadd.side_effect = xadd_side_effect
    return redis


@pytest.mark.asyncio
async def test_drain_replays_files_to_redis():
    await disk_queue.enqueue_click({"id": 1})
    await disk_queue.enqueue_click({"id": 2})
    await disk_queue.enqueue_click({"id": 3})

    redis = _make_redis_mock()
    stats = await disk_queue.drain_to_redis(redis)

    assert stats["drained"] == 3
    assert stats["failed"] == 0
    assert stats["remaining"] == 0
    assert redis.xadd.await_count == 3

    # All three files should be deleted from disk.
    files = list(Path(disk_queue.settings.disk_queue_root).rglob("*.json"))
    assert files == []


@pytest.mark.asyncio
async def test_drain_xadd_call_shape_matches_decide_path():
    """Drainer's XADD shape MUST be identical to /decide's so the
    stream contains uniform records — downstream collectors don't
    care which path the click took."""
    await disk_queue.enqueue_click({"id": 99, "country": "DE"})

    redis = _make_redis_mock()
    await disk_queue.drain_to_redis(redis)

    # One call, with stream:clicks key + maxlen + approximate=True.
    call = redis.xadd.call_args
    assert call.args[0] == "stream:clicks"
    assert "data" in call.args[1]
    assert call.kwargs["maxlen"] == 1_000_000
    assert call.kwargs["approximate"] is True

    # Round-trip the JSON payload — drained record == enqueued record.
    decoded = json.loads(call.args[1]["data"])
    assert decoded == {"id": 99, "country": "DE"}


@pytest.mark.asyncio
async def test_drain_stops_on_first_xadd_failure():
    """If Redis is still impaired, the drainer must NOT pound it.
    First failure → break the loop, leave remaining files for the
    next iteration. Otherwise we'd retry every file every 30s
    against a sick Redis until it ACK'd."""
    await disk_queue.enqueue_click({"id": 1})
    await disk_queue.enqueue_click({"id": 2})
    await disk_queue.enqueue_click({"id": 3})

    redis = _make_redis_mock(xadd_side_effect=ConnectionError("redis down"))
    stats = await disk_queue.drain_to_redis(redis)

    assert stats["drained"] == 0
    assert stats["failed"] == 1
    assert stats["remaining"] == 3

    # All three files MUST still be on disk — nothing got drained.
    files = list(Path(disk_queue.settings.disk_queue_root).rglob("*.json"))
    assert len(files) == 3


@pytest.mark.asyncio
async def test_drain_partial_recovery():
    """Redis recovers after the second click — first two drain,
    third stops the loop, third stays on disk."""
    await disk_queue.enqueue_click({"id": 1})
    await disk_queue.enqueue_click({"id": 2})
    await disk_queue.enqueue_click({"id": 3})

    redis = _make_redis_mock()
    # Succeed twice, then fail.
    redis.xadd.side_effect = [None, None, ConnectionError("fail")]

    stats = await disk_queue.drain_to_redis(redis)

    assert stats["drained"] == 2
    assert stats["failed"] == 1
    assert stats["remaining"] == 1


@pytest.mark.asyncio
async def test_drain_empty_queue_returns_zero():
    redis = _make_redis_mock()
    stats = await disk_queue.drain_to_redis(redis)

    assert stats == {
        "drained": 0,
        "skipped": 0,
        "failed": 0,
        "remaining": 0,
    }
    redis.xadd.assert_not_awaited()


@pytest.mark.asyncio
async def test_drain_skips_tmp_files():
    """Files ending in `.tmp` are mid-write — drainer must NOT
    touch them. Drainer reads `*.json` only via the sorted glob."""
    root = Path(disk_queue.settings.disk_queue_root) / "2026-05-09"
    root.mkdir(parents=True)
    (root / "good.json").write_text('{"id": 1}')
    (root / "incomplete.json.tmp").write_text("partial")

    disk_queue._reset_state_for_tests()

    redis = _make_redis_mock()
    stats = await disk_queue.drain_to_redis(redis)

    assert stats["drained"] == 1
    # The .tmp survives — we don't sweep them up.
    assert (root / "incomplete.json.tmp").exists()


@pytest.mark.asyncio
async def test_drain_unlink_failure_keeps_file_and_counter():
    """Audit fix (Agent 1 HIGH-2, 2026-05-09): when XADD succeeds
    but unlink raises OSError (permission denied, FS full during
    journal update, etc.), the file MUST stay on disk AND the
    in-memory counter MUST NOT decrement. Otherwise counter
    underestimates real queue depth → cap check lets in more
    enqueues than the disk holds → silent over-cap during a
    sustained outage.
    """
    await disk_queue.enqueue_click({"id": 1})
    await disk_queue.enqueue_click({"id": 2})
    assert await disk_queue.get_queue_size() == 2

    redis = _make_redis_mock()
    # Patch Path.unlink to raise OSError — simulates permission /
    # FS-state failure post-XADD. asyncio.to_thread wraps unlink,
    # so patching at the Path class level is the cleanest hook.
    from unittest.mock import patch
    original_unlink = Path.unlink

    def flaky_unlink(self, *a, **kw):
        raise OSError("simulated unlink failure")

    with patch.object(Path, "unlink", flaky_unlink):
        stats = await disk_queue.drain_to_redis(redis)

    # Drain reports nothing successful — both files hit the same
    # unlink-OSError branch.
    assert stats["drained"] == 0
    # Counter unchanged — files still on disk.
    assert await disk_queue.get_queue_size() == 2

    # Files truly remain on disk (sanity check the patch was
    # restored cleanly + the files weren't deleted by some other
    # path).
    files = list(Path(disk_queue.settings.disk_queue_root).rglob("*.json"))
    assert len(files) == 2


@pytest.mark.asyncio
async def test_drain_decrements_size_counter():
    """Counter must reflect drained files so future cap checks
    correctly account for the freed slots. A counter that goes
    stale would either falsely-reject (over-counts) or
    falsely-accept (under-counts)."""
    await disk_queue.enqueue_click({"id": 1})
    await disk_queue.enqueue_click({"id": 2})
    assert await disk_queue.get_queue_size() == 2

    redis = _make_redis_mock()
    await disk_queue.drain_to_redis(redis)

    assert await disk_queue.get_queue_size() == 0


# ---------------------------------------------------------------------------
# Source-level pin: drainer wired into lifespan + /decide fallback
# ---------------------------------------------------------------------------


class TestLifespanIntegration:
    """A future refactor that drops the drainer task or the /decide
    fallback would silently re-open G-23. Pin them at the source."""

    def test_drainer_wired_into_lifespan(self):
        from app import main as click_main

        source = inspect.getsource(click_main.lifespan)
        assert "run_disk_drainer" in source, (
            "FastAPI lifespan must start the disk-queue drainer "
            "task (T2.2 / G-23). Without it, files written by "
            "the /decide fallback never replay back into Redis."
        )
        # Also pin that the task is cancelled on shutdown — a
        # leak would block clean process exit.
        assert "disk_drainer_task.cancel()" in source, (
            "Lifespan must cancel disk_drainer_task on shutdown."
        )

    def test_decide_falls_back_to_disk_on_xadd_failure(self):
        from app.main import decide

        source = inspect.getsource(decide)
        assert "enqueue_click_to_disk" in source, (
            "/decide must call enqueue_click_to_disk on XADD "
            "failure (T2.2 / G-23). Without this, a Redis outage "
            "loses every click during the outage window."
        )
        # Pin that the call sits inside the `except` branch — not
        # the happy path.
        idx_except = source.find("except Exception as e:")
        idx_fallback = source.find("enqueue_click_to_disk")
        assert idx_except != -1 and idx_fallback != -1
        assert idx_except < idx_fallback, (
            "enqueue_click_to_disk must be called inside the "
            "XADD-failure except branch, not on the happy path."
        )


# ---------------------------------------------------------------------------
# run_drainer behaviour
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_drainer_cancellable():
    """Lifespan shutdown cancels the task — verify it raises
    CancelledError cleanly (no swallow)."""
    redis = _make_redis_mock()
    # Use a short interval so the test doesn't hang waiting.
    task = asyncio.create_task(disk_queue.run_drainer(redis, interval=10))

    await asyncio.sleep(0)  # let the task enter its loop
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_run_drainer_survives_iteration_error(monkeypatch):
    """A transient error in one drain iteration must NOT kill the
    loop — log + continue. We force `drain_to_redis` to raise on
    first call, succeed on second, and verify the task is still
    running after."""
    call_count = {"n": 0}

    async def flaky_drain(redis):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise RuntimeError("transient")
        return {"drained": 0, "skipped": 0, "failed": 0, "remaining": 0}

    monkeypatch.setattr(disk_queue, "drain_to_redis", flaky_drain)

    redis = _make_redis_mock()
    # Tiny interval so we can cycle through multiple iterations
    # quickly. asyncio.sleep(0) yields the loop.
    task = asyncio.create_task(disk_queue.run_drainer(redis, interval=0))
    # Let it cycle a few times.
    for _ in range(3):
        await asyncio.sleep(0.01)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # We saw at least 2 iterations — task survived the error.
    assert call_count["n"] >= 2
