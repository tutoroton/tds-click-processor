"""LOSSFIX-4 cascade (2026-08-15) — the park is a SECOND population, and it
must never be reported as the first one.

`_scan_queue_stats_sync` walks the queue root with `rglob`, and the deadletter
park lives INSIDE that root. So the moment LOSSFIX-4 started writing there, a
parked click silently became a "segment" in a metric whose own docstring says
the drainer pulls everything back on recovery. It does not — the park is
drained only by an operator running `scripts/replay_deadletter_park.py`.

The two need OPPOSITE actions, and only one of them resolves by waiting. A
number that conflates them tells the operator to wait for something that will
not happen — and, through `oldest_seconds`, dresses a quiet park up as a
drainer that has been stuck for a month.

The A/B that made this a measurement rather than a suspicion, run before the
fix: ONE parked click, ZERO segments →
    {'segments': 1, 'bytes': 33, 'oldest_seconds': 0.006}
"""

from __future__ import annotations

import asyncio
import json

import pytest

from app import disk_queue, observability


@pytest.fixture(autouse=True)
def _root(tmp_path, monkeypatch):
    monkeypatch.setattr(disk_queue.settings, "disk_queue_root", str(tmp_path))
    disk_queue._cached_queue_stats = None
    yield tmp_path
    disk_queue._cached_queue_stats = None


def _write_park(root, name: str, n: int) -> None:
    d = root / disk_queue._PARK_DIRNAME
    d.mkdir(parents=True, exist_ok=True)
    (d / name).write_text(
        "".join(json.dumps({"click_id": f"c{i}", "data": "{}"}) + "\n"
                for i in range(n)))


def _write_segment(root, name: str, n: int) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / name).write_text(
        "".join(json.dumps({"click_id": f"s{i}"}) + "\n" for i in range(n)))


class TestTheTwoPopulationsAreReportedApart:
    @pytest.mark.asyncio
    async def test_a_parked_click_is_not_a_drainable_segment(self, _root):
        """The exact pre-fix reproduction, inverted into a guard."""
        _write_park(_root, "park-100-1.ndjson", 1)
        stats = await asyncio.to_thread(disk_queue._scan_queue_stats_sync)

        assert stats["segments"] == 0, (
            "a parked click was counted as a segment awaiting drainer "
            "replay — it awaits an OPERATOR, and no amount of waiting "
            "clears it"
        )
        assert stats["park_lines"] == 1
        assert stats["park_bytes"] > 0

    @pytest.mark.asyncio
    async def test_a_park_does_not_become_the_oldest_stuck_segment(self, _root):
        """`oldest_seconds` means 'the oldest file still awaiting drainer
        replay'. A month-old park answering that question reads as a drainer
        stuck for a month — a phantom emergency."""
        _write_park(_root, "park-100-1.ndjson", 3)
        stats = await asyncio.to_thread(disk_queue._scan_queue_stats_sync)
        assert stats["oldest_seconds"] is None

    @pytest.mark.asyncio
    async def test_the_control_a_real_segment_still_counts_as_one(self, _root):
        """Without this, the two assertions above could be satisfied by a
        scan that reports nothing at all."""
        _write_segment(_root, "100-1-000001.ndjson", 2)
        stats = await asyncio.to_thread(disk_queue._scan_queue_stats_sync)

        assert stats["segments"] == 1
        assert stats["oldest_seconds"] is not None
        assert stats["park_lines"] == 0

    @pytest.mark.asyncio
    async def test_both_at_once_stay_separated(self, _root):
        _write_segment(_root, "100-1-000001.ndjson", 2)
        _write_park(_root, "park-100-1.ndjson", 4)
        _write_park(_root, "park-200-2-claimed-99.ndjson", 1)
        stats = await asyncio.to_thread(disk_queue._scan_queue_stats_sync)

        assert stats["segments"] == 1
        assert stats["park_lines"] == 5, "a claimed snapshot still counts"

    @pytest.mark.asyncio
    async def test_the_byte_cap_still_sees_the_park(self, _root):
        """🔴 The park MUST stay inside `bytes`. Excluding it would let the
        park grow unbounded until the node's disk fills — trading a reporting
        defect for a real one."""
        _write_park(_root, "park-100-1.ndjson", 10)
        stats = await asyncio.to_thread(disk_queue._scan_queue_stats_sync)
        assert stats["bytes"] == stats["park_bytes"] > 0

        disk_queue._cached_queue_stats = stats
        monkey_cap = stats["bytes"]
        orig = disk_queue.settings.disk_segment_max_total_bytes
        try:
            disk_queue.settings.disk_segment_max_total_bytes = monkey_cap
            assert disk_queue._check_byte_cap() is True
        finally:
            disk_queue.settings.disk_segment_max_total_bytes = orig

    def test_the_never_sampled_shape_carries_both_keys(self):
        """A consumer reading `park_lines` off the zeroed fresh-boot dict
        must not KeyError on the one path that exists to be safe."""
        disk_queue._cached_queue_stats = None
        got = disk_queue.get_cached_queue_stats()
        assert got["park_lines"] == 0 and got["park_bytes"] == 0


class TestTheDepthSignal:
    @pytest.mark.asyncio
    async def test_a_deep_park_pages_someone(self, _root, monkeypatch):
        monkeypatch.setattr(
            observability.settings, "deadletter_park_depth_alert_threshold", 3)
        _write_park(_root, "park-100-1.ndjson", 5)

        seen: list = []
        monkeypatch.setattr(observability, "capture_op_msg_throttled",
                            lambda *a, **kw: seen.append(a))
        n = await observability.emit_deadletter_park_depth()

        assert n == 5
        assert len(seen) == 1
        assert seen[0][0] == observability.OP_DEADLETTER_PARK_DEPTH

    @pytest.mark.asyncio
    async def test_the_control_a_shallow_park_pages_nobody(
            self, _root, monkeypatch):
        """Otherwise the alert above could be a function that always fires."""
        monkeypatch.setattr(
            observability.settings, "deadletter_park_depth_alert_threshold", 10)
        _write_park(_root, "park-100-1.ndjson", 2)

        seen: list = []
        monkeypatch.setattr(observability, "capture_op_msg_throttled",
                            lambda *a, **kw: seen.append(a))
        assert await observability.emit_deadletter_park_depth() == 2
        assert seen == []

    @pytest.mark.asyncio
    async def test_an_empty_park_is_silent(self, _root, monkeypatch):
        seen: list = []
        monkeypatch.setattr(observability, "capture_op_msg_throttled",
                            lambda *a, **kw: seen.append(a))
        assert await observability.emit_deadletter_park_depth() == 0
        assert seen == []

    @pytest.mark.asyncio
    async def test_the_emitter_is_actually_wired_into_the_loop(self):
        """An emitter nobody calls reports nothing. Pinned by source rather
        than by running the 60s loop."""
        import inspect
        src = inspect.getsource(observability.run_observability_loop)
        assert "emit_deadletter_park_depth()" in src
