"""GTD-R219/PERF-3 (GTD-V23, 2026-07-17) — the shipper's orphaned-PEL
reclaim age must be observable, not silent.

Unit coverage for `app.reclaim_metrics` in isolation (module-level
percentile window + Redis stream-ID age helper), plus the wiring in
`_reclaim_shipper_pending` that feeds it.
"""

from __future__ import annotations

import json
import time
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from app import reclaim_metrics as m
from app import shipper
from app.shipper import _reclaim_shipper_pending


@pytest.fixture(autouse=True)
def _reset():
    m._reset_for_tests()
    yield
    m._reset_for_tests()


class FakeRedis:
    """Mirrors test_shipper_reclaim.py's minimal async Redis double."""

    def __init__(self, autoclaim_batches):
        self._batches = list(autoclaim_batches)
        self.xgroup_create = AsyncMock()
        self.xack = AsyncMock()
        self.xadd = AsyncMock()
        self.xtrim = AsyncMock()

    async def xautoclaim(self, stream, group, consumer, *, min_idle_time,
                         start_id, count):
        if self._batches:
            cursor, messages = self._batches.pop(0)
            return [cursor, messages, []]
        return ["0-0", [], []]


def _msg(msg_id: str, click_id: str):
    return (msg_id, {"data": json.dumps({"click_id": click_id})})


def _resp(status: int, text: str) -> httpx.Response:
    return httpx.Response(status_code=status, text=text)


class TestStreamIdAgeMs:
    def test_recent_id_is_near_zero_age(self):
        now_ms = int(time.time() * 1000)
        age = m.stream_id_age_ms(f"{now_ms}-0")
        assert 0 <= age < 1000  # well under a second of test-execution slop

    def test_old_id_reports_large_age(self):
        # 90 seconds ago — the exact "reclaim cluster" magnitude R219 flags.
        ninety_s_ago_ms = int(time.time() * 1000) - 90_000
        age = m.stream_id_age_ms(f"{ninety_s_ago_ms}-0")
        assert 89_000 <= age <= 91_000

    def test_accepts_bytes_msg_id(self):
        now_ms = int(time.time() * 1000)
        age = m.stream_id_age_ms(f"{now_ms}-0".encode())
        assert 0 <= age < 1000

    def test_never_negative_even_for_a_future_timestamp(self):
        future_ms = int(time.time() * 1000) + 10_000
        assert m.stream_id_age_ms(f"{future_ms}-0") == 0.0


class TestReclaimAgeStats:
    def test_empty_window_is_none_and_zero(self):
        stats = m.reclaim_age_stats()
        assert stats == {
            "shipper_reclaim_age_p50_ms": None,
            "shipper_reclaim_age_p95_ms": None,
            "shipper_reclaim_age_max_ms": None,
            "shipper_reclaim_age_sample_count": 0,
        }

    def test_recorded_sample_appears_in_stats(self):
        m.record_reclaim_age_ms(75_000.0)
        stats = m.reclaim_age_stats()
        assert stats["shipper_reclaim_age_sample_count"] == 1
        assert stats["shipper_reclaim_age_p50_ms"] == 75_000.0
        assert stats["shipper_reclaim_age_max_ms"] == 75_000.0


class TestReclaimLoopFeedsTheWindow:
    @pytest.mark.asyncio
    async def test_reclaimed_batch_records_ages(self):
        old_ms = int(time.time() * 1000) - 65_000  # inside the 60-90s cluster
        redis = FakeRedis([("0-0", [_msg(f"{old_ms}-0", "c1"),
                                     _msg(f"{old_ms}-1", "c2")])])
        client = AsyncMock()
        accepted = _resp(200, '{"accepted":["c1","c2"],"rejected":[],"duplicates":[]}')

        with patch.object(shipper, "_post_batch_to_central",
                          new=AsyncMock(return_value=accepted)), \
             patch.object(shipper, "_process_new_shape_batch",
                          new=AsyncMock()):
            counts = await _reclaim_shipper_pending(redis, client)

        assert counts == {"claimed": 2, "shipped_batches": 1}
        stats = m.reclaim_age_stats()
        assert stats["shipper_reclaim_age_sample_count"] == 2
        # Both entries are ~65s old — well inside the finding's 60-90s band.
        assert 60_000 <= stats["shipper_reclaim_age_p50_ms"] <= 90_000

    @pytest.mark.asyncio
    async def test_reship_failure_still_records_ages(self):
        """Age is recorded at CLAIM time (how long the entry sat before
        this reclaim cycle found it) — independent of whether the
        subsequent re-ship attempt succeeds. A failed re-ship leaves the
        entry pending for the NEXT cycle, but this cycle's age sample is
        still real, honest data about how long it was orphaned so far."""
        old_ms = int(time.time() * 1000) - 61_000
        redis = FakeRedis([("0-0", [_msg(f"{old_ms}-0", "c1")])])
        client = AsyncMock()
        server_error = _resp(500, "upstream boom")

        with patch.object(shipper, "_post_batch_to_central",
                          new=AsyncMock(return_value=server_error)), \
             patch.object(shipper, "_process_new_shape_batch",
                          new=AsyncMock()):
            await _reclaim_shipper_pending(redis, client)

        assert m.reclaim_age_stats()["shipper_reclaim_age_sample_count"] == 1

    @pytest.mark.asyncio
    async def test_no_orphans_means_no_samples(self):
        """Steady-state (nothing to reclaim) must not synthesize samples
        — None/0 IS the healthy signal."""
        redis = FakeRedis([])
        client = AsyncMock()

        counts = await _reclaim_shipper_pending(redis, client)

        assert counts == {"claimed": 0, "shipped_batches": 0}
        assert m.reclaim_age_stats()["shipper_reclaim_age_sample_count"] == 0


class TestHealthEndpointSurfacesReclaimAge:
    def test_health_reports_none_before_any_reclaim(self):
        from fastapi.testclient import TestClient

        from app.main import app

        body = TestClient(app).get("/health").json()
        assert body["shipper_reclaim_age_sample_count"] == 0
        assert body["shipper_reclaim_age_p50_ms"] is None

    def test_health_reports_recorded_reclaim_age(self):
        from fastapi.testclient import TestClient

        from app.main import app

        m.record_reclaim_age_ms(72_000.0)
        body = TestClient(app).get("/health").json()
        assert body["shipper_reclaim_age_sample_count"] == 1
        assert body["shipper_reclaim_age_p50_ms"] == 72_000.0
        assert body["shipper_reclaim_age_max_ms"] == 72_000.0
