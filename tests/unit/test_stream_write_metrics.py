"""Unit coverage for `app.stream_write_metrics` in isolation (no HTTP
layer) — GTD-R218/PERF-2."""

from __future__ import annotations

import pytest

from app import stream_write_metrics as m


@pytest.fixture(autouse=True)
def _reset():
    m._reset_for_tests()
    yield
    m._reset_for_tests()


def test_empty_window_returns_none_and_zero_count():
    stats = m.stream_write_stats()
    assert stats == {
        "stream_write_p50_ms": None,
        "stream_write_p95_ms": None,
        "stream_write_max_ms": None,
        "stream_write_sample_count": 0,
    }


def test_single_sample_is_p50_p95_and_max():
    m.record_stream_write_ms(3.5)
    stats = m.stream_write_stats()
    assert stats["stream_write_sample_count"] == 1
    assert stats["stream_write_p50_ms"] == 3.5
    assert stats["stream_write_p95_ms"] == 3.5
    assert stats["stream_write_max_ms"] == 3.5


def test_percentiles_over_a_known_distribution():
    # 1..100 ms, ascending — p50/p95/max are exactly derivable.
    for v in range(1, 101):
        m.record_stream_write_ms(float(v))
    stats = m.stream_write_stats()
    assert stats["stream_write_sample_count"] == 100
    assert stats["stream_write_max_ms"] == 100.0
    # sorted values[idx] with idx = int(n * p) — same formula as the
    # implementation, kept explicit here so a refactor that changes the
    # percentile method (nearest-rank vs interpolated) is caught.
    values = [float(v) for v in range(1, 101)]
    assert stats["stream_write_p50_ms"] == values[int(100 * 0.50)]
    assert stats["stream_write_p95_ms"] == values[int(100 * 0.95)]


def test_max_samples_bounds_memory_under_sustained_load():
    for i in range(m._MAX_SAMPLES + 500):
        m.record_stream_write_ms(float(i))
    assert len(m._window) == m._MAX_SAMPLES
    stats = m.stream_write_stats()
    assert stats["stream_write_sample_count"] == m._MAX_SAMPLES
    # The deque dropped the OLDEST entries (maxlen eviction from the
    # left), so the newest (highest) values survive.
    assert stats["stream_write_max_ms"] == float(m._MAX_SAMPLES + 499)


def test_reset_for_tests_clears_window():
    m.record_stream_write_ms(1.0)
    m.record_stream_write_ms(2.0)
    m._reset_for_tests()
    assert m.stream_write_stats()["stream_write_sample_count"] == 0
