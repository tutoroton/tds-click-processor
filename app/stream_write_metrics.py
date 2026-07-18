"""In-memory rolling window for the synchronous `stream:clicks` XADD
latency — GTD-R218/PERF-2 (GTD-V23, 2026-07-17).

The per-click ``stream_write_ms`` cannot be embedded in that SAME
click's CH-persisted record: it measures the round-trip of the very
XADD call whose payload would need to carry it (self-referential —
see the comment above ``timing["pre_stream_ms"]`` in ``main.py``).
CH storage instead gets ``pre_stream_ms`` (everything knowable before
the write attempt). This module is the "equivalent durable metric"
GTD-R218's Verified clause explicitly allows in lieu of that impossible
self-embed: an ALWAYS-ON (unlike ``app.diag``'s test_id-gated
checkpoints, which emit nothing for ordinary production traffic),
in-process rolling window of recent XADD latencies, surfaced via
``/health`` so an operator or Sentry alert can see the backend-total
SLA component ``route_total_ms`` alone omits.

Single producer (the ``/decide`` handler, one sample per successful
XADD) + single consumer (the ``/health`` handler), both in-process,
single asyncio event loop — no locking needed (mirrors
``shipper_metrics``'s concurrency model). Window mechanics live in
``app._percentile_window`` (shared with ``app.reclaim_metrics``, R219 —
both need the same trailing-window/p50/p95/max shape).
"""

from __future__ import annotations

from app._percentile_window import PercentileWindow

# 5 minutes matches the shipper's success-ratio window
# (`_SUCCESS_RATIO_WINDOW_SECONDS` in shipper_metrics.py) and typical
# Sentry alert evaluation cadences.
_WINDOW_SECONDS = 300

# Bounds memory regardless of a traffic burst — at the grid-soak target
# (800rps) 300s of samples would be 240k entries; capping at 5k trades a
# little window-completeness under extreme sustained load for a hard
# memory ceiling (each entry is a tiny (float, float) tuple).
_MAX_SAMPLES = 5_000

_window = PercentileWindow(window_seconds=_WINDOW_SECONDS, max_samples=_MAX_SAMPLES)


def record_stream_write_ms(latency_ms: float) -> None:
    """Called once per successful `stream:clicks` XADD (main.py, right
    after the round-trip completes)."""
    _window.record(latency_ms)


def stream_write_stats() -> dict[str, float | int | None]:
    """p50/p95/max/sample-count over the trailing `_WINDOW_SECONDS`.

    All `None`/0 before the first successful XADD (fresh boot, or a
    window with only dedup-skipped/disk-fallback-diverted clicks — this
    metric only samples the happy path's own XADD round-trip).
    """
    return _window.stats("stream_write")


def _reset_for_tests() -> None:
    """Test-only — clear the window between test cases."""
    _window.reset_for_tests()
