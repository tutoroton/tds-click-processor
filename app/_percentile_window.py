"""Small reusable rolling-window percentile tracker — GTD-R218/R219
(GTD-V23, 2026-07-17).

Both `stream_write_metrics` (the synchronous XADD round-trip, R218) and
`reclaim_metrics` (the shipper's orphaned-PEL reclaim age, R219) need the
identical shape: a trailing N-second window of latency samples, p50/p95/
max + count, bounded memory under sustained traffic. Extracted here per
`reusability-discipline` at the 2nd use rather than forking the same
~30 lines twice in one session.

Single-producer / single-consumer, in-process, one asyncio event loop per
caller — no locking (mirrors `shipper_metrics`'s concurrency model).
"""

from __future__ import annotations

import time
from collections import deque


class PercentileWindow:
    """Bounded, time-windowed rolling percentile tracker.

    ``window_seconds`` bounds RECENCY (a sample older than this is
    excluded from ``stats()``, even if still physically in the deque).
    ``max_samples`` bounds MEMORY regardless of traffic burst (the deque
    evicts the oldest entry once full — a hard ceiling independent of
    the time window).
    """

    def __init__(self, window_seconds: float = 300, max_samples: int = 5_000):
        self.window_seconds = window_seconds
        self.max_samples = max_samples
        self._samples: deque[tuple[float, float]] = deque(maxlen=max_samples)

    def record(self, value: float) -> None:
        self._samples.append((time.monotonic(), value))

    def _recent_sorted(self) -> list[float]:
        cutoff = time.monotonic() - self.window_seconds
        return sorted(v for ts, v in self._samples if ts >= cutoff)

    def stats(self, prefix: str) -> dict[str, float | int | None]:
        """p50/p95/max/count over the trailing ``window_seconds``, keyed
        as ``{prefix}_p50_ms`` etc. — the shared shape both call sites
        spread directly into a Pydantic health-response model."""
        values = self._recent_sorted()
        n = len(values)
        if n == 0:
            return {
                f"{prefix}_p50_ms": None,
                f"{prefix}_p95_ms": None,
                f"{prefix}_max_ms": None,
                f"{prefix}_sample_count": 0,
            }

        def _pct(p: float) -> float:
            idx = min(n - 1, int(n * p))
            return values[idx]

        return {
            f"{prefix}_p50_ms": _pct(0.50),
            f"{prefix}_p95_ms": _pct(0.95),
            f"{prefix}_max_ms": values[-1],
            f"{prefix}_sample_count": n,
        }

    def __len__(self) -> int:
        return len(self._samples)

    def reset_for_tests(self) -> None:
        self._samples.clear()
