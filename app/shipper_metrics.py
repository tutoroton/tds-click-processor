"""In-memory shipper metrics — single source of truth for /health.

F.29 Sprint 1.4 (2026-05-23). Closes plan §3 G5: pre-F.29 the
``/health`` endpoint returned only ``redis``, ``campaigns_loaded``,
``sync_version``, ``uptime_seconds`` — NO visibility into the shipper.
A shipper task that had crashed silently (audit-2026-05-16-style
50-day blackout) would still respond ``/health=200`` with green
``redis`` because the redis ping itself worked. The fact that clicks
were not being delivered upstream was invisible to dashboards.

This module exposes ``ShipperMetrics`` — a small in-memory singleton
updated by the shipper's main loop (`run_shipper` in shipper.py) and
read by the ``/health`` endpoint. Single producer (the shipper task)
+ single consumer (the health handler), both in-process, single asyncio
event loop → no locking needed.

Fields tracked:

  * ``running`` — set to True at ``run_shipper`` entry, False on
    exit / cancellation. False + non-local env + flag=True is the
    catastrophic case the operator dashboard MUST surface.
  * ``last_ship_at`` — wall-clock UNIX timestamp of the last batch
    POST attempt (regardless of outcome). ``None`` until the first
    attempt completes.
  * ``last_ship_status`` — outcome of the last attempt:
      - ``"success"`` — 200/202 from collector, ACK + XTRIM succeeded
      - ``"ack_failed"`` — POST succeeded but local XACK/XTRIM failed
      - ``"collector_error"`` — non-2xx response body
      - ``"unreachable"`` — httpx connection failure
      - ``"parse_failed"`` — corrupt stream payload (no POST attempted)
      - ``"loop_error"`` — catch-all branch fired
      - ``"n/a"`` — initial state, no attempt yet
  * ``last_batch_size`` — number of clicks in the last POST batch.
    ``0`` before the first attempt OR when the stream was empty.

Derived (computed-property) fields:

  * ``lag_seconds`` — wall-clock seconds since ``last_ship_at``;
    ``None`` when ``last_ship_at`` is ``None``. Sprint 4.1 alert
    rule "shipper lag > 5min" keys off this exact field.

This module intentionally avoids dependencies on Redis / asyncio /
Sentry to keep ``/health`` cheap and side-effect-free. The disk-free
and stream-length fields are computed live in the ``/health`` handler
(they need Redis access and ``shutil.disk_usage`` — both fast but
not pure).
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Literal

# F.29 Sprint 2.4 (2026-05-23) — rolling success-ratio window.
#
# The shipper records every batch outcome as a triple (timestamp,
# accepted, rejected). Entries older than this window are discarded
# when a new entry is added. ``success_ratio_5m`` then computes
# sum(accepted) / sum(accepted + rejected) over the surviving
# entries.
#
# 5 minutes matches typical Sentry alert evaluation cadences (the
# alert rule in Sprint 4.1 will warn when ratio < 0.95 sustained).
# A shorter window would oscillate too much during transient
# collector blips; a longer window would dampen real outage signal.
_SUCCESS_RATIO_WINDOW_SECONDS = 300

ShipStatus = Literal[
    "success",            # all clicks accepted, ACK + XTRIM succeeded
    "ack_failed",         # POST succeeded but local XACK/XTRIM failed
    "collector_error",    # non-2xx response body (or all rejected)
    "unreachable",        # httpx connection failure
    "parse_failed",       # corrupt stream payload (no POST attempted)
    "loop_error",         # catch-all for unknown branches
    "n/a",                # initial state, no attempt yet
    # F.29 Sprint 2.2 (2026-05-23) — per-click verdict outcomes:
    "partial_ack",        # some clicks accepted, some rejected (207)
    "deadlettered",       # at least one click hit max retries this iter
    "legacy_collector",   # collector returned pre-F.29 shape — shim
                          # absorbed it as ACK-all + WARN log
]


@dataclass
class ShipperMetrics:
    """Live shipper state surfaced via ``/health``.

    Mutated by the shipper coroutine; read by the health handler.
    Both run on the same asyncio event loop → no concurrency hazard
    (no preemption between Python bytecode instructions for
    simple-attribute reads/writes).
    """

    running: bool = False
    last_ship_at: float | None = None
    last_ship_status: ShipStatus = "n/a"
    last_batch_size: int = 0

    # F.29 Sprint 2.4 — rolling 5-min success-ratio window.
    #
    # ``deque`` of (timestamp, accepted, rejected) triples. Older
    # entries are pruned lazily on each :meth:`record_outcome` call;
    # the in-memory list is bounded by traffic volume × window
    # length, not by an arbitrary maxlen, so a slow node holds fewer
    # entries (good).
    outcomes: deque[tuple[float, int, int]] = field(
        default_factory=deque,
    )

    def mark_running(self) -> None:
        """Called at the top of ``run_shipper``'s main loop."""
        self.running = True

    def mark_stopped(self) -> None:
        """Called on graceful exit / task cancellation."""
        self.running = False

    def record_ship(self, status: ShipStatus, batch_size: int = 0) -> None:
        """Record the outcome of one shipper iteration.

        Args:
            status: One of the literals declared in :data:`ShipStatus`.
                Drift from this set silently breaks Sprint 4.1 alert
                rules + dashboard widget — keep the type narrow.
            batch_size: Number of clicks attempted in this iteration.
                ``0`` is legitimate (XREADGROUP returned empty).
        """
        self.last_ship_at = time.time()
        self.last_ship_status = status
        self.last_batch_size = batch_size

    def record_outcome(
        self, accepted: int, rejected: int, _now: float | None = None,
    ) -> None:
        """F.29 Sprint 2.4 — append batch outcome + prune stale entries.

        Distinct from :meth:`record_ship` (which captures the most
        recent attempt as a single snapshot for ``last_ship_status``).
        ``record_outcome`` feeds the rolling-window success ratio that
        Sprint 4.1 will alert on.

        Args:
            accepted: Count of clicks accepted (or duplicates — both
                are "delivered" from the operator's perspective).
            rejected: Count of clicks rejected by the collector OR
                deadlettered locally. Both count as "non-delivery"
                in the success ratio.
            _now: Test seam — overrides ``time.time()`` for
                deterministic pruning tests.
        """
        now = time.time() if _now is None else _now
        self.outcomes.append((now, accepted, rejected))
        # Lazy-prune entries older than the window.
        cutoff = now - _SUCCESS_RATIO_WINDOW_SECONDS
        while self.outcomes and self.outcomes[0][0] < cutoff:
            self.outcomes.popleft()

    @property
    def success_ratio_5m(self) -> float | None:
        """Rolling-5-min success ratio = accepted / (accepted + rejected).

        Returns:
            ``None`` when no outcomes recorded yet (initial state) OR
            when the total denominator is 0 (only zero-size batches
            recorded — e.g. shipper running but stream empty). ``None``
            is JSON-serialisable to ``null`` so dashboards can
            distinguish "no data" from "0% success".
            ``1.0`` when all clicks accepted.
            ``0.0`` when all clicks rejected.
        """
        if not self.outcomes:
            return None
        total_accepted = sum(a for _, a, _ in self.outcomes)
        total_rejected = sum(r for _, _, r in self.outcomes)
        denominator = total_accepted + total_rejected
        if denominator == 0:
            return None
        return round(total_accepted / denominator, 4)

    @property
    def lag_seconds(self) -> float | None:
        """Seconds since the last ship attempt — or ``None`` if no
        attempt has happened yet (initial state).

        Round to 2 decimal places so the value is human-readable in
        dashboards without pretending to sub-millisecond precision
        (the wall-clock anchor is ``time.time()``, accurate to ~ms).
        """
        if self.last_ship_at is None:
            return None
        return round(time.time() - self.last_ship_at, 2)

    def to_health_dict(self) -> dict:
        """Serialise into the shape consumed by ``HealthResponse``.

        Kept on the dataclass itself so any new field added to the
        dataclass MUST also be wired into this method (and therefore
        into the health response) — preventing drift where a metric
        is updated but never surfaced.
        """
        return {
            "shipper_running": self.running,
            "shipper_lag_seconds": self.lag_seconds,
            "last_ship_at": self.last_ship_at,
            "last_ship_status": self.last_ship_status,
            "last_batch_size": self.last_batch_size,
            # F.29 Sprint 2.4 — rolling success-ratio window.
            "shipper_success_ratio_5m": self.success_ratio_5m,
        }


# Module-level singleton. Single click-processor instance per node →
# single shipper task → single metrics object. The shipper imports
# this binding and mutates it directly; the health handler reads it.
metrics = ShipperMetrics()


def _reset_for_tests() -> None:
    """Restore the singleton to default state. For unit tests only —
    production lifecycle is single-init.

    Mutates the existing object IN PLACE rather than rebinding the
    module attribute. Importers that did ``from app.shipper_metrics
    import metrics as shipper_metrics`` (notably shipper.py) hold a
    reference to the original object; rebinding the module attribute
    would leave those references pointing at the OLD object and
    decouple test state from production state — a subtle bug caught
    by Sprint 1.6 validation cycle test
    ``test_run_shipper_marks_stopped_on_cancellation`` 2026-05-23.
    """
    metrics.running = False
    metrics.last_ship_at = None
    metrics.last_ship_status = "n/a"
    metrics.last_batch_size = 0
    metrics.outcomes.clear()
