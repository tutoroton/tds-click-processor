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
from dataclasses import dataclass
from typing import Literal

ShipStatus = Literal[
    "success",
    "ack_failed",
    "collector_error",
    "unreachable",
    "parse_failed",
    "loop_error",
    "n/a",
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
        }


# Module-level singleton. Single click-processor instance per node →
# single shipper task → single metrics object. The shipper imports
# this binding and mutates it directly; the health handler reads it.
metrics = ShipperMetrics()


def _reset_for_tests() -> None:
    """Restore the singleton to default state. For unit tests only —
    production lifecycle is single-init. Mirrors the pattern in
    ``disk_queue._reset_state_for_tests``."""
    global metrics
    metrics = ShipperMetrics()
