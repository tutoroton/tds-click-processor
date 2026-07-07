"""P2 c3 (LOSSFIX, 2026-07-07) — edge used_memory% watermark.

Ported from the collector's ``app/watermark.py`` (LOSSFIX P1a) — same
state machine, same hysteresis/fail-open/staleness semantics, same two-
home stale-sampler visibility. Read that module's docstring for the
full rationale; only the DIVERGENCE is documented here.

The ONE semantic difference: the collector SHEDS (503, whole batch) on
trip. The edge SPILLS instead — ``should_spill()`` where the collector
has ``should_reject()`` — because the routing-cache HSETs and
``stream:clicks`` XADDs share the SAME Redis instance
(``docker-compose.node.yml``'s ``redis`` service, 256 MB volatile-lru;
confirmed via ``TDS_REDIS_URL`` — there is no separate instance to fall
back to). A 503 here would refuse legitimate traffic for a problem that
has a strictly better answer already built: divert the click into the
disk-segment fallback (same path M1's entry-count gate and a genuine
XADD failure already use) BEFORE attempting the XADD, so new clicks
stop competing with the routing cache for memory. Empirical grounding:
campaign-sync HSET threw ``OutOfMemoryError`` once click traffic filled
this instance at >=650rps (FULLCHAIN-STRESS-FINDINGS).

Sampling is a DEDICATED background task (:func:`run_watermark_sampler`,
started in ``main.lifespan``) at ``watermark_sample_interval_sec`` (~1s)
cadence — NEVER a per-click Redis ``INFO`` call. ``main.py``'s
real-click write path reads the CACHED decision only, via
:meth:`WatermarkState.should_spill`.
"""

from __future__ import annotations

import asyncio
import logging
import time

from app.config import settings
from app.telemetry import OP_WATERMARK_SIGNAL_STALE, capture_op_msg

logger = logging.getLogger("tds.click_processor.watermark")


class WatermarkState:
    """In-memory cache of the routing Redis's ``used_memory%`` + spill
    decision. Single process-wide instance (module-level
    ``watermark_state`` below), written only by
    :func:`run_watermark_sampler` and read only by ``main.py``'s
    real-click path (via :meth:`should_spill`) — no locking needed,
    asyncio is single-threaded and every mutation is a plain attribute
    set."""

    def __init__(self) -> None:
        self.used_memory_pct: float = 0.0
        self.spill_mode: bool = False
        self._last_sampled_monotonic: float | None = None
        self._stale_alert_fired: bool = False
        self._consecutive_sample_failures: int = 0
        self._created_monotonic: float = time.monotonic()

    def sample_age(self) -> float:
        if self._last_sampled_monotonic is None:
            return float("inf")
        return time.monotonic() - self._last_sampled_monotonic

    def sample_age_or_none(self) -> float | None:
        """LOSSFIX P3 (2026-07-07, L6) — /health-safe variant of
        :meth:`sample_age`. `float('inf')` is a fine internal sentinel
        for the `is_stale()` comparison, but it is NOT valid JSON —
        `json.dumps(float('inf'))` emits the non-standard literal
        `Infinity`, which many strict downstream parsers (dashboards,
        the P4 abort-guard) reject. Never-sampled reports as `None`
        instead, which every JSON consumer already handles."""
        if self._last_sampled_monotonic is None:
            return None
        return self.sample_age()

    def is_stale(self) -> bool:
        return self.sample_age() > settings.watermark_staleness_sec

    def record_sample(self, used_memory_pct: float) -> None:
        """Feed one fresh ``used_memory%`` reading; apply the hysteresis
        state transition. Called only by the background sampler on a
        SUCCESSFUL sample — ends any in-progress staleness episode."""
        self.used_memory_pct = used_memory_pct
        self._last_sampled_monotonic = time.monotonic()
        self._stale_alert_fired = False
        self._consecutive_sample_failures = 0

        if not self.spill_mode and used_memory_pct >= settings.watermark_shed_pct:
            self.spill_mode = True
            logger.warning(
                "edge node entering SPILL mode: used_memory=%.1f%% >= "
                "%.1f%% — new real clicks divert to the disk-segment "
                "fallback (not 503) until below %.1f%%",
                used_memory_pct, settings.watermark_shed_pct,
                settings.watermark_resume_pct,
            )
        elif self.spill_mode and used_memory_pct < settings.watermark_resume_pct:
            self.spill_mode = False
            logger.info(
                "edge node exiting SPILL mode: used_memory=%.1f%% < "
                "%.1f%% — XADD-ing real clicks again",
                used_memory_pct, settings.watermark_resume_pct,
            )

    def record_sample_failure(self) -> None:
        """Home 1 — called by :func:`run_watermark_sampler` every time a
        sample attempt itself fails. After ~staleness/interval
        CONSECUTIVE failures, fires the shared stale alert on the
        loop's OWN clock (fires with ZERO incoming traffic)."""
        self._consecutive_sample_failures += 1
        threshold = max(
            1,
            round(
                settings.watermark_staleness_sec
                / settings.watermark_sample_interval_sec
            ),
        )
        if self._consecutive_sample_failures >= threshold:
            self._fire_stale_alert_once()

    def should_spill(self) -> bool:
        """Divert decision for the real-click write path only (the M1
        entry-count check is separate — see ``main._check_stream_
        backpressure``).

        Fail-open: a stale sample ALWAYS returns False (proceed to
        XADD), even from active spill mode — a wedged sampler must
        never wedge the hot path shut. The never-sampled case gets a
        bounded boot grace (``watermark_boot_grace_sec``) before it
        counts as its own staleness episode, mirroring the collector's
        home-2 semantics exactly."""
        if self.is_stale():
            if self._last_sampled_monotonic is None:
                since_boot = time.monotonic() - self._created_monotonic
                if since_boot <= settings.watermark_boot_grace_sec:
                    return False  # within the boot grace — stay quiet
            self._fire_stale_alert_once()
            return False
        return self.spill_mode

    def _fire_stale_alert_once(self) -> None:
        if self._stale_alert_fired:
            return
        self._stale_alert_fired = True
        age = self.sample_age()
        logger.error(
            "watermark signal stale (age=%.1fs > %.1fs, spill_mode=%s) — "
            "FAILING OPEN (XADD proceeds) — inspect the sampler task; "
            "memory can still fill via routing/dedup keys the entry-"
            "count check does not observe",
            age, settings.watermark_staleness_sec, self.spill_mode,
        )
        capture_op_msg(
            OP_WATERMARK_SIGNAL_STALE,
            f"Edge watermark sampler stale for {age:.1f}s "
            f"(spill_mode={self.spill_mode}) — failing open (XADD "
            "proceeds); inspect the sampler task",
            level="error",
            sample_age_sec=age,
            staleness_bound_sec=settings.watermark_staleness_sec,
            spill_mode=self.spill_mode,
        )

    def reset_for_tests(self) -> None:
        """Test-only — restore boot-time state between test cases."""
        self.used_memory_pct = 0.0
        self.spill_mode = False
        self._last_sampled_monotonic = None
        self._stale_alert_fired = False
        self._consecutive_sample_failures = 0
        self._created_monotonic = time.monotonic()


watermark_state = WatermarkState()


async def _sample_used_memory_pct(redis) -> float | None:
    """One ``INFO memory`` round-trip against the routing Redis.
    Returns ``None`` on any failure (Redis impaired, ``maxmemory``
    unset/0) — the caller leaves the cached state untouched, which
    ages toward staleness and fails open."""
    try:
        info = await redis.info("memory")
    except Exception as exc:  # noqa: BLE001 — sampler must never crash the loop
        logger.warning("watermark sample failed (INFO memory): %s", exc)
        return None

    used = info.get("used_memory", 0)
    maxmem = info.get("maxmemory", 0)
    if not maxmem:
        logger.warning(
            "watermark sample: Redis maxmemory is unset/0 — cannot "
            "compute used_memory%%; watermark will age to stale and "
            "fail open"
        )
        return None
    return 100.0 * float(used) / float(maxmem)


async def run_watermark_sampler(redis, interval: float | None = None) -> None:
    """Periodic ``used_memory%`` sampler. Started in the FastAPI
    lifespan, cancelled on shutdown. A transient INFO failure just
    leaves the cache aging toward stale rather than killing the loop.
    """
    interval = interval or settings.watermark_sample_interval_sec
    logger.info(
        "Edge watermark sampler started (interval=%ss, shed=%.0f%%, "
        "resume=%.0f%%, staleness_bound=%ss)",
        interval, settings.watermark_shed_pct, settings.watermark_resume_pct,
        settings.watermark_staleness_sec,
    )
    while True:
        try:
            await asyncio.sleep(interval)
            try:
                pct = await _sample_used_memory_pct(redis)
                if pct is not None:
                    watermark_state.record_sample(pct)
                else:
                    watermark_state.record_sample_failure()
            except Exception:  # noqa: BLE001
                logger.exception("Watermark sampler iteration failed — continuing")
        except asyncio.CancelledError:
            logger.info("Watermark sampler cancelled — shutting down")
            raise
