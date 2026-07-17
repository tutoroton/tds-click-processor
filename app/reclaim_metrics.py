"""In-memory rolling window for the shipper's orphaned-PEL reclaim AGE —
GTD-R219/PERF-3 (GTD-V23, 2026-07-17).

C3's `_reclaim_shipper_pending` (shipper.py) is a DELIBERATE durability
tradeoff, not a bug: an entry only gets `XAUTOCLAIM`ed once idle past
``shipper_reclaim_min_idle_ms`` (default 60000ms), checked every
``shipper_reclaim_interval_sec`` (default 30s) — so a click whose first
delivery attempt got orphaned (consumer crash/restart between
`XREADGROUP` and ship+`XACK`) can sit for roughly `min_idle_ms` +
`interval_sec` before it re-ships and becomes CH-visible. GTD-V23 found
this produces a real, steady ~5.8%/day bimodal cluster in click→CH
visibility (a 60-90s tail alongside the 91.6% <1s norm) with NO
operational gauge — an operator has no way to see the tradeoff is
firing, only its downstream symptom (part of the postback park-retry
tail).

This module makes it observable: each reclaimed stream entry's AGE (now
minus its Redis stream-ID timestamp — the instant it was ORIGINALLY
XADDed, not when the crashed consumer read it) is recorded into a
rolling window, surfaced via `/health` next to `stream_write_metrics`
(R218's sibling gauge). NOT a fix — the reclaim cadence itself is
untouched; this only makes its latency distribution visible, per the
finding's own "NOTE: not a bug" framing.
"""

from __future__ import annotations

import time

from app._percentile_window import PercentileWindow

# Matches stream_write_metrics' window — same alert-cadence rationale
# (5min window, Sentry-evaluation-cadence aligned).
_WINDOW_SECONDS = 300

# Reclaim cycles are comparatively rare (gated by
# shipper_reclaim_interval_sec, default 30s, and typically claim a small
# batch) — 2k samples comfortably covers hours of reclaim activity even
# during a sustained consumer-crash-loop incident.
_MAX_SAMPLES = 2_000

_window = PercentileWindow(window_seconds=_WINDOW_SECONDS, max_samples=_MAX_SAMPLES)


def record_reclaim_age_ms(age_ms: float) -> None:
    """Called once per entry claimed by `_reclaim_shipper_pending`
    (shipper.py), with `age_ms` = now minus the entry's Redis stream-ID
    timestamp (its ORIGINAL XADD instant — not the crashed consumer's
    read time)."""
    _window.record(age_ms)


def reclaim_age_stats() -> dict[str, float | int | None]:
    """p50/p95/max/sample-count reclaim age (ms) over the trailing
    `_WINDOW_SECONDS`. All `None`/0 when nothing has been reclaimed in
    the window — the steady-state, healthy value (reclaim only fires
    after a consumer crash/restart orphans PEL entries)."""
    return _window.stats("shipper_reclaim_age")


def stream_id_age_ms(msg_id: str | bytes) -> float:
    """Milliseconds between now and a Redis stream entry ID's embedded
    timestamp (the ``<ms>-<seq>`` format every stream ID carries — this
    is the instant the entry was ORIGINALLY XADDed, independent of which
    consumer eventually reads/reclaims it)."""
    if isinstance(msg_id, bytes):
        msg_id = msg_id.decode("utf-8")
    ms_part = msg_id.split("-", 1)[0]
    return max(0.0, time.time() * 1000 - float(ms_part))


def _reset_for_tests() -> None:
    """Test-only — clear the window between test cases."""
    _window.reset_for_tests()
