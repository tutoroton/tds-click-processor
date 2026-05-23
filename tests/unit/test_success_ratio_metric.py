"""Tests for the F.29 Sprint 2.4 rolling success-ratio metric.

ShipperMetrics.record_outcome appends a (timestamp, accepted, rejected)
triple; ``success_ratio_5m`` computes accepted / (accepted + rejected)
across entries within the last 5 minutes. Sprint 4.1 alert rules will
key on this property (warn at <0.95 sustained).

Coverage:

  * Initial state — no outcomes → None
  * All accepted → 1.0
  * All rejected → 0.0
  * Mixed → proportional
  * Entries beyond 5min window are pruned (lazily on next call)
  * Empty batches (accepted=0, rejected=0) recorded but ratio stays
    None when ONLY zero-size batches present
  * Mixed empty + non-empty batches — non-empty drives the ratio
  * Drift in window constant pinned via the canonical 300s value

Reference: F.29 plan §4 Sprint 2.4 row.
"""

from __future__ import annotations

import time

import pytest

from app import shipper_metrics as smm
from app.shipper_metrics import (
    ShipperMetrics,
    _SUCCESS_RATIO_WINDOW_SECONDS,
)


@pytest.fixture(autouse=True)
def _reset_metrics():
    """Per-test isolation."""
    smm._reset_for_tests()
    yield
    smm._reset_for_tests()


# ---------------------------------------------------------------------------
# Window constant pin — Sprint 4.1 alert rules depend on this exact value
# ---------------------------------------------------------------------------


def test_window_is_300_seconds():
    """5 minutes = 300 seconds. Pin the constant so a future change
    that drifts to e.g. 60s silently broadens alert false-positive
    range during transient blips."""
    assert _SUCCESS_RATIO_WINDOW_SECONDS == 300


# ---------------------------------------------------------------------------
# Initial state — None until first outcome recorded
# ---------------------------------------------------------------------------


def test_ratio_is_none_initial():
    m = ShipperMetrics()
    assert m.success_ratio_5m is None


# ---------------------------------------------------------------------------
# All accepted → 1.0
# ---------------------------------------------------------------------------


def test_ratio_all_accepted_is_one():
    """100 clicks accepted, 0 rejected → 1.0. The canonical happy path."""
    m = ShipperMetrics()
    m.record_outcome(accepted=100, rejected=0)
    assert m.success_ratio_5m == 1.0


def test_ratio_all_rejected_is_zero():
    """0 accepted, 100 rejected → 0.0 (NOT None — denominator is 100,
    distinct from 'no data'). Sprint 4.1 alert wants to distinguish
    these cases."""
    m = ShipperMetrics()
    m.record_outcome(accepted=0, rejected=100)
    assert m.success_ratio_5m == 0.0


# ---------------------------------------------------------------------------
# Mixed — proportional ratio
# ---------------------------------------------------------------------------


def test_ratio_mixed_proportional():
    """40 accepted, 10 rejected → 40/50 = 0.8."""
    m = ShipperMetrics()
    m.record_outcome(accepted=40, rejected=10)
    assert m.success_ratio_5m == 0.8


def test_ratio_across_multiple_outcomes_sums_correctly():
    """Three sequential batches → ratio is computed on the SUM, not
    average of per-batch ratios. (a, r) tuples: (10, 0), (5, 5),
    (15, 10) → sum_a=30, sum_r=15 → 30/45 = 0.667."""
    m = ShipperMetrics()
    m.record_outcome(accepted=10, rejected=0)
    m.record_outcome(accepted=5, rejected=5)
    m.record_outcome(accepted=15, rejected=10)
    # 30/45 = 0.6666... → rounded to 0.6667 (4 dp).
    assert m.success_ratio_5m == 0.6667


# ---------------------------------------------------------------------------
# Window pruning — entries older than 5min are dropped lazily
# ---------------------------------------------------------------------------


def test_old_entries_pruned_on_next_record():
    """An entry timestamped 6 minutes ago should be pruned when a
    new entry is added at "now". Uses the test seam _now=… to
    inject deterministic timestamps."""
    m = ShipperMetrics()

    now = time.time()
    six_minutes_ago = now - 360

    # Stale entry: 100% success but outside the window.
    m.record_outcome(accepted=100, rejected=0, _now=six_minutes_ago)
    # Fresh entry: 0% success.
    m.record_outcome(accepted=0, rejected=10, _now=now)

    # Stale entry must have been pruned; only the fresh one counts.
    # Ratio = 0/10 = 0.0 (NOT averaged with stale 100%).
    assert m.success_ratio_5m == 0.0
    # Internal accounting: only one outcome left in the deque.
    assert len(m.outcomes) == 1


def test_entries_inside_window_are_kept():
    """Entries within 5 minutes are NOT pruned."""
    m = ShipperMetrics()
    now = time.time()
    one_minute_ago = now - 60
    four_minutes_ago = now - 240

    m.record_outcome(accepted=10, rejected=0, _now=four_minutes_ago)
    m.record_outcome(accepted=5, rejected=5, _now=one_minute_ago)
    m.record_outcome(accepted=20, rejected=0, _now=now)

    # All three entries inside window. Sum: a=35, r=5 → 35/40 = 0.875
    assert m.success_ratio_5m == 0.875
    assert len(m.outcomes) == 3


def test_boundary_entry_at_exactly_5_minutes_old_is_pruned():
    """Entries with timestamp <= (now - window_seconds) are pruned.
    Strictly-less-than would leak one boundary entry per pruning
    pass. Pin the comparison via a deterministic test."""
    m = ShipperMetrics()
    now = time.time()
    exactly_5min_ago = now - 300

    m.record_outcome(accepted=100, rejected=0, _now=exactly_5min_ago)
    m.record_outcome(accepted=0, rejected=10, _now=now)

    # The 5min-old entry is at the boundary. cutoff = now - 300 =
    # exactly_5min_ago. Comparison ``< cutoff`` keeps it (not less).
    # So we expect BOTH entries.
    # 100 + 0 = 100 accepted; 0 + 10 = 10 rejected; 100/110 = 0.9091
    assert m.success_ratio_5m == 0.9091


# ---------------------------------------------------------------------------
# Zero-denominator handling — only empty batches present
# ---------------------------------------------------------------------------


def test_only_empty_batches_returns_none():
    """If the shipper records zero-sized batches (e.g., empty stream
    iterations), the denominator is 0 → return None (distinguishes
    "no data to compute" from "0% success"). Dashboards render this
    as null/N/A."""
    m = ShipperMetrics()
    m.record_outcome(accepted=0, rejected=0)
    m.record_outcome(accepted=0, rejected=0)
    assert m.success_ratio_5m is None


def test_empty_batches_mixed_with_real_batches():
    """Empty batches (zero-size) don't bias the ratio — only real
    batches' counts matter."""
    m = ShipperMetrics()
    m.record_outcome(accepted=0, rejected=0)  # noise
    m.record_outcome(accepted=10, rejected=0)  # signal
    m.record_outcome(accepted=0, rejected=0)  # noise

    # Empty batches contribute 0 to both sums; effective ratio comes
    # from the (10, 0) batch only.
    assert m.success_ratio_5m == 1.0


# ---------------------------------------------------------------------------
# to_health_dict wiring
# ---------------------------------------------------------------------------


def test_to_health_dict_includes_success_ratio():
    """Sprint 1.4's drift guard: every dataclass field must be wired
    into ``to_health_dict``. Pin the new field."""
    m = ShipperMetrics()
    d = m.to_health_dict()
    assert "shipper_success_ratio_5m" in d
    assert d["shipper_success_ratio_5m"] is None  # initial state


def test_to_health_dict_reflects_recorded_outcomes():
    """After record_outcome calls, to_health_dict surfaces the
    rolling ratio so /health responses are populated."""
    m = ShipperMetrics()
    m.record_outcome(accepted=8, rejected=2)
    d = m.to_health_dict()
    assert d["shipper_success_ratio_5m"] == 0.8


# ---------------------------------------------------------------------------
# Reset hygiene
# ---------------------------------------------------------------------------


def test_reset_clears_outcomes(monkeypatch):
    """_reset_for_tests must also clear the outcomes deque, otherwise
    tests leak ratio state into each other."""
    smm.metrics.record_outcome(accepted=5, rejected=5)
    assert smm.metrics.success_ratio_5m == 0.5

    smm._reset_for_tests()
    assert smm.metrics.success_ratio_5m is None
    assert len(smm.metrics.outcomes) == 0


# ---------------------------------------------------------------------------
# F.29 Sprint 2.7d (2026-05-23) — deque maxlen + Sentry breadcrumb
# ---------------------------------------------------------------------------


def test_deque_has_maxlen_2000():
    """Sprint 2.7d (Agent 1 validation finding) — explicit maxlen
    bounds worst-case memory if time-based pruning fails (e.g. a
    test bug passes non-monotonic _now). At 1000 req/s ×500-click
    batches the steady-state size is ~600 entries; maxlen=2000
    gives 3x headroom while ruling out unbounded growth."""
    m = ShipperMetrics()
    assert m.outcomes.maxlen == 2000


def test_record_outcome_emits_sentry_breadcrumb(monkeypatch):
    """Sprint 2.7d — close the Sprint 2.4 acceptance criterion that
    was under-delivered: plan §4 row 2.4 said "Sentry breadcrumb
    shows shipper.batch.success_ratio value per batch", but the
    original Sprint 2.4 commit shipped /health-only. This test
    pins the breadcrumb call shape so Sprint 4.1 alert rules + the
    Sentry UI can rely on consistent breadcrumb categories/data."""
    breadcrumbs = []

    def _fake_add_breadcrumb(**kwargs):
        breadcrumbs.append(kwargs)

    import sentry_sdk as real_sentry
    monkeypatch.setattr(real_sentry, "add_breadcrumb", _fake_add_breadcrumb)

    m = ShipperMetrics()
    m.record_outcome(accepted=80, rejected=20)

    assert len(breadcrumbs) == 1
    bc = breadcrumbs[0]
    assert bc["category"] == "shipper.batch"
    assert bc["level"] == "info"
    assert bc["data"]["accepted"] == 80
    assert bc["data"]["rejected"] == 20
    assert bc["data"]["success_ratio_5m"] == 0.8
    assert bc["data"]["window_entries"] == 1


def test_record_outcome_swallows_sentry_failure(monkeypatch):
    """Breadcrumb emission MUST be non-fatal. If Sentry SDK is
    misconfigured / unavailable / raises, the shipper continues —
    /health is the canonical signal, breadcrumbs are best-effort."""

    def _failing_add_breadcrumb(**kwargs):
        raise RuntimeError("simulated sentry failure")

    import sentry_sdk as real_sentry
    monkeypatch.setattr(real_sentry, "add_breadcrumb", _failing_add_breadcrumb)

    m = ShipperMetrics()
    # Must not raise.
    m.record_outcome(accepted=5, rejected=5)
    assert m.success_ratio_5m == 0.5
