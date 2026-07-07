"""LOSSFIX P3 (2026-07-07) — DONE criterion #5: the F1 conversion-Keeper
interlock's relationship to LOSSFIX must be documented (a docs section,
not a code change). Verifies the doc exists and states every required
element, including the B1 residual-window re-affirmation.
"""

from __future__ import annotations

from pathlib import Path


def _doc_path() -> Path:
    # services/click-processor/tests/unit/this_file.py -> repo root
    repo_root = Path(__file__).resolve().parents[4]
    return repo_root / "docs" / "development" / "lossfix-p3-2026-07-07" / "F1-INTERLOCK.md"


def test_f1_interlock_doc_exists():
    assert _doc_path().is_file(), (
        f"Expected the F1-interlock doc at {_doc_path()} — the P3 brief "
        "requires this as a doc deliverable, not just a description in "
        "commit messages."
    )


def test_f1_interlock_doc_covers_required_elements():
    text = _doc_path().read_text()
    required = [
        "materialized_views_ignore_errors",  # what F1 actually is
        "INV-1",  # why it doesn't drop a conversion
        "reclaim",  # how it composes with the redeliver-on-crash pattern
        "OP_CONV_ROLLUP_DRIFT",  # the drift monitor
        "rollup_integrity.last_rebuild_at > last_drift_at",  # the rebuild interlock
        "Same residual, new number",  # B1 re-affirmation, not rediscovery
    ]
    missing = [r for r in required if r not in text]
    assert missing == [], (
        f"F1-INTERLOCK.md is missing required elements: {missing}"
    )
