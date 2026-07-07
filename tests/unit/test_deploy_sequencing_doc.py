"""LOSSFIX P3 (2026-07-07) — DONE criterion #2: the dedup-TTL deploy-
sequencing landmine (central-first + empty-queue precondition, NOT
CH-recency) must be documented, not just described in the brief.
Verifies the doc exists and states every required element — a doc-rot
guard for the runbook itself.
"""

from __future__ import annotations

from pathlib import Path


def _doc_path() -> Path:
    # services/click-processor/tests/unit/this_file.py -> repo root
    repo_root = Path(__file__).resolve().parents[4]
    return repo_root / "docs" / "development" / "lossfix-p3-2026-07-07" / "DEPLOY-SEQUENCING.md"


def test_deploy_sequencing_doc_exists():
    assert _doc_path().is_file(), (
        f"Expected the deploy-sequencing doc at {_doc_path()} — the "
        "P3 brief requires this as a doc deliverable, not just a "
        "description in commit messages."
    )


def test_deploy_sequencing_doc_covers_required_elements():
    text = _doc_path().read_text()
    required = [
        "Central-first",
        "disk_queue_size == 0",
        "CH-recency is the WRONG instrument",
        "30 minutes",
        "OP_WATERMARK_SPILL",
    ]
    missing = [r for r in required if r not in text]
    assert missing == [], (
        f"DEPLOY-SEQUENCING.md is missing required elements: {missing}"
    )
