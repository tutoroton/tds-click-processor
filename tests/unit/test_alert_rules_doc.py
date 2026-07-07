"""LOSSFIX P3 (2026-07-07) — A4 MUST: the alert-rule spec's tag
enumeration is CODE-DERIVED (grep `^OP_` in telemetry.py), never
hand-recalled. This test performs the SAME grep the doc's own methodology
section prescribes and asserts every non-excluded tag is actually
covered by docs/development/lossfix-p3-2026-07-07/ALERT-RULES.md — a
future OP_* constant added to telemetry.py without an accompanying doc
update fails this test, closing the "spec silently drifts from code" gap.
"""

from __future__ import annotations

import re
from pathlib import Path


def _repo_root() -> Path:
    # services/click-processor/tests/unit/this_file.py -> repo root
    return Path(__file__).resolve().parents[4]


def _telemetry_op_tags() -> set[str]:
    src = (_repo_root() / "services" / "click-processor" / "app" / "telemetry.py").read_text()
    return set(re.findall(r"^(OP_\w+)\s*=", src, re.MULTILINE))


# Deliberately excluded — pre-existing telemetry that is NOT a LOSSFIX
# loss/shed/pressure signal (routing/identity-resolve) or predates this
# phase and is out of its scope (Sprint 1.3 shipper-exception tags,
# except OP_LOOP_ITERATION which gets a NEW filter this phase adds).
_EXCLUDED_ROUTING_IDENTITY = {
    "OP_ROUTE_ERROR", "OP_CRITERIA_SKIP", "OP_FLOW_LOAD", "OP_PARAM_PARSE",
    "OP_PARAM_RULES", "OP_OFFER_RESOLVE", "OP_SPLIT_FALLBACK",
    "OP_FLOW_READ_FAILED", "OP_NO_FLOW_NO_OFFER", "OP_IDENTITY",
    "OP_IDENTITY_PERSIST", "OP_STICKY_WRITE", "OP_PARTIAL_ACK",
    "OP_LEGACY_COLLECTOR",
}
_EXCLUDED_PRE_EXISTING_SHIPPER = {
    "OP_XREADGROUP", "OP_PARSE_PAYLOAD", "OP_BATCH_POST", "OP_XACK",
    "OP_XACK_BATCH",
}


def _alert_rules_doc_text() -> str:
    return (
        _repo_root() / "docs" / "development" / "lossfix-p3-2026-07-07" / "ALERT-RULES.md"
    ).read_text()


def test_alert_rules_doc_exists():
    doc_path = (
        _repo_root() / "docs" / "development" / "lossfix-p3-2026-07-07" / "ALERT-RULES.md"
    )
    assert doc_path.is_file()


def test_every_non_excluded_op_tag_has_a_rule():
    tags = _telemetry_op_tags()
    excluded = _EXCLUDED_ROUTING_IDENTITY | _EXCLUDED_PRE_EXISTING_SHIPPER
    needs_a_rule = tags - excluded
    doc = _alert_rules_doc_text()

    missing = [t for t in sorted(needs_a_rule) if t not in doc]
    assert missing == [], (
        f"OP_* constants missing an alert-rule entry in ALERT-RULES.md: "
        f"{missing}. Either add a rule row, or add the tag to the "
        "excluded set in ALERT-RULES.md + this test with a stated reason."
    )


def test_every_excluded_tag_is_still_a_real_constant():
    """Catches the OTHER drift direction — an excluded-tag name that
    was renamed/removed in telemetry.py would silently make the
    exclusion list stale (excluding nothing)."""
    tags = _telemetry_op_tags()
    excluded = _EXCLUDED_ROUTING_IDENTITY | _EXCLUDED_PRE_EXISTING_SHIPPER
    stale = excluded - tags
    assert stale == [] if isinstance(stale, list) else stale == set(), (
        f"Excluded tags no longer exist in telemetry.py (renamed or "
        f"removed?): {stale}"
    )


def test_op_loop_iteration_filter_documented():
    """The failure_kind filter + T-7 mute pointer must be present,
    not just the bare tag."""
    doc = _alert_rules_doc_text()
    assert "failure_kind" in doc
    assert "TimeoutError" in doc
    assert "T-7" in doc


def test_routes_are_placeholders_not_invented_targets():
    """B3: never invent a paging target — every route must be the
    explicit TBD placeholder."""
    doc = _alert_rules_doc_text()
    # Every rule row (a markdown table row starting with `| \`OP_`)
    # must end its route cell with TBD.
    rule_rows = [
        line for line in doc.splitlines()
        if line.startswith("| `OP_") and "|" in line
    ]
    assert rule_rows, "Expected to find rule table rows in ALERT-RULES.md"
    # TBD = a real rule awaiting an owner assignment; a bare em-dash "—"
    # = the MUTED row (no route at all, by design — nothing pages).
    # Either is a non-invented placeholder; anything else would be a
    # concretely-named paging target, which B3 forbids.
    non_placeholder = [
        row for row in rule_rows
        if not (row.rstrip().endswith("TBD |") or row.rstrip().endswith("— |"))
    ]
    assert non_placeholder == [], (
        f"Found rule rows with a non-placeholder route (must be TBD or "
        f"— for MUTED): {non_placeholder}"
    )
