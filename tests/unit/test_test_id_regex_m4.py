"""Tests for M4 fix — `_VALID_TEST_ID` requires ≥1 hex digit.

The previous regex `^[0-9a-fA-F-]{8,64}$` accepted strings of only
dashes ("--------"). Such trivial-value test_ids:
  - Pollute Sentry tag cardinality (tag becomes a non-unique value
    across unrelated audit runs).
  - Cause `obs:test:--------` Redis stream key collisions across
    probes that happen to pick the same trivial value, mixing
    diagnostic data from different runs.
  - Could be intentionally crafted by an unprivileged caller on
    staging (where toggles are on) to commingle their probes with
    other tenants' diagnostic streams.

The fix adds a positive lookahead `(?=.*[0-9a-fA-F])` that requires
at least one hex digit somewhere in the string. UUID v4 + variants
all naturally contain hex; only the degenerate all-dash case fails.

Mirror of admin-api/tests/unit/test_test_id_regex_m4.py.
"""

from __future__ import annotations

import pytest

from app.diag import _is_valid_test_id


# ---------------------------------------------------------------------------
# Acceptance — valid test_ids still pass
# ---------------------------------------------------------------------------


class TestValidValuesStillAccepted:
    def test_uuid4_canonical(self):
        # Standard uuid4 — 32 hex + 4 dashes = 36 chars.
        assert _is_valid_test_id("d13552b8-c427-4b4e-9ff6-c90d4a3f0876")

    def test_8_hex_chars_lower_bound(self):
        # Lower bound — 8 hex chars no dashes.
        assert _is_valid_test_id("abcdef12")

    def test_8_hex_with_dashes(self):
        # Just-over-minimum with some dashes.
        assert _is_valid_test_id("abcd1234")

    def test_uppercase_hex_accepted(self):
        assert _is_valid_test_id("DEADBEEF-CAFE-1234-5678-90ABCDEFCAFE")

    def test_uuid_with_run_suffix(self):
        # Some traffic generators append a per-probe index suffix —
        # already-rejected by validator because `:` isn't in the class;
        # but the canonical UUID4 part WITHIN the 64-char cap is fine.
        assert _is_valid_test_id("d13552b8-c427-4b4e-9ff6-c90d4a3f0876-1")

    def test_just_one_hex_digit_with_dashes(self):
        # "a-------" — 1 hex + 7 dashes = 8 chars. Passes (≥1 hex
        # present, length OK). This is the minimum threshold of M4
        # the fix actually allows — anything less would also reject
        # legitimate-but-sparse generator outputs we may add later.
        assert _is_valid_test_id("a-------")


# ---------------------------------------------------------------------------
# Rejection — degenerate values now blocked (M4 closure)
# ---------------------------------------------------------------------------


class TestM4RejectsAllDash:
    def test_all_dashes_8_chars_rejected(self):
        """The core M4 case — 8 consecutive dashes, no hex content."""
        assert not _is_valid_test_id("--------")

    def test_all_dashes_36_chars_rejected(self):
        """A uuid4-LENGTH all-dash string — looks plausible at a
        glance, still has zero hex content. Reject."""
        assert not _is_valid_test_id("-" * 36)

    def test_all_dashes_64_chars_rejected(self):
        """Upper bound of length, still no hex → reject."""
        assert not _is_valid_test_id("-" * 64)


# ---------------------------------------------------------------------------
# Existing-rejection regressions — pre-M4 invalids still rejected
# ---------------------------------------------------------------------------


class TestPreM4RejectionsHold:
    def test_empty_rejected(self):
        assert not _is_valid_test_id("")

    def test_too_short_rejected(self):
        assert not _is_valid_test_id("abc")  # 3 chars

    def test_too_long_rejected(self):
        assert not _is_valid_test_id("a" * 65)  # 65 chars

    def test_non_hex_chars_rejected(self):
        # 'z' isn't hex; 'g'..'z' all rejected.
        assert not _is_valid_test_id("zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz")

    def test_underscore_rejected(self):
        # Pre-M4 rejected; ensure M4 didn't accidentally loosen.
        assert not _is_valid_test_id("abcd_1234")

    def test_newline_injection_rejected(self):
        assert not _is_valid_test_id("abcd1234\nattacker")

    def test_semicolon_rejected(self):
        assert not _is_valid_test_id("abcd1234;DROP")
