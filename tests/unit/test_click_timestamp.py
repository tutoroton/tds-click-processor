"""F.24 Phase 5.1b — edge-stable click timestamp.

True racing (F.24 Phase 5) makes the CF Worker fire EVERY click at ALL
connected nodes concurrently. The collector dedups on
`ON CONFLICT (click_id, created_at)` and the clicks table is
`PRIMARY KEY (click_id, created_at) PARTITION BY RANGE(created_at)` —
so a plain UNIQUE(click_id) is impossible and the conflict key's
`created_at` half MUST be byte-identical across every raced node or
one real click lands as up-to-N rows (analytics / caps / payout
inflation).

The fix: the Worker emits a canonical `click_ts` ONCE at the edge;
click-processor records THAT (not its own per-node `gmtime()`) so
`created_at` is edge-stable. These tests pin both halves of the
contract: the boundary validator on `ClickRequest.click_ts` and the
`_resolve_click_timestamp` precedence/fallback.
"""

import re

import pytest
from pydantic import ValidationError

from app.models import ClickRequest
from app.main import _resolve_click_timestamp

_GMTIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")


class TestResolveClickTimestamp:
    """Precedence: edge value wins; absent/empty → node gmtime fallback."""

    def test_edge_value_passed_through_verbatim(self):
        # The byte-for-byte passthrough is THE cross-node dedup guarantee:
        # every raced node calls this with the same edge string and must
        # emit it unchanged (no re-parse/re-format that could drift).
        edge = "2026-05-16T12:34:56.789Z"
        assert _resolve_click_timestamp(edge) == edge

    def test_edge_value_with_microseconds_passed_through(self):
        edge = "2026-05-16T12:34:56.123456Z"
        assert _resolve_click_timestamp(edge) == edge

    def test_none_falls_back_to_node_gmtime(self):
        out = _resolve_click_timestamp(None)
        assert _GMTIME_RE.match(out), f"expected node gmtime, got {out!r}"

    def test_empty_string_falls_back_to_node_gmtime(self):
        # A Pydantic-stripped "" must NOT become the click's time —
        # `or` treats it the same as None.
        out = _resolve_click_timestamp("")
        assert _GMTIME_RE.match(out), f"expected node gmtime, got {out!r}"

    def test_two_calls_same_edge_value_are_identical(self):
        # Simulates two raced nodes resolving the SAME edge click_ts —
        # they MUST produce identical strings so the collector
        # ON CONFLICT (click_id, created_at) collapses them to one row.
        edge = "2026-05-16T00:00:00.000Z"
        assert _resolve_click_timestamp(edge) == _resolve_click_timestamp(edge)


class TestClickTsValidation:
    """ClickRequest.click_ts boundary contract (api-contracts /
    data-handling — the value parameterises a TIMESTAMPTZ + partition
    routing key, so malformed input is rejected at the edge of the
    system, not silently coerced)."""

    def test_absent_is_none(self):
        req = ClickRequest(click_id="abc123")
        assert req.click_ts is None

    def test_valid_millisecond_iso_utc(self):
        req = ClickRequest(click_id="abc123", click_ts="2026-05-16T12:34:56.789Z")
        assert req.click_ts == "2026-05-16T12:34:56.789Z"

    def test_valid_second_granularity_iso_utc(self):
        req = ClickRequest(click_id="abc123", click_ts="2026-05-16T12:34:56Z")
        assert req.click_ts == "2026-05-16T12:34:56Z"

    def test_valid_microsecond_fraction(self):
        req = ClickRequest(click_id="abc123", click_ts="2026-05-16T12:34:56.123456Z")
        assert req.click_ts == "2026-05-16T12:34:56.123456Z"

    @pytest.mark.parametrize(
        "bad",
        [
            # shape errors
            "2026-05-16T12:34:56",           # missing Z (no offset)
            "2026-05-16T12:34:56+00:00",     # offset form, not Z — one canonical shape only
            "2026-05-16 12:34:56Z",          # space instead of T
            "not-a-timestamp",
            "2026-05-16T12:34:56.1234567Z",  # 7 fractional digits (>6)
            "",                              # explicit empty string is invalid as a value
            "T12:34:56Z",                    # missing date
            # F.24 Phase 5.1b finding #3 — semantic-range tightening:
            # click_ts is a PG RANGE-partition routing key, so the
            # obvious far-future / out-of-range / impossible-component
            # class is rejected at ingress (defense-in-depth alongside
            # the clicks_default partition).
            "1999-05-16T12:34:56Z",          # year not 20xx
            "3000-05-16T12:34:56Z",          # far-future year not 20xx
            "2026-13-16T12:34:56Z",          # month 13
            "2026-00-16T12:34:56Z",          # month 00
            "2026-05-32T12:34:56Z",          # day 32
            "2026-05-00T12:34:56Z",          # day 00
            "2026-05-16T24:34:56Z",          # hour 24
            "2026-05-16T12:60:56Z",          # minute 60
            "2026-05-16T12:34:60Z",          # second 60
        ],
    )
    def test_malformed_rejected(self, bad):
        with pytest.raises(ValidationError):
            ClickRequest(click_id="abc123", click_ts=bad)

    def test_boundary_components_accepted(self):
        # Legit edge values that MUST still pass after the tightening.
        for ok in (
            "2000-01-01T00:00:00Z",
            "2099-12-31T23:59:59.999999Z",
            "2026-02-28T00:00:00.000Z",
        ):
            assert ClickRequest(click_id="abc123", click_ts=ok).click_ts == ok

    def test_overlong_rejected(self):
        with pytest.raises(ValidationError):
            ClickRequest(click_id="abc123", click_ts="2026-05-16T12:34:56.789Z" + "0" * 40)
