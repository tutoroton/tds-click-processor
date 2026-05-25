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
from app.main import _created_at_from_click_id, _resolve_click_timestamp

_GMTIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")

# A canonical CF-Worker click_id: 12 hex ms-epoch + 12 hex random.
# 0x019e5be83c81 = 1779658407041 ms = 2026-05-24T21:33:27.041Z.
_CANONICAL_CLICK_ID = "019e5be83c8179896a0859dd"
_CANONICAL_DERIVED_TS = "2026-05-24T21:33:27.041Z"


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


class TestClickIdDerivedTimestamp:
    """F-4 (audit 2026-05-25) — when the Worker did NOT forward click_ts
    (older Worker in a dual-deploy window), every racing node derives the
    SAME instant from the shared UUIDv7-style click_id, keeping the
    collector's (click_id, created_at) PK skew-immune instead of each node
    stamping its own gmtime() and inflating one click into N rows."""

    def test_derives_from_canonical_click_id_when_ts_absent(self):
        assert (
            _resolve_click_timestamp(None, _CANONICAL_CLICK_ID)
            == _CANONICAL_DERIVED_TS
        )

    def test_empty_ts_also_derives_from_click_id(self):
        # "" is treated like None (Pydantic-stripped) — still derive.
        assert (
            _resolve_click_timestamp("", _CANONICAL_CLICK_ID)
            == _CANONICAL_DERIVED_TS
        )

    def test_three_nodes_no_header_yield_identical_created_at(self):
        # THE skew-immunity guarantee: 3 raced nodes, no click_ts header,
        # same click_id → identical timestamp → collector PK collapses
        # the fan-out to ONE row (the "no-header 3-node → 1 row" case).
        outs = {_resolve_click_timestamp(None, _CANONICAL_CLICK_ID) for _ in range(3)}
        assert len(outs) == 1
        assert next(iter(outs)) == _CANONICAL_DERIVED_TS

    def test_explicit_ts_wins_over_click_id_derivation(self):
        # Precedence: a present edge click_ts is authoritative; the
        # click_id derivation only fills the gap when it is absent.
        edge = "2026-05-16T12:34:56.789Z"
        assert _resolve_click_timestamp(edge, _CANONICAL_CLICK_ID) == edge

    def test_smoke_test_id_falls_back_to_gmtime(self):
        # smoke-test-* ids are not 24-hex and are NOT subject to racing
        # fan-out → per-node gmtime is acceptable (and correct: a smoke
        # click_id carries no encoded epoch).
        out = _resolve_click_timestamp(None, "smoke-test-fra1-deadbeef")
        assert _GMTIME_RE.match(out), f"expected node gmtime, got {out!r}"

    def test_legacy_short_id_falls_back_to_gmtime(self):
        out = _resolve_click_timestamp(None, "abc123")
        assert _GMTIME_RE.match(out), f"expected node gmtime, got {out!r}"

    def test_implausible_epoch_prefix_falls_back_to_gmtime(self):
        # 24 hex chars but the ms-prefix decodes to 1970 — a non-canonical
        # id whose prefix is valid hex but not a real epoch. The
        # plausibility fence rejects it rather than fabricate a 1970
        # created_at.
        out = _resolve_click_timestamp(None, "0000016b6c3f0000aaaabbbb")
        assert _GMTIME_RE.match(out), f"expected node gmtime, got {out!r}"


class TestCreatedAtFromClickId:
    """Direct unit tests of the pure derivation helper."""

    def test_canonical_id_returns_ms_iso(self):
        assert _created_at_from_click_id(_CANONICAL_CLICK_ID) == _CANONICAL_DERIVED_TS

    def test_none_returns_none(self):
        assert _created_at_from_click_id(None) is None

    @pytest.mark.parametrize(
        "bad",
        [
            "abc123",                       # too short
            "smoke-test-fra1-deadbeef",     # non-hex, wrong length
            "019e5be83c8179896a0859dd0000",  # too long (28)
            "zzzzzzzzzzzz79896a0859dd",      # 24 chars but prefix not hex
            "0000016b6c3f0000aaaabbbb",      # plausible-hex but 1970 epoch
            "ffffffffffff79896a0859dd",      # far-future epoch (> 2100)
        ],
    )
    def test_non_canonical_returns_none(self, bad):
        assert _created_at_from_click_id(bad) is None

    def test_derivation_is_deterministic(self):
        a = _created_at_from_click_id(_CANONICAL_CLICK_ID)
        b = _created_at_from_click_id(_CANONICAL_CLICK_ID)
        assert a == b == _CANONICAL_DERIVED_TS

    def test_plausibility_fence_boundaries(self):
        # The fence is inclusive [2020-01-01, 2100-01-01] in ms. Pin both
        # edges: just-inside accepts, just-outside falls back to None.
        from app.main import _CLICK_ID_MS_MIN, _CLICK_ID_MS_MAX

        def _id_for_ms(ms: int) -> str:
            return f"{ms:012x}" + "0" * 12  # 12-hex ms-prefix + 12-hex suffix

        assert _created_at_from_click_id(_id_for_ms(_CLICK_ID_MS_MIN)) is not None
        assert _created_at_from_click_id(_id_for_ms(_CLICK_ID_MS_MAX)) is not None
        assert _created_at_from_click_id(_id_for_ms(_CLICK_ID_MS_MIN - 1)) is None
        assert _created_at_from_click_id(_id_for_ms(_CLICK_ID_MS_MAX + 1)) is None

    def test_result_is_collector_parseable(self):
        # The derived string must round-trip through the collector's
        # datetime.fromisoformat(ts.replace("Z","+00:00")) parse so it
        # becomes a valid TIMESTAMPTZ created_at.
        from datetime import datetime
        ts = _created_at_from_click_id(_CANONICAL_CLICK_ID)
        parsed = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        assert parsed.year == 2026 and parsed.tzinfo is not None
