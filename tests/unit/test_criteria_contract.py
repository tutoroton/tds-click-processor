"""CF-3 cross-service criteria contract (crash-test 2026-06-07).

Two durable guards + the per-dim evaluation proof for the three dims that
admin-api accepted but the click-processor never populated (isp_asn /
time_of_day / day_of_week — "dead criteria"):

  1. CONTRACT — every BASE criterion type admin-api accepts MUST be in the
     click-processor's evaluated-dims set. This would have caught CF-3 the moment
     a dim entered the admin registry without a matcher implementation.
  2. FAIL-CLOSED — an unknown / unimplemented / legacy dim MUST drop the
     flow/target, NOT let a `not_in` exclusion silently pass for all traffic
     (the CF-3 fail-OPEN direction).
  3. POPULATION — the 3 newly-wired dims are derived correctly from data already
     on the click (req.asn / req.arrival_ts, UTC) and evaluate end-to-end.
"""

from __future__ import annotations

from app.cascade import (
    _EVALUATED_BASE_DIMS,
    _first_failing_criterion,
    normalize_hour,
)
from app.models import ClickRequest
from app.router import _extra_click_dims

# Mirror of admin-api `app/common/parameters.py` CRITERION_TYPES (the base 10).
# The click-processor cannot import the admin-api module (separate service), so
# this literal is the cross-service contract anchor — keep it in lockstep with
# the admin registry. The first test pins base ⊆ evaluated so a NEW admin base
# dim without a matcher impl fails CI here.
ADMIN_ACCEPTED_BASE_DIMS = frozenset({
    "geo", "region", "city", "os", "device_type", "browser", "language",
    "isp_asn", "time_of_day", "day_of_week",
})


# ---- 1. CONTRACT ----------------------------------------------------------

def test_every_admin_accepted_base_dim_is_evaluated():
    """Every base criterion type admin-api accepts MUST be populated + evaluated
    by the click-processor. A dim in the admin registry but not here is DEAD
    (silent no-match on `in`, fail-OPEN on `not_in`) — exactly CF-3."""
    missing = ADMIN_ACCEPTED_BASE_DIMS - _EVALUATED_BASE_DIMS
    assert not missing, (
        "admin-accepted but never evaluated (CF-3 dead, not_in fail-open): "
        f"{sorted(missing)}"
    )


# ---- 2. FAIL-CLOSED on unknown dims (both ops) ----------------------------

def test_unknown_dim_in_fails_closed():
    """An unknown dim with `op=in` drops the flow (was already fail-closed via
    the empty value — pinned to lock it)."""
    c = [{"type": "totally_unknown_dim", "op": "in", "values": ["x"]}]
    assert _first_failing_criterion(c, {"geo": "US"}) is not None


def test_unknown_dim_not_in_fails_closed():
    """THE CF-3 fix: an unknown dim with `op=not_in` MUST drop the flow. Pre-fix
    this returned None (criterion held for "" → exclusion was a no-op for ALL
    traffic → an operator's 'block these' silently became 'allow all')."""
    c = [{"type": "totally_unknown_dim", "op": "not_in", "values": ["x"]}]
    assert _first_failing_criterion(c, {"geo": "US"}) is not None


# ---- 3. POPULATION + evaluation of the 3 newly-wired dims ------------------

def _req(*, asn: int = 0, arrival_ts: str | None = None) -> ClickRequest:
    return ClickRequest(click_id="cf3", asn=asn, arrival_ts=arrival_ts)


def test_extra_dims_isp_asn_from_req_asn():
    assert _extra_click_dims(_req(asn=13335))["isp_asn"] == "13335"


def test_extra_dims_isp_asn_zero_is_matchable_string():
    """asn=0 is CF's no-data sentinel (`request.cf?.asn || 0`) and req.asn is an
    int that is ALWAYS present (default 0) → it maps to the MATCHABLE string
    "0", NOT "". This lets an operator's `not_in ['0']` exclude unknown/
    datacenter-ASN traffic (the CF-3 repro) and `in ['0']` target it; an
    `in [<real asn>]` on a 0 click still fails closed ("0" ∉ the list). Mapping
    0 → "" would re-open the not_in fail-open."""
    assert _extra_click_dims(_req(asn=0))["isp_asn"] == "0"


def test_extra_dims_temporal_utc_unpadded():
    # 2026-06-07T13:45Z (UTC) — Sunday, hour 13 (un-padded, matches the admin
    # validator which accepts "13").
    dims = _extra_click_dims(_req(arrival_ts="2026-06-07T13:45:30Z"))
    assert dims["time_of_day"] == "13"
    assert dims["day_of_week"] == "sun"


def test_extra_dims_temporal_midnight_unpadded():
    dims = _extra_click_dims(_req(arrival_ts="2026-06-08T00:05:00.123456Z"))
    assert dims["time_of_day"] == "0"   # un-padded, NOT "00"
    assert dims["day_of_week"] == "mon"


def test_extra_dims_absent_arrival_ts_empty_fail_closed():
    """Old worker / absent arrival_ts ⇒ "" ⇒ fail-closed on `in`."""
    dims = _extra_click_dims(_req(arrival_ts=None))
    assert dims["time_of_day"] == "" and dims["day_of_week"] == ""


def test_extra_dims_malformed_arrival_ts_empty():
    """The ClickRequest model already rejects a malformed arrival_ts at
    construction (strict ISO-8601-Z pattern), so this path is normally
    unreachable — but the helper is defensively safe (never raises) if handed a
    bad value. Exercise that with a minimal stub."""
    from types import SimpleNamespace

    dims = _extra_click_dims(SimpleNamespace(asn=0, arrival_ts="not-a-timestamp"))
    assert dims["time_of_day"] == "" and dims["day_of_week"] == ""


def test_isp_asn_in_evaluated_end_to_end():
    """The dim is now LIVE in the matcher: `in` matches the click's asn and
    rejects a different asn (was a silent no-match for every value pre-fix)."""
    attrs = {"geo": "US", "isp_asn": "13335"}
    assert _first_failing_criterion(
        [{"type": "isp_asn", "op": "in", "values": ["13335"]}], attrs) is None
    assert _first_failing_criterion(
        [{"type": "isp_asn", "op": "in", "values": ["15169"]}], attrs) is not None


def test_isp_asn_not_in_excludes_matching():
    """`not_in [13335]` now correctly EXCLUDES an asn-13335 click (pre-fix the
    exclusion was a no-op — fail-open)."""
    attrs = {"geo": "US", "isp_asn": "13335"}
    assert _first_failing_criterion(
        [{"type": "isp_asn", "op": "not_in", "values": ["13335"]}], attrs) is not None


def test_isp_asn_not_in_zero_excludes_no_asn_click_end_to_end():
    """CF-3 ORIGINAL repro (EX4: `isp_asn not_in [0]` WON for an asn-0 click).
    Built end-to-end through `_extra_click_dims` → matcher: asn 0 → "0" so
    "0" in ['0'] → criterion FAILS → the flow/target is EXCLUDED. Pre-follow-up
    asn 0 → "" → the exclusion was a no-op (fail-open) and the repro stayed
    open."""
    attrs = {"geo": "US", **_extra_click_dims(_req(asn=0))}
    assert attrs["isp_asn"] == "0"
    excluded = _first_failing_criterion(
        [{"type": "isp_asn", "op": "not_in", "values": ["0"]}], attrs)
    assert excluded is not None  # asn-0 click is EXCLUDED (repro closed)


def test_isp_asn_in_zero_targets_no_asn_click():
    """The dual: `in ['0']` TARGETS the no-ASN click (matchable), while
    `in [<real asn>]` on a 0 click fails closed."""
    attrs = {"geo": "US", **_extra_click_dims(_req(asn=0))}
    assert _first_failing_criterion(
        [{"type": "isp_asn", "op": "in", "values": ["0"]}], attrs) is None
    assert _first_failing_criterion(
        [{"type": "isp_asn", "op": "in", "values": ["13335"]}], attrs) is not None


def test_day_of_week_admin_value_uppercase_lowercased():
    """day_of_week is NOT case-preserved → an admin value 'SUN' is lowercased to
    match the emitted 'sun'."""
    attrs = {"day_of_week": "sun"}
    assert _first_failing_criterion(
        [{"type": "day_of_week", "op": "in", "values": ["SUN"]}], attrs) is None


def test_time_of_day_in_matches():
    attrs = {"time_of_day": "13"}
    assert _first_failing_criterion(
        [{"type": "time_of_day", "op": "in", "values": ["13", "14"]}], attrs) is None
    assert _first_failing_criterion(
        [{"type": "time_of_day", "op": "in", "values": ["9"]}], attrs) is not None


# ---- R72: time_of_day zero-pad normalization (cascade matcher) -------------
# The edge emits an un-padded hour ("9"); the admin validator accepts BOTH "9"
# and "09" (`^(0?[0-9]|1[0-9]|2[0-3])$`). Pre-fix a saved "09" criterion never
# matched a 9:00 click (`in` no-match, `not_in` silent no-op). `normalize_hour`
# canonicalizes BOTH sides at compare time, time_of_day ONLY.

def test_time_of_day_zero_padded_criterion_matches_unpadded_click():
    """THE FIX — a saved "09" matches a 9:00 click ("9"). Was: no match."""
    attrs = {"time_of_day": "9"}
    assert _first_failing_criterion(
        [{"type": "time_of_day", "op": "in", "values": ["09"]}], attrs) is None


def test_time_of_day_not_in_zero_padded_excludes_unpadded_click():
    """`not_in ["09"]` now EXCLUDES a 9:00 click (was a silent fail-open no-op)."""
    attrs = {"time_of_day": "9"}
    assert _first_failing_criterion(
        [{"type": "time_of_day", "op": "not_in", "values": ["09"]}], attrs) is not None


def test_time_of_day_midnight_padded_matches():
    """Midnight: click "0" matches both a "00"-saved and a "0"-saved criterion
    (validator already accepts "00"; zfill would have fail-OPENED here)."""
    attrs = {"time_of_day": "0"}
    assert _first_failing_criterion(
        [{"type": "time_of_day", "op": "in", "values": ["00"]}], attrs) is None
    assert _first_failing_criterion(
        [{"type": "time_of_day", "op": "in", "values": ["0"]}], attrs) is None


def test_time_of_day_regression_unpadded_and_two_digit_unaffected():
    """Regression — un-padded and two-digit hours still match as before."""
    assert _first_failing_criterion(
        [{"type": "time_of_day", "op": "in", "values": ["9"]}],
        {"time_of_day": "9"}) is None
    assert _first_failing_criterion(
        [{"type": "time_of_day", "op": "in", "values": ["13"]}],
        {"time_of_day": "13"}) is None


def test_time_of_day_absent_click_fails_closed_preserved():
    """Absent arrival_ts ⇒ click_val "" passes through normalize unchanged →
    fail-closed on `in`, fail-open on `not_in` (documented legacy semantics,
    unchanged by R72)."""
    attrs = {"time_of_day": ""}
    assert _first_failing_criterion(
        [{"type": "time_of_day", "op": "in", "values": ["09"]}], attrs) is not None
    assert _first_failing_criterion(
        [{"type": "time_of_day", "op": "not_in", "values": ["09"]}], attrs) is None


def test_normalize_hour_scope_guard_other_digit_dims_untouched():
    """SCOPE GUARD — a digit-valued NON-time_of_day dim (isp_asn) is NOT
    normalized: "09" ≠ "9" stays a no-match, proving the special-case is
    keyed `dim == "time_of_day"` only."""
    attrs = {"isp_asn": "09"}
    assert _first_failing_criterion(
        [{"type": "isp_asn", "op": "in", "values": ["9"]}], attrs) is not None


def test_normalize_hour_unit():
    """The shared helper: digits → leading-zero-stripped; "" and junk pass
    through unchanged (zfill is forbidden — it would fail-open at midnight)."""
    assert normalize_hour("09") == "9"
    assert normalize_hour("9") == "9"
    assert normalize_hour("0") == "0"
    assert normalize_hour("00") == "0"
    assert normalize_hour("13") == "13"
    assert normalize_hour("") == ""        # absent arrival → stays "" (fail-closed)
    assert normalize_hour("9am") == "9am"  # junk passes through (never a real hour)
