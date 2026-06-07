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


def test_extra_dims_isp_asn_zero_is_empty_fail_closed():
    """asn=0 is CF's no-data sentinel (`request.cf?.asn || 0`) ⇒ "" so an
    `in [<real asn>]` fails closed on a no-ASN click (we never match phantom
    data). A `not_in` on the no-data case stays open by nature — bounded + rare,
    same accepted residual as an absent arrival_ts."""
    assert _extra_click_dims(_req(asn=0))["isp_asn"] == ""


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
