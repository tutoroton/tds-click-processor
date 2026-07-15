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

GTD-R135 Phase 0 (2026-07-14) adds the filter-system-extension build's 3 pinned
invariants (Unknown 7) BEFORE any new dim lands:

  A. dashboard picker parity — see `services/dashboard/src/lib/
     criteria-dictionaries.test.ts` (separate service, separate test file).
  B. per-field {dropdown values} == {normalizer codomain} — device_type here.
  C. both matchers' dim-sets in lockstep (legacy frozen at base-10; cascade a
     superset, never narrower).
"""

from __future__ import annotations

from app.cascade import (
    KNOWN_EVALUATED_DIMS,
    STRUCTURAL_CRITERION_DIMS,
    IDENTIFIER_SLOTS,
    _EVALUATED_BASE_DIMS,
    _first_failing_criterion,
    normalize_hour,
    normalize_language,
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


# ---- G1 (GTD-R135, 2026-07-14): language region-stripping normalization ----
# The edge (`parse_accept_language`) correctly parses the FULL BCP47 primary
# tag INCLUDING region ("en-US"); the picker only ever offers bare codes
# ("en"). Pre-fix a saved "en" criterion never matched a region-tagged click
# (byte-for-byte `in`/`not_in` comparison) — a live, unguarded bug affecting
# the majority of real en/pt/zh/es/ar clicks (they carry a region tag).
# `normalize_language` canonicalizes BOTH sides at compare time, language
# ONLY. Kept in lockstep with `router.resolve_target_with_id` (test_router.py
# `test_g1_language_matcher_parity_cascade_vs_legacy`).

def test_language_region_tagged_criterion_matches_bare_click():
    """THE FIX — a saved "en" matches a region-tagged "en-US" click. Was: no
    match (this assertion is RED on unpatched code, GREEN after the fix)."""
    attrs = {"language": "en-US"}
    assert _first_failing_criterion(
        [{"type": "language", "op": "in", "values": ["en"]}], attrs) is None


def test_language_not_in_excludes_region_tagged_click():
    """`not_in ["en"]` now correctly EXCLUDES an "en-US" click (was a silent
    fail-open no-op — the exclusion never matched any saved bare code)."""
    attrs = {"language": "en-US"}
    assert _first_failing_criterion(
        [{"type": "language", "op": "not_in", "values": ["en"]}], attrs) is not None


def test_language_genuine_mismatch_still_fails_closed():
    """Regression — a genuinely different primary language still fails to
    match after normalization (proves this isn't an accidental match-all)."""
    attrs = {"language": "ru-RU"}
    assert _first_failing_criterion(
        [{"type": "language", "op": "in", "values": ["en"]}], attrs) is not None


def test_language_bare_code_unaffected():
    """Regression — a bare-code click against a bare-code criterion still
    matches exactly as before (byte-identical for the already-working case)."""
    attrs = {"language": "uk"}
    assert _first_failing_criterion(
        [{"type": "language", "op": "in", "values": ["uk"]}], attrs) is None


def test_language_absent_click_fails_closed_preserved():
    """Absent/unparseable Accept-Language ⇒ click_val "" passes through
    normalize unchanged → fail-closed on `in`, fail-open on `not_in`
    (documented legacy semantics, unchanged by G1)."""
    attrs = {"language": ""}
    assert _first_failing_criterion(
        [{"type": "language", "op": "in", "values": ["en"]}], attrs) is not None
    assert _first_failing_criterion(
        [{"type": "language", "op": "not_in", "values": ["en"]}], attrs) is None


def test_normalize_language_scope_guard_other_case_preserve_dims_untouched():
    """SCOPE GUARD — a hyphenated value in a NON-language _CASE_PRESERVE dim
    (region) is NOT normalized: proves the special-case is keyed
    `dim == "language"` only."""
    attrs = {"region": "en-US"}
    assert _first_failing_criterion(
        [{"type": "region", "op": "in", "values": ["en"]}], attrs) is not None


def test_normalize_language_unit():
    """The shared helper: strips a trailing "-XX" region suffix; a bare code
    and "" pass through unchanged (idempotent)."""
    assert normalize_language("en-US") == "en"
    assert normalize_language("pt-BR") == "pt"
    assert normalize_language("en") == "en"        # already bare — idempotent
    assert normalize_language("") == ""             # absent → stays "" (fail-closed)
    assert normalize_language("uk") == "uk"


# ---- GTD-R135 Phase 0/2 — Invariant B: device_type codomain ----------------
# "per-field {dropdown values} == {normalizer codomain}" (Unknown 7). This is
# the exact check that would have caught the 3-dead-values bug (tv/console/
# other) on day one. Verified by direct read of `ua_parser.py:52-61`: every
# branch of the collapse `if/elif/else` resolves to one of exactly THREE
# strings — there is no code path that returns "tv"/"console"/"other" from
# `parse_ua()["device_type"]` (that raw library value is captured separately
# as `device_type_raw`, never surfaced to the matcher). Both matchers read
# `parse_device_type()` → `parse_ua()["device_type"]`, so this codomain is
# shared by cascade AND the legacy matcher.
_UA_PARSER_DEVICE_TYPE_CODOMAIN = frozenset({"mobile", "tablet", "desktop"})

# Mirror of admin-api `app/common/parameters.py`
# `CRITERION_VALUE_VALIDATORS["device_type"]["enum"]`. The click-processor
# cannot import the admin-api module (separate service) — this literal is the
# cross-service contract anchor, kept in lockstep with the admin registry.
#
# Phase 2 (G2, 2026-07-14) shrunk the admin enum + dashboard
# `DEVICE_TYPE_VALUES` to match this codomain EXACTLY (tv/console/other
# deleted — structurally unreachable). Assertion below flips from subset
# (`<=`, Phase 0's honest-baseline check) to equality (`==`) — this flip
# IS Phase 2's done-criterion.
_ADMIN_DEVICE_TYPE_ENUM = frozenset({"mobile", "desktop", "tablet"})


def test_device_type_codomain_equals_admin_enum():
    """Invariant B — every value the click-processor can ever emit for
    device_type is a legal admin-enum value, AND the admin enum carries
    nothing dead: {dropdown values} == {normalizer codomain} EXACTLY (G2).
    A future accidental re-widening of the admin enum (or a narrowing of
    the codomain) fails this immediately instead of shipping an
    unreachable dropdown option."""
    assert _UA_PARSER_DEVICE_TYPE_CODOMAIN == _ADMIN_DEVICE_TYPE_ENUM


# ---- GTD-R135 Phase 0 — Invariant C: both matchers' dim-sets in lockstep ---
# Per Unknown 1's resolution (cascade-only via schema-type-gating): the
# legacy offer-target matcher's inline `click_attrs` dict (`router.py`
# `resolve_target_with_id`, ~line 2021-2033) must stay FROZEN at exactly the
# base 10 dims — new structural/identifier dims are gated to FLOW-only
# criteria (`FLOW_CRITERION_TYPES`) at the admin-api schema layer, so an
# offer_target criterion using one of them is rejected at write time and the
# legacy matcher never needs to evaluate it. Cascade's evaluated-dims set
# (`KNOWN_EVALUATED_DIMS`) is a SUPERSET — never narrower — because it also
# carries the returning dims (and will carry structural/identifier dims from
# Phase 3/4 onward). This test locks the architecture in as CI, not a
# comment: a future accidental widening of the legacy matcher's dim set (or
# accidental narrowing of cascade's) fails immediately instead of rotting
# silently.


def test_legacy_matcher_dims_frozen_at_base_10():
    """The legacy matcher's inline click_attrs keys = EXACTLY the base 10
    dims (7 static UA/geo keys + the 3 CF-3 extra dims). Built the same way
    `resolve_target_with_id` builds it (static keys ∪ `_extra_click_dims`)
    so a future edit to either side is caught here."""
    static_keys = {"geo", "os", "device_type", "browser", "region", "city", "language"}
    extra_keys = set(_extra_click_dims(_req(arrival_ts=None)))
    legacy_dims = static_keys | extra_keys
    assert legacy_dims == _EVALUATED_BASE_DIMS
    assert len(legacy_dims) == 10


def test_cascade_dims_are_superset_of_legacy_matcher_dims():
    """Cascade's KNOWN_EVALUATED_DIMS must never be a narrower set than the
    legacy matcher's — cascade may only ever GROW relative to it (structural/
    identifier dims land here in later phases, never in the legacy matcher)."""
    static_keys = {"geo", "os", "device_type", "browser", "region", "city", "language"}
    extra_keys = set(_extra_click_dims(_req(arrival_ts=None)))
    legacy_dims = static_keys | extra_keys
    assert legacy_dims <= KNOWN_EVALUATED_DIMS


# ---- GTD-R135 Phase 3/4 — Invariant C extended to structural + identifier ---
# (FIX 3, post-merge adversarial review, 2026-07-14): Phase 0's harness pinned
# the base-10 CF-3 contract + Invariant B (device_type codomain) + Invariant C
# (legacy-vs-cascade dim-set lockstep) BEFORE Phase 3/4 existed. Those two
# phases added STRUCTURAL_CRITERION_DIMS + IDENTIFIER_SLOTS to cascade.py with
# comments claiming they're "pinned by test_criteria_contract.py" — but no
# such pin was ever added here. This closes that gap: a future one-sided edit
# (e.g. a 5th structural dim added to admin-api's parameters.py without
# wiring cascade.py, or the reverse) now fails CI instead of rotting silently
# — exactly the CF-3 failure class Invariant 1 above already guards for the
# base-10 dims.

# Mirror of admin-api `app/common/parameters.py` STRUCTURAL_CRITERION_TYPES.
# Separate service, no shared import — cross-service contract anchor, same
# pattern as ADMIN_ACCEPTED_BASE_DIMS / _ADMIN_DEVICE_TYPE_ENUM above.
_ADMIN_STRUCTURAL_CRITERION_TYPES = frozenset({
    "buyer_id", "team_id", "department_id", "custom_group_id",
})

# Mirror of admin-api `app/common/parameters.py` IDENTIFIER_SLOTS — the
# owner's deliberate 34-slot subset (all 20 sub-slots + 14 named reserved
# slots that pass the cohort-vs-per-click lens), not all 39 canonical slots.
# Filter V2 (2026-07-15): added the 10 cohort slots, removed `source_click_id`
# (per-click id, not a group) — see admin-api `parameters.py`'s
# IDENTIFIER_SLOTS comment for the full rationale + the `buyer_id` security
# note (kept STRUCTURAL-ONLY, never added here).
_ADMIN_IDENTIFIER_SLOTS = frozenset(f"sub{i}" for i in range(1, 21)) | frozenset({
    "creative_id", "ad_campaign_id", "source", "keyword",
    "host", "placement", "adset_id", "ad_id", "pixel_id",
    "funnel_id", "funnel_type", "landing_id", "external_id", "app_id",
})


def test_structural_criterion_dims_in_lockstep_with_admin():
    """Exact equality (not subset) — admin-api's STRUCTURAL_CRITERION_TYPES is
    an MVP-locked set of exactly 4 org-hierarchy dims (ADR-0106); cascade's
    STRUCTURAL_CRITERION_DIMS must mirror it byte-for-byte. A one-sided
    add/remove on either side fails here instead of shipping a dead admin dim
    (CF-3-class) or an unreachable matcher branch."""
    assert STRUCTURAL_CRITERION_DIMS == _ADMIN_STRUCTURAL_CRITERION_TYPES


def test_identifier_slots_in_lockstep_with_admin():
    """Exact equality — admin-api's IDENTIFIER_SLOTS is the owner's
    deliberately-curated subset; cascade's mirror must match exactly so the
    derived `param:`-prefixed evaluated-dims set (built by the identical
    transform on both sides) never silently drifts out of sync."""
    assert IDENTIFIER_SLOTS == _ADMIN_IDENTIFIER_SLOTS


def test_structural_and_identifier_dims_are_evaluated_by_cascade():
    """CF-3 contract, extended to Phase 3/4 (FIX 3) — every structural +
    identifier dim admin-api accepts on a flow MUST be a member of cascade's
    KNOWN_EVALUATED_DIMS, exactly like the base-10 contract (Invariant 1)
    above. A dim accepted at write time but absent from the evaluator's
    known-dims set is the CF-3 fail-open shape: a stored `not_in` criterion
    on it would be a silent no-op for 100% of traffic."""
    admin_dims = _ADMIN_STRUCTURAL_CRITERION_TYPES | {
        f"param:{slot}" for slot in _ADMIN_IDENTIFIER_SLOTS
    }
    missing = admin_dims - KNOWN_EVALUATED_DIMS
    assert not missing, (
        "admin-accepted structural/identifier dim never evaluated by cascade "
        f"(CF-3-class, not_in fail-open): {sorted(missing)}"
    )


# ============================================================
# GTD-R135 Phase 6 — contains / empty / not_empty operator lockstep
# ============================================================
#
# `cascade._first_failing_criterion`'s op dispatch is a procedural
# if/elif chain, not a data structure — so there's no literal frozenset
# to mirror the SHAPE of `test_structural_criterion_dims_in_lockstep_
# with_admin` above. What CAN be pinned: the exact op-token vocabulary
# admin-api defines, and that cascade recognizes (doesn't fail-closed
# on) every one of them.

# Mirror of admin-api `app/common/parameters.py` OPERATORS (the full
# 5-token union: BASE_OPERATORS ∪ IDENTIFIER_ONLY_OPERATORS).
_ADMIN_OPERATORS = frozenset({"in", "not_in", "contains", "empty", "not_empty"})


def test_cascade_recognizes_every_admin_operator_token():
    """A future admin-api op addition without a matching cascade dispatch
    branch would silently fail-closed on 100% of traffic for that op — the
    CF-3 fail-open/fail-closed class, applied to operators instead of
    dims. Proves each token is DISPATCHED (not routed to the final
    `else: return c` catch-all) by constructing a criterion that WOULD
    match if the op were handled, on an identifier dim (the only family
    the 3 new ops are legal on)."""
    for op in _ADMIN_OPERATORS:
        if op == "empty":
            criteria = [{"type": "param:creative_id", "op": op, "values": []}]
            click_attrs = {"param:creative_id": ""}
        elif op == "not_empty":
            criteria = [{"type": "param:creative_id", "op": op, "values": []}]
            click_attrs = {"param:creative_id": "x"}
        elif op == "not_in":
            # not_in matches when click_val is ABSENT from values.
            criteria = [{"type": "param:creative_id", "op": op, "values": ["y"]}]
            click_attrs = {"param:creative_id": "x"}
        else:
            criteria = [{"type": "param:creative_id", "op": op, "values": ["x"]}]
            click_attrs = {"param:creative_id": "x"}
        failing = _first_failing_criterion(criteria, click_attrs)
        assert failing is None, (
            f"op '{op}' did not match a criterion constructed to match it — "
            "either dispatch is missing or it fell through to the unknown-op "
            "fail-closed branch"
        )


def test_cascade_still_fails_closed_on_a_token_outside_the_admin_vocabulary():
    # The inverse of the test above — an op admin-api does NOT define
    # must still drop the flow (the pre-existing unknown-op guard).
    failing = _first_failing_criterion(
        [{"type": "param:creative_id", "op": "regex", "values": ["x"]}],
        {"param:creative_id": "x"},
    )
    assert failing is not None
