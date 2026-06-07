"""Unit tests for `app/cascade.py` — Stage 2 / Vectors 2.4 + 2.5.

Pin every step of `docs/design/SCOPE-CASCADE.md` against a literal
flow-dict harness. Redis is mocked via `AsyncMock`, so these tests
run in microseconds and never depend on a real broker.

Test plan (mirrors the design doc's test plan + edge cases):
  1. Specificity — most specific scope wins.
  2. Campaign-bound vs global tie — campaign-bound wins.
  3. seq_id tie-break — lower wins.
  4. is_default — always last in its scope.
  5. Criteria mismatch — flow excluded.
  6. Empty criteria — match-all.
  7. Fallback chain — walks OUT one level on no match.
  8. No match anywhere — returns None.
  9. No buyer context — only company-level flows considered.
 10. Malformed criteria JSON — flow skipped (not match-all).
 11. Pure helpers — `_criteria_match`, `_winner_sort_key`, `_safe_int`.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.cascade import (
    SCOPE_PRIORITY,
    _criteria_match,
    _filter_by_availability,
    _filter_by_criteria,
    _pick_winner,
    _referenced_target_ids,
    _safe_int,
    _winner_sort_key,
    resolve_flow,
)


# ============================================================
# Fixture builders
# ============================================================


def _make_flow(
    *,
    fid: str,
    scope_type: str = "company",
    scope_id: int = 1,
    campaign_id: str = "0",
    seq_id: int = 1,
    is_default: bool = False,
    criteria: list | None = None,
    action_type: str = "redirect",
) -> dict:
    """Build a flow HASH the way the sync builder emits it."""
    return {
        "_id": fid,
        "scope_type": scope_type,
        "scope_id": str(scope_id),
        "campaign_id": campaign_id,
        "seq_id": str(seq_id),
        "is_default": "1" if is_default else "0",
        "criteria": json.dumps(criteria if criteria is not None else []),
        "action_type": action_type,
        "action_config": "{}",
        "name": f"flow-{fid}",
    }


def _redis_with_lists_and_hashes(
    lists: dict[str, list[str]],
    hashes: dict[str, dict],
) -> MagicMock:
    """Mock Redis with pipeline support for lrange + hgetall.

    The pipeline mock collects ops then `execute()` returns results
    matching the call order — same contract `resolve_flow` relies on.
    """

    class FakePipeline:
        def __init__(self):
            self._ops: list[tuple[str, str]] = []

        def lrange(self, key, _start, _end):
            self._ops.append(("lrange", key, None))

        def hgetall(self, key):
            self._ops.append(("hgetall", key, None))

        def hget(self, key, field):
            # v2 Phase A — availability pre-selection HGETs
            # `offer_target:{tid}` `availability`.
            self._ops.append(("hget", key, field))

        def exists(self, key):
            # Dead-offer fix (2026-06-07) — the availability loader EXISTS-checks
            # `offer_target:{tid}` to tell a desynced/evicted HASH (absent →
            # unavailable) from a present-but-no-`availability`-field one (active).
            # A HASH is "present" iff it's in the `hashes` map.
            self._ops.append(("exists", key, None))

        async def execute(self):
            out = []
            for op, key, field in self._ops:
                if op == "lrange":
                    out.append(list(lists.get(key, [])))
                elif op == "hgetall":
                    out.append(dict(hashes.get(key, {})))
                elif op == "hget":
                    out.append(hashes.get(key, {}).get(field))
                elif op == "exists":
                    out.append(1 if key in hashes else 0)
            return out

    redis = MagicMock()
    redis.pipeline = lambda: FakePipeline()
    return redis


# ============================================================
# Step 3 — Specificity (most specific scope wins)
# ============================================================


class TestSpecificity:
    @pytest.mark.asyncio
    async def test_buyer_beats_team(self):
        """Buyer-scoped flow beats team-scoped flow when both match."""
        flows = {
            "flow:10": _make_flow(fid="10", scope_type="buyer", scope_id=5, seq_id=1),
            "flow:20": _make_flow(fid="20", scope_type="team", scope_id=3, seq_id=2),
        }
        lists = {
            "campaign:1:flows": [],
            "flows:scope:1:buyer:5": ["10"],
            "flows:scope:1:team:3": ["20"],
            "flows:scope:1:company:1": [],
        }
        r = _redis_with_lists_and_hashes(lists, flows)

        winner = await resolve_flow(
            r, campaign_id="1", company_id=1, buyer_id=5,
            team_id=3, department_id=None, custom_group_id=None,
            click_attrs={"geo": "US", "os": "ios", "device_type": "mobile"},
        )
        assert winner is not None
        assert winner["_id"] == "10"

    @pytest.mark.asyncio
    async def test_custom_group_beats_team(self):
        flows = {
            "flow:30": _make_flow(fid="30", scope_type="custom_group", scope_id=10, seq_id=1),
            "flow:40": _make_flow(fid="40", scope_type="team", scope_id=3, seq_id=2),
        }
        lists = {
            "campaign:1:flows": [],
            "flows:scope:1:custom_group:10": ["30"],
            "flows:scope:1:team:3": ["40"],
        }
        r = _redis_with_lists_and_hashes(lists, flows)
        winner = await resolve_flow(
            r, campaign_id="1", company_id=1, buyer_id=None,
            team_id=3, department_id=None, custom_group_id=10,
            click_attrs={"geo": "US", "os": "ios", "device_type": "mobile"},
        )
        assert winner["_id"] == "30"

    @pytest.mark.asyncio
    async def test_team_beats_department(self):
        flows = {
            "flow:50": _make_flow(fid="50", scope_type="team", scope_id=3, seq_id=1),
            "flow:60": _make_flow(fid="60", scope_type="department", scope_id=2, seq_id=1),
        }
        lists = {
            "campaign:1:flows": [],
            "flows:scope:1:team:3": ["50"],
            "flows:scope:1:department:2": ["60"],
        }
        r = _redis_with_lists_and_hashes(lists, flows)
        winner = await resolve_flow(
            r, campaign_id="1", company_id=1, buyer_id=None,
            team_id=3, department_id=2, custom_group_id=None,
            click_attrs={"geo": "US", "os": "ios", "device_type": "mobile"},
        )
        assert winner["_id"] == "50"

    @pytest.mark.asyncio
    async def test_department_beats_company(self):
        flows = {
            "flow:70": _make_flow(fid="70", scope_type="department", scope_id=2, seq_id=1),
            "flow:80": _make_flow(fid="80", scope_type="company", scope_id=1, seq_id=1),
        }
        lists = {
            "campaign:1:flows": [],
            "flows:scope:1:department:2": ["70"],
            "flows:scope:1:company:1": ["80"],
        }
        r = _redis_with_lists_and_hashes(lists, flows)
        winner = await resolve_flow(
            r, campaign_id="1", company_id=1, buyer_id=None,
            team_id=None, department_id=2, custom_group_id=None,
            click_attrs={"geo": "US", "os": "ios", "device_type": "mobile"},
        )
        assert winner["_id"] == "70"


# ============================================================
# Step 4 — Tie-break within same scope
# ============================================================


class TestTieBreak:
    @pytest.mark.asyncio
    async def test_campaign_bound_beats_global_at_same_scope(self):
        """When 2 flows share scope, the campaign-bound one wins."""
        bound = _make_flow(
            fid="100", scope_type="company", scope_id=1, seq_id=5, campaign_id="1",
        )
        global_flow = _make_flow(
            fid="200", scope_type="company", scope_id=1, seq_id=2, campaign_id="0",
        )
        flows = {"flow:100": bound, "flow:200": global_flow}
        lists = {
            "campaign:1:flows": ["100"],
            "flows:scope:1:company:1": ["200"],
        }
        r = _redis_with_lists_and_hashes(lists, flows)

        winner = await resolve_flow(
            r, campaign_id="1", company_id=1, buyer_id=None,
            team_id=None, department_id=None, custom_group_id=None,
            click_attrs={"geo": "US", "os": "ios", "device_type": "mobile"},
        )
        # Campaign-bound wins despite higher seq_id.
        assert winner["_id"] == "100"

    @pytest.mark.asyncio
    async def test_lower_seq_id_wins_when_bound_ness_equal(self):
        a = _make_flow(fid="11", scope_type="company", scope_id=1, seq_id=1, campaign_id="1")
        b = _make_flow(fid="22", scope_type="company", scope_id=1, seq_id=5, campaign_id="1")
        flows = {"flow:11": a, "flow:22": b}
        lists = {"campaign:1:flows": ["11", "22"]}
        r = _redis_with_lists_and_hashes(lists, flows)
        winner = await resolve_flow(
            r, campaign_id="1", company_id=1, buyer_id=None,
            team_id=None, department_id=None, custom_group_id=None,
            click_attrs={"geo": "US", "os": "ios", "device_type": "mobile"},
        )
        assert winner["_id"] == "11"

    @pytest.mark.asyncio
    async def test_is_default_loses_to_explicit_at_same_scope(self):
        explicit = _make_flow(
            fid="33", scope_type="company", scope_id=1, seq_id=10,
            campaign_id="1", is_default=False,
        )
        default = _make_flow(
            fid="44", scope_type="company", scope_id=1, seq_id=1,
            campaign_id="1", is_default=True,
        )
        flows = {"flow:33": explicit, "flow:44": default}
        lists = {"campaign:1:flows": ["33", "44"]}
        r = _redis_with_lists_and_hashes(lists, flows)
        winner = await resolve_flow(
            r, campaign_id="1", company_id=1, buyer_id=None,
            team_id=None, department_id=None, custom_group_id=None,
            click_attrs={"geo": "US", "os": "ios", "device_type": "mobile"},
        )
        # Explicit wins despite higher seq_id — defaults LAST.
        assert winner["_id"] == "33"

    @pytest.mark.asyncio
    async def test_tie_break_combined_default_global_seq(self):
        """All three tie-break rules in one bucket."""
        flows_list = [
            _make_flow(fid="A", campaign_id="0", is_default=True, seq_id=1),
            _make_flow(fid="B", campaign_id="1", is_default=True, seq_id=1),
            _make_flow(fid="C", campaign_id="0", is_default=False, seq_id=10),
            _make_flow(fid="D", campaign_id="1", is_default=False, seq_id=5),
            _make_flow(fid="E", campaign_id="1", is_default=False, seq_id=2),
        ]
        sorted_ids = [f["_id"] for f in sorted(flows_list, key=_winner_sort_key)]
        # Expected order: explicit-bound by seq_id ASC, then explicit-global,
        # then default-bound, then default-global.
        assert sorted_ids == ["E", "D", "C", "B", "A"]


# ============================================================
# Step 2 — Criteria match
# ============================================================


class TestCriteriaMatch:
    def test_empty_criteria_matches_all(self):
        flows = [_make_flow(fid="1", criteria=[])]
        survivors = _filter_by_criteria(
            flows, {"geo": "US", "os": "ios", "device_type": "mobile"},
        )
        assert len(survivors) == 1

    def test_geo_in_match(self):
        flows = [_make_flow(fid="1", criteria=[
            {"type": "geo", "op": "in", "values": ["US", "CA"]},
        ])]
        ok = _filter_by_criteria(flows, {"geo": "US", "os": "ios", "device_type": "mobile"})
        assert len(ok) == 1

    def test_geo_in_no_match(self):
        flows = [_make_flow(fid="1", criteria=[
            {"type": "geo", "op": "in", "values": ["US", "CA"]},
        ])]
        ok = _filter_by_criteria(flows, {"geo": "PL", "os": "ios", "device_type": "mobile"})
        assert len(ok) == 0

    def test_geo_not_in_match(self):
        flows = [_make_flow(fid="1", criteria=[
            {"type": "geo", "op": "not_in", "values": ["RU", "CN"]},
        ])]
        ok = _filter_by_criteria(flows, {"geo": "US", "os": "ios", "device_type": "mobile"})
        assert len(ok) == 1

    def test_combined_criteria_all_must_match(self):
        flows = [_make_flow(fid="1", criteria=[
            {"type": "geo", "op": "in", "values": ["US"]},
            {"type": "os", "op": "in", "values": ["ios"]},
            {"type": "device_type", "op": "in", "values": ["mobile"]},
        ])]
        ok = _filter_by_criteria(flows, {"geo": "US", "os": "ios", "device_type": "mobile"})
        assert len(ok) == 1
        # Mobile but Android — fails os.
        no = _filter_by_criteria(flows, {"geo": "US", "os": "android", "device_type": "mobile"})
        assert len(no) == 0

    def test_unknown_operator_fails_safe(self):
        flows = [_make_flow(fid="1", criteria=[
            {"type": "geo", "op": "regex", "values": ["US"]},
        ])]
        # Unknown op — never matches.
        no = _filter_by_criteria(flows, {"geo": "US", "os": "ios", "device_type": "mobile"})
        assert len(no) == 0

    def test_malformed_criteria_skipped(self):
        bad = _make_flow(fid="1")
        bad["criteria"] = "{not json"
        good = _make_flow(fid="2", criteria=[])
        survivors = _filter_by_criteria(
            [bad, good], {"geo": "US", "os": "ios", "device_type": "mobile"},
        )
        assert [f["_id"] for f in survivors] == ["2"]

    def test_non_dict_criterion_excludes_flow(self):
        flow = _make_flow(fid="1")
        flow["criteria"] = json.dumps(["not-a-dict"])
        survivors = _filter_by_criteria(
            [flow], {"geo": "US", "os": "ios", "device_type": "mobile"},
        )
        assert survivors == []

    def test_geo_case_handling(self):
        """`_criteria_match` treats geo as ALREADY upper — caller normalizes."""
        # Caller normalizes click_attrs.geo; values come from admin-api as-is.
        assert _criteria_match(
            [{"type": "geo", "op": "in", "values": ["US"]}],
            {"geo": "US", "os": "ios", "device_type": "mobile"},
        ) is True
        # Lowercase click_attr will NOT match — proves caller normalization
        # contract is honored.
        assert _criteria_match(
            [{"type": "geo", "op": "in", "values": ["US"]}],
            {"geo": "us", "os": "ios", "device_type": "mobile"},
        ) is False


# ============================================================
# F.17 — Per-type case strategy + new criterion dimensions
# ============================================================
#
# These tests pin the cascade matcher's vocabulary contract for the
# 4 dimensions added in F.17 (browser, region, city, language). The
# matcher MUST mirror admin-api's storage casing — drift here breaks
# every saved criterion silently.


class TestCasePreserveDims:
    """Per-type casing strategy — see `cascade._CASE_PRESERVE`.

    `geo` / `region` / `browser` / `language` keep their value
    verbatim (admin-api validates them in the same casing the
    parser/CF emits). `os` / `device_type` / `city` lowercase both
    sides.
    """

    def test_browser_title_case_match(self):
        # device_detector emits "Samsung Browser" — admin-api stores
        # the same string; matcher MUST NOT lowercase the criterion.
        assert _criteria_match(
            [{"type": "browser", "op": "in", "values": ["Chrome", "Samsung Browser"]}],
            {"browser": "Samsung Browser"},
        ) is True

    def test_browser_lowercase_click_misses_title_case_criterion(self):
        # Stale lowercase emission would silently miss — pin the
        # contract that prevents accidental .lower() reintroduction.
        assert _criteria_match(
            [{"type": "browser", "op": "in", "values": ["Chrome"]}],
            {"browser": "chrome"},
        ) is False

    def test_region_human_name_match(self):
        # CF emits "California" verbatim from MaxMind; admin-api
        # stores the same.
        assert _criteria_match(
            [{"type": "region", "op": "in", "values": ["California", "Texas"]}],
            {"region": "California"},
        ) is True

    def test_region_unicode_match(self):
        assert _criteria_match(
            [{"type": "region", "op": "in", "values": ["Київська область"]}],
            {"region": "Київська область"},
        ) is True

    def test_language_bcp47_strict_casing(self):
        # `en-US` matches; `en-us` would be a save-time validator
        # rejection at admin-api so we don't test the lowercase path
        # here (it can't be stored).
        assert _criteria_match(
            [{"type": "language", "op": "in", "values": ["en-US", "uk-UA"]}],
            {"language": "uk-UA"},
        ) is True

    def test_language_short_form_match(self):
        # Operator may save just `"en"` — matcher should accept the
        # exact string, not "en-US" / "en-GB" prefix-extend.
        assert _criteria_match(
            [{"type": "language", "op": "in", "values": ["en"]}],
            {"language": "en"},
        ) is True
        assert _criteria_match(
            [{"type": "language", "op": "in", "values": ["en"]}],
            {"language": "en-US"},
        ) is False

    def test_city_lowercases_both_sides(self):
        # `city` is NOT in CASE_PRESERVE — operator-saved "London" is
        # lowercased in the matcher, click_attrs.city is also already
        # lowercased upstream by router. Either way, "london" wins.
        assert _criteria_match(
            [{"type": "city", "op": "in", "values": ["London", "Paris"]}],
            {"city": "london"},
        ) is True

    def test_empty_click_attr_misses_in_criterion(self):
        # CF didn't emit a region for this click — `op=in` fails closed.
        assert _criteria_match(
            [{"type": "region", "op": "in", "values": ["California"]}],
            {"region": ""},
        ) is False

    def test_empty_click_attr_passes_not_in_criterion(self):
        # `op=not_in` is permissive on missing data — that's the
        # "exclude these regions" semantic. Empty region passes
        # because empty is in nothing.
        assert _criteria_match(
            [{"type": "region", "op": "not_in", "values": ["California"]}],
            {"region": ""},
        ) is True


# ============================================================
# Step 5 — Fallback chain
# ============================================================


class TestFallbackChain:
    @pytest.mark.asyncio
    async def test_fallback_walks_buyer_to_team(self):
        """Buyer-level flow doesn't match → team-level still wins."""
        buyer_flow = _make_flow(
            fid="10", scope_type="buyer", scope_id=5, seq_id=1,
            criteria=[{"type": "geo", "op": "in", "values": ["RU"]}],  # excludes US
        )
        team_flow = _make_flow(
            fid="20", scope_type="team", scope_id=3, seq_id=2,
            criteria=[],  # match-all
        )
        flows = {"flow:10": buyer_flow, "flow:20": team_flow}
        lists = {
            "campaign:1:flows": [],
            "flows:scope:1:buyer:5": ["10"],
            "flows:scope:1:team:3": ["20"],
        }
        r = _redis_with_lists_and_hashes(lists, flows)
        winner = await resolve_flow(
            r, campaign_id="1", company_id=1, buyer_id=5,
            team_id=3, department_id=None, custom_group_id=None,
            click_attrs={"geo": "US", "os": "ios", "device_type": "mobile"},
        )
        # Buyer flow filtered out by criteria — falls back to team.
        assert winner["_id"] == "20"

    @pytest.mark.asyncio
    async def test_fallback_to_company_when_others_miss(self):
        team_flow = _make_flow(
            fid="20", scope_type="team", scope_id=3, seq_id=1,
            criteria=[{"type": "geo", "op": "in", "values": ["RU"]}],
        )
        company_flow = _make_flow(
            fid="30", scope_type="company", scope_id=1, seq_id=1,
            criteria=[],
        )
        flows = {"flow:20": team_flow, "flow:30": company_flow}
        lists = {
            "campaign:1:flows": [],
            "flows:scope:1:team:3": ["20"],
            "flows:scope:1:company:1": ["30"],
        }
        r = _redis_with_lists_and_hashes(lists, flows)
        winner = await resolve_flow(
            r, campaign_id="1", company_id=1, buyer_id=None,
            team_id=3, department_id=None, custom_group_id=None,
            click_attrs={"geo": "US", "os": "ios", "device_type": "mobile"},
        )
        assert winner["_id"] == "30"

    @pytest.mark.asyncio
    async def test_no_match_anywhere_returns_none(self):
        flow = _make_flow(
            fid="10", scope_type="company", scope_id=1, seq_id=1,
            criteria=[{"type": "geo", "op": "in", "values": ["RU"]}],
        )
        flows = {"flow:10": flow}
        lists = {"campaign:1:flows": [], "flows:scope:1:company:1": ["10"]}
        r = _redis_with_lists_and_hashes(lists, flows)
        winner = await resolve_flow(
            r, campaign_id="1", company_id=1, buyer_id=None,
            team_id=None, department_id=None, custom_group_id=None,
            click_attrs={"geo": "US", "os": "ios", "device_type": "mobile"},
        )
        assert winner is None


# ============================================================
# Edge cases — no candidates, no buyer context, etc.
# ============================================================


class TestEdgeCases:
    @pytest.mark.asyncio
    async def test_no_candidates_returns_none(self):
        r = _redis_with_lists_and_hashes({}, {})
        winner = await resolve_flow(
            r, campaign_id="1", company_id=1, buyer_id=5,
            team_id=3, department_id=2, custom_group_id=10,
            click_attrs={"geo": "US", "os": "ios", "device_type": "mobile"},
        )
        assert winner is None

    @pytest.mark.asyncio
    async def test_no_buyer_context_only_company(self):
        """Click without buyer chain → only company-level flows considered."""
        company_flow = _make_flow(
            fid="10", scope_type="company", scope_id=1, seq_id=1,
            criteria=[],
        )
        team_flow = _make_flow(
            fid="20", scope_type="team", scope_id=3, seq_id=1,
            criteria=[],
        )
        flows = {"flow:10": company_flow, "flow:20": team_flow}
        lists = {
            "campaign:1:flows": [],
            "flows:scope:1:company:1": ["10"],
            "flows:scope:1:team:3": ["20"],  # would never be fetched without team_id
        }
        r = _redis_with_lists_and_hashes(lists, flows)
        winner = await resolve_flow(
            r, campaign_id="1", company_id=1, buyer_id=None,
            team_id=None, department_id=None, custom_group_id=None,
            click_attrs={"geo": "US", "os": "ios", "device_type": "mobile"},
        )
        assert winner["_id"] == "10"

    @pytest.mark.asyncio
    async def test_no_company_id_means_campaign_only(self):
        """Without company_id we can't address scope keyspace."""
        bound = _make_flow(
            fid="100", campaign_id="1", scope_type="company", scope_id=1,
        )
        flows = {"flow:100": bound}
        lists = {"campaign:1:flows": ["100"]}
        r = _redis_with_lists_and_hashes(lists, flows)
        winner = await resolve_flow(
            r, campaign_id="1", company_id=None, buyer_id=None,
            team_id=None, department_id=None, custom_group_id=None,
            click_attrs={"geo": "US", "os": "ios", "device_type": "mobile"},
        )
        # campaign-bound flow's scope is `company:1` — but click_levels has
        # company=None → no level matches → returns None. This is the
        # documented behavior: without a tenant, no scope evaluation.
        assert winner is None

    @pytest.mark.asyncio
    async def test_dedupe_repeated_flow_id(self):
        """Same flow ID in two lists shouldn't double-load."""
        flow = _make_flow(fid="10", scope_type="company", scope_id=1, seq_id=1)
        flows = {"flow:10": flow}
        lists = {
            "campaign:1:flows": ["10"],
            "flows:scope:1:company:1": ["10"],
        }
        r = _redis_with_lists_and_hashes(lists, flows)
        winner = await resolve_flow(
            r, campaign_id="1", company_id=1, buyer_id=None,
            team_id=None, department_id=None, custom_group_id=None,
            click_attrs={"geo": "US", "os": "ios", "device_type": "mobile"},
        )
        assert winner["_id"] == "10"

    @pytest.mark.asyncio
    async def test_missing_flow_hash_skipped(self):
        """Sync drift: scope list has ID but hash absent → skip."""
        flows = {}  # Hash missing
        lists = {
            "campaign:1:flows": [],
            "flows:scope:1:company:1": ["999"],
        }
        r = _redis_with_lists_and_hashes(lists, flows)
        winner = await resolve_flow(
            r, campaign_id="1", company_id=1, buyer_id=None,
            team_id=None, department_id=None, custom_group_id=None,
            click_attrs={"geo": "US", "os": "ios", "device_type": "mobile"},
        )
        assert winner is None


# ============================================================
# Pure helpers
# ============================================================


class TestPureHelpers:
    def test_safe_int_normal(self):
        assert _safe_int("42") == 42
        assert _safe_int(42) == 42

    def test_safe_int_handles_bad_input(self):
        assert _safe_int(None) == 0
        assert _safe_int("abc") == 0
        assert _safe_int(None, default=99) == 99

    def test_winner_sort_key_explicit_bound_lowest(self):
        """Explicit campaign-bound flow with seq=1 is the smallest key."""
        flow = _make_flow(fid="A", campaign_id="1", is_default=False, seq_id=1)
        assert _winner_sort_key(flow) == (0, 0, 1)

    def test_winner_sort_key_default_global_largest(self):
        flow = _make_flow(fid="A", campaign_id="0", is_default=True, seq_id=99)
        assert _winner_sort_key(flow) == (1, 1, 99)

    def test_scope_priority_constant(self):
        assert SCOPE_PRIORITY == (
            "buyer", "custom_group", "team", "department", "company",
        )


# ============================================================
# Audit 2026-06-03 regression fence — criteria + multi-scope gaps
# ============================================================
#
# Pins decision-engine behaviours flagged as under-covered by the
# routing-reliability audit (Agent-C GAP3/4/5/6). Mutation-checked —
# see P2-results.md.


class TestAuditCriteriaGaps:
    """Agent-C GAP3/5/6 — per-dim criteria coverage that was thin."""

    def test_device_tablet_not_in_excludes(self):
        # GAP5: device_type=tablet against not_in[mobile, tablet] →
        # tablet IS in the list → criterion fails (flow excluded).
        assert _criteria_match(
            [{"type": "device_type", "op": "not_in", "values": ["mobile", "tablet"]}],
            {"device_type": "tablet"},
        ) is False

    def test_device_desktop_passes_not_in_mobile_tablet(self):
        # Complement: desktop is NOT excluded.
        assert _criteria_match(
            [{"type": "device_type", "op": "not_in", "values": ["mobile", "tablet"]}],
            {"device_type": "desktop"},
        ) is True

    def test_browser_not_in_with_empty_click_value_passes(self):
        # GAP3: device_detector couldn't parse a browser (empty) →
        # not_in is permissive (empty is in nothing) → passes. Same
        # fail-open-on-missing semantics as the region case, pinned
        # explicitly for the browser dimension.
        assert _criteria_match(
            [{"type": "browser", "op": "not_in", "values": ["Chrome", "Firefox"]}],
            {"browser": ""},
        ) is True

    def test_browser_not_in_excludes_listed(self):
        assert _criteria_match(
            [{"type": "browser", "op": "not_in", "values": ["Chrome"]}],
            {"browser": "Chrome"},
        ) is False

    def test_region_and_city_compound_and_match(self):
        # GAP6: a compound region AND city criterion. Both must hold.
        crit = [
            {"type": "region", "op": "in", "values": ["California"]},
            {"type": "city", "op": "in", "values": ["Los Angeles"]},
        ]
        # city is NOT case-preserving — matcher lowercases both sides,
        # router lowercases click city upstream.
        assert _criteria_match(crit, {"region": "California", "city": "los angeles"}) is True

    def test_region_and_city_compound_and_city_miss(self):
        crit = [
            {"type": "region", "op": "in", "values": ["California"]},
            {"type": "city", "op": "in", "values": ["Los Angeles"]},
        ]
        # Right region, wrong city → the AND fails.
        assert _criteria_match(crit, {"region": "California", "city": "san diego"}) is False


class TestAuditMultiScopeFallback:
    """Agent-C GAP4 — every scope level has a flow but ALL fail criteria.

    The single-scope no-match case is covered by
    `TestFallbackChain.test_no_match_anywhere_returns_none`. This pins
    the harder case: flows exist at buyer AND team AND company, the
    cascade walks every level, none survives criteria → `None` (the
    legacy split fallback contract), not a stale/last-considered flow.
    """

    @pytest.mark.asyncio
    async def test_all_scopes_present_but_all_fail_criteria_returns_none(self):
        ru_only = [{"type": "geo", "op": "in", "values": ["RU"]}]
        flows = {
            "flow:10": _make_flow(fid="10", scope_type="buyer", scope_id=5,
                                  seq_id=1, criteria=ru_only),
            "flow:20": _make_flow(fid="20", scope_type="team", scope_id=3,
                                  seq_id=1, criteria=ru_only),
            "flow:30": _make_flow(fid="30", scope_type="company", scope_id=1,
                                  seq_id=1, criteria=ru_only),
        }
        lists = {
            "campaign:1:flows": [],
            "flows:scope:1:buyer:5": ["10"],
            "flows:scope:1:team:3": ["20"],
            "flows:scope:1:company:1": ["30"],
        }
        r = _redis_with_lists_and_hashes(lists, flows)
        winner = await resolve_flow(
            r, campaign_id="1", company_id=1, buyer_id=5,
            team_id=3, department_id=None, custom_group_id=None,
            click_attrs={"geo": "US", "os": "ios", "device_type": "mobile"},
        )
        # Click is US — every scope's RU-only flow is filtered out.
        assert winner is None


# ============================================================
# v2 Phase A — availability pre-selection floor (NO-DEAD-END)
# ============================================================


def _offer_flow(*, fid, scope_type="company", scope_id=1, seq_id=1, target_id):
    """An `offer`-action flow pinning a single offer_target."""
    return {
        "_id": fid,
        "scope_type": scope_type,
        "scope_id": str(scope_id),
        "campaign_id": "0",
        "seq_id": str(seq_id),
        "is_default": "0",
        "criteria": "[]",
        "action_type": "offer",
        "action_config": json.dumps({"offer_id": 1, "target_id": target_id}),
        "name": f"flow-{fid}",
    }


class TestReferencedTargetIds:
    def test_offer_pins_target(self):
        f = _offer_flow(fid="1", target_id=7)
        assert _referenced_target_ids(f) == ["7"]

    def test_split_pins_all_targets(self):
        f = {
            "action_type": "split",
            "action_config": json.dumps(
                {"offers": [{"offer_id": 1, "target_id": 7, "weight": 50},
                            {"offer_id": 2, "target_id": 9, "weight": 50}]}
            ),
        }
        assert _referenced_target_ids(f) == ["7", "9"]

    def test_redirect_has_no_pinned_target(self):
        assert _referenced_target_ids(_make_flow(fid="1", action_type="redirect")) == []

    def test_offer_without_pinned_target_is_empty(self):
        f = {"action_type": "offer", "action_config": json.dumps({"offer_id": 1})}
        assert _referenced_target_ids(f) == []


class TestFilterByAvailabilityPure:
    def test_empty_map_excludes_nothing(self):
        flows = [_offer_flow(fid="1", target_id=7)]
        assert _filter_by_availability(flows, {}, returning_visitor=False) == flows

    def test_new_visitor_active_kept(self):
        flows = [_offer_flow(fid="1", target_id=7)]
        out = _filter_by_availability(flows, {"7": "active"}, returning_visitor=False)
        assert [f["_id"] for f in out] == ["1"]

    def test_new_visitor_draining_excluded(self):
        flows = [_offer_flow(fid="1", target_id=7)]
        out = _filter_by_availability(flows, {"7": "draining"}, returning_visitor=False)
        assert out == []

    def test_returning_visitor_draining_kept(self):
        flows = [_offer_flow(fid="1", target_id=7)]
        out = _filter_by_availability(flows, {"7": "draining"}, returning_visitor=True)
        assert [f["_id"] for f in out] == ["1"]

    def test_closed_excluded_for_all_classes(self):
        flows = [_offer_flow(fid="1", target_id=7)]
        assert _filter_by_availability(flows, {"7": "closed"}, returning_visitor=False) == []
        assert _filter_by_availability(flows, {"7": "closed"}, returning_visitor=True) == []

    def test_split_kept_if_any_target_available(self):
        f = {
            "_id": "1", "action_type": "split",
            "action_config": json.dumps(
                {"offers": [{"offer_id": 1, "target_id": 7, "weight": 50},
                            {"offer_id": 2, "target_id": 9, "weight": 50}]}
            ),
        }
        # 7 closed but 9 active → kept for a new visitor.
        out = _filter_by_availability([f], {"7": "closed", "9": "active"},
                                      returning_visitor=False)
        assert [x["_id"] for x in out] == ["1"]

    def test_redirect_never_floored(self):
        flows = [_make_flow(fid="1", action_type="redirect")]
        # Even with a non-empty map, a no-pinned-target flow is kept.
        out = _filter_by_availability(flows, {"99": "closed"}, returning_visitor=False)
        assert [f["_id"] for f in out] == ["1"]


class TestAvailabilityCascade:
    """End-to-end resolve_flow with the availability floor."""

    @pytest.mark.asyncio
    async def test_all_active_byte_identical_winner(self):
        # Two offer flows: buyer (active) beats company (active) — same as
        # without availability (byte-identical when nothing drained/closed).
        flows = {
            "flow:10": _offer_flow(fid="10", scope_type="buyer", scope_id=5, target_id=7),
            "flow:20": _offer_flow(fid="20", scope_type="company", scope_id=1, target_id=9),
        }
        hashes = {
            **flows,
            "offer_target:7": {"availability": "active"},
            "offer_target:9": {"availability": "active"},
        }
        lists = {
            "campaign:1:flows": [],
            "flows:scope:1:buyer:5": ["10"],
            "flows:scope:1:company:1": ["20"],
        }
        r = _redis_with_lists_and_hashes(lists, hashes)
        winner = await resolve_flow(
            r, campaign_id="1", company_id=1, buyer_id=5, team_id=None,
            department_id=None, custom_group_id=None,
            click_attrs={"geo": "US", "os": "ios", "device_type": "mobile"},
        )
        assert winner["_id"] == "10"

    @pytest.mark.asyncio
    async def test_closed_buyer_flow_falls_through_to_company(self):
        # Buyer flow's only target is CLOSED → excluded → cascade falls through
        # to the company flow (NO-DEAD-END within the cascade).
        flows = {
            "flow:10": _offer_flow(fid="10", scope_type="buyer", scope_id=5, target_id=7),
            "flow:20": _offer_flow(fid="20", scope_type="company", scope_id=1, target_id=9),
        }
        hashes = {
            **flows,
            "offer_target:7": {"availability": "closed"},
            "offer_target:9": {"availability": "active"},
        }
        lists = {
            "campaign:1:flows": [],
            "flows:scope:1:buyer:5": ["10"],
            "flows:scope:1:company:1": ["20"],
        }
        r = _redis_with_lists_and_hashes(lists, hashes)
        winner = await resolve_flow(
            r, campaign_id="1", company_id=1, buyer_id=5, team_id=None,
            department_id=None, custom_group_id=None,
            click_attrs={"geo": "US", "os": "ios", "device_type": "mobile"},
        )
        assert winner["_id"] == "20"

    @pytest.mark.asyncio
    async def test_missing_target_hash_floors_dead_flow_and_repicks(self):
        # Dead-offer fix (2026-06-07) — buyer flow pins target 7 whose HASH is
        # ABSENT (its offer was PAUSED → desynced; R1-DEAD-1). Pre-fix the floor
        # read None→'active', the dead flow WON and the click poached a foreign
        # campaign. Now the absent HASH ⇒ 'missing' (∉ allowed) ⇒ the buyer flow
        # is floored ⇒ the cascade re-picks the servable company flow (target 9
        # present + active). Scope-uniform: the dead flow is the MORE specific one.
        flows = {
            "flow:10": _offer_flow(fid="10", scope_type="buyer", scope_id=5, target_id=7),
            "flow:20": _offer_flow(fid="20", scope_type="company", scope_id=1, target_id=9),
        }
        hashes = {
            **flows,
            # offer_target:7 intentionally ABSENT — paused-offer desync.
            "offer_target:9": {"availability": "active"},
        }
        lists = {
            "campaign:1:flows": [],
            "flows:scope:1:buyer:5": ["10"],
            "flows:scope:1:company:1": ["20"],
        }
        r = _redis_with_lists_and_hashes(lists, hashes)
        winner = await resolve_flow(
            r, campaign_id="1", company_id=1, buyer_id=5, team_id=None,
            department_id=None, custom_group_id=None,
            click_attrs={"geo": "US", "os": "ios", "device_type": "mobile"},
        )
        assert winner["_id"] == "20"  # sibling re-picked, NOT the dead buyer flow

    @pytest.mark.asyncio
    async def test_all_targets_missing_returns_none(self):
        # Every pinned target's HASH is absent (desynced) → all flows floored →
        # no eligible flow → None. The caller then serves the campaign's OWN
        # terminal_fallback (not a foreign campaign — CF-OBS-1 backstop).
        flows = {
            "flow:10": _offer_flow(fid="10", scope_type="company", scope_id=1, target_id=7),
        }
        hashes = {**flows}  # offer_target:7 ABSENT
        lists = {"campaign:1:flows": [], "flows:scope:1:company:1": ["10"]}
        r = _redis_with_lists_and_hashes(lists, hashes)
        winner = await resolve_flow(
            r, campaign_id="1", company_id=1, buyer_id=None, team_id=None,
            department_id=None, custom_group_id=None,
            click_attrs={"geo": "US", "os": "ios", "device_type": "mobile"},
        )
        assert winner is None

    @pytest.mark.asyncio
    async def test_draining_blocks_new_serves_returning(self):
        flows = {"flow:10": _offer_flow(fid="10", scope_type="company", scope_id=1, target_id=7)}
        hashes = {**flows, "offer_target:7": {"availability": "draining"}}
        lists = {"campaign:1:flows": [], "flows:scope:1:company:1": ["10"]}
        attrs = {"geo": "US", "os": "ios", "device_type": "mobile"}

        # NEW visitor → draining target excluded → no flow → None.
        r = _redis_with_lists_and_hashes(lists, hashes)
        new_winner = await resolve_flow(
            r, campaign_id="1", company_id=1, buyer_id=None, team_id=None,
            department_id=None, custom_group_id=None, click_attrs=attrs,
            returning_visitor=False,
        )
        assert new_winner is None

        # RETURNING visitor → draining served.
        r2 = _redis_with_lists_and_hashes(lists, hashes)
        ret_winner = await resolve_flow(
            r2, campaign_id="1", company_id=1, buyer_id=None, team_id=None,
            department_id=None, custom_group_id=None, click_attrs=attrs,
            returning_visitor=True,
        )
        assert ret_winner["_id"] == "10"

    @pytest.mark.asyncio
    async def test_all_unavailable_returns_none(self):
        # Only flow's target closed → None → router emits terminal fallback.
        flows = {"flow:10": _offer_flow(fid="10", scope_type="company", scope_id=1, target_id=7)}
        hashes = {**flows, "offer_target:7": {"availability": "closed"}}
        lists = {"campaign:1:flows": [], "flows:scope:1:company:1": ["10"]}
        r = _redis_with_lists_and_hashes(lists, hashes)
        winner = await resolve_flow(
            r, campaign_id="1", company_id=1, buyer_id=None, team_id=None,
            department_id=None, custom_group_id=None,
            click_attrs={"geo": "US", "os": "ios", "device_type": "mobile"},
        )
        assert winner is None


# ============================================================
# v2 LD-F2 — routing_trace deep-dive (D22 / §05 Tier-3):
# criteria.{winner_matched,rejected} + availability.excluded_target_ids
# + the X-Test-Id heavy/light gating. Pins the audit-2 MED remediation.
# ============================================================


def _crit_flow(*, fid, scope_type="company", scope_id=1, seq_id=1, criteria, target_id=None):
    """A flow with explicit criteria (offer-action when target_id given, else
    redirect — so the availability floor is/ isn't engaged as the test needs)."""
    f = _make_flow(
        fid=fid, scope_type=scope_type, scope_id=scope_id, seq_id=seq_id,
        criteria=criteria,
        action_type="offer" if target_id is not None else "redirect",
    )
    if target_id is not None:
        f["action_config"] = json.dumps({"offer_id": 1, "target_id": target_id})
    return f


class TestLDF2CriteriaTrace:
    @pytest.mark.asyncio
    async def test_winner_matched_and_rejected_recorded(self):
        """(a) winner with criteria → trace.criteria carries winner_matched
        descriptors AND the rejected flow with its failing criterion."""
        flows = {
            # winner: geo in [US] (matches the US click)
            "flow:10": _crit_flow(fid="10", seq_id=1, criteria=[{"type": "geo", "op": "in", "values": ["US"]}]),
            # rejected: geo in [CA] (US click fails it)
            "flow:20": _crit_flow(fid="20", seq_id=2, criteria=[{"type": "geo", "op": "in", "values": ["CA"]}]),
        }
        lists = {"campaign:1:flows": ["10", "20"], "flows:scope:1:company:1": []}
        r = _redis_with_lists_and_hashes(lists, flows)
        trace: dict = {}
        winner = await resolve_flow(
            r, campaign_id="1", company_id=1, buyer_id=None, team_id=None,
            department_id=None, custom_group_id=None,
            click_attrs={"geo": "US", "os": "ios", "device_type": "mobile"},
            trace=trace,
        )
        assert winner["_id"] == "10"
        crit = trace["criteria"]
        assert crit["winner_matched"] == ["geo in [US]"]
        assert crit["rejected"] == [{"flow_id": "20", "failed": "geo in [CA]"}]
        # compact (no X-Test-Id) → rejected entry has NO full-criteria detail.
        assert "criteria" not in crit["rejected"][0]

    @pytest.mark.asyncio
    async def test_match_all_winner_has_empty_winner_matched(self):
        """A match-all winner (empty criteria) records winner_matched=[]."""
        flows = {"flow:10": _crit_flow(fid="10", criteria=[])}
        lists = {"campaign:1:flows": ["10"], "flows:scope:1:company:1": []}
        r = _redis_with_lists_and_hashes(lists, flows)
        trace: dict = {}
        winner = await resolve_flow(
            r, campaign_id="1", company_id=1, buyer_id=None, team_id=None,
            department_id=None, custom_group_id=None,
            click_attrs={"geo": "US", "os": "ios", "device_type": "mobile"},
            trace=trace,
        )
        assert winner["_id"] == "10"
        assert trace["criteria"]["winner_matched"] == []
        # no rejections → no rejected key
        assert "rejected" not in trace["criteria"]

    @pytest.mark.asyncio
    async def test_compact_caps_rejected_at_three_heavy_lifts_and_details(self):
        """(d) X-Test-Id gating: light path caps rejected at 3 with no per-flow
        criteria; diagnostic=True lifts the cap AND adds full descriptors."""
        # 5 rejected flows (all geo in [CA] — the US click fails each) + 1 winner.
        flows = {"flow:1": _crit_flow(fid="1", seq_id=1, criteria=[{"type": "geo", "op": "in", "values": ["US"]}])}
        ids = ["1"]
        for n in range(2, 7):
            flows[f"flow:{n}"] = _crit_flow(
                fid=str(n), seq_id=n,
                criteria=[{"type": "geo", "op": "in", "values": ["CA"]}],
            )
            ids.append(str(n))
        lists = {"campaign:1:flows": ids, "flows:scope:1:company:1": []}

        # Light (no diagnostic)
        r = _redis_with_lists_and_hashes(lists, flows)
        light: dict = {}
        await resolve_flow(
            r, campaign_id="1", company_id=1, buyer_id=None, team_id=None,
            department_id=None, custom_group_id=None,
            click_attrs={"geo": "US", "os": "ios", "device_type": "mobile"},
            trace=light, diagnostic=False,
        )
        assert len(light["criteria"]["rejected"]) == 3
        assert light["criteria"]["rejected_truncated"] == 2
        assert all("criteria" not in e for e in light["criteria"]["rejected"])

        # Heavy (X-Test-Id present → diagnostic=True)
        r2 = _redis_with_lists_and_hashes(lists, flows)
        heavy: dict = {}
        await resolve_flow(
            r2, campaign_id="1", company_id=1, buyer_id=None, team_id=None,
            department_id=None, custom_group_id=None,
            click_attrs={"geo": "US", "os": "ios", "device_type": "mobile"},
            trace=heavy, diagnostic=True,
        )
        assert len(heavy["criteria"]["rejected"]) == 5  # cap lifted
        assert "rejected_truncated" not in heavy["criteria"]
        # heavy entries carry full criteria descriptors
        assert heavy["criteria"]["rejected"][0]["criteria"] == ["geo in [CA]"]

    @pytest.mark.asyncio
    async def test_no_trace_is_byte_identical_no_sink_cost(self):
        """resolve_flow without a trace dict behaves exactly as before (the
        pure-unit / no-observability path) — no exceptions, same winner."""
        flows = {
            "flow:10": _crit_flow(fid="10", seq_id=1, criteria=[{"type": "geo", "op": "in", "values": ["US"]}]),
            "flow:20": _crit_flow(fid="20", seq_id=2, criteria=[{"type": "geo", "op": "in", "values": ["CA"]}]),
        }
        lists = {"campaign:1:flows": ["10", "20"], "flows:scope:1:company:1": []}
        r = _redis_with_lists_and_hashes(lists, flows)
        winner = await resolve_flow(
            r, campaign_id="1", company_id=1, buyer_id=None, team_id=None,
            department_id=None, custom_group_id=None,
            click_attrs={"geo": "US", "os": "ios", "device_type": "mobile"},
        )
        assert winner["_id"] == "10"


class TestLDF2AvailabilityTrace:
    @pytest.mark.asyncio
    async def test_excluded_target_ids_recorded(self):
        """(c) availability floor excludes a target → trace.availability
        carries the specific excluded_target_ids + the reason."""
        # buyer flow → target 7 CLOSED (excluded), company flow → target 9 active.
        flows = {
            "flow:10": _offer_flow(fid="10", scope_type="buyer", scope_id=5, target_id=7),
            "flow:20": _offer_flow(fid="20", scope_type="company", scope_id=1, target_id=9),
        }
        hashes = {
            **flows,
            "offer_target:7": {"availability": "closed"},
            "offer_target:9": {"availability": "active"},
        }
        lists = {
            "campaign:1:flows": [],
            "flows:scope:1:buyer:5": ["10"],
            "flows:scope:1:company:1": ["20"],
        }
        r = _redis_with_lists_and_hashes(lists, hashes)
        trace: dict = {}
        winner = await resolve_flow(
            r, campaign_id="1", company_id=1, buyer_id=5, team_id=None,
            department_id=None, custom_group_id=None,
            click_attrs={"geo": "US", "os": "ios", "device_type": "mobile"},
            trace=trace,
        )
        assert winner["_id"] == "20"  # fell through to company
        assert trace["availability"]["excluded_target_ids"] == [7]
        assert trace["availability"]["reason"] == "closed"
        # the legacy int counter is still maintained (decision_reason consumer)
        assert trace["availability_excluded"] == 1

    @pytest.mark.asyncio
    async def test_all_active_no_availability_subobject(self):
        """Nothing drained/closed → no availability sub-object (byte-identical
        steady state)."""
        flows = {"flow:10": _offer_flow(fid="10", scope_type="company", scope_id=1, target_id=7)}
        hashes = {**flows, "offer_target:7": {"availability": "active"}}
        lists = {"campaign:1:flows": [], "flows:scope:1:company:1": ["10"]}
        r = _redis_with_lists_and_hashes(lists, hashes)
        trace: dict = {}
        await resolve_flow(
            r, campaign_id="1", company_id=1, buyer_id=None, team_id=None,
            department_id=None, custom_group_id=None,
            click_attrs={"geo": "US", "os": "ios", "device_type": "mobile"},
            trace=trace,
        )
        assert "availability" not in trace
        assert trace["availability_excluded"] == 0
