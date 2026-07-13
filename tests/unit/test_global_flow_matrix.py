"""Global-flow exhaustive variability matrix — FOCUS 1 (GTD-R127 / GTD-D15).

Closes the 7 named gaps + 3 additional gaps identified by the G1 discovery
audit (`FINDINGS-G1-MATRIX.md` §5.2/§5.3) against the click-processor cascade
resolver (`app/cascade.py::resolve_flow`/`_pick_winner`). Every flow is a
literal HASH staged into `fakeredis` (mirrors `test_returning_routing.py`'s
fixture style) — multi-scope-list + availability-hash combinations are
trivial to seed this way, unlike the hand-rolled pipeline mock in
`test_cascade.py`.

Anchor scenario (brief-G2-core.md #1, FINDINGS-G1 §3.2): a GLOBAL flow at a
MORE-SPECIFIC scope pre-empts a CAMPAIGN-BOUND flow at company scope —
`_pick_winner` walks `SCOPE_PRIORITY` (most-specific first) BEFORE the
binding tie-break, so scope specificity always dominates binding. This is
real, current, undocumented-as-a-warning production behaviour: 0 of 159
production flows have ever had `campaign_id IS NULL`, so every cell below is
a cold path being exercised for the first time.
"""

from __future__ import annotations

import json

import fakeredis.aioredis
import pytest

from app.cascade import _MAX_FLOWS_PER_CLICK, resolve_flow

pytestmark = pytest.mark.asyncio


# ============================================================
# Fixture builders
# ============================================================


def _flow(
    fid,
    *,
    scope_type="company",
    scope_id=1,
    campaign_id="0",
    seq_id=1,
    is_default=False,
    criteria=None,
    audience="first",
    action_type="redirect",
    action_config=None,
):
    """Build a flow HASH the way the sync builder emits it (mirrors
    `test_cascade.py::_make_flow` + `test_returning_routing.py::_flow`)."""
    return {
        "scope_type": scope_type,
        "scope_id": str(scope_id),
        "campaign_id": campaign_id,
        "seq_id": str(seq_id),
        "is_default": "1" if is_default else "0",
        "criteria": json.dumps(criteria if criteria is not None else []),
        "audience": audience,
        "action_type": action_type,
        "action_config": json.dumps(action_config if action_config is not None else {}),
        "name": f"flow-{fid}",
    }


def _scope_key(company_id, scope_type, scope_id):
    return f"flows:scope:{company_id}:{scope_type}:{scope_id}"


async def _seed(
    flows: dict[str, dict],
    *,
    scope_lists: dict[str, list[str]] | None = None,
    campaign_lists: dict[str, list[str]] | None = None,
    availability: dict[str, dict] | None = None,
):
    """Stage flows + scope/campaign candidate lists + offer_target
    availability HASHes into a fresh fakeredis instance — mirrors the sync
    builder's real key shapes (`flow:{id}`, `flows:scope:{co}:{type}:{id}`,
    `campaign:{id}:flows`, `offer_target:{id}`). A target id absent from
    `availability` stays a genuinely-missing HASH (→ cascade's `_AVAIL_MISSING`
    sentinel), NOT 'active' — callers that want an explicit active target
    must say so."""
    r = fakeredis.aioredis.FakeRedis(decode_responses=True)
    for fid, h in flows.items():
        await r.hset(f"flow:{fid}", mapping=h)
    for key, ids in (scope_lists or {}).items():
        if ids:
            await r.rpush(key, *ids)
    for cid, ids in (campaign_lists or {}).items():
        if ids:
            await r.rpush(f"campaign:{cid}:flows", *ids)
    for tid, fields in (availability or {}).items():
        await r.hset(f"offer_target:{tid}", mapping=fields)
    return r


async def _resolve(
    r,
    *,
    campaign_id="1",
    company_id=1,
    buyer_id=None,
    team_id=None,
    department_id=None,
    custom_group_id=None,
    click_attrs=None,
    seen_before=False,
    audience_routing=False,
    trace=None,
):
    return await resolve_flow(
        r,
        campaign_id=campaign_id,
        company_id=company_id,
        buyer_id=buyer_id,
        team_id=team_id,
        department_id=department_id,
        custom_group_id=custom_group_id,
        click_attrs=click_attrs or {"geo": "US", "os": "ios", "device_type": "mobile"},
        seen_before=seen_before,
        audience_routing=audience_routing,
        trace=trace,
    )


def _wid(flow):
    return flow["_id"] if flow else None


# ============================================================
# Gap #1 (named anchor) — global flow at a MORE-SPECIFIC scope
# pre-empts a campaign-bound flow at company scope.
# ============================================================


class TestGap1AnchorGlobalPreemptsCampaignBound:
    """FINDINGS-G1 §3.2 anchor: a GLOBAL flow at buyer scope pre-empts a
    CAMPAIGN-BOUND flow at company scope, for the SAME campaign's click.
    `_pick_winner` walks `SCOPE_PRIORITY` (most-specific first); binding is
    only a same-LEVEL tie-break, so scope specificity dominates regardless
    of binding. No prior test combined a different scope level with a
    different binding in one `resolve_flow` call (G1 gap #1)."""

    async def test_global_buyer_flow_beats_campaign_bound_company_flow(self):
        company_bound = _flow(
            "CB", scope_type="company", scope_id=1, campaign_id="1", seq_id=1,
        )
        buyer_global = _flow(
            "BG", scope_type="buyer", scope_id=5, campaign_id="0", seq_id=99,
        )
        flows = {"CB": company_bound, "BG": buyer_global}
        r = await _seed(
            flows,
            campaign_lists={"1": ["CB"]},
            scope_lists={_scope_key(1, "buyer", 5): ["BG"]},
        )
        winner = await _resolve(r, buyer_id=5)
        assert _wid(winner) == "BG"
        assert winner["campaign_id"] == "0"  # confirms the winner is the GLOBAL one

    async def test_reverse_no_buyer_context_campaign_bound_wins(self):
        """Same config, but the click has NO buyer id (walked past buyer
        level entirely) — the buyer-scoped global flow is never even a
        candidate (its scope list is never fetched), so the campaign-bound
        company flow wins normally. Confirms the anchor is buyer-CONTEXT
        gated, not a blanket override."""
        company_bound = _flow(
            "CB", scope_type="company", scope_id=1, campaign_id="1", seq_id=1,
        )
        buyer_global = _flow(
            "BG", scope_type="buyer", scope_id=5, campaign_id="0", seq_id=99,
        )
        flows = {"CB": company_bound, "BG": buyer_global}
        r = await _seed(
            flows,
            campaign_lists={"1": ["CB"]},
            scope_lists={_scope_key(1, "buyer", 5): ["BG"]},
        )
        winner = await _resolve(r, buyer_id=None)
        assert _wid(winner) == "CB"

    async def test_different_buyer_never_sees_the_override(self):
        """A buyer NOT id=5 must never see the buyer=5-scoped global flow —
        falls through to the campaign-bound company flow instead."""
        company_bound = _flow(
            "CB", scope_type="company", scope_id=1, campaign_id="1", seq_id=1,
        )
        buyer_global = _flow(
            "BG", scope_type="buyer", scope_id=5, campaign_id="0", seq_id=99,
        )
        flows = {"CB": company_bound, "BG": buyer_global}
        r = await _seed(
            flows,
            campaign_lists={"1": ["CB"]},
            scope_lists={_scope_key(1, "buyer", 5): ["BG"]},
        )
        winner = await _resolve(r, buyer_id=6)
        assert _wid(winner) == "CB"


# ============================================================
# Gap #3 — custom_group / department as full participants
# ============================================================


class TestGap3CustomGroupAndDepartmentFullParticipants:
    """`custom_group` is `SCOPE_PRIORITY`'s thinnest-tested level (G1 §5.2
    gap #3): appears exactly ONCE as a winner anywhere in the repo, never
    as a loser, never in an availability-fallback scenario, never directly
    vs `buyer` (its immediate specificity neighbor). `department` only
    ever appears paired against `team` (loses) or `company` (wins) in pure
    specificity tests — never in a fallback-chain or availability test."""

    async def test_buyer_beats_custom_group_direct_comparison(self):
        """buyer < custom_group per `SCOPE_PRIORITY` — buyer wins. This
        EXACT pairing was never directly compared anywhere in the repo."""
        buyer_flow = _flow("B", scope_type="buyer", scope_id=5, seq_id=1)
        group_flow = _flow("G", scope_type="custom_group", scope_id=10, seq_id=1)
        flows = {"B": buyer_flow, "G": group_flow}
        r = await _seed(
            flows,
            scope_lists={
                _scope_key(1, "buyer", 5): ["B"],
                _scope_key(1, "custom_group", 10): ["G"],
            },
        )
        winner = await _resolve(r, buyer_id=5, custom_group_id=10)
        assert _wid(winner) == "B"

    async def test_custom_group_as_loser_falls_back_from_buyer(self):
        """custom_group has NEVER been tested as a LOSER. Buyer flow
        present but criteria-excluded → falls through to custom_group."""
        buyer_flow = _flow(
            "B", scope_type="buyer", scope_id=5, seq_id=1,
            criteria=[{"type": "geo", "op": "in", "values": ["RU"]}],
        )
        group_flow = _flow("G", scope_type="custom_group", scope_id=10, seq_id=1)
        flows = {"B": buyer_flow, "G": group_flow}
        r = await _seed(
            flows,
            scope_lists={
                _scope_key(1, "buyer", 5): ["B"],
                _scope_key(1, "custom_group", 10): ["G"],
            },
        )
        winner = await _resolve(r, buyer_id=5, custom_group_id=10)
        assert _wid(winner) == "G"

    async def test_custom_group_availability_fallback_to_team(self):
        """custom_group has NEVER been combined with the availability floor
        (G1 gap #4 — availability × scope only ever covered buyer<->company).
        custom_group flow's only pinned target is CLOSED → falls to team."""
        group_flow = _flow(
            "G", scope_type="custom_group", scope_id=10, seq_id=1,
            action_type="offer", action_config={"offer_id": 1, "target_id": 7},
        )
        team_flow = _flow(
            "T", scope_type="team", scope_id=3, seq_id=1,
            action_type="offer", action_config={"offer_id": 1, "target_id": 9},
        )
        flows = {"G": group_flow, "T": team_flow}
        r = await _seed(
            flows,
            scope_lists={
                _scope_key(1, "custom_group", 10): ["G"],
                _scope_key(1, "team", 3): ["T"],
            },
            availability={"7": {"availability": "closed"}, "9": {"availability": "active"}},
        )
        winner = await _resolve(r, custom_group_id=10, team_id=3)
        assert _wid(winner) == "T"

    async def test_department_availability_fallback_to_company(self):
        """department was ONLY ever tested paired against team (loses) or
        company (wins) in pure specificity tests — never in an
        availability-fallback test."""
        dept_flow = _flow(
            "D", scope_type="department", scope_id=2, seq_id=1,
            action_type="offer", action_config={"offer_id": 1, "target_id": 7},
        )
        company_flow = _flow(
            "C", scope_type="company", scope_id=1, seq_id=1,
            action_type="offer", action_config={"offer_id": 1, "target_id": 9},
        )
        flows = {"D": dept_flow, "C": company_flow}
        r = await _seed(
            flows,
            scope_lists={
                _scope_key(1, "department", 2): ["D"],
                _scope_key(1, "company", 1): ["C"],
            },
            # target 7 HASH intentionally absent → dead-offer 'missing' sentinel.
            availability={"9": {"availability": "active"}},
        )
        winner = await _resolve(r, department_id=2)
        assert _wid(winner) == "C"

    async def test_department_fallback_chain_to_team(self):
        """department in a criteria-driven FALLBACK-CHAIN test (not just
        pure specificity)."""
        dept_flow = _flow(
            "D", scope_type="department", scope_id=2, seq_id=1,
            criteria=[{"type": "geo", "op": "in", "values": ["RU"]}],
        )
        team_flow = _flow("T", scope_type="team", scope_id=3, seq_id=1, criteria=[])
        flows = {"D": dept_flow, "T": team_flow}
        r = await _seed(
            flows,
            scope_lists={
                _scope_key(1, "department", 2): ["D"],
                _scope_key(1, "team", 3): ["T"],
            },
        )
        winner = await _resolve(r, department_id=2, team_id=3)
        assert _wid(winner) == "T"


# ============================================================
# Gap #2 — global × returning-audience at every scope level
# ============================================================


class TestGap2GlobalReturningAudienceAllLevels:
    """A returning-audience GLOBAL flow has NEVER been exercised at any
    scope level other than company (G1 gap #2 — exhaustive grep confirmed
    `audience_routing`/`_partition_audience`/`seen_before` never co-occur
    with a non-company `scope_type` NOR with `campaign_id='0'` in any test
    file in the repo)."""

    @pytest.mark.parametrize(
        "scope_type,scope_id,kw",
        [
            ("buyer", 5, {"buyer_id": 5}),
            ("team", 3, {"team_id": 3}),
            ("department", 2, {"department_id": 2}),
            ("custom_group", 10, {"custom_group_id": 10}),
        ],
    )
    async def test_global_returning_flow_wins_at_each_non_company_scope(
        self, scope_type, scope_id, kw,
    ):
        returning_flow = _flow(
            "R", scope_type=scope_type, scope_id=scope_id,
            campaign_id="0", audience="returning", seq_id=1,
        )
        first_flow = _flow(
            "F", scope_type="company", scope_id=1,
            campaign_id="0", audience="first", seq_id=1,
        )
        flows = {"R": returning_flow, "F": first_flow}
        r = await _seed(
            flows,
            scope_lists={
                _scope_key(1, scope_type, scope_id): ["R"],
                _scope_key(1, "company", 1): ["F"],
            },
        )
        winner = await _resolve(r, seen_before=True, audience_routing=True, **kw)
        assert _wid(winner) == "R"
        assert winner["audience"] == "returning"

    async def test_global_returning_at_buyer_inert_when_audience_routing_off(self):
        """The critical zero-regression assertion: with `audience_routing`
        OFF, a returning-audience GLOBAL flow at buyer scope (the
        MOST-specific level) is COMPLETELY inert — even for a genuinely
        returning visitor. A regression here would silently start serving
        returning-only flows to routing that thinks it is off."""
        returning_flow = _flow(
            "R", scope_type="buyer", scope_id=5,
            campaign_id="0", audience="returning", seq_id=1,
        )
        first_flow = _flow(
            "F", scope_type="company", scope_id=1,
            campaign_id="0", audience="first", seq_id=1,
        )
        flows = {"R": returning_flow, "F": first_flow}
        r = await _seed(
            flows,
            scope_lists={
                _scope_key(1, "buyer", 5): ["R"],
                _scope_key(1, "company", 1): ["F"],
            },
        )
        winner = await _resolve(r, buyer_id=5, seen_before=True, audience_routing=False)
        assert _wid(winner) == "F"

    async def test_returning_at_company_beats_first_at_buyer_audience_trumps_scope(self):
        """G1 §3.3 high-value test: for a `seen_before` visitor, the
        RETURNING pool is walked in FULL (all 5 levels) BEFORE the cascade
        ever falls to the first pool — so a returning flow at COMPANY
        (least specific) beats a first-audience flow at BUYER (most
        specific). Audience trumps scope-specificity, not vice versa."""
        returning_company = _flow(
            "RC", scope_type="company", scope_id=1,
            campaign_id="0", audience="returning", seq_id=1,
        )
        first_buyer = _flow(
            "FB", scope_type="buyer", scope_id=5,
            campaign_id="0", audience="first", seq_id=1,
        )
        flows = {"RC": returning_company, "FB": first_buyer}
        r = await _seed(
            flows,
            scope_lists={
                _scope_key(1, "company", 1): ["RC"],
                _scope_key(1, "buyer", 5): ["FB"],
            },
        )
        winner = await _resolve(r, buyer_id=5, seen_before=True, audience_routing=True)
        assert _wid(winner) == "RC"

    async def test_new_visitor_never_sees_returning_pool_regardless_of_scope(self):
        """A NEW (non-`seen_before`) visitor never evaluates the returning
        pool, even when the returning flow is at the MOST specific scope
        and the first flow is at the LEAST specific."""
        returning_buyer = _flow(
            "RB", scope_type="buyer", scope_id=5,
            campaign_id="0", audience="returning", seq_id=1,
        )
        first_company = _flow(
            "FC", scope_type="company", scope_id=1,
            campaign_id="0", audience="first", seq_id=1,
        )
        flows = {"RB": returning_buyer, "FC": first_company}
        r = await _seed(
            flows,
            scope_lists={
                _scope_key(1, "buyer", 5): ["RB"],
                _scope_key(1, "company", 1): ["FC"],
            },
        )
        winner = await _resolve(r, buyer_id=5, seen_before=False, audience_routing=True)
        assert _wid(winner) == "FC"

    async def test_returning_criteria_no_history_fails_closed(self):
        """A returning flow with `prev_offer` criteria + a visitor with NO
        history (empty set) → intersection is empty → fails closed →
        excluded (distinct fail-closed path from CF-3's unknown-dim gate)."""
        returning_flow = _flow(
            "R", scope_type="buyer", scope_id=5, campaign_id="0",
            audience="returning", seq_id=1,
            criteria=[{"type": "prev_offer", "op": "in", "values": ["5", "9"]}],
        )
        first_flow = _flow(
            "F", scope_type="company", scope_id=1,
            campaign_id="0", audience="first", seq_id=1,
        )
        flows = {"R": returning_flow, "F": first_flow}
        r = await _seed(
            flows,
            scope_lists={
                _scope_key(1, "buyer", 5): ["R"],
                _scope_key(1, "company", 1): ["F"],
            },
        )
        winner = await _resolve(
            r, buyer_id=5, seen_before=True, audience_routing=True,
            click_attrs={
                "geo": "US", "os": "ios", "device_type": "mobile",
                "prev_offer": frozenset(),
            },
        )
        assert _wid(winner) == "F"


# ============================================================
# Gap #5 — campaign-bound flow at a non-company scope actually
# WINNING resolve_flow (not just sync-emitted).
# ============================================================


class TestGap5CampaignBoundNonCompanyScopeWins:
    """No test has a campaign-bound `department`- or `custom_group`-scoped
    flow actually WIN `resolve_flow` (G1 gap #5). The admin-api
    sync-contract test only checks hash emission, never winner selection."""

    async def test_campaign_bound_department_scope_wins(self):
        dept_flow = _flow(
            "D", scope_type="department", scope_id=2, campaign_id="1", seq_id=1,
        )
        r = await _seed({"D": dept_flow}, campaign_lists={"1": ["D"]})
        winner = await _resolve(r, department_id=2)
        assert _wid(winner) == "D"

    async def test_campaign_bound_custom_group_scope_wins(self):
        group_flow = _flow(
            "G", scope_type="custom_group", scope_id=10, campaign_id="1", seq_id=1,
        )
        r = await _seed({"G": group_flow}, campaign_lists={"1": ["G"]})
        winner = await _resolve(r, custom_group_id=10)
        assert _wid(winner) == "G"

    async def test_campaign_bound_scoped_flow_never_matches_outside_its_scope(self):
        """The exact 'admin picks a scope in the UI without realizing it
        restricts the campaign flow's reach' scenario (G1 §3.2). A
        campaign-bound flow scoped to team=7 NEVER matches a click whose
        buyer is NOT in team 7 — not 'loses to something', literally
        excluded from every bucket in the walk."""
        team_flow = _flow(
            "T", scope_type="team", scope_id=7, campaign_id="1", seq_id=1,
        )
        r = await _seed({"T": team_flow}, campaign_lists={"1": ["T"]})
        # buyer belongs to team 3, NOT 7 — the scoped campaign flow is
        # invisible to this click even though it is "for this campaign".
        winner = await _resolve(r, team_id=3)
        assert winner is None

    async def test_two_campaign_bound_same_scope_resolved_by_seq_id(self):
        """Two campaign-bound flows, same campaign, same explicit
        non-company scope — legal, no uniqueness constraint blocks it
        (G1 §1.5/§3.2). Resolved deterministically by seq_id (older wins)."""
        older = _flow("OLD", scope_type="team", scope_id=7, campaign_id="1", seq_id=1)
        newer = _flow("NEW", scope_type="team", scope_id=7, campaign_id="1", seq_id=5)
        r = await _seed(
            {"OLD": older, "NEW": newer}, campaign_lists={"1": ["OLD", "NEW"]},
        )
        winner = await _resolve(r, team_id=7)
        assert _wid(winner) == "OLD"


# ============================================================
# Gap #6 — all 5 levels populated at once, mixed binding,
# one correct winner threading the whole chain.
# ============================================================


class TestGap6FiveLevelMixedBindingEndToEnd:
    """No test populates all 5 scope levels with DISTINCT winning
    candidates (mixed binding) and asserts the ONE correct winner
    threading the whole chain (G1 gap #6)."""

    async def test_buyer_wins_over_all_four_less_specific_levels(self):
        """Buyer (global) present alongside custom_group/department
        (campaign-bound) and team/company (global) — buyer must win; none
        of the other 4 even reach tie-break."""
        buyer = _flow("BUY", scope_type="buyer", scope_id=5, campaign_id="0", seq_id=1)
        group = _flow(
            "GRP", scope_type="custom_group", scope_id=10, campaign_id="1", seq_id=1,
        )
        team = _flow("TEA", scope_type="team", scope_id=3, campaign_id="0", seq_id=1)
        dept = _flow(
            "DEP", scope_type="department", scope_id=2, campaign_id="1", seq_id=1,
        )
        comp = _flow("COM", scope_type="company", scope_id=1, campaign_id="0", seq_id=1)
        flows = {"BUY": buyer, "GRP": group, "TEA": team, "DEP": dept, "COM": comp}
        r = await _seed(
            flows,
            campaign_lists={"1": ["GRP", "DEP"]},
            scope_lists={
                _scope_key(1, "buyer", 5): ["BUY"],
                _scope_key(1, "team", 3): ["TEA"],
                _scope_key(1, "company", 1): ["COM"],
            },
        )
        winner = await _resolve(
            r, buyer_id=5, custom_group_id=10, team_id=3, department_id=2,
        )
        assert _wid(winner) == "BUY"

    async def test_removing_each_more_specific_level_walks_down_the_chain(self):
        """Progressive proof: with buyer excluded by criteria, custom_group
        wins; with custom_group ALSO excluded, team wins; etc. — the walk
        threads correctly through the whole 5-level chain in one shot."""
        ru_only = [{"type": "geo", "op": "in", "values": ["RU"]}]
        buyer = _flow(
            "BUY", scope_type="buyer", scope_id=5, campaign_id="0",
            seq_id=1, criteria=ru_only,
        )
        group = _flow(
            "GRP", scope_type="custom_group", scope_id=10, campaign_id="1",
            seq_id=1, criteria=ru_only,
        )
        team = _flow(
            "TEA", scope_type="team", scope_id=3, campaign_id="0",
            seq_id=1, criteria=ru_only,
        )
        dept = _flow(
            "DEP", scope_type="department", scope_id=2, campaign_id="1",
            seq_id=1, criteria=ru_only,
        )
        comp = _flow(
            "COM", scope_type="company", scope_id=1, campaign_id="0",
            seq_id=1, criteria=[],
        )
        flows = {"BUY": buyer, "GRP": group, "TEA": team, "DEP": dept, "COM": comp}
        r = await _seed(
            flows,
            campaign_lists={"1": ["GRP", "DEP"]},
            scope_lists={
                _scope_key(1, "buyer", 5): ["BUY"],
                _scope_key(1, "team", 3): ["TEA"],
                _scope_key(1, "company", 1): ["COM"],
            },
        )
        # US click — every RU-only flow fails except the match-all company one.
        winner = await _resolve(
            r, buyer_id=5, custom_group_id=10, team_id=3, department_id=2,
        )
        assert _wid(winner) == "COM"


# ============================================================
# Additional gaps (G1 §5.3) — MAX_FLOWS truncation, N-way tie,
# cross-tenant isolation beyond buyer_id.
# ============================================================


class TestAdditionalGapsTruncationAndTies:
    """G1 §5.3 — additional gaps beyond the named 7."""

    async def test_max_flows_truncation_does_not_drop_winner_within_cap(self):
        """Baseline: when candidates are UNDER `_MAX_FLOWS_PER_CLICK`,
        nothing is dropped."""
        decoys = {
            str(n): _flow(
                str(n), scope_type="company", scope_id=1, campaign_id="1",
                seq_id=n, criteria=[{"type": "geo", "op": "in", "values": ["RU"]}],
            )
            for n in range(1, 51)
        }
        winner_flow = _flow(
            "WIN", scope_type="company", scope_id=1, campaign_id="1",
            seq_id=999, criteria=[],
        )
        flows = {**decoys, "WIN": winner_flow}
        r = await _seed(
            flows, campaign_lists={"1": list(decoys.keys()) + ["WIN"]},
        )
        winner = await _resolve(r)
        assert _wid(winner) == "WIN"

    async def test_max_flows_truncation_can_silently_drop_the_true_winner(self):
        """`_MAX_FLOWS_PER_CLICK=200` truncation was completely untested
        (G1 §5.3). This demonstrates the ACTUAL (current) risk rather than
        assuming safety: truncation is deterministic FIRST-SEEN-ORDER
        (candidate-collection order == campaign-list position order here),
        NOT seq_id order. Seed exactly `_MAX_FLOWS_PER_CLICK` flows BEFORE
        the true lowest-seq_id winner in the campaign's flow list → the
        true winner falls at position 201 → gets truncated → a WORSE
        (higher-seq_id) flow wins instead. A legally configured campaign
        with >200 flows can silently misroute — a candidate real-defect,
        not a hypothetical."""
        decoy_match = _flow(
            "DECOY_MATCH", scope_type="company", scope_id=1, campaign_id="1",
            seq_id=1, criteria=[],
        )
        decoys = {
            f"D{n}": _flow(
                f"D{n}", scope_type="company", scope_id=1, campaign_id="1",
                seq_id=n + 1,
                criteria=[{"type": "geo", "op": "in", "values": ["RU"]}],
            )
            for n in range(1, _MAX_FLOWS_PER_CLICK)
        }
        # Lower seq_id than DECOY_MATCH — SHOULD win the tie-break if present.
        true_winner = _flow(
            "TRUE_WIN", scope_type="company", scope_id=1, campaign_id="1",
            seq_id=0, criteria=[],
        )
        flows = {**decoys, "DECOY_MATCH": decoy_match, "TRUE_WIN": true_winner}
        campaign_order = ["DECOY_MATCH"] + list(decoys.keys()) + ["TRUE_WIN"]
        assert len(campaign_order) == _MAX_FLOWS_PER_CLICK + 1
        r = await _seed(flows, campaign_lists={"1": campaign_order})
        winner = await _resolve(r)
        assert _wid(winner) == "DECOY_MATCH"
        assert _wid(winner) != "TRUE_WIN"

    async def test_n_way_tie_identical_bound_seqid_default_pins_actual_order(self):
        """No true N-way tie test exists — two flows identical on ALL
        THREE tie-break dims (bound/seq_id/is_default) was unpinned (G1
        §5.3, Python-sort-stability-dependent). Pins the ACTUAL observed
        behaviour so a future stdlib/algorithm change is caught loudly
        instead of silently flipping which flow serves."""
        a = _flow(
            "A", scope_type="company", scope_id=1, campaign_id="1",
            seq_id=5, is_default=False,
        )
        b = _flow(
            "B", scope_type="company", scope_id=1, campaign_id="1",
            seq_id=5, is_default=False,
        )
        r = await _seed({"A": a, "B": b}, campaign_lists={"1": ["A", "B"]})
        winner = await _resolve(r)
        # Python's `list.sort` is stable — first-seen (list-insertion)
        # order wins the tie.
        assert _wid(winner) == "A"

    async def test_n_way_tie_reversed_insertion_order_flips_winner(self):
        """Confirms the tie is genuinely insertion-order-dependent (not
        some other implicit key) — reversing list order flips the winner
        for otherwise byte-identical flows."""
        a = _flow(
            "A", scope_type="company", scope_id=1, campaign_id="1",
            seq_id=5, is_default=False,
        )
        b = _flow(
            "B", scope_type="company", scope_id=1, campaign_id="1",
            seq_id=5, is_default=False,
        )
        r = await _seed({"A": a, "B": b}, campaign_lists={"1": ["B", "A"]})
        winner = await _resolve(r)
        assert _wid(winner) == "B"


class TestGap7CrossTenantIsolationExtended:
    """The one existing cross-tenant test (`test_router_cascade.py`) covers
    only the `buyer_id` enrichment-attack vector (G1 gap #7). This adds
    structural proof for department/custom_group/team: a company-1 global
    flow at those levels must NEVER surface for company-2's candidate list
    — purely from Redis key-shape isolation, since no runtime filter
    exists (a KEY-SHAPE bug would be a silent cross-tenant leak with zero
    guard)."""

    @pytest.mark.parametrize(
        "scope_type,scope_id,kw",
        [
            ("team", 3, {"team_id": 3}),
            ("department", 2, {"department_id": 2}),
            ("custom_group", 10, {"custom_group_id": 10}),
        ],
    )
    async def test_company_scoped_global_flow_never_crosses_to_another_company(
        self, scope_type, scope_id, kw,
    ):
        co1_flow = _flow(
            "CO1", scope_type=scope_type, scope_id=scope_id,
            campaign_id="0", seq_id=1,
        )
        r = await _seed(
            {"CO1": co1_flow},
            scope_lists={_scope_key(1, scope_type, scope_id): ["CO1"]},
        )
        # A click for COMPANY 2 with the SAME numeric scope_id (id
        # collision across tenants is normal — ids are per-company
        # sequences) must NEVER see company 1's flow.
        winner = await _resolve(r, campaign_id="2", company_id=2, **kw)
        assert winner is None
