"""Global-flow completeness gaps — FOCUS 1 (GTD-R127), closes
`FINDINGS-G3-CRITIC.md` §(c) gaps 1-5, plus a regression pin for the new
`routing_trace.candidates_truncated` marker (GTD-R129 / ADR-0102) stamped by
`cascade.py`'s `_MAX_FLOWS_PER_CLICK` truncation branch.

Kept in its own file rather than appended to `test_global_flow_matrix.py`
(already 738 lines, over the 600-line test-file cap per
`code-organization.md` — a pre-existing condition from G2, not grown
further here) — same rationale G2 itself used for
`test_offer_default_target_availability.py`.

Fixture-builder helpers (`_flow`/`_scope_key`/`_seed`/`_resolve`/`_wid`)
mirror `test_global_flow_matrix.py`'s, per this codebase's established
per-test-file fixture convention (see that file's own docstring, which
mirrors `test_cascade.py`/`test_returning_routing.py` in turn).

IMPORTANT: gap 5 (`TestGap5CrossScopeTruncationCounterExample`) PINS the
CURRENT, documented-defective truncation behaviour (GTD-R129) — it does
NOT assert correctness. The ordering fix is explicitly DEFERRED per
ADR-0102 (see `cascade.py`'s comment near `_MAX_FLOWS_PER_CLICK`); do not
read a passing test here as "the truncation bug is fixed."
"""

from __future__ import annotations

import json

import fakeredis.aioredis
import pytest

from app.action_executor import BLOCK_RESULT, execute_action
from app.cascade import _MAX_FLOWS_PER_CLICK, resolve_flow
from app.models import ClickRequest

pytestmark = pytest.mark.asyncio


# ============================================================
# Fixture builders (mirrors test_global_flow_matrix.py)
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
    `test_global_flow_matrix.py::_flow`)."""
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
    availability HASHes into a fresh fakeredis instance (mirrors
    `test_global_flow_matrix.py::_seed`)."""
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


def _click() -> ClickRequest:
    return ClickRequest(
        click_id="abc-123",
        country="US",
        user_agent="Mozilla/5.0 (iPhone)",
        query_params={},
    )


def _stub_build_url():
    """Minimal `build_url_fn` stub — signature MUST mirror `router.build_url`
    (mirrors `test_action_executor.py::_stub_build_url`)."""

    def fn(template, req, campaign_id, offer_id, *,
           source_mappings, campaign_mappings, target_id=None, flow_id=None):
        return f"FINAL[{template}|cid={campaign_id}|oid={offer_id}]"

    return fn


# ============================================================
# Gap 1 — block/split action_type with a WINNING global/cross-scope flow
# ============================================================


class TestGap1ActionCompositionOnGlobalWinner:
    """FINDINGS-G3-CRITIC.md §(c) gap 1 — `block`/`split` were never
    exercised on a flow that won specifically because it was global and
    out-scoped a campaign-bound competitor. Each layer already has generic
    coverage on its own (`TestGap1AnchorGlobalPreemptsCampaignBound` for
    winner-selection, `test_action_executor.py::TestSplit/TestBlock` for
    execution mechanics) — these tests prove the two layers COMPOSE
    correctly for a global-flow winner specifically, by feeding the exact
    `resolve_flow` winner dict into `execute_action`."""

    async def test_global_split_flow_wins_and_executes(self):
        campaign_bound = _flow(
            "CB", scope_type="company", scope_id=1, campaign_id="1", seq_id=1,
            action_type="offer", action_config={"offer_id": 99, "target_id": 999},
        )
        global_split = _flow(
            "GS", scope_type="buyer", scope_id=5, campaign_id="0", seq_id=2,
            action_type="split",
            action_config={"offers": [{"offer_id": 5, "target_id": 101, "weight": 100}]},
        )
        r = await _seed(
            {"CB": campaign_bound, "GS": global_split},
            campaign_lists={"1": ["CB"]},
            scope_lists={_scope_key(1, "buyer", 5): ["GS"]},
            availability={
                "999": {"url": "https://cb-offer", "availability": "active"},
                "101": {"url": "https://leg-101", "availability": "active"},
            },
        )
        winner = await _resolve(r, campaign_id="1", company_id=1, buyer_id=5)
        assert _wid(winner) == "GS"
        assert winner["scope_type"] == "buyer"
        assert winner["campaign_id"] == "0"

        result = await execute_action(
            r, winner, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=_stub_build_url(),
        )
        assert result is not None
        assert result["offer_id"] == "5"
        assert result["target_id"] == "101"

    async def test_global_block_flow_wins_and_executes(self):
        campaign_bound = _flow(
            "CB", scope_type="company", scope_id=1, campaign_id="1", seq_id=1,
            action_type="redirect", action_config={"url": "https://fallback"},
        )
        global_block = _flow(
            "GB", scope_type="buyer", scope_id=5, campaign_id="0", seq_id=2,
            action_type="block",
        )
        r = await _seed(
            {"CB": campaign_bound, "GB": global_block},
            campaign_lists={"1": ["CB"]},
            scope_lists={_scope_key(1, "buyer", 5): ["GB"]},
        )
        winner = await _resolve(r, campaign_id="1", company_id=1, buyer_id=5)
        assert _wid(winner) == "GB"
        assert winner["scope_type"] == "buyer"

        result = await execute_action(
            r, winner, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=_stub_build_url(),
        )
        assert result == BLOCK_RESULT


# ============================================================
# Gap 2 — `not_in` fail-open on an absent dim, on a global flow +
# combined with the scope-priority walk
# ============================================================


class TestGap2NotInFailOpenGlobalCrossScope:
    """FINDINGS-G3-CRITIC.md §(c) gap 2 — `not_in` fail-open on a missing
    dim is ALREADY pinned generically and confirmed INTENTIONAL
    (`test_cascade.py::test_empty_click_attr_passes_not_in_criterion`) —
    this is NOT a new bug. It was never exercised on a GLOBAL flow at a
    non-company scope, nor combined with the scope-priority walk. Pins the
    documented trade-off at the compound shape G3 flagged as worth naming:
    a buyer-scope flow whose block-list criterion silently passes because
    its dimension failed to resolve wins the walk BEFORE a company-level
    flow enforcing the same intent via a different (present) dimension is
    ever reached — by design (the walk stops at the first matching
    bucket), not a bug."""

    async def test_buyer_scope_not_in_fail_open_preempts_company_scope_check(self):
        buyer_flow = _flow(
            "BUYER_FAILOPEN", scope_type="buyer", scope_id=5, campaign_id="0",
            seq_id=1, criteria=[{"type": "geo", "op": "not_in", "values": ["RU"]}],
        )
        company_flow = _flow(
            "COMPANY_ENFORCED", scope_type="company", scope_id=1, campaign_id="1",
            seq_id=1, criteria=[{"type": "geo", "op": "in", "values": ["US"]}],
        )
        r = await _seed(
            {"BUYER_FAILOPEN": buyer_flow, "COMPANY_ENFORCED": company_flow},
            campaign_lists={"1": ["COMPANY_ENFORCED"]},
            scope_lists={_scope_key(1, "buyer", 5): ["BUYER_FAILOPEN"]},
        )
        # `geo` intentionally absent — simulates an upstream geo-IP
        # resolution failure hitting the click.
        winner = await _resolve(
            r, campaign_id="1", company_id=1, buyer_id=5,
            click_attrs={"os": "ios", "device_type": "mobile"},
        )
        assert _wid(winner) == "BUYER_FAILOPEN"


# ============================================================
# Gap 3 — unicode / case-mismatch in a string criterion value,
# combined with a global flow
# ============================================================


class TestGap3UnicodeCaseMismatchGlobalFlow:
    """FINDINGS-G3-CRITIC.md §(c) gap 3 — case-preserved dims
    (`geo`/`region`/`browser`/`language`, `_CASE_PRESERVE`) had zero
    coverage anywhere (G1 §5.3, not added by G2), and never in combination
    with a global flow. Low bug-risk per G1's own reasoning (a genuine
    mismatch would be a producer-side bug elsewhere in the pipeline, not a
    cascade bug) — this pins the EXISTING exact-string-equality contract so
    a future refactor that accidentally normalizes a case-preserved dim on
    a global flow is caught."""

    async def test_case_mismatch_on_case_preserved_dim_fails_global_flow(self):
        """`browser` is in `_CASE_PRESERVE` — no case-folding. A criterion
        value of "Chrome" must NOT match a click_val of "chrome"."""
        global_flow = _flow(
            "GLOBAL_BROWSER", scope_type="buyer", scope_id=5, campaign_id="0",
            seq_id=1, criteria=[{"type": "browser", "op": "in", "values": ["Chrome"]}],
        )
        fallback = _flow(
            "FALLBACK", scope_type="company", scope_id=1, campaign_id="1",
            seq_id=1, criteria=[],
        )
        r = await _seed(
            {"GLOBAL_BROWSER": global_flow, "FALLBACK": fallback},
            campaign_lists={"1": ["FALLBACK"]},
            scope_lists={_scope_key(1, "buyer", 5): ["GLOBAL_BROWSER"]},
        )
        winner = await _resolve(
            r, campaign_id="1", company_id=1, buyer_id=5,
            click_attrs={
                "geo": "US", "os": "ios", "device_type": "mobile", "browser": "chrome",
            },
        )
        # Case mismatch → the global buyer-scope flow's criterion fails →
        # falls through to the company-scope fallback.
        assert _wid(winner) == "FALLBACK"

    async def test_exact_unicode_value_matches_global_flow(self):
        """Non-ASCII criterion values on a case-preserved dim (`language`)
        compare by exact string equality — pins that unicode values work
        (and remain case-sensitive) on a global flow."""
        global_flow = _flow(
            "GLOBAL_LANG", scope_type="buyer", scope_id=5, campaign_id="0",
            seq_id=1, criteria=[{"type": "language", "op": "in", "values": ["Français"]}],
        )
        r = await _seed(
            {"GLOBAL_LANG": global_flow},
            scope_lists={_scope_key(1, "buyer", 5): ["GLOBAL_LANG"]},
        )
        winner = await _resolve(
            r, campaign_id="1", company_id=1, buyer_id=5,
            click_attrs={
                "geo": "US", "os": "ios", "device_type": "mobile", "language": "Français",
            },
        )
        assert _wid(winner) == "GLOBAL_LANG"

        # Lowercase mismatch on the same unicode value → no match, no
        # candidate survives anywhere → None.
        r2 = await _seed(
            {"GLOBAL_LANG": global_flow},
            scope_lists={_scope_key(1, "buyer", 5): ["GLOBAL_LANG"]},
        )
        winner2 = await _resolve(
            r2, campaign_id="1", company_id=1, buyer_id=5,
            click_attrs={
                "geo": "US", "os": "ios", "device_type": "mobile", "language": "français",
            },
        )
        assert winner2 is None


# ============================================================
# Gap 4 — the 20-criteria cap at RUNTIME, on a global flow
# ============================================================


class TestGap4TwentyCriteriaCapRuntime:
    """FINDINGS-G3-CRITIC.md §(c) gap 4 — the 20-criteria cap is enforced
    ONLY at admin-api write-time (`max_length=20` on the criteria schema).
    Click-processor's runtime evaluator (`_first_failing_criterion`) has no
    cap of its own and was never proven to correctly walk a flow with MORE
    than 20 criteria (G1 §5.3 — doc/hygiene-tier, out of `cascade.py`'s own
    file scope). Pins the runtime's actual (uncapped, all-AND) behaviour on
    a global flow, so a future cap accidentally added here doesn't silently
    regress correctness."""

    async def test_global_flow_with_21_matching_criteria_wins(self):
        criteria = [
            {"type": "geo", "op": "in", "values": ["US"]} for _ in range(20)
        ] + [{"type": "os", "op": "in", "values": ["ios"]}]
        assert len(criteria) == 21
        global_flow = _flow(
            "GLOBAL_21", scope_type="buyer", scope_id=5, campaign_id="0",
            seq_id=1, criteria=criteria,
        )
        r = await _seed(
            {"GLOBAL_21": global_flow},
            scope_lists={_scope_key(1, "buyer", 5): ["GLOBAL_21"]},
        )
        winner = await _resolve(
            r, campaign_id="1", company_id=1, buyer_id=5,
            click_attrs={"geo": "US", "os": "ios", "device_type": "mobile"},
        )
        assert _wid(winner) == "GLOBAL_21"

    async def test_global_flow_21st_criterion_failing_still_excludes(self):
        """Proves the 21st criterion is actually EVALUATED (not silently
        dropped past a would-be cap) — flip it to fail and the flow must
        lose, even though the first 20 all match."""
        criteria = [
            {"type": "geo", "op": "in", "values": ["US"]} for _ in range(20)
        ] + [{"type": "os", "op": "in", "values": ["android"]}]  # fails — click is ios
        global_flow = _flow(
            "GLOBAL_21_FAIL", scope_type="buyer", scope_id=5, campaign_id="0",
            seq_id=1, criteria=criteria,
        )
        fallback = _flow(
            "FALLBACK", scope_type="company", scope_id=1, campaign_id="1",
            seq_id=1, criteria=[],
        )
        r = await _seed(
            {"GLOBAL_21_FAIL": global_flow, "FALLBACK": fallback},
            campaign_lists={"1": ["FALLBACK"]},
            scope_lists={_scope_key(1, "buyer", 5): ["GLOBAL_21_FAIL"]},
        )
        winner = await _resolve(
            r, campaign_id="1", company_id=1, buyer_id=5,
            click_attrs={"geo": "US", "os": "ios", "device_type": "mobile"},
        )
        assert _wid(winner) == "FALLBACK"


# ============================================================
# Gap 5 — the cross-scope truncation counter-example (G3 §(b) Reason 1)
# ============================================================


class TestGap5CrossScopeTruncationCounterExample:
    """FINDINGS-G3-CRITIC.md §(c) gap 5 (== §(b) Reason 1's counter-example,
    restated as a coverage gap) — G2's own truncation test
    (`test_global_flow_matrix.py::test_max_flows_truncation_can_silently_drop_the_true_winner`)
    is SAME-SCOPE-ONLY (every candidate `scope_type="company"`); it cannot
    exercise the worse, cross-scope shape the G3 critic independently
    traced: `campaign:{id}:flows` is fetched WHOLE and placed FIRST in the
    concatenated candidate list (`cascade.py::_collect_candidate_ids`), so
    a single campaign with >200 bound flows can consume the ENTIRE cap
    before a single global candidate at ANY org-scope level is ever
    appended — not just risk one tied flow's outcome, but a WHOLE-WALK
    BLACKOUT for every global flow visible to that click.

    This PINS the CURRENT (defective, documented — GTD-R129 / ADR-0102)
    behaviour and asserts the new `routing_trace.candidates_truncated`
    marker (this same phase's prod-code change) fires. It does NOT assert
    correctness — the ordering fix is explicitly DEFERRED per ADR-0102."""

    async def test_overgrown_campaign_blanks_out_a_buyer_scope_global_override(self):
        # 205 old campaign-bound, company-scope flows — comfortably over the
        # 200 cap so truncation fires regardless of exact boundary count.
        old_campaign_flows = {
            f"OLD{n}": _flow(
                f"OLD{n}", scope_type="company", scope_id=1, campaign_id="1",
                seq_id=n, criteria=[],
            )
            for n in range(1, 206)
        }
        # A fresh admin-created global override at buyer scope — the "new
        # global override vs old campaign backlog" shape FINDINGS-G3 names
        # as exactly what this whole test campaign exists to validate.
        buyer_override = _flow(
            "BUYER_OVERRIDE", scope_type="buyer", scope_id=5, campaign_id="0",
            seq_id=9000, criteria=[],
        )
        flows = {**old_campaign_flows, "BUYER_OVERRIDE": buyer_override}
        # Concatenation order mirrors `_collect_candidate_ids`'s real fixed
        # pipe-issue order (campaign list FIRST, buyer scope list appended
        # after) — the buyer override lands past position 200.
        r = await _seed(
            flows,
            campaign_lists={"1": list(old_campaign_flows.keys())},
            scope_lists={_scope_key(1, "buyer", 5): ["BUYER_OVERRIDE"]},
        )
        trace: dict = {}
        winner = await _resolve(
            r, campaign_id="1", company_id=1, buyer_id=5, trace=trace,
        )
        # Defect confirmed: the buyer-scope override never became a
        # candidate — the walk falls through to a company-scope,
        # campaign-bound flow instead.
        assert _wid(winner) != "BUYER_OVERRIDE"
        assert winner["scope_type"] == "company"
        # The safe observability marker (this phase's prod-code change)
        # fires — the silent half of "silently misroute" is closed.
        assert trace["candidates_truncated"] is True


# ============================================================
# `routing_trace.candidates_truncated` marker — regression pin (ADR-0102)
# ============================================================


class TestCandidatesTruncatedMarker:
    """The safe, purely-additive `routing_trace.candidates_truncated`
    marker (ADR-0102) — stamped ONLY inside the `_MAX_FLOWS_PER_CLICK`
    truncation branch, absent otherwise. Zero behaviour change; closes the
    'silent' half of GTD-R129's silent-misroute risk."""

    async def test_marker_absent_when_under_cap(self):
        flows = {
            str(n): _flow(str(n), scope_type="company", scope_id=1, campaign_id="1", seq_id=n)
            for n in range(1, 6)
        }
        r = await _seed(flows, campaign_lists={"1": list(flows.keys())})
        trace: dict = {}
        winner = await _resolve(r, campaign_id="1", company_id=1, trace=trace)
        assert winner is not None
        assert "candidates_truncated" not in trace

    async def test_marker_stamped_true_when_over_cap(self):
        flows = {
            str(n): _flow(str(n), scope_type="company", scope_id=1, campaign_id="1", seq_id=n)
            for n in range(1, _MAX_FLOWS_PER_CLICK + 11)
        }
        r = await _seed(flows, campaign_lists={"1": list(flows.keys())})
        trace: dict = {}
        winner = await _resolve(r, campaign_id="1", company_id=1, trace=trace)
        assert winner is not None
        assert trace["candidates_truncated"] is True

    async def test_marker_absent_when_trace_not_threaded(self):
        """`trace=None` (the pure-unit / no-op path) must stay
        byte-identical — no crash, no attribute access on `None`."""
        flows = {
            str(n): _flow(str(n), scope_type="company", scope_id=1, campaign_id="1", seq_id=n)
            for n in range(1, _MAX_FLOWS_PER_CLICK + 11)
        }
        r = await _seed(flows, campaign_lists={"1": list(flows.keys())})
        winner = await _resolve(r, campaign_id="1", company_id=1, trace=None)
        assert winner is not None  # no exception raised
