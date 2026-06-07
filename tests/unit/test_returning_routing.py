"""Returning-user segmented routing — cascade 2-pass tests (P4, 2026-06-05).

Pins the audience partition + fallthrough + the G1 invariant (seen_before, NOT
is_returning, selects the returning pool) + set-valued prev_* matching, and
PROVES zero-regression when segmented routing is OFF.
"""

from __future__ import annotations

import json

import fakeredis.aioredis
import pytest

from app import cascade
from app.cascade import _criteria_match, resolve_flow

pytestmark = pytest.mark.asyncio


def _flow(fid, *, audience="first", criteria=None, seq_id=1, is_default=False):
    return {
        "scope_type": "company",
        "scope_id": "1",
        "campaign_id": "1",            # campaign-bound
        "seq_id": str(seq_id),
        "is_default": "1" if is_default else "0",
        "criteria": json.dumps(criteria if criteria is not None else []),
        "audience": audience,
        "action_type": "redirect",
        "action_config": "{}",
        "name": f"flow-{fid}",
    }


async def _setup(flows: dict[str, dict]):
    r = fakeredis.aioredis.FakeRedis(decode_responses=True)
    if flows:
        await r.rpush("campaign:1:flows", *flows.keys())
    for fid, h in flows.items():
        await r.hset(f"flow:{fid}", mapping=h)
    return r


async def _resolve(r, *, click_attrs=None, seen_before=False, audience_routing=False):
    return await resolve_flow(
        r,
        campaign_id="1",
        company_id=1,
        buyer_id=None,
        team_id=None,
        department_id=None,
        custom_group_id=None,
        click_attrs=click_attrs or {"geo": "US"},
        seen_before=seen_before,
        audience_routing=audience_routing,
    )


def _wid(flow):
    return flow["_id"] if flow else None


# ============================================================
# Zero-regression for LEGACY flows + F-LC-1 (returning flows
# inert under fresh / DARK — audience routing OFF)
# ============================================================

class TestOffByteIdentical:
    async def test_off_legacy_flows_byte_identical(self):
        # OFF + only first/legacy flows → lowest seq_id wins exactly as pre-P4.
        # The partition leaves every 'first' (and missing-audience) flow in the
        # candidate pool, so legacy behaviour is untouched.
        r = await _setup({
            "F1": _flow("F1", audience="first", seq_id=2),
            "F2": _flow("F2", audience="first", seq_id=1),
        })
        flow = await _resolve(r, seen_before=True, audience_routing=False)
        assert _wid(flow) == "F2"  # lower seq_id wins among first-pool flows

    async def test_off_returning_flow_inert_first_flow_wins(self):
        # F-LC-1: OFF → a 'returning' flow (even with a LOWER seq_id) is INERT;
        # the first-pool flow wins instead of the returning flow (D5).
        r = await _setup({
            "F1": _flow("F1", audience="first", seq_id=2),
            "F2": _flow("F2", audience="returning", seq_id=1),
        })
        flow = await _resolve(r, seen_before=True, audience_routing=False)
        assert _wid(flow) == "F1"  # returning flow excluded under OFF

    async def test_off_lone_returning_flow_inert_returns_none(self):
        # F-LC-1: OFF + a single 'returning' flow → inert → no flow wins → None
        # (caller falls back to legacy split selection). Pre-fix this returned F1.
        r = await _setup({"F1": _flow("F1", audience="returning")})
        flow = await _resolve(r, seen_before=False, audience_routing=False)
        assert flow is None


# ============================================================
# F-LC-1 (audit-2 MED) — returning-audience flow must be INERT
# under fresh / DARK, and must NOT mislabel a new visitor's click
# with audience_pool=returning (→ decision_reason=override_returning_flow).
# Evidence: docs/development/audit-findings/2/raw-LIVE-C-v2.md.
# ============================================================

class TestFLC1ReturningFlowInertUnderFresh:
    async def test_fresh_returning_flow_no_criteria_does_not_serve_new_visitor(self):
        # The exact F-LC-1 shape: fresh campaign (audience_routing OFF) + a
        # returning-audience flow WITHOUT returning criteria (match-all, so it
        # would match a new visitor) + a first default. Pre-fix the returning
        # flow won and the click was stamped audience_pool=returning. Post-fix
        # the returning flow is inert and the first default serves.
        r = await _setup({
            "R1": _flow("R1", audience="returning", seq_id=1),  # match-all
            "F1": _flow("F1", audience="first", seq_id=2, is_default=True),
        })
        flow = await _resolve(r, seen_before=False, audience_routing=False)
        assert _wid(flow) == "F1"
        # The winner's audience drives router.py's `audience_pool` → it MUST NOT
        # be 'returning' (else _decision_reason → override_returning_flow).
        assert (flow.get("audience") or "first") != "returning"

    async def test_fresh_lone_returning_flow_falls_through_to_legacy(self):
        # Fresh + ONLY a returning flow → inert → None → caller uses legacy
        # split selection. No flow → no audience_pool=returning mislabel.
        r = await _setup({"R1": _flow("R1", audience="returning", seq_id=1)})
        flow = await _resolve(r, seen_before=False, audience_routing=False)
        assert flow is None

    async def test_returning_routing_on_returning_flow_still_serves(self):
        # COUNTER-TEST: with returning routing ON and a returning (seen_before)
        # visitor, the returning-audience flow STILL serves correctly in the
        # returning pool — the F-LC-1 fix did not regress the live feature.
        r = await _setup({
            "R1": _flow("R1", audience="returning", seq_id=1),
            "F1": _flow("F1", audience="first", seq_id=2),
        })
        flow = await _resolve(r, seen_before=True, audience_routing=True)
        assert _wid(flow) == "R1"
        assert flow.get("audience") == "returning"  # serves from returning pool


# ============================================================
# Segmented routing ON
# ============================================================

class TestSegmentedRouting:
    async def test_new_user_first_pool_only(self):
        # ON + new user (seen_before False) → 'returning' flows EXCLUDED.
        r = await _setup({
            "R1": _flow("R1", audience="returning", seq_id=1),
            "F1": _flow("F1", audience="first", seq_id=2),
        })
        flow = await _resolve(r, seen_before=False, audience_routing=True)
        assert _wid(flow) == "F1"  # returning flow never selected for a new user

    async def test_returning_user_prefers_returning_pool(self):
        # ON + seen_before → returning pool wins even though a first flow with a
        # LOWER seq_id also matches (returning pool evaluated FIRST).
        r = await _setup({
            "R1": _flow("R1", audience="returning", seq_id=9),
            "F1": _flow("F1", audience="first", seq_id=1),
        })
        flow = await _resolve(r, seen_before=True, audience_routing=True)
        assert _wid(flow) == "R1"

    async def test_fallthrough_to_first_when_no_returning_match(self):
        # ON + seen_before, but the returning flow's criteria DON'T match →
        # fall through to the first pool.
        r = await _setup({
            "R1": _flow("R1", audience="returning", seq_id=1,
                        criteria=[{"type": "geo", "op": "in", "values": ["DE"]}]),
            "F1": _flow("F1", audience="first", seq_id=2),
        })
        flow = await _resolve(r, click_attrs={"geo": "US"},
                              seen_before=True, audience_routing=True)
        assert _wid(flow) == "F1"  # returning didn't match → first pool

    async def test_no_returning_flows_immediate_fallthrough(self):
        # ON + seen_before but the company authored NO returning flows →
        # returning pool empty → immediate fallthrough → identical to today.
        r = await _setup({"F1": _flow("F1", audience="first")})
        flow = await _resolve(r, seen_before=True, audience_routing=True)
        assert _wid(flow) == "F1"


# ============================================================
# G1 — seen_before (B∪C), NOT the is_returning flag
# ============================================================

class TestG1SeenBeforeNotIsReturning:
    async def test_segment_C_reaches_returning_pool(self):
        # Segment C = returning via a NEW funnel → is_returning=False. It is
        # STILL a seen_before user and MUST evaluate the returning pool. A
        # returning flow targeting `is_returning in [false]` (i.e. segment C)
        # must be selectable.
        r = await _setup({
            "RC": _flow("RC", audience="returning", seq_id=1,
                        criteria=[{"type": "is_returning", "op": "in",
                                   "values": ["false"]}]),
            "F1": _flow("F1", audience="first", seq_id=2),
        })
        attrs = {"geo": "US", "is_returning": "false",
                 "prev_offer": frozenset(), "prev_offer_target": frozenset(),
                 "prev_sub": frozenset()}
        flow = await _resolve(r, click_attrs=attrs,
                              seen_before=True, audience_routing=True)
        assert _wid(flow) == "RC"  # segment-C user reached the returning pool

    async def test_segment_B_targeted_by_is_returning_true(self):
        r = await _setup({
            "RB": _flow("RB", audience="returning", seq_id=1,
                        criteria=[{"type": "is_returning", "op": "in",
                                   "values": ["true"]}]),
            "F1": _flow("F1", audience="first", seq_id=2),
        })
        # Segment B (same funnel) → is_returning=true → RB matches.
        b = await _resolve(r, click_attrs={"geo": "US", "is_returning": "true"},
                           seen_before=True, audience_routing=True)
        assert _wid(b) == "RB"
        # Segment C (is_returning=false) → RB does NOT match → fallthrough.
        c = await _resolve(r, click_attrs={"geo": "US", "is_returning": "false"},
                           seen_before=True, audience_routing=True)
        assert _wid(c) == "F1"


# ============================================================
# prev_* set-valued matching
# ============================================================

class TestPrevMatching:
    async def test_prev_offer_intersection_hit(self):
        r = await _setup({
            "R1": _flow("R1", audience="returning", seq_id=1,
                        criteria=[{"type": "prev_offer", "op": "in",
                                   "values": ["5", "9"]}]),
            "F1": _flow("F1", audience="first", seq_id=2),
        })
        attrs = {"geo": "US", "prev_offer": frozenset({"5"})}  # user hit offer 5
        flow = await _resolve(r, click_attrs=attrs,
                              seen_before=True, audience_routing=True)
        assert _wid(flow) == "R1"

    async def test_prev_offer_no_intersection_fallthrough(self):
        r = await _setup({
            "R1": _flow("R1", audience="returning", seq_id=1,
                        criteria=[{"type": "prev_offer", "op": "in",
                                   "values": ["7"]}]),
            "F1": _flow("F1", audience="first", seq_id=2),
        })
        attrs = {"geo": "US", "prev_offer": frozenset({"5", "9"})}  # never hit 7
        flow = await _resolve(r, click_attrs=attrs,
                              seen_before=True, audience_routing=True)
        assert _wid(flow) == "F1"

    async def test_criteria_match_set_intersection_unit(self):
        # in: any history value ∈ criterion values
        assert _criteria_match(
            [{"type": "prev_offer", "op": "in", "values": ["5", "9"]}],
            {"prev_offer": frozenset({"9"})},
        )
        assert not _criteria_match(
            [{"type": "prev_offer", "op": "in", "values": ["5", "9"]}],
            {"prev_offer": frozenset({"1"})},
        )
        # not_in: none of the history values may be in criterion values
        assert _criteria_match(
            [{"type": "prev_sub", "op": "not_in", "values": ["bad"]}],
            {"prev_sub": frozenset({"good"})},
        )
        assert not _criteria_match(
            [{"type": "prev_sub", "op": "not_in", "values": ["bad"]}],
            {"prev_sub": frozenset({"bad", "ok"})},
        )

    async def test_base_str_dims_unchanged(self):
        # A normal str dim must match exactly as before (no set branch taken).
        assert _criteria_match(
            [{"type": "geo", "op": "in", "values": ["US"]}], {"geo": "US"},
        )
        assert not _criteria_match(
            [{"type": "geo", "op": "in", "values": ["US"]}], {"geo": "DE"},
        )
