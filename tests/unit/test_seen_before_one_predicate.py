"""Ф1 — the returning-audience predicate is computed in ONE place.

WHY THIS FILE EXISTS
--------------------
Until 2026-08-22 the expression

    bool(attribution.get("uid")) and (attribution.get("is_unique") is False)

was written out TWICE in `app/router.py` — once in `_allowed_availability`, once
in `_try_flow_cascade` — and so were the two lines computing the routing gate
around it. Each copy carried a comment saying it MUST stay the exact mirror of
the other.

A comment is not a mechanism. Two places that each compute the same decision will
eventually diverge, and no guard on either side can see it, because neither site
is individually wrong. The redesign now in flight changes what "seen before"
MEANS — it is precisely the edit that would have split them, and the split would
have shown up as clicks routed one way and their availability judged another.

So the predicate became a function, and this file is the mechanism that keeps it
one. It pins BOTH halves:

  * behaviour — the truth tables, including a DISCRIMINATING pair that must give
    different answers, so the suite cannot pass vacuously;
  * structure — the raw expression appears exactly once in the module, so a
    future edit cannot quietly re-inline a second copy.

NOT pinned here, deliberately: that a full click record is unchanged end-to-end.
It cannot be — `weighted_select` is seeded per click and two runs legitimately
differ. A done-check that can never pass is not a strict check, it is a broken
one. Cascade INPUT equivalence is the right level, and that is what the
`_allowed_availability` cases below assert.
"""
from __future__ import annotations

import pathlib
import re

import pytest
from unittest.mock import patch

from app import router


ROUTER_SRC = pathlib.Path(router.__file__).read_text(encoding="utf-8")


# ------------------------------------------------------------ the predicate --


@pytest.mark.parametrize(
    "attribution, expected, why",
    [
        ({"uid": "u1", "is_unique": False}, True,
         "the uid existed before this click - B union C"),
        ({"uid": "u1", "is_unique": True}, False,
         "resolver minted the uid on THIS click - a new visitor"),
        ({"uid": "u1"}, False,
         "no is_unique key at all: resolver dark - fail closed to first pool"),
        ({"uid": "", "is_unique": False}, False,
         "empty uid is no uid"),
        ({"is_unique": False}, False,
         "no uid: nothing to have seen before"),
        ({}, False, "empty attribution"),
        ({"uid": "u1", "is_unique": 0}, False,
         "0 is not False under `is` - identity, not truthiness, on purpose"),
    ],
)
def test_seen_before_truth_table(attribution, expected, why):
    assert router._seen_before(attribution) is expected, why


def test_seen_before_has_a_discriminating_pair():
    """The two inputs that differ in exactly one key must differ in the answer.

    Without this, every assertion above could be satisfied by a function that
    returns a constant, and the suite would still be green."""
    seen = {"uid": "u1", "is_unique": False}
    fresh = {"uid": "u1", "is_unique": True}
    assert router._seen_before(seen) != router._seen_before(fresh)


# ----------------------------------------------------------------- the gate --


def test_returning_live_needs_env_and_company():
    on = {"returning_routing": "1"}
    off = {"returning_routing": "0"}
    with patch.object(router.settings, "returning_routing_enabled", True):
        assert router._returning_live(on) is True
        assert router._returning_live(off) is False
        assert router._returning_live({}) is False
    with patch.object(router.settings, "returning_routing_enabled", False):
        # env off dominates - the per-company opt-in cannot switch it back on
        assert router._returning_live(on) is False


def test_audience_routing_is_live_routing_minus_the_campaign_opt_out():
    with patch.object(router.settings, "returning_routing_enabled", True):
        assert router._audience_routing({"returning_routing": "1"}) is True
        assert router._audience_routing(
            {"returning_routing": "1", "disable_returning_flows": "1"}) is False
        assert router._audience_routing({"returning_routing": "0"}) is False


# ---------------------------------------------- the caller both sites share --


def test_allowed_availability_admits_draining_only_for_a_returning_visitor():
    """`_allowed_availability` is one of the two former copies. Its answer must
    follow the shared predicate, and it must be able to answer BOTH ways."""
    campaign = {"returning_routing": "1"}
    seen = {"uid": "u1", "is_unique": False}
    fresh = {"uid": "u1", "is_unique": True}
    with patch.object(router.settings, "returning_routing_enabled", True):
        assert router._allowed_availability(campaign, seen) == frozenset(
            {"active", "draining"})
        assert router._allowed_availability(campaign, fresh) == frozenset({"active"})


def test_allowed_availability_is_dark_when_routing_is_off():
    """The zero-regress invariant: routing off ⇒ {active} for everyone, so a
    'draining' target blocks all, exactly as before the partition existed."""
    seen = {"uid": "u1", "is_unique": False}
    with patch.object(router.settings, "returning_routing_enabled", False):
        assert router._allowed_availability(
            {"returning_routing": "1"}, seen) == frozenset({"active"})
    with patch.object(router.settings, "returning_routing_enabled", True):
        assert router._allowed_availability(
            {"returning_routing": "1", "disable_returning_flows": "1"}, seen
        ) == frozenset({"active"})


# -------------------------------------------------- the anti-re-split pin --


def test_seen_before_is_defined_once():
    """The raw expression must appear EXACTLY once in router.py - inside the
    helper. This is the mechanism that replaces the two 'MUST stay the exact
    mirror' comments; if someone re-inlines a copy, this goes red immediately
    rather than after the two copies have silently drifted apart."""
    hits = re.findall(r'attribution\.get\("is_unique"\)\s+is\s+False', ROUTER_SRC)
    assert len(hits) == 1, (
        f"the seen_before predicate is written out {len(hits)} times in "
        "router.py - it must be computed once, in `_seen_before`, and called. "
        "Two copies of one decision diverge, and no guard on either side can "
        "see it because neither site is individually wrong."
    )


def test_the_routing_gate_is_defined_once():
    """Same argument for the gate the two sites shared."""
    hits = re.findall(
        r"settings\.returning_routing_enabled\s+and\s+_company_routing_enabled",
        ROUTER_SRC)
    assert len(hits) == 1, (
        f"the returning-live gate is written out {len(hits)} times - it must be "
        "computed once, in `_returning_live`."
    )


def test_both_former_sites_now_call_the_helpers():
    """Non-vacuity for the two pins above: proving the expression appears once
    is worthless if the second site stopped asking the question entirely."""
    assert ROUTER_SRC.count("_seen_before(attribution)") >= 2, (
        "both `_allowed_availability` and `_try_flow_cascade` must still ASK "
        "whether the visitor was seen before - a pin that passes because a "
        "caller vanished is not a pin"
    )
    assert ROUTER_SRC.count("_audience_routing(campaign)") >= 2
