"""The node READS `flow_family` from the campaign HASH — and decides nothing.

Step B2.2 of the offerwall-routing build. admin-api ships the field (migration
327) and the sync builder puts it on every `campaign:{id}` HASH; this is the
node half, and its whole job today is to be **observable**.

Why observability is the requirement rather than a nicety: activation ordering
depends on every node being able to read the field BEFORE any campaign is
switched to 'offerwall'. The transport fails open — a node that has never heard
of the field routes everything as standard, byte-identical to today — and that
same fail-open turns fail-CLOSED at the product level the moment a wall
campaign, which has no ordinary flows, meets a stale node. A reader that
recorded nothing would make "the fleet is ready" an assumption about deploy
coverage; recording it into the routing trace makes it a question a single
click answers.

So these tests pin two things and deliberately nothing else:

1. the reader normalises every odd input to 'standard' instead of raising or
   forwarding an unknown family;
2. the value actually reaches `cascade_trace`, in BOTH directions — a test that
   only asserted 'standard' would pass against a hardcoded constant.

They do NOT pin any routing behaviour, because there is none to pin yet.
"""

from __future__ import annotations

import json

import pytest

from app.router import (
    DEFAULT_FLOW_FAMILY,
    FLOW_FAMILIES,
    _campaign_flow_family,
)
# Reuse the existing routing harness rather than building a second one: a
# private fake that drifts from the real one is how two tests come to disagree
# about what a click does.
from tests.unit.test_router_cascade import FakeRedis, _click, _route_with


class TestTheReaderNormalises:
    def test_standard_reads_back(self):
        assert _campaign_flow_family({"flow_family": "standard"}) == "standard"

    def test_offerwall_reads_back(self):
        # The discriminating case: a reader hardcoded to the default passes
        # every other test in this class and fails only this one.
        assert _campaign_flow_family({"flow_family": "offerwall"}) == "offerwall"

    @pytest.mark.parametrize("raw, expected", [
        ("OFFERWALL", "offerwall"),
        ("  offerwall  ", "offerwall"),
        ("Standard", "standard"),
    ])
    def test_case_and_whitespace_tolerated(self, raw, expected):
        # Same tolerance `_campaign_returning_flows_disabled` applies to its own
        # field, so the two readers cannot disagree about HASH hygiene.
        assert _campaign_flow_family({"flow_family": raw}) == expected

    @pytest.mark.parametrize("odd", [
        {},                              # a node whose sync predates the field
        {"flow_family": ""},             # present but empty
        {"flow_family": "wall"},         # a value the DB CHECK should refuse
        {"flow_family": "all_visitors"},  # plausible, still not a family
        {"flow_family": None},
        {"flow_family": 7},
    ])
    def test_anything_odd_fails_open_to_standard(self, odd):
        assert _campaign_flow_family(odd) == "standard"

    def test_it_never_raises_on_a_missing_key(self):
        # A raise here would propagate into the routing hot path, where the
        # cost of a config gap must never be a failed click.
        assert _campaign_flow_family({}) == DEFAULT_FLOW_FAMILY


class TestTheVocabularyIsClosed:
    def test_exactly_two_families(self):
        assert set(FLOW_FAMILIES) == {"standard", "offerwall"}

    def test_the_default_is_the_identity_value(self):
        # 'standard' is what every campaign already does, which is why shipping
        # this reader changes nothing anywhere.
        assert DEFAULT_FLOW_FAMILY == "standard"
        assert DEFAULT_FLOW_FAMILY in FLOW_FAMILIES

    def test_the_wire_vocabulary_matches_the_writers(self):
        # admin-api's sync builder declares the same two values. They are two
        # ends of one wire; if they ever diverge, a family emitted by one side
        # is silently unreadable by the other.
        assert FLOW_FAMILIES == ("standard", "offerwall")


def _redis_with_family(campaign_id: str, family: str | None) -> FakeRedis:
    """A minimal routable campaign, with or without the family field.

    A redirect flow, because it is the shortest path that produces a routed
    click — this asserts nothing about offers and must not depend on them.
    """
    campaign_hash = {"company_id": "1", "priority": "0", "weight": "100"}
    if family is not None:
        campaign_hash["flow_family"] = family
    return FakeRedis(
        sets={
            "geo:US": {campaign_id},
            "device:mobile": {campaign_id},
            "os:ios": {campaign_id},
            "campaigns:active": {campaign_id},
        },
        hashes={
            f"campaign:{campaign_id}": campaign_hash,
            "flow:900": {
                "campaign_id": campaign_id,
                "scope_type": "company",
                "scope_id": "1",
                "seq_id": "1",
                "is_default": "0",
                "criteria": "[]",
                "action_type": "redirect",
                "action_config": json.dumps({"url": "https://lp.example.com/x"}),
            },
        },
        lists={f"campaign:{campaign_id}:flows": ["900"]},
    )


class TestTheValueReachesTheRoutingTrace:
    """The reader is only useful if what it read is VISIBLE on a real click.

    Without this, "every node can read the field" stays an assumption about
    deploy coverage. With it, one click answers the question.
    """

    def test_a_standard_campaign_records_standard(self):
        result = _route_with(_redis_with_family("801", "standard"), _click())
        assert result is not None
        assert result["attribution"]["routing_trace"]["flow_family"] == "standard"

    def test_an_offerwall_campaign_records_offerwall(self):
        # 🔴 The discriminating case. Every other test here passes against a
        # hardcoded "standard"; only this one fails.
        result = _route_with(_redis_with_family("802", "offerwall"), _click())
        assert result is not None
        assert result["attribution"]["routing_trace"]["flow_family"] == "offerwall"

    def test_a_campaign_hash_without_the_field_still_routes(self):
        # A node whose sync predates the field. The click must route exactly as
        # before and record the identity value — never raise, never dead-end.
        result = _route_with(_redis_with_family("803", None), _click())
        assert result is not None
        assert result["url"].startswith("https://lp.example.com/")
        assert result["attribution"]["routing_trace"]["flow_family"] == "standard"

    def test_an_offerwall_campaign_still_routes_exactly_as_standard_today(self):
        # The inert claim, stated as a test: setting the family changes the
        # DESTINATION of nothing. When the dispatcher lands this must be
        # revisited deliberately, not silently.
        standard = _route_with(_redis_with_family("804", "standard"), _click())
        offerwall = _route_with(_redis_with_family("805", "offerwall"), _click())
        assert standard is not None and offerwall is not None
        assert standard["url"] == offerwall["url"]
        assert standard["attribution"]["flow_id"] == offerwall["attribution"]["flow_id"]
