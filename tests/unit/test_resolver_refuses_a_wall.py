"""G4.5.7 — the ROUTING RESOLVER refuses a wall, independently of the publisher.

WHY THIS FILE EXISTS AT ALL, stated first because it is the whole point.

`sync/builders/splits.py` already excludes walls with a deliberately POSITIVE predicate,
and `sync/builders/flows.py` (G4.5.2) now diverts a wall's id into its own lists so it never
enters `campaign:{id}:flows`. Both are real. **Both are facts about the PUBLISHER'S OUTPUT.**

The dossier says so in one line and it is worth repeating here, because the distinction is
exactly what makes this file non-redundant:

    "that guard proves the PUBLISHER'S OUTPUT is clean, not that the resolver refuses.
     Test the resolver, not the builder."
    -- 37-G5-BLOCKED-NO-PUBLISHER.md

🔴 SO EVERY TEST BELOW DELIBERATELY PUTS A WALL WHERE THE PUBLISHER WOULD NEVER PUT ONE.
That is not an invalid fixture — it is the ONLY way to exercise the second layer. A test that
fed the resolver a correctly-published snapshot would prove the publisher works, which is
already pinned in `admin-api/tests/unit/test_wall_publisher.py`, and would say nothing about
what the resolver does when something upstream is wrong.

The states this defends against are real and none of them require a bug in today's publisher:

  * SYNC DRIFT — a node holding keys written by an OLDER admin-api that still published walls
    into the routing list, before the divert existed;
  * a hand-written or restored Redis key;
  * a future publisher change that regresses the divert. This file is what turns that
    regression from an outage into a caught defect.

WHAT "REFUSES" MEANS, PRECISELY — and it is the stronger of the two possible readings:
`_partition_audience` returns a wall in NEITHER pool ("the two returned lists no longer
reconstruct the input"), so the wall is not merely deprioritised behind a routing flow. It is
absent from the candidate set. A campaign whose ONLY flow is a wall therefore resolves to
None, and the caller must NOT read that as permission to serve the wall.
"""

from __future__ import annotations

import pytest

from app.cascade import _partition_audience, resolve_flow

from .test_cascade import _make_flow, _redis_with_lists_and_hashes

_CLICK = {"geo": "UA", "device": "desktop"}
_SCOPE = dict(company_id=1, buyer_id=None, team_id=None,
              department_id=None, custom_group_id=None)


class TestAWallOnlyCampaignResolvesToNothing:
    """The headline case. Nothing routes, and nothing falls through."""

    @pytest.mark.asyncio
    async def test_a_wall_in_the_routing_list_yields_no_winner(self):
        """A wall sitting in `campaign:{id}:flows` — the exact shape the publisher
        prevents — must not win. If this ever returns the wall, a catalogue is serving
        live traffic."""
        winner = await resolve_flow(
            _redis_with_lists_and_hashes(
                {"campaign:900:flows": ["1"]},
                {"flow:1": _make_flow(fid="1", campaign_id="900",
                                      audience="offerwall", action_type="offerwall")},
            ),
            campaign_id="900", click_attrs=_CLICK, **_SCOPE,
        )
        assert winner is None

    @pytest.mark.asyncio
    async def test_it_is_ABSENT_from_the_pools_not_merely_outranked(self):
        """The stronger claim, and the one that survives a change in tie-breaking.

        `_partition_audience` returns a wall in NEITHER list — its own docstring warns
        that the two lists no longer reconstruct the input. Asserting "the routing flow
        won" would pass even if the wall were merely sorted last, which is a different
        and much weaker property."""
        returning, first = _partition_audience([
            _make_flow(fid="1", audience="offerwall", action_type="offerwall"),
            _make_flow(fid="2", audience="first"),
        ])
        assert [f["_id"] for f in first] == ["2"]
        assert returning == []

    @pytest.mark.asyncio
    async def test_a_wall_marked_DEFAULT_still_does_not_win(self):
        """The dangerous variant. `is_default` makes a flow the catch-all for its bucket,
        so a wall carrying it would win by falling through rather than by matching.

        A32 forbids a wall being default at WRITE time, which is why this cannot happen
        today — and is exactly why it is worth pinning here: this test survives A32 being
        weakened, and it is the resolver's own answer rather than the writer's."""
        winner = await resolve_flow(
            _redis_with_lists_and_hashes(
                {"campaign:901:flows": ["1"]},
                {"flow:1": _make_flow(fid="1", campaign_id="901", is_default=True,
                                      audience="offerwall", action_type="offerwall")},
            ),
            campaign_id="901", click_attrs=_CLICK, **_SCOPE,
        )
        assert winner is None


class TestAWallCannotDISPLACEARoutingFlow:
    """Isolation in the direction that costs money: a wall must not take a click that a
    routing flow could have served."""

    @pytest.mark.asyncio
    async def test_the_routing_flow_wins_even_when_the_wall_is_more_specific(self):
        """Scope specificity is the cascade's first axis, so a buyer-scoped wall beside a
        company-scoped routing flow is the case where a naive exclusion ordering breaks:
        the wall looks like the better candidate on every axis the picker considers."""
        winner = await resolve_flow(
            _redis_with_lists_and_hashes(
                {"campaign:902:flows": ["1", "2"]},
                {
                    "flow:1": _make_flow(fid="1", campaign_id="902", scope_type="buyer",
                                         scope_id=5, audience="offerwall",
                                         action_type="offerwall"),
                    "flow:2": _make_flow(fid="2", campaign_id="902",
                                         audience="first", is_default=True),
                },
            ),
            campaign_id="902", buyer_id=5, company_id=1, team_id=None,
            department_id=None, custom_group_id=None, click_attrs=_CLICK,
        )
        assert winner is not None
        assert winner["_id"] == "2"

    @pytest.mark.asyncio
    async def test_a_wall_does_not_consume_the_lower_seq_id_tie_break(self):
        """`seq_id` is the tie-break, lower wins. A wall at seq_id 1 must not beat a
        routing flow at seq_id 2 — same isolation, different axis, because a guard that
        holds on one axis and not another is the shape that ships green."""
        winner = await resolve_flow(
            _redis_with_lists_and_hashes(
                {"campaign:903:flows": ["1", "2"]},
                {
                    "flow:1": _make_flow(fid="1", campaign_id="903", seq_id=1,
                                         audience="offerwall", action_type="offerwall"),
                    "flow:2": _make_flow(fid="2", campaign_id="903", seq_id=2,
                                         audience="first", is_default=True),
                },
            ),
            campaign_id="903", click_attrs=_CLICK, **_SCOPE,
        )
        assert winner is not None
        assert winner["_id"] == "2"


class TestTheGlobalScopeLadderToo:
    """The wider blast radius. A company-scoped GLOBAL wall would enter the cascade for
    every campaign of that tenant, so proving the campaign-bound case alone is not enough."""

    @pytest.mark.asyncio
    async def test_a_global_wall_in_a_scope_list_yields_no_winner(self):
        winner = await resolve_flow(
            _redis_with_lists_and_hashes(
                {"campaign:904:flows": [], "flows:scope:1:company:1": ["7"]},
                {"flow:7": _make_flow(fid="7", campaign_id="0", audience="offerwall",
                                      action_type="offerwall")},
            ),
            campaign_id="904", click_attrs=_CLICK, **_SCOPE,
        )
        assert winner is None


class TestCalibrationTheResolverStillROUTES:
    """🔴 Without these, every assertion above is satisfiable by a resolver that returns
    None for everything — which is precisely how an isolation test passes while being
    blind. Each mirrors one test above with the audience changed and nothing else."""

    @pytest.mark.asyncio
    async def test_an_ordinary_flow_in_the_same_shape_DOES_win(self):
        winner = await resolve_flow(
            _redis_with_lists_and_hashes(
                {"campaign:905:flows": ["1"]},
                {"flow:1": _make_flow(fid="1", campaign_id="905", audience="first")},
            ),
            campaign_id="905", click_attrs=_CLICK, **_SCOPE,
        )
        assert winner is not None
        assert winner["_id"] == "1"

    @pytest.mark.asyncio
    async def test_a_global_ordinary_flow_in_the_same_scope_list_DOES_win(self):
        winner = await resolve_flow(
            _redis_with_lists_and_hashes(
                {"campaign:906:flows": [], "flows:scope:1:company:1": ["7"]},
                {"flow:7": _make_flow(fid="7", campaign_id="0", audience="first")},
            ),
            campaign_id="906", click_attrs=_CLICK, **_SCOPE,
        )
        assert winner is not None
        assert winner["_id"] == "7"
