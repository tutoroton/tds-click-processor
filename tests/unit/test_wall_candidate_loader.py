"""The missing link: the node READS the wall keyspace (G5 prerequisite).

`sync/builders/flows.py` publishes walls into `campaign:{id}:walls` and
`walls:scope:{company}:{type}:{id}` (#4024, on `stage`). `select_wall` picks among
candidates someone else fetched. Until `load_wall_candidates` existed, **nothing on this
node read those keys**, so the two halves could not meet — a grep for `walls` across
`click-processor/app/` returned only `offerwall.py`'s own docstring.

WHAT THESE TESTS ARE FOR, in order of how much they would cost if absent:

1. **Isolation on the READ side.** The publisher keeps wall ids out of the routing lists.
   That is a WRITE-side property. A loader that "helpfully" also read
   `campaign:{id}:flows` would re-open the same hole from the other direction, and no
   publisher test could see it. So the first class asserts which keys are asked for.

2. **The budget is TILES, not walls** (risk A19). `35-G4-WALL-CONTRACT.md` says it
   outright: *"the work unit is buckets x candidates x tiles, not the winner's tiles"*.
   A cap on the number of walls bounds the wrong quantity.

3. **Truncation is visible.** A capacity decision an operator cannot see gets mistaken
   for a missing wall, and then for a broken feature.
"""

from __future__ import annotations

import json
import logging
from unittest.mock import MagicMock

import pytest

from app.offerwall import MAX_WALL_TILES_PER_REQUEST, load_wall_candidates, select_wall

from .test_cascade import _make_flow

_CLICK = {"geo": "UA", "device": "desktop"}
_SCOPE = dict(company_id=7, buyer_id=None, team_id=None,
              department_id=None, custom_group_id=None)


def _wall(fid: str, *, tiles: int = 2, campaign_id: str = "0", **over) -> dict:
    """A wall record in the shape the PUBLISHER emits (Redis strings throughout)."""
    rec = _make_flow(fid=fid, campaign_id=campaign_id, audience="offerwall",
                     action_type="offerwall", **over)
    rec["action_config"] = json.dumps(
        {"tiles": [{"offer_id": i, "target_id": i} for i in range(tiles)]})
    return rec


def _redis(lists: dict[str, list[str]], hashes: dict[str, dict]) -> tuple[MagicMock, list]:
    """Mock Redis that RECORDS which keys were asked for.

    The recording is the point: the strongest thing these tests assert is a
    NEGATIVE — that no routing key is ever read — and that cannot be checked by
    looking at the result.
    """
    asked: list[str] = []

    class FakePipeline:
        def __init__(self):
            self._ops: list[tuple[str, str]] = []

        def lrange(self, key, _start, _end):
            asked.append(key)
            self._ops.append(("lrange", key))

        def hgetall(self, key):
            asked.append(key)
            self._ops.append(("hgetall", key))

        async def execute(self):
            out = []
            for op, key in self._ops:
                out.append(list(lists.get(key, [])) if op == "lrange"
                           else dict(hashes.get(key, {})))
            return out

    r = MagicMock()
    r.pipeline = lambda: FakePipeline()
    return r, asked


class TestItReadsTheWallKeyspaceAndOnlyThat:
    """🔴 The isolation, from the READ side. The publisher's guarantee is about what it
    WRITES; this is the other half, and nothing else covers it."""

    @pytest.mark.asyncio
    async def test_no_routing_list_is_ever_read(self):
        r, asked = _redis(
            {"campaign:900:walls": ["1"], "walls:scope:7:company:7": ["2"]},
            {"flow:1": _wall("1", campaign_id="900"), "flow:2": _wall("2")},
        )
        await load_wall_candidates(r, campaign_id="900", click_attrs=_CLICK, **_SCOPE)
        routing = [k for k in asked if k.endswith(":flows") or k.startswith("flows:scope:")]
        assert routing == [], (
            f"the loader touched routing keys {routing} — routing candidates are "
            f"tail-capped BEFORE the audience partition, so sharing that read is the "
            f"eviction hazard 35-G4-WALL-CONTRACT.md 4.1 reproduces"
        )

    @pytest.mark.asyncio
    async def test_it_asks_for_the_campaign_wall_list_and_every_scope_rung(self):
        r, asked = _redis({}, {})
        await load_wall_candidates(
            r, campaign_id="901", company_id=7, buyer_id=5, team_id=3,
            department_id=2, custom_group_id=4, click_attrs=_CLICK)
        assert "campaign:901:walls" in asked
        for scope_type, sid in (("buyer", 5), ("custom_group", 4), ("team", 3),
                                ("department", 2), ("company", 7)):
            assert f"walls:scope:7:{scope_type}:{sid}" in asked, scope_type

    @pytest.mark.asyncio
    async def test_a_scope_rung_with_no_id_is_not_asked_for(self):
        """`walls:scope:7:buyer:None` is meaningless and would be one wasted round trip
        per absent rung, on the hot path, forever."""
        r, asked = _redis({}, {})
        await load_wall_candidates(r, campaign_id="902", click_attrs=_CLICK, **_SCOPE)
        assert not [k for k in asked if k.endswith("None")]

    @pytest.mark.asyncio
    async def test_no_scope_list_is_read_when_the_company_is_unknown(self):
        """Without a company there is no tenant to scope to, and
        `walls:scope:None:…` would be a cross-tenant-shaped key. Only the campaign
        list is legitimate."""
        r, asked = _redis({}, {})
        await load_wall_candidates(
            r, campaign_id="903", company_id=None, buyer_id=None, team_id=None,
            department_id=None, custom_group_id=None, click_attrs=_CLICK)
        assert [k for k in asked if k.startswith("walls:scope:")] == []
        assert "campaign:903:walls" in asked


class TestTheBudgetIsCountedInTILES:
    """Risk A19. A cap on the number of WALLS bounds the wrong quantity."""

    @pytest.mark.asyncio
    async def test_many_small_walls_all_fit(self):
        """20 walls x 2 tiles = 40 tiles — far under the ceiling. A wall-COUNT cap of,
        say, 10 would have dropped half of them for no cost reason."""
        ids = [str(i) for i in range(1, 21)]
        r, _ = _redis({"campaign:910:walls": ids},
                      {f"flow:{i}": _wall(i, tiles=2, campaign_id="910") for i in ids})
        walls, stats = await load_wall_candidates(
            r, campaign_id="910", click_attrs=_CLICK, **_SCOPE)
        assert len(walls) == 20
        assert stats["tiles"] == 40
        assert stats["truncated"] is False

    @pytest.mark.asyncio
    async def test_few_FAT_walls_are_truncated(self):
        """The mirror. 30 walls x 24 tiles = 720 tiles, over the 480 ceiling — so the
        SAME wall count that passed above is refused here. That difference IS the
        argument for counting tiles."""
        ids = [str(i) for i in range(1, 31)]
        r, _ = _redis({"campaign:911:walls": ids},
                      {f"flow:{i}": _wall(i, tiles=24, campaign_id="911") for i in ids})
        walls, stats = await load_wall_candidates(
            r, campaign_id="911", click_attrs=_CLICK, **_SCOPE)
        assert stats["truncated"] is True
        assert stats["tiles"] <= MAX_WALL_TILES_PER_REQUEST
        assert 0 < len(walls) < 30

    @pytest.mark.asyncio
    async def test_a_single_oversized_wall_is_still_admitted(self):
        """Fail-USEFUL. If one wall alone exceeds the budget, admitting nothing would
        turn a capacity ceiling into a total outage for that campaign. The `and admitted`
        term in the loop is what makes this true; without it this test goes red."""
        r, _ = _redis({"campaign:912:walls": ["1"]},
                      {"flow:1": _wall("1", tiles=9999, campaign_id="912")})
        walls, stats = await load_wall_candidates(
            r, campaign_id="912", click_attrs=_CLICK, **_SCOPE)
        assert len(walls) == 1
        assert stats["truncated"] is False

    @pytest.mark.asyncio
    async def test_an_UNREADABLE_wall_costs_a_FULL_wall_not_zero(self):
        """Fail-EXPENSIVE on purpose. If a corrupt `action_config` counted as 0 tiles, a
        corrupt row would buy itself unlimited admission by being corrupt — the budget
        would have a hole shaped exactly like the thing it protects against."""
        ids = [str(i) for i in range(1, 41)]
        r, _ = _redis({"campaign:913:walls": ids},
                      {f"flow:{i}": {**_wall(i, campaign_id="913"),
                                     "action_config": "{not json"} for i in ids})
        _, stats = await load_wall_candidates(
            r, campaign_id="913", click_attrs=_CLICK, **_SCOPE)
        assert stats["truncated"] is True, (
            "40 unreadable walls must exhaust the budget, not slip through it free"
        )


class TestTruncationIsVISIBLE:
    @pytest.mark.asyncio
    async def test_it_warns_when_it_drops_walls(self, caplog):
        """A capacity decision an operator cannot see gets read as a missing wall, and
        then as a broken feature. The warning is the difference between the two."""
        ids = [str(i) for i in range(1, 31)]
        r, _ = _redis({"campaign:914:walls": ids},
                      {f"flow:{i}": _wall(i, tiles=24, campaign_id="914") for i in ids})
        with caplog.at_level(logging.WARNING, logger="tds.offerwall"):
            await load_wall_candidates(r, campaign_id="914", click_attrs=_CLICK, **_SCOPE)
        assert any("truncated" in rec.message for rec in caplog.records)

    @pytest.mark.asyncio
    async def test_it_stays_QUIET_when_nothing_is_dropped(self):
        """Calibration. A warning that fires on every request is a warning nobody reads —
        the same reasoning A27 applied one plane up."""
        r, _ = _redis({"campaign:915:walls": ["1"]},
                      {"flow:1": _wall("1", campaign_id="915")})
        import logging as _l
        recs: list[_l.LogRecord] = []

        class _Cap(_l.Handler):
            def emit(self, record): recs.append(record)

        lg = _l.getLogger("tds.offerwall")
        h = _Cap()
        lg.addHandler(h)
        try:
            await load_wall_candidates(r, campaign_id="915", click_attrs=_CLICK, **_SCOPE)
        finally:
            lg.removeHandler(h)
        assert [r for r in recs if r.levelno >= _l.WARNING] == []


class TestEligibilityAndTheHandoffToSelectWall:
    @pytest.mark.asyncio
    async def test_criteria_are_applied_by_the_ROUTING_matcher(self):
        """Reused, never re-implemented: a wall's targeting must behave EXACTLY as a
        flow's, or two systems drift on what `geo IN (UA)` means."""
        r, _ = _redis(
            {"campaign:920:walls": ["1", "2"]},
            {
                "flow:1": _wall("1", campaign_id="920",
                                criteria=[{"type": "geo", "operator": "in",
                                           "values": ["UA"]}]),
                "flow:2": _wall("2", campaign_id="920",
                                criteria=[{"type": "geo", "operator": "in",
                                           "values": ["PL"]}]),
            },
        )
        walls, stats = await load_wall_candidates(
            r, campaign_id="920", click_attrs=_CLICK, **_SCOPE)
        assert [w["_id"] for w in walls] == ["1"]
        assert stats["loaded"] == 2 and stats["eligible"] == 1

    @pytest.mark.asyncio
    async def test_its_output_feeds_select_wall_unchanged(self):
        """The handoff, end to end on this node: loader -> select_wall, no adaptation
        layer in between. If a shim were ever needed, that shim is where the Redis-shaped
        contract would rot."""
        # 🔴 `scope_id=7` is load-bearing, not decoration. `_make_flow` defaults it to
        # 1, and `_pick_winner` matches a wall's scope against the CLICK's hierarchy —
        # so a wall at scope_id=1 is simply not in a company-7 click's scope and the
        # picker correctly returns None. The first run of this test failed exactly
        # there, and the failure was about the fixture, not the loader: every other
        # test here passes because the loader itself never consults scope.
        r, _ = _redis(
            {"campaign:921:walls": ["1"], "walls:scope:7:company:7": ["2"]},
            {"flow:1": _wall("1", campaign_id="921", scope_id=7),
             "flow:2": _wall("2", scope_id=7)},
        )
        walls, _ = await load_wall_candidates(
            r, campaign_id="921", click_attrs=_CLICK, **_SCOPE)
        winner = select_wall(walls, {"buyer": None, "custom_group": None, "team": None,
                                     "department": None, "company": 7})
        assert winner is not None
        assert winner["_id"] == "1", "campaign-bound beats global (2.1)"

    @pytest.mark.asyncio
    async def test_a_wall_listed_in_BOTH_places_is_counted_once(self):
        """The publisher's invariant is EITHER/OR, so a duplicate here is sync drift.
        Charging it twice against the budget would let drift shrink capacity."""
        r, _ = _redis(
            {"campaign:922:walls": ["1"], "walls:scope:7:company:7": ["1"]},
            {"flow:1": _wall("1", campaign_id="922")},
        )
        walls, stats = await load_wall_candidates(
            r, campaign_id="922", click_attrs=_CLICK, **_SCOPE)
        assert len(walls) == 1
        assert stats["loaded"] == 1

    @pytest.mark.asyncio
    async def test_no_walls_anywhere_is_an_empty_set_not_an_error(self):
        """Every campaign that has no wall takes this path on every preview. It must be
        cheap and quiet: no HGETALL, no exception."""
        r, asked = _redis({}, {})
        walls, stats = await load_wall_candidates(
            r, campaign_id="923", click_attrs=_CLICK, **_SCOPE)
        assert walls == [] and stats["loaded"] == 0
        assert not [k for k in asked if k.startswith("flow:")], (
            "an empty candidate set must not issue a single HGETALL"
        )
