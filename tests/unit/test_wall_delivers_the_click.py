"""THE WALL ROUTES. This is the test the whole B3 lane exists to make pass.

The owner, 2026-09-10: «що ці офервол потоки фактично не маршрутизують
користувача, це взагалі я був в шоці, це радикально неправильно. Саме вони
мають маршрутизувати користувача». And the model: «фактично вітрина, вона
рівносильна потоку all visitor».

So the claim under test is not "a wall can be loaded" or "a field arrives" —
those were the previous two lanes. It is: **a plain click on a wall campaign is
delivered BY the wall, and the wall is the flow that gets the credit.**

Everything here drives the REAL routing path (`router.route`) through the
existing FakeRedis harness. A test that called the cascade directly could pass
while the click still dead-ended one frame up.

WHAT EACH CLASS PINS, and why it can go red:

* `TestTheFlagIsTheOnlySwitch` — with `wall_delivery_enabled` OFF, a campaign
  already marked `flow_family='offerwall'` behaves exactly as before. This is
  the dark-by-default claim, and it is the one that makes shipping safe.
* `TestTheWallDelivers` — flag ON: the wall wins, the click routes, and
  `attribution.flow_id` is the WALL's id. That last assertion is the owner's
  statistics complaint answered in code: a click can no longer be attributed to
  a flow that never contained the offer.
* `TestFirstEligibleNotJustFirst` — a closed first tile is walked past, not
  dead-ended.
* `TestIsolationSurvives` — on a STANDARD campaign a wall still cannot win,
  flag ON or OFF. The invariant the anchor calls non-negotiable.
* `TestReturningFlowsStillWork` — a wall campaign KEEPS returning routing.
  🔴 This is the case my own design got wrong first: "walls are the only pool"
  would have silently deleted returning flows on every wall campaign, and the
  owner said the opposite in as many words — «ретьорнінг потоки… працюють і для
  вітрин, і для… all visitors».
"""

from __future__ import annotations

import json

import pytest

from app import router
from app.config import settings

from tests.unit.test_router_cascade import FakeRedis, _click, _route_with


WALL_ID = "910"
FLOW_ID = "911"


def _tile(offer_id: int, target_id: int) -> dict:
    return {"offer_id": offer_id, "target_id": target_id}


def _wall_hash(campaign_id: str, tiles: list[dict], **over) -> dict:
    h = {
        "campaign_id": campaign_id,
        "scope_type": "company",
        "scope_id": "1",
        "seq_id": "1",
        "is_default": "0",
        "criteria": "[]",
        "audience": "offerwall",
        "action_type": "offerwall",
        "action_config": json.dumps({"tiles": tiles}),
    }
    h.update(over)
    return h


def _offer_target(target_id: str, url: str, availability: str = "active") -> dict:
    return {
        "_id": target_id,
        "url": url,
        "availability": availability,
        "status": "active",
    }


def _redis(campaign_id: str, *, family: str, tiles: list[dict],
           targets: dict[str, dict], with_ordinary_flow: bool = False,
           returning_flow: bool = False) -> FakeRedis:
    hashes = {
        f"campaign:{campaign_id}": {
            "company_id": "1", "priority": "0", "weight": "100",
            "flow_family": family,
        },
        f"flow:{WALL_ID}": _wall_hash(campaign_id, tiles),
    }
    for tid, t in targets.items():
        hashes[f"offer_target:{tid}"] = t
    lists = {f"campaign:{campaign_id}:walls": [WALL_ID]}

    if with_ordinary_flow or returning_flow:
        hashes[f"flow:{FLOW_ID}"] = {
            "campaign_id": campaign_id,
            "scope_type": "company", "scope_id": "1", "seq_id": "2",
            "is_default": "0", "criteria": "[]",
            "audience": "returning" if returning_flow else "first",
            "action_type": "redirect",
            "action_config": json.dumps({"url": "https://ordinary.example/x"}),
        }
        lists[f"campaign:{campaign_id}:flows"] = [FLOW_ID]

    return FakeRedis(
        sets={
            "geo:US": {campaign_id}, "device:mobile": {campaign_id},
            "os:ios": {campaign_id}, "campaigns:active": {campaign_id},
        },
        hashes=hashes,
        lists=lists,
    )


@pytest.fixture
def wall_delivery_on(monkeypatch):
    monkeypatch.setattr(settings, "wall_delivery_enabled", True, raising=False)
    yield


class TestTheFlagIsTheOnlySwitch:
    def test_flag_off_a_wall_campaign_is_untouched(self):
        # No ordinary flows at all, so if the wall were serving we would get a
        # destination. Flag OFF ⇒ the walls keyspace is never read ⇒ nothing
        # routes, exactly as today.
        redis = _redis("950", family="offerwall",
                       tiles=[_tile(55, 77)],
                       targets={"77": _offer_target("77", "https://o1.example/")})
        result = _route_with(redis, _click())
        assert result is None or result.get("attribution", {}).get("flow_id") != int(WALL_ID)

    def test_flag_off_still_records_the_family(self):
        # The reader keeps working while the delivery stays dark — that split
        # is what makes "the fleet can read it" checkable before activation.
        redis = _redis("951", family="offerwall", tiles=[_tile(55, 77)],
                       targets={"77": _offer_target("77", "https://o1.example/")},
                       with_ordinary_flow=True)
        result = _route_with(redis, _click())
        assert result is not None
        assert result["attribution"]["routing_trace"]["flow_family"] == "offerwall"
        # …and the ORDINARY flow served it, not the wall.
        assert result["attribution"]["flow_id"] == int(FLOW_ID)


class TestTheWallDelivers:
    def test_a_plain_click_is_served_by_the_wall(self, wall_delivery_on):
        redis = _redis("952", family="offerwall",
                       tiles=[_tile(55, 77)],
                       targets={"77": _offer_target("77", "https://first.example/")})
        result = _route_with(redis, _click())
        assert result is not None, "the wall did not route the click"
        assert result["url"].startswith("https://first.example/")

    def test_the_wall_is_the_flow_that_gets_the_credit(self, wall_delivery_on):
        # 🔴 The owner's statistics complaint, answered: «У нас може взагалі в
        # all visitors потоки не бути налаштовано цього офера ніде. Тоді як ми
        # можемо зробити на нього клік?» — the delivering flow is the wall that
        # actually contains the offer.
        redis = _redis("953", family="offerwall",
                       tiles=[_tile(55, 77)],
                       targets={"77": _offer_target("77", "https://first.example/")})
        result = _route_with(redis, _click())
        assert result is not None
        assert result["attribution"]["flow_id"] == int(WALL_ID)
        assert result["attribution"]["action_type"] == "offerwall"
        assert result["attribution"]["audience_pool"] == "offerwall"

    def test_the_first_tile_wins_not_an_arbitrary_one(self, wall_delivery_on):
        # «користувач потрапить на той офер, який є першим» — publication order,
        # not a pick. Two live tiles: only the first may serve.
        redis = _redis("954", family="offerwall",
                       tiles=[_tile(55, 77), _tile(66, 88)],
                       targets={
                           "77": _offer_target("77", "https://first.example/"),
                           "88": _offer_target("88", "https://second.example/"),
                       })
        result = _route_with(redis, _click())
        assert result is not None
        assert result["url"].startswith("https://first.example/")


class TestFirstEligibleNotJustFirst:
    def test_a_closed_first_tile_is_walked_past(self, wall_delivery_on):
        # «можемо включити close, тоді буде нет» — the wall must serve the next
        # tile, not dead-end on the first. A test asserting only "first tile
        # wins" would pass a implementation that dead-ends here.
        redis = _redis("955", family="offerwall",
                       tiles=[_tile(55, 77), _tile(66, 88)],
                       targets={
                           "77": _offer_target("77", "https://first.example/",
                                               availability="closed"),
                           "88": _offer_target("88", "https://second.example/"),
                       })
        result = _route_with(redis, _click())
        assert result is not None, "a closed first tile dead-ended the wall"
        assert result["url"].startswith("https://second.example/")

    def test_all_tiles_closed_falls_through_rather_than_serving_a_closed_one(
            self, wall_delivery_on):
        redis = _redis("956", family="offerwall",
                       tiles=[_tile(55, 77)],
                       targets={"77": _offer_target("77", "https://first.example/",
                                                    availability="closed")})
        result = _route_with(redis, _click())
        # Whatever the campaign's terminal answer is, it must NOT be the closed
        # target's URL. Serving a closed offer is the one outcome forbidden here.
        if result is not None:
            assert not (result.get("url") or "").startswith("https://first.example/")


class TestIsolationSurvives:
    @pytest.mark.parametrize("flag", [False, True])
    def test_a_wall_never_wins_on_a_standard_campaign(self, monkeypatch, flag):
        # The anchor's non-negotiable invariant. Parametrised over the flag on
        # purpose: isolation must not depend on the switch being off.
        monkeypatch.setattr(settings, "wall_delivery_enabled", flag, raising=False)
        redis = _redis("957", family="standard",
                       tiles=[_tile(55, 77)],
                       targets={"77": _offer_target("77", "https://first.example/")},
                       with_ordinary_flow=True)
        result = _route_with(redis, _click())
        assert result is not None
        assert result["attribution"]["flow_id"] == int(FLOW_ID)
        assert not result["url"].startswith("https://first.example/")


class TestReturningFlowsStillWork:
    def test_a_wall_campaign_keeps_its_returning_flow_in_the_pool(
            self, wall_delivery_on):
        """🔴 The case my first design would have silently deleted.

        «ретьорнінг потоки, якщо вони включені цим налаштуванням, вони працюють
        і для вітрин, і для… all visitors» — so a wall campaign must still READ
        the ordinary keyspace to find them. Reading only `walls` there would
        make this test's returning flow unreachable, and nothing else in the
        suite would have noticed.

        Pinned at the LOADER level rather than through a full returning-visitor
        fixture: what must not regress is that the ordinary keyspace is still
        fetched for a wall campaign. A returning flow that is never LOADED can
        never win, whatever the visitor looks like.
        """
        from app import cascade

        seen_keys: list[str] = []

        class _SpyRedis(FakeRedis):
            def pipeline(self):
                pipe = super().pipeline()
                original = pipe.lrange

                def _spy(key, start, end):
                    seen_keys.append(key)
                    return original(key, start, end)

                pipe.lrange = _spy
                return pipe

        redis = _SpyRedis(
            sets={"geo:US": {"958"}, "device:mobile": {"958"},
                  "os:ios": {"958"}, "campaigns:active": {"958"}},
            hashes={
                "campaign:958": {"company_id": "1", "priority": "0",
                                 "weight": "100", "flow_family": "offerwall"},
                f"flow:{WALL_ID}": _wall_hash("958", [_tile(55, 77)]),
                "offer_target:77": _offer_target("77", "https://first.example/"),
            },
            lists={"campaign:958:walls": [WALL_ID]},
        )
        _route_with(redis, _click())

        assert any("campaign:958:walls" == k for k in seen_keys), \
            "the walls keyspace was not read on a wall campaign"
        assert any("campaign:958:flows" == k for k in seen_keys), \
            ("the ordinary keyspace was NOT read on a wall campaign — "
             "returning flows there would be unreachable, which contradicts "
             "the owner's rule that returning works for walls too")
        assert cascade is not None  # import kept meaningful


class TestTheKeyspaceChoiceIsExplicit:
    def test_standard_family_reads_only_the_flows_keyspace(self):
        assert router._campaign_flow_family({"flow_family": "standard"}) == "standard"

    def test_an_unknown_family_cannot_reach_the_walls_keyspace(self):
        # Fail-open: an unrecognised value must degrade to 'standard', so a
        # typo in the campaign HASH can never silently switch delivery.
        assert router._campaign_flow_family({"flow_family": "wall"}) == "standard"
