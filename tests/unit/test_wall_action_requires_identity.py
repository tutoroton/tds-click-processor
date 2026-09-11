"""A wall ACTION requires a wall IDENTITY — and the dark flag must reach here.

🔴 WHAT WAS MEASURED, 2026-09-11, before either guard existed. A flow carrying

    audience     = 'first'        (an ordinary, legitimate audience)
    action_type  = 'offerwall'    (the wall action)

on a **STANDARD** campaign served a redirect to the wall's first tile — with
`wall_delivery_enabled` **OFF**:

    wall_delivery_enabled=False -> url='https://TILE-ONE/' flow_id=920
    wall_delivery_enabled=True  -> url='https://TILE-ONE/' flow_id=920

Two separate defects sit in that one line.

**1. The flag did not reach the executor.** Selection is gated in the router —
which keyspaces are read — and the EXECUTOR was not gated at all. So "dark by
default" was a true statement about the selector and a false one about the
system: any flow that arrived here by another route served tiles regardless of
the flag. Before the dispatcher, `action_type='offerwall'` had no branch and
fell through to the unknown-action fallback; that is exactly what flag-OFF must
keep doing, or "flag OFF is byte-identical to before" is a hope, not a claim.

**2. The action did not require the identity.** `(first, offerwall)` is a real
misconfiguration this design documented before delivery existed — no audience
guard catches it, because `first` IS legitimate. What the dispatcher changed is
its CONSEQUENCE: from "renders a catalogue where a 302 was expected" to
"silently routes to tile 1 on a standard campaign", which is the isolation
invariant bypassed by CONFIGURATION rather than by code. Isolation that holds
only for correctly-configured flows is not an invariant.

Both refusals fall through to the unknown-action path, which logs and returns
None so the click still reaches the ordinary fallback. Refusing loudly would
page on an operator's typo; refusing silently would hide it.

EVERY case here is paired with the POSITIVE control — the legitimate wall,
which must keep serving. A guard that also blocks the real path is not a guard,
it is an outage.
"""

from __future__ import annotations

import json

import pytest

from app import router
from app.config import settings

from tests.unit.test_router_cascade import FakeRedis, _click, _route_with


BAD_FLOW = "920"      # audience='first' + action_type='offerwall'
GOOD_WALL = "921"     # audience='offerwall' + action_type='offerwall'
TILE = "111"


def _flow(flow_id: str, campaign_id: str, audience: str) -> dict:
    return {
        "campaign_id": campaign_id,
        "scope_type": "company", "scope_id": "1",
        "seq_id": "1", "is_default": "0", "criteria": "[]",
        "audience": audience,
        "action_type": "offerwall",
        "action_config": json.dumps(
            {"tiles": [{"offer_id": 55, "target_id": int(TILE)}]}),
    }


def _redis(campaign_id: str, *, family: str, audience: str,
           in_walls_keyspace: bool) -> FakeRedis:
    """`in_walls_keyspace` decides HOW the flow reaches the executor.

    A wall published as a wall lands in `campaign:{id}:walls`; the
    misconfigured flow lands in the ordinary `:flows` list, which is precisely
    why no audience guard sees it.
    """
    flow_id = GOOD_WALL if audience == "offerwall" else BAD_FLOW
    lists = {
        (f"campaign:{campaign_id}:walls" if in_walls_keyspace
         else f"campaign:{campaign_id}:flows"): [flow_id],
    }
    return FakeRedis(
        sets={"geo:US": {campaign_id}, "device:mobile": {campaign_id},
              "os:ios": {campaign_id}, "campaigns:active": {campaign_id}},
        hashes={
            f"campaign:{campaign_id}": {
                "company_id": "1", "priority": "0", "weight": "100",
                "flow_family": family,
            },
            f"flow:{flow_id}": _flow(flow_id, campaign_id, audience),
            f"offer_target:{TILE}": {
                "_id": TILE, "url": "https://TILE-ONE/",
                "availability": "active", "status": "active",
            },
        },
        lists=lists,
    )


def _url(result) -> str:
    return (result or {}).get("url") or ""


class TestTheFlagReachesTheExecutor:
    @pytest.mark.parametrize("flag", [False, True])
    def test_a_first_audience_flow_never_serves_tiles(self, monkeypatch, flag):
        """The measured defect, both flag states.

        Parametrised deliberately: after the identity guard this must hold with
        the flag ON too, and a test that only checked flag-OFF would let the
        isolation bypass back in the moment the flag is armed.
        """
        monkeypatch.setattr(settings, "wall_delivery_enabled", flag, raising=False)
        redis = _redis("980", family="standard", audience="first",
                       in_walls_keyspace=False)
        result = _route_with(redis, _click())
        assert not _url(result).startswith("https://TILE-ONE/"), (
            f"a (first, offerwall) flow served a wall tile with the flag {flag} "
            "— on a STANDARD campaign, which is the isolation invariant "
            "bypassed by configuration"
        )

    def test_flag_OFF_refuses_even_a_legitimate_wall(self, monkeypatch):
        """Dark by default, end to end — not just in the selector."""
        monkeypatch.setattr(settings, "wall_delivery_enabled", False, raising=False)
        redis = _redis("981", family="offerwall", audience="offerwall",
                       in_walls_keyspace=True)
        result = _route_with(redis, _click())
        assert not _url(result).startswith("https://TILE-ONE/"), (
            "a wall served tiles with wall_delivery_enabled OFF"
        )


class TestThePositiveControlStillServes:
    """🔴 The half that makes the guards evidence rather than an outage."""

    def test_a_real_wall_on_a_wall_campaign_still_delivers(self, monkeypatch):
        monkeypatch.setattr(settings, "wall_delivery_enabled", True, raising=False)
        redis = _redis("982", family="offerwall", audience="offerwall",
                       in_walls_keyspace=True)
        result = _route_with(redis, _click())
        assert _url(result).startswith("https://TILE-ONE/"), (
            "POSITIVE CONTROL FAILED — the guards blocked the legitimate wall "
            "path. A guard that also blocks the real path is an outage, and "
            "every refusal test above is meaningless without this one passing"
        )
        assert result["attribution"]["flow_id"] == int(GOOD_WALL)


class TestTheTwoGuardsAreDISTINCT:
    """Each guard must be able to fire alone, or one is dead weight.

    Flag OFF + wrong identity would be refused by either; the discriminating
    cases are flag OFF + RIGHT identity (only the flag can refuse) and flag ON +
    WRONG identity (only the identity can).
    """

    def test_only_the_flag_can_refuse_a_correct_wall(self, monkeypatch):
        monkeypatch.setattr(settings, "wall_delivery_enabled", False, raising=False)
        redis = _redis("983", family="offerwall", audience="offerwall",
                       in_walls_keyspace=True)
        assert not _url(_route_with(redis, _click())).startswith("https://TILE-ONE/")

    def test_only_the_identity_can_refuse_an_armed_misconfiguration(self, monkeypatch):
        monkeypatch.setattr(settings, "wall_delivery_enabled", True, raising=False)
        redis = _redis("984", family="standard", audience="first",
                       in_walls_keyspace=False)
        assert not _url(_route_with(redis, _click())).startswith("https://TILE-ONE/")


# ============================================================
# The flag guard, tested where it is actually REACHABLE
# ============================================================

class TestTheFlagGuardAtTheExecutorContract:
    """🔴 WHY THIS ENTERS AT `execute_action` AND NOT AT THE ROUTER.

    Mutation-tested 2026-09-11: removing the executor's flag guard left every
    router-level test above GREEN. Not because the guard is wrong — because the
    ROUTER's own gate shadows it. With `wall_delivery_enabled` OFF the router
    never reads the walls keyspace, so a wall never becomes a candidate and the
    executor is never reached with one.

    That makes the executor guard **unreachable through the router today**, and
    a guard nothing can discriminate is indistinguishable from dead code by any
    test that only drives the router. So it is tested where its contract lives:
    `execute_action` is a public function, callable by anything, and its own
    promise is "a wall action does not serve while wall delivery is dark".

    KEPT rather than deleted, deliberately: the reason it cannot fire is a
    property of a DIFFERENT component. If the router ever loads walls for some
    other purpose — a preview, a diagnostic, a future family — this is the line
    that keeps the flag meaning what it says. The honest statement is that it is
    defence in depth with no reachable path through the router today, which is
    exactly what this class pins.
    """

    @staticmethod
    def _wall_flow() -> dict:
        return {
            "_id": GOOD_WALL,
            "audience": "offerwall",
            "action_type": "offerwall",
            "action_config": json.dumps(
                {"tiles": [{"offer_id": 55, "target_id": int(TILE)}]}),
        }

    def _run(self, flag: bool):
        import asyncio
        import fakeredis.aioredis
        from app import action_executor
        from app.models import ClickRequest

        async def _inner():
            r = fakeredis.aioredis.FakeRedis(decode_responses=True)
            await r.hset("offer_target:%s" % TILE, mapping={
                "url": "https://TILE-ONE/", "availability": "active",
                "is_default": "0", "offer_id": "55",
                "criteria": "[]", "priority": "0",
            })
            return await action_executor.execute_action(
                r, self._wall_flow(),
                ClickRequest(click_id="ex" + "0" * 19, country="US",
                             user_agent="t/1.0"),
                "990",
                source_mappings=None, campaign_mappings=None,
                build_url_fn=lambda tpl, *a, **k: tpl,
            )

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(settings, "wall_delivery_enabled", flag, raising=False)
            return asyncio.run(_inner())

    def test_flag_OFF_the_executor_refuses_a_correct_wall_action(self):
        result = self._run(False)
        assert result is None or not (result.get("url") or "").startswith(
            "https://TILE-ONE/"), (
            "execute_action served wall tiles with wall_delivery_enabled OFF — "
            "the flag does not reach the point where serving happens"
        )

    def test_CONTROL_flag_ON_the_same_call_serves(self):
        """Without this the refusal above passes on a broken fixture that could
        never have served anything."""
        result = self._run(True)
        assert (result or {}).get("url", "").startswith("https://TILE-ONE/"), (
            "CONTROL FAILED — this fixture cannot serve even with the flag ON, "
            "so the refusal above is not evidence about the flag"
        )
