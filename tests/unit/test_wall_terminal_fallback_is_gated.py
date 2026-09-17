"""F2i step 2 — the wall fall-through switches to terminal_fallback, GATED.

WHAT THE GATE IS, and why it is the whole design. ADR-0561 ruled that (b) —
serving the campaign's own terminal_fallback — is the correct semantics AND
CANNOT SHIP UNGATED, because it was measured first: all EIGHT offerwall-family
campaigns have `fallback_url` NULL (214 of 285 campaigns overall do), and
ungated this would have converted 290 clicks across 29 countries in one week, on
campaign 347 alone, from "an offer chosen badly" into "no offer at all".

So the subject of this file is not "does it serve the fallback". It is
**"does it serve the fallback ONLY when there IS one"** — and three of the four
arms exist to prove the gate refuses, not that it fires.

    wall · ON · fallback SET      the switch. The subject.
    wall · ON · NO fallback       🔴 THE GATE CONTROL. Unchanged behaviour.
                                  If this arm switched, the ruling's whole
                                  measurement would be defeated and every wall
                                  campaign on today's fleet would serve a dead
                                  end, because not one of them has a fallback.
    standard · ON · fallback SET  CONTROL — proves the gate keys on the FAMILY
                                  as well. Without it, "wall campaigns switch"
                                  is indistinguishable from "any campaign with
                                  a fallback stops using the legacy split",
                                  which would change 71 campaigns' behaviour.
    wall · OFF · fallback SET     CONTROL — the dark flag. With
                                  `wall_delivery_enabled` off the node acts on
                                  every campaign as standard, so this
                                  fall-through is the ordinary pre-existing one
                                  and must not be touched.

🔴 A CORRECTION THIS LANE OWES, and it is measured rather than argued.
`test_wall_fall_through_is_labelled.py`'s docstring predicted that its serve
assertions "MUST go red" in this lane. **They do not, and they should not.** That
prediction was written when step 2 was imagined UNGATED; the ruling added the
gate after measuring 8/8 NULL. Its fixture campaign has no `fallback_url`, so it
is the `wall · ON · NO fallback` arm above — the one whose behaviour is
deliberately unchanged. Its staying green is the gate working, and the whole
suite was diffed against an unmodified extract to confirm no test changed state.
That docstring is corrected in this same commit.
"""
from __future__ import annotations

import asyncio
from unittest.mock import patch

import fakeredis.aioredis

from app import identity as identity_mod
from app import router
from app import sticky as sticky_mod
from app.config import settings
from app.main import _decision_reason
from app.models import ClickRequest

from tests.unit.test_route_code_honoured import FakeIdentRedis
from tests.unit.test_wall_campaign_falls_to_legacy_split import (
    _CAMPAIGN,
    _UID,
    VISITOR_COUNTRY,
    _seed,
)

FALLBACK = "https://fallback.example/terminal"
SWITCHED = "terminal_fallback"
WALL_SPLIT = "wall_campaign_legacy_split"
STANDARD_SPLIT = "matched_legacy_split"


def _run(*, wall_family: bool, delivery: bool, fallback: bool):
    """The same seed as steps 0 and 1, with the fallback as a third knob."""
    ident = FakeIdentRedis(strings={})

    async def _gir():
        return ident

    async def _stamp(**kw):
        return identity_mod.IdentityResult(
            uid=_UID, is_returning=False, seen_before=False,
            signal_tier="cookie",
        )

    async def _inner():
        r = fakeredis.aioredis.FakeRedis(decode_responses=True)
        campaign = await _seed(r, wall_family=wall_family)
        if fallback:
            # On the campaign hash AND on the dict the router was handed —
            # `_route_via_campaign` reads the dict, and seeding only Redis
            # would make every arm look like "no fallback" and pass vacuously.
            await r.hset("campaign:%d" % _CAMPAIGN, "fallback_url", FALLBACK)
            campaign["fallback_url"] = FALLBACK
        req = ClickRequest(
            click_id="f2s" + "0" * 18, country=VISITOR_COUNTRY,
            user_agent="t/1.0", query_params={},
        )
        with patch.object(sticky_mod, "get_identity_redis", _gir), \
                patch.object(identity_mod, "resolve_and_stamp", _stamp), \
                patch.object(settings, "returning_resolver_enabled", True), \
                patch.object(settings, "wall_delivery_enabled", delivery):
            return await router._route_via_campaign(
                r, campaign, str(_CAMPAIGN), req, {}, result_label="matched",
            )

    return asyncio.run(_inner())


def _reason(res) -> str:
    return _decision_reason(res, res["timing"], res["attribution"])


def _trace(res) -> dict:
    return (res or {}).get("attribution", {}).get("routing_trace") or {}


class TestTheSwitchFires:
    def test_a_wall_with_a_fallback_serves_terminal_fallback(self):
        res = _run(wall_family=True, delivery=True, fallback=True)
        assert _reason(res) == SWITCHED, (
            f"expected {SWITCHED!r}, got {_reason(res)!r} — the wall campaign "
            "is still monetising through the criteria-free legacy split"
        )

    def test_it_stops_using_the_legacy_split_at_all(self):
        """The ROUTING change, not just the label. This is what makes it step 2."""
        res = _run(wall_family=True, delivery=True, fallback=True)
        assert res["timing"].get("route_via") != "legacy_split", (
            "the split still ran — step 2 must not merely relabel it"
        )
        assert res.get("fallback_url") == FALLBACK
        assert res.get("non_routed") is True

    def test_no_offer_is_served_from_the_excluded_pool(self):
        res = _run(wall_family=True, delivery=True, fallback=True)
        assert not res.get("offer_id"), (
            "an offer the routing layer already excluded was served anyway"
        )


class TestTheGATERefuses:
    """🔴 Three arms, and each could fire. The gate is the ruling."""

    def test_a_wall_WITHOUT_a_fallback_is_UNCHANGED(self):
        res = _run(wall_family=True, delivery=True, fallback=False)
        assert _reason(res) == WALL_SPLIT, (
            f"expected {WALL_SPLIT!r}, got {_reason(res)!r}. This is the arm "
            "every offerwall campaign on today's fleet is in — 8 of 8 have "
            "fallback_url NULL — so switching it would hand all of them a dead "
            "end, which is the degradation ADR-0561 measured and refused."
        )
        assert res["timing"].get("route_via") == "legacy_split"

    def test_a_STANDARD_campaign_with_a_fallback_is_UNCHANGED(self):
        res = _run(wall_family=False, delivery=True, fallback=True)
        assert _reason(res) == STANDARD_SPLIT, (
            f"expected {STANDARD_SPLIT!r}, got {_reason(res)!r} — the gate is "
            "keying on 'has a fallback' alone, so it would change behaviour "
            "for the 71 campaigns that have one"
        )
        assert res["timing"].get("route_via") == "legacy_split"

    def test_the_DARK_FLAG_still_disables_it(self):
        res = _run(wall_family=True, delivery=False, fallback=True)
        assert _reason(res) == STANDARD_SPLIT, (
            f"expected {STANDARD_SPLIT!r}, got {_reason(res)!r}. With wall "
            "delivery off the node acts on every campaign as standard, so this "
            "fall-through is the ordinary pre-existing one and the switch must "
            "be unreachable — otherwise the label reads what the campaign "
            "CLAIMS rather than what the node DID."
        )
        assert res["timing"].get("route_via") == "legacy_split"


class TestTheTRACETellsTheTruthAboutTheMECHANISM:
    """The reason names the OUTCOME; the trace must name the CAUSE."""

    def test_the_wall_key_is_set_only_on_the_switched_arm(self):
        on = _run(wall_family=True, delivery=True, fallback=True)
        assert _trace(on).get("wall_terminal_fallback") is True
        for arm in (
            _run(wall_family=True, delivery=True, fallback=False),
            _run(wall_family=False, delivery=True, fallback=True),
            _run(wall_family=True, delivery=False, fallback=True),
        ):
            assert "wall_terminal_fallback" not in _trace(arm), (
                "the discriminator is set on an arm that did not switch"
            )

    def test_it_does_NOT_claim_availability_excluded(self):
        """🔴 The honesty assertion.

        `terminal_fallback` is also produced by `availability_excluded`, which
        means the availability floor excluded the flows. That did NOT happen
        here — the wall admitted nobody. Reusing that flag to get the reason
        would make the row state a mechanism that never ran, which is the exact
        class of false statement step 1's comment refuses. Same reason, two
        causes, two keys.
        """
        res = _run(wall_family=True, delivery=True, fallback=True)
        assert not _trace(res).get("availability_excluded"), (
            "the switched arm is claiming the availability floor ran"
        )

    def test_routing_status_stays_in_its_existing_vocabulary(self):
        """No new member in ANY closed enum — that is what lets this ship.

        `decision_reason` lives in five places and is pinned by a parity test;
        `routing_status` has a vocabulary of its own. Step 2 reuses an existing
        member of both.
        """
        res = _run(wall_family=True, delivery=True, fallback=True)
        assert res.get("routing_status") == "no_offer"
        assert _reason(res) == SWITCHED
