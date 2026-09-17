"""F2i step 1 — a wall campaign's fall-through to the legacy split is LABELLED.

WHAT THIS PINS, and what it deliberately does not. Step 1 of the F2r ruling is
a LABEL: the click is served exactly as before, and the only new fact is that
its `decision_reason` says WHICH mechanism sent it there. So the load-bearing
assertion in this file is not the new value — it is
`test_the_SERVE_is_byte_identical_it_is_a_LABEL_only`. A step-1 change that
altered routing would be step 2 arriving early and unmeasured.

🔴 THIS FILE IS THE RED BASELINE FOR STEP 2 — AND ITS FIRST PREDICTION WAS
WRONG, which is corrected here in step 2's own lane rather than left standing.

It said the serve assertions "MUST go red" when step 2 lands. **They do not, and
they must not.** That sentence was written while step 2 was still imagined
UNGATED; ADR-0561 added the gate afterwards, on a measurement — all EIGHT
offerwall-family campaigns have `fallback_url` NULL, so an ungated switch would
have handed every one of them a dead end.

This fixture's campaign has no `fallback_url`. It is therefore the
`wall · delivery ON · NO fallback` case, which step 2 deliberately leaves
UNCHANGED — so this file staying green IS the gate working, and step 2 asserts
exactly that in `test_a_wall_WITHOUT_a_fallback_is_UNCHANGED`
(`test_wall_terminal_fallback_is_gated.py`). Verified rather than argued: the
whole unit suite was diffed against an unmodified extract of `origin/stage` and
the failure sets were identical, so no test anywhere changed state.

What WOULD make this file go red is giving its campaign a `fallback_url` — and
that is a different fixture, not a regression in this one.

THE THREE ARMS, and why each exists:

    wall, delivery ON   the new reason. The subject.
    standard, ON        CONTROL — the same split, the same URL, the OLD reason.
                        Without it, "wall campaigns get the new label" is
                        indistinguishable from "the legacy split got renamed",
                        which would silently relabel every standard campaign's
                        last-resort fallback across the fleet.
    wall, delivery OFF  CONTROL — the dark flag. With `wall_delivery_enabled`
                        off the node acts on every campaign as 'standard', so
                        `first` flows ARE evaluated and this fall-through is
                        the ordinary pre-existing one. If this arm produced the
                        new reason, the label would be reading what the
                        campaign CLAIMS rather than what the node DID.

Fixture reused verbatim from `test_wall_campaign_falls_to_legacy_split`, which
established the behaviour being labelled. Reusing it rather than rebuilding it
is deliberate: two fixtures for one scenario can drift, and then neither file
is measuring what the other one is.
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

NEW_REASON = "wall_campaign_legacy_split"
OLD_REASON = "matched_legacy_split"


def _run(*, wall_family: bool, delivery: bool):
    """The fall-through fixture, with BOTH knobs exposed.

    The existing module's own `_run` pins `wall_delivery_enabled=True`, so the
    dark-flag control it does not need is unreachable through it. One extra
    parameter here, the same seed underneath.
    """
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
        req = ClickRequest(
            click_id="f2i" + "0" * 18, country=VISITOR_COUNTRY,
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


def _url(res) -> str:
    return str((res or {}).get("url") or "")


class TestTheWallFallThroughIsNowItsOwnReason:
    def test_a_wall_campaign_that_admits_nobody_gets_the_NEW_reason(self):
        res = _run(wall_family=True, delivery=True)
        assert _reason(res) == NEW_REASON, (
            f"expected {NEW_REASON!r}; got {_reason(res)!r}. If this is "
            f"{OLD_REASON!r}, the wall fall-through is invisible again and the "
            "denominator F2r ruled on cannot be counted."
        )

    def test_the_precondition_holds_it_really_did_reach_the_split(self):
        """Asserted, not assumed.

        Without this, the test above could pass on a run where the wall matched
        and something else produced a legacy route — green for the wrong reason.
        """
        res = _run(wall_family=True, delivery=True)
        assert res["timing"].get("route_via") == "legacy_split"
        assert "legacy-split" in _url(res)


class TestTheSERVEIsUnchanged:
    """🔴 THE POINT OF STEP 1. A label, not a routing change."""

    def test_the_SERVE_is_byte_identical_it_is_a_LABEL_only(self):
        wall = _run(wall_family=True, delivery=True)
        standard = _run(wall_family=False, delivery=True)
        assert _url(wall) == _url(standard) != "", (
            "step 1 must not change WHAT is served — only what the row says "
            f"about it. wall={_url(wall)!r} standard={_url(standard)!r}"
        )

    def test_the_offer_served_is_the_same_one_too(self):
        wall = _run(wall_family=True, delivery=True)
        standard = _run(wall_family=False, delivery=True)
        assert wall.get("offer_id") == standard.get("offer_id")


class TestTheControlsThatKeepTheLabelHonest:
    def test_CONTROL_a_standard_campaign_keeps_the_OLD_reason(self):
        """If this ever returns the new value, the split was RENAMED fleet-wide
        rather than a wall case being separated out of it."""
        res = _run(wall_family=False, delivery=True)
        assert _reason(res) == OLD_REASON, (
            f"a standard campaign must stay {OLD_REASON!r}; got {_reason(res)!r}"
        )

    def test_CONTROL_with_wall_delivery_OFF_the_wall_campaign_keeps_the_OLD_reason(self):
        """The dark flag. With delivery off the node acts on every campaign as
        'standard', so this fall-through is the ordinary pre-existing one and
        labelling it as wall-caused would be a false statement about which
        mechanism served the click."""
        res = _run(wall_family=True, delivery=False)
        assert _reason(res) == OLD_REASON, (
            "with wall_delivery_enabled off the node must be byte-identical to "
            f"itself before this change; got {_reason(res)!r}"
        )

    def test_CONTROL_the_trace_key_is_ABSENT_on_both_control_arms(self):
        """Read the discriminator itself, not only its consequence — a reason
        that happened to be right with the key set would pass the tests above."""
        for arm in (_run(wall_family=False, delivery=True),
                    _run(wall_family=True, delivery=False)):
            trace = arm["attribution"].get("routing_trace") or {}
            assert "wall_fell_to_legacy_split" not in trace


class TestTheReasonMapperItself:
    """`_decision_reason` in isolation — the branch, without the router."""

    def test_the_key_selects_the_new_reason(self):
        assert _decision_reason(
            {}, {"route_via": "legacy_split"},
            {"routing_trace": {"wall_fell_to_legacy_split": True}},
        ) == NEW_REASON

    def test_no_key_keeps_the_old_reason(self):
        assert _decision_reason(
            {}, {"route_via": "legacy_split"}, {"routing_trace": {}},
        ) == OLD_REASON

    def test_no_trace_at_all_keeps_the_old_reason(self):
        """A non-cascade path may carry no trace; it must not raise."""
        assert _decision_reason(
            {}, {"route_via": "legacy_split"}, {},
        ) == OLD_REASON

    def test_the_key_does_NOT_leak_into_a_non_split_route(self):
        """The key is only ever read under `route_via == 'legacy_split'`. A
        flow-cascade click that somehow carried it must still be a flow match."""
        assert _decision_reason(
            {}, {"route_via": "flow_cascade"},
            {"routing_trace": {"wall_fell_to_legacy_split": True}},
        ) != NEW_REASON
