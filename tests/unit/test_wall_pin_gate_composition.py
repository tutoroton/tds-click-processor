"""ADR-0516 — the wall pin gate's own COMPOSITION, driven through its CALLER.

🔴 WHY THIS FILE EXISTS. Measured 2026-09-08 by mutation.

`wall_pin_eligible` is composed at `router.py:2230` as the deliberate MIRROR of
`sticky_active` and `fresh_track` MINUS one term — the D35 exclusion
`audience != "returning"`. Omitting that term is the whole point of ADR-0516:
including it would make the gate False for exactly the visitor the owner's
ruling is about, so the tile landing would silently stop re-pinning for every
returning visitor. The comment above the gate says so in red, and warns that a
future edit must not "harmonise" the term back in.

Nothing enforced that. Adding the term back left **97 of 97** wall tests green,
because EVERY existing wall test calls `router._resolve_action_with_sticky`
DIRECTLY and is HANDED `wall_pin_eligible` as an argument. A test that receives
the gate cannot see the gate being built wrongly one frame up.

This file drives `router._route_via_campaign`, which composes the gate itself.
The mutation kills the first test here; the rest are its calibration.

⚠️ The instrument is the WRITE COUNT on the identity pool, not a value. Under
this fixture the wall pin is the ONLY possible writer — `sticky_active` is
False by the D35 exclusion (a returning flow won) and `fresh_track` needs
`effective_mode == "fresh"` — so a write proves the gate opened, and its absence
proves it did not.
"""

from __future__ import annotations

import asyncio
import json
from unittest.mock import patch

import fakeredis.aioredis

from app import identity as identity_mod
from app import router
from app import sticky as sticky_mod
from app.config import settings
from app.models import ClickRequest

from tests.unit.test_route_code_honoured import (
    _ACTIVE_KID,
    _CAMPAIGN,
    _CODED_TARGET,
    _COMPANY,
    _KEYS,
    _NORMAL_TARGET,
    _OFFER,
    FakeIdentRedis,
)
from tests.unit.test_wall_tile_membership import WALL_ID, _sign_wall

_RETURNING_FLOW = 500


async def _seed(r, *, returning_mode: str, wall_present: bool = True) -> dict:
    """One campaign, one RETURNING-audience offer flow, and the wall the code
    was minted from. `returning_mode` is the ONLY thing that varies between the
    subject and its FRESH calibration."""
    await r.hset("campaign:%d" % _CAMPAIGN, mapping={
        "company_id": str(_COMPANY),
        "flow_mode": "global",
        "returning_mode": returning_mode,
        "returning_routing": "1",          # per-company opt-in, router.py:757
    })
    await r.rpush("campaign:%d:flows" % _CAMPAIGN, str(_RETURNING_FLOW))
    await r.hset("flow:%d" % _RETURNING_FLOW, mapping={
        "campaign_id": str(_CAMPAIGN), "company_id": str(_COMPANY),
        "scope_type": "company", "scope_id": str(_COMPANY),
        "audience": "returning",           # the D35 term's subject
        "action_type": "offer",
        "action_config": json.dumps(
            {"offer_id": _OFFER, "target_id": int(_NORMAL_TARGET)}),
        "criteria": "[]", "seq_id": "1", "is_default": "1",
    })
    if wall_present:
        await r.hset("flow:%d" % WALL_ID, mapping={
            "campaign_id": str(_CAMPAIGN), "company_id": str(_COMPANY),
            "audience": "offerwall", "action_type": "offerwall",
            "action_config": json.dumps({"tiles": [
                {"offer_id": _OFFER, "target_id": _CODED_TARGET}]}),
        })
    for tid in (int(_NORMAL_TARGET), _CODED_TARGET):
        await r.hset("offer_target:%d" % tid, mapping={
            "url": "https://land/%d" % tid, "availability": "active",
            "is_default": "0", "offer_id": str(_OFFER),
            "criteria": "[]", "priority": "0",
        })
    campaign = await r.hgetall("campaign:%d" % _CAMPAIGN)
    campaign["_id"] = str(_CAMPAIGN)
    return campaign


def _run(*, returning_mode: str, code: str | None, wall_present: bool = True):
    """Drive the REAL caller so the gate is COMPOSED, never injected.

    Returns (result, ident) — `ident.set_calls` is the discriminator.
    """
    ident = FakeIdentRedis()

    async def _gir():
        return ident

    async def _stamp(**kw):
        # 🔴 The REAL dataclass, not a stand-in. A hand-rolled stub missing
        # `is_unique` made the resolver fail OPEN, which silently darkened the
        # whole returning layer — and the test then failed for a fixture
        # reason that looked exactly like the defect it was hunting.
        return identity_mod.IdentityResult(
            uid="U", is_returning=True, seen_before=True, signal_tier="cookie",
        )

    async def _inner():
        r = fakeredis.aioredis.FakeRedis(decode_responses=True)
        campaign = await _seed(r, returning_mode=returning_mode,
                               wall_present=wall_present)
        # 🔴 `query_params`, and the param NAME read from the module rather
        # than typed as a literal — both were guessed first, and the guess
        # produced a red that looked exactly like the defect under test.
        req = ClickRequest(
            click_id="gate" + "0" * 18, country="US", user_agent="t/1.0",
            query_params={router.ROUTE_CODE_PARAM: code} if code else {},
        )
        timing: dict = {}
        with patch.object(sticky_mod, "get_identity_redis", _gir), \
                patch.object(identity_mod, "resolve_and_stamp", _stamp), \
                patch.object(settings, "returning_resolver_enabled", True), \
                patch.object(settings, "returning_routing_enabled", True), \
                patch.object(settings, "route_preview_enabled", True), \
                patch.object(settings, "route_code_keys", _KEYS), \
                patch.object(settings, "route_code_active_kid", _ACTIVE_KID), \
                patch.object(settings, "returning_uid_ttl_seconds", 1000):
            return await router._route_via_campaign(
                r, campaign, str(_CAMPAIGN), req, timing,
                result_label="matched",
            )

    result = asyncio.run(_inner())
    return result, ident


class TestTheGateIsComposedCorrectlyOneFrameUp:
    """The mutation `and (flow.get("audience") or "first") != "returning"`
    added to `wall_pin_eligible` kills the first test and NOTHING else in the
    repo. That is the whole reason this file exists."""

    def test_a_RETURNING_flow_winner_under_STICKY_still_gets_the_wall_pin(self):
        result, ident = _run(returning_mode="sticky", code=_sign_wall())
        assert result is not None, "the click must route at all"
        assert ident.set_calls, (
            "the wall pin was NOT written for a visitor a RETURNING flow "
            "captured. That is the case ADR-0516 exists for, and the D35 term "
            "must stay OUT of `wall_pin_eligible` (router.py:2230). If this "
            "went red after an edit to that predicate, the edit disabled the "
            "owner's ruling for every returning visitor."
        )

    def test_under_FRESH_the_same_click_writes_NO_wall_pin(self):
        """Calibration one — the assertion is about the MODE TERM, not the
        fixture. Only `returning_mode` differs from the test above."""
        _result, ident = _run(returning_mode="fresh", code=_sign_wall())
        assert not ident.set_calls, (
            "under FRESH nothing may be pinned for routing — the owner ruled "
            "«fresh … ми не запам'ятовуємо останній офер»"
        )

    def test_without_a_route_code_there_is_no_wall_pin(self):
        """Calibration two — the write is caused by the honoured WALL CLAIM,
        not merely by the campaign being sticky."""
        _result, ident = _run(returning_mode="sticky", code=None)
        assert not ident.set_calls, (
            "a plain click by a returning-flow winner must not write a wall "
            "pin: there was no tile choice to remember"
        )
