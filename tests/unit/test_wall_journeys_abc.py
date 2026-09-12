"""J1 (§4) — the conflicting-state matrix, on a fixture where A, B and C DIFFER.

🔴 THE DEFECT THIS FILE EXISTS TO REPAIR IS IN THE PREVIOUS MATRIX, NOT IN THE CODE.

The plan states it in one sentence: *"make default/pin = A, tile = B, and the
eligible returning flow = C — three DIFFERENT destinations. On campaign 347 they
collapsed, so a row could pass without discriminating anything."*

A row whose expected and unexpected answers are the same string cannot fail. Every
such row was reported as evidence, and none of it was: it measured that the fixture
answers at all. So this file's first duty is not to add coverage — it is to make the
coverage that already exists mean something, by separating the three destinations
that were one.

    A = 501   what a plain click gets — the wall's FIRST tile, and the pin's value
    B = 502   what the TILE code names
    C = 503   what the eligible RETURNING flow serves

Three ids, three URLs, no overlap. Every assertion below names which of the three
it expects, so a failure says *which rule broke*, not merely "not what I wanted".

WHAT IS DRIVEN. `router._route_via_campaign` — the composing frame. Entering lower
would be handed the availability floor and the sticky decision as ARGUMENTS, which
is how a fixture ends up measuring itself (the "one frame up" mistake this programme
has already paid for twice, recorded in `test_wall_tile_identity.py`).

THE RULES UNDER TEST, each by its record:

  * ADR-0515 — a v3 WALL claim outranks a matching RETURNING flow («Плитка виграє»);
    a v2 PREVIEW claim gains nothing.
  * ADR-0516 — a tile landing under a STICKY campaign REWRITES the pin, narrowly.
  * ADR-0454 — the returning system outranks a guess about an anonymous visitor.
  * U3 (2026-09-12) — on a wall campaign the availability class follows freshness,
    not `disable_returning_flows`; the PARTITION still obeys the flag.
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
    _COMPANY,
    _KEYS,
    _OFFER,
    FakeIdentRedis,
    _sign,
)
from tests.unit.test_wall_tile_membership import WALL_ID, _sign_wall

# The three destinations. Deliberately far from every other fixture's ids, so a
# cross-import cannot quietly make two of them the same number again.
A = 501   # plain click / the pin
B = 502   # the tile
C = 503   # the eligible returning flow

RET_FLOW_ID = 920
_UID = "U"
_PIN_KEY = "sticky:%d:%s:%d" % (_COMPANY, _UID, _CAMPAIGN)

_URL = {A: "https://a.example/default", B: "https://b.example/tile",
        C: "https://c.example/returning"}


async def _seed(r, *, sticky_mode: bool, disable_returning: bool):
    """One offerwall campaign carrying BOTH a wall and a returning flow."""
    campaign_hash = {
        "company_id": str(_COMPANY),
        "flow_mode": "global",
        "returning_mode": "sticky" if sticky_mode else "fresh",
        "returning_routing": "1",
        "flow_family": "offerwall",
    }
    if disable_returning:
        campaign_hash["disable_returning_flows"] = "1"
    await r.hset("campaign:%d" % _CAMPAIGN, mapping=campaign_hash)

    # The wall. Tile ORDER matters: A is first, so a plain click lands on A and
    # the code naming B has a visible signature of its own.
    await r.hset("flow:%d" % WALL_ID, mapping={
        "campaign_id": str(_CAMPAIGN), "company_id": str(_COMPANY),
        "scope_type": "company", "scope_id": str(_COMPANY),
        "audience": "offerwall", "action_type": "offerwall",
        "criteria": "[]", "seq_id": "1", "is_default": "0",
        "action_config": json.dumps({"tiles": [
            {"offer_id": _OFFER, "target_id": A},
            {"offer_id": _OFFER, "target_id": B},
        ]}),
    })
    await r.rpush("campaign:%d:walls" % _CAMPAIGN, str(WALL_ID))

    # The eligible returning flow — a THIRD destination, which is the whole point.
    await r.hset("flow:%d" % RET_FLOW_ID, mapping={
        "campaign_id": str(_CAMPAIGN), "company_id": str(_COMPANY),
        "scope_type": "company", "scope_id": str(_COMPANY),
        "audience": "returning", "action_type": "offer",
        "criteria": "[]", "seq_id": "2", "is_default": "0",
        "action_config": json.dumps({"offer_id": _OFFER, "target_id": C}),
    })
    await r.rpush("campaign:%d:flows" % _CAMPAIGN, str(RET_FLOW_ID))

    for tid in (A, B, C):
        await r.hset("offer_target:%d" % tid, mapping={
            "url": _URL[tid], "availability": "active",
            "is_default": "0", "offer_id": str(_OFFER),
            "criteria": "[]", "priority": "0",
        })

    campaign = await r.hgetall("campaign:%d" % _CAMPAIGN)
    campaign["_id"] = str(_CAMPAIGN)
    return campaign


def _journey(*, sticky_mode=False, disable_returning=False, seen_before=True,
             code=None, pinned=None):
    """Drive the REAL composing frame once. Returns (served_target, ident)."""
    ident = FakeIdentRedis(strings={_PIN_KEY: str(pinned)} if pinned else {})

    async def _gir():
        return ident

    async def _stamp(**kw):
        return identity_mod.IdentityResult(
            uid=_UID, is_returning=seen_before, seen_before=seen_before,
            signal_tier="cookie",
        )

    async def _inner():
        r = fakeredis.aioredis.FakeRedis(decode_responses=True)
        campaign = await _seed(r, sticky_mode=sticky_mode,
                               disable_returning=disable_returning)
        req = ClickRequest(
            click_id="abc" + "0" * 18, country="US", user_agent="t/1.0",
            query_params={router.ROUTE_CODE_PARAM: code} if code else {},
        )
        with patch.object(sticky_mod, "get_identity_redis", _gir), \
                patch.object(identity_mod, "resolve_and_stamp", _stamp), \
                patch.object(settings, "returning_resolver_enabled", True), \
                patch.object(settings, "returning_routing_enabled", True), \
                patch.object(settings, "wall_delivery_enabled", True), \
                patch.object(settings, "wall_tile_honour_enabled", True), \
                patch.object(settings, "route_preview_enabled", True), \
                patch.object(settings, "route_code_keys", _KEYS), \
                patch.object(settings, "route_code_active_kid", _ACTIVE_KID), \
                patch.object(settings, "returning_uid_ttl_seconds", 1000):
            return await router._route_via_campaign(
                r, campaign, str(_CAMPAIGN), req, {}, result_label="matched",
            )

    result = asyncio.run(_inner())
    attr = (result or {}).get("attribution") or {}
    served = attr.get("offer_target_id")
    return (int(served) if served is not None else None), ident


def _name(target) -> str:
    return {A: "A (plain/pin)", B: "B (tile)", C: "C (returning flow)"}.get(
        target, repr(target))


class TestTheFixtureItselfDiscriminates:
    """🔴 FIRST, AND IT IS NOT CEREMONY.

    This file's entire claim is that the three destinations differ. If that ever
    stops being true the rest of the file keeps passing and stops meaning
    anything — which is precisely the failure being repaired. So the separation
    is asserted, not assumed.
    """

    def test_the_three_ids_are_distinct(self):
        assert len({A, B, C}) == 3

    def test_the_three_urls_are_distinct(self):
        assert len({_URL[A], _URL[B], _URL[C]}) == 3

    def test_each_of_the_three_is_reachable_at_all(self):
        """The positive control for the whole matrix. Without it, every row
        below is equally satisfied by a fixture that can only ever answer A."""
        assert _journey(seen_before=False)[0] == A
        assert _journey(seen_before=True)[0] == C
        assert _journey(seen_before=True, code=_sign_wall(target_id=B))[0] == B


class TestFreshMode:
    """`returning_mode='fresh'` — no pin. The contest is tile vs returning flow."""

    def test_a_NEW_visitor_gets_the_walls_first_tile(self):
        got, _ = _journey(seen_before=False)
        assert got == A, f"expected A, got {_name(got)}"

    def test_a_RETURNING_visitor_gets_the_returning_flow(self):
        got, _ = _journey(seen_before=True)
        assert got == C, f"expected C, got {_name(got)}"

    def test_a_TILE_beats_the_returning_flow(self):
        # ADR-0515 «Плитка виграє». Discriminating only because C != B.
        got, _ = _journey(seen_before=True, code=_sign_wall(target_id=B))
        assert got == B, f"expected B, got {_name(got)}"

    def test_a_PREVIEW_code_does_NOT_beat_the_returning_flow(self):
        # ADR-0454 still stands for a v2 claim — the half that keeps ADR-0515
        # narrow. On the old collapsed fixture this and the row above were the
        # same assertion.
        # 🔴 THE PREVIEW CODE MUST NAME A TARGET THIS FIXTURE HAS. Signed at
        # the default `_CODED_TARGET`, it names a target absent from this seed,
        # so the honour path would fall through on the LOOKUP and this row
        # would pass whether the kind guard existed or not. Caught by mutation:
        # deleting the guard (`wall_only=False`) left all 18 rows green.
        got, _ = _journey(seen_before=True, code=_sign(target_id=B))
        assert got == C, (
            f"expected C, got {_name(got)} — a v2 PREVIEW claim was honoured "
            "over a returning flow, so ADR-0515's guard was DELETED rather "
            "than narrowed (ADR-0454 still stands for a preview code)"
        )


class TestDisableReturningFlows:
    """The flag suppresses the PARTITION — and, since U3, nothing else."""

    def test_the_returning_flow_no_longer_wins(self):
        got, _ = _journey(seen_before=True, disable_returning=True)
        assert got == A, f"expected A, got {_name(got)}"

    def test_but_a_TILE_still_wins(self):
        got, _ = _journey(seen_before=True, disable_returning=True,
                          code=_sign_wall(target_id=B))
        assert got == B, f"expected B, got {_name(got)}"

    def test_a_NEW_visitor_is_unaffected_by_the_flag(self):
        # The control: the flag's effect above is about the RETURNING class, so
        # the new-visitor answer must not move with it.
        assert _journey(seen_before=False, disable_returning=False)[0] == A
        assert _journey(seen_before=False, disable_returning=True)[0] == A


class TestStickyModeWithAnEligibleReturningFlow:
    """🔴 THE ROW THE COLLAPSED FIXTURE COULD NOT SHOW, and it went red first.

    My first version of this class asserted that a live pin on A serves a
    returning visitor. It does not: `sticky_active` carries the **D35
    exclusion** (`audience != "returning"`), so a visitor an eligible returning
    FLOW captured has it forced False and the pin is never consulted. C wins.

    On a fixture where pin and returning flow pointed at the same place, "the
    pin served" and "the returning flow served" were the SAME STRING — so this
    rule was invisible, and a test asserting either one passed for whichever
    reason happened to be true. That is the defect §4 names, caught on the first
    run of the repaired fixture. The code was right; the expectation was not.
    """

    def test_an_eligible_returning_flow_BEATS_the_live_pin(self):
        got, _ = _journey(sticky_mode=True, seen_before=True, pinned=A)
        assert got == C, (
            f"expected C — D35 forces sticky_active False when a returning "
            f"flow wins, so the pin is not consulted — got {_name(got)}"
        )

    def test_and_a_TILE_still_beats_BOTH(self):
        got, _ = _journey(sticky_mode=True, seen_before=True, pinned=A,
                          code=_sign_wall(target_id=B))
        assert got == B, f"expected B, got {_name(got)}"


class TestStickyModeWithThePinACTUALLYLive:
    """The pin only decides when no returning flow is eligible, so every row
    here holds the partition OFF. That is not a convenience — with it ON the
    rows below would pass while measuring the returning flow, which is exactly
    how the previous matrix produced evidence that meant nothing.

    ⚠️ `sticky_active` gates on `_returning_live`, NOT on `_audience_routing`,
    so disabling returning FLOWS leaves the pin fully alive. That is the
    property that makes this class possible at all.
    """

    def test_the_pin_serves_a_returning_visitor(self):
        got, _ = _journey(sticky_mode=True, disable_returning=True,
                          seen_before=True, pinned=A)
        assert got == A, f"expected A, got {_name(got)}"

    def test_a_TILE_beats_the_live_pin(self):
        # ADR-0516 / G7.2 — and NOW it is genuinely pin-versus-tile, because
        # nothing else is in play. The identically-named test in my first draft
        # passed while the pin was not even being consulted.
        got, _ = _journey(sticky_mode=True, disable_returning=True,
                          seen_before=True, pinned=A,
                          code=_sign_wall(target_id=B))
        assert got == B, f"expected B, got {_name(got)}"

    def test_that_tile_landing_REWRITES_the_pin_to_B(self):
        # The narrow supersession of the never-writes rule. Asserted on the
        # WRITE, not the destination: a value assertion cannot see a missing
        # mode-gate, only a COUNT can.
        _got, ident = _journey(sticky_mode=True, disable_returning=True,
                               seen_before=True, pinned=A,
                               code=_sign_wall(target_id=B))
        assert len(ident.set_calls) == 1, (
            "exactly one write: the tile landing becomes the last destination"
        )
        key, value, nx, _ex = ident.set_calls[0]
        assert key == _PIN_KEY
        assert value == str(B), "the pin must name the offer the visitor CHOSE"
        assert nx is False, "overwrite, not NX — a later tile replaces an earlier"

    def test_a_PREVIEW_code_neither_beats_the_pin_nor_writes(self):
        # Both halves in one row, because ADR-0516's licence is granted by the
        # verified KIND and by nothing else.
        # Same correction as above: the code must name a target that EXISTS,
        # or the refusal is produced by the lookup rather than by the rule.
        got, ident = _journey(sticky_mode=True, disable_returning=True,
                              seen_before=True, pinned=A,
                              code=_sign(target_id=B))
        assert got == A, f"expected A, got {_name(got)}"
        assert ident.set_calls == [], "a preview code must never write the pin"


class TestR2IsolatedFromTheReturningFlow:
    """🔴 J1's named requirement: *tile → plain URL with returning DISABLED*.

    R2 is "sticky returns the last offer". Tested with returning flows ENABLED,
    its expected answer secretly depends on the returning flow also pointing at
    the same place — so a pass would not tell you which mechanism produced it.
    With the partition OFF, the returning flow cannot contribute at all, and
    what remains is R2 alone.

    The journey is two clicks: the first carries the tile code (and writes the
    pin), the second is a PLAIN url. The second click is the measurement.
    """

    def test_the_plain_click_after_a_tile_returns_the_TILE_offer(self):
        # Click 1 — establishes the pin at B. Asserted, because a journey whose
        # first step silently failed would make step 2 prove nothing.
        first, ident = _journey(sticky_mode=True, disable_returning=True,
                                seen_before=True, pinned=A,
                                code=_sign_wall(target_id=B))
        assert first == B, f"step 1 expected B, got {_name(first)}"
        assert ident.set_calls, "step 1 wrote no pin — step 2 cannot mean anything"
        _key, written, _nx, _ex = ident.set_calls[0]

        # Click 2 — the SAME visitor, no code, returning flows still disabled.
        second, _ = _journey(sticky_mode=True, disable_returning=True,
                             seen_before=True, pinned=int(written))
        assert second == B, (
            f"expected B — the pin the tile wrote — got {_name(second)}. "
            "With the returning partition off, nothing but R2 can produce B "
            "here, which is exactly why this row is isolated."
        )

    def test_the_control_without_the_first_click_returns_A(self):
        """Without step 1 the same second click returns A. That is what makes
        the row above attributable to the tile rather than to the fixture."""
        got, _ = _journey(sticky_mode=True, disable_returning=True,
                          seen_before=True, pinned=A)
        assert got == A, f"expected A, got {_name(got)}"
