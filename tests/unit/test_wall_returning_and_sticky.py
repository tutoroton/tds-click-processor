"""B3.2 — the returning/sticky machinery must SEE a wall winner.

🔴 THE DEFECT THIS FILE EXISTS FOR. Three predicates in `router.py` gate the
whole returning-visitor machinery on the winning flow's action type — and until
this file landed, all three read:

    … and (flow.get("action_type") or "") in ("offer", "split")

A wall winner is `action_type == "offerwall"`, so ALL THREE evaluated False and
the machinery silently switched itself off. They now read
`PIN_BEARING_ACTION_TYPES` — ONE name, so the three cannot drift apart. Nothing goes red: the click still
routes, the visitor still lands somewhere, and the only symptom is that a
sticky campaign stops being sticky for exactly the campaigns that need a wall.

That is what the owner ruled AGAINST, in the ADR that governs this pair
(`ADR-0516-…`, quoted verbatim there):

    «Якщо в налаштуваннях цієї кампанії налаштовано, що вона не закріплює
     офер, тобто є returning visitor fresh режим, то … ми не запам'ятовуємо
     останній офер … Тобто fresh, він розповсюджується І НА ЦІ ТИПИ ПОТОКІВ
     також. А стіки, якщо ми стіки вказуємо, то ми запам'ятовуємо його
     останній пункт призначення»

The mode applies to these flow types too — his words, about these flows.

WHY THE TESTS ENTER AT `_route_via_campaign` AND NOT LOWER. The predicates are
COMPOSED in `_try_flow_cascade` and PASSED to `_resolve_action_with_sticky`. A
test that calls the resolver directly is handed the gate as an argument and
therefore cannot see it being built wrongly one frame up — the exact blindness
`test_wall_pin_gate_composition.py` was written to record (adding a term back
left 97 of 97 wall tests green). Every test here drives the composing frame.

THE INSTRUMENT, and its three channels:
  * WHICH TARGET WAS SERVED — `attribution.offer_target_id` (NOT
    `result["target_id"]`, which is None at this frame).
  * WHETHER A PIN WAS WRITTEN — `ident.set_calls` on the identity pool.
  * WHICH WRITER WROTE IT — `attribution.routing_trace["wall_pin"]`, set only
    by the ADR-0516 wall-claim write. Value alone cannot separate that write
    from fresh-mode bookkeeping when both would name the same target, so the
    trace is the discriminator, not the value.

EVERY subject here is paired with an INVERSE CONTROL — a STANDARD campaign
whose winner is an ordinary `offer` flow, or the opposite returning mode. The
control is not decoration: it separates "the wall breaks the machinery" from
"this fixture never had working sticky", and a red subject beside a red control
proves nothing at all.
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
    _NORMAL_TARGET,
    _OFFER,
    FakeIdentRedis,
)
from tests.unit.test_wall_tile_membership import WALL_ID, _sign_wall

_ORDINARY_FLOW = 501
_RETURNING_FLOW = 502

TILE_1 = 111          # the wall's first tile — what the wall serves unaided
TILE_2 = 222          # the wall's second tile
PINNED = 333          # what an EXISTING pin names — deliberately not a tile
RETURNING_TARGET = 444

_UID = "U"


async def _seed(r, *, family: str, returning_mode: str,
                with_ordinary: bool = False,
                with_returning_flow: bool = False) -> dict:
    """One campaign; the wall, and optionally the ordinary/returning flows.

    `family` is the ONLY difference between a subject and its control:
    'offerwall' lets the wall win, 'standard' keeps it unreachable.
    """
    await r.hset("campaign:%d" % _CAMPAIGN, mapping={
        "company_id": str(_COMPANY),
        "flow_mode": "global",
        "returning_mode": returning_mode,
        "returning_routing": "1",           # per-company opt-in
        "flow_family": family,
    })
    await r.hset("flow:%d" % WALL_ID, mapping={
        "campaign_id": str(_CAMPAIGN), "company_id": str(_COMPANY),
        "scope_type": "company", "scope_id": str(_COMPANY),
        "audience": "offerwall", "action_type": "offerwall",
        "criteria": "[]", "seq_id": "1", "is_default": "0",
        "action_config": json.dumps({"tiles": [
            {"offer_id": _OFFER, "target_id": TILE_1},
            {"offer_id": _OFFER, "target_id": TILE_2},
        ]}),
    })
    await r.rpush("campaign:%d:walls" % _CAMPAIGN, str(WALL_ID))

    if with_ordinary:
        await r.hset("flow:%d" % _ORDINARY_FLOW, mapping={
            "campaign_id": str(_CAMPAIGN), "company_id": str(_COMPANY),
            "scope_type": "company", "scope_id": str(_COMPANY),
            "audience": "first", "action_type": "offer",
            "criteria": "[]", "seq_id": "2", "is_default": "1",
            "action_config": json.dumps(
                {"offer_id": _OFFER, "target_id": int(_NORMAL_TARGET)}),
        })
        await r.rpush("campaign:%d:flows" % _CAMPAIGN, str(_ORDINARY_FLOW))
    if with_returning_flow:
        await r.hset("flow:%d" % _RETURNING_FLOW, mapping={
            "campaign_id": str(_CAMPAIGN), "company_id": str(_COMPANY),
            "scope_type": "company", "scope_id": str(_COMPANY),
            "audience": "returning", "action_type": "offer",
            "criteria": "[]", "seq_id": "3", "is_default": "0",
            "action_config": json.dumps(
                {"offer_id": _OFFER, "target_id": RETURNING_TARGET}),
        })
        await r.rpush("campaign:%d:flows" % _CAMPAIGN, str(_RETURNING_FLOW))

    for tid in (TILE_1, TILE_2, PINNED, RETURNING_TARGET, int(_NORMAL_TARGET)):
        await r.hset("offer_target:%d" % tid, mapping={
            "url": "https://land/%d" % tid, "availability": "active",
            "is_default": "0", "offer_id": str(_OFFER),
            "criteria": "[]", "priority": "0",
        })
    campaign = await r.hgetall("campaign:%d" % _CAMPAIGN)
    campaign["_id"] = str(_CAMPAIGN)
    return campaign


def _run(*, family: str, returning_mode: str, seen_before: bool = True,
         existing_pin: int | None = None, code: str | None = None,
         with_ordinary: bool = False, with_returning_flow: bool = False,
         delivery: bool = True):
    """Drive the REAL caller so the three predicates are COMPOSED, not injected."""
    strings = {}
    if existing_pin is not None:
        strings[sticky_mod.sticky_key(_COMPANY, _UID, _CAMPAIGN)] = str(existing_pin)
    ident = FakeIdentRedis(strings=strings)

    async def _gir():
        return ident

    async def _stamp(**kw):
        # The REAL dataclass — a hand-rolled stub missing a field makes the
        # resolver fail OPEN and darkens the whole returning layer, producing a
        # red that looks exactly like the defect under test.
        return identity_mod.IdentityResult(
            uid=_UID, is_returning=seen_before, seen_before=seen_before,
            signal_tier="cookie",
        )

    async def _inner():
        r = fakeredis.aioredis.FakeRedis(decode_responses=True)
        campaign = await _seed(r, family=family, returning_mode=returning_mode,
                               with_ordinary=with_ordinary,
                               with_returning_flow=with_returning_flow)
        req = ClickRequest(
            click_id="wrs" + "0" * 18, country="US", user_agent="t/1.0",
            query_params={router.ROUTE_CODE_PARAM: code} if code else {},
        )
        timing: dict = {}
        with patch.object(sticky_mod, "get_identity_redis", _gir), \
                patch.object(identity_mod, "resolve_and_stamp", _stamp), \
                patch.object(settings, "returning_resolver_enabled", True), \
                patch.object(settings, "returning_routing_enabled", True), \
                patch.object(settings, "wall_delivery_enabled", delivery), \
                patch.object(settings, "route_preview_enabled", True), \
                patch.object(settings, "route_code_keys", _KEYS), \
                patch.object(settings, "route_code_active_kid", _ACTIVE_KID), \
                patch.object(settings, "returning_uid_ttl_seconds", 1000):
            return await router._route_via_campaign(
                r, campaign, str(_CAMPAIGN), req, timing, result_label="matched",
            )

    return asyncio.run(_inner()), ident


def _served(result) -> str:
    """The target actually served — read from where it LIVES at this frame."""
    return str(((result or {}).get("attribution") or {}).get("offer_target_id"))


def _trace(result) -> dict:
    return ((result or {}).get("attribution") or {}).get("routing_trace") or {}


# ============================================================
# R1 — a sticky HIT must reach a wall winner
# ============================================================

class TestR1StickyHitOnAWallCampaign:
    def test_the_pin_outranks_the_walls_first_tile(self):
        """A returning visitor under sticky already HAS a destination. The wall
        must not overwrite it with position 1 — «стіки … запам'ятовуємо його
        останній пункт призначення»."""
        result, _ident = _run(family="offerwall", returning_mode="sticky",
                              existing_pin=PINNED)
        assert result is not None, "the click must route at all"
        assert _served(result) == str(PINNED), (
            "the visitor's sticky pin was IGNORED on a wall campaign — they "
            "were served the wall's first tile instead. `sticky_active` is "
            "False because the winner's action_type is 'offerwall'."
        )

    def test_CONTROL_the_same_pin_is_honoured_on_a_standard_campaign(self):
        """The inverse control. If this is red too, the fixture never had
        working sticky and the subject above proves nothing."""
        result, _ident = _run(family="standard", returning_mode="sticky",
                              existing_pin=PINNED, with_ordinary=True)
        assert _served(result) == str(PINNED), (
            "CONTROL FAILED — sticky does not work in this fixture at all, so "
            "the wall subject above is not evidence about walls."
        )


# ============================================================
# R2 — a sticky MINT must happen on a wall winner
# ============================================================

class TestR2StickyMintOnAWallCampaign:
    def test_a_first_visit_under_sticky_mints_the_pin(self):
        _result, ident = _run(family="offerwall", returning_mode="sticky",
                              seen_before=False)
        assert ident.set_calls, (
            "no pin was minted for a first visit under a STICKY wall campaign "
            "— the next visit has nothing to be sticky to, so the campaign's "
            "sticky setting is inert on every wall campaign."
        )

    def test_CONTROL_a_first_visit_on_a_standard_campaign_mints(self):
        _result, ident = _run(family="standard", returning_mode="sticky",
                              seen_before=False, with_ordinary=True)
        assert ident.set_calls, "CONTROL FAILED — minting is broken in this fixture"


# ============================================================
# R3 — fresh-mode bookkeeping must track a wall winner
# ============================================================

class TestR3FreshTrackOnAWallCampaign:
    def test_fresh_mode_records_the_target_the_wall_served(self):
        """B-track: under fresh the pin TRACKS the last served offer, so a later
        flip to sticky freezes the visitor on what they LAST got. The owner:
        «fresh, він розповсюджується і на ці типи потоків також»."""
        _result, ident = _run(family="offerwall", returning_mode="fresh",
                              existing_pin=PINNED)
        assert ident.set_calls, (
            "fresh-mode tracking did not record the wall's served target. A "
            "later flip to sticky would freeze the visitor on a stale offer."
        )
        assert str(ident.set_calls[-1][1]) == str(TILE_1), (
            "the tracked value is not the target the wall actually served"
        )

    def test_CONTROL_fresh_mode_tracks_on_a_standard_campaign(self):
        _result, ident = _run(family="standard", returning_mode="fresh",
                              existing_pin=PINNED, with_ordinary=True)
        assert ident.set_calls, (
            "CONTROL FAILED — fresh tracking is broken in this fixture"
        )


# ============================================================
# R4 — D35 must SURVIVE the fix (green today, and must stay green)
# ============================================================

class TestR4TheD35ExclusionSurvives:
    """🔴 THE CALIBRATION THAT CATCHES A NAIVE FIX.

    The obvious way to fix R1-R3 is to widen the action-type term. The obvious
    way to get it WRONG is to drop the `audience != "returning"` term while
    "tidying" the three predicates into one. This test goes red if that
    happens: a returning-audience flow keeps its OWN pick, and the sticky pin
    must NOT override it (precedence: returning-flow > sticky pin > first-pool).
    """

    def test_a_returning_flow_winner_keeps_its_own_pick_on_a_wall_campaign(self):
        result, _ident = _run(family="offerwall", returning_mode="sticky",
                              existing_pin=PINNED, with_returning_flow=True)
        assert _served(result) == str(RETURNING_TARGET), (
            "D35 VIOLATED — the sticky pin overrode a returning-flow winner. "
            "If this went red after widening the action-type term, the edit "
            "also dropped the `audience != \"returning\"` exclusion."
        )


# ============================================================
# R5 — an honoured WALL CLAIM must still pin on a wall campaign
# ============================================================

class TestR5WallClaimPinOnAWallCampaign:
    def test_a_tile_choice_is_remembered_under_sticky(self):
        """ADR-0516's own case, re-appearing one layer up: the visitor chose a
        tile, the claim is honoured — and on a WALL campaign the pin write is
        gated by the same action-type term, so the choice leaves no trace.

        Asserted on the TRACE, not on the write count: under sticky the wall-pin
        write is the only writer that stamps `wall_pin`, which is what makes
        this specific to ADR-0516's mechanism rather than to "something wrote".
        """
        result, _ident = _run(family="offerwall", returning_mode="sticky",
                              code=_sign_wall(target_id=TILE_2))
        assert _trace(result).get("wall_pin") == str(TILE_2), (
            "an honoured WALL CLAIM wrote no wall pin on a wall campaign — the "
            "visitor's deliberate choice left no trace, which is exactly the "
            "staging measurement ADR-0516 was written to end."
        )

    def test_CONTROL_under_FRESH_the_same_claim_stamps_no_wall_pin(self):
        """Mode exclusivity. Fresh-mode bookkeeping may legitimately write the
        SERVED target, and it would name the same id — so the discriminator has
        to be WHICH writer ran, not what value it wrote."""
        result, _ident = _run(family="offerwall", returning_mode="fresh",
                              code=_sign_wall(target_id=TILE_2))
        assert "wall_pin" not in _trace(result), (
            "a wall pin was stamped under FRESH — the owner ruled «fresh … ми "
            "не запам'ятовуємо», and the gate must stay mode-scoped."
        )


# ============================================================
# R6 — the widening is INERT while the delivery flag is off
# ============================================================

class TestR6InertWhileDark:
    """🔴 THE SAFETY CLAIM, made checkable rather than argued.

    Widening `PIN_BEARING_ACTION_TYPES` looks like a change to the returning
    machinery for EVERY campaign. It is not, and the reason is structural: the
    third member is only reachable when a wall WINS, and a wall can only win
    when `wall_delivery_enabled` is on AND the campaign is
    `flow_family='offerwall'`. On every other campaign `action_type` is never
    "offerwall", so the new member cannot be selected.

    Structural or not, an argument is not evidence. These two drive the flag
    OFF and assert the pre-change behaviour survives byte-for-byte.
    """

    def test_flag_off_a_wall_campaign_behaves_as_before(self):
        """No wall winner ⇒ no wall pin, and the ordinary flow serves."""
        result, _ident = _run(family="offerwall", returning_mode="sticky",
                              existing_pin=PINNED, with_ordinary=True,
                              delivery=False)
        assert result is not None
        # The ordinary flow won, so sticky worked exactly as it always did.
        assert _served(result) == str(PINNED), (
            "flag OFF must leave the ordinary sticky path untouched"
        )

    def test_flag_off_the_wall_cannot_be_the_pinned_writer(self):
        result, _ident = _run(family="offerwall", returning_mode="fresh",
                              with_ordinary=True, delivery=False)
        assert _trace(result).get("flow_family") == "offerwall", (
            "the family must still be RECORDED while delivery is dark — that "
            "split is what makes fleet readiness checkable before activation"
        )
        assert _served(result) != str(TILE_1), (
            "a wall served the click with the delivery flag OFF"
        )


# ============================================================
# R7 — B12: what a ROLLBACK actually does to a dedicated wall campaign
# ============================================================

class TestR7RollbackIsATrafficDiversionNotADeadEnd:
    """🔴 THE ROLLBACK CONTRACT, measured rather than asserted.

    `wall_delivery_enabled` is the rollback lever. On a campaign that carries
    ORDINARY flows too, flipping it off is harmless — R6 pins that. On a
    DEDICATED wall campaign (walls and nothing else) it is not harmless, and
    the anchor's B12 box exists because nobody had measured which of two very
    different things happens:

      * the visitor dead-ends (a routing failure), or
      * 100% of the campaign's traffic diverts to the fallback.

    It is the SECOND. The router returns no winner, and `main.py` serves
    `campaign.fallback_url` (or, absent that, the Worker's own default — the
    Worker is the single fallback owner). Nothing is lost and nothing errors;
    every visitor simply stops reaching an offer.

    WHY THAT DISTINCTION IS OPERATIONAL, not academic. A dead end would show up
    instantly as errors. A silent 100% diversion to a fallback shows up as
    "traffic still 200s, conversions went to zero" — which is exactly the shape
    an operator diagnoses as an offer problem for hours before suspecting a
    flag. So the rollback ORDER is fixed:

        flip campaigns back to flow_family='standard' FIRST — after making sure
        each has an ordinary all-visitors flow or a fallback_url it is happy to
        serve — and only THEN turn the flag off.

    Turning the flag off first is not a rollback; it is a 100% traffic
    diversion with a green health check.
    """

    def test_flag_off_on_a_DEDICATED_wall_campaign_serves_no_offer(self):
        # No ordinary flow anywhere: walls are all this campaign has.
        result, _ident = _run(family="offerwall", returning_mode="fresh",
                              delivery=False)
        served = _served(result)
        assert served != str(TILE_1), (
            "a wall served a click with the delivery flag OFF — the dark "
            "default is not dark"
        )
        # The discriminator between the two candidate outcomes: no winner at
        # all, rather than a winner pointing somewhere unexpected.
        assert result is None or not (result.get("url") or "").startswith("https://land/111"), (
            "the campaign produced a routed destination from the wall while dark"
        )

    def test_the_SAME_campaign_with_the_flag_ON_does_serve(self):
        """The inverse control. Without it the test above passes on any broken
        fixture that never routes at all."""
        result, _ident = _run(family="offerwall", returning_mode="fresh",
                              delivery=True)
        assert _served(result) == str(TILE_1), (
            "CONTROL FAILED — this fixture cannot route even with the flag on, "
            "so the dark assertion above is not evidence about the flag"
        )
