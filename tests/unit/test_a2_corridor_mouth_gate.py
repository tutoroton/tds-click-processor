"""A2 (tenant isolation, 2026-08-21) — the geo corridor is closed at its MOUTH.

WHY THIS FILE EXISTS. Domain traffic that resolved no usable route used to fall
through into geo targeting, which reads the GLOBAL `campaigns:active` set. A
click that arrived on company X's hostname could therefore be served by company
Y's campaign. That is not a hypothetical: it was reproduced live on staging
(a click on company 1's domain served by company 38's campaign 304, and four
campaigns of three companies observed on a single hostname).

WHY A GATE AT THE MOUTH AND NOT PATCHES AT THE ENTRANCES. The corridor has
several entrances, and three of them were patched individually over the years
(F9 `domains:disabled`, §6 wildcard, R69 dead binding) while the corridor they
all led into stayed open. A gate at the mouth cannot be bypassed by an entrance
nobody enumerated — which matters, because the enumeration was wrong twice
during this sprint alone.

WHAT THIS FILE MUST NEVER BECOME. A test that only proves refusals happen would
pass on a router that refuses EVERYTHING. Every refusal case below is therefore
paired with a control that must still SERVE, and the controls are the reason
this file can fail in both directions.
"""

from __future__ import annotations

import asyncio
import json
from unittest.mock import patch

import fakeredis.aioredis
import pytest

from app import identity, router
from app.models import ClickRequest

pytestmark = pytest.mark.asyncio

CO_OWN = 1          # the tenant whose hostname the click arrives on
CO_FOREIGN = 38     # the tenant whose campaign must never be reachable
CAMP_OWN = "100"
CAMP_FOREIGN = "304"
OFFER_OWN = "500"
OFFER_FOREIGN = "504"
HOST = "own.test"
FOREIGN_URL = "https://foreign-tenant.example/{click_id}"


async def _seed_foreign_geo_pool(fake) -> None:
    """A foreign tenant's campaign, geo-eligible and ready to poach.

    No targeting flags are set, which mirrors production: no sync builder emits
    `geo:` / `device:` / `os:` / `has_geo` keys, so the geo filter is
    structurally empty and every active campaign of every tenant is a candidate.
    """
    await fake.sadd("campaigns:active", CAMP_FOREIGN)
    await fake.hset(
        f"campaign:{CAMP_FOREIGN}",
        mapping={"company_id": str(CO_FOREIGN), "priority": "0"},
    )
    await fake.sadd(f"campaign:{CAMP_FOREIGN}:offers", OFFER_FOREIGN)
    await fake.hset(
        f"offer:{OFFER_FOREIGN}",
        mapping={"url": FOREIGN_URL, "has_targets": "0"},
    )


async def _seed_serving_binding(fake, *, param_c: str) -> None:
    """A healthy same-tenant binding that MUST keep serving (the control)."""
    await fake.set(
        f"domain:{HOST}:param:{param_c}",
        json.dumps({"campaign_id": CAMP_OWN, "binding_id": 11, "binding_alias": "ok"}),
    )
    await fake.hset(
        f"campaign:{CAMP_OWN}",
        mapping={"company_id": str(CO_OWN), "priority": "0"},
    )
    await fake.sadd(f"campaign:{CAMP_OWN}:offers", OFFER_OWN)
    await fake.hset(
        f"offer:{OFFER_OWN}",
        mapping={"url": "https://own-tenant.example/{click_id}", "has_targets": "0"},
    )


def _click(click_id: str, *, hostname: str = HOST, params: dict | None = None) -> ClickRequest:
    return ClickRequest(
        click_id=click_id,
        country="US",
        user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_2)",
        hostname=hostname,
        query_params=params if params is not None else {},
    )


async def _route(fake, req: ClickRequest):
    async def _aget():
        return fake

    with patch.object(router, "get_redis", _aget), \
            patch.object(identity, "get_identity_redis", _aget):
        result = await router.route(req)
    for _ in range(5):
        await asyncio.sleep(0)
    return result


def _assert_refused(result) -> None:
    """A refusal must be RECORDED, not silent.

    `url is None` alone would also be true of a `route()` returning None, which
    takes the click off the recording path entirely — a click that vanishes
    from analytics is worse than one that is misrouted, because nobody can
    count what is not there.
    """
    assert result is not None, "refusal must return a sentinel, never None"
    assert result["blocked"] is True
    assert result["url"] is None
    assert result["timing"]["result"] == "blocked_no_route"
    # Stage 2 stamps `geo_lookup_ms`; its absence proves the global pool was
    # never even read, which is the property the gate exists to guarantee.
    assert "geo_lookup_ms" not in result["timing"]


class TestCorridorIsClosedForDomainTraffic:
    async def test_no_binding_at_all_is_refused(self):
        """Entrance 1: the hostname resolves to no binding whatsoever."""
        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        await _seed_foreign_geo_pool(fake)

        result = await _route(fake, _click("a2-nobind"))

        _assert_refused(result)
        assert result["campaign_id"] != CAMP_FOREIGN

    async def test_binding_present_but_routes_nowhere_is_refused(self):
        """Entrance 2: a live binding whose campaign yields no route.

        This is the `fall_through_on_no_route=True` path — the one that looked
        most like a legitimate catch-all and was in fact the widest door.
        """
        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        await _seed_foreign_geo_pool(fake)
        await fake.set(f"domain:{HOST}:param:dead", CAMP_OWN)
        # Campaign hash present, but no flows / offers / split / fallback.
        await fake.hset(
            f"campaign:{CAMP_OWN}",
            mapping={"company_id": str(CO_OWN), "priority": "0"},
        )

        result = await _route(fake, _click("a2-noroute", params={"c": "dead"}))

        _assert_refused(result)
        assert result["campaign_id"] != CAMP_FOREIGN

    async def test_corrupt_binding_json_is_refused(self):
        """Entrance 3: the binding value does not parse.

        Measured 0 of 128 live bindings on staging, but a parser that fails
        open is a leak waiting for its first malformed row.
        """
        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        await _seed_foreign_geo_pool(fake)
        await fake.set(f"domain:{HOST}:param:bad", "{not-json{{")

        result = await _route(fake, _click("a2-corrupt", params={"c": "bad"}))

        _assert_refused(result)
        assert result["campaign_id"] != CAMP_FOREIGN

    async def test_unknown_selector_on_a_bound_host_is_refused(self):
        """Entrance 4: the host has bindings, but not for THIS `?c=` value.

        The nearest miss — a hostname we genuinely serve, reached with a
        selector nobody registered.
        """
        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        await _seed_foreign_geo_pool(fake)
        await _seed_serving_binding(fake, param_c="real")

        result = await _route(fake, _click("a2-wrongsel", params={"c": "ghost"}))

        _assert_refused(result)
        assert result["campaign_id"] != CAMP_FOREIGN


class TestTheGateDoesNotOverBlock:
    """The controls. Without these the file would pass on a router that
    refuses every click, which is the failure mode a one-sided test invites."""

    async def test_healthy_binding_still_serves(self):
        """INVARIANT 1 — no degradation of routing. A correctly bound click is
        behaviour-identical to before the change."""
        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        await _seed_foreign_geo_pool(fake)
        await _seed_serving_binding(fake, param_c="real")

        result = await _route(fake, _click("a2-ok", params={"c": "real"}))

        assert result is not None
        assert not result.get("blocked")
        assert result["campaign_id"] == CAMP_OWN
        assert "own-tenant.example" in result["url"]

    async def test_host_less_call_still_reaches_geo(self):
        """A `/decide` with no hostname names no domain, so no tenant owns it
        and the domain-authority argument does not apply. Its behaviour is
        deliberately unchanged — and this test is what would catch the gate
        widening beyond domain traffic by accident."""
        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        await _seed_foreign_geo_pool(fake)

        result = await _route(fake, _click("a2-hostless", hostname=""))

        assert result is not None
        # Geo WAS consulted — the corridor is untouched for this shape.
        assert "geo_lookup_ms" in result["timing"]


class TestRefusalIsRecordable:
    async def test_refusal_carries_the_binding_it_arrived_through(self):
        """Analytics must still be able to say WHICH binding was attempted,
        otherwise a refused click cannot be traced back to the configuration
        that should have served it."""
        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        await _seed_foreign_geo_pool(fake)
        await fake.set(
            f"domain:{HOST}:param:dead",
            json.dumps({"campaign_id": CAMP_OWN, "binding_id": 77, "binding_alias": "att"}),
        )
        await fake.hset(
            f"campaign:{CAMP_OWN}",
            mapping={"company_id": str(CO_OWN), "priority": "0"},
        )

        result = await _route(fake, _click("a2-meta", params={"c": "dead"}))

        _assert_refused(result)
        assert result["binding_id"] == 77
        assert result["binding_alias"] == "att"

    async def test_decision_reason_is_domain_blocked_not_flow_blocked(self):
        """The refusal is a DOMAIN-level edge block. Reporting it as
        `blocked_by_flow` would state that a flow refused the click when no
        flow was ever consulted — a false claim about which mechanism acted."""
        from app.main import _decision_reason

        reason = _decision_reason({"blocked": True}, {"result": "blocked_no_route"}, {})
        assert reason == "domain_blocked"
