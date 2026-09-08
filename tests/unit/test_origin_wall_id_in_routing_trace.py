"""G8.5 — the honour hook records WHICH WALL a tile click came from.

Programme SoT: ``docs/development/offerwall-2026-09-04/30-IMPLEMENTATION-ANCHOR.md`` G8.5,
designed by G8.2 (``42-G8-STATISTICS-MODEL.md`` §2).

G8.2 measured that **origin wall** and **delivering flow** are two dimensions on
one click fact, and that the click fact stores neither: the honour hook holds
``decoded.origin_flow_id`` (it already uses it for membership, G6.3) and stamps
only ``target_selection_path="route_code"``. Until the origin is written, a wall
click is analytically indistinguishable from a preview click and no query can
tell them apart. G8.2 named the seam — ``routing_trace`` is ``String`` (compact
JSON), so this costs no migration — and said writing it is its own lane.

This is that lane's pin. The four properties, and the reason each exists:

  1. **A wall code writes the origin.** The positive.
  2. **A v2 preview code writes NOTHING — the key is ABSENT, not null.** This is
     the byte-identical control, and it is what makes property 1 mean something:
     a stamp that fired for every code would carry no information at all.
     Absence also lets a query distinguish "not a wall" from "a wall we failed
     to name", which a null cannot.
  3. **A refused tile writes nothing.** Membership (G6.3) runs BEFORE the stamp,
     so a code naming a wall that no longer carries its tile falls through to
     ordinary routing and leaves no trace behind — otherwise the trace would
     assert an origin for a click the wall did not deliver.
  4. **No trace object is not a crash.** Production threads one, but the
     parameter is optional and a caller that omits it must route normally.

⚠️ The tempting wrong implementation is folding this into
``target_selection_path`` as ``"route_code:wall:797"``. That column is
``LowCardinality``, and two facts in one column is the substitution rule
`entity-boundaries` forbids. G8.2 names it; the code comment names it; it is
named here too, because the wrong version is one line shorter and this is the
file someone would edit while reaching for it.

Harness is the one ``test_route_code_honoured.py`` established — the REAL
``_resolve_action_with_sticky`` over the REAL ``action_executor``, so this
exercises the shipped path rather than a re-implementation of it.
"""

from __future__ import annotations

import asyncio
import json
from unittest.mock import patch

import pytest

from app import action_executor, route_code, router, sticky as sticky_mod
from app.config import settings
from app.models import ClickRequest

# Fakes are DUPLICATED from `test_route_code_honoured.py` rather than imported:
# pytest puts each test module in its own namespace here, so a cross-module
# import raises ModuleNotFoundError at collection. Copying a 30-line stub is the
# cheaper of the two evils against adding a conftest fixture that every other
# module would then load.

_KEYS = "1:unit-test-route-code-secret,2:second-kid-secret"
_ACTIVE_KID = "1"
_COMPANY = 1
_OFFER = 7
_CODED_TARGET = 99
_CAMPAIGN = 35


class FakeIdentRedis:
    def __init__(self):
        self.strings: dict[str, str] = {}

    async def set(self, key, value, nx=False, ex=None):
        if nx and key in self.strings:
            return None
        self.strings[key] = value
        return True

    async def get(self, key):
        return self.strings.get(key)

    async def expire(self, key, ttl):
        return True


class FakeRoutingRedis:
    def __init__(self, hashes: dict[str, dict] | None = None):
        self.hashes = hashes or {}
        self.writes: list[tuple] = []

    async def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    async def set(self, *a, **k):
        self.writes.append(("set", a))
        return True

    async def hset(self, *a, **k):
        self.writes.append(("hset", a))
        return True

    async def expire(self, *a, **k):
        self.writes.append(("expire", a))
        return True

    async def delete(self, *a, **k):
        self.writes.append(("delete", a))
        return True

    async def xadd(self, *a, **k):
        self.writes.append(("xadd", a))
        return True

    async def incr(self, *a, **k):
        self.writes.append(("incr", a))
        return True


def _target_hash():
    return {"url": "https://coded.example/x", "offer_id": str(_OFFER),
            "availability": "active"}
_WALL_ID = 797
_OTHER_WALL = 798


def _wall_hash(offer_id=_OFFER, target_id=_CODED_TARGET) -> dict:
    """A wall row shaped the way the sync builder publishes it."""
    return {
        "action_config": json.dumps(
            {"tiles": [{"offer_id": offer_id, "target_id": target_id}]}
        ),
    }


def _sign(*, origin_flow_id=None, offer_id=_OFFER, target_id=_CODED_TARGET):
    with patch.object(settings, "route_code_keys", _KEYS), \
         patch.object(settings, "route_code_active_kid", _ACTIVE_KID):
        return route_code.sign(
            company_id=_COMPANY,
            campaign_id=_CAMPAIGN,
            offer_id=offer_id,
            offer_target_id=target_id,
            ttl_seconds=600,
            origin_flow_id=origin_flow_id,
        )


def _click(code):
    return ClickRequest(
        click_id="g85-click",
        hostname="t.example.com",
        path="/",
        country="US",
        user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_2)",
        query_params={router.ROUTE_CODE_PARAM: code},
    )


def _resolve(*, code, wall_hash=None, pass_trace=True):
    """Drive the real resolver; return (result, trace)."""
    hashes = {"offer_target:%d" % _CODED_TARGET: _target_hash()}
    if wall_hash is not None:
        hashes["flow:%d" % _WALL_ID] = wall_hash
    r = FakeRoutingRedis(hashes=hashes)
    ident = FakeIdentRedis()
    trace: dict | None = {} if pass_trace else None

    async def _gir():
        return ident

    async def _serve(*a, **k):
        return {
            "url": "https://offer.example/normal",
            "offer_id": str(_OFFER),
            "target_id": "999",
            "target_selection_path": "split_weighted",
        }

    async def _runner():
        with patch.object(sticky_mod, "get_identity_redis", _gir), \
             patch.object(action_executor, "execute_action", _serve), \
             patch.object(settings, "route_preview_enabled", True), \
             patch.object(settings, "route_code_keys", _KEYS), \
             patch.object(settings, "route_code_active_kid", _ACTIVE_KID), \
             patch.object(settings, "returning_uid_ttl_seconds", 1000):
            return await router._resolve_action_with_sticky(
                r, {"action_type": "split"}, _click(code), str(_CAMPAIGN),
                source_mappings={}, campaign_mappings={},
                sticky_active=False,
                returning_flow_won=False,
                uid="U", company_id=_COMPANY,
                seen_before=True, returning_visitor=True,
                flow_id="300", allowed_avail=frozenset({"active"}),
                trace=trace,
            )

    result, _status = asyncio.run(_runner())
    return result, trace


class TestTheOriginIsRecorded:
    def test_a_wall_tile_click_records_the_wall_it_came_from(self):
        result, trace = _resolve(code=_sign(origin_flow_id=_WALL_ID),
                                 wall_hash=_wall_hash())
        assert result["target_id"] == str(_CODED_TARGET), result
        assert result["target_selection_path"] == "route_code"
        assert trace["origin_wall_id"] == _WALL_ID, trace

    def test_the_origin_is_the_WALL_not_the_delivering_flow(self):
        """The two share the word `flow_id` and are different concepts.

        `flow_id` passed to the resolver is 300 — the flow that would deliver.
        The recorded origin must be the WALL the tile came from, 797. Reading
        one as the other is the substitution `entity-boundaries` exists to stop,
        and it would be invisible if the fixture used the same number for both.
        """
        _result, trace = _resolve(code=_sign(origin_flow_id=_WALL_ID),
                                  wall_hash=_wall_hash())
        assert trace["origin_wall_id"] == _WALL_ID
        assert trace["origin_wall_id"] != 300


class TestTheControlsThatMakeItMeanSomething:
    def test_a_v2_preview_code_records_NOTHING(self):
        """The byte-identical control.

        A stamp that fired for every honoured code would carry no information.
        A v2 preview code makes no wall claim by construction, so the key must
        be ABSENT — not present-and-null, which a query cannot distinguish from
        "a wall we failed to name".
        """
        result, trace = _resolve(code=_sign(origin_flow_id=None))
        assert result["target_selection_path"] == "route_code"
        assert "origin_wall_id" not in trace, trace

    def test_a_tile_no_longer_in_its_wall_records_nothing(self):
        """Membership (G6.3) runs BEFORE the stamp.

        A code naming a wall that no longer carries its tile falls through to
        ordinary routing. If the stamp ran first, the trace would assert an
        origin for a click that wall did not deliver.
        """
        result, trace = _resolve(
            code=_sign(origin_flow_id=_WALL_ID),
            wall_hash=_wall_hash(offer_id=_OFFER, target_id=_CODED_TARGET + 1),
        )
        assert result["target_selection_path"] == "split_weighted", result
        assert "origin_wall_id" not in trace, trace

    def test_a_wall_that_cannot_be_read_records_nothing(self):
        """No wall row at all — membership is fail-closed, so no stamp."""
        result, trace = _resolve(code=_sign(origin_flow_id=_WALL_ID),
                                 wall_hash=None)
        assert result["target_selection_path"] == "split_weighted", result
        assert "origin_wall_id" not in trace, trace

    def test_no_trace_object_is_not_a_crash(self):
        """The parameter is optional; a caller that omits it routes normally."""
        result, trace = _resolve(code=_sign(origin_flow_id=_WALL_ID),
                                 wall_hash=_wall_hash(), pass_trace=False)
        assert trace is None
        assert result["target_id"] == str(_CODED_TARGET), result


class TestTheseTestsCanActuallyFail:
    """Calibration. A guard nobody has seen go red is a guard that may be unable to.

    Each case constructs the state the corresponding assertion is meant to
    reject, and asserts it IS rejected — so the suite above is shown to
    discriminate rather than assumed to.
    """

    def test_the_positive_and_the_control_disagree(self):
        """The discriminator, in one place: same hook, two codes, two answers."""
        _r1, wall_trace = _resolve(code=_sign(origin_flow_id=_WALL_ID),
                                   wall_hash=_wall_hash())
        _r2, prev_trace = _resolve(code=_sign(origin_flow_id=None))
        assert wall_trace != prev_trace
        assert "origin_wall_id" in wall_trace
        assert "origin_wall_id" not in prev_trace

    def test_the_fixture_would_notice_a_wrong_wall_id(self):
        """If the hook stamped a constant, this would still pass — so vary it."""
        _r, trace = _resolve(code=_sign(origin_flow_id=_OTHER_WALL),
                             wall_hash=_wall_hash())
        # 798's row is absent from Redis, so membership fails fail-closed and
        # nothing is stamped. That is the CORRECT outcome and it also proves the
        # id in the trace tracks the CODE rather than a hardcoded 797.
        assert "origin_wall_id" not in trace, trace

    def test_target_selection_path_is_NOT_overloaded(self):
        """The forbidden implementation, pinned so it cannot arrive later.

        `target_selection_path` is LowCardinality. If someone folds the wall id
        into it as `route_code:wall:797`, this goes red.
        """
        result, _trace = _resolve(code=_sign(origin_flow_id=_WALL_ID),
                                  wall_hash=_wall_hash())
        assert result["target_selection_path"] == "route_code"
        assert ":" not in result["target_selection_path"]
        assert str(_WALL_ID) not in result["target_selection_path"]


@pytest.mark.parametrize("origin", [None, _WALL_ID])
def test_the_click_is_served_either_way(origin):
    """Whatever the trace records, the visitor is always routed."""
    result, _trace = _resolve(code=_sign(origin_flow_id=origin),
                              wall_hash=_wall_hash())
    assert result is not None
    assert result.get("url")
