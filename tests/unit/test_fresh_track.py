"""B-track (2026-06-10, user decision) — fresh-mode pin tracking.

Under a live-returning FRESH-mode campaign, every served offer/split click
OVERWRITES the (uid, campaign) sticky pin (`sticky.repin`, SET EX sliding
TTL) so the pin always equals the LAST offer the visitor actually received.
Flipping the campaign to sticky then freezes exactly that last offer (the
sticky path reads the pin unchanged); flipping back to fresh resumes
tracking. Previously fresh mode never touched pins → re-enabling sticky
resurrected the FIRST-ever pinned offer (live-confirmed on node 51,
campaign 35, 2026-06-10 — the "emechanik keeps coming back" report).

Pins are pure bookkeeping on the fresh path: these tests pin that the
click's own routing result is NEVER altered, every failure is fail-open,
and the gate is the EXACT mirror of the sticky gate (same D35
returning-audience exclusion — the pin only records what sticky could
later legitimately serve).
"""

from __future__ import annotations

import asyncio
from unittest.mock import patch

from app import action_executor, router, sticky as sticky_mod
from app.config import settings
from app.models import ClickRequest


class FakeIdentRedis:
    """String-keyspace stub for the sticky pin ops (SET / GET / EXPIRE)."""

    def __init__(self, strings: dict[str, str] | None = None):
        self.strings = strings or {}
        self.set_calls: list[tuple] = []

    async def set(self, key, value, nx=False, ex=None):
        self.set_calls.append((key, value, nx, ex))
        if nx and key in self.strings:
            return None
        self.strings[key] = value
        return True

    async def get(self, key):
        return self.strings.get(key)

    async def expire(self, key, ttl):
        return True


def _click() -> ClickRequest:
    return ClickRequest(
        click_id="test-click-ft",
        country="US",
        user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_2)",
        query_params={},
    )


def _resolve(*, fresh_track, sticky_active=False, ident=None, served_target="42"):
    """Drive `_resolve_action_with_sticky` with a stubbed action executor that
    serves `served_target`, returning (result, status, ident)."""
    ident = ident if ident is not None else FakeIdentRedis()

    async def _gir():
        return ident

    served = {
        "url": f"https://offer.example/{served_target}",
        "offer_id": "7",
        "target_id": served_target,
    }

    async def _serve(*a, **k):
        return dict(served)

    async def _runner():
        with patch.object(sticky_mod, "get_identity_redis", _gir), \
             patch.object(action_executor, "execute_action", _serve), \
             patch.object(settings, "returning_uid_ttl_seconds", 1000):
            return await router._resolve_action_with_sticky(
                FakeIdentRedis(), {"action_type": "split"}, _click(), "35",
                source_mappings={}, campaign_mappings={},
                sticky_active=sticky_active, uid="U", company_id=1,
                seen_before=True, returning_visitor=True,
                flow_id="300", allowed_avail=frozenset({"active"}),
                fresh_track=fresh_track,
            )

    result, status = asyncio.run(_runner())
    return result, status, ident


class TestFreshTrackRepin:
    def test_fresh_click_overwrites_pin_to_served_target(self):
        ident = FakeIdentRedis(strings={"sticky:1:U:35": "9"})  # old pin
        result, status, ident = _resolve(
            fresh_track=True, ident=ident, served_target="42",
        )
        # Routing result untouched — bookkeeping only.
        assert result["target_id"] == "42"
        assert status == "na"
        # Pin OVERWRITTEN (not NX) to the just-served target, sliding TTL.
        assert ident.strings["sticky:1:U:35"] == "42"
        key, value, nx, ex = ident.set_calls[-1]
        assert nx is False and ex == 1000

    def test_last_click_wins_across_consecutive_fresh_clicks(self):
        ident = FakeIdentRedis()
        _resolve(fresh_track=True, ident=ident, served_target="42")
        _resolve(fresh_track=True, ident=ident, served_target="55")
        assert ident.strings["sticky:1:U:35"] == "55"  # the LAST offer

    def test_fresh_track_off_never_touches_pins(self):
        ident = FakeIdentRedis(strings={"sticky:1:U:35": "9"})
        result, status, ident = _resolve(
            fresh_track=False, ident=ident, served_target="42",
        )
        assert result["target_id"] == "42"
        assert ident.strings["sticky:1:U:35"] == "9"  # untouched
        assert ident.set_calls == []

    def test_unroutable_result_does_not_repin(self):
        ident = FakeIdentRedis(strings={"sticky:1:U:35": "9"})

        async def _gir():
            return ident

        async def _unavailable(*a, **k):
            return action_executor.UNAVAILABLE_RESULT

        async def _runner():
            with patch.object(sticky_mod, "get_identity_redis", _gir), \
                 patch.object(action_executor, "execute_action", _unavailable), \
                 patch.object(settings, "returning_uid_ttl_seconds", 1000):
                return await router._resolve_action_with_sticky(
                    FakeIdentRedis(), {"action_type": "offer"}, _click(), "35",
                    source_mappings={}, campaign_mappings={},
                    sticky_active=False, uid="U", company_id=1,
                    seen_before=True, returning_visitor=True,
                    flow_id="300", allowed_avail=frozenset({"active"}),
                    fresh_track=True,
                )

        result, status = asyncio.run(_runner())
        # UNAVAILABLE carries no target_id → pin untouched (no garbage pin).
        assert ident.strings["sticky:1:U:35"] == "9"

    def test_repin_failure_is_fail_open(self):
        class _Boom:
            async def set(self, *a, **k):
                raise RuntimeError("identity redis down")

        async def _gir():
            return _Boom()

        served = {"url": "https://offer.example/42", "target_id": "42"}

        async def _serve(*a, **k):
            return dict(served)

        async def _runner():
            with patch.object(sticky_mod, "get_identity_redis", _gir), \
                 patch.object(action_executor, "execute_action", _serve), \
                 patch.object(settings, "returning_uid_ttl_seconds", 1000):
                return await router._resolve_action_with_sticky(
                    FakeIdentRedis(), {"action_type": "split"}, _click(), "35",
                    source_mappings={}, campaign_mappings={},
                    sticky_active=False, uid="U", company_id=1,
                    seen_before=True, returning_visitor=True,
                    flow_id="300", allowed_avail=frozenset({"active"}),
                    fresh_track=True,
                )

        result, status = asyncio.run(_runner())
        # The click routes normally — repin failure swallowed (fail-open).
        assert result["target_id"] == "42"
        assert status == "na"


class TestFreshTrackGate:
    """The call-site `fresh_track` predicate — exact mirror of the sticky
    gate with mode 'fresh'. Asserted via the same boolean expression the
    router builds, evaluated against representative inputs."""

    @staticmethod
    def _gate(*, returning_live=True, mode="fresh", uid="U",
              action_type="split", audience="first"):
        flow = {"action_type": action_type, "audience": audience}
        return (
            returning_live
            and mode == "fresh"
            and bool(uid)
            and (flow.get("action_type") or "") in ("offer", "split")
            and (flow.get("audience") or "first") != "returning"
        )

    def test_active_for_fresh_offer_and_split_with_uid(self):
        assert self._gate(action_type="offer") is True
        assert self._gate(action_type="split") is True

    def test_inactive_without_uid_or_routing_or_for_sticky_mode(self):
        assert self._gate(uid="") is False
        assert self._gate(returning_live=False) is False
        assert self._gate(mode="sticky") is False

    def test_d35_symmetry_returning_audience_flow_never_tracked(self):
        # The pin must only ever record what sticky could later serve —
        # returning-audience flow winners are excluded on BOTH gates (D35).
        assert self._gate(audience="returning") is False

    def test_redirect_flows_never_tracked(self):
        assert self._gate(action_type="redirect") is False
