"""P3 — honouring a signed route code at CLICK time (dark by default).

Programme SoT: ``docs/development/route-preview-2026-08-31/00-ANCHOR.md``

A landing page asked ``/preview`` which offer a visitor would get; the node
minted a signed code (``app/route_code.py``); the visitor arrives on the tracker
link carrying it. These tests pin what happens then.

The two properties everything else serves:

  1. **The code is a RE-VALIDATED HINT, never an authority.** Every anomaly —
     forged, expired, foreign tenant, closed target, vanished target, incoherent
     offer/target pairing, Redis fault — falls through to ORDINARY routing,
     silently. A bearer never learns anything from the difference.

  2. **The code never mutates returning-user state** (anchor §21.2). The sticky
     pin outranks it; when sticky is active the code is not consulted at all;
     and the code itself performs no writes of its own.

Harness shape is deliberately the one ``test_fresh_track.py`` established — it
drives the REAL ``_resolve_action_with_sticky`` with the REAL
``action_executor.pinned_target_result``, so these tests exercise the shipped
path rather than a re-implementation of it.
"""

from __future__ import annotations

import asyncio
import time
from unittest.mock import patch

from app import action_executor, route_code, router, sticky as sticky_mod
from app.config import settings
from app.models import ClickRequest

# A key ring the tests own outright. `sign` is only possible with an active kid.
_KEYS = "1:unit-test-route-code-secret,2:second-kid-secret"
_ACTIVE_KID = "1"

_COMPANY = 1
_OFFER = 7
_CODED_TARGET = 99      # what the CODE names
_NORMAL_TARGET = "42"   # what ordinary routing would serve


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


class FakeRoutingRedis:
    """Hash-keyspace stub for the ROUTING reads, with a write recorder.

    Every mutating verb is recorded rather than refused, so a test can assert
    the honouring path wrote NOTHING — and, crucially, so the recorder can be
    CALIBRATED (a test below drives a write through it deliberately and proves
    the recorder goes red). A detector nobody has watched fail is not evidence.
    """

    def __init__(self, hashes: dict[str, dict] | None = None, fail: bool = False):
        self.hashes = hashes or {}
        self.fail = fail
        self.reads: list[str] = []
        self.writes: list[tuple] = []

    async def hgetall(self, key):
        self.reads.append(key)
        if self.fail:
            raise RuntimeError("redis unreachable")
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


def _target_hash(*, offer_id=_OFFER, availability="active",
                 url="https://coded.example/x"):
    return {"url": url, "offer_id": str(offer_id), "availability": availability}


def _click(code):
    return ClickRequest(
        click_id="test-click-rc",
        country="US",
        user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_2)",
        query_params={router.ROUTE_CODE_PARAM: code} if code else {},
    )


def _sign(*, company_id=_COMPANY, offer_id=_OFFER, target_id=_CODED_TARGET,
          ttl=600, now=None):
    with patch.object(settings, "route_code_keys", _KEYS), \
         patch.object(settings, "route_code_active_kid", _ACTIVE_KID):
        return route_code.sign(
            company_id=company_id, offer_id=offer_id,
            offer_target_id=target_id, ttl_seconds=ttl, now=now,
        )


def _resolve(
    *,
    code,
    enabled=True,
    sticky_active=False,
    returning_flow_won=False,
    seen_before=True,
    fresh_track=False,
    hashes=None,
    redis_fail=False,
    ident=None,
    company_id=_COMPANY,
):
    """Drive the REAL resolver; ordinary routing serves `_NORMAL_TARGET`."""
    ident = ident if ident is not None else FakeIdentRedis()
    r = FakeRoutingRedis(
        hashes=hashes if hashes is not None
        else {"offer_target:%d" % _CODED_TARGET: _target_hash()},
        fail=redis_fail,
    )

    async def _gir():
        return ident

    async def _serve(*a, **k):
        return {
            "url": "https://offer.example/" + _NORMAL_TARGET,
            "offer_id": str(_OFFER),
            "target_id": _NORMAL_TARGET,
            "target_selection_path": "split_weighted",
        }

    async def _runner():
        with patch.object(sticky_mod, "get_identity_redis", _gir), \
             patch.object(action_executor, "execute_action", _serve), \
             patch.object(settings, "route_preview_enabled", enabled), \
             patch.object(settings, "route_code_keys", _KEYS), \
             patch.object(settings, "route_code_active_kid", _ACTIVE_KID), \
             patch.object(settings, "returning_uid_ttl_seconds", 1000):
            return await router._resolve_action_with_sticky(
                r, {"action_type": "split"}, _click(code), "35",
                source_mappings={}, campaign_mappings={},
                sticky_active=sticky_active,
                returning_flow_won=returning_flow_won,
                uid="U", company_id=company_id,
                seen_before=seen_before, returning_visitor=True,
                flow_id="300", allowed_avail=frozenset({"active"}),
                fresh_track=fresh_track,
            )

    result, status = asyncio.run(_runner())
    return result, status, r, ident


class TestValidCodeIsHonoured:
    def test_valid_code_serves_the_coded_target(self):
        result, status, r, _ = _resolve(code=_sign())
        assert result["target_id"] == str(_CODED_TARGET)
        assert result["offer_id"] == str(_OFFER)
        assert status == "na"

    def test_honoured_click_is_stamped_route_code(self):
        """anchor §21.3 — provenance rides an EXISTING free-text column, so
        analytics can count preview-followed clicks with no migration."""
        result, _, _, _ = _resolve(code=_sign())
        assert result["target_selection_path"] == "route_code"

    def test_honoured_url_comes_from_the_coded_targets_template(self):
        result, _, _, _ = _resolve(code=_sign())
        assert result["url"].startswith("https://coded.example/")


class TestAnomaliesFallThroughToOrdinaryRouting:
    def _assert_classic(self, result, status):
        assert result["target_id"] == _NORMAL_TARGET
        assert result["target_selection_path"] == "split_weighted"
        assert status == "na"

    def test_forged_signature(self):
        good = _sign()
        payload, _, _sig = good.partition(".")
        forged = payload + "." + ("A" * 43)
        self._assert_classic(*_resolve(code=forged)[:2])

    def test_expired_code(self):
        stale = _sign(ttl=60, now=int(time.time()) - 600)
        self._assert_classic(*_resolve(code=stale)[:2])

    def test_code_from_another_tenant(self):
        """Cross-tenant: the code's claim is checked against the CLICK's
        company, never trusted on its own (rule `multi-tenant-isolation`)."""
        foreign = _sign(company_id=_COMPANY + 1)
        self._assert_classic(*_resolve(code=foreign)[:2])

    def test_target_closed_for_this_clicks_class(self):
        hashes = {"offer_target:%d" % _CODED_TARGET: _target_hash(availability="closed")}
        self._assert_classic(*_resolve(code=_sign(), hashes=hashes)[:2])

    def test_target_vanished_from_redis(self):
        self._assert_classic(*_resolve(code=_sign(), hashes={})[:2])

    def test_target_with_empty_url(self):
        hashes = {"offer_target:%d" % _CODED_TARGET: _target_hash(url="")}
        self._assert_classic(*_resolve(code=_sign(), hashes=hashes)[:2])

    def test_offer_target_pairing_incoherent(self):
        """The target must belong to the offer the code names."""
        hashes = {"offer_target:%d" % _CODED_TARGET: _target_hash(offer_id=_OFFER + 5)}
        self._assert_classic(*_resolve(code=_sign(), hashes=hashes)[:2])

    def test_garbage_in_the_parameter(self):
        self._assert_classic(*_resolve(code="not-a-code-at-all")[:2])

    def test_redis_fault_is_fail_open(self):
        """An optional enhancement must never be able to break a redirect."""
        self._assert_classic(*_resolve(code=_sign(), redis_fail=True)[:2])

    def test_click_with_no_company_resolved(self):
        self._assert_classic(*_resolve(code=_sign(), company_id=None)[:2])


class TestFlagOffIsByteIdentical:
    def test_flag_off_ignores_a_perfectly_valid_code(self):
        result, status, r, _ = _resolve(code=_sign(), enabled=False)
        assert result["target_id"] == _NORMAL_TARGET
        assert result["target_selection_path"] == "split_weighted"
        assert status == "na"

    def test_flag_off_result_equals_the_no_code_result(self):
        off, off_status, off_r, _ = _resolve(code=_sign(), enabled=False)
        none, none_status, none_r, _ = _resolve(code=None, enabled=True)
        assert off == none
        assert off_status == none_status

    def test_flag_off_reads_nothing_extra_from_redis(self):
        """Byte-identical means no EXTRA WORK either — the helper returns on
        its first line, so the coded target is never looked up."""
        _, _, r, _ = _resolve(code=_sign(), enabled=False)
        assert r.reads == []


class TestReturningSystemIsNotDisturbed:
    def test_sticky_pin_beats_the_code(self):
        """anchor §21.2 — a pin is knowledge about a RECOGNISED visitor; a code
        is a guess about an anonymous one. The guess never overwrites it."""
        ident = FakeIdentRedis(strings={"sticky:1:U:35": "555"})
        hashes = {
            "offer_target:%d" % _CODED_TARGET: _target_hash(),
            "offer_target:555": _target_hash(url="https://pinned.example/x"),
        }
        result, status, _, _ = _resolve(
            code=_sign(), sticky_active=True, seen_before=True,
            hashes=hashes, ident=ident,
        )
        assert result["target_id"] == "555"
        assert result["target_selection_path"] == "sticky"
        assert status == "hit"

    def test_code_is_not_consulted_at_all_under_sticky(self):
        """Structural, not incidental: the hook lives only in the
        `not sticky_active` branch, so under sticky the coded target is never
        even READ."""
        ident = FakeIdentRedis(strings={"sticky:1:U:35": "555"})
        hashes = {
            "offer_target:%d" % _CODED_TARGET: _target_hash(),
            "offer_target:555": _target_hash(url="https://pinned.example/x"),
        }
        _, _, r, _ = _resolve(
            code=_sign(), sticky_active=True, seen_before=True,
            hashes=hashes, ident=ident,
        )
        assert ("offer_target:%d" % _CODED_TARGET) not in r.reads

    def test_returning_flow_beats_the_code(self):
        """ADR-0454 term 1 — `returning-flow pick > sticky pin > ROUTE CODE`.

        Full slug (the number names two decisions):
        ADR-0454-route-code-yields-to-the-sticky-pin-and-never-writes-the-returning-system-outranks-a-guess-about-an-anonymous-visitor

        This test did not exist, and its absence is the whole reason the defect
        shipped: the ADR's evidence cited the two STICKY tests above as pinning
        "the priority", and they pin the middle term only.

        The trap is that `sticky_active` is DELIBERATELY False here. The D35
        exclusion (`router.py`, `sticky_active = ... and flow.audience !=
        "returning"`) forces it False precisely so the sticky pin cannot
        override a returning-flow pick — which, with the hook gated on `not
        sticky_active` alone, handed the decision straight to the code.

        Measured live before the fix (staging campaign 333, N=10, BOTH returning
        modes): a returning visitor carrying a preview's code got the FIRST-time
        offer 10/10; without the code, the returning offer 10/10.
        """
        result, status, _, _ = _resolve(
            code=_sign(), sticky_active=False, returning_flow_won=True,
            seen_before=True,
        )
        assert result["target_id"] == _NORMAL_TARGET, (
            "the route code overrode the returning-flow pick — ADR-0454 term 1"
        )
        assert result["target_selection_path"] != "route_code"

    def test_code_is_not_consulted_at_all_when_a_returning_flow_won(self):
        """Structural twin of `test_code_is_not_consulted_at_all_under_sticky`.

        Not merely "the code loses" — it is never READ. A weaker assertion would
        pass against an implementation that consults the code and then discards
        it, which is a different (and still wrong) design.
        """
        _, _, r, _ = _resolve(
            code=_sign(), sticky_active=False, returning_flow_won=True,
            seen_before=True,
        )
        assert ("offer_target:%d" % _CODED_TARGET) not in r.reads

    def test_a_first_audience_flow_still_honours_the_code(self):
        """The calibration in the OTHER direction: the fix must not disable the
        feature. Same call, `returning_flow_won=False`, code still wins."""
        result, _, _, _ = _resolve(
            code=_sign(), sticky_active=False, returning_flow_won=False,
        )
        assert result["target_id"] == str(_CODED_TARGET)

    def test_honouring_a_code_writes_nothing_to_the_routing_keyspace(self):
        _, _, r, _ = _resolve(code=_sign())
        assert r.writes == []

    def test_honouring_a_code_writes_no_sticky_pin(self):
        """No `repin`, no `set_sticky_nx` — the code introduces no write of its
        own into the identity keyspace either."""
        ident = FakeIdentRedis()
        _, _, _, ident = _resolve(code=_sign(), ident=ident, fresh_track=False)
        assert ident.set_calls == []

    def test_fresh_track_bookkeeping_still_records_what_was_SERVED(self):
        """The one write on this path is pre-existing fresh-mode bookkeeping,
        and it is not the code's write — it happens on every fresh click either
        way. Its contract is 'the pin equals the LAST offer the visitor actually
        received', so when the code decides, the pin must record the CODED
        target. Skipping it would leave the pin naming an offer the visitor was
        never sent to — corrupting the returning system, which is precisely what
        §21.2 exists to prevent."""
        ident = FakeIdentRedis()
        result, _, _, ident = _resolve(code=_sign(), ident=ident, fresh_track=True)
        assert result["target_id"] == str(_CODED_TARGET)
        assert ident.strings["sticky:1:U:35"] == str(_CODED_TARGET)

    def test_seen_before_and_the_uid_are_untouched_by_the_code(self):
        """`is_unique`/`seen_before` are computed by the identity resolver from
        visitor signals; the code only ever changes WHICH TARGET is served. The
        honouring path takes no identity redis and calls nothing on it."""
        ident = FakeIdentRedis(strings={"uid:seen": "1"})
        _, _, _, ident = _resolve(code=_sign(), ident=ident)
        assert ident.strings == {"uid:seen": "1"}
        assert ident.set_calls == []


class TestTheDetectorsThemselvesGoRed:
    def test_the_routing_write_recorder_actually_records(self):
        r = FakeRoutingRedis()
        asyncio.run(r.set("k", "v"))
        asyncio.run(r.xadd("s", {"f": "v"}))
        assert [w[0] for w in r.writes] == ["set", "xadd"]

    def test_the_identity_write_recorder_actually_records(self):
        ident = FakeIdentRedis()
        asyncio.run(ident.set("sticky:1:U:35", "42", nx=False, ex=10))
        assert ident.set_calls and ident.strings["sticky:1:U:35"] == "42"

    def test_the_harness_can_tell_the_two_targets_apart(self):
        """If ordinary routing and the coded target were ever the same id, every
        assertion above would pass vacuously."""
        assert str(_CODED_TARGET) != _NORMAL_TARGET
