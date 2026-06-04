"""Returning-user identity resolver tests (P2, 2026-06-05).

Covers the R4-audit gates that make P2 acceptable:
  * T2  — OFF ⇒ byte-identical record (the provable regression proof).
  * T4  — fail-open: resolver raises ⇒ legacy flags, click proceeds.
  * gate #1 — toggle-first-no-IO: OFF ⇒ resolver never invoked (zero identity I/O).
  * G7  — NX-mint race: two concurrent first-clicks ⇒ one uid, one is_unique.
  * #7  — cross-tenant isolation: same signal in two companies ⇒ isolated uids.
  * flags A/B/C, signal precedence + trusted-source gate (G6), latency ≤2 RT,
    non-blocking persist (#8).
"""

from __future__ import annotations

import asyncio

import fakeredis.aioredis
import pytest

from app import identity
from app.config import settings
from app.identity import (
    IdentityResult,
    _funnels_key,
    _hash,
    _profile_key,
    _sig_key,
    persist_identity,
    resolve_identity,
)
from app.models import ClickRequest
from app.router import _build_campaign_attribution
from app.main import _phase3_attribution_fields

pytestmark = pytest.mark.asyncio

TTL = 1000


def _fr():
    return fakeredis.aioredis.FakeRedis(decode_responses=True)


def _req(**kw):
    """Minimal valid ClickRequest (only click_id is required)."""
    return ClickRequest(click_id="0" * 24, **kw)


async def _resolve(r, *, company_id=1, fuid=None, vid=None, funnel=None, trusted=False):
    return await resolve_identity(
        r,
        company_id=company_id,
        funnel_user_id=fuid,
        visitor_id=vid,
        funnel_id=funnel,
        source_trusted=trusted,
        ttl=TTL,
    )


# ============================================================
# Round-trip counter (latency proof)
# ============================================================

class _RTPipe:
    def __init__(self, real, parent):
        self._real = real
        self._parent = parent

    def __getattr__(self, name):
        return getattr(self._real, name)

    async def execute(self):
        self._parent.rt += 1
        return await self._real.execute()


class _RT:
    """Wraps a fakeredis client and counts round-trips: one per pipeline
    `.execute()` and one per direct awaited command."""

    _COUNTED = {"get", "set", "sismember", "sadd", "expire", "hsetnx", "scard"}

    def __init__(self, real):
        self._real = real
        self.rt = 0

    def pipeline(self, *a, **k):
        return _RTPipe(self._real.pipeline(*a, **k), self)

    def __getattr__(self, name):
        attr = getattr(self._real, name)
        if name in self._COUNTED:
            async def counted(*a, **k):
                self.rt += 1
                return await attr(*a, **k)
            return counted
        return attr


# ============================================================
# Core resolution + flags A/B/C
# ============================================================

class TestResolveCore:
    async def test_no_signal_is_unique_new_segment_A(self):
        # cookie-less, untrusted funnel → no persistent identity (segment A).
        res = await _resolve(_fr(), vid=None, fuid="U", trusted=False)
        assert res == IdentityResult(uid="", is_unique=True, is_returning=False)

    async def test_new_vid_user_mints_unique(self):
        r = _fr()
        res = await _resolve(r, vid="VID1", funnel="F")
        assert res.is_unique is True and res.is_returning is False and res.uid
        assert await r.get(_sig_key(1, "vid", "VID1")) == res.uid  # minted map

    async def test_returning_same_funnel_is_segment_B(self):
        r = _fr()
        v1 = await _resolve(r, vid="V", funnel="F")
        await persist_identity(
            r, company_id=1, uid=v1.uid, funnel_user_id=None,
            visitor_id="V", funnel_id="F", source_trusted=False, ttl=TTL,
        )
        v2 = await _resolve(r, vid="V", funnel="F")
        assert v2.uid == v1.uid
        assert (v2.is_unique, v2.is_returning) == (False, True)  # B

    async def test_returning_new_funnel_is_segment_C(self):
        r = _fr()
        v1 = await _resolve(r, vid="V", funnel="F")
        await persist_identity(
            r, company_id=1, uid=v1.uid, funnel_user_id=None,
            visitor_id="V", funnel_id="F", source_trusted=False, ttl=TTL,
        )
        v2 = await _resolve(r, vid="V", funnel="G")  # different funnel
        assert v2.uid == v1.uid
        assert (v2.is_unique, v2.is_returning) == (False, False)  # C

    @pytest.mark.parametrize(
        "first_funnel,second_funnel,exp_unique,exp_returning,segment",
        [
            (None, None, True, False, "A (new)"),          # first visit
            ("F", "F", False, True, "B (same funnel)"),
            ("F", "G", False, False, "C (new funnel)"),
        ],
    )
    async def test_flag_semantics_table(
        self, first_funnel, second_funnel, exp_unique, exp_returning, segment,
    ):
        r = _fr()
        v1 = await _resolve(r, vid="V", funnel=first_funnel)
        if segment.startswith("A"):
            assert (v1.is_unique, v1.is_returning) == (exp_unique, exp_returning), segment
            return
        await persist_identity(
            r, company_id=1, uid=v1.uid, funnel_user_id=None,
            visitor_id="V", funnel_id=first_funnel, source_trusted=False, ttl=TTL,
        )
        v2 = await _resolve(r, vid="V", funnel=second_funnel)
        assert (v2.is_unique, v2.is_returning) == (exp_unique, exp_returning), segment


# ============================================================
# Concurrency (G7) + multi-tenant (#7)
# ============================================================

class TestConcurrencyAndTenancy:
    async def test_nx_mint_race_converges_to_one_uid(self):
        r = _fr()
        results = await asyncio.gather(
            *[_resolve(r, vid="RACE", funnel="F") for _ in range(2)]
        )
        uids = {x.uid for x in results}
        assert len(uids) == 1, "two concurrent first-clicks must share ONE uid"
        winners = sum(1 for x in results if x.is_unique)
        assert winners == 1, "exactly one click reports is_unique (NX winner)"

    async def test_cross_tenant_same_signal_isolated_uids(self):
        r = _fr()
        a = await _resolve(r, company_id=1, vid="SAME", funnel="F")
        b = await _resolve(r, company_id=2, vid="SAME", funnel="F")
        assert a.uid != b.uid
        assert a.is_unique and b.is_unique  # each is new within its OWN tenant
        assert await r.get(_sig_key(1, "vid", "SAME")) == a.uid
        assert await r.get(_sig_key(2, "vid", "SAME")) == b.uid


# ============================================================
# Signal precedence + trusted-source gate (G6)
# ============================================================

class TestSignalGating:
    async def test_funnel_user_id_ignored_when_untrusted(self):
        # untrusted source → funnel_user_id is NOT identity; no vid → segment A.
        res = await _resolve(_fr(), fuid="U", vid=None, trusted=False)
        assert res.uid == ""

    async def test_funnel_user_id_used_when_trusted(self):
        r = _fr()
        res = await _resolve(r, fuid="U", vid=None, trusted=True)
        assert res.uid and res.is_unique
        assert await r.get(_sig_key(1, "fuid", _hash("U"))) == res.uid

    async def test_funnel_user_id_outranks_vid_on_conflict(self):
        r = _fr()
        await r.set(_sig_key(1, "fuid", _hash("U")), "uidA")
        await r.set(_sig_key(1, "vid", "V"), "uidB")
        res = await _resolve(r, fuid="U", vid="V", funnel="F", trusted=True)
        assert res.uid == "uidA"  # highest-precedence (funnel_user_id) wins
        assert res.is_unique is False


# ============================================================
# Latency ≤2 RT on the critical path
# ============================================================

class TestLatency:
    async def test_new_user_at_most_two_round_trips(self):
        rt = _RT(_fr())
        await _resolve(rt, vid="V", funnel="F")
        assert rt.rt <= 2, f"new-user critical path took {rt.rt} RT"

    async def test_returning_user_at_most_two_round_trips(self):
        r = _fr()
        v1 = await _resolve(r, vid="V", funnel="F")
        await persist_identity(
            r, company_id=1, uid=v1.uid, funnel_user_id=None,
            visitor_id="V", funnel_id="F", source_trusted=False, ttl=TTL,
        )
        rt = _RT(r)
        await _resolve(rt, vid="V", funnel="F")
        assert rt.rt <= 2, f"returning critical path took {rt.rt} RT"

    async def test_no_signal_zero_round_trips(self):
        rt = _RT(_fr())
        await _resolve(rt, vid=None, fuid=None)
        assert rt.rt == 0

    async def test_returning_with_history_still_two_round_trips(self):
        # P4 RT-budget: reading the prev_* history sets folds into RT#2 (the
        # funnels SISMEMBER pipeline) → still ≤2 round-trips for a returning user.
        r = _fr()
        v1 = await _resolve(r, vid="V", funnel="F")
        await persist_identity(
            r, company_id=1, uid=v1.uid, funnel_user_id=None,
            visitor_id="V", funnel_id="F", source_trusted=False, ttl=TTL,
        )
        rt = _RT(r)
        res = await resolve_identity(
            rt, company_id=1, funnel_user_id=None, visitor_id="V",
            funnel_id="F", source_trusted=False, ttl=TTL, with_history=True,
        )
        assert rt.rt <= 2, f"returning+history critical path took {rt.rt} RT"
        assert res.is_returning is True  # same funnel

    async def test_with_history_returns_prev_sets(self):
        # The history sets written by the P3 capture surface on the result.
        r = _fr()
        v1 = await _resolve(r, vid="V", funnel="F")  # mints + sets the vid map
        await r.sadd(f"id:1:uid:{v1.uid}:offers", "5", "9")
        await r.sadd(f"id:1:uid:{v1.uid}:targets", "3")
        await r.sadd(f"id:1:uid:{v1.uid}:subs", "aff")
        res = await resolve_identity(
            r, company_id=1, funnel_user_id=None, visitor_id="V",
            funnel_id="F", source_trusted=False, ttl=TTL, with_history=True,
        )
        assert res.prev_offers == frozenset({"5", "9"})
        assert res.prev_targets == frozenset({"3"})
        assert res.prev_subs == frozenset({"aff"})


# ============================================================
# Deferred persist (#8 non-blocking)
# ============================================================

class TestPersist:
    async def test_persist_records_funnel_profile_and_attaches_maps(self):
        r = _fr()
        await persist_identity(
            r, company_id=1, uid="U1", funnel_user_id=None,
            visitor_id="V", funnel_id="F", source_trusted=False, ttl=TTL,
        )
        assert await r.sismember(_funnels_key(1, "U1"), "F")
        assert await r.get(_sig_key(1, "vid", "V")) == "U1"
        assert await r.hget(_profile_key(1, "U1"), "first_seen")

    async def test_persist_swallows_errors(self):
        class _Boom:
            def pipeline(self, *a, **k):
                raise RuntimeError("redis down")

        # MUST NOT raise — a failed identity write never fails a click.
        await persist_identity(
            _Boom(), company_id=1, uid="U", funnel_user_id=None,
            visitor_id="V", funnel_id="F", source_trusted=False, ttl=TTL,
        )

    async def test_persist_noop_on_empty_uid(self):
        r = _fr()
        await persist_identity(
            r, company_id=1, uid="", funnel_user_id=None,
            visitor_id="V", funnel_id="F", source_trusted=False, ttl=TTL,
        )
        assert await r.get(_sig_key(1, "vid", "V")) is None  # nothing written


# ============================================================
# T2 — OFF byte-identical (the provable regression proof)
# ============================================================

class TestPhase3FlagsByteIdentical:
    @staticmethod
    def _result(extra_attr=None):
        attr = {"slots": {}, "company_id": 1}
        if extra_attr:
            attr.update(extra_attr)
        return {"attribution": attr, "offer_id": None}

    async def test_off_record_matches_legacy_semantics(self):
        # Resolver OFF ⇒ attribution carries NO uid/flag keys ⇒ phase-3 falls
        # back to the EXACT legacy computation. This is the byte-identical proof.
        for legacy_is_returning in (False, True):
            req = _req(is_returning=legacy_is_returning)
            fields = _phase3_attribution_fields(self._result(), req, {}, "2026-06-05T00:00:00.000Z")
            assert fields["is_unique"] == (not legacy_is_returning)   # legacy rule
            assert fields["is_returning"] == legacy_is_returning      # legacy rule
            assert fields["uid"] == ""                                 # dark column

    async def test_on_record_uses_resolver_values(self):
        # Resolver ON stamps canonical values that OVERRIDE the legacy literal.
        req = _req(is_returning=True)  # legacy would be (unique=False, ret=True)
        result = self._result({"uid": "U9", "is_unique": True, "is_returning": False})
        fields = _phase3_attribution_fields(result, req, {}, "2026-06-05T00:00:00.000Z")
        assert fields["uid"] == "U9"
        assert fields["is_unique"] is True
        assert fields["is_returning"] is False


# ============================================================
# Router gates: #1 (no-IO when OFF) + V1 (fail-open)
# ============================================================

class TestRouterGate:
    @staticmethod
    def _campaign(opt_in: bool):
        c = {"company_id": "1"}
        if opt_in:
            c["returning_resolver"] = "1"
        return c

    async def test_off_skips_resolver_no_io(self, monkeypatch):
        monkeypatch.setattr(settings, "returning_resolver_enabled", False)

        async def _boom(**k):
            raise AssertionError("resolver must not be called when OFF")

        monkeypatch.setattr(identity, "resolve_and_stamp", _boom)
        _, _, attr = await _build_campaign_attribution(
            _fr(), self._campaign(opt_in=True), "1", _req(),
        )
        assert "uid" not in attr and "is_unique" not in attr

    async def test_company_opt_out_skips_resolver(self, monkeypatch):
        monkeypatch.setattr(settings, "returning_resolver_enabled", True)

        async def _boom(**k):
            raise AssertionError("resolver must not run for an opted-out company")

        monkeypatch.setattr(identity, "resolve_and_stamp", _boom)
        _, _, attr = await _build_campaign_attribution(
            _fr(), self._campaign(opt_in=False), "1", _req(),
        )
        assert "uid" not in attr

    async def test_fail_open_when_resolver_raises(self, monkeypatch):
        monkeypatch.setattr(settings, "returning_resolver_enabled", True)

        async def _explode(**k):
            raise RuntimeError("resolver exploded")

        monkeypatch.setattr(identity, "resolve_and_stamp", _explode)
        # MUST NOT raise; attribution stays legacy (no uid/flag keys).
        _, _, attr = await _build_campaign_attribution(
            _fr(), self._campaign(opt_in=True), "1", _req(),
        )
        assert "uid" not in attr and "is_unique" not in attr and "is_returning" not in attr

    async def test_on_stamps_resolver_output(self, monkeypatch):
        monkeypatch.setattr(settings, "returning_resolver_enabled", True)

        async def _stamp(**k):
            return IdentityResult(uid="UID-OK", is_unique=False, is_returning=True)

        monkeypatch.setattr(identity, "resolve_and_stamp", _stamp)
        _, _, attr = await _build_campaign_attribution(
            _fr(), self._campaign(opt_in=True), "1", _req(),
        )
        assert attr["uid"] == "UID-OK"
        assert attr["is_unique"] is False
        assert attr["is_returning"] is True
