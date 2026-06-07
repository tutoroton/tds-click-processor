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
    _campaigns_key,
    _hash,
    _profile_key,
    _sig_key,
    persist_identity,
    resolve_identity,
    resolve_via_token,
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


async def _resolve(r, *, company_id=1, fuid=None, vid=None, campaign=None, trusted=False):
    return await resolve_identity(
        r,
        company_id=company_id,
        funnel_user_id=fuid,
        visitor_id=vid,
        campaign_id=campaign,
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
        assert res.is_roaming is False and res.signal_tier == "none"

    async def test_new_vid_user_mints_unique(self):
        r = _fr()
        res = await _resolve(r, vid="VID1", campaign="10")
        assert res.is_unique is True and res.is_returning is False and res.uid
        assert res.is_roaming is False
        assert res.signal_tier == "vid"
        assert await r.get(_sig_key(1, "vid", "VID1")) == res.uid  # minted map

    async def test_returning_same_campaign_is_segment_B(self):
        r = _fr()
        v1 = await _resolve(r, vid="V", campaign="10")
        await persist_identity(
            r, company_id=1, uid=v1.uid, funnel_user_id=None,
            visitor_id="V", campaign_id="10", source_trusted=False, ttl=TTL,
        )
        v2 = await _resolve(r, vid="V", campaign="10")
        assert v2.uid == v1.uid
        assert (v2.is_unique, v2.is_returning, v2.is_roaming) == (False, True, False)  # B

    async def test_roaming_different_campaign_is_segment_C(self):
        r = _fr()
        v1 = await _resolve(r, vid="V", campaign="10")
        await persist_identity(
            r, company_id=1, uid=v1.uid, funnel_user_id=None,
            visitor_id="V", campaign_id="10", source_trusted=False, ttl=TTL,
        )
        v2 = await _resolve(r, vid="V", campaign="20")  # different campaign
        assert v2.uid == v1.uid
        assert (v2.is_unique, v2.is_returning, v2.is_roaming) == (False, False, True)  # C

    @pytest.mark.parametrize(
        "first_campaign,second_campaign,exp_unique,exp_returning,exp_roaming,segment",
        [
            (None, None, True, False, False, "A (new)"),          # first visit
            ("10", "10", False, True, False, "B (same campaign)"),
            ("10", "20", False, False, True, "C (roaming)"),
        ],
    )
    async def test_flag_semantics_table(
        self, first_campaign, second_campaign, exp_unique, exp_returning,
        exp_roaming, segment,
    ):
        r = _fr()
        v1 = await _resolve(r, vid="V", campaign=first_campaign)
        if segment.startswith("A"):
            assert (v1.is_unique, v1.is_returning, v1.is_roaming) == (
                exp_unique, exp_returning, exp_roaming), segment
            return
        await persist_identity(
            r, company_id=1, uid=v1.uid, funnel_user_id=None,
            visitor_id="V", campaign_id=first_campaign, source_trusted=False, ttl=TTL,
        )
        v2 = await _resolve(r, vid="V", campaign=second_campaign)
        assert (v2.is_unique, v2.is_returning, v2.is_roaming) == (
            exp_unique, exp_returning, exp_roaming), segment


# ============================================================
# Concurrency (G7) + multi-tenant (#7)
# ============================================================

class TestConcurrencyAndTenancy:
    async def test_nx_mint_race_converges_to_one_uid(self):
        r = _fr()
        results = await asyncio.gather(
            *[_resolve(r, vid="RACE", campaign="10") for _ in range(2)]
        )
        uids = {x.uid for x in results}
        assert len(uids) == 1, "two concurrent first-clicks must share ONE uid"
        winners = sum(1 for x in results if x.is_unique)
        assert winners == 1, "exactly one click reports is_unique (NX winner)"

    async def test_cross_tenant_same_signal_isolated_uids(self):
        r = _fr()
        a = await _resolve(r, company_id=1, vid="SAME", campaign="10")
        b = await _resolve(r, company_id=2, vid="SAME", campaign="10")
        assert a.uid != b.uid
        assert a.is_unique and b.is_unique  # each is new within its OWN tenant
        assert await r.get(_sig_key(1, "vid", "SAME")) == a.uid
        assert await r.get(_sig_key(2, "vid", "SAME")) == b.uid


# ============================================================
# Signal precedence + trusted-source gate (G6) + provenance (v2 R)
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
        assert res.signal_tier == "funnel_user_id"  # DOC-1 canonical label
        # DOC-1 decouple proof: the Redis KEY tier stays "fuid" (label≠key) so
        # existing identity maps are NOT orphaned by the relabel.
        assert await r.get(_sig_key(1, "fuid", _hash("U"))) == res.uid

    async def test_funnel_user_id_outranks_vid_on_conflict(self):
        r = _fr()
        await r.set(_sig_key(1, "fuid", _hash("U")), "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        await r.set(_sig_key(1, "vid", "V"), "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
        res = await _resolve(r, fuid="U", vid="V", campaign="10", trusted=True)
        assert res.uid == "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"  # highest-precedence (funnel_user_id) wins
        assert res.is_unique is False
        assert res.signal_tier == "funnel_user_id"  # DOC-1 canonical label
        # vid↔fuid resolve to DIFFERENT uids → conflict flagged (log-not-merge),
        # but NOT merged (uid stays the highest-precedence one).
        assert res.identity_conflict is True

    async def test_no_conflict_when_signals_agree(self):
        r = _fr()
        await r.set(_sig_key(1, "fuid", _hash("U")), "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        await r.set(_sig_key(1, "vid", "V"), "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")  # SAME uid
        res = await _resolve(r, fuid="U", vid="V", campaign="10", trusted=True)
        assert res.uid == "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        assert res.identity_conflict is False
        assert res.signal_tier == "funnel_user_id"  # DOC-1 canonical label

    async def test_signal_tier_vid_when_only_vid_resolves(self):
        r = _fr()
        await r.set(_sig_key(1, "vid", "V"), "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
        res = await _resolve(r, vid="V", campaign="10")
        assert res.uid == "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" and res.signal_tier == "vid"
        assert res.identity_conflict is False


# ============================================================
# SEC-M2 — read-back uid shape validation (fail-open as new on malformed)
# ============================================================

class TestSecM2UidValidation:
    async def test_valid_uid_helper(self):
        from app.identity import _valid_uid
        assert _valid_uid("a" * 32) is True               # token_hex(16) shape
        assert _valid_uid("ABCDEF" + "a" * 26) is False   # uppercase rejected
        assert _valid_uid("a" * 31) is False              # too short
        assert _valid_uid("a" * 33) is False              # too long
        assert _valid_uid("../inject" + "a" * 23) is False  # key-injection chars
        assert _valid_uid("") is False and _valid_uid(None) is False

    async def test_malformed_readback_not_adopted_fails_open_new(self):
        # A poisoned/corrupt signal-map value must NEVER be adopted as identity
        # nor concatenated into a sticky/history key. The hit is ignored → mint
        # NX fails on the existing (garbage) key → adopted read-back invalid →
        # uid="" (segment A, new). The garbage is NOT returned.
        r = _fr()
        await r.set(_sig_key(1, "vid", "V"), "this-is-not-a-valid-uid")
        res = await _resolve(r, vid="V", campaign="10")
        assert res.uid == ""                  # garbage NOT adopted
        assert res.is_unique is True          # treated as new
        assert res.is_returning is False and res.is_roaming is False

    async def test_malformed_lower_tier_falls_through_to_valid(self):
        # Top tier (fuid) holds garbage, lower tier (vid) holds a VALID uid →
        # the garbage hit is skipped, the valid vid uid is adopted.
        r = _fr()
        await r.set(_sig_key(1, "fuid", _hash("U")), "garbage")
        await r.set(_sig_key(1, "vid", "V"), "c" * 32)
        res = await _resolve(r, fuid="U", vid="V", campaign="10", trusted=True)
        assert res.uid == "c" * 32 and res.signal_tier == "vid"
        # the malformed fuid value is not counted as a competing identity
        assert res.identity_conflict is False


# ============================================================
# Latency ≤2 RT on the critical path
# ============================================================

class TestLatency:
    async def test_new_user_at_most_two_round_trips(self):
        rt = _RT(_fr())
        await _resolve(rt, vid="V", campaign="10")
        assert rt.rt <= 2, f"new-user critical path took {rt.rt} RT"

    async def test_returning_user_at_most_two_round_trips(self):
        r = _fr()
        v1 = await _resolve(r, vid="V", campaign="10")
        await persist_identity(
            r, company_id=1, uid=v1.uid, funnel_user_id=None,
            visitor_id="V", campaign_id="10", source_trusted=False, ttl=TTL,
        )
        rt = _RT(r)
        await _resolve(rt, vid="V", campaign="10")
        assert rt.rt <= 2, f"returning critical path took {rt.rt} RT"

    async def test_no_signal_zero_round_trips(self):
        rt = _RT(_fr())
        await _resolve(rt, vid=None, fuid=None)
        assert rt.rt == 0

    async def test_returning_with_history_still_two_round_trips(self):
        # P4 RT-budget: reading the prev_* history sets folds into RT#2 (the
        # campaigns SISMEMBER pipeline) → still ≤2 round-trips for a returning user.
        r = _fr()
        v1 = await _resolve(r, vid="V", campaign="10")
        await persist_identity(
            r, company_id=1, uid=v1.uid, funnel_user_id=None,
            visitor_id="V", campaign_id="10", source_trusted=False, ttl=TTL,
        )
        rt = _RT(r)
        res = await resolve_identity(
            rt, company_id=1, funnel_user_id=None, visitor_id="V",
            campaign_id="10", source_trusted=False, ttl=TTL, with_history=True,
        )
        assert rt.rt <= 2, f"returning+history critical path took {rt.rt} RT"
        assert res.is_returning is True  # same campaign

    async def test_with_history_returns_prev_sets(self):
        # The history sets written by the P3 capture surface on the result.
        r = _fr()
        v1 = await _resolve(r, vid="V", campaign="10")  # mints + sets the vid map
        await r.sadd(f"id:1:uid:{v1.uid}:offers", "5", "9")
        await r.sadd(f"id:1:uid:{v1.uid}:targets", "3")
        await r.sadd(f"id:1:uid:{v1.uid}:subs", "aff")
        res = await resolve_identity(
            r, company_id=1, funnel_user_id=None, visitor_id="V",
            campaign_id="10", source_trusted=False, ttl=TTL, with_history=True,
        )
        assert res.prev_offers == frozenset({"5", "9"})
        assert res.prev_targets == frozenset({"3"})
        assert res.prev_subs == frozenset({"aff"})


# ============================================================
# Deferred persist (#8 non-blocking)
# ============================================================

class TestPersist:
    async def test_persist_records_campaign_profile_and_attaches_maps(self):
        r = _fr()
        await persist_identity(
            r, company_id=1, uid="U1", funnel_user_id=None,
            visitor_id="V", campaign_id="10", source_trusted=False, ttl=TTL,
        )
        assert await r.sismember(_campaigns_key(1, "U1"), "10")
        assert await r.get(_sig_key(1, "vid", "V")) == "U1"
        assert await r.hget(_profile_key(1, "U1"), "first_seen")

    async def test_persist_swallows_errors(self):
        class _Boom:
            def pipeline(self, *a, **k):
                raise RuntimeError("redis down")

        # MUST NOT raise — a failed identity write never fails a click.
        await persist_identity(
            _Boom(), company_id=1, uid="U", funnel_user_id=None,
            visitor_id="V", campaign_id="10", source_trusted=False, ttl=TTL,
        )

    async def test_persist_noop_on_empty_uid(self):
        r = _fr()
        await persist_identity(
            r, company_id=1, uid="", funnel_user_id=None,
            visitor_id="V", campaign_id="10", source_trusted=False, ttl=TTL,
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
            return IdentityResult(
                uid="UID-OK", is_unique=False, is_returning=True,
                campaigns_seen=frozenset({"7", "9"}),
            )

        monkeypatch.setattr(identity, "resolve_and_stamp", _stamp)
        _, _, attr = await _build_campaign_attribution(
            _fr(), self._campaign(opt_in=True), "1", _req(),
        )
        assert attr["uid"] == "UID-OK"
        assert attr["is_unique"] is False
        assert attr["is_returning"] is True
        # P3 mint — the router MUST thread company_id + the resolver's
        # campaigns-seen set into attribution so /decide can re-stamp the cookie.
        # (Guards the wiring the main.py injection tests can't see — they mock
        # route() and fabricate attribution.)
        assert attr["company_id"] == 1  # _to_int(_campaign company_id "1")
        assert attr["campaigns_seen"] == frozenset({"7", "9"})


# ============================================================
# P5 — per-company gates + cutover marker + trusted field (2026-06-05)
# ============================================================

from app.router import (
    _company_returning_enabled,
    _company_routing_enabled,
    _source_trusted,
)


class TestP5Gates:
    def test_company_returning_enabled(self):
        assert _company_returning_enabled({"returning_resolver": "1"}) is True
        assert _company_returning_enabled({"returning_resolver": "0"}) is False
        assert _company_returning_enabled({}) is False  # legacy HASH → dark

    def test_company_routing_enabled(self):
        assert _company_routing_enabled({"returning_routing": "1"}) is True
        assert _company_routing_enabled({"returning_routing": "0"}) is False
        assert _company_routing_enabled({}) is False  # legacy HASH → dark

    def test_source_trusted_reads_source_trusted_field(self):
        assert _source_trusted({"source_trusted": "1"}) is True
        assert _source_trusted({"source_trusted": "0"}) is False
        assert _source_trusted({}) is False  # legacy source HASH → not trusted


class TestP5FlagsSemanticsVersion:
    @staticmethod
    def _result(extra_attr=None):
        attr = {"slots": {}, "company_id": 1}
        if extra_attr:
            attr.update(extra_attr)
        return {"attribution": attr, "offer_id": None}

    async def test_version_0_when_resolver_off(self):
        # No resolver output in attr → legacy semantics → marker 0.
        fields = _phase3_attribution_fields(self._result(), _req(), {}, "ts")
        assert fields["flags_semantics_version"] == 0

    async def test_version_1_when_resolver_on(self):
        # Resolver stamped is_unique → canonical semantics → marker 1.
        result = self._result({"uid": "U", "is_unique": True, "is_returning": False})
        fields = _phase3_attribution_fields(result, _req(), {}, "ts")
        assert fields["flags_semantics_version"] == 1


# ============================================================
# v2 R — is_roaming + signal_tier + identity_conflict in the click record
# ============================================================


class TestRProvenanceFields:
    @staticmethod
    def _result(extra_attr=None):
        attr = {"slots": {}, "company_id": 1}
        if extra_attr:
            attr.update(extra_attr)
        return {"attribution": attr, "offer_id": None}

    async def test_defaults_when_resolver_off(self):
        # Resolver OFF (no keys in attr) → default-safe values (additive,
        # no behaviour change on the legacy path).
        fields = _phase3_attribution_fields(self._result(), _req(), {}, "ts")
        assert fields["is_roaming"] is False
        assert fields["signal_tier"] == ""
        assert fields["identity_conflict"] is False

    async def test_populated_from_attribution(self):
        result = self._result({
            "uid": "U", "is_unique": False, "is_returning": False,
            "is_roaming": True, "signal_tier": "funnel_user_id",
            "identity_conflict": True,
        })
        fields = _phase3_attribution_fields(result, _req(), {}, "ts")
        assert fields["is_roaming"] is True
        assert fields["signal_tier"] == "funnel_user_id"
        assert fields["identity_conflict"] is True


# ============================================================
# v2 P0.3 — identity Redis no-eviction boot gate
# ============================================================


class _FakeIdentityRedis:
    """Minimal stand-in for the identity Redis client used by the boot gate.

    Drives the three observable outcomes the gate branches on: reachability
    (``ping``), and the ``maxmemory-policy`` value / readability
    (``config_get``).
    """

    def __init__(self, *, policy="noeviction", reachable=True, policy_readable=True):
        self._policy = policy
        self._reachable = reachable
        self._policy_readable = policy_readable

    async def ping(self):
        if not self._reachable:
            raise ConnectionError("identity redis down")
        return True

    async def config_get(self, key):
        if not self._policy_readable:
            raise RuntimeError("CONFIG GET restricted")
        return {"maxmemory-policy": self._policy}


def _patch_identity_redis(monkeypatch, fake):
    async def _gir():
        return fake

    monkeypatch.setattr(identity, "get_identity_redis", _gir)


class TestIdentityNamespaceGate:
    """The boot gate: DEGRADE-not-refuse in non-local when the identity Redis is
    absent or evicting (2026-06-06 incident fix — a missing optional-feature
    store must never take an edge node offline); warn-only in local; pure no-op
    when the resolver is OFF (dark default → every existing node boots
    byte-identically)."""

    async def test_resolver_off_is_noop_even_nonlocal_empty(self, monkeypatch):
        # The dark default: OFF ⇒ never raises, even in a non-local env with
        # no identity URL (this is exactly today's live state on every node).
        monkeypatch.setattr(settings, "returning_resolver_enabled", False)
        monkeypatch.setattr(settings, "environment", "staging")
        monkeypatch.setattr(settings, "identity_redis_url", "")
        await identity.assert_identity_namespace_safe()  # must not raise

    async def test_local_empty_url_warns_not_raises(self, monkeypatch):
        monkeypatch.setattr(settings, "returning_resolver_enabled", True)
        monkeypatch.setattr(settings, "environment", "local")
        monkeypatch.setattr(settings, "identity_redis_url", "")
        # Even an evicting shared Redis is tolerated in local dev.
        _patch_identity_redis(monkeypatch, _FakeIdentityRedis(policy="allkeys-lru"))
        await identity.assert_identity_namespace_safe()  # must not raise

    async def test_nonlocal_empty_url_degrades(self, monkeypatch):
        # Incident fix (2026-06-06): non-local + resolver ON + no identity URL
        # → DEGRADE (disable the resolver in-memory + alert, then boot) rather
        # than refuse-to-start, so a missing optional-feature store never takes
        # an edge node offline. Routing continues byte-identical (legacy).
        monkeypatch.setattr(settings, "returning_resolver_enabled", True)
        monkeypatch.setattr(settings, "returning_routing_enabled", True)
        monkeypatch.setattr(settings, "environment", "staging")
        monkeypatch.setattr(settings, "identity_redis_url", "")
        await identity.assert_identity_namespace_safe()  # must NOT raise
        assert settings.returning_resolver_enabled is False
        assert settings.returning_routing_enabled is False

    async def test_nonlocal_noeviction_ok(self, monkeypatch):
        monkeypatch.setattr(settings, "returning_resolver_enabled", True)
        monkeypatch.setattr(settings, "environment", "staging")
        monkeypatch.setattr(settings, "identity_redis_url", "redis://id:6379/0")
        _patch_identity_redis(monkeypatch, _FakeIdentityRedis(policy="noeviction"))
        await identity.assert_identity_namespace_safe()  # must not raise

    async def test_nonlocal_eviction_policy_degrades(self, monkeypatch):
        # Incident fix: a CONFIRMED evicting identity Redis in non-local
        # degrades (disable resolver, boot) instead of refusing to start.
        monkeypatch.setattr(settings, "returning_resolver_enabled", True)
        monkeypatch.setattr(settings, "returning_routing_enabled", True)
        monkeypatch.setattr(settings, "environment", "production")
        monkeypatch.setattr(settings, "identity_redis_url", "redis://id:6379/0")
        _patch_identity_redis(monkeypatch, _FakeIdentityRedis(policy="allkeys-lru"))
        await identity.assert_identity_namespace_safe()  # must NOT raise
        assert settings.returning_resolver_enabled is False
        assert settings.returning_routing_enabled is False

    async def test_nonlocal_unreachable_degrades(self, monkeypatch):
        # Incident fix: an unreachable identity Redis in non-local degrades
        # (disable resolver, boot) instead of refusing to start.
        monkeypatch.setattr(settings, "returning_resolver_enabled", True)
        monkeypatch.setattr(settings, "returning_routing_enabled", True)
        monkeypatch.setattr(settings, "environment", "staging")
        monkeypatch.setattr(settings, "identity_redis_url", "redis://id:6379/0")
        _patch_identity_redis(monkeypatch, _FakeIdentityRedis(reachable=False))
        await identity.assert_identity_namespace_safe()  # must NOT raise
        assert settings.returning_resolver_enabled is False
        assert settings.returning_routing_enabled is False

    async def test_nonlocal_unreadable_policy_tolerated(self, monkeypatch):
        # Managed Redis may restrict CONFIG GET — we cannot prove a misconfig,
        # so the gate CRITICAL-logs and allows boot rather than blocking.
        monkeypatch.setattr(settings, "returning_resolver_enabled", True)
        monkeypatch.setattr(settings, "environment", "staging")
        monkeypatch.setattr(settings, "identity_redis_url", "redis://id:6379/0")
        _patch_identity_redis(monkeypatch, _FakeIdentityRedis(policy_readable=False))
        await identity.assert_identity_namespace_safe()  # must not raise


# ============================================================
# P2 — signed `_tds_id` token dual-accept (in-process recognition)
# ============================================================
from app import identity_token as idtok  # noqa: E402

_TOK_KEY = "k" * 40
_TOK_UID = "0123456789abcdef0123456789abcdef"


def _enable_codec(monkeypatch, *, keys=f"1:{_TOK_KEY}", active="1"):
    monkeypatch.setattr(settings, "identity_cookie_keys", keys)
    monkeypatch.setattr(settings, "identity_cookie_active_kid", active)


def _mk_token(*, company_id, uid=_TOK_UID, seen=None, exp=None, kid=1):
    import time as _t
    return idtok.sign(
        company_id=company_id, uid=uid, first_seen=1,
        exp=exp if exp is not None else int(_t.time()) + 3600,
        seen=seen or [], kid=kid,
    )


class _GetSpy:
    """Wraps a fakeredis client and records every direct GET key + every
    pipeline GET key, so a test can assert NO `vid→uid` GET was issued."""

    def __init__(self, real):
        self._real = real
        self.get_keys: list = []

    def pipeline(self, *a, **k):
        return _GetSpyPipe(self._real.pipeline(*a, **k), self)

    def __getattr__(self, name):
        attr = getattr(self._real, name)
        if name == "get":
            async def spy_get(key, *a, **k):
                self.get_keys.append(key)
                return await attr(key, *a, **k)
            return spy_get
        return attr


class _GetSpyPipe:
    def __init__(self, real, parent):
        self._real = real
        self._parent = parent

    def get(self, key, *a, **k):
        self._parent.get_keys.append(key)
        return self._real.get(key, *a, **k)

    def __getattr__(self, name):
        return getattr(self._real, name)


class TestTokenDualAccept:
    async def test_valid_token_skips_vid_uid_get(self, monkeypatch):
        _enable_codec(monkeypatch)
        spy = _GetSpy(_fr())
        tok = _mk_token(company_id=1)
        res = await resolve_identity(
            spy, company_id=1, funnel_user_id=None, visitor_id="VID1",
            campaign_id="10", source_trusted=False, ttl=TTL, identity_token=tok,
        )
        assert res.uid == _TOK_UID
        assert res.is_unique is False           # token proves seen-before
        assert res.signal_tier == "token"
        # The decisive assertion: NO `id:1:vid:VID1` GET was issued — recognition
        # was an in-process HMAC verify, zero store hit for the uid.
        assert _sig_key(1, "vid", "VID1") not in spy.get_keys
        assert not any(str(k).startswith("id:1:vid:") for k in spy.get_keys)

    async def test_token_seen_hint_makes_returning(self, monkeypatch):
        _enable_codec(monkeypatch)
        r = _fr()
        # Token carries campaign 10 in its seen hint → same-campaign return = B.
        tok = _mk_token(company_id=1, seen=[10])
        res = await resolve_identity(
            r, company_id=1, funnel_user_id=None, visitor_id=None,
            campaign_id="10", source_trusted=False, ttl=TTL, identity_token=tok,
        )
        assert (res.is_returning, res.is_roaming) == (True, False)
        # Hint was unioned into the LOCAL set.
        assert await r.sismember(_campaigns_key(1, _TOK_UID), "10")

    async def test_token_roaming_different_campaign(self, monkeypatch):
        _enable_codec(monkeypatch)
        r = _fr()
        tok = _mk_token(company_id=1, seen=[10])  # seen 10, now hitting 20
        res = await resolve_identity(
            r, company_id=1, funnel_user_id=None, visitor_id=None,
            campaign_id="20", source_trusted=False, ttl=TTL, identity_token=tok,
        )
        assert (res.is_returning, res.is_roaming) == (False, True)

    async def test_company_mismatch_not_adopted(self, monkeypatch):
        """Multi-tenant: a token minted for company 1 presented on company 2 ⇒
        token NOT adopted; falls back to legacy → fresh mint for company 2 with
        a DIFFERENT uid. No cross-tenant identity adoption."""
        _enable_codec(monkeypatch)
        r = _fr()
        tok = _mk_token(company_id=1, uid=_TOK_UID)
        res = await resolve_identity(
            r, company_id=2, funnel_user_id=None, visitor_id="VID2",
            campaign_id="10", source_trusted=False, ttl=TTL, identity_token=tok,
        )
        assert res.uid != _TOK_UID          # did NOT adopt the cross-tenant uid
        assert res.is_unique is True        # fell back → fresh mint for company 2
        assert res.signal_tier == "vid"     # legacy path won, not token

    async def test_malformed_token_falls_back(self, monkeypatch):
        _enable_codec(monkeypatch)
        r = _fr()
        res = await resolve_identity(
            r, company_id=1, funnel_user_id=None, visitor_id="VID1",
            campaign_id="10", source_trusted=False, ttl=TTL,
            identity_token="garbage.notavalidsig",
        )
        assert res.is_unique is True and res.signal_tier == "vid"  # legacy mint

    async def test_expired_token_falls_back(self, monkeypatch):
        _enable_codec(monkeypatch)
        r = _fr()
        tok = _mk_token(company_id=1, exp=1)  # long past
        res = await resolve_identity(
            r, company_id=1, funnel_user_id=None, visitor_id="VID1",
            campaign_id="10", source_trusted=False, ttl=TTL, identity_token=tok,
        )
        assert res.signal_tier == "vid"  # token expired → legacy path

    async def test_codec_disabled_byte_identical(self, monkeypatch):
        """Gate OFF (no keys): even a structurally valid token is ignored ⇒
        byte-identical legacy resolution."""
        # Build a token while codec ON...
        _enable_codec(monkeypatch)
        tok = _mk_token(company_id=1)
        # ...then disable the codec → verify fails-open, legacy path runs.
        monkeypatch.setattr(settings, "identity_cookie_keys", "")
        monkeypatch.setattr(settings, "identity_cookie_active_kid", "")
        r = _fr()
        res = await resolve_identity(
            r, company_id=1, funnel_user_id=None, visitor_id="VID1",
            campaign_id="10", source_trusted=False, ttl=TTL, identity_token=tok,
        )
        assert res.is_unique is True and res.signal_tier == "vid"

    async def test_no_token_unchanged_legacy(self, monkeypatch):
        _enable_codec(monkeypatch)
        r = _fr()
        res = await resolve_identity(
            r, company_id=1, funnel_user_id=None, visitor_id="VID1",
            campaign_id="10", source_trusted=False, ttl=TTL, identity_token=None,
        )
        assert res.is_unique is True and res.signal_tier == "vid"

    async def test_seen_hint_retrimmed_to_max_seen(self, monkeypatch):
        """SEC-LOW-03 (audit-2): a forged/compromised token whose claims carry
        MORE than MAX_SEEN campaigns (bypassing the codec's own decode cap) must
        still union at most MAX_SEEN entries into the local campaigns-seen set.
        We simulate the bypass by patching `idtok.verify` to hand back oversized
        claims (a real `verify` would reject count > MAX_SEEN)."""
        import time as _t
        _enable_codec(monkeypatch)
        r = _fr()
        oversized = list(range(100, 100 + idtok.MAX_SEEN + 20))  # 36 > 16
        fake_claims = {
            "v": 1, "kid": 1, "c": 1, "u": _TOK_UID, "fs": 1,
            "exp": int(_t.time()) + 3600, "seen": oversized,
        }
        monkeypatch.setattr(idtok, "verify", lambda *a, **k: fake_claims)
        res = await resolve_identity(
            r, company_id=1, funnel_user_id=None, visitor_id=None,
            campaign_id="999", source_trusted=False, ttl=TTL,
            identity_token="forged.token",
        )
        assert res.uid == _TOK_UID
        members = await r.smembers(_campaigns_key(1, _TOK_UID))
        assert len(members) <= idtok.MAX_SEEN  # SADD fan-out bounded

    async def test_token_fault_emits_throttled_telemetry(self, monkeypatch):
        """SEC-LOW-02 (audit-2): a token-path Redis fault (seen-union/read)
        surfaces a throttled Sentry signal (OP_IDENTITY) instead of degrading
        silently. Recognition still succeeds (uid from the signed token).

        CF-1 form-4b (2026-06-07): the degraded branch now honours the signed
        `seen` hint — a SAME-campaign returner is classified is_returning even
        during the Redis blip (the hint is proof the uid saw this campaign).
        Only a campaign NOT in the hint degrades to roaming (covered below)."""
        _enable_codec(monkeypatch)
        tok = _mk_token(company_id=1, seen=[10])
        captured: list = []
        monkeypatch.setattr(
            identity, "capture_op_msg_throttled",
            lambda op, dedup, msg, **kw: captured.append((op, dedup, msg, kw)) or True,
        )

        class _BoomPipe:
            def __getattr__(self, name):
                if name == "execute":
                    async def _ex():
                        raise RuntimeError("redis down")
                    return _ex
                return lambda *a, **k: None

        class _BoomRedis:
            def pipeline(self, *a, **k):
                return _BoomPipe()

        res = await resolve_identity(
            _BoomRedis(), company_id=1, funnel_user_id=None, visitor_id=None,
            campaign_id="10", source_trusted=False, ttl=TTL, identity_token=tok,
        )
        # Degraded but recognized — fail-open. CF-1 form-4b: hint proves campaign
        # 10 was seen → is_returning even on the degraded branch.
        assert res.uid == _TOK_UID
        assert (res.is_returning, res.is_roaming) == (True, False)
        # Telemetry fired with the canonical OP_IDENTITY op tag.
        assert captured and captured[0][0] == identity.OP_IDENTITY

    async def test_token_fault_different_campaign_degrades_roaming(self, monkeypatch):
        """CF-1 form-4b complement: on the degraded branch a campaign NOT in the
        signed hint still degrades to roaming (seen-before, not provably this
        campaign) — the safe non-first classification is preserved."""
        _enable_codec(monkeypatch)
        tok = _mk_token(company_id=1, seen=[10])  # hint covers 10, we hit 20

        class _BoomPipe:
            def __getattr__(self, name):
                if name == "execute":
                    async def _ex():
                        raise RuntimeError("redis down")
                    return _ex
                return lambda *a, **k: None

        class _BoomRedis:
            def pipeline(self, *a, **k):
                return _BoomPipe()

        res = await resolve_identity(
            _BoomRedis(), company_id=1, funnel_user_id=None, visitor_id=None,
            campaign_id="20", source_trusted=False, ttl=TTL, identity_token=tok,
        )
        assert res.uid == _TOK_UID
        assert (res.is_returning, res.is_roaming) == (False, True)
        # Hint still carried forward for the cookie re-stamp (no shrink).
        assert "10" in res.campaigns_seen

    async def test_token_seen_hint_returning_on_commit_false(self, monkeypatch):
        """CF-1: cold local set + signed hint=[10], hitting campaign 10 with
        commit=False (the domain fall-through branch) must classify
        is_returning=True WITHOUT a SADD. Pre-fix this read the COLD local set
        (union skipped on commit=False) → mis-stamped is_returning=False /
        is_roaming=True for a cross-node returner on its first cold-node hit."""
        _enable_codec(monkeypatch)
        r = _fr()
        tok = _mk_token(company_id=1, seen=[10])
        res = await resolve_via_token(
            r, company_id=1, identity_token=tok, campaign_id="10", commit=False,
        )
        assert (res.is_returning, res.is_roaming) == (True, False)
        # Side-effect-free: the SADD was deferred — the local set stays COLD.
        assert not await r.sismember(_campaigns_key(1, _TOK_UID), "10")
        # The cookie hint is preserved for the re-stamp (no shrink).
        assert "10" in res.campaigns_seen

    async def test_token_commit_false_no_cookie_shrink(self, monkeypatch):
        """CF-1 ALT#1: a multi-campaign cookie must NOT lose history on a cold
        commit=False hit. Pre-fix campaigns_seen = the cold SMEMBERS (empty) →
        the re-stamp dropped campaigns 20 & 30 from the durable cookie."""
        _enable_codec(monkeypatch)
        r = _fr()
        tok = _mk_token(company_id=1, seen=[10, 20, 30])
        res = await resolve_via_token(
            r, company_id=1, identity_token=tok, campaign_id="10", commit=False,
        )
        assert {"10", "20", "30"} <= set(res.campaigns_seen)
        # No write happened — the local set is still cold.
        assert not await r.smembers(_campaigns_key(1, _TOK_UID))

    async def test_token_commit_true_byte_identical(self, monkeypatch):
        """CF-1 invariant: the commit=True path is unchanged — the union SADD
        runs, the SISMEMBER reads warm, and the `or hint` is redundant. A
        different-campaign hit still classifies roaming (bucket ∉ hint)."""
        _enable_codec(monkeypatch)
        r = _fr()
        tok = _mk_token(company_id=1, seen=[10])
        # Same campaign → returning, AND the SADD landed (commit=True).
        same = await resolve_via_token(
            r, company_id=1, identity_token=tok, campaign_id="10", commit=True,
        )
        assert (same.is_returning, same.is_roaming) == (True, False)
        assert await r.sismember(_campaigns_key(1, _TOK_UID), "10")
        # Different campaign → roaming (bucket "20" ∉ hint {10}).
        diff = await resolve_via_token(
            r, company_id=1, identity_token=tok, campaign_id="20", commit=True,
        )
        assert (diff.is_returning, diff.is_roaming) == (False, True)
