"""Returning-user previous-visit history capture tests (P3, 2026-06-05).

Pure capture (DARK): proves the post-route outcome lands in the uid's
company-scoped, capped sets, fires only when gated (uid present), is bounded,
idempotent, and never raises.
"""

from __future__ import annotations

import asyncio

import fakeredis.aioredis
import pytest

from app import history
from app.history import (
    _CAP,
    _accumulate_capped,
    _offers_key,
    _subs_key,
    _targets_key,
    capture_from_record,
    schedule_capture,
)

pytestmark = pytest.mark.asyncio
TTL = 1000


def _fr():
    return fakeredis.aioredis.FakeRedis(decode_responses=True)


def _record(uid="U1", company_id="1", offer_id=5, target_id=9, subs=None):
    rec = {
        "uid": uid,
        "company_id": company_id,
        "offer_id": offer_id,
        "offer_target_id": target_id,
    }
    subs = subs or {}
    for i in range(1, 21):
        rec[f"sub{i}"] = subs.get(i)
    return rec


def _patch_redis(monkeypatch, r):
    async def _gir():
        return r
    monkeypatch.setattr(history, "get_identity_redis", _gir)


class TestCapture:
    async def test_records_offer_target_and_subs(self, monkeypatch):
        r = _fr()
        _patch_redis(monkeypatch, r)
        await capture_from_record(
            _record(offer_id=5, target_id=9, subs={1: "aff", 3: "camp"})
        )
        assert await r.sismember(_offers_key(1, "U1"), "5")
        assert await r.sismember(_targets_key(1, "U1"), "9")
        assert await r.sismember(_subs_key(1, "U1"), "aff")
        assert await r.sismember(_subs_key(1, "U1"), "camp")

    async def test_gated_noop_when_uid_empty(self, monkeypatch):
        r = _fr()
        _patch_redis(monkeypatch, r)
        await capture_from_record(_record(uid="", offer_id=5))
        assert await r.keys("id:*") == []  # zero writes

    async def test_gated_noop_when_company_zero(self, monkeypatch):
        r = _fr()
        _patch_redis(monkeypatch, r)
        await capture_from_record(_record(company_id="0", offer_id=5))
        assert await r.keys("id:*") == []

    async def test_no_outcome_no_write(self, monkeypatch):
        # A fallback / no-offer click carries no outcome → nothing recorded.
        r = _fr()
        _patch_redis(monkeypatch, r)
        await capture_from_record(_record(offer_id=0, target_id=0, subs={}))
        assert await r.keys("id:*") == []

    async def test_idempotent_sadd(self, monkeypatch):
        r = _fr()
        _patch_redis(monkeypatch, r)
        rec = _record(offer_id=5)
        await capture_from_record(rec)
        await capture_from_record(rec)
        assert await r.scard(_offers_key(1, "U1")) == 1  # re-capture = no growth

    async def test_company_scoped_isolation(self, monkeypatch):
        r = _fr()
        _patch_redis(monkeypatch, r)
        await capture_from_record(_record(company_id="1", offer_id=5))
        await capture_from_record(_record(company_id="2", offer_id=5))
        assert _offers_key(1, "U1") != _offers_key(2, "U1")
        assert await r.sismember(_offers_key(1, "U1"), "5")
        assert await r.sismember(_offers_key(2, "U1"), "5")
        # company-1's set is a DISTINCT key from company-2's (no cross-read).
        assert await r.scard(_offers_key(1, "U1")) == 1
        assert await r.scard(_offers_key(2, "U1")) == 1

    async def test_swallows_redis_errors(self, monkeypatch):
        class _Boom:
            def pipeline(self, *a, **k):
                raise RuntimeError("redis down")

        async def _gir():
            return _Boom()

        monkeypatch.setattr(history, "get_identity_redis", _gir)
        # MUST NOT raise — a failed history write never affects the click.
        await capture_from_record(_record(offer_id=5))


class TestCap:
    async def test_cap_enforced_single_call(self):
        r = _fr()
        key = _subs_key(1, "U1")
        await _accumulate_capped(r, [(key, [f"v{i}" for i in range(25)])], TTL)
        assert await r.scard(key) == _CAP  # 25 distinct → bounded to 20

    async def test_cap_enforced_across_calls(self, monkeypatch):
        r = _fr()
        _patch_redis(monkeypatch, r)
        # 3 clicks, each carrying 20 DISTINCT sub values → would be 60 unbounded.
        for c in range(3):
            subs = {i: f"c{c}s{i}" for i in range(1, 21)}
            await capture_from_record(_record(subs=subs))
        assert await r.scard(_subs_key(1, "U1")) == _CAP


class TestSchedule:
    async def test_schedule_noop_when_uid_empty(self, monkeypatch):
        called = {"n": 0}

        async def _cap(rec):
            called["n"] += 1

        monkeypatch.setattr(history, "capture_from_record", _cap)
        schedule_capture({"uid": "", "company_id": "1", "offer_id": 5})
        await asyncio.sleep(0)
        assert called["n"] == 0  # no task spawned when uid empty

    async def test_schedule_spawns_capture_task(self, monkeypatch):
        called = {"n": 0}

        async def _cap(rec):
            called["n"] += 1

        monkeypatch.setattr(history, "capture_from_record", _cap)
        schedule_capture({"uid": "U1", "company_id": "1", "offer_id": 5})
        await asyncio.sleep(0)  # let the fire-and-forget task run
        assert called["n"] == 1
