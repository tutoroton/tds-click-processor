"""P3 (2026-06-06) — node MINTS / re-stamps the signed `_tds_id` identity cookie.

The node emits the cookie VALUE in the `/decide` JSON response (`set_identity`);
the worker (P4) builds the Set-Cookie. Covers the contract invariants:

  * mint helper: dark when uid empty / codec disabled; first_seen PRESERVED
    cross-restamp for a SAME-tenant token, reset to now otherwise (multi-tenant);
    seen = recent-≤16 numeric campaign ids.
  * `/decide` injection: success + duplicate-skipped paths carry `set_identity`
    when resolver+codec ON; the key is OMITTED (byte-identical legacy) when the
    resolver is OFF or the codec is disabled.
  * resolver `campaigns_seen` surface (new=∅, seen-before=set, token-path=set)
    without disturbing the existing RT#2 indices.
  * P2 review debt — a verified same-tenant token resolves the uid WITHOUT a
    `vid→uid` GET (in-process recognition).
"""

from __future__ import annotations

import json
import time
from unittest.mock import AsyncMock, MagicMock, patch

import fakeredis.aioredis
import pytest
from fastapi.testclient import TestClient

from app import identity
from app import identity_token as idtok
from app.config import settings
from app.identity import (
    IdentityResult,
    _campaigns_key,
    _sig_key,
    mint_identity_cookie,
    resolve_identity,
)

_KEY = "k" * 40
_UID = "0123456789abcdef0123456789abcdef"
TTL = 1000


def _fr():
    return fakeredis.aioredis.FakeRedis(decode_responses=True)


def _enable_codec(monkeypatch, *, keys=f"1:{_KEY}", active="1"):
    monkeypatch.setattr(settings, "identity_cookie_keys", keys)
    monkeypatch.setattr(settings, "identity_cookie_active_kid", active)


# ============================================================
# mint_identity_cookie — unit
# ============================================================
class TestMintHelper:
    def test_dark_when_no_uid(self, monkeypatch):
        _enable_codec(monkeypatch)
        assert mint_identity_cookie(
            company_id=1, uid="", campaigns_seen={10}, incoming_token=None,
        ) is None

    def test_dark_when_codec_disabled(self, monkeypatch):
        # Resolver may be ON but with no keys the codec cannot sign → None.
        monkeypatch.setattr(settings, "identity_cookie_keys", "")
        monkeypatch.setattr(settings, "identity_cookie_active_kid", "")
        assert mint_identity_cookie(
            company_id=1, uid=_UID, campaigns_seen={10}, incoming_token=None,
        ) is None

    def test_mint_round_trips_uid_company_and_seen(self, monkeypatch):
        _enable_codec(monkeypatch)
        tok = mint_identity_cookie(
            company_id=7, uid=_UID, campaigns_seen={20, 10, 10},
            incoming_token=None,
        )
        assert tok is not None
        claims = idtok.verify(tok)
        assert claims is not None
        assert claims["c"] == 7
        assert claims["u"] == _UID
        assert sorted(claims["seen"]) == [10, 20]

    def test_first_seen_preserved_same_tenant(self, monkeypatch):
        _enable_codec(monkeypatch)
        # An incoming token of THIS tenant with a known fs → re-stamp keeps fs.
        incoming = idtok.sign(
            company_id=3, uid=_UID, first_seen=12345,
            exp=int(time.time()) + 3600, seen=[10],
        )
        tok = mint_identity_cookie(
            company_id=3, uid=_UID, campaigns_seen={10, 20},
            incoming_token=incoming,
        )
        claims = idtok.verify(tok)
        assert claims["fs"] == 12345  # carried over, NOT reset to now

    def test_first_seen_reset_for_other_tenant_token(self, monkeypatch):
        _enable_codec(monkeypatch)
        now = int(time.time())
        # Incoming token belongs to company 9; we mint for company 3 → fs=now
        # (never inherit another tenant's anchor).
        incoming = idtok.sign(
            company_id=9, uid=_UID, first_seen=12345,
            exp=now + 3600, seen=[10],
        )
        tok = mint_identity_cookie(
            company_id=3, uid=_UID, campaigns_seen={10},
            incoming_token=incoming,
        )
        claims = idtok.verify(tok)
        assert claims["fs"] != 12345
        assert abs(claims["fs"] - now) <= 5  # ~now

    def test_seen_capped_to_max(self, monkeypatch):
        _enable_codec(monkeypatch)
        tok = mint_identity_cookie(
            company_id=1, uid=_UID,
            campaigns_seen=set(range(1, 100)),  # 99 campaigns
            incoming_token=None,
        )
        claims = idtok.verify(tok)
        assert len(claims["seen"]) == idtok.MAX_SEEN  # bounded to 16
        # The most-recent (largest) ids are kept.
        assert max(claims["seen"]) == 99

    def test_non_numeric_buckets_dropped(self, monkeypatch):
        _enable_codec(monkeypatch)
        # The "" defensive bucket / non-digit strings must not enter the token.
        tok = mint_identity_cookie(
            company_id=1, uid=_UID, campaigns_seen={"", "10", "abc", 20},
            incoming_token=None,
        )
        claims = idtok.verify(tok)
        assert sorted(claims["seen"]) == [10, 20]


# ============================================================
# resolver — campaigns_seen surface (RT#2 indices intact)
# ============================================================
@pytest.mark.asyncio
class TestCampaignsSeenSurface:
    async def test_new_user_empty_set(self):
        r = _fr()
        res = await resolve_identity(
            r, company_id=1, funnel_user_id=None, visitor_id="VID1",
            campaign_id="10", source_trusted=False, ttl=TTL,
        )
        assert res.is_unique is True
        assert res.campaigns_seen == frozenset()

    async def test_seen_before_surfaces_set(self):
        r = _fr()
        # First click mints + persist records campaign 10; pre-seed the set.
        await r.sadd(_campaigns_key(1, _UID), "10", "20")
        await r.set(_sig_key(1, "vid", "VID1"), _UID)
        res = await resolve_identity(
            r, company_id=1, funnel_user_id=None, visitor_id="VID1",
            campaign_id="10", source_trusted=False, ttl=TTL,
        )
        assert res.uid == _UID
        assert res.is_returning is True
        assert res.campaigns_seen == frozenset({"10", "20"})

    async def test_seen_before_with_history_indices_intact(self):
        r = _fr()
        await r.set(_sig_key(1, "vid", "VID1"), _UID)
        await r.sadd(_campaigns_key(1, _UID), "10")
        # Seed history sets to prove prev_* indices did not shift.
        from app.history import _offers_key, _targets_key, _subs_key
        await r.sadd(_offers_key(1, _UID), "off-1")
        await r.sadd(_targets_key(1, _UID), "tgt-1")
        await r.sadd(_subs_key(1, _UID), "sub-1")
        res = await resolve_identity(
            r, company_id=1, funnel_user_id=None, visitor_id="VID1",
            campaign_id="10", source_trusted=False, ttl=TTL, with_history=True,
        )
        assert res.is_returning is True
        assert res.prev_offers == frozenset({"off-1"})
        assert res.prev_targets == frozenset({"tgt-1"})
        assert res.prev_subs == frozenset({"sub-1"})
        assert res.campaigns_seen == frozenset({"10"})

    async def test_token_path_surfaces_set(self, monkeypatch):
        _enable_codec(monkeypatch)
        r = _fr()
        tok = idtok.sign(
            company_id=1, uid=_UID, first_seen=1,
            exp=int(time.time()) + 3600, seen=[10, 30],
        )
        res = await resolve_identity(
            r, company_id=1, funnel_user_id=None, visitor_id=None,
            campaign_id="10", source_trusted=False, ttl=TTL, identity_token=tok,
        )
        assert res.signal_tier == "token"
        # The hint was unioned into the local set, then SMEMBERS surfaced it.
        assert res.campaigns_seen == frozenset({"10", "30"})
        # Exercise rt[idx] index-correctness on the token path (seen_hint present,
        # with_history=False): campaign "10" was unioned in, so SISMEMBER("10")
        # is True ⇒ is_returning. An off-by-one in the pipeline layout (e.g. the
        # appended SMEMBERS shifting the sismember slot) would flip these.
        assert res.is_returning is True
        assert res.is_roaming is False


# ============================================================
# P2 review debt — token resolves uid without vid→uid GET
# ============================================================
class _GetSpy:
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


@pytest.mark.asyncio
class TestTokenSignalRebindNoGet:
    async def test_verified_token_resolves_without_vid_get(self, monkeypatch):
        """A verified same-tenant `_tds_id` resolves the uid from the token —
        NO `id:{co}:vid:*` GET is issued (in-process HMAC recognition). The
        signal-map (`_sig_key`) for the legacy vid path is never read."""
        _enable_codec(monkeypatch)
        spy = _GetSpy(_fr())
        tok = idtok.sign(
            company_id=1, uid=_UID, first_seen=1,
            exp=int(time.time()) + 3600, seen=[10],
        )
        res = await resolve_identity(
            spy, company_id=1, funnel_user_id=None, visitor_id="VID1",
            campaign_id="10", source_trusted=False, ttl=TTL, identity_token=tok,
        )
        assert res.uid == _UID
        assert res.signal_tier == "token"
        # The decisive assertion: no legacy vid→uid GET was issued.
        assert _sig_key(1, "vid", "VID1") not in spy.get_keys
        assert not any(str(k).startswith("id:1:vid:") for k in spy.get_keys)


# ============================================================
# /decide injection — success + duplicate paths, DARK invariant
# ============================================================
@pytest.fixture
def client():
    from app.main import app
    return TestClient(app)


@pytest.fixture
def patched_auth():
    with patch("app.main._check_tds_key", new_callable=AsyncMock):
        yield


def _payload(click_id: str = "019e5be83c8179896a0859dd") -> dict:
    return {
        "click_id": click_id,
        "ip": "1.2.3.4",
        "country": "DE",
        "user_agent": "geo-tds-test/1.0",
    }


def _matched(uid=_UID, company_id=1, campaigns_seen=frozenset({"10"})):
    """A matched route() return whose attribution carries the resolver output
    the mint block reads (uid / company_id / campaigns_seen)."""
    return {
        "url": "https://offer.example.com/track?cid=1",
        "campaign_id": "10",
        "offer_id": "offer-9",
        "binding_id": 0,
        "binding_alias": None,
        "timing": {"result": "flow_cascade"},
        "attribution": {
            "uid": uid,
            "company_id": company_id,
            "campaigns_seen": campaigns_seen,
        },
    }


def _fake_redis(*, first_seen=True):
    r = MagicMock()
    r.set = AsyncMock(return_value=first_seen)  # node-local dedup state
    r.xadd = AsyncMock(return_value="1-0")
    return r


def _post(client, route_return, *, first_seen=True):
    fake_redis = _fake_redis(first_seen=first_seen)
    fake_route = AsyncMock(return_value=route_return)
    with patch("app.main.get_redis", new=AsyncMock(return_value=fake_redis)), \
         patch("app.main.route", new=fake_route):
        r = client.post("/decide", json=_payload(), headers={"X-TDS-Key": "x"})
    return r


class TestDecideInjection:
    def test_success_carries_set_identity(self, client, patched_auth, monkeypatch):
        _enable_codec(monkeypatch)
        monkeypatch.setattr(settings, "returning_resolver_enabled", True)
        r = _post(client, _matched())
        assert r.status_code == 200
        body = r.json()
        tok = body.get("set_identity")
        assert tok, "success response must carry the minted set_identity"
        claims = idtok.verify(tok)
        assert claims["c"] == 1
        assert claims["u"] == _UID
        # seen MUST include the current campaign (10).
        assert 10 in claims["seen"]

    def test_duplicate_skipped_carries_set_identity(self, client, patched_auth, monkeypatch):
        _enable_codec(monkeypatch)
        monkeypatch.setattr(settings, "returning_resolver_enabled", True)
        # first_seen=False → node-local dedup says duplicate → skipped path.
        r = _post(client, _matched(), first_seen=False)
        assert r.status_code == 200
        body = r.json()
        tok = body.get("set_identity")
        assert tok, "duplicate-skipped response must also carry set_identity"
        claims = idtok.verify(tok)
        assert claims["u"] == _UID

    def test_resolver_off_omits_key_byte_identical(self, client, patched_auth, monkeypatch):
        _enable_codec(monkeypatch)
        monkeypatch.setattr(settings, "returning_resolver_enabled", False)
        r = _post(client, _matched())
        assert r.status_code == 200
        body = r.json()
        # DARK: the key is OMITTED entirely (not present-with-None).
        assert "set_identity" not in body

    def test_codec_disabled_omits_key(self, client, patched_auth, monkeypatch):
        # Resolver ON but the codec has no keys → mint returns None → omitted.
        monkeypatch.setattr(settings, "identity_cookie_keys", "")
        monkeypatch.setattr(settings, "identity_cookie_active_kid", "")
        monkeypatch.setattr(settings, "returning_resolver_enabled", True)
        r = _post(client, _matched())
        assert r.status_code == 200
        assert "set_identity" not in r.json()

    def test_no_uid_omits_key(self, client, patched_auth, monkeypatch):
        # Resolver ON, codec ON, but attribution has no uid → nothing to mint.
        _enable_codec(monkeypatch)
        monkeypatch.setattr(settings, "returning_resolver_enabled", True)
        r = _post(client, _matched(uid=""))
        assert r.status_code == 200
        assert "set_identity" not in r.json()
