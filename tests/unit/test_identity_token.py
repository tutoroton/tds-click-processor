"""Signed identity-cookie codec tests (P2, 2026-06-06).

Covers the R4/R7 token scheme + the architecture invariants:
  * round-trip (sign → verify → claims),
  * tamper (flip a byte) ⇒ None,
  * unknown / rotated-out kid ⇒ None; rotation: old kid still verifies,
  * expired ⇒ None; server-anchored exp,
  * malformed / empty / disabled ⇒ None (fail-CLOSED verify, fail-OPEN system),
  * seen-list bounded to MAX_SEEN (16),
  * size in the R7-measured ~95-190 B band,
  * `is_enabled()` reflects key+active-kid config.
"""

from __future__ import annotations

import time

import pytest

from app import identity_token as idtok
from app.config import settings

KEY_A = "a" * 40  # dedicated identity key (>=32 to mirror real config)
KEY_B = "b" * 40

UID = "0123456789abcdef0123456789abcdef"  # 32 hex / 16 bytes


@pytest.fixture
def keys_1(monkeypatch):
    """One key, kid=1 active."""
    monkeypatch.setattr(settings, "identity_cookie_keys", f"1:{KEY_A}")
    monkeypatch.setattr(settings, "identity_cookie_active_kid", "1")


@pytest.fixture
def keys_ring(monkeypatch):
    """Two keys (rotation overlap): kid=1 old, kid=2 new+active."""
    monkeypatch.setattr(settings, "identity_cookie_keys", f"1:{KEY_A},2:{KEY_B}")
    monkeypatch.setattr(settings, "identity_cookie_active_kid", "2")


def _future() -> int:
    return int(time.time()) + 3600


# ============================================================
# Round-trip
# ============================================================
def test_round_trip(keys_1):
    exp = _future()
    tok = idtok.sign(company_id=7, uid=UID, first_seen=111, exp=exp, seen=[10, 20])
    claims = idtok.verify(tok)
    assert claims is not None
    assert claims["v"] == idtok.TOKEN_VERSION
    assert claims["kid"] == 1
    assert claims["c"] == 7
    assert claims["u"] == UID
    assert claims["fs"] == 111
    assert claims["exp"] == exp
    assert sorted(claims["seen"]) == [10, 20]


def test_round_trip_empty_seen(keys_1):
    tok = idtok.sign(company_id=1, uid=UID, first_seen=0, exp=_future())
    claims = idtok.verify(tok)
    assert claims is not None and claims["seen"] == []


def test_size_in_measured_band(keys_1):
    """R7: ~95 B at N=1, ~110 B at N=16. Stay in the documented band."""
    small = idtok.sign(company_id=1, uid=UID, first_seen=1, exp=_future(), seen=[5])
    big = idtok.sign(
        company_id=999999, uid=UID, first_seen=1, exp=_future(),
        seen=list(range(1, 17)),
    )
    assert 60 <= len(small) <= 160
    assert len(big) <= 260  # 16 seen + b64 overhead, still tiny


# ============================================================
# Tamper / forgery
# ============================================================
def test_tamper_payload_byte_flip(keys_1):
    tok = idtok.sign(company_id=1, uid=UID, first_seen=1, exp=_future(), seen=[3])
    p, _, sig = tok.partition(".")
    # Flip a char in the payload → HMAC mismatch.
    flipped = ("A" if p[5] != "A" else "B")
    tampered = p[:5] + flipped + p[6:] + "." + sig
    assert idtok.verify(tampered) is None


def test_tamper_signature_byte_flip(keys_1):
    tok = idtok.sign(company_id=1, uid=UID, first_seen=1, exp=_future())
    p, _, sig = tok.partition(".")
    flipped = ("A" if sig[0] != "A" else "B") + sig[1:]
    assert idtok.verify(p + "." + flipped) is None


def test_forge_with_wrong_key(monkeypatch):
    # Sign with key A (kid 1)...
    monkeypatch.setattr(settings, "identity_cookie_keys", f"1:{KEY_A}")
    monkeypatch.setattr(settings, "identity_cookie_active_kid", "1")
    tok = idtok.sign(company_id=1, uid=UID, first_seen=1, exp=_future())
    # ...then the ring rebinds kid 1 to a DIFFERENT secret → sig no longer valid.
    monkeypatch.setattr(settings, "identity_cookie_keys", f"1:{KEY_B}")
    assert idtok.verify(tok) is None


# ============================================================
# kid ring / rotation
# ============================================================
def test_unknown_kid_returns_none(monkeypatch):
    monkeypatch.setattr(settings, "identity_cookie_keys", f"1:{KEY_A}")
    monkeypatch.setattr(settings, "identity_cookie_active_kid", "1")
    tok = idtok.sign(company_id=1, uid=UID, first_seen=1, exp=_future())
    # kid 1 rotated entirely out of the ring → cannot verify.
    monkeypatch.setattr(settings, "identity_cookie_keys", f"2:{KEY_B}")
    monkeypatch.setattr(settings, "identity_cookie_active_kid", "2")
    assert idtok.verify(tok) is None


def test_rotation_old_kid_still_verifies(keys_ring):
    """A token signed by the OLD kid (1) must still verify while it remains in
    the ring — this is what makes rotation gap-free."""
    tok_old = idtok.sign(company_id=4, uid=UID, first_seen=1, exp=_future(), kid=1)
    tok_new = idtok.sign(company_id=4, uid=UID, first_seen=1, exp=_future())  # active=2
    assert idtok.verify(tok_old) is not None
    assert idtok.verify(tok_new) is not None
    assert idtok.verify(tok_old)["kid"] == 1
    assert idtok.verify(tok_new)["kid"] == 2


# ============================================================
# Expiry (server-anchored)
# ============================================================
def test_expired_returns_none(keys_1):
    past = int(time.time()) - 10
    tok = idtok.sign(company_id=1, uid=UID, first_seen=1, exp=past)
    assert idtok.verify(tok) is None


def test_not_yet_expired_ok_with_explicit_now(keys_1):
    exp = 2_000
    tok = idtok.sign(company_id=1, uid=UID, first_seen=1, exp=exp)
    assert idtok.verify(tok, now=1_000) is not None  # before exp
    assert idtok.verify(tok, now=2_001) is None       # after exp


def test_is_expired_helper(keys_1):
    assert idtok.is_expired({"exp": 100}, now=200) is True
    assert idtok.is_expired({"exp": 300}, now=200) is False
    assert idtok.is_expired({}, now=200) is True         # missing ⇒ expired
    assert idtok.is_expired({"exp": "x"}, now=200) is True  # malformed ⇒ expired


# ============================================================
# Malformed / disabled — fail-CLOSED verify
# ============================================================
@pytest.mark.parametrize("bad", [None, "", ".", "no-dot", "a.b.c", "!!!.###", "x." + "y" * 5000])
def test_malformed_returns_none(keys_1, bad):
    assert idtok.verify(bad) is None


def test_disabled_no_keys_returns_none(monkeypatch):
    # First sign with a real ring so we have a structurally valid token...
    monkeypatch.setattr(settings, "identity_cookie_keys", f"1:{KEY_A}")
    monkeypatch.setattr(settings, "identity_cookie_active_kid", "1")
    tok = idtok.sign(company_id=1, uid=UID, first_seen=1, exp=_future())
    # ...then disable the codec entirely → verify must fail-open (None).
    monkeypatch.setattr(settings, "identity_cookie_keys", "")
    monkeypatch.setattr(settings, "identity_cookie_active_kid", "")
    assert idtok.verify(tok) is None


def test_verify_never_raises(keys_1):
    # Garbage that could trip a decoder must yield None, not an exception.
    for junk in ["====.====", "\x00.\x00", "A" * 600 + "." + "B" * 600]:
        assert idtok.verify(junk) is None


# ============================================================
# seen-list bound
# ============================================================
def test_seen_bounded_to_max(keys_1):
    over = list(range(1, idtok.MAX_SEEN + 50))  # way over the cap
    tok = idtok.sign(company_id=1, uid=UID, first_seen=1, exp=_future(), seen=over)
    claims = idtok.verify(tok)
    assert claims is not None
    assert len(claims["seen"]) == idtok.MAX_SEEN
    # Keeps the MOST-RECENT (largest) ids.
    assert max(claims["seen"]) == over[-1]


def test_seen_dedup_and_sorted(keys_1):
    tok = idtok.sign(company_id=1, uid=UID, first_seen=1, exp=_future(),
                     seen=[5, 5, 1, 3, 1])
    claims = idtok.verify(tok)
    assert claims["seen"] == [1, 3, 5]


# ============================================================
# sign() guards
# ============================================================
def test_sign_requires_active_kid(monkeypatch):
    monkeypatch.setattr(settings, "identity_cookie_keys", f"1:{KEY_A}")
    monkeypatch.setattr(settings, "identity_cookie_active_kid", "")
    with pytest.raises(ValueError):
        idtok.sign(company_id=1, uid=UID, first_seen=1, exp=_future())


def test_sign_rejects_bad_uid(keys_1):
    with pytest.raises(ValueError):
        idtok.sign(company_id=1, uid="short", first_seen=1, exp=_future())


# ============================================================
# is_enabled / key parsing
# ============================================================
def test_is_enabled(monkeypatch):
    monkeypatch.setattr(settings, "identity_cookie_keys", "")
    monkeypatch.setattr(settings, "identity_cookie_active_kid", "")
    assert idtok.is_enabled() is False

    monkeypatch.setattr(settings, "identity_cookie_keys", f"1:{KEY_A}")
    monkeypatch.setattr(settings, "identity_cookie_active_kid", "1")
    assert idtok.is_enabled() is True

    # Active kid not in ring ⇒ disabled (cannot sign).
    monkeypatch.setattr(settings, "identity_cookie_active_kid", "9")
    assert idtok.is_enabled() is False


def test_malformed_key_entries_skipped(monkeypatch):
    monkeypatch.setattr(
        settings, "identity_cookie_keys",
        f" , bad-no-colon , x:nosuchint , -1:{KEY_B} , 1:{KEY_A} ",
    )
    monkeypatch.setattr(settings, "identity_cookie_active_kid", "1")
    # Only kid 1 survived parsing; signing+verify on it works.
    tok = idtok.sign(company_id=1, uid=UID, first_seen=1, exp=_future())
    assert idtok.verify(tok) is not None
    assert idtok.is_enabled() is True
