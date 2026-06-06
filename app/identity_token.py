"""Signed identity-cookie codec — the Layer-1 RECOGNITION primitive (P2, dark).

Architecture SoT: ``docs/development/returning-users-v2/DECISION-edge-identity-architecture.md``
Plan            : ``docs/development/returning-users-v2/08-EDGE-IDENTITY-PLAN.md`` §P2
Scheme + sizing : ``research/R4-cookie-carried-identity.md`` §3, ``research/R7-measured-feasibility.md``

WHAT this is
------------
A compact, HMAC-signed token that carries ONLY immutable/monotonic identity —
the WHO of a returning user — so any edge node recognizes the same user with a
single in-process HMAC verify (R7: ~3 µs p50) and ZERO store hit. The cookie
travels with the user, so cross-node recognition is gap-free and needs no
replication (R6 theorem: the hot path depends on zero cross-node-shared mutable
state). Everything mutable (pins, history, availability) stays node-local — it
is NOT in this token (R4 §4-5).

This module is VERIFY/SIGN only. P2 wires VERIFY into the resolve path
(dual-accept with the legacy ``_tds_vid`` path). Minting / Set-Cookie is P3.

Token fields (the WHO — nothing mutable, nothing authoritative-routing)::

    v    : format version (int, 1 byte)          — rotation / schema evolution
    kid  : signing key id (int, varint)          — key ring; rotation-safe verify
    c    : company_id (int, varint)              — tenant scope; checked NODE-side
    u    : uid (16 bytes / 32 lowercase hex)     — the canonical company-scoped id
    fs   : first_seen epoch seconds (uint32 BE)  — analytics anchor
    exp  : expiry epoch seconds (uint32 BE)      — server-anchored; verify rejects past
    seen : recent campaign ids (bounded to 16)   — union-merge HINT, never authority

Wire format (R7 encoding "b" — compact binary + base64url, the measured ~95 B
at N=1, ~110 B at N=16)::

    payload_bytes = 1B(v) ‖ varint(kid) ‖ varint(c) ‖ 16B(uid)
                    ‖ uint32_BE(fs) ‖ uint32_BE(exp)
                    ‖ varint(len seen) ‖ delta-varint(seen sorted asc)
    cookie_value  = b64url_nopad(payload_bytes) "." b64url_nopad(HMAC_SHA256(key[kid], payload_bytes))

Security posture (R4 §3.6)
--------------------------
* HMAC-SHA256 over the FULL payload; verify uses ``hmac.compare_digest``.
* **Dedicated key + kid ring** — NOT ``TDS_SECRET_KEY``. Rotating it never makes
  the fleet "look new" (old kid still verifies during the overlap window), and a
  routing-secret (X-TDS-Key) leak cannot forge identity.
* **Fail-closed verify:** ANY anomaly (bad b64, bad sig, unknown kid, truncated,
  wrong version, malformed uid, missing keys) returns ``None`` — NEVER raises.
  ``None`` ⇒ "no token" ⇒ caller falls back to the legacy path (fail-open).
* ``exp`` is server-anchored (the minting node sets it; verify rejects expired) —
  no dependence on a client clock.
* The uid carried by a *validly signed* token is STILL shape-gated by the caller
  (``identity._valid_uid``) before any Redis key is built from it (defense in
  depth, in case of a key-compromise downgrade).

Disabled-by-default: with no configured keys the codec is inert —
``verify`` returns ``None`` (fail-open to legacy), ``sign`` raises (callers must
gate on ``is_enabled()`` / configured keys; P2 only ever VERIFIES).
"""

from __future__ import annotations

import base64
import hmac
import logging
import time
from hashlib import sha256

from app.config import settings

logger = logging.getLogger(__name__)

# Current wire-format version. Bump to evolve the layout; an old version on the
# wire fails verify (treated as no-token), which is the graceful fail-open path.
TOKEN_VERSION = 1

# Hard bound on the seen-list carried in the token (R7 §RECOMMENDED: recent-16
# LRU; measured p99 = 2 campaigns/uid, so 16 is ~8× headroom and non-binding for
# real users). A producer should pass the most-recent <=16; verify also defends
# by refusing a token whose declared seen count exceeds this cap.
MAX_SEEN = 16

# uid is exactly 16 bytes (32 lowercase hex). Fixed-width in the payload.
_UID_BYTES = 16

# A signed-token payload is tiny; an attacker-supplied cookie that decodes to
# something huge should be rejected cheaply before we parse. The largest valid
# payload (v + kid + c + uid + fs + exp + count + 16 delta-varints) is well under
# this; the cap only stops pathological inputs from driving the varint loop.
_MAX_PAYLOAD_BYTES = 512


# --------------------------------------------------------------------------- #
# base64url (no padding)                                                       #
# --------------------------------------------------------------------------- #
def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _b64url_decode(s: str) -> bytes:
    # Restore stripped padding; ``urlsafe_b64decode`` requires it.
    pad = (-len(s)) % 4
    return base64.urlsafe_b64decode(s + ("=" * pad))


# --------------------------------------------------------------------------- #
# varint (unsigned LEB128) + fixed-width helpers                              #
# --------------------------------------------------------------------------- #
def _write_varint(out: bytearray, value: int) -> None:
    if value < 0:
        raise ValueError("varint must be non-negative")
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return


def _read_varint(buf: bytes, pos: int) -> tuple[int, int]:
    """Return (value, next_pos). Raises on truncation / overlong (>10 bytes)."""
    result = 0
    shift = 0
    start = pos
    while True:
        if pos >= len(buf) or pos - start >= 10:
            raise ValueError("truncated or overlong varint")
        byte = buf[pos]
        result |= (byte & 0x7F) << shift
        pos += 1
        if not (byte & 0x80):
            return result, pos
        shift += 7


def _u32_be(value: int) -> bytes:
    if not (0 <= value <= 0xFFFFFFFF):
        raise ValueError("uint32 out of range")
    return value.to_bytes(4, "big")


# --------------------------------------------------------------------------- #
# Key ring                                                                     #
# --------------------------------------------------------------------------- #
def _parse_keys(spec: str) -> dict[int, bytes]:
    """Parse ``TDS_IDENTITY_COOKIE_KEYS`` into ``{kid: secret_bytes}``.

    Accepted form: ``kid:secret,kid:secret`` — kid is a small non-negative
    integer, secret is the raw signing key (UTF-8). Whitespace around entries is
    tolerated. A malformed entry is SKIPPED (logged once) rather than raising —
    a bad config line must never take down the codec; the worst case is a kid is
    simply absent ⇒ tokens signed with it fail verify ⇒ fall back to legacy.

    Empty / blank spec ⇒ ``{}`` (codec disabled — verify returns None).
    """
    keys: dict[int, bytes] = {}
    if not spec:
        return keys
    for entry in spec.split(","):
        entry = entry.strip()
        if not entry:
            continue
        kid_str, sep, secret = entry.partition(":")
        if not sep or not secret:
            logger.warning("identity_token: ignoring malformed key entry (no kid:secret)")
            continue
        try:
            kid = int(kid_str.strip())
        except ValueError:
            logger.warning("identity_token: ignoring key entry with non-integer kid")
            continue
        if kid < 0:
            logger.warning("identity_token: ignoring key entry with negative kid")
            continue
        keys[kid] = secret.encode("utf-8")
    return keys


def _keyring() -> dict[int, bytes]:
    return _parse_keys(settings.identity_cookie_keys)


def _active_kid() -> int | None:
    raw = (settings.identity_cookie_active_kid or "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def is_enabled() -> bool:
    """True iff the codec has at least one configured key AND a valid active kid
    present in the ring. Verify still works on ANY kid in the ring (rotation);
    this reflects whether SIGNING (P3) is possible."""
    ring = _keyring()
    kid = _active_kid()
    return bool(ring) and kid is not None and kid in ring


# --------------------------------------------------------------------------- #
# uid <-> 16 bytes                                                             #
# --------------------------------------------------------------------------- #
def _uid_to_bytes(uid: str) -> bytes:
    """32-lowercase-hex uid → 16 bytes. Raises on bad shape."""
    if not isinstance(uid, str) or len(uid) != 32:
        raise ValueError("uid must be 32 hex chars")
    return bytes.fromhex(uid)  # raises ValueError on non-hex


def _uid_from_bytes(raw: bytes) -> str:
    return raw.hex()  # 16 bytes → 32 lowercase hex


# --------------------------------------------------------------------------- #
# Encode / Decode payload                                                      #
# --------------------------------------------------------------------------- #
def _encode_payload(
    *,
    kid: int,
    company_id: int,
    uid: str,
    first_seen: int,
    exp: int,
    seen: list[int] | None,
) -> bytes:
    out = bytearray()
    out.append(TOKEN_VERSION & 0xFF)
    _write_varint(out, kid)
    _write_varint(out, company_id)
    out += _uid_to_bytes(uid)
    out += _u32_be(int(first_seen))
    out += _u32_be(int(exp))

    # Bound to the most-recent MAX_SEEN, store sorted ascending so delta-varint
    # stays small + positive. De-dup + drop negatives defensively.
    clean = sorted({int(c) for c in (seen or []) if int(c) >= 0})
    if len(clean) > MAX_SEEN:
        clean = clean[-MAX_SEEN:]
        clean.sort()
    _write_varint(out, len(clean))
    prev = 0
    for cid in clean:
        _write_varint(out, cid - prev)  # delta (ascending ⇒ non-negative)
        prev = cid
    return bytes(out)


def _decode_payload(payload: bytes) -> dict:
    """Decode the binary payload → claims dict. Raises on ANY malformation."""
    if len(payload) > _MAX_PAYLOAD_BYTES:
        raise ValueError("payload too large")
    pos = 0
    if len(payload) < 1:
        raise ValueError("empty payload")
    version = payload[0]
    pos = 1
    if version != TOKEN_VERSION:
        raise ValueError(f"unsupported version {version}")
    kid, pos = _read_varint(payload, pos)
    company_id, pos = _read_varint(payload, pos)
    if pos + _UID_BYTES + 8 > len(payload):
        raise ValueError("truncated fixed fields")
    uid = _uid_from_bytes(payload[pos : pos + _UID_BYTES])
    pos += _UID_BYTES
    first_seen = int.from_bytes(payload[pos : pos + 4], "big")
    pos += 4
    exp = int.from_bytes(payload[pos : pos + 4], "big")
    pos += 4
    count, pos = _read_varint(payload, pos)
    if count > MAX_SEEN:
        raise ValueError("seen count exceeds bound")
    seen: list[int] = []
    prev = 0
    for _ in range(count):
        delta, pos = _read_varint(payload, pos)
        prev += delta
        seen.append(prev)
    if pos != len(payload):
        raise ValueError("trailing bytes")
    return {
        "v": version,
        "kid": kid,
        "c": company_id,
        "u": uid,
        "fs": first_seen,
        "exp": exp,
        "seen": seen,
    }


# --------------------------------------------------------------------------- #
# Public API: sign / verify                                                    #
# --------------------------------------------------------------------------- #
def sign(
    *,
    company_id: int,
    uid: str,
    first_seen: int,
    exp: int,
    seen: list[int] | None = None,
    kid: int | None = None,
) -> str:
    """Produce a signed cookie value for the given identity claims.

    ``kid`` defaults to the configured active kid. Raises ``ValueError`` if the
    codec is disabled (no keys / no valid active kid) or the inputs are
    malformed — SIGNING is a P3 concern; callers MUST gate on ``is_enabled()``.
    (P2 only ever VERIFIES; tests use this to build fixtures.)
    """
    ring = _keyring()
    if kid is None:
        kid = _active_kid()
    if kid is None:
        raise ValueError("no active kid configured")
    key = ring.get(kid)
    if key is None:
        raise ValueError(f"kid {kid} not in key ring")
    payload = _encode_payload(
        kid=kid,
        company_id=company_id,
        uid=uid,
        first_seen=first_seen,
        exp=exp,
        seen=seen,
    )
    sig = hmac.new(key, payload, sha256).digest()
    return _b64url(payload) + "." + _b64url(sig)


def verify(cookie_value: str | None, *, now: int | None = None) -> dict | None:
    """Verify a ``_tds_id`` cookie value → claims dict, or ``None``.

    FAIL-CLOSED: returns ``None`` for ANY problem — empty/None, codec disabled
    (no keys), malformed structure, bad base64, unknown kid, signature mismatch,
    unsupported version, truncated/over-long payload, malformed uid, or expired
    (``exp <= now``). NEVER raises (the hot path must not be interrupted).

    On success returns ``{v, kid, c, u, fs, exp, seen}``. The caller MUST still
    validate ``c == company_id`` node-side (tenant scope; the codec cannot know
    the caller's tenant) and shape-gate ``u`` before building any Redis key.
    """
    try:
        if not cookie_value:
            return None
        ring = _keyring()
        if not ring:
            return None  # codec disabled ⇒ fail-open to legacy
        if "." not in cookie_value:
            return None
        p_b64, _, sig_b64 = cookie_value.partition(".")
        if not p_b64 or not sig_b64:
            return None
        payload = _b64url_decode(p_b64)
        if len(payload) > _MAX_PAYLOAD_BYTES:
            return None
        provided_sig = _b64url_decode(sig_b64)

        # The kid is the FIRST field after the version byte; read it cheaply to
        # pick the key WITHOUT trusting the rest of the payload yet.
        if len(payload) < 1 or payload[0] != TOKEN_VERSION:
            return None
        kid, _ = _read_varint(payload, 1)
        key = ring.get(kid)
        if key is None:
            return None  # unknown / rotated-out kid

        expected_sig = hmac.new(key, payload, sha256).digest()
        if not hmac.compare_digest(expected_sig, provided_sig):
            return None

        claims = _decode_payload(payload)  # full structural validation
        if claims["kid"] != kid:
            return None  # paranoia: kid consistency

        ts = int(time.time()) if now is None else int(now)
        if is_expired(claims, now=ts):
            return None
        return claims
    except Exception:  # noqa: BLE001 — fail-closed, NEVER raise into routing
        return None


def is_expired(claims: dict, *, now: int | None = None) -> bool:
    """True iff the token's server-anchored ``exp`` is in the past (<= now)."""
    ts = int(time.time()) if now is None else int(now)
    try:
        return int(claims.get("exp", 0)) <= ts
    except (TypeError, ValueError):
        return True
