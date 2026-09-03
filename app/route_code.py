"""Signed route-code codec — the carried ROUTING DECISION (dark by default).

Programme SoT: ``docs/development/route-preview-2026-08-31/00-ANCHOR.md``
Plan         : ``docs/development/route-preview-2026-08-31/01-PLAN.md`` §4 P1

WHAT this is
------------
A compact, HMAC-signed token that carries a routing decision the system made
EARLIER, for itself, so a later click can be served the SAME offer_target the
landing page already advertised to the visitor.

The landing page (the FIRST hop) asks for a preview; the node computes a real
routing decision without committing it and mints this code; the visitor arrives
on the tracker link carrying the code as an ordinary query parameter; the node
verifies it and — **only if the target is still servable** — honours it.

🔴 The code is a RE-VALIDATED HINT, never an authority.
------------------------------------------------------
This is the single property that makes carrying a decision to the client safe
at all, and it is not ours to relax. ``DECISION-edge-identity-architecture.md``
rejected client-carried pins outright *except* under exactly this condition:

    "Cookie carries pins — FATAL unless every read re-validates against live
     local availability (so the cookie pin is at best a re-validated *hint*,
     never authoritative)."

So a valid signature buys the bearer ONE thing: the right to have a target
*considered*. Whether it is served is decided by the same availability floor
every other click obeys. A code naming a paused / closed / vanished target
falls through to ordinary routing — silently, because that is the correct
product behaviour, not an error.

Token fields (a decision — nothing mutable, nothing secret)::

    v    : format version (uint8)           — schema evolution; an old v fails verify
    kid  : signing key id (uint8)           — key ring; rotation-safe verify
    c    : company_id (uint32 BE)           — tenant scope; RE-CHECKED node-side
    o    : offer_id (uint32 BE)             — the advertised offer
    t    : offer_target_id (uint32 BE)      — the decision itself
    exp  : expiry epoch seconds (uint32 BE) — server-anchored; verify rejects past

Wire format — FIXED WIDTH, deliberately::

    payload_bytes = 1B(v) | 1B(kid) | 4B(c) | 4B(o) | 4B(t) | 4B(exp)   = 18 B
    code          = b64url_nopad(payload) "." b64url_nopad(HMAC_SHA256(key[kid], payload))

That is **68 ASCII characters**, comfortably inside the CF Worker's
``MAX_PARAM_VALUE_LENGTH = 512`` — which is why this rides to the node as an
ordinary query parameter and ``services/worker/`` needs no change at all.

⚠️ Why fixed-width and not ``identity_token``'s varint layout: every field here
is a bounded integer, so there is no variable-length member to encode. Dropping
varints removes the parser loop entirely — a fixed-size payload either is 18
bytes or is rejected, with no length arithmetic to get wrong. This is a
deliberate divergence from the neighbouring codec, not an oversight.

Security posture
----------------
* HMAC-SHA256 over the FULL payload; verify uses ``hmac.compare_digest``.
* **Dedicated key ring** — NOT ``TDS_SECRET_KEY`` and NOT the identity-cookie
  key. A leak of either must not let an attacker forge routing decisions, and
  rotating this ring must not disturb identity recognition.
* **Fail-closed verify:** ANY anomaly (bad b64, bad signature, unknown kid,
  wrong length, wrong version, expired, non-positive ids) returns ``None`` and
  NEVER raises. ``None`` => "no code" => the caller routes normally. A forged or
  stale code is therefore indistinguishable, to the visitor, from no code.
* ``exp`` is server-anchored — no dependence on a client clock.
* The code carries **no secret and nothing commercially sensitive**: an offer id
  and a target id, both of which the bearer is about to be sent to anyway.

Disabled-by-default: with no configured keys the codec is inert — ``verify``
returns ``None`` and ``sign`` raises, so callers must gate on ``is_enabled()``.
"""

from __future__ import annotations

import base64
import hmac
import logging
import time
from dataclasses import dataclass
from hashlib import sha256

from app.config import settings

logger = logging.getLogger("tds.route_code")

__all__ = ["RouteCode", "CODE_VERSION", "is_enabled", "sign", "verify"]

# Current wire-format version. Bump to evolve the layout; an old version on the
# wire fails verify (treated as no-code), which is the graceful fall-through.
#
# v2 (2026-09-03) BINDS THE CAMPAIGN. v1 carried no campaign id, so a code minted
# by previewing campaign X could be replayed on campaign Y of the same company
# and served X's target -- measured 20/20 on staging, a total override of Y's own
# routing, not a partial effect. ADR-0454 recorded that as a known limitation and
# named this exact remedy ("campaign_id in the payload, i.e. CODE_VERSION 2").
# Full slug, the number is ambiguous in this repo:
# ADR-0454-route-code-yields-to-the-sticky-pin-and-never-writes-the-returning-system-outranks-a-guess-about-an-anonymous-visitor
# Old v1 codes fail closed at the version check below, which is the correct
# outcome: their TTL is 30 minutes and they carry an unbindable claim.
CODE_VERSION = 2

# 1B(v) + 1B(kid) + 4B(company) + 4B(campaign) + 4B(offer) + 4B(target) + 4B(exp)
_PAYLOAD_BYTES = 22

# HMAC-SHA256 digest width.
_SIG_BYTES = 32

# An attacker-supplied parameter should be rejected cheaply, before any base64
# work. The valid code is 68 chars; this bound only stops pathological input.
_MAX_CODE_CHARS = 128

_UINT32_MAX = 0xFFFFFFFF
_UINT8_MAX = 0xFF


@dataclass(frozen=True)
class RouteCode:
    """A verified routing decision carried by the visitor.

    Frozen because a verified decision is evidence, not a working value — a
    caller that needs a different target must go through ordinary routing.
    """

    company_id: int
    # v2. The campaign the code was MINTED on. The honour site refuses a code
    # whose campaign is not the campaign the click landed on, which is what
    # makes the code a hint about ONE campaign rather than a company-wide
    # override.
    campaign_id: int
    offer_id: int
    offer_target_id: int
    expires_at: int


# --------------------------------------------------------------------------- #
# base64url (no padding)                                                       #
# --------------------------------------------------------------------------- #
def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _b64url_decode(s: str) -> bytes:
    pad = (-len(s)) % 4
    return base64.urlsafe_b64decode(s + ("=" * pad))


# --------------------------------------------------------------------------- #
# Key ring                                                                     #
# --------------------------------------------------------------------------- #
def _parse_keys(spec: str) -> dict[int, bytes]:
    """Parse ``TDS_ROUTE_CODE_KEYS`` into ``{kid: secret_bytes}``.

    Accepted form: ``kid:secret,kid:secret``. A malformed entry is SKIPPED
    (logged) rather than raising — a bad config line must never take the codec
    down; the worst case is that one kid is absent, so codes signed with it fail
    verify and those clicks route normally.

    Empty / blank spec => ``{}`` (codec inert).
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
            logger.warning("route_code: ignoring malformed key entry (no kid:secret)")
            continue
        try:
            kid = int(kid_str.strip())
        except ValueError:
            logger.warning("route_code: ignoring key entry with non-integer kid")
            continue
        if not (0 <= kid <= _UINT8_MAX):
            logger.warning("route_code: ignoring key entry with out-of-range kid")
            continue
        keys[kid] = secret.encode("utf-8")
    return keys


def _keyring() -> dict[int, bytes]:
    return _parse_keys(settings.route_code_keys)


def _active_kid() -> int | None:
    raw = (settings.route_code_active_kid or "").strip()
    if not raw:
        return None
    try:
        kid = int(raw)
    except ValueError:
        return None
    return kid if 0 <= kid <= _UINT8_MAX else None


def is_enabled() -> bool:
    """True iff SIGNING is possible: a non-empty ring AND an active kid in it.

    Verify accepts ANY kid present in the ring (that is what makes rotation
    safe); this reflects only whether this node can MINT a new code.
    """
    ring = _keyring()
    kid = _active_kid()
    return bool(ring) and kid is not None and kid in ring


# --------------------------------------------------------------------------- #
# Sign                                                                         #
# --------------------------------------------------------------------------- #
def sign(
    *,
    company_id: int,
    campaign_id: int,
    offer_id: int,
    offer_target_id: int,
    ttl_seconds: int,
    now: int | None = None,
) -> str:
    """Mint a signed route code. Raises if the codec is not enabled.

    Raising (rather than returning None) is deliberate: a caller that reaches
    here without checking ``is_enabled()`` has a logic error, and silently
    returning no code would present as "the preview works but never honours",
    which is far harder to diagnose than an exception at the mint site.
    """
    ring = _keyring()
    kid = _active_kid()
    if not ring or kid is None or kid not in ring:
        raise RuntimeError("route_code: codec disabled (no keys / no active kid)")

    for name, value in (
        ("company_id", company_id),
        ("campaign_id", campaign_id),
        ("offer_id", offer_id),
        ("offer_target_id", offer_target_id),
    ):
        if not isinstance(value, int) or not (1 <= value <= _UINT32_MAX):
            raise ValueError(f"route_code: {name} must be a positive uint32")
    if ttl_seconds <= 0:
        raise ValueError("route_code: ttl_seconds must be positive")

    issued = int(now if now is not None else time.time())
    exp = issued + int(ttl_seconds)
    if not (0 <= exp <= _UINT32_MAX):
        raise ValueError("route_code: expiry out of uint32 range")

    payload = (
        bytes([CODE_VERSION, kid])
        + company_id.to_bytes(4, "big")
        + campaign_id.to_bytes(4, "big")
        + offer_id.to_bytes(4, "big")
        + offer_target_id.to_bytes(4, "big")
        + exp.to_bytes(4, "big")
    )
    sig = hmac.new(ring[kid], payload, sha256).digest()
    return f"{_b64url(payload)}.{_b64url(sig)}"


# --------------------------------------------------------------------------- #
# Verify                                                                       #
# --------------------------------------------------------------------------- #
def verify(code: str | None, *, now: int | None = None) -> RouteCode | None:
    """Return the carried decision, or ``None`` for ANY anomaly. Never raises.

    ``None`` is not an error signal — it means "there is no usable code here",
    and every caller must treat it exactly as it treats an absent parameter:
    route normally.
    """
    if not code or not isinstance(code, str):
        return None
    if len(code) > _MAX_CODE_CHARS:
        return None

    ring = _keyring()
    if not ring:
        return None

    payload_b64, sep, sig_b64 = code.partition(".")
    if not sep or not payload_b64 or not sig_b64:
        return None

    try:
        payload = _b64url_decode(payload_b64)
        sig = _b64url_decode(sig_b64)
    except Exception:
        return None

    if len(payload) != _PAYLOAD_BYTES or len(sig) != _SIG_BYTES:
        return None
    if payload[0] != CODE_VERSION:
        return None

    kid = payload[1]
    secret = ring.get(kid)
    if secret is None:
        return None

    expected = hmac.new(secret, payload, sha256).digest()
    if not hmac.compare_digest(sig, expected):
        return None

    company_id = int.from_bytes(payload[2:6], "big")
    campaign_id = int.from_bytes(payload[6:10], "big")
    offer_id = int.from_bytes(payload[10:14], "big")
    offer_target_id = int.from_bytes(payload[14:18], "big")
    exp = int.from_bytes(payload[18:22], "big")

    # A validly signed code naming id 0 is still nonsense — refuse it rather
    # than hand a caller an id it would go on to look up.
    if (company_id <= 0 or campaign_id <= 0
            or offer_id <= 0 or offer_target_id <= 0):
        return None

    current = int(now if now is not None else time.time())
    if exp <= current:
        return None

    return RouteCode(
        company_id=company_id,
        campaign_id=campaign_id,
        offer_id=offer_id,
        offer_target_id=offer_target_id,
        expires_at=exp,
    )
