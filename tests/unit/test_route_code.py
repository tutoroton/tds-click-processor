"""Route-code codec contract (GTD-R965 / plan route-preview M1.P1).

Programme: ``docs/development/route-preview-2026-08-31/00-ANCHOR.md``

WHAT THIS PINS, and why each half is load-bearing
-------------------------------------------------
The codec has two duties that pull in opposite directions, so this file has to
prove BOTH or it proves nothing:

* it must ACCEPT a code we minted ourselves — otherwise the feature silently
  never honours anything and looks like "the offer changed";
* it must REJECT everything else — forged, tampered, expired, foreign-key,
  wrong-version, malformed — by returning ``None`` and never raising.

A file of only-rejection tests would pass against a ``verify`` that returns
``None`` unconditionally. ``test_round_trip`` is what makes every rejection test
below mean something: together they calibrate the gate in both directions.
"""

from __future__ import annotations

import time

import pytest

from app import route_code
from app.config import settings

KEY_A = "route-code-test-key-aaaaaaaaaaaaaaaaaaaaaaaa"
KEY_B = "route-code-test-key-bbbbbbbbbbbbbbbbbbbbbbbb"

COMPANY = 7
CAMPAIGN = 4242   # v2: the campaign the code is bound to
OFFER = 181
TARGET = 195
TTL = 1800


@pytest.fixture
def keys_1(monkeypatch):
    """A single-key ring, kid=1 active."""
    monkeypatch.setattr(settings, "route_code_keys", f"1:{KEY_A}")
    monkeypatch.setattr(settings, "route_code_active_kid", "1")


@pytest.fixture
def keys_ring(monkeypatch):
    """Two kids in the ring, the NEWER one active — the rotation shape."""
    monkeypatch.setattr(settings, "route_code_keys", f"1:{KEY_A},2:{KEY_B}")
    monkeypatch.setattr(settings, "route_code_active_kid", "2")


def _mint(**over) -> str:
    kwargs = {
        "company_id": COMPANY,
        # v2 binds the campaign into the payload; the codec now refuses to mint
        # without one, because an unbindable code is the defect v2 closes.
        "campaign_id": CAMPAIGN,
        "offer_id": OFFER,
        "offer_target_id": TARGET,
        "ttl_seconds": TTL,
    }
    kwargs.update(over)
    return route_code.sign(**kwargs)


# --------------------------------------------------------------------------- #
# The positive half — without this, every rejection test below is vacuous      #
# --------------------------------------------------------------------------- #
def test_round_trip(keys_1):
    decision = route_code.verify(_mint())

    assert decision is not None
    assert decision.company_id == COMPANY
    assert decision.offer_id == OFFER
    assert decision.offer_target_id == TARGET
    assert decision.expires_at > int(time.time())


def test_round_trip_at_the_uint32_ceiling(keys_1):
    """The wire format is fixed-width; the largest legal ids must survive it."""
    big = 0xFFFFFFFF
    decision = route_code.verify(
        _mint(company_id=big, offer_id=big, offer_target_id=big)
    )

    assert decision is not None
    assert (decision.company_id, decision.offer_id, decision.offer_target_id) == (
        big,
        big,
        big,
    )


def test_code_fits_the_workers_query_param_cap(keys_1):
    """🔴 THE CLAIM THE WHOLE DELIVERY PLAN RESTS ON.

    ``services/worker/`` is deliberately not modified by this programme: the
    code reaches the node as an ordinary query parameter because the worker
    already forwards every one of them. That only holds while the code fits
    inside the worker's own clamp, ``MAX_PARAM_VALUE_LENGTH = 512``
    (``services/worker/src/index.js``), which TRUNCATES rather than rejects —
    so an over-long code would not fail loudly, it would arrive corrupted and
    every click would silently route as if no code were present.

    The length is pinned exactly, so any payload change has to come here and
    re-justify itself against the clamp rather than drift toward it silently.

    v1 was 68 chars (18-byte payload). v2 added a 4-byte campaign id to close the
    cross-campaign replay (see `route_code.CODE_VERSION`), taking the payload to
    22 bytes and the code to 74. Verified against the real clamp, not a
    remembered one: `MAX_PARAM_VALUE_LENGTH = 512` at
    `services/worker/src/index.js:264`, applied by `substring` at :1363. 74 of
    512 leaves ~85% of the budget unused.
    """
    code = _mint()

    assert len(code) == 74, f"expected the documented 74-char v2 code, got {len(code)}"
    # The invariant that actually matters: the worker TRUNCATES above this, so
    # exceeding it would corrupt the code silently rather than fail loudly.
    assert len(code) < 512


def test_rotation_a_code_signed_by_the_old_kid_still_verifies(keys_ring, monkeypatch):
    """Rotation must not invalidate codes already handed to landing pages."""
    monkeypatch.setattr(settings, "route_code_active_kid", "1")
    signed_by_old = _mint()

    monkeypatch.setattr(settings, "route_code_active_kid", "2")
    signed_by_new = _mint()

    # Both verify while both kids remain in the ring.
    assert route_code.verify(signed_by_old) is not None
    assert route_code.verify(signed_by_new) is not None


# --------------------------------------------------------------------------- #
# The rejection half                                                           #
# --------------------------------------------------------------------------- #
def test_tampered_payload_is_rejected(keys_1):
    code = _mint()
    payload, _, sig = code.partition(".")
    flipped = ("A" if payload[0] != "A" else "B") + payload[1:]

    assert route_code.verify(f"{flipped}.{sig}") is None


def test_tampered_signature_is_rejected(keys_1):
    code = _mint()
    payload, _, sig = code.partition(".")
    flipped = ("A" if sig[0] != "A" else "B") + sig[1:]

    assert route_code.verify(f"{payload}.{flipped}") is None


def test_a_code_signed_with_another_key_is_rejected(monkeypatch):
    """The forgery case: same kid number, different secret."""
    monkeypatch.setattr(settings, "route_code_keys", f"1:{KEY_A}")
    monkeypatch.setattr(settings, "route_code_active_kid", "1")
    code = _mint()

    monkeypatch.setattr(settings, "route_code_keys", f"1:{KEY_B}")
    assert route_code.verify(code) is None


def test_a_kid_absent_from_the_ring_is_rejected(monkeypatch):
    monkeypatch.setattr(settings, "route_code_keys", f"1:{KEY_A}")
    monkeypatch.setattr(settings, "route_code_active_kid", "1")
    code = _mint()

    monkeypatch.setattr(settings, "route_code_keys", f"2:{KEY_B}")
    monkeypatch.setattr(settings, "route_code_active_kid", "2")
    assert route_code.verify(code) is None


def test_expired_is_rejected(keys_1):
    code = _mint()
    assert route_code.verify(code, now=int(time.time()) + TTL + 1) is None


def test_expiry_is_exclusive_at_the_boundary(keys_1):
    """A code is dead AT its expiry instant, not one second after it."""
    issued = 1_700_000_000
    code = _mint(now=issued)

    assert route_code.verify(code, now=issued + TTL - 1) is not None
    assert route_code.verify(code, now=issued + TTL) is None


def test_a_wrong_version_byte_is_rejected(keys_1):
    """An old code must fail closed when the layout is bumped, not mis-parse."""
    code = _mint()
    payload = route_code._b64url_decode(code.partition(".")[0])
    bumped = bytes([route_code.CODE_VERSION + 1]) + payload[1:]
    # Re-sign so ONLY the version differs — otherwise this would merely retest
    # the signature check.
    import hmac
    from hashlib import sha256

    sig = hmac.new(KEY_A.encode(), bumped, sha256).digest()
    forged = f"{route_code._b64url(bumped)}.{route_code._b64url(sig)}"

    assert route_code.verify(forged) is None


@pytest.mark.parametrize(
    "bad",
    [
        None,
        "",
        "not-a-code",
        ".",
        "onlypayload.",
        ".onlysig",
        "!!!.!!!",
        "a" * 200,
    ],
)
def test_malformed_input_returns_none_and_never_raises(keys_1, bad):
    assert route_code.verify(bad) is None


def test_a_signed_code_naming_id_zero_is_still_refused(keys_1):
    """A validly signed nonsense id must not be handed on to a lookup."""
    import hmac
    from hashlib import sha256

    payload = (
        bytes([route_code.CODE_VERSION, 1])
        + (0).to_bytes(4, "big")
        + OFFER.to_bytes(4, "big")
        + TARGET.to_bytes(4, "big")
        + (int(time.time()) + TTL).to_bytes(4, "big")
    )
    sig = hmac.new(KEY_A.encode(), payload, sha256).digest()
    forged = f"{route_code._b64url(payload)}.{route_code._b64url(sig)}"

    assert route_code.verify(forged) is None


# --------------------------------------------------------------------------- #
# Inert-when-unconfigured — the DARK default                                   #
# --------------------------------------------------------------------------- #
def test_codec_is_inert_without_keys(monkeypatch):
    """The shipped default: no ring configured anywhere."""
    monkeypatch.setattr(settings, "route_code_keys", "")
    monkeypatch.setattr(settings, "route_code_active_kid", "")

    assert route_code.is_enabled() is False
    assert route_code.verify("anything.at-all") is None
    with pytest.raises(RuntimeError):
        _mint()


def test_is_enabled_requires_the_active_kid_to_be_IN_the_ring(monkeypatch):
    """A ring plus an active kid that names nothing is a misconfiguration, and
    it must read as disabled rather than as a signing attempt that explodes at
    the first preview request."""
    monkeypatch.setattr(settings, "route_code_keys", f"1:{KEY_A}")
    monkeypatch.setattr(settings, "route_code_active_kid", "9")

    assert route_code.is_enabled() is False
    with pytest.raises(RuntimeError):
        _mint()


def test_a_verifiable_ring_needs_no_active_kid(monkeypatch):
    """Verification must survive a node that can no longer SIGN — that is the
    end state of a rotation where the active kid was removed."""
    monkeypatch.setattr(settings, "route_code_keys", f"1:{KEY_A}")
    monkeypatch.setattr(settings, "route_code_active_kid", "1")
    code = _mint()

    monkeypatch.setattr(settings, "route_code_active_kid", "")
    assert route_code.is_enabled() is False
    assert route_code.verify(code) is not None


@pytest.mark.parametrize(
    "spec",
    ["garbage", "1", ":secret", "1:", "notanint:secret", "-1:secret", "999:secret"],
)
def test_a_malformed_key_entry_is_skipped_not_fatal(monkeypatch, spec):
    """A bad config line must never take the codec down."""
    monkeypatch.setattr(settings, "route_code_keys", spec)
    monkeypatch.setattr(settings, "route_code_active_kid", "1")

    assert route_code.is_enabled() is False
    assert route_code.verify("x.y") is None


# --------------------------------------------------------------------------- #
# Sign-side input validation                                                   #
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "over",
    [
        {"company_id": 0},
        {"offer_id": 0},
        {"offer_target_id": 0},
        {"company_id": -1},
        {"offer_id": 0x1_0000_0000},
        {"ttl_seconds": 0},
        {"ttl_seconds": -5},
    ],
)
def test_sign_refuses_out_of_contract_input(keys_1, over):
    with pytest.raises(ValueError):
        _mint(**over)
