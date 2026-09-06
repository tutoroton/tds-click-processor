"""G6 — `CODE_VERSION 3`: a signed KIND and a signed origin `flow_id`.

WHY THE FORMAT GREW. Two walls in one campaign holding the SAME offer mint v2
codes identical in every field, so a returning visitor's code could not answer
*which wall they were shown*. Risks A7/A8; plan
`docs/development/offerwall-2026-09-04/26-THE-PLAN-PART-A.md` Phase 5, which asks
for BOTH: "a kind byte alone answers 'is this a wall link'; only the flow id
answers 'which wall'".

🔴 BOTH VERSIONS ARE MINTED ON PURPOSE. An ordinary preview still mints v2; only a
wall tile mints v3. They are not two generations of one claim — they are two
different claims, so this is a permanent two-format protocol rather than a
migration window. That is also what makes the change zero-risk for the live
preview path: its bytes do not move.

WHAT EVERY FORGERY TEST HERE DOES, and why it matters: it RE-SIGNS the mutated
payload with the real key. Without that, each one would merely re-test the HMAC
check that already has its own tests, and would pass no matter what the version,
kind or origin rules did. The signature is not the property under test here —
COHERENCE of an authentic claim is.
"""
from __future__ import annotations

import hmac
import time
from hashlib import sha256

import pytest

from app import route_code
from app.config import settings

KEY_A = "route-code-test-key-aaaaaaaaaaaaaaaaaaaaaaaa"

COMPANY = 7
CAMPAIGN = 4242
OFFER = 181
TARGET = 195
WALL = 90210
TTL = 1800


@pytest.fixture
def keys_1(monkeypatch):
    monkeypatch.setattr(settings, "route_code_keys", f"1:{KEY_A}")
    monkeypatch.setattr(settings, "route_code_active_kid", "1")


def _mint(**over) -> str:
    kwargs = {
        "company_id": COMPANY,
        "campaign_id": CAMPAIGN,
        "offer_id": OFFER,
        "offer_target_id": TARGET,
        "ttl_seconds": TTL,
    }
    kwargs.update(over)
    return route_code.sign(**kwargs)


def _payload_of(code: str) -> bytes:
    return route_code._b64url_decode(code.partition(".")[0])


def _resign(payload: bytes) -> str:
    """Re-sign a mutated payload with the real key.

    THE WHOLE POINT of these tests: without re-signing, every mutation below is
    refused by the MAC and the test passes while proving nothing about the rule
    it names.
    """
    sig = hmac.new(KEY_A.encode(), payload, sha256).digest()
    return f"{route_code._b64url(payload)}.{route_code._b64url(sig)}"


# --------------------------------------------------------------------------- #
# The positive half — without it every refusal below is vacuous                #
# --------------------------------------------------------------------------- #
class TestBothVersionsRoundTrip:
    def test_an_ordinary_preview_still_mints_v2_byte_for_byte(self, keys_1):
        """G6.2. The live preview path must not move at all."""
        payload = _payload_of(_mint())
        assert payload[0] == route_code.PREVIEW_CODE_VERSION == 2
        assert len(payload) == 22

    def test_a_wall_tile_mints_v3(self, keys_1):
        payload = _payload_of(_mint(origin_flow_id=WALL))
        assert payload[0] == route_code.CODE_VERSION == 3
        assert len(payload) == 27

    def test_v2_verifies_and_reports_a_preview_with_no_origin(self, keys_1):
        got = route_code.verify(_mint())
        assert got is not None
        assert got.kind == route_code.KIND_PREVIEW
        assert got.origin_flow_id is None
        assert got.is_wall_claim is False

    def test_v3_verifies_and_carries_the_wall_it_came_from(self, keys_1):
        got = route_code.verify(_mint(origin_flow_id=WALL))
        assert got is not None
        assert got.kind == route_code.KIND_WALL
        assert got.origin_flow_id == WALL
        assert got.is_wall_claim is True

    def test_the_shared_fields_parse_identically_in_both_versions(self, keys_1):
        """v3 APPENDS; it does not re-lay-out. One parse for bytes 2..22."""
        v2 = route_code.verify(_mint())
        v3 = route_code.verify(_mint(origin_flow_id=WALL))
        assert v2 is not None and v3 is not None
        for field in ("company_id", "campaign_id", "offer_id",
                      "offer_target_id"):
            assert getattr(v2, field) == getattr(v3, field), field

    def test_two_walls_with_the_SAME_offer_now_mint_DIFFERENT_codes(self, keys_1):
        """🔴 THE ENTIRE REASON v3 EXISTS (risks A7/A8).

        Under v2 these two are byte-identical, so no honour site could tell them
        apart. The assertion is on the DECODED origin, not on the strings —
        two strings could differ for a trivial reason and prove nothing.
        """
        a = route_code.verify(_mint(origin_flow_id=111))
        b = route_code.verify(_mint(origin_flow_id=222))
        assert a is not None and b is not None
        assert (a.offer_id, a.offer_target_id) == (b.offer_id, b.offer_target_id)
        assert a.origin_flow_id != b.origin_flow_id

        # And the control: under v2 the same pair really is indistinguishable,
        # so the sentence above is a fact about the change, not a hope.
        c = route_code.verify(_mint())
        d = route_code.verify(_mint())
        assert c is not None and d is not None
        assert c.origin_flow_id == d.origin_flow_id is None


# --------------------------------------------------------------------------- #
# Version and length are a PAIR                                                #
# --------------------------------------------------------------------------- #
class TestVersionAndLengthAreCheckedTogether:
    def test_a_v3_header_over_a_v2_body_is_refused(self, keys_1):
        """The dangerous shape: a 22-byte payload claiming to be v3.

        Parsed leniently it would read `kind` and `origin` off the end, or off
        whatever followed. Re-signed, so this tests the length rule and not the
        MAC.
        """
        payload = _payload_of(_mint())
        forged = bytes([route_code.CODE_VERSION]) + payload[1:]
        assert len(forged) == 22
        assert route_code.verify(_resign(forged)) is None

    def test_a_v2_header_over_a_v3_body_is_refused(self, keys_1):
        """The mirror: 27 bytes claiming to be v2 — the trailing five would be
        silently ignored, and a wall claim would read as an ordinary preview."""
        payload = _payload_of(_mint(origin_flow_id=WALL))
        forged = bytes([route_code.PREVIEW_CODE_VERSION]) + payload[1:]
        assert len(forged) == 27
        assert route_code.verify(_resign(forged)) is None

    def test_v1_stays_refused(self, keys_1):
        """v1 carried an unbindable claim; accepting two versions must not
        quietly re-admit the one that was refused for a reason."""
        payload = _payload_of(_mint())
        assert route_code.verify(_resign(bytes([1]) + payload[1:])) is None

    def test_an_unknown_future_version_is_refused(self, keys_1):
        payload = _payload_of(_mint(origin_flow_id=WALL))
        assert route_code.verify(_resign(bytes([4]) + payload[1:])) is None

    def test_trailing_bytes_on_a_v3_payload_are_refused(self, keys_1):
        payload = _payload_of(_mint(origin_flow_id=WALL))
        assert route_code.verify(_resign(payload + b"\x00")) is None

    def test_a_truncated_v3_payload_is_refused(self, keys_1):
        payload = _payload_of(_mint(origin_flow_id=WALL))
        assert route_code.verify(_resign(payload[:-1])) is None


# --------------------------------------------------------------------------- #
# An AUTHENTIC code must still make a COHERENT claim                           #
# --------------------------------------------------------------------------- #
class TestTheClaimMustBeCoherent:
    """Authenticity is not applicability. The MAC proves we minted these bytes;
    it says nothing about whether the combination means anything."""

    def _v3_with(self, kind: int, origin: int) -> str:
        payload = _payload_of(_mint(origin_flow_id=WALL))
        forged = (payload[:22] + bytes([kind])
                  + origin.to_bytes(4, "big"))
        assert len(forged) == 27
        return _resign(forged)

    def test_an_unknown_kind_is_refused(self, keys_1):
        assert route_code.verify(self._v3_with(9, WALL)) is None

    def test_kind_zero_is_refused(self, keys_1):
        """0 is the value an uninitialised byte carries; it must not be a kind."""
        assert route_code.verify(self._v3_with(0, WALL)) is None

    def test_a_wall_claim_that_cannot_name_its_wall_is_refused(self, keys_1):
        """kind=WALL with origin 0 — the shape a lazy minter would produce."""
        assert route_code.verify(
            self._v3_with(route_code.KIND_WALL, 0)) is None

    def test_a_preview_claim_carrying_an_origin_is_refused(self, keys_1):
        """Nobody signed a meaning for that field on a preview, so it must not
        be quietly ignored: an ignored field is one a later reader may honour."""
        assert route_code.verify(
            self._v3_with(route_code.KIND_PREVIEW, WALL)) is None

    def test_a_v3_preview_with_a_zero_origin_is_ACCEPTED(self, keys_1):
        """Forward compatibility, stated rather than assumed: the verifier
        already understands a v3 ordinary preview even though nothing mints one
        yet. It reports no origin, so it can never pass a wall check."""
        got = route_code.verify(self._v3_with(route_code.KIND_PREVIEW, 0))
        assert got is not None
        assert got.kind == route_code.KIND_PREVIEW
        assert got.origin_flow_id is None
        assert got.is_wall_claim is False

    def test_a_refused_v3_is_NEVER_retried_as_v2(self, keys_1):
        """🔴 The substitution this whole layer exists to stop.

        A v3 code that fails its coherence rules must be discarded, not re-read
        as an ordinary preview over its first 22 bytes — which would turn a
        rejected wall claim into an accepted routing hint. The proof is that the
        first 22 bytes ARE a perfectly valid v2 payload when re-signed alone.
        """
        bad_v3 = self._v3_with(route_code.KIND_WALL, 0)
        assert route_code.verify(bad_v3) is None

        # The control: those same FIELDS, correctly labelled v2, DO verify. So
        # the refusal above is a decision about the v3 rules, not a property of
        # the bytes.
        #
        # ⚠️ The first draft of this control sliced [:22] and re-signed it as-is,
        # forgetting that byte 0 still said "3" — so it was refused by the
        # version/length pair and the control reported the opposite of what it
        # was checking. A control that fails for its own reason is worse than
        # none: it would have read as "even a valid v2 code is refused".
        head = (bytes([route_code.PREVIEW_CODE_VERSION])
                + _payload_of(bad_v3)[1:22])
        assert route_code.verify(_resign(head)) is not None


# --------------------------------------------------------------------------- #
# Minting refuses what it cannot substantiate                                  #
# --------------------------------------------------------------------------- #
class TestSignRefusesAnIncoherentOrigin:
    @pytest.mark.parametrize("bad", [0, -1, 2 ** 32, "5", 1.0, True])
    def test_an_origin_that_is_not_a_positive_uint32_raises(self, keys_1, bad):
        """Raising, not returning None: a caller that reaches here with a bad
        origin has a logic error, and a silently unsigned wall code would
        present as "the wall works but never honours"."""
        with pytest.raises(ValueError):
            _mint(origin_flow_id=bad)

    def test_the_uint32_ceiling_is_mintable(self, keys_1):
        got = route_code.verify(_mint(origin_flow_id=2 ** 32 - 1))
        assert got is not None and got.origin_flow_id == 2 ** 32 - 1


# --------------------------------------------------------------------------- #
# G6.4 — the failure modes CONVERGE                                            #
# --------------------------------------------------------------------------- #
class TestFailureModesConverge:
    def test_every_refusal_is_the_same_outcome_as_no_code_at_all(self, keys_1):
        """risk A11. Expired, tampered, incoherent and absent must be ONE
        outcome to the caller — `None`, meaning "route normally".

        Kept as a table so a future rule that returns something else (an error,
        a sentinel, a partially-filled RouteCode) fails here rather than at the
        honour site, where the difference would be a served redirect.
        """
        past = int(time.time()) - 10
        payload = _payload_of(_mint(origin_flow_id=WALL))

        cases = {
            "absent": None,
            "empty": "",
            "malformed": "not-a-code",
            "expired": _mint(origin_flow_id=WALL, ttl_seconds=1,
                             now=past - 3600),
            "tampered signature": _mint(origin_flow_id=WALL)[:-1] + "A",
            "unknown kind": _resign(payload[:22] + bytes([9])
                                    + WALL.to_bytes(4, "big")),
            "wall without an origin": _resign(payload[:22]
                                              + bytes([route_code.KIND_WALL])
                                              + (0).to_bytes(4, "big")),
            "v1": _resign(bytes([1]) + payload[1:]),
            "length/version mismatch": _resign(
                bytes([route_code.CODE_VERSION]) + payload[1:22]),
        }
        outcomes = {name: route_code.verify(code)
                    for name, code in cases.items()}
        assert all(v is None for v in outcomes.values()), {
            k: v for k, v in outcomes.items() if v is not None
        }

    def test_the_convergence_table_is_not_vacuous(self, keys_1):
        """Calibration for the test above: a VALID code must not be None.

        Without this, a verifier that returned None for absolutely everything
        would pass the convergence test with full marks.
        """
        assert route_code.verify(_mint()) is not None
        assert route_code.verify(_mint(origin_flow_id=WALL)) is not None
