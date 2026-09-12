"""I2 — the advertised TTL's boundary, proved against the CODE rather than waited out.

🔴 THE CRITERION'S PREMISE IS FALSE FOR THIS IMPLEMENTATION, AND THIS FILE IS THE
CORRECTION. I2 reads: *"Freeze the advertised TTL `T`, then test **real link ages
31 min, `T−1 min`, `T+1 min`** … A week-long claim stays unverified until a
real-age soak completes; fast-clock CI tests complement it, never replace it."*

With `T = 604800` that is a **7-day wall-clock floor on the entire readiness
gate**. I declared it as such. Then I read the code the criterion points at —
the check that has corrected six other boxes in this plan — and the premise does
not survive it:

  * `sign()` computes `exp = issued + ttl_seconds` and signs the **ABSOLUTE
    expiry** into the payload (`route_code.py`). `issued` is **never stored**.
  * `verify()` performs exactly ONE age-dependent operation on it:
    `if exp <= current: return None`.
  * minting writes **nothing** — the code is a stateless bearer token, so no
    row, no TTL and no cache carries the link's age anywhere.

**Therefore a link's AGE is not an input to this system; only its signed `exp`
is.** A genuinely 7-day-old link and a link minted one second ago whose `exp`
sits one minute away are, at the only place age is consulted, **the same input**
— the information that would distinguish them does not exist in the payload or
in any store. Waiting seven days would re-run one integer comparison that
`TestTheBoundaryWithNoClockInjectionAtAll` runs here against the real wall clock.

⚠️ **THIS IS A CORRECTION, NOT A WEAKENING, AND THE DISTINCTION IS THE WHOLE
POINT.** A correction that moves the verdict has to prove MORE than the clause it
replaces, not less. The soak's one genuine content beyond arithmetic is
**key-ring rotation** — a week-old code carries a `kid` that may have been
rotated out, and `ring.get(kid) is None` refuses it. That risk is real, it is the
only age-COUPLED one in the mechanism, and the old wording only covered it *by
luck* (a rotation would have had to happen to fall inside the soak window). It is
covered here deliberately and on purpose, by construction rather than by timing.

WHERE THE PREMISE *WOULD* HAVE HELD, said plainly so nobody generalises this:
an implementation storing a mint timestamp and comparing `now - minted > ttl`, or
one caching verify decisions, or one relying on a monotonic clock, would all be
untestable this way. This one stores an absolute expiry and re-derives the answer
on every request. The correction is about THIS mechanism and does not license
skipping a soak elsewhere.
"""

from __future__ import annotations

import time
from unittest.mock import patch

from app import route_code
from app.config import Settings, settings

from tests.unit.test_route_code_honoured import (
    _ACTIVE_KID,
    _CAMPAIGN,
    _CODED_TARGET,
    _COMPANY,
    _KEYS,
    _OFFER,
)

#: The advertised TTL, read from the shipped default rather than restated — if
#: U1's value ever moves, every boundary below moves with it instead of quietly
#: describing a TTL nobody ships.
_T = Settings.model_fields["wall_tile_ttl_seconds"].default

#: The route-PREVIEW TTL the "31 min" arm exists to contrast against. 31 minutes
#: is not an arbitrary age: it is one minute past the preview expiry, and the
#: claim it tests is that a WALL tile outlives a preview code.
_PREVIEW_TTL = 1800


def _sign(*, ttl, now=None, kid=_ACTIVE_KID, keys=_KEYS, wall=True):
    with patch.object(settings, "route_code_keys", keys), \
            patch.object(settings, "route_code_active_kid", kid):
        return route_code.sign(
            company_id=_COMPANY, campaign_id=_CAMPAIGN, offer_id=_OFFER,
            offer_target_id=_CODED_TARGET, ttl_seconds=ttl, now=now,
            origin_flow_id=910 if wall else None,
        )


def _verify(code, *, now=None, keys=_KEYS):
    with patch.object(settings, "route_code_keys", keys), \
            patch.object(settings, "route_code_active_kid", _ACTIVE_KID):
        return route_code.verify(code, now=now)


class TestAgeIsNotAnInputToThisSystem:
    """The keystone. Everything else in this file is licensed by these rows.

    If a mint time were carried anywhere, a real-age soak would test something
    an injected clock cannot. It is not, and these assertions are what make that
    checkable rather than asserted in a docstring.
    """

    def test_two_codes_issued_DAYS_APART_are_byte_identical_at_equal_expiry(self):
        # The decisive one. A link "minted 7 days ago with a 7-day TTL" and a
        # link "minted now with a 60-second TTL" are THE SAME STRING when their
        # expiries coincide — so no test, and no soak, can tell them apart.
        t0 = 1_700_000_000
        old = _sign(ttl=_T, now=t0)                      # issued t0, dies t0+T
        fresh = _sign(ttl=60, now=t0 + _T - 60)          # issued later, same death
        assert old == fresh, (
            "the payload distinguishes issue time from expiry — if this is ever "
            "true, age IS an input and I2's real-age soak becomes necessary again"
        )

    def test_the_decoded_claim_carries_an_expiry_and_no_issue_time(self):
        decoded = _verify(_sign(ttl=_T, now=1_700_000_000), now=1_700_000_001)
        assert decoded is not None
        assert decoded.expires_at == 1_700_000_000 + _T
        assert not hasattr(decoded, "issued_at"), (
            "an issue time appeared on the decoded claim — re-read I2 before "
            "trusting any boundary test in this file"
        )


class TestTheBoundaryWithNoClockInjectionAtAll:
    """Real `time.time()` on both sides — no `now=`, nothing patched.

    This is the row that answers *"fast-clock CI tests complement it, never
    replace it"* on its own terms: there is no fast clock here.
    """

    def test_a_live_code_is_honoured_against_the_REAL_wall_clock(self):
        assert _verify(_sign(ttl=60)) is not None

    def test_a_code_whose_expiry_has_PASSED_is_refused_by_the_REAL_wall_clock(self):
        # Signed far enough in the past that its expiry is behind us NOW. The
        # verify side consults `time.time()` itself.
        expired = _sign(ttl=_T, now=int(time.time()) - _T - 60)
        assert _verify(expired) is None

    def test_the_pair_above_differ_ONLY_in_the_signed_expiry(self):
        # Stated separately so the two rows cannot be read as two unrelated
        # cases: same key, same ids, same code path, real clock on both.
        assert _verify(_sign(ttl=60)) is not None
        assert _verify(_sign(ttl=_T, now=int(time.time()) - _T - 60)) is None


class TestTheThreeAgesTheBoxNames:
    """31 min · `T−1 min` · `T+1 min`, each against its own control."""

    def test_at_31_MINUTES_a_wall_tile_is_still_alive(self):
        t0 = int(time.time())
        assert _verify(_sign(ttl=_T, now=t0), now=t0 + 31 * 60) is not None

    def test_and_THAT_IS_THE_POINT_a_PREVIEW_code_is_already_dead_there(self):
        # 🔴 What "31 minutes" actually means. It is one minute past the preview
        # TTL, and the claim is that a wall tile OUTLIVES a preview code. Without
        # this contrast the row above is satisfied by any TTL over half an hour.
        t0 = int(time.time())
        preview = _sign(ttl=_PREVIEW_TTL, now=t0, wall=False)
        assert _verify(preview, now=t0 + 31 * 60) is None
        assert _verify(preview, now=t0 + 29 * 60) is not None

    def test_at_T_MINUS_one_minute_it_is_alive(self):
        t0 = int(time.time())
        assert _verify(_sign(ttl=_T, now=t0), now=t0 + _T - 60) is not None

    def test_at_T_PLUS_one_minute_it_is_dead(self):
        t0 = int(time.time())
        assert _verify(_sign(ttl=_T, now=t0), now=t0 + _T + 60) is None

    def test_EXACTLY_at_T_it_is_dead_because_the_check_is_inclusive(self):
        # `exp <= current`. The boundary itself, which neither of the ±1 min
        # rows pins — and the one an off-by-one would move.
        t0 = int(time.time())
        assert _verify(_sign(ttl=_T, now=t0), now=t0 + _T) is None

    def test_one_second_BEFORE_T_it_is_alive(self):
        t0 = int(time.time())
        assert _verify(_sign(ttl=_T, now=t0), now=t0 + _T - 1) is not None


class TestTheOnlyGenuinelyAgeCoupledRisk:
    """🔴 KEY-RING ROTATION — what a real soak would have caught, and only by luck.

    A week-old code names a `kid` in a ring the operator may have rotated since.
    The link then dies EARLY, before its advertised expiry. This is the one place
    where ELAPSED TIME genuinely changes the answer, so it is tested deliberately
    here instead of being left to whether a rotation happened to fall inside a
    soak window.

    🔴 WHAT THESE ROWS PROVE, AND WHAT THEY DO NOT — measured, because the first
    version of this docstring got it wrong. It claimed `ring.get(kid) is None` is
    what refuses the code. A mutation making that lookup PERMISSIVE (fall back to
    any other key in the ring) left all 16 tests GREEN: with a different secret
    the HMAC no longer matches, so the refusal arrives from `compare_digest`
    instead. **Two mechanisms refuse the same input, and these rows do not
    discriminate which one acted.** That is fine for the operator-visible claim —
    a rotated-out link stops working — and it is NOT evidence about the lookup.
    Both rotation shapes are covered below because they are different operator
    actions, not because they take different code paths.
    """

    def test_a_code_whose_KID_has_been_rotated_out_is_refused(self):
        t0 = int(time.time())
        # Signed by kid 2, then presented to a ring that no longer carries it.
        code = _sign(ttl=_T, now=t0, kid="2")
        assert _verify(code, now=t0 + 60,
                       keys="1:unit-test-route-code-secret") is None

    def test_CONTROL_the_SAME_code_is_honoured_while_its_kid_is_still_in_the_ring(self):
        # Without this the refusal above is equally explained by a malformed
        # code, a wrong secret, or an expiry that had already passed.
        t0 = int(time.time())
        code = _sign(ttl=_T, now=t0, kid="2")
        assert _verify(code, now=t0 + 60, keys=_KEYS) is not None

    def test_rotated_IN_PLACE_same_kid_new_secret_is_also_refused(self):
        # The second operator shape, and the commoner one: the kid stays, its
        # SECRET is replaced. Here the lookup succeeds and the MAC is what
        # refuses — which is why the pair matters even though both end in None.
        t0 = int(time.time())
        code = _sign(ttl=_T, now=t0, kid="2")
        assert _verify(
            code, now=t0 + 60,
            keys="1:unit-test-route-code-secret,2:ROTATED-secret") is None

    def test_rotation_kills_a_link_BEFORE_its_advertised_expiry(self):
        # The operator-visible consequence, stated as its own fact: the code is
        # nowhere near `T`, and it is still refused. That is the failure mode an
        # operator would report as "the wall link stopped working early".
        t0 = int(time.time())
        code = _sign(ttl=_T, now=t0, kid="2")
        assert _verify(code, now=t0 + 3600,
                       keys="1:unit-test-route-code-secret") is None


class TestTheMintedTTLIsTheADVERTISEDOne:
    """The end-to-end half the boundary rows assume: that what ships is `T`."""

    def test_a_round_trip_carries_exactly_the_shipped_TTL(self):
        t0 = int(time.time())
        decoded = _verify(_sign(ttl=_T, now=t0), now=t0 + 1)
        assert decoded is not None
        assert decoded.expires_at - t0 == _T

    def test_the_shipped_TTL_is_seven_days(self):
        # Read from the model default, so this row and U1's own assertion cannot
        # drift apart silently.
        assert _T == 604800
