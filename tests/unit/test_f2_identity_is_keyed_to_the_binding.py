"""Ф2 / W7 — a visitor is `returning` to a ROUTER, not to a campaign.

THE PROPERTY, in the protocol's own words:

    after re-key, a visitor clicking two DIFFERENT bindings of the SAME campaign
    is `unique` on both; the same binding twice is `returning` on the second

and its falsifier, which these tests run as a CONTROL rather than describe:

    pre-change both are `returning` on the second click regardless of binding

Both arms run the identical steps against the identical fake Redis; the only
difference is `settings.identity_rekey_to_binding`. That is what makes the pair
a measurement of the re-key instead of a measurement of the fixture.

🔴 THE HAZARD THIS DESIGN EXISTS TO AVOID. `_roaming` counts SET SIZE. Re-keying
in place — writing binding ids into the campaign-keyed set — would have made
campaign 17 and binding 17 the same member `"17"`: `is_returning` matching on the
wrong space, and one visitor's single place counted twice into roaming. Hence a
separate key AND prefixed members, both pinned below.
"""
from __future__ import annotations

import fakeredis.aioredis
import pytest

from app.config import settings
from app.identity import (
    _campaigns_key,
    _place_bucket,
    _places_key,
    persist_identity,
    resolve_identity,
)

TTL = 1000
COMPANY = 1


def _fr():
    return fakeredis.aioredis.FakeRedis(decode_responses=True)


@pytest.fixture
def rekey_on():
    """Turn the READ side of the re-key on for one test, then put it back."""
    before = settings.identity_rekey_to_binding
    settings.identity_rekey_to_binding = True
    yield
    settings.identity_rekey_to_binding = before


@pytest.fixture
def rekey_off():
    before = settings.identity_rekey_to_binding
    settings.identity_rekey_to_binding = False
    yield
    settings.identity_rekey_to_binding = before


async def _click(r, *, campaign, binding, vid="V"):
    """One full visit: resolve, then persist what the resolve learned."""
    res = await resolve_identity(
        r, company_id=COMPANY, funnel_user_id=None, visitor_id=vid,
        campaign_id=campaign, binding_id=binding, source_trusted=False, ttl=TTL,
    )
    await persist_identity(
        r, company_id=COMPANY, uid=res.uid, funnel_user_id=None, visitor_id=vid,
        campaign_id=campaign, binding_id=binding, source_trusted=False, ttl=TTL,
    )
    return res


class TestThePlaceBucketCannotCollide:
    """The prefix is load-bearing, not cosmetic."""

    def test_a_campaign_id_and_a_binding_id_of_the_same_number_differ(self):
        assert _place_bucket("17", 0) != _place_bucket("17", 17)
        assert _place_bucket("17", 0) == "c17"
        assert _place_bucket("17", 17) == "b17"

    def test_binding_zero_means_no_binding_not_binding_zero(self):
        """Every binding-less click keeping a campaign-scoped place is what
        stops the geo branch pooling unrelated visitors into one shared place."""
        assert _place_bucket("42", 0) == "c42"

    def test_a_click_with_neither_is_the_empty_sentinel(self):
        assert _place_bucket(None, 0) == ""


@pytest.mark.asyncio
class TestTheReKeyedBehaviour:
    async def test_two_bindings_of_one_campaign_are_both_unique(self, rekey_on):
        """The property W7 names. Same campaign, different router."""
        r = _fr()
        first = await _click(r, campaign="10", binding=1)
        second = await _click(r, campaign="10", binding=2)

        assert second.uid == first.uid, "same visitor, so the uid must be stable"
        assert first.is_unique is True
        assert second.is_unique is True, (
            "a different binding of the same campaign is a different PLACE"
        )
        assert second.is_returning is False

    async def test_the_same_binding_twice_is_returning_on_the_second(self, rekey_on):
        r = _fr()
        first = await _click(r, campaign="10", binding=1)
        second = await _click(r, campaign="10", binding=1)

        assert second.uid == first.uid
        assert first.is_returning is False
        assert second.is_returning is True
        assert second.is_unique is False

    async def test_a_second_binding_makes_the_visitor_a_roamer(self, rekey_on):
        """Axis 2 is the visitor's HISTORY: two places seen, so roaming — and
        `(1,0,1)`, the cell that did not exist before Ф3(b)."""
        r = _fr()
        await _click(r, campaign="10", binding=1)
        second = await _click(r, campaign="10", binding=2)

        assert (second.is_unique, second.is_returning, second.is_roaming) == (
            True, False, True,
        )

    async def test_coming_back_to_the_first_binding_is_returning_AND_roaming(
        self, rekey_on,
    ):
        """The owner's own sentence — «роумінг, який повертається на той самий
        роутер» — which is the `(0,1,1)` cell."""
        r = _fr()
        await _click(r, campaign="10", binding=1)
        await _click(r, campaign="10", binding=2)
        third = await _click(r, campaign="10", binding=1)

        assert (third.is_unique, third.is_returning, third.is_roaming) == (
            False, True, True,
        )


@pytest.mark.asyncio
class TestTheControlIsTheFalsifier:
    """Run as the protocol's falsifier: the SAME steps with the flag off must
    reproduce the pre-change answer. If this ever agrees with the arm above,
    the flag is not gating anything and neither arm proves the re-key."""

    async def test_pre_change_a_second_binding_is_returning(self, rekey_off):
        r = _fr()
        first = await _click(r, campaign="10", binding=1)
        second = await _click(r, campaign="10", binding=2)

        assert second.uid == first.uid
        assert second.is_returning is True, (
            "campaign-keyed: the campaign was seen, so the visitor is returning "
            "regardless of which binding they arrived through"
        )
        assert second.is_unique is False


@pytest.mark.asyncio
class TestDualWriteWarmsTheNewSetWhileTheReadIsDark:
    async def test_the_places_set_is_written_even_with_the_flag_off(
        self, rekey_off,
    ):
        """The reason the cutover cannot mass-re-uniquify anyone: by the time
        the read flips, the places set already knows where everyone has been."""
        r = _fr()
        res = await _click(r, campaign="10", binding=7)

        places = await r.smembers(_places_key(COMPANY, res.uid))
        campaigns = await r.smembers(_campaigns_key(COMPANY, res.uid))

        assert places == {"b7"}, "the binding-keyed set is populated while dark"
        assert campaigns == {"10"}, "and the campaign-keyed set is untouched"

    async def test_the_two_sets_never_share_a_member(self, rekey_off):
        """The collision that a re-key IN PLACE would have produced: campaign
        "7" and binding 7 as the same member of one set."""
        r = _fr()
        res = await _click(r, campaign="7", binding=7)

        places = await r.smembers(_places_key(COMPANY, res.uid))
        campaigns = await r.smembers(_campaigns_key(COMPANY, res.uid))

        assert places == {"b7"}
        assert campaigns == {"7"}
        assert not (places & campaigns), (
            "prefixed members are why one set could never be mistaken for the other"
        )

    async def test_a_binding_less_click_still_writes_a_place(self, rekey_off):
        r = _fr()
        res = await _click(r, campaign="10", binding=0)
        assert await r.smembers(_places_key(COMPANY, res.uid)) == {"c10"}
