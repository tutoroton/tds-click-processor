"""G6.3 — a WALL-kind code acts as a tile only while its wall still carries it.

WHAT THIS IMPLEMENTS, and it is a ruling rather than an invention. Owner decision
D3-OPEN-3: an offer REMOVED from a wall, while the visitor still holds its link,
*"falls back to the DEFAULT scenario, not an error"*. So a stale tile is not
honoured and the click routes normally — which is what the honour hook returning
`None` means.

🔴 WHY IT MATTERS MORE ONCE THE TILE OUTRANKS THE PIN. `41-G7-PRECEDENCE-DECISION`
rules that a WALL-kind code beats a pre-existing sticky pin (owner D3,
«Сильніша плитка»). Membership is what keeps that from becoming a blank cheque:
without it, a code minted for wall W would buy that precedence forever, including
after the operator took the offer off W. The two are one mechanism — the
precedence is what makes the check load-bearing, and the check is what makes the
precedence safe.

⚠️ ORIGIN IS NOT DELIVERY. The comparison is the code's origin wall against THAT
WALL's own membership, never against the flow that wins routing for this click.
Both are called `flow_id`; they are different concepts, and conflating them
because of the shared word is the substitution `entity-boundaries` forbids.
"""

import json

import pytest

from app import offerwall


def _wall(*pairs) -> dict:
    return {"action_config": json.dumps(
        {"tiles": [{"offer_id": o, "target_id": t} for o, t in pairs]})}


class TestTheHappyDirectionExists:
    """The calibration half. Without it every refusal below is satisfied by a
    function that refuses everything."""

    def test_a_tile_on_the_wall_is_recognised(self):
        assert offerwall.wall_contains_tile(_wall((21, 31), (22, 32)), 21, 31)

    def test_the_OTHER_tile_is_recognised_too(self):
        assert offerwall.wall_contains_tile(_wall((21, 31), (22, 32)), 22, 32)


class TestARemovedOrForeignTileIsRefused:
    def test_an_offer_no_longer_on_the_wall(self):
        # D3-OPEN-3 itself: the operator took offer 99 off this wall.
        assert not offerwall.wall_contains_tile(_wall((21, 31)), 99, 31)

    def test_the_right_offer_with_the_WRONG_target(self):
        # The pair is the unit, not either id. An offer can be pinned to a
        # different target on a different wall, and a code naming the old
        # pairing must not survive the change.
        assert not offerwall.wall_contains_tile(_wall((21, 31)), 21, 32)

    def test_the_right_target_with_the_WRONG_offer(self):
        assert not offerwall.wall_contains_tile(_wall((21, 31)), 22, 31)

    def test_an_empty_wall_carries_nothing(self):
        assert not offerwall.wall_contains_tile(_wall(), 21, 31)


class TestUnreadableMeansUNPROVEN:
    """🔴 FAIL-CLOSED here, and its sibling `_tile_count` fails the OTHER way on
    the same input. The asymmetry is deliberate: an unreadable wall means the
    claim cannot be proven, and an unproven claim must not buy precedence —
    while for the admission budget, treating unknown as cheap would let a
    corrupt row buy unlimited work."""

    @pytest.mark.parametrize("wall", [
        {},
        {"action_config": ""},
        {"action_config": "not json at all"},
        {"action_config": json.dumps({"tiles": "not a list"})},
        {"action_config": json.dumps({})},
    ])
    def test_an_unreadable_wall_never_confirms_membership(self, wall):
        assert not offerwall.wall_contains_tile(wall, 21, 31)

    def test_and_the_BUDGET_treats_the_same_input_as_expensive(self):
        # The two callers of `parse_tiles` want opposite defaults on `None`, and
        # this is the test that would go red if someone "simplified" the parser
        # into a single fused return.
        assert offerwall._tile_count({"action_config": "not json"}) > 0

    def test_a_readable_EMPTY_wall_is_zero_for_the_budget(self):
        # …and the discriminator for that: `[]` and `None` are different
        # answers. A readable wall with no tiles genuinely costs nothing.
        assert offerwall._tile_count(_wall()) == 0


class TestOneBadTileDoesNotPoisonTheWall:
    def test_a_malformed_entry_is_skipped_not_fatal(self):
        wall = {"action_config": json.dumps({"tiles": [
            {"offer_id": "banana", "target_id": None},
            {"offer_id": 21, "target_id": 31},
        ]})}
        assert offerwall.wall_contains_tile(wall, 21, 31)

    def test_a_non_object_entry_is_skipped(self):
        wall = {"action_config": json.dumps({"tiles": [
            7, "nope", {"offer_id": 21, "target_id": 31},
        ]})}
        assert offerwall.wall_contains_tile(wall, 21, 31)


class TestIdsCompareAsNUMBERS:
    """The stored config is JSON written by another service, so an id may arrive
    as `5` or `"5"`.

    ⚠️ **This class does NOT discriminate int-compare from string-compare, and
    saying so is the point.** The first draft claimed it did; a mutation
    swapping `int()` for `str()` was predicted RED and came back GREEN, because
    both agree on every shape a JSON writer emits. What these tests DO pin is
    that a string-typed id matches at all — which a naive `tile["offer_id"] is
    offer_id` or a typed-dict lookup would break. The int compare itself is a
    semantic choice, recorded as such in `wall_contains_tile`'s docstring."""

    def test_string_ids_in_the_config_still_match(self):
        wall = {"action_config": json.dumps(
            {"tiles": [{"offer_id": "21", "target_id": "31"}]})}
        assert offerwall.wall_contains_tile(wall, 21, 31)

    def test_and_a_genuinely_different_id_still_does_not(self):
        # The calibration for the coercion: loosening the compare must not have
        # loosened WHAT is compared.
        wall = {"action_config": json.dumps(
            {"tiles": [{"offer_id": "21", "target_id": "31"}]})}
        assert not offerwall.wall_contains_tile(wall, 21, 999)


# --------------------------------------------------------------------------- #
# The HOOK actually asks. The classes above test the helper; these test the    #
# WIRING, because a correct helper nobody calls is the failure mode this run   #
# has caught three times today in other people's guards and twice in mine.     #
# --------------------------------------------------------------------------- #

from unittest.mock import patch  # noqa: E402

from app import route_code  # noqa: E402
from app.config import settings  # noqa: E402
from tests.unit.test_route_code_honoured import (  # noqa: E402
    _ACTIVE_KID,
    _CAMPAIGN,
    _CODED_TARGET,
    _COMPANY,
    _KEYS,
    _NORMAL_TARGET,
    _OFFER,
    _resolve,
    _target_hash,
)

WALL_ID = 910


def _sign_wall(*, origin=WALL_ID, offer_id=_OFFER, target_id=_CODED_TARGET) -> str:
    """A v3 WALL code — the shape only a wall endpoint mints."""
    with patch.object(settings, "route_code_keys", _KEYS), \
         patch.object(settings, "route_code_active_kid", _ACTIVE_KID):
        return route_code.sign(
            company_id=_COMPANY, campaign_id=_CAMPAIGN, offer_id=offer_id,
            offer_target_id=target_id, ttl_seconds=600, origin_flow_id=origin,
        )


def _hashes(wall_tiles) -> dict:
    h = {"offer_target:%d" % _CODED_TARGET: _target_hash()}
    if wall_tiles is not None:
        h["flow:%d" % WALL_ID] = _wall(*wall_tiles)
    return h


class TestTheHonourHookConsultsMembership:
    def test_a_tile_STILL_on_its_wall_is_honoured(self):
        # The calibration for this whole class: if a wall code were refused
        # unconditionally, every refusal below would prove nothing.
        result, status, _r, _i = _resolve(
            code=_sign_wall(),
            hashes=_hashes([(_OFFER, _CODED_TARGET)]),
        )
        assert result["target_id"] == str(_CODED_TARGET)
        assert result["target_selection_path"] == "route_code"

    def test_a_tile_REMOVED_from_its_wall_falls_back_to_normal_routing(self):
        # Owner D3-OPEN-3. Not an error, not a 404 — the ordinary route.
        result, status, _r, _i = _resolve(
            code=_sign_wall(),
            hashes=_hashes([(_OFFER, 999)]),
        )
        assert result["target_id"] == _NORMAL_TARGET
        assert result["target_selection_path"] == "split_weighted"

    def test_a_wall_that_cannot_be_READ_refuses_the_claim(self):
        # Fail-closed: `flow:910` absent entirely. Unproven is not proven.
        result, status, _r, _i = _resolve(
            code=_sign_wall(),
            hashes=_hashes(None),
        )
        assert result["target_id"] == _NORMAL_TARGET

    def test_a_v2_PREVIEW_code_is_untouched_by_any_of_this(self):
        # 🔴 The regression guard that matters most. `is_wall_claim` is False by
        # construction on v2, so a preview code minted before walls existed must
        # not need a wall to exist — and must not be refused for lacking one.
        from tests.unit.test_route_code_honoured import _sign

        result, status, _r, _i = _resolve(
            code=_sign(),
            hashes={"offer_target:%d" % _CODED_TARGET: _target_hash()},
        )
        assert result["target_id"] == str(_CODED_TARGET)
        assert result["target_selection_path"] == "route_code"

    def test_the_wall_read_HAPPENS_and_names_the_origin(self):
        # Proves the hook reached the wall for the reason claimed, rather than
        # having refused earlier for some unrelated reason and looking right.
        _result, _status, r, _i = _resolve(
            code=_sign_wall(),
            hashes=_hashes([(_OFFER, _CODED_TARGET)]),
        )
        assert "flow:%d" % WALL_ID in r.reads

    def test_the_membership_check_ADDS_NO_WRITES(self):
        _result, _status, r, ident = _resolve(
            code=_sign_wall(),
            hashes=_hashes([(_OFFER, _CODED_TARGET)]),
        )
        assert r.writes == [], r.writes
