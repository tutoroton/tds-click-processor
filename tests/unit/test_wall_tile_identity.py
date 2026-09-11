"""T5 (C9) — a TILE click carries the WALL's identity, asserted rather than read.

🔴 THE LOUDEST CLAIM OF THIS REBUILD HAD NO ASSERTION BEHIND IT.

`attribution["flow_id"]` is taken from the CASCADE WINNER (`flow.get("_id")`)
and set UNCONDITIONALLY — the route-code branch overrides only the TARGET. So on
an offerwall-family campaign a TILE click carries `flow_id = the wall` by the
very same line as a plain one, and C3 was therefore delivered BY CONSTRUCTION.

Until this file existed that was a fact about the SOURCE and nothing else. The
tile-path suite (`test_wall_tile_beats_a_returning_flow_and_pins.py`) drives
`_resolve_action_with_sticky`, one frame BELOW where attribution is built: it
passes `flow_id="300"` in as a fixture INPUT and asserts the destination and the
pin writes, never the resulting attribution. Adding an assertion THERE would
have measured the fixture, not the mechanism — the same "one frame up" mistake
this programme has already paid for twice.

So every test here drives the FULL path (`router.route`) with a real signed v3
WALL code. That is the only frame where the claim is observable at all.

🔴 WHY THE WALL CARRIES TWO TILES AND THE CODE NAMES THE SECOND.

On an offerwall-family campaign the wall wins the cascade anyway, so a one-tile
fixture would report `flow_id == WALL_ID` whether the code acted or not — green
by construction, and structurally unable to fail. With two tiles the code's
effect has a visible signature of its own: the FIRST tile is what serves when no
code is present, so a destination of the SECOND proves the code was honoured on
the very click whose attribution is being asserted.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from app import router
from app.config import settings

from tests.unit.test_router_cascade import _click, _route_with
from tests.unit.test_route_code_honoured import (
    _ACTIVE_KID,
    _CAMPAIGN,
    _CODED_TARGET,
    _KEYS,
    _OFFER,
)
from tests.unit.test_wall_delivers_the_click import (
    FLOW_ID,
    WALL_ID,
    _redis,
    _tile,
)
from tests.unit.test_wall_tile_membership import _sign_wall

_OTHER_OFFER = 8
_OTHER_TARGET = 88


@pytest.fixture
def wall_delivery_on(monkeypatch):
    monkeypatch.setattr(settings, "wall_delivery_enabled", True, raising=False)
    yield


def _redis_two_tiles(*, family: str, with_ordinary_flow: bool = False):
    """Tile ORDER matters here: the coded pair is deliberately NOT first."""
    return _redis(
        str(_CAMPAIGN), family=family,
        tiles=[_tile(_OTHER_OFFER, _OTHER_TARGET),
               _tile(_OFFER, _CODED_TARGET)],
        targets={
            str(_OTHER_TARGET): {
                "_id": str(_OTHER_TARGET),
                "url": "https://first-tile.example/",
                "availability": "active", "status": "active",
                "offer_id": str(_OTHER_OFFER),
            },
            str(_CODED_TARGET): {
                "_id": str(_CODED_TARGET),
                "url": "https://coded.example/x",
                "availability": "active", "status": "active",
                "offer_id": str(_OFFER),
            },
        },
        with_ordinary_flow=with_ordinary_flow,
    )


def _route(redis, *, code):
    params = {} if code is None else {router.ROUTE_CODE_PARAM: code}
    with patch.object(settings, "route_preview_enabled", True), \
            patch.object(settings, "route_code_keys", _KEYS), \
            patch.object(settings, "route_code_active_kid", _ACTIVE_KID):
        return _route_with(redis, _click(params))


class TestATileClickCarriesTheWallsIdentity:
    def test_WITHOUT_a_code_the_FIRST_tile_serves(self, wall_delivery_on):
        """The calibration half. Without it, "the second tile served" proves
        nothing — the fixture might simply always answer with it."""
        result = _route(_redis_two_tiles(family="offerwall"), code=None)
        assert result is not None
        assert result["url"].startswith("https://first-tile.example/")

    def test_a_TILE_click_is_attributed_to_the_WALL(self, wall_delivery_on):
        """T5 itself. Both halves on ONE click: the code was honoured (the
        SECOND tile's destination) AND the attribution names the wall."""
        result = _route(_redis_two_tiles(family="offerwall"), code=_sign_wall())
        assert result is not None
        # the code ACTED — otherwise the FIRST tile would have served
        assert result["url"].startswith("https://coded.example/")
        # …and this is the claim that had no assertion until now
        assert result["attribution"]["flow_id"] == int(WALL_ID)
        assert result["attribution"]["audience_pool"] == "offerwall"


class TestTheOlderEpochStaysCorrect:
    def test_the_SAME_tile_on_a_STANDARD_campaign_names_the_ORDINARY_flow(self):
        """The control that makes the claim above mean something.

        Same signed code, same wall, same visitor — only the campaign's FAMILY
        differs. Here the wall cannot win (isolation), so the ordinary flow is
        the delivering one and the wall is named by PROVENANCE instead. If
        `flow_id` simply followed the code's origin, this would read `WALL_ID`
        and go red.

        `wall_delivery_enabled` is deliberately NOT requested: a tile code is a
        visitor's link and is honoured independently of the delivery flag,
        which is exactly why the two ids must stay distinct concepts.
        """
        result = _route(
            _redis_two_tiles(family="standard", with_ordinary_flow=True),
            code=_sign_wall(),
        )
        assert result is not None
        assert result["url"].startswith("https://coded.example/")
        assert result["attribution"]["flow_id"] == int(FLOW_ID)
        trace = result["attribution"]["routing_trace"]
        assert int(trace["origin_wall_id"]) == int(WALL_ID)


class TestTheRefutedDiscriminatorStaysRefuted:
    """🔴 `flow_id == origin_wall_id` was proposed as the way to recognise a wall
    click. It is REFUTED — and the two tests below are why the refutation has to
    be stated as *"the equality carries no information"* rather than as
    *"the equality is false"*.

    The same wall campaign produces BOTH answers, depending only on whether the
    visitor picked a tile:

      * delivered, no tile picked  ->  910 == 0    (unequal, and a wall click)
      * delivered via a tile code  ->  910 == 910  (equal, also a wall click)

    So the test fails in one direction and succeeds in the other on clicks of
    the same kind. That is worse than a discriminator that is merely wrong: a
    reader who meets the second case first will conclude it works. The working
    discriminator is `audience_pool == 'offerwall'`, asserted above.
    """

    def test_a_DELIVERED_click_has_identity_and_NO_provenance(self, wall_delivery_on):
        """No tile was picked, so there is no provenance to record — and the
        proposed equality is FALSE on the bulk of the new epoch's traffic."""
        result = _route(_redis_two_tiles(family="offerwall"), code=None)
        assert result is not None
        assert result["attribution"]["flow_id"] == int(WALL_ID)
        trace = result["attribution"]["routing_trace"]
        assert int(trace.get("origin_wall_id") or 0) == 0

    def test_a_TILE_click_on_a_WALL_campaign_makes_them_COINCIDE(self, wall_delivery_on):
        """…and here the very same equality is TRUE, by coincidence rather than
        by aliasing: one wall both served the click and was chosen from.

        Measured, not assumed — this case was found by probing the router, and
        it is the half that makes the equality untrustworthy in BOTH directions.
        """
        result = _route(_redis_two_tiles(family="offerwall"), code=_sign_wall())
        assert result is not None
        attribution = result["attribution"]
        assert attribution["flow_id"] == int(WALL_ID)
        assert int(attribution["routing_trace"]["origin_wall_id"]) == int(WALL_ID)
        # The two are equal here — and that is precisely why equality cannot be
        # the test for "is this a wall click".
        assert attribution["audience_pool"] == "offerwall"
