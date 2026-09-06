"""Every tile of a served wall carries its OWN signed code — or none does.

WHY THIS EXISTS. `WallResponse`'s docstring said "NO TILE CODES YET … a separate
open lane", naming its own retirement condition: v3 landing. v3 landed in PR
#4044 and nobody went back, so for a while the code announced a blocker that was
already gone. Minting is the bridge between the endpoint (G5, merged) and
precedence (G7, decided): without a tile code there is nothing for the honour
hook to honour, so G6.3 and the kind-aware fork both have nothing to act on.

🔴 THE PROPERTY THE WHOLE DESIGN RESTS ON IS **ALL-OR-NOTHING**. A wall answered
with codes on SOME tiles is worse than one with none: whether the visitor's
choice survives their click would depend on which tile they happened to pick — a
coin-flip the operator cannot see, explain or debug. So the preconditions are
checked once, before any tile is touched, and the tests below assert both
directions rather than only the happy one.

Verification is done by VERIFYING, never by string-shape. A test asserting "looks
like base64" would pass on a code minted with the wrong kind, the wrong origin,
or for another tenant — which is every property that matters here.

The harness (`_fake` / `_post` / `_seed` / `armed`) is imported from the endpoint
suite verbatim: a second, subtly different fixture for one endpoint is how two
test files start describing two different systems.
"""

import asyncio
import json

import pytest

from app import route_code
from app.config import settings

from tests.unit.test_wall_endpoint import (  # noqa: F401  (fixtures are used)
    CAMPAIGN,
    COMPANY,
    OFFER_1,
    OFFER_2,
    TARGET_1,
    TARGET_2,
    WALL_A,
    _fake,
    _post,
    _seed,
    armed,
)

KEY = "wall-tile-code-test-key-aaaaaaaaaaaaaaaaaaaa"


@pytest.fixture
def coded(armed, monkeypatch):
    """`armed`, PLUS a live signing ring — the only difference."""
    monkeypatch.setattr(settings, "route_code_keys", f"1:{KEY}")
    monkeypatch.setattr(settings, "route_code_active_kid", "1")


def _served(store) -> dict:
    asyncio.run(_seed(store.client()))
    r = _post(store)
    assert r.status_code == 200, r.text
    return r.json()


class TestTheRingDecidesWhetherCodesExistAtAll:
    def test_ring_OFF_mints_nothing_and_says_so(self, armed):
        # `armed` alone leaves the ring empty. Every tile still serves — the
        # catalogue is the product; the code is an enhancement on top of it.
        body = _served(_fake())
        assert body["matched"] is True
        assert body["tiles"]
        assert all(t["route_code"] is None for t in body["tiles"])
        # `expires_at` absent rather than a number: a caller must be able to
        # tell "no codes here" from "codes that never expire", which is the far
        # more dangerous reading of a missing expiry.
        assert body["expires_at"] is None

    def test_ring_ON_mints_one_per_tile(self, coded):
        body = _served(_fake())
        assert body["matched"] is True
        assert len(body["tiles"]) >= 2
        assert all(t["route_code"] for t in body["tiles"])
        assert isinstance(body["expires_at"], int)


class TestTheCodeSAYSWhatItIs:
    """Read back through `route_code.verify()`, never off the string."""

    def test_every_tile_code_is_a_WALL_claim_carrying_its_origin(self, coded):
        body = _served(_fake())
        for tile in body["tiles"]:
            claims = route_code.verify(tile["route_code"])
            assert claims is not None, "a code this node minted must verify here"
            # `verify()` returns a frozen `RouteCode`, not a dict — a verified
            # decision is evidence, not a working value.
            assert claims.kind == route_code.KIND_WALL
            # 🔴 The origin is the WALL, not the delivering flow. They are
            # different concepts that share the word `flow_id`, and conflating
            # them is the substitution `entity-boundaries` exists to stop.
            assert claims.origin_flow_id == int(WALL_A)
            # And ask the QUESTION the codec exposes rather than re-deriving it
            # from the two fields: `is_wall_claim` exists so kind and origin
            # cannot drift apart at a call site, and a test that reimplements it
            # is one more site to drift.
            assert claims.is_wall_claim is True
            assert claims.company_id == COMPANY
            assert claims.campaign_id == int(CAMPAIGN)

    def test_each_tile_names_its_OWN_offer_and_target(self, coded):
        # Rules out a real and easy bug: minting once and copying the same code
        # onto every tile. Every assertion above would still pass — kind,
        # origin, tenant and campaign are identical across tiles by construction.
        body = _served(_fake())
        pairs = set()
        for t in body["tiles"]:
            c = route_code.verify(t["route_code"])
            pairs.add((c.offer_id, c.offer_target_id))
        assert pairs == {(OFFER_1, TARGET_1), (OFFER_2, TARGET_2)}

        codes = [t["route_code"] for t in body["tiles"]]
        assert len(set(codes)) == len(codes), "two tiles must not share a code"

    def test_a_minted_code_does_NOT_verify_under_another_secret(self, coded, monkeypatch):
        # The calibration for this whole class: if `verify()` accepted anything,
        # every assertion above would be theatre.
        body = _served(_fake())
        code = body["tiles"][0]["route_code"]
        monkeypatch.setattr(settings, "route_code_keys", "1:" + "z" * 44)
        assert route_code.verify(code) is None


class TestAllOrNothing:
    """A partially-coded wall is the one shape this must never produce."""

    def test_an_unreadable_wall_id_mints_NOTHING_and_still_serves(self, coded, monkeypatch):
        # `sign()` refuses a v3 whose origin is 0, so this is not tidiness: an
        # unknown wall id has no honest v3 to mint. The catalogue must survive
        # the loss of the enhancement.
        from app import main as main_mod

        real = main_mod._to_int_or_none
        monkeypatch.setattr(
            main_mod, "_to_int_or_none",
            lambda v: None if str(v) == WALL_A else real(v),
        )
        body = _served(_fake())
        assert body["matched"] is True
        assert body["tiles"], "the catalogue still serves without codes"
        assert all(t["route_code"] is None for t in body["tiles"])
        assert body["expires_at"] is None


class TestMintingAddsNoWrites:
    def test_a_CODED_wall_still_performs_zero_writes(self, coded):
        # The wall path's zero-write property is pinned by the endpoint suite
        # with detectors proven able to fire. This asserts it SURVIVES minting.
        # `sign()` is pure over the keyring and touches no Redis — but "by
        # construction" is a claim, and the claim is cheap to check.
        store = _fake()
        asyncio.run(_seed(store.client()))
        log: list = []
        r = _post(store, log=log)
        assert r.status_code == 200, r.text
        body = r.json()

        assert all(t["route_code"] for t in body["tiles"]), (
            "the ring must be live, or this test proves nothing about minting"
        )
        assert log == [], f"the coded wall path wrote: {json.dumps(log)[:400]}"
