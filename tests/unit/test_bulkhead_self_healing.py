"""THE PROPERTY both admission bulkheads must have: they SELF-HEAL.

Owner mandate, 2026-09-09, verbatim: «якщо система впала через велике
навантаження, то потім, коли навантаження спало, вона має приходити в норму».

A fuse may trip under load. What it may never do is stay tripped after the load
is gone. So the property under test is not "does it shed at the cap" — that is
already covered by `test_wall_endpoint.py` and `test_preview_bulkhead.py` — it is:

    after a request is OVER, the in-flight budget is back where it started,
    on EVERY exit path, for EVERY input size.

🔴 WHY THIS FILE EXISTS SEPARATELY, and why it must never grow an autouse reset.
`test_wall_endpoint.py` carries a deliberate `_counter_reset` autouse fixture that
zeroes `main._wall_tiles_inflight` before AND after every test. Its purpose is
legitimate (a test that 503s must not poison the next one) and its side effect is
total: **that suite is structurally incapable of observing budget retained across
requests.** The instrument resets the very quantity it would need to measure. This
file measures it, so it resets ONLY at the start of a test and never at the end.

CALIBRATION, so an all-red run can never be mistaken for a broken file: the
preview cases below PASS on the code as it stands. If they ever go red together
with the wall cases, the harness is broken, not the subject.
"""
from __future__ import annotations

import asyncio

import pytest
from unittest.mock import patch

from app import main
from app.config import settings

from tests.unit import test_wall_endpoint as W


# --------------------------------------------------------------------------- #
# helpers                                                                      #
# --------------------------------------------------------------------------- #
def _wall_of(n_tiles: int) -> str:
    """A wall carrying exactly `n_tiles` DISTINCT tiles.

    Distinct because the candidate loader dedupes by (offer, target); duplicates
    would silently shrink the one number this file varies.
    """
    pairs = [(W.OFFER_1, W.TARGET_1), (W.OFFER_2, W.TARGET_2)]
    while len(pairs) < n_tiles:
        i = len(pairs)
        pairs.append((900 + i, 9000 + i))
    return W._tiles(*pairs[:n_tiles])


@pytest.fixture
def wall_armed(monkeypatch):
    monkeypatch.setattr(settings, "offerwall_serve_enabled", True)
    monkeypatch.setattr(settings, "tds_secret_key", W.SECRET)
    monkeypatch.setattr(settings, "returning_resolver_enabled", True)


def _seeded_store(walls=("A", "B"), tiles=30):
    store = W._fake()
    asyncio.run(W._seed(store.client(), walls=walls, wall_a_tiles=_wall_of(tiles)))
    return store


# --------------------------------------------------------------------------- #
# the wall                                                                     #
# --------------------------------------------------------------------------- #
class TestTheWallBudgetReturnsToZero:
    @pytest.mark.parametrize("tiles", [2, 23, 24, 25, 30, 60])
    def test_a_completed_request_retains_no_budget_at_any_candidate_size(
        self, wall_armed, tiles
    ):
        """The size sweep straddles the 24-tile admission charge on purpose.

        Measured on the pre-fix code: sizes at or below 23 candidate tiles came
        back clean and everything above retained `candidate - 24` per request,
        accumulating linearly (2/23 clean; 24->1,2,3; 30->7,14,21; 60->37,74,111),
        with every response a 200. So a bulkhead can be leaking its whole budget
        away while every single response looks perfect.
        """
        main._wall_tiles_inflight = 0
        store = _seeded_store(tiles=tiles)

        for i in range(3):
            resp = W._post(store)
            assert resp.status_code == 200, f"request {i} did not complete"
            assert main._wall_tiles_inflight == 0, (
                f"after request {i + 1} of a {tiles}-tile wall the bulkhead still "
                f"holds {main._wall_tiles_inflight} tiles of budget. Nothing is in "
                f"flight — the request is over — so this budget is never coming "
                f"back, and enough of these permanently wedge the worker at 503."
            )

    def test_an_exception_inside_the_body_retains_no_budget(self, wall_armed):
        """The `finally` is the whole reason admission is safe on the sad path.

        Pinned rather than assumed: this is the property a future refactor is
        most likely to break by moving the release out of the `finally`.
        """
        main._wall_tiles_inflight = 0
        store = _seeded_store(tiles=30)

        async def _boom(req, charged):
            raise RuntimeError("deliberate")

        with patch.object(main, "_wall_body", _boom):
            with pytest.raises(Exception):
                W._post(store)

        assert main._wall_tiles_inflight == 0, (
            "a request that raised still holds budget — the release is not on "
            "the exception path"
        )

    def test_a_shed_request_consumes_nothing(self, wall_armed, monkeypatch):
        """A refusal must not charge. Otherwise a flood of 503s is itself the

        thing that keeps the bulkhead shut, and the fuse can never re-open.
        """
        monkeypatch.setattr(settings, "offerwall_max_tiles_inflight", 24)
        monkeypatch.setattr(settings, "offerwall_admission_charge_tiles", 24)
        main._wall_tiles_inflight = 24  # already full
        store = _seeded_store(tiles=30)

        assert W._post(store).status_code == 503
        assert main._wall_tiles_inflight == 24, (
            "a shed request moved the counter; a refusal must be free"
        )


# --------------------------------------------------------------------------- #
# the ordinary preview — the CALIBRATION half of this file                     #
# --------------------------------------------------------------------------- #
class TestThePreviewBudgetReturnsToZero:
    """These pass on the code as it stands, and that is the point.

    `_preview_inflight` is `+= 1` / `-= 1` in a `finally` with no second
    increment anywhere, so it has nothing to reconcile and nothing to leak. If
    these ever fail alongside the wall cases, suspect the harness before the
    subject.
    """

    def test_the_preview_counter_is_symmetric_by_construction(self):
        """One acquire site, one release site — asserted on the SOURCE.

        A count, not a run: the wall's defect was two increments against one
        decrement, and that is visible by counting the sites rather than by
        exercising them. This is the cheapest possible regression guard against
        someone adding a second increment again.
        """
        import inspect

        src = inspect.getsource(main)
        acquires = src.count("_preview_inflight += ")
        releases = src.count("_preview_inflight -= ")
        assert acquires == releases == 1, (
            f"_preview_inflight has {acquires} acquire and {releases} release "
            f"sites; symmetry is what makes this bulkhead leak-proof"
        )

    def test_the_wall_counter_is_symmetric_by_construction(self):
        """The same count for the wall — this is the structural statement of

        the defect, and the fix must make it true rather than merely make the
        numbers work out on today's inputs.
        """
        import inspect

        src = inspect.getsource(main)
        acquires = src.count("_wall_tiles_inflight += ")
        releases = src.count("_wall_tiles_inflight -= ")
        assert acquires == releases, (
            f"_wall_tiles_inflight has {acquires} acquire site(s) against "
            f"{releases} release site(s). Every path that TAKES budget must have "
            f"a path that GIVES IT BACK; an unmatched increment is a permanent "
            f"leak no matter how small the delta looks."
        )
