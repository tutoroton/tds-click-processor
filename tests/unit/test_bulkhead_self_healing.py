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
zeroes the wall budget before AND after every test. Its purpose is legitimate — a
test that 503s must not poison the next one — and its cost is that accumulation
can never surface as a failure in some LATER test, which is how this class of
defect is usually stumbled upon.

⚠️ An earlier draft of this docstring said that suite was "structurally incapable
of observing" the leak. That is FALSE, and the reviewer was right to refuse it:
the reset is per-test, so several requests inside ONE test accumulate normally —
which is exactly what the cases below do. The accurate statement is narrower and
duller: the old suite never asserted it. The reset removed the accidental
discovery path, not the possibility. Overstating a mechanism makes it sound
inevitable that nobody caught this; it was not inevitable, it was missed.

This file resets ONLY at the start of a test and never at the end, so an assertion
that runs after the requests sees what they left behind.

CALIBRATION, so an all-red run can never be mistaken for a broken file: the
preview cases below PASS on the code as it stands. If they ever go red together
with the wall cases, the harness is broken, not the subject.
"""
from __future__ import annotations

import asyncio

import pytest
from unittest.mock import patch

from app import main
from app.bulkhead import Budget, NoCapacity
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
        # ⚠️ Assert on the LIVE budget, never on `main._wall_tiles_inflight`.
        # That module integer is retired and frozen at 0, so asserting it equals
        # 0 is true by construction — a green that measures nothing. This suite
        # passed 16/16 that way for one run before the substitution was caught.
        main._wall_budget._reset_for_tests()
        store = _seeded_store(tiles=tiles)

        for i in range(3):
            resp = W._post(store)
            assert resp.status_code == 200, f"request {i} did not complete"
            assert main._wall_budget.used == 0, (
                f"after request {i + 1} of a {tiles}-tile wall the bulkhead still "
                f"holds {main._wall_budget.used} tiles across "
                f"{main._wall_budget.live} reservation(s). Nothing is in flight — "
                f"the request is over — so this budget is never coming back, and "
                f"enough of these permanently wedge the worker at 503."
            )

    def test_an_exception_inside_the_body_retains_no_budget(self, wall_armed):
        """The `finally` is the whole reason admission is safe on the sad path.

        Pinned rather than assumed: this is the property a future refactor is
        most likely to break by moving the release out of the `finally`.
        """
        main._wall_budget._reset_for_tests()
        store = _seeded_store(tiles=30)

        async def _boom(req, reconcile):
            raise RuntimeError("deliberate")

        with patch.object(main, "_wall_body", _boom):
            with pytest.raises(Exception):
                W._post(store)

        assert main._wall_budget.used == 0, (
            "a request that raised still holds budget — the release is not on "
            "the exception path"
        )
        assert main._wall_budget.live == 0, "the reservation itself outlived the request"

    def test_the_endpoint_charges_the_REAL_tile_cost_not_the_flat_charge(self, wall_armed):
        """🔴 THE HOLE THE REST OF THIS FILE COULD NOT SEE.

        Measured, not argued: comment out `reconcile(...)` at the endpoint and
        every other test here — all 16, plus all 22 in `test_wall_endpoint.py` —
        stays GREEN. The bulkhead would then charge a flat 24 for a request of
        any real size, the variable tile budget would be gone entirely, and the
        suite would report perfect health. Clean release was proven; the WEIGHT
        never was.

        The reason is structural: every other case reads the budget when the
        request is OVER, and by then a correct release has erased the difference
        between a reconciled reservation and one that was never reconciled. This
        one looks while the request is still in flight.

        The gating half — that a larger held weight actually refuses the next
        arrival — is the primitive's contract and is pinned separately by
        `TestTheReservationPrimitive`. Together they close the chain.
        """
        main._wall_budget._reset_for_tests()
        store = _seeded_store(tiles=30)  # wall A 30 + wall B 1 = 31 candidates

        seen: dict = {}
        original = main._wall_body

        async def spy(req, reconcile):
            def watched(weight):
                reconcile(weight)
                seen["weight"] = weight
                seen["used"] = main._wall_budget.used
                seen["live"] = main._wall_budget.live

            return await original(req, watched)

        with patch.object(main, "_wall_body", spy):
            assert W._post(store).status_code == 200

        assert seen, (
            "the endpoint never called reconcile at all — the reservation kept "
            "its presumptive charge, so the budget no longer tracks real work"
        )
        assert seen["weight"] == 31, (
            f"reconciled with {seen['weight']} for a 31-tile candidate set; the "
            f"weight must be the REAL cost, not the flat admission charge"
        )
        assert seen["used"] == 31, (
            f"in flight the budget held {seen['used']}, not the reconciled 31 — "
            f"reconciliation ran but did not reach the budget"
        )
        assert seen["live"] == 1, "one request, one reservation"
        assert main._wall_budget.used == 0, "and it still released afterwards"

    def test_a_shed_request_consumes_nothing(self, wall_armed, monkeypatch):
        """A refusal must not charge. Otherwise a flood of 503s is itself the

        thing that keeps the bulkhead shut, and the fuse can never re-open.
        """
        monkeypatch.setattr(settings, "offerwall_max_tiles_inflight", 24)
        monkeypatch.setattr(settings, "offerwall_admission_charge_tiles", 24)
        store = _seeded_store(tiles=30)

        # Fill the budget with a REAL reservation. Setting the retired module
        # integer would fill nothing — it no longer governs admission, and this
        # assertion passed against it only because the probe and the subject had
        # quietly stopped being the same object.
        main._wall_budget._reset_for_tests()
        occupant = main._wall_budget.hold(cap=24, charge=24)
        occupant.__enter__()
        try:
            assert W._post(store).status_code == 503
            assert main._wall_budget.used == 24, (
                "a shed request moved the budget; a refusal must be free"
            )
        finally:
            occupant.__exit__(None, None, None)
        assert main._wall_budget.used == 0, "the occupant did not release"


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

    def test_the_retired_counter_name_raises_instead_of_reading_a_dead_zero(self):
        """A number that lies quietly is worse than a name that is absent loudly.

        `_wall_tiles_inflight` was briefly kept, frozen at 0, so an operator's
        habit would not break. That is the wrong trade: a stale tool reading it
        during saturation gets `0`, which is indistinguishable from healthy. It
        is now gone, and the module's `__getattr__` answers with the replacement.
        """
        with pytest.raises(AttributeError) as excinfo:
            main._wall_tiles_inflight  # noqa: B018 - the raise IS the assertion
        assert "_wall_budget.used" in str(excinfo.value), (
            "the signpost must name what to read instead, or it is just a crash"
        )

    def test_the_wall_takes_budget_only_through_a_reservation(self):
        """The structural statement of the fix.

        ⚠️ Written as a POSITIVE requirement on purpose. The obvious form —
        "acquire sites == release sites" — became `0 == 0` the moment the bare
        counter stopped being mutated, i.e. it would pass vacuously and keep
        passing if someone deleted the bulkhead outright. A guard whose only
        assertion is a set difference is green on an empty set.

        So this asserts what must BE there, not merely what must be absent:
        exactly one admission site, going through the reservation primitive.
        """
        import inspect

        # 🔴 Strip comments before counting. The comment that EXPLAINS the old
        # defect necessarily quotes it, so a naive text probe counts the
        # documentation as the subject and reports a leak that is only prose.
        # Measured: this exact assertion failed at 1 with zero live mutations.
        src = "\n".join(
            line.split("#", 1)[0]
            for line in inspect.getsource(main).splitlines()
        )
        direct = src.count("_wall_tiles_inflight += ") + src.count(
            "_wall_tiles_inflight -= "
        )
        assert direct == 0, (
            f"{direct} site(s) still mutate the wall budget as a bare number. "
            f"That is the shape that leaked: a released amount is REMEMBERED, "
            f"so a second acquisition has nothing matching it."
        )
        holds = src.count("_wall_budget.hold(")
        assert holds == 1, (
            f"expected exactly one wall admission site through the reservation "
            f"primitive, found {holds}. More than one means two ways in; zero "
            f"means the bulkhead is gone and this file would otherwise go green "
            f"about it."
        )


# --------------------------------------------------------------------------- #
# the primitive itself                                                         #
# --------------------------------------------------------------------------- #
class TestTheReservationPrimitive:
    """Direct tests of `Budget`, because the endpoint tests above can only

    exercise the paths the endpoint happens to take. These pin the contract the
    endpoint relies on.
    """

    def test_a_reservation_is_released_even_when_the_scope_raises(self):
        b = Budget()
        with pytest.raises(RuntimeError):
            with b.hold(cap=100, charge=10):
                raise RuntimeError("boom")
        assert b.used == 0 and b.live == 0

    def test_reconciling_replaces_rather_than_accumulates(self):
        """The whole defect in one assertion: calling reconcile repeatedly must

        not grow the held amount. The old code ADDED a delta each time.
        """
        b = Budget()
        with b.hold(cap=100, charge=10) as reconcile:
            reconcile(40)
            assert b.used == 40
            reconcile(40)
            reconcile(40)
            assert b.used == 40, "repeated reconciliation accumulated"
        assert b.used == 0

    def test_the_reconciled_weight_never_drops_below_the_admission_charge(self):
        """A cheaper-than-expected request must not buy extra admission capacity

        for its neighbours — the floor the previous `max(0, actual - charge)`
        produced by accident, kept deliberately so the CAP semantics are
        unchanged by the refactor.
        """
        b = Budget()
        with b.hold(cap=100, charge=10) as reconcile:
            reconcile(1)
            assert b.used == 10

    def test_a_closed_reservation_cannot_be_resurrected(self):
        """Reconciling after the scope exits would re-insert an entry that

        nothing will ever remove — the exact leak, through a new door.
        """
        b = Budget()
        with b.hold(cap=100, charge=10) as reconcile:
            pass
        with pytest.raises(RuntimeError):
            reconcile(50)
        assert b.used == 0

    def test_overlapping_reservations_release_only_their_own(self):
        b = Budget()
        with b.hold(cap=100, charge=10) as first:
            first(30)
            with b.hold(cap=100, charge=10) as second:
                second(20)
                assert b.used == 50
            assert b.used == 30, "the inner scope released more than its own"
        assert b.used == 0

    def test_a_reservation_releases_its_OWN_weight_when_it_finishes_out_of_order(self):
        """🔴 The second hole, and it was invisible for the same reason.

        Measured: replace `self._holds.pop(token, None)` with `popitem()` — drop
        the NEWEST reservation instead of the one that finished — and all 38
        tests stay green. Every ownership case here closed in nested LIFO order
        or used equal charges, and under both of those a wrong-owner release is
        arithmetically indistinguishable from a right one.

        So this one uses UNEQUAL weights and finishes the OLDEST first.
        """
        b = Budget()
        first = b.hold(cap=100, charge=10)
        r1 = first.__enter__()
        r1(40)
        second = b.hold(cap=100, charge=10)
        r2 = second.__enter__()
        r2(25)
        assert b.used == 65

        first.__exit__(None, None, None)  # the OLDEST leaves first
        assert b.used == 25, (
            f"released to {b.used}: the departing reservation took someone "
            f"else's weight with it. 40 would mean it dropped the survivor."
        )

        r2(30)  # the survivor's own callback must still work afterwards
        assert b.used == 30, "the surviving reservation lost its identity"
        second.__exit__(None, None, None)
        assert b.used == 0 and b.live == 0

    def test_the_reservation_is_inserted_INSIDE_the_protected_block(self):
        """Ordering, asserted on the source, because no test can inject a
        MemoryError at the one instruction where it would matter.

        If the insert happens before `try:`, anything raised between the two —
        an allocation failure while building the closure is the realistic one —
        strands the entry with no cleanup registered. Narrow, and free to close,
        so it should stay closed.
        """
        import inspect

        lines = [
            ln.split("#", 1)[0]
            for ln in inspect.getsource(Budget.hold).splitlines()
        ]
        try_at = next(i for i, ln in enumerate(lines) if ln.strip() == "try:")
        insert_at = next(
            i for i, ln in enumerate(lines) if "self._holds[token] = charge" in ln
        )
        assert try_at < insert_at, (
            "the reservation is inserted before the `try` that removes it — "
            "a raise in between strands it with nothing registered to clean up"
        )

    def test_reconciling_normalises_before_it_checks_membership(self):
        """`int(weight)` can run user code, and user code can close this scope.

        Checking membership first and converting second lets that conversion
        delete the entry and the assignment then RESURRECT it — the original leak
        through a new door. Converting first makes the check and the write see
        one world.
        """
        b = Budget()
        reservation = b.hold(cap=100, charge=10)
        reconcile = reservation.__enter__()

        class ClosesWhileConverting(int):
            def __int__(self):
                reservation.__exit__(None, None, None)
                return 40

        with pytest.raises(RuntimeError):
            reconcile(ClosesWhileConverting(40))
        assert b.used == 0 and b.live == 0, (
            f"conversion closed the reservation and the write brought it back: "
            f"used={b.used}, live={b.live}"
        )

    def test_saturation_then_full_drain_then_fresh_admission(self):
        """The owner's property, at the primitive level: a fuse that trips must

        RE-OPEN once the load is gone.
        """
        b = Budget()
        held = []
        for _ in range(5):
            cm = b.hold(cap=50, charge=10)
            cm.__enter__()
            held.append(cm)
        assert b.used == 50
        with pytest.raises(NoCapacity):
            with b.hold(cap=50, charge=10):
                pass
        for cm in held:
            cm.__exit__(None, None, None)
        assert b.used == 0
        with b.hold(cap=50, charge=10):
            pass  # admitted again — the fuse re-opened
