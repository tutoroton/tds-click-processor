"""Admission budgets for the SECONDARY products, as owned reservations.

Click serving is priority #1 on this node. Route preview and the offer wall are
priority #2, and they share the worker process with it, so each carries an
admission budget whose only job is to stop a flood of secondary work from
starving click serving. This module is that budget.

WHY A RESERVATION MAP RATHER THAN A COUNTER
-------------------------------------------
The previous wall bulkhead kept a single mutable integer and mutated it from
three places: `+= charge` on admission, `-= charge` in a `finally`, and — inside
the request body — `+= max(0, actual - charge)` to reconcile a presumptive
charge against the real cost. Two sites TOOK budget and one gave it back, so
every request whose real cost exceeded the presumptive charge retained the
difference permanently.

Measured before the fix: with a 31-tile candidate set, three sequential and
fully successful requests left 7, 14 and 21 tiles held with nothing in flight;
with 61 tiles, thirteen requests reached 481 against a 480 cap and every
subsequent wall request answered 503 for the life of the process. Every one of
those responses was a 200. A bulkhead can give its entire budget away while each
individual response looks perfect.

The defect was not a race and not a rare exit path. It was ownership: the
quantity released was a REMEMBERED NUMBER rather than the reservation itself. So
this module removes the remembered number. There is no total to maintain:

  * admission inserts ONE entry;
  * reconciliation REPLACES that entry's weight;
  * cleanup DELETES the entry, whatever it happens to weigh by then;
  * `used` is derived by summing the live entries, never accumulated.

An unmatched increment is then not "a bug that got fixed" but a shape with
nowhere to live: there is no `+=` to forget to match.

WHAT THIS DOES NOT PROMISE
--------------------------
It closes leakage through the supported interface. It cannot stop code that
bypasses `hold()`, replaces this primitive, or lets work escape the reservation's
lifetime — so "impossible by construction" is deliberately not claimed here.
It also does not turn the cap into a hard instantaneous work ceiling: admission
still charges a presumptive cost before the real one is known, so N requests can
each be admitted at `charge` and only afterwards discover they are larger.
Bounding the work itself is a separate concern from accounting for it honestly.

CONCURRENCY
-----------
Per worker PROCESS, like every counter in this service — there are
`WEB_CONCURRENCY` of them per node and they share nothing. Inside one process the
event loop is single-threaded and there is no `await` between the capacity test
and the insert, so the pair is atomic without a lock. Keep it that way: adding an
await between them reintroduces a race this design does not defend against.
"""
from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager


class NoCapacity(Exception):
    """Raised by `Budget.hold` when admitting this request would exceed the cap.

    Deliberately a plain exception rather than an HTTP error: this module knows
    about budgets, not about status codes. The caller decides what a refusal
    looks like on the wire — and both current callers must keep answering 503
    rather than falling through to a click, which is the whole reason the
    bulkhead exists.
    """


class Budget:
    """A pool of `cap` units held by live reservations.

    One instance per PRODUCT, never shared. Two products drawing on one budget
    would let either exhaust the protection the other depends on, and would make
    tuning one silently retune the other.
    """

    __slots__ = ("_holds",)

    def __init__(self) -> None:
        self._holds: dict[object, int] = {}

    @property
    def used(self) -> int:
        """Units currently held. DERIVED, never accumulated.

        Summing is O(live reservations), which at the shipped wall defaults is at
        most 20 entries — a secondary-path cost, and not on the click path at
        all. Deriving is the point: a maintained total is a second
        representation of the truth, and two representations can disagree.
        """
        return sum(self._holds.values())

    @property
    def live(self) -> int:
        """How many reservations are open. For diagnostics, not for decisions."""
        return len(self._holds)

    @contextmanager
    def hold(self, *, cap: int, charge: int) -> Iterator[Callable[[int], None]]:
        """Reserve `charge` units, or raise `NoCapacity`.

        Yields a `reconcile(weight)` callable that REPLACES this reservation's
        weight once the real cost is known. Replacing rather than adding is what
        makes repeated reconciliation harmless.

        The reconciled weight never drops below `charge`: the admission floor is
        preserved, so a cheaper-than-expected request does not buy extra
        admission capacity for its neighbours. This matches the behaviour the
        previous code had by accident of its `max(0, actual - charge)` term, and
        keeping it means the CAP semantics are unchanged by this refactor — only
        the release is fixed.
        """
        if cap < 1 or charge < 1:
            raise ValueError("cap and charge must both be positive")
        # No await between this test and the insert below — see module docstring.
        if self.used + charge > cap:
            raise NoCapacity

        token = object()
        self._holds[token] = charge

        def reconcile(weight: int) -> None:
            # A closed reservation cannot be resurrected: reconciling after the
            # scope has exited would re-insert an entry nobody will ever remove,
            # which is precisely the leak this module exists to make unavailable.
            if token not in self._holds:
                raise RuntimeError("this reservation is already closed")
            self._holds[token] = max(charge, int(weight))

        try:
            yield reconcile
        finally:
            # Deletes the RESERVATION, not a remembered amount. Runs on every
            # exit: return, exception, and task cancellation alike.
            self._holds.pop(token, None)

    def _reset_for_tests(self) -> None:
        """Drop every reservation. Tests only — never call this in a request."""
        self._holds.clear()
