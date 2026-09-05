"""`_partition_audience` must not hand an UNKNOWN audience to the live-serving pool.

WHY THIS FILE EXISTS
--------------------
`_partition_audience` splits a campaign's flows into (returning, first). Everything
that is not literally `"returning"` falls into `first` — **the pool that serves LIVE
CLICKS**. That default is deliberate and documented in the function itself: it keeps
legacy rows with a missing audience routing exactly as before ("guaranteeing
zero-regress").

The offerwall programme adds a THIRD value, `offerwall`, for a catalogue of tiles a
visitor picks from. A catalogue must NEVER be evaluated by the ordinary cascade. With
the old two-way split it would be, silently: nothing errors, a visitor is simply routed
by a catalogue.

THE DISTINCTION THIS FILE PINS, WHICH THE OLD CODE MERGED
---------------------------------------------------------
    missing / None / ""      -> `first`  (LEGACY. Must not regress.)
    "returning"              -> returning
    any other NON-EMPTY value-> NEITHER pool

Collapsing those two cases in either direction is a defect:
  - treating unknown as `first` puts a catalogue in front of real traffic
  - treating empty as unknown silently removes every legacy flow from routing

So the tests below are written to fail for BOTH mistakes, not just the new one. A test
that only checked `offerwall` would stay green against an implementation that also
dropped the legacy rows — which would be the more damaging bug of the two.

Isolation ships BEFORE the value can exist: `flows.audience` is still
`CHECK (audience IN ('first','returning'))` at this point, so nothing can author an
`offerwall` row yet. That ordering is the point (ADR-0500).
"""

from __future__ import annotations

import pytest

from app.cascade import _partition_audience


def _flow(fid: int, audience=...):
    """A minimal flow dict. `audience=...` omits the key entirely, which is a
    different input from `None` and from `""` and is tested separately."""
    f = {"_id": fid, "name": f"flow-{fid}"}
    if audience is not ...:
        f["audience"] = audience
    return f


class TestLegacyStaysInTheServingPool:
    """The zero-regress contract. These three must keep routing exactly as before."""

    @pytest.mark.parametrize(
        "audience, label",
        [(..., "key absent"), (None, "explicit None"), ("", "empty string")],
    )
    def test_absent_or_empty_audience_is_a_first_flow(self, audience, label):
        returning, first = _partition_audience([_flow(1, audience)])
        assert [f["_id"] for f in first] == [1], (
            f"{label}: a legacy flow MUST stay in the first pool. Dropping it would "
            f"remove it from live routing, which is worse than the bug this guard "
            f"exists to prevent."
        )
        assert returning == []


class TestReturningIsUnchanged:
    def test_returning_goes_to_the_returning_pool(self):
        returning, first = _partition_audience([_flow(2, "returning")])
        assert [f["_id"] for f in returning] == [2]
        assert first == []


class TestUnknownReachesNeitherPool:
    """The new behaviour. An unknown value must not be evaluated at all."""

    @pytest.mark.parametrize(
        "audience",
        ["offerwall", "future-audience", "FIRST", "Returning", "first ", " returning"],
    )
    def test_unknown_audience_is_excluded_from_both_pools(self, audience):
        returning, first = _partition_audience([_flow(3, audience)])
        assert returning == [], f"{audience!r} must not enter the returning pool"
        assert first == [], (
            f"{audience!r} must not enter the pool that serves LIVE CLICKS. This is "
            f"the whole point of the guard: the failure is silent, and a visitor "
            f"would simply be routed by a catalogue."
        )

    def test_case_and_whitespace_variants_are_unknown_not_normalised(self):
        """Deliberate: we do NOT trim or lowercase. A value that is not exactly one
        of the known strings is unknown, because silently normalising would let a
        typo become live routing config."""
        returning, first = _partition_audience([_flow(4, "Returning")])
        assert returning == [] and first == []


class TestMixedInputPartitionsIndependently:
    """The discriminating case: legacy, returning and unknown in ONE list. An
    implementation that gets any single rule wrong fails here even if its
    single-flow behaviour looks right."""

    def test_one_list_of_every_kind(self):
        flows = [
            _flow(10),                 # legacy, key absent   -> first
            _flow(11, None),           # legacy, None         -> first
            _flow(12, ""),             # legacy, empty        -> first
            _flow(13, "first"),        # explicit first       -> first
            _flow(14, "returning"),    # returning            -> returning
            _flow(15, "offerwall"),    # the new value        -> NEITHER
            _flow(16, "something-new"),# a future value       -> NEITHER
        ]
        returning, first = _partition_audience(flows)

        assert [f["_id"] for f in returning] == [14]
        assert [f["_id"] for f in first] == [10, 11, 12, 13]

        # Explicit: the excluded ones are in NEITHER list. Asserting the two lists
        # above is not quite the same claim — this says the rows vanished from
        # routing rather than moved between pools.
        seen = {f["_id"] for f in returning} | {f["_id"] for f in first}
        assert 15 not in seen and 16 not in seen

    def test_an_excluded_flow_is_visible_in_the_trace_not_silently_gone(self):
        """G2.5 — the exclusion must be OBSERVABLE, or it is just a silent drop
        with better intentions than the one it replaced.

        It reuses the sink every other drop reason already writes to, so an
        operator reads one list rather than learning about a new counter.
        """
        sink: list[dict] = []
        _partition_audience([_flow(40, "offerwall")], rejected_sink=sink)
        assert len(sink) == 1
        assert sink[0]["flow_id"] == 40
        assert "offerwall" in sink[0]["failed"]

    def test_ordinary_and_legacy_flows_never_enter_the_sink(self):
        """A sink that also collected the flows that DID route would tell an
        operator nothing — every flow would be in it."""
        sink: list[dict] = []
        _partition_audience(
            [_flow(41), _flow(42, "first"), _flow(43, "returning"), _flow(44, "")],
            rejected_sink=sink,
        )
        assert sink == []

    def test_the_no_trace_path_is_unchanged(self):
        """The sink is None on a live click unless a trace was threaded. The
        partition must be identical with and without it — otherwise the
        instrumentation changed the routing it was meant to observe."""
        flows = [_flow(45, "first"), _flow(46, "offerwall"), _flow(47, "returning")]
        without = _partition_audience(flows)
        with_sink = _partition_audience(flows, rejected_sink=[])
        assert [f["_id"] for f in without[0]] == [f["_id"] for f in with_sink[0]]
        assert [f["_id"] for f in without[1]] == [f["_id"] for f in with_sink[1]]

    def test_a_hostile_audience_value_cannot_bloat_the_trace(self):
        """The value is read from Redis, so nothing in this process bounds its
        length, and the trace has a defensive size limit downstream. One row
        must not be able to crowd out every other rejection reason."""
        sink: list[dict] = []
        _partition_audience([_flow(48, "x" * 5000)], rejected_sink=sink)
        assert len(sink[0]["failed"]) < 120, (
            f"the reason string grew with the input: {len(sink[0]['failed'])} chars"
        )

    def test_the_partition_no_longer_conserves_the_input(self):
        """A property worth pinning because downstream code might assume it:
        `returning + first` USED to reconstruct the whole input list. It no longer
        does, by design. Anything relying on that reconstruction must be found and
        fixed, not discovered in production."""
        flows = [_flow(20, "first"), _flow(21, "offerwall")]
        returning, first = _partition_audience(flows)
        assert len(returning) + len(first) == 1
        assert len(flows) == 2
