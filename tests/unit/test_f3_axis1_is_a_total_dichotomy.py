"""Ф3(a) — axis 1 is a TOTAL dichotomy, and it is unconstructible-otherwise.

The owner's model, in his words: «або унік, або ретьорнінг». Every click is one
or the other — never both, never neither.

Before Ф3, `is_unique` was a stored field and the model was a CONVENTION held by
ten construction sites agreeing with each other. A convention drifts, and this
one already had: a ROAMING click (seen before, different campaign) was stored as
`is_unique=False, is_returning=False` — a nameless third state that every
"new vs repeat" consumer silently dropped. Measured on deployed staging,
2026-08-23, 30 days: 4567 such clicks.

So the fix is not "set the field correctly at ten sites". It is to remove the
field, so that no site can set it at all. These pins hold that shut.
"""
from __future__ import annotations

import dataclasses
import inspect

import pytest

from app import identity
from app.identity import IdentityResult


def test_is_unique_is_derived_not_stored():
    """A stored flag can disagree with the dichotomy; a property cannot."""
    fields = {f.name for f in dataclasses.fields(IdentityResult)}
    assert "is_unique" not in fields, (
        "`is_unique` is a dataclass FIELD again. Ф3 makes it derived precisely "
        "so no construction site can express a click that is neither unique nor "
        "returning - which is the state the owner's model forbids."
    )
    assert isinstance(
        inspect.getattr_static(IdentityResult, "is_unique"), property
    ), "`is_unique` must be a property on IdentityResult"


def test_no_construction_can_express_the_third_state():
    """The exhaustive proof: over every value of the two stored inputs, the
    dichotomy holds. `is_roaming` is deliberately varied too — axis 2 must NOT
    be able to disturb axis 1 (that independence is what Ф3(b) then exploits)."""
    seen = set()
    for is_returning in (False, True):
        for is_roaming in (False, True):
            for seen_before in (False, True):
                r = IdentityResult(
                    uid="u", is_returning=is_returning,
                    is_roaming=is_roaming, seen_before=seen_before,
                )
                assert r.is_unique != r.is_returning, (
                    f"NOT a dichotomy at returning={is_returning} "
                    f"roaming={is_roaming} seen_before={seen_before}"
                )
                seen.add((r.is_unique, r.is_returning))
    assert seen == {(True, False), (False, True)}, (
        f"axis 1 reached states outside the dichotomy: {sorted(seen)}"
    )


def test_is_unique_cannot_be_passed_to_the_constructor():
    """Non-vacuity for the pin above: the field is not merely absent from the
    field list, the constructor genuinely refuses it. A property shadowed by an
    accepted kwarg would pass `test_is_unique_is_derived_not_stored` and still
    let a caller store a contradiction."""
    with pytest.raises(TypeError):
        IdentityResult(uid="u", is_returning=False, is_unique=False)


def test_the_semantics_version_is_bumped_with_the_meaning():
    """A version tag is a LABEL, not a ranking.

    v2 is not "better than" v1 — it is a different definition of `is_unique`.
    Ф3(b) redefines `is_roaming` and must bump again rather than reuse 2: one
    version, one definition, which is the property `resolved.semantics` (W5)
    relies on to tell a caller their window is blended.
    """
    from app.main import _FLAGS_SEMANTICS_VERSION
    assert _FLAGS_SEMANTICS_VERSION == 3, (
        "if you changed what a flag MEANS, bump this; if you bumped it without "
        "changing a meaning, don't"
    )


def test_roaming_is_no_longer_the_negation_of_returning():
    """Ф3(b) LANDED — the pin that stood here said the opposite.

    It read: "is_roaming is still `not is_returning`, so the fourth cell remains
    impossible; when Ф3(b) lands this goes red and is deleted." It went red, and
    this is the replacement. Axis 2 is now read from the visitor's places-seen
    set, so it can never again be the arithmetic negation of axis 1.
    """
    src = inspect.getsource(identity)
    assert "is_roaming = not is_returning" not in src, (
        "axis 2 is the negation of axis 1 again - the fourth cell is impossible"
    )
    assert "def _roaming(" in src, "the ONE definition of axis 2 must exist"


@pytest.mark.parametrize(
    "places_seen, here, exp_roaming, why",
    [
        (frozenset(), "A", False, "first ever: one place"),
        (frozenset({"A"}), "A", False, "again on A: still one place"),
        (frozenset({"A"}), "B", True, "now on B: two places -> unique + roaming"),
        (frozenset({"A", "B"}), "A", True,
         "back to A: two places -> RETURNING + roaming, the owner's 4th cell"),
        (frozenset({"A", "B"}), "C", True, "a third place"),
    ],
)
def test_axis2_is_the_visitors_history_not_this_clicks_place(
    places_seen, here, exp_roaming, why,
):
    assert identity._roaming(places_seen, here) is exp_roaming, why


def test_all_four_cells_are_constructible():
    """The whole point of G4: four cells, not three.

    Before Ф3 two of them could not be built at all. This enumerates them from
    the two independent inputs and asserts the set of reachable (unique,
    returning, roaming) triples is exactly the owner's four.
    """
    cells = set()
    for is_returning in (False, True):
        for is_roaming in (False, True):
            r = IdentityResult(uid="u", is_returning=is_returning,
                               is_roaming=is_roaming, seen_before=True)
            cells.add((r.is_unique, r.is_returning, r.is_roaming))
    assert cells == {
        (True, False, False),    # unique
        (False, True, False),    # returning
        (True, False, True),     # unique + roaming
        (False, True, True),     # returning + roaming
    }, sorted(cells)
    assert (False, False, False) not in cells, "the nameless third state is back"
