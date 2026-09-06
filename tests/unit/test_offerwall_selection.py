"""G4 gate — wall selection is DETERMINISTIC, pinned with more than one match.

The anchor's gate is exact: *"selection is deterministic and pinned by a test
with more than one matching wall."* **More than one is the whole point** — a
single-wall test passes in both worlds and discriminates nothing.

The bar comes from `34-G4-WALL-SELECTION.md` §6, which also warns that the
design's FIRST draft would have encoded its own errors into its test list. So
each test below names what it discriminates, and the two that the SoT leaves
open are declared open here rather than quietly asserted.
"""

from __future__ import annotations

import pytest

from app.cascade import _partition_audience
from app.offerwall import select_wall


def _wall(
    *, seq: int, scope_type: str = "company", scope_id: int = 1,
    campaign_id: str = "0", is_default: str = "0", name: str | None = None,
) -> dict:
    """A wall in the LOADER's representation — Redis-shaped, i.e. strings.

    `is_default` is `"1"`/`"0"` and `campaign_id` is `"0"` for global. Native
    types silently invert the comparisons in `_winner_sort_key`; §2.4 calls this
    a correctness matter rather than plumbing, and `test_native_types_*` below
    is what makes that concrete instead of a warning nobody acts on.
    """
    return {
        "flow_id": f"w{seq}",
        "name": name or f"wall-{seq}",
        "seq_id": str(seq),
        "scope_type": scope_type,
        "scope_id": str(scope_id),
        "campaign_id": campaign_id,
        "is_default": is_default,
        "audience": "offerwall",
        "action_type": "offerwall",
    }


# A click that belongs at every level, so scope specificity is exercisable.
_LEVELS = {"buyer": 7, "custom_group": None, "team": 5, "department": 3, "company": 1}


def test_campaign_bound_beats_a_MORE_SPECIFIC_global_wall():
    """🔴 §2.1 — the inversion a mixed list would cause, and the reason the
    order in `select_wall` is load-bearing rather than cosmetic.

    The global wall here is at `buyer`, the MOST specific scope. The bound wall
    is at `company`, the LEAST specific. Bound must still win, because binding is
    the outer axis and specificity the inner one.

    Hand both to `_pick_winner` in one list and it walks scope first, finds the
    buyer-scoped global wall, and returns it — the tie-break's bound term never
    gets a say, because the two are never in the same bucket. That is exactly
    the failure this test exists for, and it is invisible to any single-wall test.
    """
    bound = _wall(seq=9, scope_type="company", scope_id=1, campaign_id="42")
    globl = _wall(seq=1, scope_type="buyer", scope_id=7, campaign_id="0")
    assert select_wall([globl, bound], _LEVELS)["flow_id"] == "w9"
    # order of the input list must not matter
    assert select_wall([bound, globl], _LEVELS)["flow_id"] == "w9"


def test_within_one_binding_the_more_specific_scope_wins():
    """The inner axis, isolated — both walls global, so binding cannot decide."""
    broad = _wall(seq=1, scope_type="company", scope_id=1)
    narrow = _wall(seq=2, scope_type="team", scope_id=5)
    assert select_wall([broad, narrow], _LEVELS)["flow_id"] == "w2"


def test_at_the_same_scope_the_lower_seq_id_wins():
    """The tie-break, isolated — same binding, same scope."""
    older = _wall(seq=3, scope_type="team", scope_id=5)
    newer = _wall(seq=11, scope_type="team", scope_id=5)
    assert select_wall([newer, older], _LEVELS)["flow_id"] == "w3"


def test_a_wall_carrying_the_default_flag_sorts_LAST_not_first():
    """§2.3 was OPEN when the SoT was written; A32 closed it — a wall may not be
    `is_default` at all (`flows/validation.py` refuses it at create).

    So this asserts the SAFE DIRECTION rather than a policy: if a wall somehow
    carried the flag, it must lose to an explicit one, not win. That keeps the
    reused `_winner_sort_key` honest without special-casing walls out of it.
    """
    defaulted = _wall(seq=1, scope_type="team", scope_id=5, is_default="1")
    explicit = _wall(seq=8, scope_type="team", scope_id=5, is_default="0")
    assert select_wall([defaulted, explicit], _LEVELS)["flow_id"] == "w8"


def test_no_matching_wall_returns_None():
    """§4 — and the caller must NOT read this as 'fall back to routing'."""
    elsewhere = _wall(seq=1, scope_type="buyer", scope_id=999)
    assert select_wall([elsewhere], {"buyer": 7, "company": None}) is None
    assert select_wall([], _LEVELS) is None


def test_the_pools_are_disjoint_in_BOTH_directions():
    """The isolation, checked both ways rather than one.

    A wall must not appear in a routing pool, AND an ordinary flow must not
    appear among wall candidates. Testing only the first would miss a partition
    that swept everything into the wall side.
    """
    wall = _wall(seq=1)
    ordinary = {**_wall(seq=2), "audience": "first", "action_type": "redirect"}
    returning, first = _partition_audience([wall, ordinary], rejected_sink=[])
    routed = [f["flow_id"] for f in returning + first]
    assert "w1" not in routed, "a wall reached a routing pool"
    assert "w2" in routed, "an ordinary flow was excluded from routing"


def test_native_types_silently_invert_the_flags_which_is_why_the_loader_matters():
    """§2.4 as a demonstration, not a warning.

    `is_default` is a STRING flag. Given native `True`, `_winner_sort_key`
    evaluates `True == "1"` -> False, so a default sorts as NON-default and can
    WIN. Nothing raises; the wrong wall is simply shown.

    This is why the loader's representation is a correctness matter. The test
    asserts the broken behaviour deliberately, so that a future loader emitting
    native types fails HERE with an explanation rather than in production with a
    wrong catalogue.
    """
    native_default = {**_wall(seq=1, scope_type="team", scope_id=5), "is_default": True}
    explicit = _wall(seq=8, scope_type="team", scope_id=5)
    winner = select_wall([native_default, explicit], _LEVELS)
    assert winner["flow_id"] == "w1", (
        "a native-typed is_default no longer inverts — if the loader was fixed "
        "to normalise types, update this test and §2.4; if it was not, this "
        "means the sort key changed and the string contract is now unpinned"
    )


@pytest.mark.xfail(
    reason="§3.2 Q-EMPTY-WALL is an OPEN owner decision (P-serve: skip an empty "
           "wall and serve a populated broader one). Selection cannot implement "
           "it — it sees no tiles, and the SoT is explicit that a test must not "
           "silently assert a disputed policy. Marked xfail so the gap is "
           "VISIBLE in the suite rather than absent from it.",
    strict=True,
)
def test_an_empty_specific_wall_yields_to_a_populated_broader_one():
    narrow_empty = {**_wall(seq=1, scope_type="team", scope_id=5), "tiles": []}
    broad_full = {**_wall(seq=2, scope_type="company", scope_id=1), "tiles": [{"o": 1}]}
    assert select_wall([narrow_empty, broad_full], _LEVELS)["flow_id"] == "w2"
