"""Offer-wall SELECTION — which wall a visitor is shown, when several match.

🔴 NOTHING CALLS THIS YET, ON PURPOSE. It is a pure function with no wiring, so
landing it changes no behaviour on any live path. G5 (the read endpoint, behind
its own flag defaulting OFF) is what will call it. Shipping the decision logic
separately from the surface that invokes it is what lets this be reviewed as
routing logic rather than as one more thing inside an endpoint diff.

SoT: `docs/development/offerwall-2026-09-04/34-G4-WALL-SELECTION.md` (selection)
and `35-G4-WALL-CONTRACT.md` (the wall's own shape). This module implements §0's
ratified answer and §2's composition contract; it does not re-decide either.

WHAT A WALL IS, SO THE REUSE BELOW IS NOT MISREAD
--------------------------------------------------
A wall is a CATALOGUE of tiles a visitor picks from — never a routing decision.
It is excluded from routing in two planes (`cascade._partition_audience`,
`sync/builders/flows.py`). This module decides WHICH CATALOGUE to show; it never
decides where traffic goes.

WHY IT REUSES THE CASCADE'S MACHINERY — AND WHY THAT NEEDED ARGUING
--------------------------------------------------------------------
§0: *reuse is an IMPLEMENTATION choice argued property by property, never a
semantic conclusion drawn from the fact that a wall is stored in the same table.*
The first draft of the design drew exactly that conclusion and was refused —
`entity-boundaries` has a law about it: **a shape match is evidence about
STORAGE, never about MEANING.**

What is reused, and the property that justifies each:

| Reused | Property that makes it correct here |
|---|---|
| `_split_by_binding` | a wall is written into EITHER a campaign list or a scope list, never both — the same write-path invariant the routing builder enforces |
| `_pick_winner` | scope specificity, then `seq_id`, are the same *"which of these did the operator mean"* question for a catalogue as for a route |
| bound-before-global | a campaign-bound wall is a narrower statement of intent than a global one, exactly as for routing |

🔴 **THE ORDER IS LOAD-BEARING, NOT COSMETIC** (§2.1). `_pick_winner` must NEVER
see a mixed list. Its own tie-break contains a bound-beats-global term, so a
mixed list would let a GLOBAL wall at a more specific scope beat a
CAMPAIGN-BOUND wall at a broader one — an inversion of the intended precedence,
silently. That is why this splits first and calls the picker twice, mirroring
`cascade.resolve_flow`'s `_pick` closure exactly rather than approximately.
"""

from __future__ import annotations

from typing import Any

from app.cascade import _pick_winner, _split_by_binding

__all__ = ["select_wall"]


def select_wall(
    candidates: list[dict[str, Any]],
    click_levels: dict[str, int | None],
) -> dict[str, Any] | None:
    """Pick the one wall to show, or `None` when no wall matches.

    `candidates` MUST already be the eligible set — walls whose criteria match
    this click, in the loader's Redis-shaped representation. Membership is the
    LOADER's job, not this function's (§2.2): filtering here would re-run the
    criteria pass and double-count the trace accounting that `_eligible`'s
    closure accumulates, which is the same guardrail `_split_by_binding`'s own
    docstring states for routing.

    ⚠️ REDIS-SHAPED means strings, and the flags are STRING flags. `is_default`
    is `"1"`, not `True`; `campaign_id` is `"0"` for global, not `None`. Handing
    this native Python types silently inverts the comparisons in
    `_winner_sort_key` — a wall with `is_default=True` would read as NOT default,
    because `True == "1"` is False. §2.4 calls the loader's representation a
    correctness matter rather than plumbing, and this is why.

    Returns the winning wall dict, or `None`.

    🔴 `None` MEANS "SHOW NO WALL" — IT DOES NOT MEAN "FALL BACK TO ROUTING"
    (§4). A wall and an ordinary flow are different products answering different
    questions; a visitor who asked for a catalogue and matched none must not be
    silently redirected into traffic routing. The caller decides what to render;
    it must not treat `None` as permission to consult the routing cascade.

    On `is_default`: a wall can never carry it — A32 forbids it at write time
    (`flows/validation.py`), because the default slot is an EXCLUSIVE claim over
    a (company, campaign, scope) bucket and a wall taking it would demote the
    ordinary default, changing routing with no wall in either candidate set. So
    `_winner_sort_key`'s default term is permanently 0 here. Left in the reused
    function rather than special-cased out: a wall that somehow carried the flag
    would sort LAST, which is the safe direction.
    """
    campaign_bound, global_ = _split_by_binding(candidates)
    return (
        _pick_winner(campaign_bound, click_levels)
        or _pick_winner(global_, click_levels)
    )
