"""Offer-wall SELECTION — which wall a visitor is shown, when several match.

⚠️ THIS MODULE IS NOW CALLED — corrected 2026-09-12. `main.py:68` imports
`load_wall_candidates` and `select_wall`, and the delivery path reaches them.

It said "🔴 NOTHING CALLS THIS YET, ON PURPOSE… G5 is what will call it" until
that date, which was true when the selector landed ahead of its surface. Left
here as history rather than deleted, because the REASON is still the right one:
shipping decision logic separately from the surface that invokes it is what let
this be reviewed as routing logic instead of as one more thing inside an
endpoint diff. But a stale "nothing calls this" is the most expensive kind of
comment — it tells a reader that editing the file is free.

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

import logging
from typing import Any

from app.cascade import (
    _filter_by_criteria,
    _load_flow_records,
    _pick_winner,
    _split_by_binding,
)

logger = logging.getLogger("tds.offerwall")

__all__ = ["load_wall_candidates", "select_wall", "MAX_WALL_TILES_PER_REQUEST"]


# The wall plane's OWN work ceiling, and it is counted in TILES, not in walls.
#
# 🔴 RISK A19, IN THE CONTRACT'S OWN WORDS: "the work unit is buckets x
# candidates x tiles, not the winner's tiles". A cap on the number of WALLS
# would be a cap on the wrong quantity — six buckets of twenty walls of
# twenty-four tiles is 2 880 tiles under a "20 candidates" ceiling that sounds
# tight. What a preview actually costs is proportional to the tiles it has to
# load and render, so that is what is bounded.
#
# DELIBERATELY SEPARATE from `_MAX_FLOWS_PER_CLICK` (50). Sharing routing's
# ceiling would mean a wall-capacity decision silently changes routing capacity,
# and vice versa — the same welding mistake `app/common/audiences.py` documents
# for the audience sets, one plane down.
#
# The number: `35-G4-WALL-CONTRACT.md` puts the per-wall tile ceiling at 24, so
# this admits ~20 fully-loaded walls before it bites. It is a BULKHEAD, not a
# product limit: an operator who legitimately has more walls in scope loses the
# oldest ones from the candidate set, exactly as routing does, and the truncation
# is reported rather than silent.
MAX_WALL_TILES_PER_REQUEST: int = 480

# How many tiles a wall is ASSUMED to carry when its own config cannot be read.
# Fail-EXPENSIVE on purpose: an unparseable wall counts against the budget as if
# it were full, so a corrupt row cannot buy itself unlimited admission by being
# unreadable. The opposite default is the one that turns a parse failure into a
# capacity hole.
_ASSUMED_TILES_ON_UNREADABLE = 24


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


def parse_tiles(wall: dict[str, Any]) -> list[dict[str, Any]] | None:
    """This wall's tiles, or `None` when the config cannot be read.

    🔴 `None` AND `[]` ARE DIFFERENT ANSWERS and the callers need both. `[]` is
    "this wall has no tiles", a readable fact; `None` is "we do not know what
    this wall carries". Collapsing them would let a corrupt row present as an
    empty one, and the two callers below want OPPOSITE defaults on it — the
    budget treats unknown as expensive, the membership check treats unknown as
    unproven. A single fused return could not serve both.

    Extracted at the SECOND use (`reusability-discipline`): `_tile_count`
    parsed this and threw the list away, and `wall_contains_tile` needs the
    list itself.
    """
    import json

    raw = wall.get("action_config")
    if not raw:
        return None
    try:
        cfg = json.loads(raw) if isinstance(raw, str) else raw
        tiles = cfg.get("tiles")
    except (ValueError, AttributeError, TypeError):
        return None
    if not isinstance(tiles, list):
        return None
    return [t for t in tiles if isinstance(t, dict)]


def wall_contains_tile(
    wall: dict[str, Any], offer_id: int, offer_target_id: int,
) -> bool:
    """Does this wall carry exactly this (offer, target) pair RIGHT NOW?

    The question a tile's route code has to answer before it may act as one.
    G6.3, and it implements owner decision D3-OPEN-3 verbatim: an offer REMOVED
    from a wall while a visitor still holds its link *"falls back to the DEFAULT
    scenario, not an error"* — so a stale tile is simply not honoured and the
    click routes normally.

    🔴 FAIL-CLOSED, unlike its sibling `_tile_count`, and the asymmetry is the
    point. An unreadable wall means the claim is UNPROVEN, and an unproven claim
    must not buy precedence over ordinary routing. `_tile_count` fails the other
    way for the same reason inverted: there, treating unknown as cheap would let
    a corrupt row buy unlimited admission.

    Both ids are compared as INTs. ⚠️ **And that choice is NOT load-bearing, a
    correction to this docstring's own first draft.** It claimed a string
    compare "would refuse a legitimate tile" — a mutation swapping `int()` for
    `str()` was predicted RED and came back GREEN, because for every shape that
    actually occurs the two agree: `int("21") == int(21)` and
    `str("21") == str(21)` are both True. They diverge only on inputs no JSON
    writer emits (`"021"`, `21.0`).

    So the int compare is kept for SEMANTICS — these are numeric ids and should
    be compared as numbers — and this paragraph records that no test
    discriminates it from the alternative, rather than pretending one does. The
    calibration is what found it; a justification that survives review by
    sounding load-bearing is worse than none.
    """
    tiles = parse_tiles(wall)
    if tiles is None:
        return False
    for tile in tiles:
        try:
            if (int(tile.get("offer_id")) == int(offer_id)
                    and int(tile.get("target_id")) == int(offer_target_id)):
                return True
        except (TypeError, ValueError):
            # A malformed ENTRY is skipped, never fatal: one bad tile must not
            # invalidate a wall whose other tiles are fine.
            continue
    return False


def _tile_count(wall: dict[str, Any]) -> int:
    """How many tiles this wall carries, for the WORK budget.

    Unreadable config counts as a full wall (`_ASSUMED_TILES_ON_UNREADABLE`)
    rather than as zero: a corrupt row must not be able to buy unlimited
    admission by being corrupt.
    """
    tiles = parse_tiles(wall)
    return _ASSUMED_TILES_ON_UNREADABLE if tiles is None else len(tiles)


async def load_wall_candidates(
    r,
    *,
    campaign_id: str,
    company_id: int | None,
    buyer_id: int | None,
    team_id: int | None,
    department_id: int | None,
    custom_group_id: int | None,
    click_attrs: dict[str, str],
    max_tiles: int = MAX_WALL_TILES_PER_REQUEST,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Read the ELIGIBLE walls for this request from the node's own Redis.

    The missing link. `sync/builders/flows.py` publishes walls into
    `campaign:{id}:walls` and `walls:scope:{company}:{type}:{id}`; `select_wall`
    picks among candidates someone else fetched. Until now NOTHING on this node
    read those keys, so the two halves could not meet.

    Returns `(candidates, stats)`. `candidates` is what `select_wall` expects:
    walls whose criteria match this click, in the loader's Redis-shaped
    representation, with `_id` stamped on. `stats` carries what a caller needs to
    be honest about truncation.

    🔴 IT READS THE WALL KEYSPACE AND ONLY THE WALL KEYSPACE. Not one LRANGE
    touches `campaign:{id}:flows` or `flows:scope:…`. That is not tidiness: the
    two keyspaces exist because routing's candidate read is TAIL-CAPPED BEFORE
    the audience partition (`cascade.py:778`, `:823`), so anything sharing those
    lists can evict a routing flow that no later filtering recovers
    (`35-G4-WALL-CONTRACT.md` §4.1). A loader that "helpfully" also read the
    routing lists would re-open that from the read side.

    THE BUDGET IS TILES, NOT WALLS (risk A19). A wall costs what its tiles cost,
    so `max_tiles` bounds the work rather than the row count. Walls are admitted
    newest-first — the same direction routing trims, and for the same reason: a
    freshly-authored override is the one an operator is watching.

    WHAT IT DOES NOT DO, so nothing is inherited as covered:
      * it does not decide WHICH wall wins — that is `select_wall`, and keeping
        membership out of it is §2.2's contract;
      * it does not check tenancy. The caller resolved this campaign for this
        request and passes the hierarchy ids; a foreign tenant's wall is
        unreachable because its ids are never asked for, not because this
        function rejects them. G4.3 tests that at the endpoint, where a foreign
        tenant can actually be asked for.
    """
    fetch_log: list[str] = []

    def _build_pipe():
        fetch_log.clear()
        pipe = r.pipeline()
        # cap+1, the same probe trick the cascade uses: a list AT the cap and a
        # list OVER it are otherwise indistinguishable, and "we may have dropped
        # something" is exactly what a caller must be told.
        pipe.lrange(f"campaign:{campaign_id}:walls", -(max_tiles + 1), -1)
        fetch_log.append(f"campaign:{campaign_id}")
        if company_id is not None:
            for scope_type, scope_id in (
                ("buyer", buyer_id),
                ("custom_group", custom_group_id),
                ("team", team_id),
                ("department", department_id),
                ("company", company_id),
            ):
                if scope_id is not None:
                    pipe.lrange(
                        f"walls:scope:{company_id}:{scope_type}:{scope_id}",
                        -(max_tiles + 1), -1,
                    )
                    fetch_log.append(f"scope:{scope_type}:{scope_id}")
        return pipe

    results = await _build_pipe().execute()

    wall_ids: list[str] = []
    seen: set[str] = set()
    for raw in results:
        for wid in raw or []:
            wid = wid.decode() if isinstance(wid, bytes) else str(wid)
            # A wall is in EITHER a campaign list or a scope list, never both
            # (the publisher's invariant) — so a repeat here is sync drift, not
            # a legitimate second binding. Dedupe rather than double-charge it
            # against the budget.
            if wid not in seen:
                seen.add(wid)
                wall_ids.append(wid)

    if not wall_ids:
        return [], {"buckets": len(fetch_log), "loaded": 0, "eligible": 0,
                    "tiles": 0, "truncated": False}

    records = await _load_flow_records(r, wall_ids)

    # THEN the budget, applied to what was actually loaded. Counting before the
    # HGETALL would bound a guess; counting after bounds the real thing.
    admitted: list[dict[str, Any]] = []
    tiles = 0
    truncated = False
    for wall in reversed(records):        # newest first
        cost = _tile_count(wall)
        if tiles + cost > max_tiles and admitted:
            truncated = True
            break
        tiles += cost
        admitted.append(wall)
    admitted.reverse()                    # restore publication order

    if truncated:
        # Visible, never silent. A capacity decision the operator cannot see is
        # a capacity decision that will be mistaken for a missing wall.
        logger.warning(
            "offerwall: candidate set truncated at %d tiles (%d of %d walls "
            "admitted, buckets=%s) — an operator will see fewer walls than they "
            "authored",
            max_tiles, len(admitted), len(records), fetch_log,
        )

    eligible = _filter_by_criteria(admitted, click_attrs)
    return eligible, {
        "buckets": len(fetch_log),
        "loaded": len(records),
        "eligible": len(eligible),
        "tiles": tiles,
        "truncated": truncated,
    }
