"""Returning-user previous-visit history capture (P3, 2026-06-05). DARK.

After the routing winner is finalized, accumulate the click's OUTCOME into the
uid's company-scoped, capped history sets so a P4 returning-flow can match
"this user PREVIOUSLY hit offer X / target Y / arrived with sub Z". This is the
WRITE half only — nothing reads these sets in routing yet (that is P4).

Design : docs/development/returning-users-implementation-plan-2026-06-04.md §4,§P3
Audit  : returning-users-regression-safety-2026-06-04.md (G9 — the captured
         target is the STOCHASTIC split pick; we record honestly what was
         chosen, the operator-facing caveat is a P4/UI concern).

Membership = "any previous visit" (R3 §4): a SADD per dimension, idempotent,
hard-capped at `_CAP` distinct values to bound cardinality (a runaway sub
sprayer cannot grow one uid's profile without limit). Off the redirect-critical
path — fire-and-forget, error-swallowing (gate #8, mirroring
``identity.persist_identity``). Company-scoped keys
(gate #7). Implicitly gated by a non-empty uid: only the P2 resolver, when ON
for the tenant, ever sets one, so this is a zero-I/O no-op when the resolver is
OFF.
"""

from __future__ import annotations

import asyncio
import logging

from app.config import settings
from app.redis_client import get_identity_redis

logger = logging.getLogger(__name__)

# Per-dimension distinct-value cap. Realistic users touch a handful of
# offers/targets/subs; 20 is generous headroom while hard-bounding memory.
_CAP = 20


def _pos_int(value) -> int:
    """Coerce a routing id to a positive int (0 / None / junk → 0 = 'no hit')."""
    try:
        n = int(value)
    except (TypeError, ValueError):
        return 0
    return n if n > 0 else 0


def _offers_key(company_id: int, uid: str) -> str:
    return f"id:{company_id}:uid:{uid}:offers"


def _targets_key(company_id: int, uid: str) -> str:
    return f"id:{company_id}:uid:{uid}:targets"


def _subs_key(company_id: int, uid: str) -> str:
    return f"id:{company_id}:uid:{uid}:subs"


def schedule_capture(click_record: dict) -> None:
    """Fire-and-forget entrypoint from the /decide success path.

    No-op — zero I/O, zero task spawned — when ``uid`` is empty (resolver OFF or
    no identity resolved). Otherwise schedules the capture off the response
    path. Never raises.
    """
    if not click_record.get("uid"):
        return
    try:
        asyncio.create_task(capture_from_record(click_record))
    except RuntimeError:  # pragma: no cover — no running loop (non-async caller)
        pass


async def capture_from_record(click_record: dict) -> None:
    """Extract the finalized outcome from a click record and accumulate it into
    the uid's capped history sets. Fully error-swallowing — a failed history
    write must NEVER affect the click."""
    try:
        uid = click_record.get("uid") or ""
        company_id = _pos_int(click_record.get("company_id"))
        if not uid or not company_id:
            return

        offer_id = _pos_int(click_record.get("offer_id"))
        target_id = _pos_int(click_record.get("offer_target_id"))
        sub_values = [
            v for i in range(1, 21)
            if (v := click_record.get(f"sub{i}"))
        ]

        members: list[tuple[str, list[str]]] = [
            (_offers_key(company_id, uid), [str(offer_id)] if offer_id else []),
            (_targets_key(company_id, uid), [str(target_id)] if target_id else []),
            (_subs_key(company_id, uid), [str(v) for v in sub_values]),
        ]
        members = [(k, vals) for k, vals in members if vals]
        if not members:
            return  # a fallback/no-offer click carries no outcome to record

        r = await get_identity_redis()
        await _accumulate_capped(r, members, settings.returning_uid_ttl_seconds)
    except Exception as e:  # pragma: no cover — best-effort, never fail a click
        logger.warning("history capture failed (swallowed): %s", e)


async def _accumulate_capped(
    r, members: list[tuple[str, list[str]]], ttl: int,
) -> None:
    """SADD each dimension's members up to ``_CAP`` distinct values, then slide
    the key TTL. Hard-bounded: once a set holds ``_CAP`` values, new members are
    DROPPED (first-seen kept) so the set size can never exceed the cap. SADD is
    idempotent, so re-capturing an already-seen value is a no-op.
    """
    # RT#1 — current sizes (off critical path, so the extra read is free).
    size_pipe = r.pipeline()
    for key, _vals in members:
        size_pipe.scard(key)
    sizes = await size_pipe.execute()

    # RT#2 — add only what fits under the cap.
    write_pipe = r.pipeline()
    wrote = False
    for (key, vals), size in zip(members, sizes):
        room = _CAP - int(size or 0)
        if room <= 0:
            continue
        # Conservative bound: at most `room` new members. An already-present
        # value won't grow the set, so this can only UNDER-fill, never exceed.
        fitted = vals[:room]
        if fitted:
            write_pipe.sadd(key, *fitted)
            write_pipe.expire(key, ttl)
            wrote = True
    if wrote:
        await write_pipe.execute()
