"""Sticky binding (v2 Phase S) — (uid, campaign) → offer_target pin.

"Куди потрапив першого разу, туди й далі": once a returning-user routes to an
offer_target under a `sticky`-mode campaign, the SAME target serves every later
visit (until the target closes/vanishes → re-pin). DARK: only the router's
sticky path calls this, and only when the effective returning_mode is 'sticky'
AND returning routing is live.

Storage: the pin lives in the IDENTITY store (the dedicated / noeviction Redis,
`get_identity_redis`) alongside the uid keyspace — an evicted pin silently
degrades to "re-pin on next visit", so noeviction matters (boot-gated by
`identity.assert_identity_namespace_safe`). 180d SLIDING TTL (refreshed on read
AND write) so the pin tracks the ACTIVE returning audience.

Atomicity (no Lua — the click-processor sticky path is fakeredis-testable; the
collector Lua-EVAL gap is a different service):
  * `set_sticky_nx` = `SET NX EX` — FIRST-WRITE-WINS. Two concurrent first
    clicks for one uid converge on ONE pinned target (the NX winner).
  * `repin` = `SET EX` (overwrite) — the ONLY explicit overwrite, used solely
    for the invalid/closed re-pin case (NX would refuse to replace a stale pin,
    so re-pin must overwrite deterministically: the new selection wins).

Fail-OPEN (lose no click): EVERY operation swallows Redis errors. A failed pin
read ⇒ treat as "no pin" (normal selection); a failed write ⇒ the click still
routes + is still XADD'd. Identity/sticky is enrichment, never a gate.
"""

from __future__ import annotations

import logging

from app.redis_client import get_identity_redis

logger = logging.getLogger("tds.sticky")

__all__ = ["sticky_key", "get_sticky", "set_sticky_nx", "repin"]


def sticky_key(company_id, uid: str, campaign_id) -> str:
    """Company-scoped pin key — the company_id FIRST segment is the hard
    multi-tenant boundary (a pin NEVER crosses tenants), the uid scopes it to
    exactly the current visitor (no cross-uid), campaign_id to this campaign."""
    return f"sticky:{company_id}:{uid}:{campaign_id}"


async def get_sticky(company_id, uid: str, campaign_id, ttl: int) -> str | None:
    """Return the pinned offer_target_id (str) or None. Refresh the TTL on a
    hit (sliding). FAIL-OPEN: any error ⇒ None (caller does normal selection)."""
    if not uid:
        return None
    try:
        r = await get_identity_redis()
        key = sticky_key(company_id, uid, campaign_id)
        tid = await r.get(key)
        if tid:
            await r.expire(key, ttl)  # sliding refresh on read
            return tid
        return None
    except Exception as e:  # pragma: no cover — fail-open
        logger.warning("sticky get failed (swallowed): %s", e)
        return None


async def set_sticky_nx(company_id, uid: str, campaign_id, target_id, ttl: int) -> None:
    """First-write-wins pin (SET NX EX). No-op on missing uid/target. FAIL-OPEN."""
    if not uid or not target_id:
        return
    try:
        r = await get_identity_redis()
        await r.set(sticky_key(company_id, uid, campaign_id), str(target_id),
                    nx=True, ex=ttl)
    except Exception as e:  # pragma: no cover — fail-open
        logger.warning("sticky set_nx failed (swallowed): %s", e)


async def repin(company_id, uid: str, campaign_id, target_id, ttl: int) -> None:
    """Overwrite an INVALID pin (closed/missing target) with the new selection
    (SET EX). The only non-NX write — NX would refuse to replace a stale pin, so
    the deterministic rule is "a closed/missing pin is replaced by the freshly
    selected target". No-op on missing uid/target. FAIL-OPEN."""
    if not uid or not target_id:
        return
    try:
        r = await get_identity_redis()
        await r.set(sticky_key(company_id, uid, campaign_id), str(target_id), ex=ttl)
    except Exception as e:  # pragma: no cover — fail-open
        logger.warning("sticky repin failed (swallowed): %s", e)
