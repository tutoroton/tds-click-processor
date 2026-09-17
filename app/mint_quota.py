"""N1 — a per-principal RATE bound on route-code issuance.

**What this is, precisely, because the finding it closes was phrased three times
and measured wrong each time** (`docs/development/ssv-completion-2026-09-16/
54-N1-BOUND-DESIGN.md`):

The node already carries two D149 admission bulkheads — `_preview_inflight` and
the offerwall tile budget. Those bound **concurrency**, node-globally, and shed
with 503. They do NOT bound a RATE: a caller pacing themselves below the
in-flight cap can mint route codes indefinitely.

This module adds the missing half: **how many codes one principal may be issued
per window.** It is charged in CODES, never in requests, because one wall request
mints one code per tile (`main.py` `_mint_tile_codes`) — a request quota would
therefore not be a capability quota at all.

## Two keyspaces, deliberately, not one

They are separate **by construction rather than by convention**, for the same
reason D149 separated its own two budgets after a shared dedup key made one shed
suppress the other's report:

| path | principal | why |
|---|---|---|
| preview | the credential hash | a preview REQUIRES a key — the Worker returns no verdict without one, so nothing mints |
| wall | the campaign | a wall mint requires only a known company and wall id (`_mint_tile_codes`), **no credential** — so a credential-keyed budget would leave this path unbounded, which is the hole this module exists to close |

⚠️ **Stated, not implied:** the wall budget is therefore shared by every visitor
of a campaign. For that path it is a SHEDDER, not per-caller abuse protection —
a campaign under genuine load and a campaign under abuse look the same to it.

## Ordering — charged AFTER the bulkhead, never before

The bulkhead's own comment records why its position is deliberate: an over-limit
request must cost *"one counter read and nothing else: no Redis, no routing
engine, no tile reads"*. A rate charge MUST touch Redis, so putting it first
would destroy that property. It therefore runs only for requests that already
passed the cheap gate, and only immediately before signing.

## Failure posture — CLOSED for the mint, never for the click

If the budget cannot be established, no codes are issued. That is safe here and
would not be safe elsewhere: the ordinary click path never calls this, and a
preview that cannot mint degrades to the answer the feature already has for a
node that cannot help. It does **not** become a click — see `main.py`'s D149
block: *"a preview flood converted into clicks is R3 reopened through capacity"*.

🔴 Note the asymmetry with admin-api's `common/rate_limit.py`, which is NOT the
model: that one fails OPEN by design (right for an auth flow, wrong for a quota)
and charges 1 per call with no way to weight a batch.

## Disabled by default

Both caps default to 0, which means OFF and byte-identical: no Redis op, no
exception, nothing. The bound activates per node when an operator sets it, which
is how every routing-plane change in this programme has landed.
"""
from __future__ import annotations

import time

from app.config import settings
from app.redis_client import get_redis

__all__ = ["MintQuotaExceeded", "charge_preview", "charge_wall"]


class MintQuotaExceeded(Exception):
    """The principal has spent its issuance budget for this window.

    Carries the numbers so the refusal can SAY what it refused — a bare refusal
    tells an operator nothing, and this programme has paid for silent ones.
    """

    def __init__(self, *, scope: str, cap: int, window_seconds: int, units: int):
        self.scope = scope
        self.cap = cap
        self.window_seconds = window_seconds
        self.units = units
        super().__init__(
            f"mint quota exhausted: scope={scope} cap={cap} "
            f"window={window_seconds}s requested={units}"
        )


def _window_start(now: float, window_seconds: int) -> int:
    """The current fixed window's start, in whole seconds.

    A FIXED window, not a sliding one, and the choice is deliberate: it is one
    INCRBY plus one conditional EXPIRE, the cheapest thing that can sit in front
    of signing. Its known cost is burst tolerance at a boundary — up to 2× the
    cap across two adjacent windows. For a shedder that is acceptable, and it is
    written down here rather than discovered later.
    """
    return int(now) - (int(now) % window_seconds)


async def _charge(*, scope: str, principal: str, units: int,
                  cap: int, window_seconds: int) -> None:
    if cap <= 0:
        # OFF. Touch nothing — not Redis, not the clock. A disabled bound must
        # be byte-identical to a node that has never heard of this module.
        return
    if units <= 0:
        return
    window = max(1, window_seconds)
    key = f"mintq:{scope}:{_window_start(time.time(), window)}:{principal}"
    try:
        redis = await get_redis()
        spent = await redis.incrby(key, units)
        if spent == units:
            # First charge in this window — give the key the window's life.
            # If this EXPIRE were lost the key would never reset and the
            # principal would be refused FOREVER, so it is issued immediately
            # after the INCRBY and its failure is treated as a charge failure
            # below rather than ignored. admin-api's limiter ignores exactly
            # this, and that is a latent permanent-refusal defect there.
            await redis.expire(key, window)
    except Exception as exc:  # noqa: BLE001 — see the module docstring
        # FAIL CLOSED for the mint: the budget could not be established, so
        # nothing is issued. The click path never reaches this code.
        raise MintQuotaExceeded(
            scope=scope, cap=cap, window_seconds=window, units=units,
        ) from exc
    if spent > cap:
        raise MintQuotaExceeded(
            scope=scope, cap=cap, window_seconds=window, units=units,
        )


async def charge_preview(key_hash: str | None, units: int = 1) -> None:
    """Charge `units` code-issuances to a preview credential.

    `key_hash` is the sha256 the Worker already computes and sends, so the
    principal costs no new plumbing. A preview with no credential never reaches
    a mint (the Worker returns no verdict), so `None` is not a hole here — it is
    a path that cannot issue.
    """
    if not key_hash:
        return
    await _charge(
        scope="pv",
        principal=key_hash,
        units=units,
        cap=settings.mint_quota_preview_codes_per_window,
        window_seconds=settings.mint_quota_window_seconds,
    )


async def charge_wall(campaign_id: int | None, units: int) -> None:
    """Charge `units` code-issuances to a campaign's wall budget.

    The campaign, not the credential: a wall mint needs only a known company and
    wall id, so a credential-keyed budget would leave this path unbounded. See
    the module docstring for what that costs in honesty.
    """
    if not campaign_id:
        return
    await _charge(
        scope="wall",
        principal=str(campaign_id),
        units=units,
        cap=settings.mint_quota_wall_codes_per_window,
        window_seconds=settings.mint_quota_window_seconds,
    )
