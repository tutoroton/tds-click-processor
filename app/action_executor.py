"""Per-flow action execution — Stage 2 / Vectors 2.4 + 2.5.

After `cascade.resolve_flow` picks a winning flow, this module translates
its `action_type` + `action_config` into the concrete redirect URL (or a
block sentinel) that `router.py` returns to the worker.

Why a separate module: keeps `router.py` an orchestrator and lets each
action variant stay focused. `action_executor` is the dispatch layer
between flow → URL.

Action contract (per `routing-entities` rule + admin-api
`flows/schemas.py:VALID_ACTION_TYPES = {redirect, offer, split, block}`):

    redirect — `action_config = {"url": "<template>"}`. Substitute via
               `build_url`. The url_template was validated at write time
               by admin-api `validate_url_template` (Vector A2 defence).
    offer    — `action_config = {"offer_id": N, "target_id": M}`. Load
               `offer_target:{M}` HASH for the URL template, then apply
               the same `build_url` substitution. If `target_id` is
               missing or the target row is gone, fall back to the
               offer's default target via `resolve_target`.
    split    — `action_config = {"offers": [{offer_id, target_id, weight}, ...]}`.
               Weighted-random pick over the inline offers list, then
               same load+substitute path as the `offer` branch.
    block    — Returns `None`. Caller (router) treats `None` from this
               module as "explicit block" and surfaces a 404. Alert
               sub-config in `action_config.alert` is consumed by the
               Stage 6 alert module — execution layer just notes its
               presence in the timing dict.

`pass` is intentionally NOT supported — admin-api's `VALID_ACTION_TYPES`
does not include it. The `routing-entities` rule's mention of "pass" is
a documentation drift to be cleaned up post-Stage 2 (deferred ticket).
"""

from __future__ import annotations

import json
import logging
import random
from typing import Any

from app.models import ClickRequest
from app.telemetry import (
    OP_OFFER_RESOLVE,
    OP_SPLIT_FALLBACK,
    capture_op_msg_throttled,
)

logger = logging.getLogger("tds.action")


__all__ = ["execute_action", "pinned_target_result", "BLOCK_RESULT"]


def pinned_target_result(
    target: dict[str, Any],
    target_id,
    req: ClickRequest,
    campaign_id: str,
    build_url_fn,
    source_mappings,
    campaign_mappings,
    flow_id: str | None,
) -> dict[str, Any] | None:
    """v2 Phase S — build the redirect result for a STICKY-pinned offer_target.

    Takes the ALREADY-LOADED `offer_target:{tid}` HASH (the caller HGETALLs it
    once to validate availability, then reuses it here — no double read). The
    pin is a (uid,campaign)→target decision, independent of the winning flow's
    own offer pick; we read the target's `offer_id` for the `{offer_id}` macro
    and build the URL. Returns the standard result dict with
    `target_selection_path='sticky'`, or None when the target has no url (caller
    falls back to normal selection + re-pin).
    """
    if not target or not target.get("url"):
        return None
    offer_id = target.get("offer_id") or ""
    url = build_url_fn(
        target["url"], req, campaign_id, offer_id,
        source_mappings=source_mappings,
        campaign_mappings=campaign_mappings,
        target_id=str(target_id),
        flow_id=flow_id,
    )
    return {
        "url": url,
        "offer_id": offer_id or None,
        "target_id": str(target_id),
        "target_selection_path": "sticky",
    }


# Sentinel for `action_type='block'`. Caller tells worker to return 404
# (or worker may render a block page — that's a worker concern). Use a
# typed dict so callers can pattern-match on `result.get("action")`
# without colliding with the `{url, offer_id, target_id}` happy-path.
BLOCK_RESULT: dict[str, Any] = {"action": "block", "url": None}


async def execute_action(
    r,
    flow: dict[str, Any],
    req: ClickRequest,
    campaign_id: str,
    *,
    source_mappings: list | None,
    campaign_mappings: list | None,
    build_url_fn,
) -> dict[str, Any] | None:
    """Translate a winning flow into a concrete routing result.

    Args:
        r: Redis async client — needed for offer/target HGETALLs.
        flow: Flow HASH from `cascade.resolve_flow` (with `_id`).
        req: Original click request (used by `build_url_fn` for macros).
        campaign_id: Stringified campaign PK (used for substitution).
        source_mappings / campaign_mappings: Param-mapping JSON arrays
            already parsed by `_fetch_resolution_context`. Passed
            through to `build_url_fn`.
        build_url_fn: `router.build_url` injected to avoid a circular
            import (router imports this module). Same signature as
            `build_url(template, req, campaign_id, offer_id, *,
            source_mappings, campaign_mappings)`.

    Returns:
        `{"url": str, "offer_id": str | None, "target_id": str | None}`
        on a successful redirect/offer/split.
        `BLOCK_RESULT` on `block`.
        `None` when the action couldn't be executed (malformed config,
        missing offer/target row) — caller falls back to legacy split.
    """
    action_type = flow.get("action_type", "")
    config = _parse_action_config(flow.get("action_config", "{}"))

    # T2.5 — extract flow_id once so the per-action helpers can
    # thread it into build_url for the {flow_id} macro. The Redis
    # HASH key is `flow:{id}`, surfaced by the cascade resolver
    # as `flow["_id"]`. Stringified at the boundary so build_url
    # always sees a str | None (avoids per-helper int/str coercion).
    flow_id = str(flow["_id"]) if isinstance(flow, dict) and flow.get("_id") else None

    if action_type == "redirect":
        return _execute_redirect(config, req, campaign_id, build_url_fn,
                                 source_mappings, campaign_mappings, flow_id)
    if action_type == "offer":
        return await _execute_offer(r, config, req, campaign_id, build_url_fn,
                                     source_mappings, campaign_mappings, flow_id)
    if action_type == "split":
        return await _execute_split(r, config, req, campaign_id, build_url_fn,
                                     source_mappings, campaign_mappings, flow_id)
    if action_type == "block":
        return BLOCK_RESULT

    # Unknown action_type — sync drift or admin-api bug. Surface as
    # "no execution possible" so router falls back gracefully.
    logger.warning(
        "execute_action: flow %s has unknown action_type=%r — falling back",
        flow.get("_id"), action_type,
    )
    return None


# ============================================================
# Per-action helpers
# ============================================================


def _execute_redirect(
    config: dict[str, Any],
    req: ClickRequest,
    campaign_id: str,
    build_url_fn,
    source_mappings,
    campaign_mappings,
    flow_id: str | None,
) -> dict[str, Any] | None:
    """`redirect` — substitute macros in `action_config.url`.

    Admin-api rejects redirect flows without a URL at write time, so
    the `not url` branch is defense-in-depth against sync drift.
    """
    url_template = config.get("url")
    if not isinstance(url_template, str) or not url_template:
        logger.warning("redirect action missing url — falling back")
        return None

    # T2.5 — thread flow_id so {flow_id} macro resolves. redirect
    # actions have no offer/target so those macros remain NULL
    # and collapse via safe_substitute's cleanup pass.
    url = build_url_fn(
        url_template, req, campaign_id, "",
        source_mappings=source_mappings,
        campaign_mappings=campaign_mappings,
        target_id=None,
        flow_id=flow_id,
    )
    # v2 Phase A2 — provenance: a redirect action is a bare URL template.
    return {"url": url, "offer_id": None, "target_id": None,
            "target_selection_path": "bare_url"}


async def _execute_offer(
    r,
    config: dict[str, Any],
    req: ClickRequest,
    campaign_id: str,
    build_url_fn,
    source_mappings,
    campaign_mappings,
    flow_id: str | None,
) -> dict[str, Any] | None:
    """`offer` — flow has pinned offer_id + target_id."""
    offer_id = config.get("offer_id")
    target_id = config.get("target_id")
    if not _is_positive_int(offer_id):
        logger.warning("offer action missing offer_id — falling back")
        return None

    return await _resolve_offer_url(
        r, str(offer_id), target_id,
        req, campaign_id, build_url_fn,
        source_mappings, campaign_mappings,
        flow_id,
    )


async def _execute_split(
    r,
    config: dict[str, Any],
    req: ClickRequest,
    campaign_id: str,
    build_url_fn,
    source_mappings,
    campaign_mappings,
    flow_id: str | None,
) -> dict[str, Any] | None:
    """`split` — weighted random pick over `action_config.offers`."""
    entries = config.get("offers")
    if not isinstance(entries, list) or len(entries) < 1:
        logger.warning("split action missing offers list — falling back")
        return None

    valid: list[dict[str, Any]] = []
    weights: list[int] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        oid = entry.get("offer_id")
        if not _is_positive_int(oid):
            continue
        weight = entry.get("weight", 0)
        if not isinstance(weight, (int, float)) or weight < 0:
            continue
        valid.append(entry)
        weights.append(int(weight))

    if not valid or sum(weights) <= 0:
        logger.warning("split action has no usable offers — falling back")
        # B3 (audit 2026-06-03) — a split flow whose every branch is
        # invalid/zero-weight silently falls back to legacy selection on
        # EVERY click. Surface it (throttled per flow id) — an operator
        # likely has a misconfigured split (e.g. all weights truncated,
        # all offers archived).
        capture_op_msg_throttled(
            OP_SPLIT_FALLBACK, flow_id,
            f"split flow {flow_id} has no usable offers "
            f"({len(entries)} entries) — falling back to legacy selection",
            level="warning",
            flow_id=flow_id,
            entry_count=len(entries),
        )
        return None

    chosen = random.choices(valid, weights=weights, k=1)[0]
    result = await _resolve_offer_url(
        r, str(chosen["offer_id"]), chosen.get("target_id"),
        req, campaign_id, build_url_fn,
        source_mappings, campaign_mappings,
        flow_id,
    )
    # v2 Phase A2 — the SELECTION mechanism for a split is the weighted pick,
    # regardless of how the chosen entry's URL resolved underneath.
    if result is not None:
        result["target_selection_path"] = "split_weighted"
    return result


async def _resolve_offer_url(
    r,
    offer_id: str,
    target_id: Any,
    req: ClickRequest,
    campaign_id: str,
    build_url_fn,
    source_mappings,
    campaign_mappings,
    flow_id: str | None,
) -> dict[str, Any] | None:
    """Shared between offer + split actions — load target → URL.

    Resolution:
      1. If `target_id` set and `offer_target:{tid}` exists → use its
         `url_template`.
      2. Else if offer has `has_targets='1'` → fall back to the offer's
         `is_default='1'` target via SET enumeration.
      3. Else use offer's bare `url` field (legacy offers without
         per-target URLs — kept as a safety net during migration).
      4. Nothing usable → `None` so router falls back to legacy split.
    """
    pinned_template: str | None = None
    pinned_target_id: str | None = None
    # v2 Phase A2 — target_selection_path provenance: how the destination
    # was resolved (pinned target / offer's default target / offer bare url).
    selection_path = "pinned"
    if _is_positive_int(target_id):
        target = await r.hgetall(f"offer_target:{target_id}")
        if target and target.get("url"):
            pinned_template = target["url"]
            pinned_target_id = str(target_id)

    if pinned_template is None:
        # Fall back to offer-level resolution: load offer, pick
        # `is_default=1` target (if `has_targets=1`), or use the bare
        # offer.url field.
        offer = await r.hgetall(f"offer:{offer_id}")
        if not offer:
            logger.warning(
                "offer:%s not found in Redis — sync drift?", offer_id,
            )
            # D3 (audit 2026-06-03) — the flow points at an offer that
            # isn't in Redis (sync drift / archived). Silent fallback on
            # every click → surface it (throttled per offer id).
            capture_op_msg_throttled(
                OP_OFFER_RESOLVE, offer_id,
                f"offer {offer_id} not found in Redis (sync drift?) — "
                "action falling back to legacy selection",
                level="warning",
                offer_id=offer_id,
            )
            return None
        pinned_template, pinned_target_id = await _offer_default_template(
            r, offer_id, offer,
        )
        # default-target resolution → 'offer_default'; bare offer.url (no
        # default target) → 'bare_url'.
        selection_path = "offer_default" if pinned_target_id else "bare_url"
        if not pinned_template:
            logger.warning(
                "offer:%s has no usable URL — falling back", offer_id,
            )
            # D3 — offer exists but has no default target + no bare url
            # (the B4 shape). Falls back silently → surface it.
            capture_op_msg_throttled(
                OP_OFFER_RESOLVE, offer_id,
                f"offer {offer_id} has no usable URL (no is_default target, "
                "no bare url) — falling back to legacy selection",
                level="warning",
                offer_id=offer_id,
            )
            return None

    # T2.5 — pinned_target_id resolves the {offer_target_id} macro;
    # flow_id (threaded from execute_action) resolves {flow_id}.
    # When `target_id` was pinned by config, `pinned_target_id` is
    # that value; when fallback walked to offer.is_default_target,
    # it's the discovered target. Either way the macro substitution
    # reflects the actual destination, not the requested one.
    url = build_url_fn(
        pinned_template, req, campaign_id, offer_id,
        source_mappings=source_mappings,
        campaign_mappings=campaign_mappings,
        target_id=pinned_target_id,
        flow_id=flow_id,
    )
    return {
        "url": url,
        "offer_id": offer_id,
        "target_id": pinned_target_id,
        "target_selection_path": selection_path,
    }


# Defensive cap on per-offer target enumeration. Realistic offers have
# 1-10 targets; a misconfigured offer with thousands of targets would
# blow the hot-path Redis pipeline length without this cap. Mirror of
# `router._MAX_SOURCES_PER_CAMPAIGN_AT_CLICK` per security audit
# 2026-04-28 MEDIUM-005.
_MAX_TARGETS_PER_OFFER_AT_CLICK = 100


async def _offer_default_template(
    r, offer_id: str, offer: dict,
) -> tuple[str | None, str | None]:
    """Read the offer's `is_default=1` target, or fall back to offer.url.

    Returns `(template, target_id)`. Either may be `None`.

    Sort key uses `int()` so the iteration is in numeric (not
    lexicographic) order — e.g. `["2", "10", "100"]` instead of
    `["10", "100", "2"]`. When multiple `is_default=1` targets exist
    (data integrity issue), the lower-numbered one wins
    deterministically (code review MEDIUM 2026-04-28).
    """
    if offer.get("has_targets") == "1":
        target_ids = await r.smembers(f"offer:{offer_id}:targets")
        if target_ids:
            sorted_ids = sorted(target_ids, key=_safe_target_sort_key)
            if len(sorted_ids) > _MAX_TARGETS_PER_OFFER_AT_CLICK:
                logger.warning(
                    "offer:%s has %d targets (>cap %d); truncating enumeration",
                    offer_id, len(sorted_ids), _MAX_TARGETS_PER_OFFER_AT_CLICK,
                )
                sorted_ids = sorted_ids[:_MAX_TARGETS_PER_OFFER_AT_CLICK]
            pipe = r.pipeline()
            for tid in sorted_ids:
                pipe.hgetall(f"offer_target:{tid}")
            rows = await pipe.execute()
            for tid, row in zip(sorted_ids, rows):
                if row and row.get("is_default") == "1" and row.get("url"):
                    return row["url"], str(tid)

    bare = offer.get("url")
    if bare:
        return bare, None
    return None, None


def _safe_target_sort_key(tid: Any) -> tuple[int, int, str]:
    """Sort target IDs numerically with a stable fallback for malformed input.

    Returns `(bucket, value, original)`:
      - bucket=0 + numeric value when `int(tid)` succeeds.
      - bucket=1 + 0 + raw string when parse fails (sorts after numeric;
        original string used as tiebreaker for determinism).
    """
    try:
        return (0, int(tid), str(tid))
    except (ValueError, TypeError):
        return (1, 0, str(tid))


# ============================================================
# Pure helpers
# ============================================================


def _parse_action_config(raw: Any) -> dict[str, Any]:
    """Robust parse — Redis HGETALL returns string, sync builder JSON-encoded.

    Returns `{}` for malformed/missing input. Caller's `if not config`
    branch handles bad data uniformly across action types.
    """
    if isinstance(raw, dict):
        return raw
    if not isinstance(raw, str) or not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _is_positive_int(value: Any) -> bool:
    """Strict check — accepts only `int > 0`, NOT numeric strings or bools.

    Admin-api validates `offer_id` / `target_id` as positive ints at
    write time; sync builder preserves the type. A string here means
    drift / corruption — refuse.

    Booleans are explicitly excluded because `isinstance(True, int) is
    True` in Python — without the bool guard, a malformed `action_config
    = {"offer_id": True}` would pass and then `str(True)` produces the
    Redis key `offer:True`, silently failing on hgetall (per code-review
    HIGH 2026-04-28).
    """
    return (
        isinstance(value, int)
        and not isinstance(value, bool)
        and value > 0
    )
