"""Click parameter resolution per `docs/design/PARAMETER-SYSTEM.md`.

Pure helpers — NO I/O. Used by click-processor's `/decide` to:
  1. Build the merged effective key map from source + campaign
     `param_mappings` (campaign-wins-per-slot for aliases).
  2. Run the per-slot value resolution chain
     (request URL > campaign hardcoded > source hardcoded > NULL).
  3. Capture unmapped query params into `extras` for the eventual
     `clicks.extras` JSONB column (Stage 3 storage target).

Design pin: PARAMETER-SYSTEM.md §"Resolution chain" (lines 74-110).
The semantics are a textbook 4-priority chain with one subtlety —
when both layers define a slot, the campaign's alias wins for the
URL-key lookup, but if neither the request nor the campaign hardcode
fills it, the source's hardcoded value still applies as a final
fallback. That layered behavior is what `resolve_slots` encodes.

Stage 4 (event-processor) will reuse this same module for postback
slot resolution — the algorithm is identical, just over a different
(request URL > postback config > NULL) chain. Stage 4 entry decides
whether to share via `app/common/` or vendor.
"""

from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger("tds.resolution")


__all__ = [
    "parse_param_mappings",
    "resolve_slots",
]


# Pre-parse cap on `param_mappings` JSON. The 19-slot RESERVED_SLOTS
# fully denormalized with 200-char free-text fields fits in a few KB;
# 256 KB is generous and bounds the JSON-bomb / sync-poison surface
# where an admin-authored mapping could be huge. See security audit
# 2026-04-28 (HIGH).
_MAX_RAW_LENGTH = 256 * 1024


def parse_param_mappings(raw: Any) -> list[dict[str, Any]]:
    """Defensive parse of `param_mappings` as it arrives from Redis.

    Sync builder stringifies the JSONB column via `json.dumps` before
    writing to Redis (`services/admin-api/app/sync/builders/sources.py`,
    `campaigns.py`). Redis always hands strings back, so the
    `isinstance(raw, list)` branch is purely defensive — it covers
    test fixtures that pass pre-parsed data and any future caller
    that bypasses Redis.

    Returns `[]` for any malformed / None / empty / oversized input.
    The resolution code below treats "no mapping" as "every query
    key is extras", which matches the design doc fallback semantics.

    Logging discipline: malformed JSON does NOT log raw content. Per
    `outbound-http-safety` rule, `default_value` is admin-authored
    free text and could carry secrets. We log only length + parser
    error position to avoid Sentry breadcrumb leakage.
    """
    if raw is None or raw == "":
        return []
    if isinstance(raw, list):
        # Pre-parsed input — entry-level shape is re-validated inside
        # `resolve_slots` (isinstance dict + non-empty slot checks).
        return raw
    if isinstance(raw, str):
        if len(raw) > _MAX_RAW_LENGTH:
            logger.warning(
                "param_mappings exceeds size cap: len=%d (max=%d); ignoring",
                len(raw), _MAX_RAW_LENGTH,
            )
            return []
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            # Surface drift to ops without leaking content. `len` +
            # error position is enough to diagnose; the offending
            # value lives in PG anyway and ops can fetch it there.
            logger.warning(
                "Malformed param_mappings JSON: len=%d err_pos=%d msg=%s",
                len(raw), exc.pos, exc.msg,
            )
            return []
        if isinstance(parsed, list):
            return parsed
        # Pre-Stage-1 data once stored mappings as dict-of-slot keyed
        # by sub_id; that schema is gone but defensive return keeps
        # us from blowing up if a stale node still serves it.
        logger.warning(
            "param_mappings parsed to non-list (%s); ignoring",
            type(parsed).__name__,
        )
        return []
    logger.warning(
        "param_mappings has unexpected type %s; ignoring",
        type(raw).__name__,
    )
    return []


def _entry_alias(entry: dict[str, Any]) -> str:
    """Effective URL key for a SubIdMapping entry.

    Mirrors the back-compat property on the admin-api Pydantic model
    (`SubIdMapping.get_param`): `alias` if defined and non-empty,
    otherwise fall back to the slot name itself.
    """
    alias = entry.get("alias")
    if isinstance(alias, str) and alias.strip():
        return alias.strip()
    slot = entry.get("slot", "")
    return slot if isinstance(slot, str) else ""


def _entry_default(entry: dict[str, Any]) -> str | None:
    """Hardcoded `default_value` for an entry, normalized.

    Empty string is treated as "no default" — the admin-api Pydantic
    validator strips edges and converts empty back to None, but
    legacy data may have raw "" — defensive normalization.

    Boolean coercion mirrors `macros._coerce_value` (lowercase
    `"true"` / `"false"`) so the macro substitution path stays
    consistent regardless of which layer produced the value.
    """
    val = entry.get("default_value")
    if val is None:
        return None
    if isinstance(val, str):
        v = val.strip()
        return v if v else None
    if isinstance(val, bool):
        # `isinstance(True, int)` is True — handle bool BEFORE int.
        return "true" if val else "false"
    if isinstance(val, (int, float)):
        return str(val)
    # Other unexpected types — log and ignore rather than serialise
    # garbage like `"[1, 2, 3]"` into a redirect URL.
    logger.warning(
        "default_value has unexpected type %s; ignoring",
        type(val).__name__,
    )
    return None


def resolve_slots(
    *,
    query_params: dict[str, Any],
    source_mappings: list[dict[str, Any]] | None,
    campaign_mappings: list[dict[str, Any]] | None,
) -> tuple[dict[str, str | None], dict[str, str]]:
    """Resolve canonical slots + extras for one click.

    Args:
        query_params: Incoming GET params from the request URL.
            Values are the raw string form (or `None`).
        source_mappings: Source's `param_mappings` (already
            JSON-parsed via `parse_param_mappings`). May be `None`
            when no source matches the click — fall back to
            campaign-only resolution.
        campaign_mappings: Campaign's `default_param_mappings`
            (same shape).

    Returns:
        `(slots, extras)`:
          - `slots` — `{slot_name: value_or_None}`. Every slot that
            either layer defines is keyed; value is `None` when no
            priority level filled it (the substituter then handles
            NULL safely).
          - `extras` — `{get_key: value}` for incoming query params
            not consumed by any mapping. Becomes the `clicks.extras`
            JSONB row in Stage 3.

    Resolution order per PARAMETER-SYSTEM.md:
      1. Request URL via merged map (campaign alias wins per slot).
      2. Campaign hardcoded `default_value` (when campaign defines
         the slot).
      3. Source hardcoded `default_value` (when source defines the
         slot — applies even if campaign also defined but had no
         hardcoded value to contribute).
      4. NULL.
    """
    src = source_mappings if isinstance(source_mappings, list) else []
    cmp = campaign_mappings if isinstance(campaign_mappings, list) else []

    # Index entries by slot. Skip malformed (no slot or non-dict) —
    # mapping is a denormalised list, so slot conflicts within one
    # layer are an admin-api validator error, not a click-time
    # concern; we just keep the last entry per slot defensively.
    src_by_slot: dict[str, dict[str, Any]] = {}
    for m in src:
        if isinstance(m, dict):
            slot = m.get("slot")
            if isinstance(slot, str) and slot:
                src_by_slot[slot] = m

    cmp_by_slot: dict[str, dict[str, Any]] = {}
    for m in cmp:
        if isinstance(m, dict):
            slot = m.get("slot")
            if isinstance(slot, str) and slot:
                cmp_by_slot[slot] = m

    # Union of slots that ANY layer defines — that's the set we need
    # to resolve. Anything outside this set has no chance of being
    # bound to a canonical slot, so it falls into extras.
    all_slots = set(src_by_slot) | set(cmp_by_slot)

    # Track which incoming GET keys were consumed by a mapping so
    # `extras` can exclude them. We mark a key consumed even when
    # its value is empty — the advertiser explicitly aliased it,
    # and putting it in extras would be redundant noise.
    examined_keys: set[str] = set()
    slots: dict[str, str | None] = {}

    for slot in all_slots:
        cmp_entry = cmp_by_slot.get(slot)
        src_entry = src_by_slot.get(slot)

        # Effective alias = campaign's wins per slot (per design
        # doc §"Key resolution"). When campaign doesn't define the
        # slot, source's alias is the only option.
        primary_entry = cmp_entry if cmp_entry is not None else src_entry
        # `primary_entry` is non-None because slot came from union.
        get_key = _entry_alias(primary_entry) if primary_entry else slot

        # Step 1 — request URL via merged map.
        if get_key and get_key in query_params:
            examined_keys.add(get_key)
            value = query_params[get_key]
            if value is not None and value != "":
                slots[slot] = str(value)
                continue
            # Empty value → fall through to hardcoded layers.

        # Step 2 — campaign hardcoded (only when campaign defines).
        if cmp_entry is not None:
            cmp_default = _entry_default(cmp_entry)
            if cmp_default is not None:
                slots[slot] = cmp_default
                continue

        # Step 3 — source hardcoded (independent fallback even when
        # campaign overrode the alias but left default_value empty).
        if src_entry is not None:
            src_default = _entry_default(src_entry)
            if src_default is not None:
                slots[slot] = src_default
                continue

        # Step 4 — NULL.
        slots[slot] = None

    # Extras = query keys not consumed. Coerce values to str for
    # the eventual JSONB column (Stage 3 will pin the shape).
    extras: dict[str, str] = {}
    for k, v in query_params.items():
        if k in examined_keys:
            continue
        if v is None:
            continue
        extras[k] = str(v)

    return slots, extras
