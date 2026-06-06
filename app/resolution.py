"""Click parameter resolution per `docs/design/PARAMETER-SYSTEM.md`.

Pure helpers — NO I/O. Used by click-processor's `/decide` to:
  1. Build the merged effective key map from source + campaign
     `param_mappings` (source-wins-per-slot for aliases — SOURCE
     specializes/overrides the campaign on conflict).
  2. Run the per-slot value resolution chain
     (request URL > effective_source hardcoded > campaign hardcoded > NULL).
  3. Capture unmapped query params into `extras` for the eventual
     `clicks.extras` JSONB column (Stage 3 storage target).

SOURCE-WINS (`docs/development/param-source-campaign-overrides-2026-06-02.md`,
DESIGN LOCKED 2026-06-02): the `source_mappings` argument is the
EFFECTIVE source layer — the per-link `campaign_sources.params_override`
when set, else the source's global `param_mappings` (the router resolves
which one via `_effective_source_mappings`). The source SPECIALIZES the
campaign, so on a per-slot conflict the source's alias and the source's
hardcoded default both beat the campaign's. The campaign provides the
fallback for any slot the source did not specialize (non-conflict ⇒
campaign remains). This inverts the pre-2026-06-02 CAMPAIGN-WINS
tiebreak.

Design pin: PARAMETER-SYSTEM.md §"Key resolution".

Canonical-binding rule (F.X, locked 2026-05-14; plan doc:
`docs/roadmap/stage-1a-research/canonical-slot-binding-fix.md`):

  Every name in `CANONICAL_SLOTS` (= `RESERVED_SLOTS` ∪ `SUB_SLOTS`,
  39 names) is ALWAYS treated as a primary input key for its slot
  regardless of whether a `SubIdMapping` entry exists. The
  `alias` field on an entry adds an ADDITIONAL alternative URL
  key. Both keys are tried at click time; canonical name wins on
  collision (i.e., `?source=A&s=B` with `{slot:"source", alias:"s"}`
  binds `slots["source"]="A"`). The previous Vector 2.8 behaviour
  — where canonical slots only resolved when explicitly enumerated
  in `param_mappings` — silently produced empty `{macro}`
  substitutions for any operator who relied on the default vocabulary
  (see Flow 300 v3 debug, 2026-05-13).

Stage 4 (event-processor) will reuse this same module for postback
slot resolution — the algorithm is identical, just over a different
(request URL > postback config > NULL) chain. The same
canonical-binding rule applies once that path lands; the postback
registry (`RESERVED_POSTBACK_SLOTS`, 5 names) extends the canonical
set in that scope.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from app.parameters import CANONICAL_SLOTS
from app.telemetry import OP_PARAM_PARSE, capture_op_msg_throttled

logger = logging.getLogger("tds.resolution")


# F-PARAM-2 — the binding-selector query key. `resolve_domain_campaign`
# (router.py) reads `?c=<alias>` at the param tier to pick a domain binding, so
# `c` is a GLOBALLY-RESERVED routing-control key — never legitimate advertiser
# data. resolve_slots is a pure function with no view of domain resolution, so
# without this it had no way to know `c` was consumed and leaked it into
# `extras` → clicks.extra_params (stream/PG/CH) on every click carrying it. The
# router imports this constant for its `query_params.get(...)` so the key has a
# single source of truth (no drift between consumer and exclusion).
BINDING_SELECTOR_KEY = "c"


__all__ = [
    "BINDING_SELECTOR_KEY",
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
            # D10 (audit 2026-06-03) — malformed param_mappings means
            # EVERY click for that source/campaign resolves params as if
            # unmapped (raw GET keys → extras, reserved/sub columns empty)
            # with only a log line. Surface it (throttled; key by error
            # position so distinct corruptions are each visible without
            # leaking the admin-authored content). No PII — pos + len only.
            capture_op_msg_throttled(
                OP_PARAM_PARSE, f"pos{exc.pos}:len{len(raw)}",
                f"param_mappings JSON unparseable (len={len(raw)} "
                f"err_pos={exc.pos}) — params resolve as unmapped until fixed",
                level="warning",
                raw_len=len(raw),
                err_pos=exc.pos,
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
        source_mappings: The EFFECTIVE source layer (per-link
            `params_override` when set, else the source global
            `param_mappings`), already JSON-parsed via
            `parse_param_mappings`. May be `None` when no source
            matches the click — fall back to campaign-only resolution.
            On a per-slot conflict the source WINS (specializes the
            campaign) — both for the alias and the hardcoded default.
        campaign_mappings: Campaign's `default_param_mappings`
            (same shape).

    Returns:
        `(slots, extras)`:
          - `slots` — `{slot_name: value_or_None}`. Includes every
            slot that EITHER (a) resolved to a non-NULL value through
            any priority level, OR (b) is explicitly enumerated in
            `source_mappings` / `campaign_mappings`. Canonical slots
            (`CANONICAL_SLOTS`) that auto-iterated but didn't resolve
            and were never explicitly mapped are OMITTED — that keeps
            the dict small and preserves the pre-F.X test contract
            (`"keyword" not in slots` when nothing populates it).
          - `extras` — `{get_key: value}` for incoming query params
            not consumed by any mapping AND not matching a canonical
            slot name. Becomes the `clicks.extras` JSONB row in
            Stage 3.

    Resolution chain per the SOURCE-WINS contract
    (`param-source-campaign-overrides-2026-06-02.md`, F.X canonical-binding):
      For each slot in `CANONICAL_SLOTS ∪ src_by_slot ∪ cmp_by_slot`:

        1. Request URL — try the canonical slot name first, then the
           explicit `alias` (when defined and different). First
           non-empty value wins. Canonical-first guarantees that a
           same-named GET key beats an alias collision per the
           design decision matrix. URL ALWAYS wins.
        2. effective_source hardcoded `default_value` (only when the
           source defines the slot — the source specializes the
           campaign, so its default is checked BEFORE the campaign's).
        3. Campaign hardcoded `default_value` (fallback for any slot the
           source did not specialize, and when the source defined the
           slot's alias but left `default_value` empty).
        4. NULL.

    Examined-key bookkeeping: every candidate GET key (canonical
    name AND alias) that exists in `query_params` is marked
    consumed before priority-1 picks a winner. That way the loser
    (the key whose value canonical-first ignored) does NOT leak
    into `extras`. Canonical slot names are also dropped from
    `extras` unconditionally as a defence-in-depth — even when the
    slot never matched any of the resolution layers, its GET key
    was semantically claimed.
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

    # F.X — canonical-binding union. Every canonical slot
    # auto-iterates so a same-named GET key reaches its slot even
    # without an admin-authored mapping entry. Explicitly-mapped
    # non-canonical slots (legacy advertiser flows that mapped to
    # an arbitrary slot name not in the canonical registry) keep
    # working through the same loop.
    all_slots = CANONICAL_SLOTS | set(src_by_slot) | set(cmp_by_slot)

    # Track which incoming GET keys were consumed by a mapping so
    # `extras` can exclude them. We mark a key consumed even when
    # its value is empty — the advertiser explicitly aliased it,
    # and putting it in extras would be redundant noise.
    examined_keys: set[str] = set()
    slots: dict[str, str | None] = {}

    for slot in all_slots:
        cmp_entry = cmp_by_slot.get(slot)
        src_entry = src_by_slot.get(slot)
        is_explicitly_mapped = (cmp_entry is not None) or (src_entry is not None)

        # Candidate GET keys in the locked value-chain order
        # `URL(canonical > eff_source.alias > campaign.alias)`.
        # Canonical slot name FIRST (so `?<slot_name>=` always trumps
        # `?<alias>=` on collision), then the SOURCE alias (SOURCE-WINS:
        # the source specializes the campaign), then the CAMPAIGN alias
        # as the final URL fallback.
        #
        # Finding #4 (2026-06-03): consult BOTH layers' aliases, not
        # just the source's. When a slot is aliased by eff_source AND
        # campaign with DIFFERENT keys this is load-bearing twice over:
        #   (1) the campaign alias is a real resolution fallback — with
        #       only the source alias in get_keys, a click carrying just
        #       the campaign-alias key resolved the slot EMPTY, breaking
        #       the `> campaign.alias` rung of the contract.
        #   (2) every present alias key is marked consumed by the loop
        #       below, so the losing alias key no longer bleeds into
        #       `extras` (measured leak: campaign-alias `ckw` for an
        #       eff_source-won keyword slot).
        # The value loop (first non-empty in get_keys order) then yields
        # canonical > src_alias > cmp_alias automatically.
        get_keys: list[str] = [slot]
        src_alias = _entry_alias(src_entry) if src_entry is not None else None
        cmp_alias = _entry_alias(cmp_entry) if cmp_entry is not None else None
        if src_alias and src_alias != slot:
            get_keys.append(src_alias)
        if cmp_alias and cmp_alias != slot and cmp_alias != src_alias:
            get_keys.append(cmp_alias)

        # Mark every candidate GET key that exists in the query as
        # examined BEFORE picking a winner. This ensures the
        # canonical-loser (or alias-loser, depending on which won)
        # is dropped from `extras` instead of leaking as a duplicate
        # of the resolved slot value.
        for key in get_keys:
            if key in query_params:
                examined_keys.add(key)

        # Step 1 — request URL. First non-empty value wins (canonical
        # first guaranteed by `get_keys` ordering).
        request_value: str | None = None
        for key in get_keys:
            raw = query_params.get(key)
            if raw is not None and raw != "":
                request_value = str(raw)
                break

        if request_value is not None:
            slots[slot] = request_value
            continue

        # Step 2 — effective_source hardcoded (SOURCE-WINS: the source
        # specializes the campaign, so its default is checked FIRST).
        if src_entry is not None:
            src_default = _entry_default(src_entry)
            if src_default is not None:
                slots[slot] = src_default
                continue

        # Step 3 — campaign hardcoded (fallback for any slot the source
        # did not specialize, and when the source defined the alias but
        # left default_value empty).
        if cmp_entry is not None:
            cmp_default = _entry_default(cmp_entry)
            if cmp_default is not None:
                slots[slot] = cmp_default
                continue

        # Step 4 — NULL. Only emit the slot into the result dict
        # when it was explicitly enumerated by either layer. A
        # canonical slot that auto-iterated but resolved to nothing
        # is omitted from `slots` — the macro substituter handles
        # an absent key identically to a `None` value, and omitting
        # keeps the contract close to pre-F.X tests that asserted
        # exact dict shape on the explicit-only slot set.
        if is_explicitly_mapped:
            slots[slot] = None

    # Extras = query keys not consumed by any slot iteration AND
    # not matching a canonical slot name. The canonical-name guard
    # is defence-in-depth: even when a canonical slot's auto-binding
    # loop didn't iterate over the key (e.g., the key arrived with
    # an empty value the priority chain dropped), the key is still
    # semantically the slot's input and should never leak as a
    # generic extras entry. Coerce values to str for the eventual
    # JSONB column (Stage 3 will pin the shape).
    extras: dict[str, str] = {}
    for k, v in query_params.items():
        if k in examined_keys:
            continue
        if k in CANONICAL_SLOTS:
            continue
        # F-PARAM-2 — the binding-selector key is consumed by domain-resolution
        # upstream (router.resolve_domain_campaign), which this pure function
        # can't see. Drop it so it never leaks into extra_params as if it were
        # an advertiser custom param. It's globally reserved → byte-identical for
        # every NON-`c` key (only `c` is removed). Binding attribution still
        # lives in the dedicated binding_id/binding_alias columns.
        if k == BINDING_SELECTOR_KEY:
            continue
        if v is None:
            continue
        extras[k] = str(v)

    return slots, extras
