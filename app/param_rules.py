"""Campaign parameter rules — post-chain, fill-only slot assignment (GTD-R166 W2).

Pure helpers — NO I/O. Evaluated ONCE per click by the click-processor's
`/decide` path, in `router._build_campaign_attribution` immediately AFTER the
base `resolution.resolve_slots()` and BEFORE buyer-chain enrichment, so a
rule-filled `buyer_id` flows through the existing org enrichment + cross-tenant
guard and affects routing scope naturally.

Semantics (design SoT: `docs/design/PARAMETER-SYSTEM.md` §"Post-chain rules
layer"; frozen contract: `gtd-param-rules-disco/G-build-spec.md` §3):

  1. Config rides the existing `campaign:{cid}` Redis hash under key
     `param_rules` (a JSON array; W1 ships the producer). Absent / empty /
     malformed ⇒ NO rules ⇒ behaviour byte-identical to a campaign without
     rules. Zero new hot-path I/O — the hash was already fetched.
  2. Per ENABLED rule, in array order: conditions (``and`` / ``or``) evaluate
     against context dims (geo / device / os / browser / domain / referrer /
     source token) + the CURRENT slot state — which INCLUDES earlier rules'
     fills, so later rules see earlier assignments.
  3. On match, each assignment FILLS its slot only if that slot is still
     empty/absent after the full existing chain (URL > effective_source >
     campaign hardcode) and any earlier rule fill. "URL wins" is never pierced.
     Assignment values are literals or click-time-legal macros; a macro
     expanding to the empty string ⇒ NO fill (the slot stays empty and falls
     through). Assigning the ``source`` slot is forbidden (defensively skipped
     — W1 rejects it at save time).
  4. Fail-open at BOTH grains: a malformed whole payload ⇒ all rules ignored +
     one structured warning; any per-rule evaluation error ⇒ that rule skipped
     + a throttled structured warning. Traffic ALWAYS keeps flowing.

The evaluation OUTCOME (``{"fills": {...}, "applied": [...]}``) lives on the
per-campaign ``attribution`` result (NEVER on ``req`` — a domain→geo
fall-through drops the whole attribution dict, so fills can't leak across
candidate campaigns). ``fills`` is threaded to ``router.build_url`` (via the
``param_fills`` kwarg) so the second, independent ``resolve_slots`` inside
``build_url`` gets the SAME fills overlaid — the DB row and the 302 URL never
diverge. ``applied`` is written as the ``_param_rules`` provenance key
post-resolution (see ``main._build_extra_params``).

Budget: pure in-memory, no regex on attacker-controlled condition inputs (only
a bounded literal macro-name scan on admin-authored assignment values), no new
Redis op — sub-ms per click.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any

from app.parameters import CANONICAL_SLOTS
from app.telemetry import OP_PARAM_RULES, capture_op_msg_throttled
from app.ua_parser import parse_ua

logger = logging.getLogger("tds.param_rules")

__all__ = [
    "parse_param_rules",
    "apply_param_rules",
]


# Pre-parse cap on the `param_rules` JSON as it arrives from Redis. The
# per-click bandwidth is the whole `campaign:{cid}` hash on EVERY click, so this
# is tighter than resolution's 256 KB mapping cap: 50 rules × (≤10 conditions +
# ≤10 assignments) of bounded strings fits comfortably in 64 KB.
_MAX_RAW_LENGTH = 64 * 1024

# Defensive caps mirroring the frozen shape (G §2) — the node NEVER trusts the
# sync payload to already be within admin-api's save-time caps (forward-compat +
# a poisoned/oversized hash must not blow the hot-path budget).
_MAX_RULES = 50
_MAX_CONDITIONS = 10
_MAX_ASSIGNMENTS = 10
_MAX_IN_VALUES = 50
# Upper bound on a single filled value AFTER macro expansion. A literal is
# ≤512 (W1), but `{user_agent}`-style macros can expand larger; bound the slot
# so a filled column stays comparable to a URL-delivered one (query values are
# capped at 1024 at the model boundary — `models.ClickRequest`).
_MAX_FILL_LENGTH = 1024

# The context dims that DON'T depend on slot state (stable per click). `source`
# and `param:<slot>` are read live from the mutating `slots` dict so later rules
# observe earlier fills.
_STABLE_DIMS = ("geo", "device", "os", "browser", "domain", "referrer")

# Macro placeholder grammar — BYTE-IDENTICAL to `macros._MACRO_RE` so a rule
# value and a landing/redirect template resolve the same tokens. Only matched,
# registry-shaped `{name}` tokens are substituted; a stray literal `{` passes
# through untouched (same guarantee as the URL substituter).
_MACRO_RE: re.Pattern[str] = re.compile(r"\{([a-z][a-z0-9_]{0,99})\}")

# `source` is a reserved slot but assigning it is FORBIDDEN (G §2) — source
# matching precedes resolution, so a rule could never legitimately set it.
_FORBIDDEN_ASSIGN_SLOTS = frozenset({"source"})


def parse_param_rules(raw: Any) -> list[dict[str, Any]]:
    """Defensive parse of the `param_rules` payload as it arrives from Redis.

    The sync builder stringifies the JSONB column via ``json.dumps`` before
    HSET, so Redis hands back a JSON string; the ``list`` branch covers
    pre-parsed test fixtures. Returns ``[]`` for any absent / empty / malformed
    / oversized input — the caller treats "no rules" identically to a campaign
    without a `param_rules` key (byte-identical to today). Malformed JSON logs
    (throttled, content-free) so drift is visible to ops without leaking the
    admin-authored rule bodies.
    """
    if raw is None or raw == "":
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        if len(raw) > _MAX_RAW_LENGTH:
            logger.warning(
                "param_rules exceeds size cap: len=%d (max=%d); ignoring",
                len(raw), _MAX_RAW_LENGTH,
            )
            capture_op_msg_throttled(
                OP_PARAM_RULES, f"oversize:len{len(raw)}",
                f"param_rules payload over cap (len={len(raw)} "
                f"max={_MAX_RAW_LENGTH}) — all rules ignored until fixed",
                level="warning", raw_len=len(raw),
            )
            return []
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            logger.warning(
                "Malformed param_rules JSON: len=%d err_pos=%d msg=%s",
                len(raw), exc.pos, exc.msg,
            )
            capture_op_msg_throttled(
                OP_PARAM_RULES, f"pos{exc.pos}:len{len(raw)}",
                f"param_rules JSON unparseable (len={len(raw)} "
                f"err_pos={exc.pos}) — all rules ignored until fixed",
                level="warning", raw_len=len(raw), err_pos=exc.pos,
            )
            return []
        if isinstance(parsed, list):
            return parsed
        logger.warning(
            "param_rules parsed to non-list (%s); ignoring",
            type(parsed).__name__,
        )
        return []
    logger.warning(
        "param_rules has unexpected type %s; ignoring", type(raw).__name__,
    )
    return []


def _stable_context_dims(req: Any) -> dict[str, str]:
    """The slot-independent context dims, built with the SAME normalization the
    routing criteria evaluator uses for the equivalent dims (``cascade``
    ``click_attrs``): geo uppercased ISO, os/device lowercased, browser Title
    Case verbatim. ``domain``/``referrer`` have no existing criteria equivalent
    — lowercased (hostnames + referrers are case-insensitive for matching). The
    per-op comparison additionally lowercases both sides (see ``_matches``), so
    the stored casing here only matters for readability."""
    ua = parse_ua(req.user_agent or "")
    return {
        "geo": (getattr(req, "country", "") or "").upper(),
        "device": (ua.get("device_type") or "").lower(),
        "os": (ua.get("os") or "").lower(),
        "browser": ua.get("browser") or "",
        "domain": (getattr(req, "hostname", "") or "").lower(),
        "referrer": getattr(req, "referer", "") or "",
    }


def _dim_value(
    dim: str, stable: dict[str, str], slots: dict[str, str | None]
) -> str | None:
    """Resolve a condition's ``dim`` to its click value.

    ``source`` (the source token) and ``param:<slot>`` read the LIVE ``slots``
    dict so a rule sees earlier rules' fills. An unrecognized dim returns the
    sentinel ``None`` and the condition fails closed (no match) — W1 validates
    dim membership, so this is defence-in-depth for a poisoned/forward-compat
    payload, never a live path."""
    if dim in stable:
        return stable[dim]
    if dim == "source":
        return slots.get("source")
    if dim.startswith("param:"):
        return slots.get(dim[6:])
    return None


def _norm(s: str | None) -> str:
    """Case-insensitive normalization (lowercase). Applied to BOTH sides of
    every value comparison. This is the existing cascade default for
    non-case-preserve dims; for the two case-preserve dims (geo, browser) it is
    strictly more forgiving of admin value-casing and — since ISO geo codes and
    UA-parsed os/device/browser names are all unique case-insensitively — never
    yields a false match. (Documented divergence from cascade's per-dim casing;
    see PARAMETER-SYSTEM.md.)"""
    return (s or "").lower()


def _matches(op: str, click_val: str | None, value: Any) -> bool:
    """Evaluate ONE condition. Fails closed (returns False) on an unknown op or
    a shape-invalid value — W1 validates these at save time, so a mismatch here
    is a poisoned/forward-compat payload and MUST NOT fill."""
    if op == "empty":
        return click_val is None or click_val == ""
    if op == "not_empty":
        return click_val is not None and click_val != ""

    if op == "in":
        if not isinstance(value, list):
            return False
        wanted = {_norm(v) for v in value if isinstance(v, str)}
        return _norm(click_val) in wanted

    # Single-string-value ops.
    if not isinstance(value, str):
        return False
    if op == "eq":
        return _norm(click_val) == _norm(value)
    if op == "ne":
        return _norm(click_val) != _norm(value)
    if op == "contains":
        return _norm(value) in _norm(click_val)
    return False


def _rule_matches(
    rule: dict[str, Any], stable: dict[str, str], slots: dict[str, str | None]
) -> bool:
    """Whether a rule's conditions hold. Empty conditions ⇒ matches ALL clicks
    (G §2). ``and`` = every condition holds; ``or`` = any holds."""
    conditions = rule.get("conditions") or []
    if not isinstance(conditions, list) or not conditions:
        return True  # 0 conditions = match-all
    logic = rule.get("conditions_logic", "and")
    results: list[bool] = []
    for cond in conditions[:_MAX_CONDITIONS]:
        if not isinstance(cond, dict):
            results.append(False)
            continue
        dim = cond.get("dim", "")
        op = cond.get("op", "")
        click_val = _dim_value(dim, stable, slots)
        results.append(_matches(op, click_val, cond.get("value")))
    if logic == "or":
        return any(results)
    return all(results)


def _expand_value(value: str, macro_values: dict[str, Any]) -> str:
    """Plain macro substitution of an assignment value (NOT URL-encoded — this
    is a slot value, not a URL). ``{macro}`` → the click-time value from
    ``macro_values`` (the SAME landing values dict ``build_url`` uses), or ""
    when the macro resolved to None/empty/absent. Bounded output."""
    def _repl(m: re.Match[str]) -> str:
        v = macro_values.get(m.group(1))
        if v is None or v == "":
            return ""
        if isinstance(v, bool):
            return "true" if v else "false"
        if isinstance(v, (int, float)):
            return str(v)
        s = str(v)
        return s
    out = _MACRO_RE.sub(_repl, value)
    if len(out) > _MAX_FILL_LENGTH:
        out = out[:_MAX_FILL_LENGTH]
    return out


def _apply_assignments(
    rule: dict[str, Any],
    slots: dict[str, str | None],
    macro_values: dict[str, Any],
    fills: dict[str, str],
) -> list[str]:
    """Apply a matched rule's fill-only assignments. Mutates ``slots`` (and
    ``macro_values`` for later rules' ``{slot}`` expansion) in place. Returns
    the list of slot names this rule actually filled (for provenance)."""
    assignments = rule.get("assignments") or []
    if not isinstance(assignments, list):
        return []
    filled: list[str] = []
    for a in assignments[:_MAX_ASSIGNMENTS]:
        if not isinstance(a, dict):
            continue
        slot = a.get("slot")
        raw_value = a.get("value")
        if not isinstance(slot, str) or not slot:
            continue
        if slot in _FORBIDDEN_ASSIGN_SLOTS:
            continue
        if slot not in CANONICAL_SLOTS:
            # Only canonical slots (reserved + subN) have columns / macros; a
            # non-canonical slot has nowhere to land. W1 rejects it at save.
            continue
        if not isinstance(raw_value, str):
            continue
        # Fill-ONLY: skip a slot already carrying a value (URL / hardcode /
        # earlier rule). ``slots.get`` covers both absent and explicit-None.
        current = slots.get(slot)
        if current is not None and current != "":
            continue
        expanded = _expand_value(raw_value, macro_values)
        if expanded == "":
            # Macro expanded to empty ⇒ NO fill; slot stays empty, falls through.
            continue
        slots[slot] = expanded
        macro_values[slot] = expanded  # later rules' {slot} macro sees the fill
        fills[slot] = expanded
        filled.append(slot)
    return filled


def apply_param_rules(
    *,
    rules_raw: Any,
    req: Any,
    slots: dict[str, str | None],
    macro_values: dict[str, Any],
    company_id: Any = None,
) -> dict[str, Any]:
    """Evaluate a campaign's parameter rules and apply fill-only assignments.

    Args:
        rules_raw: The raw ``param_rules`` value from the ``campaign:{cid}``
            Redis hash (JSON string, pre-parsed list, or absent/None).
        req: The ``ClickRequest`` (context dims: country/UA/hostname/referer).
        slots: The post-chain resolved slots from ``resolve_slots`` — MUTATED
            in place (fill-only) so buyer enrichment + the click record see the
            fills.
        macro_values: The click-time landing values dict (from
            ``router.build_macro_values``) used to expand macro assignment
            values — MUTATED in place as slots fill so later rules' ``{slot}``
            macros resolve.
        company_id: Optional tenant id, used only as the Sentry dedup key.

    Returns:
        ``{"fills": {slot: value}, "applied": [{"id": rule_id, "slots": [...]}]}``
        — ``fills`` is threaded to ``build_url`` (``param_fills`` kwarg);
        ``applied`` is the ``_param_rules`` provenance (written only when
        non-empty). Both empty ⇒ zero behaviour change.
    """
    rules = parse_param_rules(rules_raw)
    fills: dict[str, str] = {}
    applied: list[dict[str, Any]] = []
    if not rules:
        return {"fills": fills, "applied": applied}

    stable = _stable_context_dims(req)

    for rule in rules[:_MAX_RULES]:
        # Fail-open PER RULE: any error skips just this rule; traffic flows.
        try:
            if not isinstance(rule, dict):
                continue
            if rule.get("enabled") is not True:
                continue
            if not _rule_matches(rule, stable, slots):
                continue
            filled = _apply_assignments(rule, slots, macro_values, fills)
            if filled:
                applied.append({"id": rule.get("id"), "slots": filled})
        except Exception as exc:  # noqa: BLE001 — fail-open is the contract
            rid = rule.get("id") if isinstance(rule, dict) else "?"
            logger.warning("param_rule %s eval failed — skipped: %s", rid, exc)
            capture_op_msg_throttled(
                OP_PARAM_RULES, f"{company_id}:{rid}",
                f"param_rule {rid} evaluation failed — rule skipped: {exc}",
                level="warning",
            )
            continue

    return {"fills": fills, "applied": applied}
