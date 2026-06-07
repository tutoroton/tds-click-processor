"""Scope cascade flow resolution — Stage 2 / Vectors 2.4 + 2.5.

The click-processor's flow-aware routing primitive. Picks ONE flow from
all candidates (campaign-bound + global at the click's hierarchy levels)
per the deterministic rules in `docs/design/SCOPE-CASCADE.md`.

Why a separate module: `router.py` is already over its 250-line cap and
the cascade algorithm is testable with literal flow dicts, no Redis.
Extracting it lets us unit-test specificity / tie-break / fallback
without spinning up the whole route() pipeline.

Public API:
    `resolve_flow(...)` — async, single Redis-touching entry point.

Internal helpers (all pure, easy to reason about):
    `_collect_candidate_ids` — fetches campaign + scope flow ID lists.
    `_load_flow_records`     — batch HGETALL → list of flow HASHes.
    `_filter_by_criteria`    — applies geo/os/device-type match.
    `_pick_winner`           — implements the 3-step priority algorithm.

Algorithm summary (full spec: `docs/design/SCOPE-CASCADE.md`):

  Step 1 — collect: campaign-bound flows + scope-level flows for every
           scope the click belongs to (buyer < custom_group < team <
           department < company).
  Step 2 — criteria match: each flow's effective criteria must satisfy
           the click attrs (geo, os, device_type). Survivors keep going.
  Step 3 — specificity: the most-specific scope among survivors wins.
  Step 4 — tie-break (within same scope level):
              4a. campaign-bound beats global,
              4b. lower seq_id wins,
              4c. is_default flows are always last.
  Step 5 — fallback: if no flow matches at the deepest level, walk OUT
           one scope level and re-evaluate. Walk continues until a flow
           is found or all levels are exhausted.

The function returns the winning flow HASH (with `_id` field added) or
`None` when nothing matches at any level. Caller decides what to do:
the legacy `select_offer` fallback in `router.py` runs only when this
returns `None`, preserving backward compatibility while the migration
to flow-aware routing completes.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Final

from app.action_executor import _is_positive_int, _parse_action_config
from app.telemetry import (
    OP_CRITERIA_SKIP,
    OP_FLOW_LOAD,
    capture_op_msg_throttled,
)

logger = logging.getLogger("tds.cascade")


__all__ = ["resolve_flow", "SCOPE_PRIORITY"]


# Specificity ordering from MOST specific to LEAST specific.
# `buyer` is the personal override → highest priority. `company`
# is the org-wide catch-all → lowest priority. Lower index in the
# tuple == higher priority. Pinned by `docs/design/SCOPE-CASCADE.md`
# step 3 ("buyer < custom_group < team < department < company,
# more specific wins").
SCOPE_PRIORITY: tuple[str, ...] = (
    "buyer",
    "custom_group",
    "team",
    "department",
    "company",
)


# Defensive cap on per-click flow enumeration. Realistic campaigns have
# 1-10 flows + small org-hierarchy fan-out; a cascade that fetches
# thousands of flow HASHes in one pipeline blows the 10ms hot-path
# budget. The cap closes a DoS surface where an insider with admin
# role creates ~10k buyer-scoped flows targeting their own user_id —
# every click for that buyer would otherwise issue 10k HGETALLs.
# Truncate + Sentry warning per security audit 2026-04-28 (HIGH-003).
# Mirror of `router._MAX_SOURCES_PER_CAMPAIGN_AT_CLICK` and
# `action_executor._MAX_TARGETS_PER_OFFER_AT_CLICK` patterns.
_MAX_FLOWS_PER_CLICK = 200


async def resolve_flow(
    r,
    *,
    campaign_id: str,
    company_id: int | None,
    buyer_id: int | None,
    team_id: int | None,
    department_id: int | None,
    custom_group_id: int | None,
    click_attrs: dict[str, Any],
    seen_before: bool = False,
    audience_routing: bool = False,
    returning_visitor: bool = False,
    trace: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Resolve the winning flow for a click via scope cascade.

    P4 returning-user segmented routing (DARK unless `audience_routing`):
      * `audience_routing=False` (default) → single pass over the FIRST pool
        only. Returning-audience flows are INERT (F-LC-1, audit-2): under fresh
        / DARK a returning flow must never serve a click (D5). Byte-identical to
        pre-P4 for any legacy flow set — every flow with a missing/unknown
        audience defaults to 'first', so only explicitly returning-tagged flows
        are excluded. `seen_before` is ignored.
      * `audience_routing=True` → partition the loaded flows by `flow.audience`
        (default 'first'). A `seen_before` visitor (B∪C — NOT the is_returning
        flag) evaluates the 'returning' pool FIRST and, on no match, FALLS
        THROUGH to the 'first' pool. A new visitor sees the 'first' pool only.
        `_pick_winner` is pure over its survivor list, so the two passes are
        independent — the fallthrough cannot perturb first-pool selection.

    Args:
        r: Redis async client.
        campaign_id: Stringified PK of the campaign the click resolved to.
        company_id: Click's tenant. Required for scope keyspace; if
            `None` we cannot evaluate global flows so we fall back to
            campaign-bound flows only.
        buyer_id / team_id / department_id / custom_group_id: Resolved
            org-hierarchy chain (typically from `enrich_buyer`). Any
            level that's `None` is skipped — its scope_flows_list is
            simply not fetched.
        click_attrs: `{"geo": str, "os": str, "device_type": str}` —
            normalized for criteria matching. Caller is responsible for
            casing (geo upper, os/device lower) so this module stays
            transport-agnostic.

    Returns:
        Winning flow HASH with `_id` field, or `None` if no flow matches
        at any scope level. `None` does NOT mean "block" — it means
        "fall back to legacy split selection" per the dual-path
        contract during the Stage 2 → Stage 3 transition.

    Hot-path budget:
        ~2 Redis pipeline round-trips: one for candidate ID lists, one
        for flow HGETALLs. Within the per-click 10ms total budget per
        `architecture.md` Latency Budgets.
    """
    candidate_ids = await _collect_candidate_ids(
        r,
        campaign_id=campaign_id,
        company_id=company_id,
        buyer_id=buyer_id,
        team_id=team_id,
        department_id=department_id,
        custom_group_id=custom_group_id,
    )
    if not candidate_ids:
        return None

    # De-dupe — a flow can theoretically appear in multiple lists (e.g.
    # if a campaign-bound flow also got accidentally tagged with a
    # scope_id). HSET-based reads are idempotent so we keep the order
    # of first appearance for deterministic logging.
    seen: set[str] = set()
    deduped: list[str] = []
    for fid in candidate_ids:
        if fid not in seen:
            seen.add(fid)
            deduped.append(fid)

    # Bound the pipeline length on the hot path. With cascade lists
    # potentially union'ing 6 scopes worth of flow IDs, an insider-
    # authored flood could push deduped past the realistic ~10 flows
    # per click into thousands. Truncate deterministically (first-
    # seen order — campaign-bound flows always come first) so the
    # behaviour is stable, and emit Sentry so ops see the misconfig.
    if len(deduped) > _MAX_FLOWS_PER_CLICK:
        logger.warning(
            "cascade: candidate count %d > cap %d for campaign %s — truncating",
            len(deduped), _MAX_FLOWS_PER_CLICK, campaign_id,
        )
        try:
            import sentry_sdk
            sentry_sdk.capture_message(
                f"cascade flow count exceeds cap for campaign {campaign_id}",
                level="warning",
            )
        except ImportError:  # pragma: no cover — sentry installed in prod
            pass
        deduped = deduped[:_MAX_FLOWS_PER_CLICK]

    flows = await _load_flow_records(r, deduped)
    if not flows:
        # D4 (audit 2026-06-03) — the scope cascade found candidate flow
        # IDs but EVERY flow HASH was empty/missing (sync drift between the
        # scope list and the flow hashes). The click silently falls back to
        # legacy split selection with no signal. Surface it (throttled per
        # campaign) so an operator sees the drift instead of a quiet misroute.
        capture_op_msg_throttled(
            OP_FLOW_LOAD, campaign_id,
            f"cascade: campaign {campaign_id} had {len(deduped)} candidate "
            "flow id(s) but zero loaded as flow HASHes (sync drift) — "
            "falling back to legacy split selection",
            level="warning",
            campaign_id=campaign_id,
            candidate_count=len(deduped),
        )
        return None

    click_levels = {
        "buyer": buyer_id,
        "custom_group": custom_group_id,
        "team": team_id,
        "department": department_id,
        "company": company_id,
    }

    # v2 Phase A — availability pre-selection floor (NO-DEAD-END). Load the
    # availability of every offer_target a candidate flow would route to (one
    # pipelined read; absent / pre-076 → 'active' → no exclusion → byte-identical
    # when nothing is drained/closed). A flow whose pinned targets are ALL
    # unavailable for this click's class is dropped from survivors, so
    # `_pick_winner` naturally falls through to the next scope level. Combining
    # the criteria + availability filters keeps `_pick_winner` pure over its
    # survivor list (purity preserved — the two cascade passes stay independent).
    avail_map = await _load_target_availability(r, flows)
    if trace is not None:
        trace["candidates"] = len(deduped)
        trace["loaded"] = len(flows)
        trace["availability_excluded"] = 0
        trace["scope_walk"] = [
            st for st in SCOPE_PRIORITY if click_levels.get(st) is not None
        ]

    def _eligible(pool: list[dict[str, Any]]) -> list[dict[str, Any]]:
        crit = _filter_by_criteria(pool, click_attrs)
        avail = _filter_by_availability(crit, avail_map, returning_visitor)
        if trace is not None:
            trace["availability_excluded"] += len(crit) - len(avail)
        return avail

    # `_pick_winner([])` returns None, so each branch collapses to a single
    # winner expression — the audience partition + first-pool fallthrough is
    # preserved (returning pool first for a seen_before visitor, else first
    # pool).
    winner: dict[str, Any] | None
    returning_flows, first_flows = _partition_audience(flows)
    if not audience_routing:
        # F-LC-1 (audit-2 MED) — under fresh / DARK (audience routing OFF) a
        # returning-audience flow is INERT: it must NEVER serve a click (D5).
        # Pre-fix this branch picked over ALL flows, so a returning-audience
        # flow WITHOUT returning criteria (match-all / base-dim only) could win
        # for a NEW visitor and falsely stamp the click with
        # decision_reason=override_returning_flow + audience_pool=returning.
        # Restricting to the FIRST pool drops returning flows from candidacy, so
        # the cascade falls through to a first/default flow (or to legacy split
        # selection when the first pool is empty). Byte-identical for any pre-P4
        # flow set — every legacy/unknown-audience flow defaults to the first
        # pool in `_partition_audience`, so only explicitly returning-tagged
        # flows are excluded. `seen_before` is ignored under OFF.
        winner = _pick_winner(_eligible(first_flows), click_levels)
    else:
        winner = None
        if seen_before:
            winner = _pick_winner(_eligible(returning_flows), click_levels)
        if winner is None:
            winner = _pick_winner(_eligible(first_flows), click_levels)

    if trace is not None and winner is not None:
        trace["winning_flow_id"] = winner.get("_id")
        trace["winning_scope_type"] = winner.get("scope_type")
        trace["winning_scope_id"] = _safe_int(winner.get("scope_id"))
        trace["audience_pool"] = winner.get("audience") or "first"
    return winner


def _partition_audience(
    flows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split loaded flows into (returning, first) by `flow.audience`. A missing
    or unknown audience defaults to 'first' — so every legacy flow (and any
    flow synced before P4) is a first-flow, guaranteeing zero-regress."""
    returning: list[dict[str, Any]] = []
    first: list[dict[str, Any]] = []
    for f in flows:
        if (f.get("audience") or "first") == "returning":
            returning.append(f)
        else:
            first.append(f)
    return returning, first


def _referenced_target_ids(flow: dict[str, Any]) -> list[str]:
    """The offer_target ids a flow's action would PIN-route to.

    offer → `action_config.target_id` (when pinned); split → each entry's
    `target_id`. redirect/block carry no target; an offer/split WITHOUT a
    pinned target_id resolves to the offer's default target at action time —
    not knowable cheaply here, so it returns [] and is NOT subject to the
    pre-selection availability floor (the terminal fallback is its safety net).
    Reuses action_executor's canonical config parser + positive-int guard so
    the pre-selection view of a flow's targets cannot drift from execution.
    """
    action_type = flow.get("action_type", "")
    if action_type not in ("offer", "split"):
        return []
    config = _parse_action_config(flow.get("action_config", "{}"))
    out: list[str] = []
    if action_type == "offer":
        tid = config.get("target_id")
        if _is_positive_int(tid):
            out.append(str(tid))
    else:  # split
        for entry in config.get("offers") or []:
            if isinstance(entry, dict) and _is_positive_int(entry.get("target_id")):
                out.append(str(entry["target_id"]))
    return out


async def _load_target_availability(
    r, flows: list[dict[str, Any]],
) -> dict[str, str]:
    """`{target_id: availability}` for every PINNED target across the flows.

    ONE pipelined HGET per referenced target. Returns `{}` when no flow pins a
    target (redirect/block-only campaign) → zero extra Redis cost. FAIL-OPEN:
    any Redis error → `{}` → the availability filter excludes nothing (a click
    is NEVER lost because availability state is unreadable). A target HASH
    missing the `availability` field (synced before migration 076) defaults to
    'active' in the filter.
    """
    tids: set[str] = set()
    for f in flows:
        tids.update(_referenced_target_ids(f))
    if not tids:
        return {}
    ordered = list(tids)
    pipe = r.pipeline()
    for tid in ordered:
        pipe.hget(f"offer_target:{tid}", "availability")
    try:
        vals = await pipe.execute()
    except Exception as exc:  # pragma: no cover — fail-open to all-active
        logger.warning(
            "cascade: availability load failed (%s) — treating all targets "
            "active (fail-open)", exc,
        )
        return {}
    return {tid: (val or "active") for tid, val in zip(ordered, vals)}


def _filter_by_availability(
    flows: list[dict[str, Any]],
    avail_map: dict[str, str],
    returning_visitor: bool,
) -> list[dict[str, Any]]:
    """Pre-selection availability floor (v2 Phase A, plan §4 step 7).

    A flow is kept only if it has ≥1 PINNED target available for the click's
    class — NEW traffic needs an 'active' target; RETURNING traffic accepts
    'active' OR 'draining'; 'closed' never counts. A flow with no pinned target
    is kept (resolved at action time). Empty `avail_map` (nothing pinned, or a
    fail-open read) → no exclusion → byte-identical when nothing is
    drained/closed.

    Split semantics: a multi-target (split) flow is KEPT if ANY of its legs has
    an available target for the class — this is a coarse PRE-selection gate
    ("can this flow serve the click at all?"). Per-leg availability (e.g. one
    drained leg among several) is enforced at EXECUTION time by the action
    executor's weighted pick, not here; this floor only excludes a flow whose
    EVERY pinned leg is unavailable.

    Excluding a dead-target flow shrinks the survivor set, so `_pick_winner`
    falls through to the next scope level — the NO-DEAD-END behaviour, with no
    new control flow in the winner picker (purity preserved).
    """
    if not avail_map:
        return flows
    allowed = {"active", "draining"} if returning_visitor else {"active"}
    survivors: list[dict[str, Any]] = []
    for f in flows:
        tids = _referenced_target_ids(f)
        if not tids:
            survivors.append(f)  # no pinned target → not floored here
            continue
        if any(avail_map.get(t, "active") in allowed for t in tids):
            survivors.append(f)
        # else: every pinned target unavailable for the class → EXCLUDE
    return survivors


async def _collect_candidate_ids(
    r,
    *,
    campaign_id: str,
    company_id: int | None,
    buyer_id: int | None,
    team_id: int | None,
    department_id: int | None,
    custom_group_id: int | None,
) -> list[str]:
    """Single pipeline batch — fetch all relevant flow ID lists.

    Returns concatenated flow IDs from campaign-bound + each present
    scope level. Order follows fetch order: campaign first, then
    buyer / custom_group / team / department / company. Caller de-dupes.
    """
    pipe = r.pipeline()
    fetch_log: list[str] = []  # for debug logging only

    pipe.lrange(f"campaign:{campaign_id}:flows", 0, -1)
    fetch_log.append(f"campaign:{campaign_id}")

    if company_id is not None:
        # Each scope level gets ONE LRANGE. Skip levels with no ID since
        # `flows:scope:{company}:{type}:None` is meaningless.
        scope_targets = (
            ("buyer", buyer_id),
            ("custom_group", custom_group_id),
            ("team", team_id),
            ("department", department_id),
            ("company", company_id),
        )
        for scope_type, scope_id in scope_targets:
            if scope_id is not None:
                pipe.lrange(
                    f"flows:scope:{company_id}:{scope_type}:{scope_id}",
                    0, -1,
                )
                fetch_log.append(f"scope:{scope_type}:{scope_id}")

    try:
        results = await pipe.execute()
    except Exception as exc:  # pragma: no cover — Redis errors caught by route()
        logger.warning("cascade: candidate fetch failed: %s", exc)
        return []

    out: list[str] = []
    for items in results:
        if items:
            out.extend(items)
    return out


async def _load_flow_records(r, flow_ids: list[str]) -> list[dict[str, Any]]:
    """Single pipeline batch — HGETALL for every candidate flow.

    Returns flow HASHes with `_id` field added so winner-picking can
    surface the flow ID without re-reading. Empty/missing rows are
    skipped (sync drift between scope list and flow hash).
    """
    pipe = r.pipeline()
    for fid in flow_ids:
        pipe.hgetall(f"flow:{fid}")
    try:
        rows = await pipe.execute()
    except Exception as exc:  # pragma: no cover
        logger.warning("cascade: flow load failed: %s", exc)
        return []

    flows: list[dict[str, Any]] = []
    for fid, row in zip(flow_ids, rows):
        if row:
            row["_id"] = fid
            flows.append(row)
    return flows


def _filter_by_criteria(
    flows: list[dict[str, Any]],
    click_attrs: dict[str, str],
) -> list[dict[str, Any]]:
    """Per `SCOPE-CASCADE.md` step 2 — criteria match.

    Each flow carries `criteria` JSON (resolved from inline filters or
    linked traffic_target at sync time). Empty list = match-all.
    Malformed JSON = skip (don't treat as match-all — same defensive
    rule as `resolve_target` in `router.py`).
    """
    survivors: list[dict[str, Any]] = []
    for flow in flows:
        criteria_raw = flow.get("criteria", "[]")
        try:
            criteria = (
                json.loads(criteria_raw)
                if isinstance(criteria_raw, str)
                else criteria_raw
            )
        except (json.JSONDecodeError, TypeError):
            fid = flow.get("_id")
            logger.warning(
                "cascade: malformed criteria for flow %s — skipping",
                fid,
            )
            # B12 (audit 2026-06-03) — a flow with corrupt criteria JSON is
            # silently dropped from candidacy on EVERY click → the click
            # may route elsewhere with no signal. Surface it (throttled per
            # flow id so one bad flow doesn't flood Sentry).
            capture_op_msg_throttled(
                OP_CRITERIA_SKIP, fid,
                f"cascade: flow {fid} skipped — malformed criteria JSON "
                "(routing decision excludes it until the flow is re-saved)",
                level="warning",
                flow_id=fid,
            )
            continue

        if not criteria:
            survivors.append(flow)
            continue

        if _criteria_match(criteria, click_attrs):
            survivors.append(flow)

    return survivors


# F.17 (2026-05-03): per-type casing strategy. The 4 dimensions in
# this set carry their value verbatim — admin-api validates them in
# the same casing the click-processor emits, so lowercasing here would
# break the match. Everything else (currently `os`, `device_type`,
# `city`) is lowercased on both sides.
#
#   geo       — ISO 3166-1 uppercase ("US"), enforced by both ends
#   region    — CF / GeoNames human name ("California", "Київська область")
#   browser   — device_detector canonical Title Case ("Samsung Browser")
#   language  — BCP47 strict casing ("en-US", not "en-us")
#
# Mirror this set in `router.py`'s legacy `resolve_target` matcher
# (it walks an offer-target list directly when cascade misses).
# Drift between the two matchers is a silent foot-gun.
_CASE_PRESERVE: Final[frozenset[str]] = frozenset({
    "geo", "region", "browser", "language",
})


def _criteria_match(
    criteria: list[dict[str, Any]], click_attrs: dict[str, str],
) -> bool:
    """All criteria must hold (AND semantics).

    Supports `op='in'` and `op='not_in'`. Per-type casing per
    `_CASE_PRESERVE` above — `geo` / `region` / `browser` / `language`
    keep their value verbatim, all other dims lowercase both sides.

    Membership uses a `frozenset` for O(1) check instead of `O(n)`
    list scan. Admin-api caps `values` at 500 strings per criterion
    (`traffic_targets/schemas.py`), so the constant-factor win
    matters at the upper bound — 50 flows × 20 criteria × 500 values
    via list scan = 500k ops/click vs `~1k` set lookups (security
    audit 2026-04-28 HIGH-004 mitigation).
    """
    for c in criteria:
        if not isinstance(c, dict):
            return False
        dim = c.get("type", "")
        op = c.get("op", "in")
        raw_values = c.get("values", []) or []
        click_val = click_attrs.get(dim, "")

        # P4 — set-valued dims (prev_offer / prev_offer_target / prev_sub): the
        # click "value" is the user's HISTORY set, so membership becomes
        # intersection. `in` = history hit ANY criterion value; `not_in` = none.
        # ONLY a set click_val takes this branch, so every base (str) dim — and
        # the offer_target inline matcher, which never carries set values — is
        # byte-identical to pre-P4.
        if isinstance(click_val, (set, frozenset)):
            cvals = frozenset(
                v.lower() if isinstance(v, str) else v for v in raw_values
            )
            hist = frozenset(
                x.lower() if isinstance(x, str) else x for x in click_val
            )
            hit = bool(hist & cvals)
            if op == "in":
                if not hit:
                    return False
            elif op == "not_in":
                if hit:
                    return False
            else:
                return False
            continue

        if dim in _CASE_PRESERVE:
            values = frozenset(v for v in raw_values if isinstance(v, str))
        else:
            values = frozenset(
                v.lower() if isinstance(v, str) else v for v in raw_values
            )

        if op == "in":
            if click_val not in values:
                return False
        elif op == "not_in":
            if click_val in values:
                return False
        else:
            # Unknown operator — fail safe. Matches admin-api's CRITERION
            # validator which rejects unknown ops at write time.
            return False
    return True


def _pick_winner(
    survivors: list[dict[str, Any]],
    click_levels: dict[str, int | None],
) -> dict[str, Any] | None:
    """Per `SCOPE-CASCADE.md` steps 3-5 — specificity + tie-break + fallback.

    Algorithm:
      Walk SCOPE_PRIORITY from most to least specific. For each level
      where the click has a hierarchy ID, gather flows whose
      (scope_type, scope_id) matches. If non-empty, apply tie-break
      and return. Otherwise continue to the next level.

      `company` is special — every click belongs to a company scope, so
      we always evaluate it (the final catch-all).

    Tie-break order (per step 4):
      a. Campaign-bound flow (campaign_id != "0") beats global.
      b. Lower `seq_id` wins.
      c. `is_default=True` flows are always last.
    """
    for scope_type in SCOPE_PRIORITY:
        click_id = click_levels.get(scope_type)
        if click_id is None:
            continue

        bucket = [
            f for f in survivors
            if f.get("scope_type") == scope_type
            and _safe_int(f.get("scope_id")) == click_id
        ]
        if not bucket:
            continue

        # Sort key: (is_default ASC, campaign_bound DESC, seq_id ASC).
        # Python sorts ascending by default; we negate booleans into
        # 0/1 so the desired ordering falls out naturally.
        bucket.sort(key=_winner_sort_key)
        return bucket[0]

    return None


def _winner_sort_key(flow: dict[str, Any]) -> tuple[int, int, int]:
    """Sort key implementing tie-break rule (step 4).

    Returns `(default_bucket, bound_bucket, seq_id)`:
      - default_bucket: 0 if non-default, 1 if is_default — defaults LAST.
      - bound_bucket:   0 if campaign-bound, 1 if global — bound FIRST.
      - seq_id:         lower wins (oldest-by-creation; user-visible).

    All ascending; first item after sort wins.
    """
    is_default = flow.get("is_default") == "1"
    campaign_id = flow.get("campaign_id") or "0"
    is_global = campaign_id == "0"
    seq_id = _safe_int(flow.get("seq_id"))
    return (1 if is_default else 0, 1 if is_global else 0, seq_id)


def _safe_int(value: Any, default: int = 0) -> int:
    """Parse Redis-returned strings without exploding on bad data."""
    if value is None:
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default
