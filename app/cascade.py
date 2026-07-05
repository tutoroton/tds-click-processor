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

import redis  # F4 — for the BASE `redis.RedisError` exception class only.

from app.action_executor import _is_positive_int, _parse_action_config
from app.telemetry import (
    OP_CRITERIA_SKIP,
    OP_FLOW_LOAD,
    OP_FLOW_READ_FAILED,
    capture_op_msg_throttled,
)

logger = logging.getLogger("tds.cascade")


__all__ = ["resolve_flow", "SCOPE_PRIORITY", "FlowReadError"]


class FlowReadError(Exception):
    """A Redis read on the flow/offer routing-resolution path FAILED persistently
    (retry-once exhausted) — F4 (GTD-R173).

    Raised by the flow-cascade reads (`_collect_candidate_ids`,
    `_load_flow_records`, `_load_target_availability`) and by the legacy
    `router.select_offer` INSTEAD of the pre-F4 silent fail-open (`return []` /
    `{}` / `None`). Caught in `router._route_via_campaign` — the one frame holding
    both `attribution` and `timing` — and turned into a RECORDED non-routed result
    (`decision_reason="flow_read_failed"`) so the click flows through the normal
    record → dedup → XADD path and is NEVER (a) dropped [it must not reach
    route()'s catch-all, which returns before the record path] nor (b)
    masqueraded as a genuine `no_flow_no_offer`. `stage` labels which read failed
    (for the Sentry counter tag); the RECORDED decision_reason is uniformly
    `flow_read_failed`. SoT: FIX-DESIGN-F4.md / FIX-PLAN.md §1.2 Layer 2/2b.
    """

    def __init__(self, stage: str = "flow") -> None:
        self.stage = stage
        super().__init__(f"flow-resolution redis read failed at stage={stage}")


async def _execute_pipe_with_retry(build_pipe, *, stage: str, dedup_key: str):
    """Execute a freshly-built Redis pipeline with RETRY-ONCE, raising
    `FlowReadError` on a persistent `redis.RedisError` — F4 (GTD-R173).

    `build_pipe` MUST return a NEW, fully-buffered pipeline on each call (a
    pipeline is single-use — it resets after `execute()`), so the retry re-issues
    the same idempotent reads (LRANGE/HGETALL/EXISTS+HGET → no double-processing).
    A transient pool-acquire / socket blip clears on the immediate retry; only a
    PERSISTENT failure raises (+ a throttled Sentry counter keyed on the offending
    entity so a hot path can't flood Sentry).

    Catches the BASE `redis.RedisError` — covers the `ConnectionError` the
    default pool raised on exhaustion AND the `TimeoutError` the new
    `socket_timeout` can now surface — never `MaxConnectionsError` (absent from
    the top-level redis 5.2.1 namespace) and never bare `Exception` (which would
    swallow a genuine logic bug as a read failure).
    """
    try:
        return await build_pipe().execute()
    except redis.RedisError:
        pass  # transient — retry once below
    try:
        return await build_pipe().execute()
    except redis.RedisError as exc:
        capture_op_msg_throttled(
            OP_FLOW_READ_FAILED, dedup_key,
            f"cascade: {stage} read failed after retry ({exc!r}) — recording "
            "flow_read_failed (was a silent fail-open pre-F4)",
            level="error", stage=stage,
        )
        raise FlowReadError(stage) from exc


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


# v2 LD-F2 — `routing_trace.criteria.rejected` bounds (D22 / §05 Tier-3).
# Compact-always: at most this many rejected flows are echoed into the
# steady-state trace (ids + the one failing criterion each, no full
# criteria JSON). Under the `X-Test-Id` diagnostic header the cap lifts to
# `_MAX_REJECTED_DIAGNOSTIC` and each entry gains its full criteria
# descriptors — the "heavy parts gated behind X-Test-Id" half of D22. The
# diagnostic cap keeps the JSON bounded so the `main.py` 4000-char defensive
# truncation never cuts mid-string even on a pathological candidate flood.
_MAX_REJECTED_COMPACT = 3
_MAX_REJECTED_DIAGNOSTIC = 25

# Dead-offer fix (2026-06-07) — sentinel availability for a target whose HASH is
# ABSENT from Redis (offer paused → desynced / target evicted / drifted). It is
# in NO allowed set, so `_filter_by_availability` floors the flow and
# `_pick_winner` re-picks a servable sibling instead of letting the dead flow win
# and poach a foreign campaign. Distinct from a PRESENT hash with no
# `availability` field (pre-076 → 'active', byte-identical for live targets).
_AVAIL_MISSING: Final[str] = "missing"


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
    diagnostic: bool = False,
) -> dict[str, Any] | None:
    """Resolve the winning flow for a click via scope cascade.

    Returning-user segmented routing (MODEL V3 — existence-driven; DARK unless
    `audience_routing`). The partition is gated upstream by `audience_routing`
    (returning routing live for the company AND the campaign has not opted out
    via `disable_returning_flows`); whether a returning flow actually EXISTS in
    scope is handled HERE by the empty-returning-pool fallthrough:
      * `audience_routing=False` (routing OFF / partition disabled) → single pass
        over the FIRST pool only. Returning-audience flows are INERT: a returning
        flow must never serve a click while the partition is off. Byte-identical
        to non-returning routing for any legacy flow set — every flow with a
        missing/unknown audience defaults to 'first', so only explicitly
        returning-tagged flows are excluded. `seen_before` is ignored.
      * `audience_routing=True` → partition the loaded flows by `flow.audience`
        (default 'first'). A `seen_before` visitor (B∪C — NOT the is_returning
        flag) evaluates the 'returning' pool FIRST and, on no match (including
        when NO returning flow exists in scope), FALLS THROUGH to the 'first'
        pool. A new visitor sees the 'first' pool only. `_pick_winner` is pure
        over its survivor list, so the two passes are independent — the
        fallthrough cannot perturb first-pool selection. This natural fallthrough
        is what makes returning routing "activate by existence": with no returning
        flow present the returning pass is a no-op and the click routes fresh.

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
    # v2 LD-F2 — Tier-3 deep-dive sinks. Allocated only when a trace dict is
    # threaded (every live click passes one; pure-unit `_pick_winner` tests do
    # not), so the no-trace path stays byte-identical. `_filter_by_criteria` /
    # `_filter_by_availability` append into these as they ALREADY walk each
    # flow — no extra pass on the hot path (per the cost invariant: capture
    # alongside, don't recompute).
    rejected_sink: list[dict[str, Any]] | None = [] if trace is not None else None
    avail_excluded_sink: list[tuple[str, str]] | None = (
        [] if trace is not None else None
    )
    if trace is not None:
        trace["candidates"] = len(deduped)
        trace["loaded"] = len(flows)
        trace["availability_excluded"] = 0
        trace["scope_walk"] = [
            st for st in SCOPE_PRIORITY if click_levels.get(st) is not None
        ]

    def _eligible(pool: list[dict[str, Any]]) -> list[dict[str, Any]]:
        crit = _filter_by_criteria(
            pool, click_attrs,
            rejected_sink=rejected_sink, diagnostic=diagnostic,
        )
        avail = _filter_by_availability(
            crit, avail_map, returning_visitor,
            excluded_sink=avail_excluded_sink,
        )
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
        # MODEL V3 — when the partition is OFF (returning routing not live for the
        # company, OR the campaign opted out via `disable_returning_flows`) a
        # returning-audience flow is INERT: it must NEVER serve a click. A
        # returning-audience flow WITHOUT returning criteria (match-all / base-dim
        # only) could otherwise win for a NEW visitor and falsely stamp the click
        # with audience_pool=returning. Restricting to the FIRST pool drops
        # returning flows from candidacy, so the cascade falls through to a
        # first/default flow (or to legacy split selection when the first pool is
        # empty). Byte-identical to non-returning routing for any legacy flow set
        # — every legacy/unknown-audience flow defaults to the first pool in
        # `_partition_audience`, so only explicitly returning-tagged flows are
        # excluded. `seen_before` is ignored under OFF.
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
    if trace is not None:
        _finalize_criteria_trace(
            trace, winner, rejected_sink, diagnostic=diagnostic,
        )
        _finalize_availability_trace(trace, avail_excluded_sink)
    return winner


def _finalize_criteria_trace(
    trace: dict[str, Any],
    winner: dict[str, Any] | None,
    rejected_sink: list[dict[str, Any]] | None,
    *,
    diagnostic: bool,
) -> None:
    """v2 LD-F2 — fold the criteria deep-dive into `trace["criteria"]` (D22).

    `winner_matched` = the winning flow's own criteria descriptors (it satisfied
    ALL of them — AND semantics). `rejected` = flows dropped by the criteria
    filter, each with the ONE failing criterion. Compact-always caps the list at
    `_MAX_REJECTED_COMPACT` (ids + reason); under `X-Test-Id` (`diagnostic`) the
    cap lifts and each entry carries its full criteria descriptors — the
    heavy/gated half of D22. Absent when nothing has criteria (match-all winner,
    no rejections) → byte-identical for a trivial single-flow campaign.
    """
    crit_obj: dict[str, Any] = {}
    if winner is not None:
        crit_obj["winner_matched"] = _criteria_descriptors(
            _parse_criteria(winner.get("criteria", "[]"))
        )
    if rejected_sink:
        cap = _MAX_REJECTED_DIAGNOSTIC if diagnostic else _MAX_REJECTED_COMPACT
        shown = rejected_sink[:cap]
        crit_obj["rejected"] = shown
        if len(rejected_sink) > len(shown):
            crit_obj["rejected_truncated"] = len(rejected_sink) - len(shown)
    if crit_obj:
        trace["criteria"] = crit_obj


def _finalize_availability_trace(
    trace: dict[str, Any],
    avail_excluded_sink: list[tuple[str, str]] | None,
) -> None:
    """v2 LD-F2 — fold the cascade flow-level availability exclusions into
    `trace["availability"]` (D22 / §05 Tier-3). Records the SPECIFIC
    `excluded_target_ids` (the int `availability_excluded` counter only ever
    held a count) + the availability state that caused the first exclusion.
    `action_executor._execute_split` MERGES its per-leg exclusions into the same
    sub-object later (shared trace dict), so split-leg drops are visible too —
    the LD-F2 evidence's blind spot. Absent when nothing was drained/closed →
    byte-identical when the availability floor excludes nothing."""
    if not avail_excluded_sink:
        return
    ids: list[int] = []
    seen: set[int] = set()
    for tid, _avail in avail_excluded_sink:
        ti = _safe_int(tid)
        if ti not in seen:
            seen.add(ti)
            ids.append(ti)
    avail = trace.setdefault("availability", {})
    existing = avail.setdefault("excluded_target_ids", [])
    for ti in ids:
        if ti not in existing:
            existing.append(ti)
    avail.setdefault("reason", avail_excluded_sink[0][1])


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

    ONE pipelined EXISTS + HGET per referenced target. Returns `{}` when no flow
    pins a target (redirect/block-only campaign) → zero extra Redis cost.

    F4 (GTD-R173) Layer 2b: a PERSISTENT Redis read failure (after retry-once) no
    longer fails open to `{}` (= all-active, which could serve a CLOSED/draining
    target under pool exhaustion) — it raises `FlowReadError`, caught in
    `router._route_via_campaign` as a RECORDED `flow_read_failed` outcome. Only a
    genuinely target-less flow set returns `{}` (early, above).

    Dead-offer fix (2026-06-07): distinguish a target whose HASH is ABSENT (its
    offer was paused → desynced, or the target was evicted/drifted) from one that
    is PRESENT but pre-migration-076 (no `availability` field). An absent HASH ⇒
    `_AVAIL_MISSING` (∉ the allowed set ⇒ the flow is floored, so `_pick_winner`
    re-picks a servable sibling at any scope instead of the dead flow winning and
    poaching a foreign campaign). A present HASH with no `availability` field ⇒
    'active' (byte-identical for live targets). A CONFIRMED-absent HASH is a
    definite exclude.
    """
    tids: set[str] = set()
    for f in flows:
        tids.update(_referenced_target_ids(f))
    if not tids:
        return {}
    ordered = list(tids)

    def _build_pipe():
        pipe = r.pipeline()
        for tid in ordered:
            pipe.exists(f"offer_target:{tid}")
            pipe.hget(f"offer_target:{tid}", "availability")
        return pipe

    # F4 (GTD-R173) Layer 2b: retry-once → FlowReadError on persistent failure,
    # NOT the pre-F4 silent `return {}` (= treat all targets active), which under
    # pool exhaustion could SERVE a CLOSED/draining target. The new socket_timeout
    # can newly surface a slow-but-alive availability read as TimeoutError; either
    # way a RAISED read is now a RECORDED honest outcome upstream, never a silent
    # serve-closed. A genuinely target-less flow set still returns {} early above.
    raw = await _execute_pipe_with_retry(
        _build_pipe, stage="availability",
        dedup_key=ordered[0] if ordered else "unknown",
    )
    out: dict[str, str] = {}
    for i, tid in enumerate(ordered):
        exists, avail = raw[i * 2], raw[i * 2 + 1]
        out[tid] = (avail or "active") if exists else _AVAIL_MISSING
    return out


def _filter_by_availability(
    flows: list[dict[str, Any]],
    avail_map: dict[str, str],
    returning_visitor: bool,
    *,
    excluded_sink: list[tuple[str, str]] | None = None,
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
        else:
            # every pinned target unavailable for the class → EXCLUDE. v2 LD-F2:
            # record the specific (target_id, availability) drops for the trace
            # (decision unchanged — this only OBSERVES the exclusion).
            if excluded_sink is not None:
                for t in tids:
                    av = avail_map.get(t, "active")
                    if av not in allowed:
                        excluded_sink.append((t, av))
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
    fetch_log: list[str] = []  # for debug logging only

    def _build_pipe():
        # Rebuilt per attempt — a pipeline is single-use (resets after execute),
        # so the retry re-issues the same idempotent LRANGEs. `fetch_log` is
        # debug-only; a duplicate append on the rare retry is inert.
        pipe = r.pipeline()
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
        return pipe

    # F4 (GTD-R173): retry-once → FlowReadError on persistent failure, NOT the
    # pre-F4 silent `return []` — that masqueraded a pool-exhaustion / socket
    # read FAILURE as "genuinely no flows" → offer-miss under load. A SUCCESSFUL
    # empty read still returns [] (genuinely-flowless campaign, byte-identical).
    results = await _execute_pipe_with_retry(
        _build_pipe, stage="candidate", dedup_key=campaign_id,
    )

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
    def _build_pipe():
        pipe = r.pipeline()
        for fid in flow_ids:
            pipe.hgetall(f"flow:{fid}")
        return pipe

    # F4 (GTD-R173): retry-once → FlowReadError on persistent failure, NOT the
    # pre-F4 silent `return []`. A SUCCESSFUL read whose rows are all empty
    # (sync drift) still yields [] → the caller's D4 drift signal fires (a
    # genuinely-empty result stays byte-identical); only a RAISED read is honest.
    rows = await _execute_pipe_with_retry(
        _build_pipe, stage="flow_load",
        dedup_key=flow_ids[0] if flow_ids else "unknown",
    )

    flows: list[dict[str, Any]] = []
    for fid, row in zip(flow_ids, rows):
        if row:
            row["_id"] = fid
            flows.append(row)
    return flows


def _parse_criteria(criteria_raw: Any) -> list[dict[str, Any]]:
    """Parse a flow's `criteria` field into a list, defensively. Returns `[]`
    on malformed JSON (callers distinguish 'empty = match-all' from 'malformed'
    earlier; this is the observability-side parse for trace descriptors)."""
    try:
        parsed = (
            json.loads(criteria_raw)
            if isinstance(criteria_raw, str)
            else criteria_raw
        )
    except (json.JSONDecodeError, TypeError):
        return []
    return parsed if isinstance(parsed, list) else []


def _criteria_descriptors(criteria: list[dict[str, Any]]) -> list[str]:
    """Compact, bounded human-readable descriptors for a flow's criteria —
    e.g. `["geo in [US,CA]", "device_type not_in [bot]"]`. Used only when
    building the trace (cold relative to routing). Per-criterion values are
    capped (count + length) so a 500-value criterion cannot bloat the JSON."""
    out: list[str] = []
    for c in criteria:
        if isinstance(c, dict):
            out.append(_format_criterion(c))
    return out


def _format_criterion(c: dict[str, Any]) -> str:
    """One criterion → compact `"<dim> <op> [<≤5 values>]"` string (bounded)."""
    dim = c.get("type", "?")
    op = c.get("op", "in")
    vals = c.get("values", []) or []
    joined = ",".join(str(v) for v in vals[:5])
    if len(joined) > 60:
        joined = joined[:57] + "..."
    op_label = "not_in" if op == "not_in" else ("in" if op == "in" else str(op))
    return f"{dim} {op_label} [{joined}]"


def _first_failing_criterion(
    criteria: list[dict[str, Any]], click_attrs: dict[str, str],
) -> dict[str, Any] | None:
    """The first criterion that does NOT hold (AND semantics), or `None` if all
    hold. SINGLE source of truth for criteria evaluation — `_criteria_match` is
    the bool wrapper, and the trace's rejected-reason builder formats the
    returned criterion. One walk = no decision/explanation drift.

    Supports `op='in'` / `op='not_in'`; per-type casing per `_CASE_PRESERVE`;
    set-valued history dims (prev_offer / prev_offer_target / prev_sub) use
    intersection. Identical decision logic to the pre-LD-F2 `_criteria_match`.
    """
    for c in criteria:
        if not isinstance(c, dict):
            return {"type": "?", "op": "?", "values": []}
        dim = c.get("type", "")
        op = c.get("op", "in")
        raw_values = c.get("values", []) or []
        click_val = click_attrs.get(dim, "")

        # CF-3 (2026-06-07): fail-CLOSED on a dim the evaluator does not know how
        # to populate. Without this, an admin-accepted-but-unevaluated dim (or a
        # legacy/future criterion type) reads click_val="" → a `not_in` exclusion
        # silently passes for ALL traffic (fail-OPEN — "block these" becomes
        # "allow all"). Treat any unknown dim as a non-match so the flow/target is
        # dropped. The known returning dims pass this gate and fail-closed via the
        # empty-value `in` test when absent (audience-gated population).
        if dim not in KNOWN_EVALUATED_DIMS:
            return c

        # P4 — set-valued dims: the click "value" is the user's HISTORY set, so
        # membership becomes intersection. ONLY a set click_val takes this
        # branch, so every base (str) dim is byte-identical to pre-P4.
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
                    return c
            elif op == "not_in":
                if hit:
                    return c
            else:
                return c
            continue

        if dim in _CASE_PRESERVE:
            values = frozenset(v for v in raw_values if isinstance(v, str))
        else:
            values = frozenset(
                v.lower() if isinstance(v, str) else v for v in raw_values
            )

        # R72 — time_of_day: the edge emits an un-padded "9" while the admin
        # validator accepts both "9" and "09"; normalize BOTH sides so a saved
        # "09" matches a 9:00 click. Scoped to time_of_day ONLY → every other
        # dim byte-identical. Kept in lockstep with `router.resolve_target`.
        if dim == "time_of_day":
            click_val = normalize_hour(click_val)
            values = frozenset(
                normalize_hour(v) if isinstance(v, str) else v for v in values
            )

        if op == "in":
            if click_val not in values:
                return c
        elif op == "not_in":
            if click_val in values:
                return c
        else:
            # Unknown operator — fail safe. Matches admin-api's CRITERION
            # validator which rejects unknown ops at write time.
            return c
    return None


def _filter_by_criteria(
    flows: list[dict[str, Any]],
    click_attrs: dict[str, str],
    *,
    rejected_sink: list[dict[str, Any]] | None = None,
    diagnostic: bool = False,
) -> list[dict[str, Any]]:
    """Per `SCOPE-CASCADE.md` step 2 — criteria match.

    Each flow carries `criteria` JSON (resolved from inline filters or
    linked traffic_target at sync time). Empty list = match-all.
    Malformed JSON = skip (don't treat as match-all — same defensive
    rule as `resolve_target` in `router.py`).

    v2 LD-F2: when `rejected_sink` is provided, every flow dropped by the
    criteria filter is appended as `{flow_id, failed}` (the one failing
    criterion); under `diagnostic` each entry also carries its full criteria
    descriptors. The SURVIVOR set is unchanged with or without the sink —
    capture is pure observation alongside the walk the filter already does.
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
            if rejected_sink is not None:
                rejected_sink.append(
                    {"flow_id": fid, "failed": "malformed criteria JSON"}
                )
            continue

        if not criteria:
            survivors.append(flow)
            continue

        failing = _first_failing_criterion(criteria, click_attrs)
        if failing is None:
            survivors.append(flow)
        elif rejected_sink is not None:
            entry: dict[str, Any] = {
                "flow_id": flow.get("_id"),
                "failed": _format_criterion(failing),
            }
            if diagnostic:
                # heavy/gated half of D22 — full criteria descriptors per
                # rejected flow (only under X-Test-Id, bounded list).
                entry["criteria"] = _criteria_descriptors(criteria)
            rejected_sink.append(entry)

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

# CF-3 (crash-test 2026-06-07): the dims the click-processor evaluator KNOWS how
# to populate. A criterion on any dim OUTSIDE this set is fail-CLOSED (the flow /
# target is dropped) instead of letting `op=not_in` pass on the empty "" value —
# which would silently turn an operator's "block these" into "allow all" for an
# unimplemented/legacy/future dim (the CF-3 fail-open). Must stay a SUPERSET of
# every base criterion type admin-api accepts (`app/common/parameters.py`
# CRITERION_TYPES) — pinned by `tests/unit/test_criteria_contract.py`.
#
#   * BASE (always populated, both matchers) — geo/region/city/os/device_type/
#     browser/language + isp_asn/time_of_day/day_of_week (the 3 added by CF-3).
#   * RETURNING (conditionally populated in the cascade path under audience
#     routing; never in the offer_target matcher per schema) — is_returning /
#     is_roaming / prev_offer / prev_offer_target / prev_sub. They are KNOWN
#     dims: when absent they correctly fail-closed via the empty-value `in` test,
#     not via this gate.
_EVALUATED_BASE_DIMS: Final[frozenset[str]] = frozenset({
    "geo", "region", "city", "os", "device_type", "browser", "language",
    "isp_asn", "time_of_day", "day_of_week",
})
_EVALUATED_RETURNING_DIMS: Final[frozenset[str]] = frozenset({
    "is_returning", "is_roaming", "prev_offer", "prev_offer_target", "prev_sub",
})
KNOWN_EVALUATED_DIMS: Final[frozenset[str]] = (
    _EVALUATED_BASE_DIMS | _EVALUATED_RETURNING_DIMS
)


def normalize_hour(value: str) -> str:
    """R72 — canonicalize a `time_of_day` value to its leading-zero-stripped form
    so a saved "09" criterion matches an un-padded "9" click (and vice-versa).
    "09"/"9"→"9", "00"/"0"→"0". Empty ("" — absent arrival_ts) and non-digit junk
    pass THROUGH unchanged, so the absent-arrival fail-closed ("" never equals a
    real hour) is preserved. zfill is FORBIDDEN here ("".zfill(2)=="00" would
    fail-OPEN at midnight). Shared by BOTH matchers (cascade
    `_first_failing_criterion` + router `resolve_target`) so they stay in
    lockstep — router already imports cascade, so no circular import."""
    return str(int(value)) if value.isdigit() else value


def _criteria_match(
    criteria: list[dict[str, Any]], click_attrs: dict[str, str],
) -> bool:
    """All criteria must hold (AND semantics) — bool wrapper over
    `_first_failing_criterion` (the SINGLE evaluation walk; see its docstring
    for op/casing/set-dim semantics + the HIGH-004 frozenset rationale)."""
    return _first_failing_criterion(criteria, click_attrs) is None


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
