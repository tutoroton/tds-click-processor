"""Routing engine — the core of click-processor.

Reads campaign/offer/rule data from Redis, evaluates targeting conditions,
selects destination URL. All lookups are Redis-only, no SQL.

Every stage is timed to millisecond precision for observability.

Stage 2 / Vector 2.8 — `build_url()` resolves macros via the merged
source∪campaign mapping chain (per `docs/design/PARAMETER-SYSTEM.md`)
and emits URL-safe output through `safe_substitute()` (`macros.py`).
Incoming GET keys must be aliased to a canonical slot via Source
`param_mappings` or Campaign `default_param_mappings` to land in the
redirect URL.

Stage 2 / Vectors 2.4 + 2.5 — flow-aware routing via scope cascade
resolution (`docs/design/SCOPE-CASCADE.md`). After a winner campaign is
picked, the click-processor consults `flow:{id}` candidates from
`campaign:{cid}:flows` + `flows:scope:{...}` lists, applies criteria
matching, and walks the scope hierarchy (buyer < custom_group < team
< department < company) to pick exactly one flow. The chosen flow's
`action_type` (redirect / offer / split / block) drives URL emission
via `app.action_executor`. Legacy `select_offer` is preserved as a
fallback when no flow matches at any scope level — this makes the
migration safe for campaigns whose flows haven't yet been authored.
"""

import functools
import json
import logging
import math
import random
import time
from datetime import datetime, timezone
from typing import Any, Final, NamedTuple

import sentry_sdk
from app import action_executor, cascade, identity, sticky
from app.config import settings
from app.diag import get_test_id
from app.enrichment import enrich_buyer
from app.macros import safe_substitute
from app.models import ClickRequest
from app.redis_client import get_redis
from app.resolution import BINDING_SELECTOR_KEY, parse_param_mappings, resolve_slots
from app.telemetry import OP_IDENTITY, capture_op_msg_throttled
from app.ua_parser import parse_ua

logger = logging.getLogger("tds.router")


def coerce_cost(raw: Any) -> float | None:
    """Strict numeric coercion for the advertiser-supplied ``?cost=`` param.

    Returns a non-negative, finite float, or ``None`` when the value is
    absent / non-numeric / negative / NaN / ±inf.

    A2 (audit 2026-06-03): ``?cost=`` is attacker-controllable raw GET
    input. Pre-fix, both the stored ``cost`` column (``main._phase3_
    attribution_fields``) and the ``{cost}`` macro (``build_url`` below)
    read it verbatim via ``get("cost") or 0``, so ``?cost=abc'inj`` put
    arbitrary text into the click record (a numeric CH column — risking
    a collector insert failure, the C1 poison-pill class) and could be
    reflected into the redirect. This gate mirrors the ``isdigit()``
    discipline ``enrichment.py`` applies to ``buyer_id``: validate first,
    drop on fail — never propagate unvalidated text.

    Callers pick the fallback: the stored column uses ``coerce_cost(raw)
    or 0`` (numeric 0 on miss); the macro uses the ``None`` directly so
    ``safe_substitute`` collapses the ``{cost}`` placeholder.
    """
    if raw is None or raw == "":
        return None
    try:
        val = float(raw)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(val) or val < 0:
        return None
    return val


def safe_int(value, default=0):
    """Convert to int safely — never crash on bad Redis data."""
    if value is None:
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def _ms_since(start: float) -> float:
    """Milliseconds elapsed since start (perf_counter)."""
    return round((time.perf_counter() - start) * 1000, 2)


async def route(req: ClickRequest) -> dict | None:
    """Find matching campaign + offer for this click.

    Returns one of four shapes:
      - Happy path: `{"url": str, "campaign_id": str, "offer_id": str,
        "timing": dict}` — caller emits 302 to `url`.
      - Block sentinel: `{"url": None, "campaign_id": str, "offer_id":
        None, "timing": dict, "blocked": True}` — caller emits 404
        (or worker may serve a block page). Distinguished by
        `result.get("blocked") is True` OR `result.get("url") is None`.
      - Non-routed sentinel (G2, 2026-06-02): `{"url": None,
        "campaign_id": str, "non_routed": True, "attribution": dict,
        "routing_status": str, ...}` — a campaign matched but the click
        could not be routed (capped / no flow + no legacy offer). Caller
        records it to the admin-configured fallback URL WITH the resolved
        attribution so campaign(+effective_source) hardcoded defaults
        persist (instead of every slot column being NULL).
      - `None` — no campaign matched at all; caller emits the worker's
        default fallback (typically 404 with a generic page). No
        attribution exists because no campaign anchored the click.

    Timing dict contains ms-precision breakdown of every routing stage,
    plus a `route_via` tag (`flow_cascade`, `flow_cascade_block`, or
    `legacy_split`) for ops drill-down.
    """
    t_start = time.perf_counter()
    timing = {}

    r = await get_redis()

    # Stage 0: Domain-based campaign resolution (highest priority)
    t0 = time.perf_counter()
    resolution = await resolve_domain_campaign(r, req)
    timing["domain_resolve_ms"] = _ms_since(t0)

    # §6 (F.30 security): an unmatched subdomain of a wildcard-enabled
    # base fails closed — it must NOT inherit the base's binding nor
    # fall through to geo targeting. The `*.{base}` wildcard DNS (F.30
    # A.1) makes arbitrary subdomains reachable; pre-F.30 they were
    # NXDOMAIN. A block sentinel (404) is the only safe disposition.
    # See `resolve_domain_campaign` + plan §6.
    if resolution.blocked:
        timing["domain_matched"] = False
        timing["route_total_ms"] = _ms_since(t_start)
        timing["result"] = "blocked_unmatched_subdomain"
        return {
            "url": None,
            "campaign_id": None,
            "offer_id": None,
            "binding_id": 0,
            "binding_alias": None,
            "timing": timing,
            "blocked": True,
        }

    domain_campaign_id = resolution.campaign_id
    if domain_campaign_id:
        # Domain resolved — skip geo targeting, go straight to flow cascade.
        timing["domain_matched"] = True
        timing["campaign_source"] = "domain"

        t0 = time.perf_counter()
        campaign = await r.hgetall(f"campaign:{domain_campaign_id}")
        timing["campaign_fetch_ms"] = _ms_since(t0)

        if campaign:
            campaign["_id"] = domain_campaign_id

            # F.31 — thread the resolved binding's id + alias so the click
            # record can attribute analytics to the exact binding the
            # click arrived through.
            routed = await _route_via_campaign(
                r, campaign, domain_campaign_id, req, timing,
                result_label="domain_matched",
                binding_id=resolution.binding_id,
                binding_alias=resolution.binding_alias,
                # Domain match is NOT terminal — a no-route outcome here
                # falls through to geo targeting (return None), not a
                # non-routed fallback.
                fall_through_on_no_route=True,
            )
            if routed is not None:
                # LA-F1 — the domain campaign SERVED (did not fall through), so
                # commit its side-effect-free identity resolution now (mint +
                # campaigns-seen for THIS serving campaign). No-op when the
                # resolver is off / nothing was deferred.
                await _commit_deferred_identity(routed.get("attribution"))
                return routed

        # Domain matched but no usable routing path — fall through to geo targeting.
        timing["domain_fallthrough"] = True

    # Stage 1: UA parsing (cached, should be <0.1ms on cache hit)
    t0 = time.perf_counter()
    device_type = parse_device_type(req.user_agent)
    os_name = parse_os(req.user_agent)
    timing["ua_parse_ms"] = _ms_since(t0)

    # Stage 2: Geo/device/OS set lookup (single pipeline round-trip)
    t0 = time.perf_counter()
    pipe = r.pipeline()
    pipe.smembers(f"geo:{req.country}")
    pipe.smembers(f"device:{device_type}")
    pipe.smembers(f"os:{os_name}")
    pipe.smembers("campaigns:active")
    results = await pipe.execute()
    timing["geo_lookup_ms"] = _ms_since(t0)

    geo_ids = results[0] or set()
    device_ids = results[1] or set()
    os_ids = results[2] or set()
    active_ids = results[3] or set()

    if not active_ids:
        timing["route_total_ms"] = _ms_since(t_start)
        timing["result"] = "no_active_campaigns"
        return None

    active_list = sorted(active_ids)

    # Stage 3: Targeting flags check (batched pipeline)
    t0 = time.perf_counter()
    pipe = r.pipeline()
    for cid in active_list:
        pipe.exists(f"campaign:{cid}:has_geo")
        pipe.exists(f"campaign:{cid}:has_device")
        pipe.exists(f"campaign:{cid}:has_os")
    exists_results = await pipe.execute()

    candidates = []
    for i, cid in enumerate(active_list):
        has_geo = exists_results[i * 3]
        has_device = exists_results[i * 3 + 1]
        has_os = exists_results[i * 3 + 2]
        if ((cid in geo_ids) or (not has_geo)) and \
           ((cid in device_ids) or (not has_device)) and \
           ((cid in os_ids) or (not has_os)):
            candidates.append(cid)
    timing["targeting_ms"] = _ms_since(t0)
    timing["candidates_count"] = len(candidates)

    if not candidates:
        timing["route_total_ms"] = _ms_since(t_start)
        timing["result"] = "no_candidates"
        return None

    # Stage 4: Fetch campaign details
    t0 = time.perf_counter()
    pipe = r.pipeline()
    for cid in candidates:
        pipe.hgetall(f"campaign:{cid}")
    campaigns = await pipe.execute()
    timing["campaign_fetch_ms"] = _ms_since(t0)

    # Stage 5: build the eligible set — every candidate campaign whose
    # HASH loaded. The campaign-level click-cap / frequency filter was
    # removed in returning-users v2 Phase 0: the cap columns never existed
    # on the live DB (migration 002 is a no-op on the bootstrapped schema),
    # so the engine always read None→0→disabled and the filter was dead
    # code. Removing it is behaviour-preserving on staging/prod.
    eligible = []
    for i, campaign in enumerate(campaigns):
        if not campaign:
            continue
        campaign["_id"] = candidates[i]
        eligible.append(campaign)
    timing["eligible_count"] = len(eligible)

    if not eligible:
        # No candidate campaign HASH loaded (index pointed at missing
        # keys) — nothing to route. Matches the pre-v2 terminal: the old
        # all-capped fallback resolved to `_select_winner([]) → None`
        # here too, since with caps gone `eligible` == all non-None
        # campaigns.
        timing["route_total_ms"] = _ms_since(t_start)
        timing["result"] = "no_candidates"
        return None

    # Stage 6: Campaign selection (priority + weight)
    t0 = time.perf_counter()
    winner = _select_winner(eligible)
    timing["selection_ms"] = _ms_since(t0)

    # Stages 6.5-9: flow cascade → action execution → counter increment.
    routed = await _route_via_campaign(
        r, winner, winner["_id"], req, timing, result_label="matched",
    )
    if routed is not None:
        return routed

    # No routing path found — defensive only. `_route_via_campaign` with
    # `fall_through_on_no_route=False` (the default, geo-branch context)
    # now ALWAYS returns a routed result or the G2 non-routed sentinel,
    # never bare None, so this line is unreachable in practice.
    timing["route_total_ms"] = _ms_since(t_start)
    timing["result"] = "no_offer"
    return None


def _select_winner(campaigns: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Pick the routing winner from a list of campaign HASHes.

    Top-priority bucket, then weighted-random within it (Stage 6
    selection). Returns `None` for an empty list.
    """
    if not campaigns:
        return None
    top_priority = max(safe_int(c.get("priority"), 0) for c in campaigns)
    top = [c for c in campaigns if safe_int(c.get("priority"), 0) == top_priority]
    return weighted_select(top)


def _attribution_buyer_chain(attribution: dict[str, Any]) -> dict[str, int | None]:
    """Re-project the org-hierarchy chain out of a built `attribution`.

    `_build_campaign_attribution` already resolved the buyer chain and
    folded it into `attribution`; the cascade needs it back as the
    `{buyer_id, team_id, ...}` shape `cascade.resolve_flow` expects. Pure
    dict re-projection — no Redis.
    """
    return {
        "buyer_id": attribution["buyer_id"],
        "team_id": attribution["team_id"],
        "department_id": attribution["department_id"],
        "custom_group_id": attribution["custom_group_id"],
        "company_id": attribution["company_id"],
    }


async def _build_campaign_attribution(
    r,
    campaign: dict[str, Any],
    campaign_id: str,
    req: ClickRequest,
    *,
    commit_identity: bool = True,
) -> tuple[list[dict[str, Any]] | None, list[dict[str, Any]] | None, dict[str, Any]]:
    """Resolve the click's full attribution for one matched campaign.

    Returns `(source_mappings, campaign_mappings, attribution)` where:
      - `source_mappings` is the EFFECTIVE source layer (per-link
        override or source global) and `campaign_mappings` the campaign
        layer — both threaded into `build_url` / the cascade.
      - `attribution` is the by-reference dict the click record reads
        (org chain + source_id + resolved `slots` + `extras`). The
        cascade later mutates it with flow / target ids.

    Cost: one `_fetch_resolution_context` (source HASH + effective-source
    HGET) + one `_resolve_buyer_chain` HGETALL — the same reads the
    matched path always performed. Extracted (G2, 2026-06-02) so the
    non-routed paths (capped / no-flow / all-capped) can persist the
    campaign(+effective_source) hardcoded defaults instead of dropping
    every slot column to NULL.
    """
    source_mappings, campaign_mappings, source_id, source_trusted = (
        await _fetch_resolution_context(
            r, campaign_id, campaign, req.query_params or {},
        )
    )

    # `slots` is pure-Python over the source∪campaign param_mappings
    # (the source HASH was already fetched in `_fetch_resolution_context`)
    # — no extra Redis op (03 §5 open-Q #3 / 03 §4 hot-path guardrail).
    slots, slot_extras = resolve_slots(
        query_params=req.query_params or {},
        source_mappings=source_mappings,
        campaign_mappings=campaign_mappings,
    )
    buyer_chain = await _resolve_buyer_chain(r, slots, campaign)

    # `company_id` ALWAYS from the campaign anchor (never buyer) — the
    # chain already enforces this. Reserved slots + source_id ride along;
    # the cascade fills in flow/target ids on the routed path.
    #
    # `extras` — the canonical resolver's authoritative "unmapped keys"
    # set: every incoming query param NOT bound to a reserved or sub slot
    # (by canonical name or source/campaign alias). Threaded up so the
    # click record's `extra_params` is sourced from it (C-1, 2026-06-02)
    # instead of a hand-rolled legacy-key filter — a param that landed in
    # a dedicated column is therefore NEVER duplicated into extras.
    attribution: dict[str, Any] = {
        "buyer_id": buyer_chain["buyer_id"],
        "team_id": buyer_chain["team_id"],
        "department_id": buyer_chain["department_id"],
        "custom_group_id": buyer_chain["custom_group_id"],
        "company_id": buyer_chain["company_id"],
        "source_id": source_id,
        "slots": slots,
        "extras": slot_extras,
    }

    # Returning-user identity (P2, 2026-06-05) — DARK + fail-open.
    #
    # Gate #1 (no-IO-when-OFF): the cached `settings.returning_resolver_enabled`
    # bool is checked FIRST — OFF ⇒ instant skip, zero identity Redis I/O,
    # `attribution` carries no uid/flag keys, so `_phase3_attribution_fields`
    # falls back to the legacy is_unique/is_returning computation → the click
    # record is byte-identical to pre-P2. The per-company flag rides on the
    # already-in-hand campaign HASH (free; admin sync wires it in P4) so a
    # tenant opts in individually (default closed).
    #
    # Gate V1 (fail-open): the WHOLE resolver call is wrapped — ANY exception
    # degrades to legacy (no keys stamped) and the click still routes. The
    # resolver never raises out of here, never 5xx, never loses a click.
    if settings.returning_resolver_enabled and _company_returning_enabled(campaign):
        try:
            # Read previous-visit history (for prev_* matching) only when
            # segmented routing is ALSO live for this company (env AND
            # per-company) — otherwise RT#2 stays a single SISMEMBER.
            with_history = (
                settings.returning_routing_enabled
                and _company_routing_enabled(campaign)
            )
            ident = await identity.resolve_and_stamp(
                company_id=buyer_chain["company_id"],
                funnel_user_id=slots.get("funnel_user_id"),
                visitor_id=req.visitor_id,
                campaign_id=campaign_id,
                source_trusted=source_trusted,
                with_history=with_history,
                # P2 dual-accept: signed `_tds_id` cookie value (None until the
                # worker forwards it in P4) → in-process HMAC recognition,
                # skipping the `vid → uid` Redis GET. Dark when absent.
                identity_token=req.identity_token,
                # LA-F1 (2026-06-07): when this campaign is only being EVALUATED
                # and may FALL THROUGH (the domain branch), resolve
                # SIDE-EFFECT-FREE — no mint, no persist. The mint + campaigns-seen
                # write must happen ONLY for the campaign that actually serves the
                # click. The serving path commits via `_commit_deferred_identity`.
                commit=commit_identity,
            )
            # LA-F1 — stash the args needed to commit the deferred writes once the
            # campaign is confirmed to serve. Popped (never reaches the click
            # record) by `_commit_deferred_identity`; harmlessly discarded if the
            # campaign falls through (this whole attribution dict is dropped).
            if not commit_identity:
                attribution["_identity_deferred"] = {
                    "result": ident,
                    "company_id": buyer_chain["company_id"],
                    "funnel_user_id": slots.get("funnel_user_id"),
                    "visitor_id": req.visitor_id,
                    "campaign_id": campaign_id,
                    "source_trusted": source_trusted,
                }
            attribution["uid"] = ident.uid
            attribution["is_unique"] = ident.is_unique
            # v2 R — is_returning is now CAMPAIGN-relative; is_roaming = seen
            # before but a DIFFERENT campaign (mutually exclusive).
            attribution["is_returning"] = ident.is_returning
            attribution["is_roaming"] = ident.is_roaming
            # Identity provenance (v2 R) — which signal resolved the uid, and
            # whether two signals disagreed (log-not-merge).
            attribution["signal_tier"] = ident.signal_tier
            attribution["identity_conflict"] = ident.identity_conflict
            # P4 — previous-visit history sets for prev_* criteria matching
            # (empty unless segmented routing is ON and the user is returning).
            attribution["prev_offers"] = ident.prev_offers
            attribution["prev_targets"] = ident.prev_targets
            attribution["prev_subs"] = ident.prev_subs
            # P3 mint — the company_id + recent campaigns-seen set the /decide
            # handler needs to re-stamp the signed `_tds_id` cookie (the current
            # campaign is unioned in at the mint call site).
            attribution["company_id"] = buyer_chain["company_id"]
            attribution["campaigns_seen"] = ident.campaigns_seen
        except Exception as e:  # fail-open — never fail the click
            capture_op_msg_throttled(
                OP_IDENTITY, buyer_chain["company_id"],
                f"returning-user resolver failed; degraded to legacy flags: {e}",
                level="warning",
            )
            logger.warning("identity resolver failed — fail-open to legacy: %s", e)

    return source_mappings, campaign_mappings, attribution


async def _commit_deferred_identity(attribution: dict[str, Any] | None) -> None:
    """LA-F1 (2026-06-07) — commit a side-effect-free identity resolution once the
    campaign is confirmed to SERVE this click.

    The domain branch resolves identity with ``commit=False`` (no mint, no
    persist) because that campaign may FALL THROUGH to geo targeting; a campaign
    that routes nowhere must never mint a uid nor write itself into campaigns-seen
    (that poisoned a brand-new visitor's identity — audit-2 LA-F1). When the
    campaign DOES serve, the router calls this to perform the deferred mint +
    persist exactly once, for the serving campaign, and re-stamps the resolved
    uid / is_unique onto the attribution.

    Pops the private ``_identity_deferred`` payload so it never reaches the click
    record. No-op when nothing was deferred (resolver off, or the geo/terminal
    branch already committed inline). Fail-open: any error is swallowed."""
    if not attribution:
        return
    deferred = attribution.pop("_identity_deferred", None)
    if not deferred:
        return
    try:
        committed = await identity.commit_resolution(
            deferred["result"],
            company_id=deferred["company_id"],
            funnel_user_id=deferred["funnel_user_id"],
            visitor_id=deferred["visitor_id"],
            campaign_id=deferred["campaign_id"],
            source_trusted=deferred["source_trusted"],
        )
        # Re-stamp — a lost NX race may have adopted a different canonical uid.
        attribution["uid"] = committed.uid
        attribution["is_unique"] = committed.is_unique
    except Exception as e:  # fail-open — never fail a click on a commit error
        logger.warning("deferred identity commit failed — fail-open: %s", e)


def _company_returning_enabled(campaign: dict[str, Any]) -> bool:
    """Per-company opt-in for the returning-user resolver, read FREE from the
    already-fetched campaign HASH (default closed). Admin sync populates
    `returning_resolver` in P4; until then it is absent → False → dark."""
    return str(campaign.get("returning_resolver", "")).strip().lower() in (
        "1", "true", "yes",
    )


def _company_routing_enabled(campaign: dict[str, Any]) -> bool:
    """Per-company opt-in for SEGMENTED ROUTING (the 2-pass cascade), read FREE
    from the already-fetched campaign HASH (default closed). Admin sync emits
    `returning_routing` from the company setting (P5); absent → False → dark.
    Combined with the `TDS_RETURNING_ROUTING` env toggle (env AND per-company)."""
    return str(campaign.get("returning_routing", "")).strip().lower() in (
        "1", "true", "yes",
    )


def _campaign_returning_flows_disabled(campaign: dict[str, Any]) -> bool:
    """MODEL V3 — per-campaign opt-OUT of the returning-flow partition, read FREE
    from the already-fetched campaign HASH.

    V3 activates returning routing by the EXISTENCE of a returning-only flow in
    scope (the cascade's empty-returning-pool fallthrough handles "no returning
    flow" naturally), NOT by a per-campaign override mode. This flag is the only
    per-campaign override left: when set, the campaign's returning partition is
    suppressed and every visitor — returning or not — is routed through the
    first/fresh pool (the campaign `returning_mode` fresh|sticky fallthrough
    still applies).

    FAIL-OPEN: absent / empty / unparseable ⇒ False ⇒ NOT disabled ⇒ partition
    eligible. Admin sync emits `disable_returning_flows` as "1"/"0" (B2); until
    a full sync rebuild populates it, the field is absent → not-disabled, which
    is the activation-preserving default (a returning flow that exists stays
    eligible exactly as before the gate flip)."""
    return str(campaign.get("disable_returning_flows", "")).strip().lower() in (
        "1", "true", "yes",
    )


def _non_routed_result(
    campaign_id: str,
    attribution: dict[str, Any],
    timing: dict[str, Any],
    *,
    binding_id: int = 0,
    binding_alias: str | None = None,
    fallback_url: str | None = None,
) -> dict[str, Any]:
    """Build the G2 non-routed sentinel for a matched-but-unrouted click.

    A campaign matched (targeting + tenant resolved) but the click could
    not be routed (capped / no flow + no legacy offer). The sentinel
    carries `campaign_id` + the resolved `attribution` so the click
    record persists the campaign(+effective_source) hardcoded defaults —
    instead of the pre-G2 behaviour where `route()` returned bare `None`
    and main.py wrote every slot column NULL.

    `non_routed=True` is the marker main.py keys on (alongside the legacy
    `result is None` / `blocked` cases) to drive the SAME
    record-build → dedup → XADD → 302-to-fallback path. `url=None` →
    main.py substitutes the admin-configured fallback URL.
    """
    return {
        "url": None,
        "campaign_id": campaign_id,
        "offer_id": None,
        "binding_id": binding_id,
        "binding_alias": binding_alias,
        "timing": timing,
        "attribution": attribution,
        "non_routed": True,
        "routing_status": timing.get("result", "non_routed"),
        # v2 Phase A — per-campaign terminal fallback URL (synced from
        # `campaign.fallback_url`). main.py prefers this over the node default
        # when building the no-dead-end fallback redirect. None ⇒ node default
        # (byte-identical).
        "fallback_url": fallback_url,
    }


def _identity_macros(attribution: dict[str, Any]) -> dict[str, Any]:
    """FIX-LD-F1 (2026-06-07) — project the resolved returning-user identity onto
    the macro-values shape `build_url` consumes for `{uid}`, `{is_unique}`,
    `{is_returning}`, `{is_roaming}`.

    `uid` → the canonical hex string, or `None` when the resolver produced
    nothing (DARK / anonymous / resolver OFF / fail-open) so `{uid}` collapses
    cleanly. The three flags pass through `bool(...)` → `build_url` renders them
    `true` / `false` (DARK ⇒ all `False` ⇒ `false`, the sensible default), never
    a leftover `{macro}`. Pure dict projection — no I/O, latency-neutral."""
    return {
        "uid": attribution.get("uid") or None,
        "is_unique": bool(attribution.get("is_unique")),
        "is_returning": bool(attribution.get("is_returning")),
        "is_roaming": bool(attribution.get("is_roaming")),
    }


# CF-3 (2026-06-07): day_of_week index → mon..sun label (Python weekday():
# Monday=0). Matches the admin validator's accepted {mon..sun} value set.
_DOW_LABELS: Final[tuple[str, ...]] = (
    "mon", "tue", "wed", "thu", "fri", "sat", "sun",
)


def _extra_click_dims(req: ClickRequest) -> dict[str, str]:
    """CF-3 — the 3 criterion dims admin-api accepts but the matcher historically
    never populated (isp_asn / time_of_day / day_of_week), now derived from data
    ALREADY on the click (zero worker change):

      * isp_asn      — ``req.asn`` as a digit string, ALWAYS (``req.asn`` is an
        int that defaults to 0 = CF's no-data sentinel, ``request.cf?.asn || 0``;
        it is never None). asn 0 → "0" — a MATCHABLE value: an operator's
        ``not_in ['0']`` (exclude unknown/datacenter ASN) correctly EXCLUDES a
        no-ASN click, and ``in ['0']`` targets it. ``in [<real asn>]`` on a 0
        click still fails closed ("0" ∉ the list). Mapping 0 → "" would re-open
        the CF-3 ``not_in [0]`` fail-open ("" ∉ ['0'] → exclusion no-op).
      * time_of_day  — the UTC hour of ``req.arrival_ts``, un-padded ("0".."23").
      * day_of_week  — the UTC weekday of ``req.arrival_ts`` ("mon".."sun").

    TZ = UTC (``arrival_ts`` is the worker's edge-arrival instant, ISO-8601 Z), so
    the operator's criteria match in UTC, not visitor-local time — documented in
    the criterion help text. An absent/malformed ``arrival_ts`` (old worker)
    leaves time_of_day/day_of_week "" → fail-closed on ``in`` (a ``not_in`` on
    such a click still fails open, bounded to the legacy-worker edge)."""
    isp_asn = str(req.asn)  # always digits; 0 (no-data) → "0" (matchable)
    time_of_day = ""
    day_of_week = ""
    ts = req.arrival_ts
    if ts:
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(
                timezone.utc
            )
            time_of_day = str(dt.hour)
            day_of_week = _DOW_LABELS[dt.weekday()]
        except (ValueError, TypeError):
            pass
    return {
        "isp_asn": isp_asn,
        "time_of_day": time_of_day,
        "day_of_week": day_of_week,
    }


def _allowed_availability(
    campaign: dict[str, Any], attribution: dict[str, Any]
) -> frozenset[str]:
    """The availability classes a target may have to be SERVED for this click —
    the single rule shared by the cascade pre-floor (`cascade._filter_by_
    availability`) and EVERY delivery path (v2 C2). Computed ONCE per click in
    `_route_via_campaign` and threaded (no recompute-drift).

      returning visitor (seen_before, under live returning routing) → {active, draining}
      everyone else (incl. routing OFF / partition disabled / new visitor) → {active}

    Gated identically to the audience partition (MODEL V3): routing OFF, OR the
    campaign's `disable_returning_flows` flag set ⇒ `returning_visitor` is False
    ⇒ {active} ⇒ a 'draining' target blocks all ⇒ TOTAL byte-identical invariant
    with production dark (all targets 'active' → every class passes). This MUST
    stay the exact mirror of the main-route gate in `_try_flow_cascade`."""
    returning_live = settings.returning_routing_enabled and _company_routing_enabled(
        campaign
    )
    audience_routing = returning_live and not _campaign_returning_flows_disabled(
        campaign
    )
    seen_before = bool(attribution.get("uid")) and (
        attribution.get("is_unique") is False
    )
    returning_visitor = seen_before if audience_routing else False
    return (
        frozenset({"active", "draining"})
        if returning_visitor
        else frozenset({"active"})
    )


def _resolve_fallback_template(
    template: str | None,
    req: ClickRequest,
    campaign_id: str,
    source_mappings,
    campaign_mappings,
    identity: dict[str, Any] | None = None,
) -> str | None:
    """v2 F-MACRO-1 — macro-resolve a campaign `fallback_url` BEFORE it is served.
    Reuses `build_url` (the single substitution chokepoint: `safe_substitute` +
    the full values dict) so a terminal_fallback URL never leaks a literal
    `{macro}`. `None` template → `None` (node-default path stays raw; it carries
    no macros). Offer/target macros collapse cleanly (no offer on this path).
    `identity` (FIX-LD-F1) threads the returning-user macros so a fallback URL
    template with `{uid}` / flags resolves too (DARK ⇒ empty uid + 'false')."""
    if not template:
        return None
    return build_url(
        template, req, campaign_id, "",
        source_mappings=source_mappings,
        campaign_mappings=campaign_mappings,
        identity=identity,
    )


async def _route_via_campaign(
    r,
    campaign: dict[str, Any],
    campaign_id: str,
    req: ClickRequest,
    timing: dict[str, Any],
    *,
    result_label: str,
    binding_id: int = 0,
    binding_alias: str | None = None,
    fall_through_on_no_route: bool = False,
) -> dict[str, Any] | None:
    """Drive the routing tail end for one resolved campaign.

    `fall_through_on_no_route`: when True (the domain-resolved branch),
    a campaign that matched the domain binding but has no usable routing
    path returns `None` so the caller falls through to geo targeting —
    geo still gets a chance to route the click, so emitting a non-routed
    fallback here would be premature. When False (the geo branch, the
    terminal branch), a no-flow / no-offer outcome is FINAL → return the
    G2 non-routed sentinel carrying attribution.

    Encapsulates Stages 6.5-8 so both the domain-resolved branch and the
    geo-targeting branch share one implementation. Stages:

      6.5 — Flow cascade (Vectors 2.4 + 2.5): resolve a single flow per
            `docs/design/SCOPE-CASCADE.md`. If a flow matches, its
            `action_type` drives URL emission via
            `action_executor.execute_action`. `block` short-circuits to
            None (caller surfaces 404). Other actions return URL.
      7   — Legacy `select_offer` fallback: when cascade returns None,
            pick an offer from `split:{campaign_id}` HASH and use the
            offer's URL/target. Preserves backward compat for campaigns
            whose flows haven't been authored yet (Stage 2 → Stage 3
            transition).
      8   — `build_url` substitution.

    Returns a routing result dict (`{url, campaign_id, offer_id, timing}`)
    when a path is found; a NON-ROUTED sentinel
    (`{"non_routed": True, "campaign_id", "attribution", ...}`) when a
    campaign matched but the click could not be routed (no flow + no
    legacy split) — G2 (2026-06-02): the sentinel carries the resolved
    `attribution` so the click record persists the campaign(+
    effective_source) hardcoded defaults instead of dropping every slot
    column to NULL. Returns `None` only when this branch should fall
    through to the next routing branch (domain match but no usable path —
    geo targeting still gets a chance).
    """
    t_branch = time.perf_counter()

    # Stage 3 / Phase 3 — attribution population. Resolve the canonical
    # slots + org-hierarchy chain ONCE here and thread them (plus the
    # routing-decision ids surfaced by the cascade) up to the click
    # record via a mutable `attribution` dict — the same by-reference
    # pattern the `timing` dict already uses. This is LATENCY-NEUTRAL:
    # `_resolve_buyer_chain` (one Redis HGETALL) previously ran INSIDE
    # `_try_flow_cascade` on EVERY routed click (before the no-flow
    # check), so lifting it here adds no new round-trip — it just stops
    # the already-computed result from being discarded.
    # LA-F1 (2026-06-07): when this branch may FALL THROUGH (the domain branch,
    # `fall_through_on_no_route=True`), resolve identity SIDE-EFFECT-FREE — a
    # campaign that matches the domain but routes nowhere must NOT mint a uid nor
    # write itself into campaigns-seen. The serving path commits the deferred
    # writes once (the geo/terminal branch resolves with commit=True as before,
    # byte-identical).
    source_mappings, campaign_mappings, attribution = (
        await _build_campaign_attribution(
            r, campaign, campaign_id, req,
            commit_identity=not fall_through_on_no_route,
        )
    )
    buyer_chain = _attribution_buyer_chain(attribution)

    # FIX-LD-F1 — returning-user macro projection for {uid}/{is_*}, resolved ONCE
    # from this click's attribution and threaded to every URL-build path (cascade
    # action, legacy split, terminal_fallback) so the four macros resolve
    # uniformly. DARK / anon ⇒ empty uid + 'false' flags (no literal {macro}).
    identity_macros = _identity_macros(attribution)

    # v2 C2 — the availability classes a target may have to be served for THIS
    # click, computed ONCE and threaded to every delivery path (cascade action +
    # legacy Stage 7-8). Same rule + fail-open 'active' default as the cascade
    # pre-floor → byte-identical when all targets active / routing OFF.
    allowed_avail = _allowed_availability(campaign, attribution)

    # Stage 6.5 — flow cascade.
    t0 = time.perf_counter()
    cascade_result = await _try_flow_cascade(
        r, campaign, campaign_id, req,
        source_mappings=source_mappings,
        campaign_mappings=campaign_mappings,
        buyer_chain=buyer_chain,
        attribution=attribution,
        allowed_avail=allowed_avail,
    )
    timing["cascade_ms"] = _ms_since(t0)

    if cascade_result is not None:
        # `block` action: short-circuit with no redirect URL — a blocked
        # click still routed (to a 404). Stage 6 alert module consumes
        # `action_config.alert` separately.
        if cascade_result.get("action") == "block":
            timing["route_via"] = "flow_cascade_block"
            timing["route_total_ms"] = _ms_since(t_branch)
            timing["result"] = "blocked_by_flow"
            return {
                "url": None,
                "campaign_id": campaign_id,
                "offer_id": None,
                "binding_id": binding_id,
                "binding_alias": binding_alias,
                "timing": timing,
                "attribution": attribution,
                "blocked": True,
            }

        url = cascade_result["url"]
        # Preserve `None` rather than coercing to empty string — Stage 3
        # `clicks` row writer will treat None as SQL NULL, which is the
        # right shape for a redirect-action click that has no offer
        # attribution (code review LOW-002 2026-04-28).
        offer_id = cascade_result.get("offer_id")
        timing["url_build_ms"] = timing.get("cascade_ms", 0)
        timing["route_via"] = "flow_cascade"
        timing["route_total_ms"] = _ms_since(t_branch)
        timing["result"] = result_label
        return {
            "url": url,
            "campaign_id": campaign_id,
            "offer_id": offer_id if offer_id is not None else "",
            "binding_id": binding_id,
            "binding_alias": binding_alias,
            "timing": timing,
            "attribution": attribution,
        }

    # v2 C2 — matched-but-unroutable → terminal_fallback (NOT legacy re-serve).
    # The cascade pre-floor excluded candidate flow(s) on availability, OR a
    # matched flow's delivery returned UNAVAILABLE (recorded into the trace by
    # `_try_flow_cascade`). Either way the campaign HAD a routing intent that the
    # availability floor blocked → serve THIS campaign's terminal_fallback
    # (macro-resolved, F-MACRO-1) rather than re-serving the drained/closed
    # target via Stage 7-8 (the C2 bug) or poaching another campaign via geo.
    # availability_excluded == 0 ⇒ genuine no-flow ⇒ Stage 7-8 runs below
    # (now self-filtering its own targets by availability). Byte-identical when
    # nothing is drained/closed (the floor excludes nothing → counter stays 0).
    _trace = attribution.get("routing_trace") or {}
    if _trace.get("availability_excluded"):
        timing["route_total_ms"] = _ms_since(t_branch)
        timing["result"] = "no_offer"
        return _non_routed_result(
            campaign_id, attribution, timing,
            binding_id=binding_id, binding_alias=binding_alias,
            fallback_url=_resolve_fallback_template(
                campaign.get("fallback_url"), req, campaign_id,
                source_mappings, campaign_mappings, identity_macros,
            ),
        )

    # Stage 7 — legacy fallback (no flow matched).
    t0 = time.perf_counter()
    offer = await select_offer(r, campaign_id)
    timing["offer_ms"] = _ms_since(t0)
    if not offer:
        if fall_through_on_no_route:
            # CF-OBS-1 (2026-06-07): the domain/?c= binding matched a campaign
            # that has NO flow + NO legacy offer. If the bound campaign declares
            # its OWN terminal_fallback, serve THAT (no foreign poach, no
            # cross-attribution) rather than returning None to fall through to
            # global geo and re-attribute the click to a foreign campaign. Only a
            # campaign with NO terminal_fallback falls through — preserving the
            # legitimate bare-domain catch-all (a host pointed at a campaign with
            # no terminal config genuinely wants the global-geo path).
            # `_resolve_fallback_template(None, ...) → None` is the precise
            # discriminator "did the admin configure a terminal_fallback".
            # Byte-identical for any campaign without a fallback_url.
            own_fallback = _resolve_fallback_template(
                campaign.get("fallback_url"), req, campaign_id,
                source_mappings, campaign_mappings, identity_macros,
            )
            if own_fallback:
                timing["route_total_ms"] = _ms_since(t_branch)
                timing["result"] = "no_offer"
                return _non_routed_result(
                    campaign_id, attribution, timing,
                    binding_id=binding_id, binding_alias=binding_alias,
                    fallback_url=own_fallback,
                )
            # No own terminal_fallback — let geo targeting try. The geo branch
            # (if it also lands here) will emit the G2 non-routed sentinel.
            return None
        timing["route_total_ms"] = _ms_since(t_branch)
        timing["result"] = "no_offer"
        return _non_routed_result(
            campaign_id, attribution, timing,
            binding_id=binding_id, binding_alias=binding_alias,
            # v2 Phase A — per-campaign terminal fallback (synced HASH field).
            # v2 F-MACRO-1 — macro-resolved before serve (no literal {macro} leak).
            fallback_url=_resolve_fallback_template(
                campaign.get("fallback_url"), req, campaign_id,
                source_mappings, campaign_mappings, identity_macros,
            ),
        )

    # Stage 8 — legacy URL build via offer.url / target resolution.
    # v2 C2 — resolve_target now honours the availability floor so the legacy
    # path never RE-SERVES a drained/closed target; an unavailable target is
    # skipped → falls to the offer's available default / bare url (byte-identical
    # when all targets active).
    # OBS-B1/B2 (audit-2 2026-06-07) — ACCEPTED DRIFT-ONLY RESIDUAL, documented.
    # `resolve_target` returns None both when (a) the offer has NO targets at all
    # (legit → serve the offer's own `url`), and when (b) the offer HAS targets
    # but ALL were availability-excluded (drained/closed). In case (b) the legacy
    # path serves the bare `offer.url` rather than the campaign terminal_fallback
    # — bypassing the no-dead-end availability machine. This is NOT fixed by a
    # contract change here because:
    #   * It is NOT reachable via authored config — admin-api rejects an offer /
    #     split that has no valid active target, so a live offer always has at
    #     least one servable target. Case (b) only arises from DRIFT (every target
    #     manually drained/closed out-of-band), out of the availability-machine
    #     scope by design.
    #   * The bare `offer.url` is itself an admin-AUTHORED destination, so even in
    #     the drift case the click routes to a real URL (not a 404 dead-end) — the
    #     only gap is provenance (offer.url vs campaign terminal_fallback).
    #   * A clean distinction would change `resolve_target`'s deliberate C2
    #     contract ("all-excluded → None → bare url") and break its pinning test
    #     (`test_c2_availability_delivery.test_closed_default_excluded`) — higher
    #     risk than the residual it removes. The cascade path already routes
    #     all-unavailable flows to terminal_fallback (the reachable case).
    t0 = time.perf_counter()
    target_url = await resolve_target(r, offer, req, allowed_avail)
    url_template = target_url if target_url else offer.get("url", "")
    url = build_url(
        url_template, req, campaign_id, offer.get("_id", ""),
        source_mappings=source_mappings,
        campaign_mappings=campaign_mappings,
        identity=identity_macros,
    )
    timing["url_build_ms"] = _ms_since(t0)
    timing["target_resolved"] = target_url is not None
    timing["route_via"] = "legacy_split"

    timing["route_total_ms"] = _ms_since(t_branch)
    timing["result"] = result_label

    # v2 Phase A2 — legacy-split-bridge provenance (no flow/scope/audience).
    attribution["action_type"] = "split"
    attribution["winning_scope_type"] = "legacy"
    attribution["audience_pool"] = "none"
    attribution["target_selection_path"] = "split_weighted"

    return {
        "url": url,
        "campaign_id": campaign_id,
        "offer_id": offer.get("_id", ""),
        "binding_id": binding_id,
        "binding_alias": binding_alias,
        "timing": timing,
        "attribution": attribution,
    }


async def _resolve_action_with_sticky(
    r,
    flow: dict[str, Any],
    req: ClickRequest,
    campaign_id: str,
    *,
    source_mappings,
    campaign_mappings,
    sticky_active: bool,
    uid: str,
    company_id: int | None,
    seen_before: bool,
    returning_visitor: bool,
    flow_id: str | None,
    allowed_avail=frozenset({"active"}),
    identity_macros: dict[str, Any] | None = None,
    trace: dict[str, Any] | None = None,
) -> tuple[dict[str, Any] | None, str]:
    """v2 Phase S — resolve the destination, applying the sticky pin when active.

    Returns `(result, sticky_status)`. When `sticky_active` is False this is a
    pure pass-through to `execute_action` (status "na" → byte-identical). When
    active (offer/split flow under a live sticky-mode campaign with a uid):

      * returning visitor + pin AVAILABLE for the class (active|draining) →
        serve the pin, `hit`. (`closed` never serves — the Phase-A availability
        floor still wins → no dead-end.)
      * returning visitor + pin closed/missing → re-pick the flow's target,
        `repin` (overwrite), `invalid_closed`.
      * returning visitor + no pin → re-pick + mint (NX), `miss`.
      * first visit → re-pick + mint (NX), `minted` (next visit honours it).

    FAIL-OPEN: every sticky Redis op swallows errors (a fault degrades to normal
    selection); the click always routes + is XADD'd.
    """
    # FIX-LD-F1 — bind the returning-user identity macros onto `build_url` so the
    # action-executor delivery paths (redirect / offer / split) and the sticky
    # pin resolve `{uid}`/`{is_*}` without threading a new kwarg through every
    # per-action helper. `identity_macros=None` → DARK defaults (empty uid +
    # 'false' flags), byte-identical to the pre-fix collapse for non-identity
    # templates.
    _build_url = functools.partial(build_url, identity=identity_macros)

    async def _normal() -> dict[str, Any] | None:
        return await action_executor.execute_action(
            r, flow, req, campaign_id,
            source_mappings=source_mappings,
            campaign_mappings=campaign_mappings,
            build_url_fn=_build_url,
            allowed_avail=allowed_avail,
            trace=trace,
        )

    if not sticky_active:
        return await _normal(), "na"

    ttl = settings.returning_uid_ttl_seconds
    # v2 C2 — use the threaded allowed_avail (computed ONCE in _route_via_campaign)
    # instead of recomputing here; the sticky pin-HIT availability gate below and
    # every delivery path then share ONE rule (no recompute-drift).
    allowed = allowed_avail

    if seen_before:
        pinned_tid = await sticky.get_sticky(company_id, uid, campaign_id, ttl)
        if pinned_tid:
            target = await r.hgetall(f"offer_target:{pinned_tid}")
            avail = (target.get("availability") if target else None) or "active"
            if target and target.get("url") and avail in allowed:
                pinned = action_executor.pinned_target_result(
                    target, pinned_tid, req, campaign_id, _build_url,
                    source_mappings, campaign_mappings, flow_id,
                )
                if pinned is not None:
                    return pinned, "hit"
            # Pin closed / missing / no url → re-pick the flow's target.
            result = await _normal()
            tid = result.get("target_id") if result else None
            if tid:
                # Genuine re-pin to a fresh, available target (the click serves
                # it; decision_reason `fresh_repin`).
                await sticky.repin(company_id, uid, campaign_id, tid, ttl)
                return result, "invalid_closed"
            # C-L-1 (audit-2 2026-06-07): the pin was invalid AND the re-pick
            # produced NO routable target (every sibling also drained/closed →
            # UNAVAILABLE / terminal_fallback, or a plain no-offer None). No
            # re-pin happened, so reporting "invalid_closed" — which main.py maps
            # to decision_reason `fresh_repin` — would falsely advertise a re-pin
            # that did not occur. The click routes to terminal_fallback (the
            # availability_excluded short-circuit handles that, decision_reason
            # already `terminal_fallback`); emit an honest sticky_status so the
            # column never claims a re-pin. Only reachable when a target drifted
            # closed → byte-identical when nothing is drained/closed.
            return result, "invalid_closed_term"
        # Returning visitor, no pin (e.g. minted before sticky was enabled).
        result = await _normal()
        tid = result.get("target_id") if result else None
        if tid:
            await sticky.set_sticky_nx(company_id, uid, campaign_id, tid, ttl)
        return result, "miss"

    # First visit under sticky mode → pick + mint so the NEXT visit honours it.
    result = await _normal()
    tid = result.get("target_id") if result else None
    if tid:
        await sticky.set_sticky_nx(company_id, uid, campaign_id, tid, ttl)
    return result, "minted"


async def _try_flow_cascade(
    r,
    campaign: dict[str, Any],
    campaign_id: str,
    req: ClickRequest,
    *,
    source_mappings,
    campaign_mappings,
    buyer_chain: dict[str, int | None],
    attribution: dict[str, Any],
    allowed_avail=frozenset({"active"}),
) -> dict[str, Any] | None:
    """Run scope cascade + action execution. Returns None if no flow.

    Steps:
      a. Resolve canonical slots from query_params + mappings (cheap,
         pure Python). Used to extract `buyer_id` for enrichment.
      b. Enrich `buyer_id` → org-hierarchy chain via single Redis
         HGETALL (`enrich_buyer`). When buyer slot is missing or
         non-numeric, the chain is empty — cascade falls back to
         company-level scope (resolved from campaign.company_id).
      c. Resolve winning flow via `cascade.resolve_flow`.
      d. Execute action via `action_executor.execute_action`.

    Per `architecture.md` Latency Budgets: this branch adds at most
    1 enrich + 2 cascade pipelines + 1-2 action HGETALLs ≈ 4-5ms in
    the cascade-hit shape. Within the per-click 10ms total budget on
    healthy Redis.
    """
    # `buyer_chain` is resolved once by the caller (`_route_via_campaign`)
    # and passed in — see the latency-neutral rationale there. Steps a/b
    # (slot resolve + buyer enrich) happen there now; this function owns
    # steps c (cascade) + d (action) and records the routing-decision ids
    # it discovers into the shared `attribution` dict (Phase 3).
    # F.17 (2026-05-03) + CF-3 (2026-06-07): 10-dim base click_attrs (the 7 UA/geo
    # dims + isp_asn / time_of_day / day_of_week). Each value's casing matches what
    # admin-api validates — see `cascade._CASE_PRESERVE` for which dims preserve
    # case (geo / region / browser / language) vs lowercase (os / device_type /
    # city). A KNOWN dim whose value CF or the parser couldn't resolve falls
    # through as `""` — `op=in` fails closed (no match), `op=not_in` passes
    # everyone (unchanged). A dim OUTSIDE the evaluated set (cascade.
    # KNOWN_EVALUATED_DIMS) now fails CLOSED in the matcher (CF-3, no not_in
    # fail-open).
    click_attrs: dict[str, Any] = {
        "geo": (req.country or "").upper(),
        "os": parse_os(req.user_agent).lower(),
        "device_type": parse_device_type(req.user_agent).lower(),
        "browser": parse_browser(req.user_agent),  # Title Case verbatim
        "region": req.region or "",                # CF human name verbatim
        "city": (req.city or "").lower(),          # case-insensitive match
        "language": parse_accept_language(req.accept_language),
        # CF-3 (2026-06-07): isp_asn / time_of_day / day_of_week — admin-accepted
        # base dims that were never populated (dead criteria). Derived from data
        # already on the click (req.asn / req.arrival_ts, UTC) — zero worker change.
        **_extra_click_dims(req),
    }

    # P4 — returning-user segmented routing. `seen_before` = the uid existed
    # BEFORE this click (= B∪C; NOT the is_returning flag, which is B-only —
    # conflating them silently drops segment C, R4 G1). Only meaningful when the
    # P2 resolver produced a uid; absent → False → first pool only (zero-regress
    # when the resolver / routing is OFF).
    # MODEL V3 — the returning-audience PARTITION is gated by EXISTENCE, not by a
    # per-campaign override mode. `audience_routing` only decides whether the
    # cascade RUNS the 2-pass (returning pool first, fall through to first pool);
    # whether a returning flow actually EXISTS in scope is handled naturally by
    # the cascade's empty-returning-pool fallthrough (`cascade.resolve_flow`). So
    # the gate is simply "returning routing live for this company AND the campaign
    # has not opted out via `disable_returning_flows`". The campaign `returning_mode`
    # (fresh|sticky) no longer gates the partition — it governs only the
    # fallthrough/recorded mode + the Phase-S sticky pin (read below). routing OFF
    # OR partition disabled ⇒ no 2-pass ⇒ byte-identical to non-returning routing.
    # MUST stay the exact mirror of `_allowed_availability`'s gate.
    returning_live = settings.returning_routing_enabled and _company_routing_enabled(
        campaign
    )
    # `campaign_mode` (fresh|sticky) is KEPT — it drives the recorded effective
    # mode + the sticky pin below; it no longer participates in the gate.
    campaign_mode = (campaign.get("returning_mode") or "fresh").strip().lower()
    audience_routing = returning_live and not _campaign_returning_flows_disabled(
        campaign
    )
    seen_before = bool(attribution.get("uid")) and (
        attribution.get("is_unique") is False
    )
    # Returning-flow criterion palette (flow-level only, v1). Injected ONLY for
    # a seen_before user under segmented routing — first-pool flows never carry
    # these dims (palette-guard), and the offer_target inline matcher (which
    # uses its own base-dim click_attrs) never sees them.
    if audience_routing and seen_before:
        click_attrs["is_returning"] = (
            "true" if attribution.get("is_returning") else "false"
        )
        # v2 Phase A — is_roaming joins the returning-flow criterion palette
        # (Phase-R handoff: the dim is now computed + a valid criterion, and
        # the cascade matches on it here). Same gate as is_returning.
        click_attrs["is_roaming"] = (
            "true" if attribution.get("is_roaming") else "false"
        )
        click_attrs["prev_offer"] = attribution.get("prev_offers") or frozenset()
        click_attrs["prev_offer_target"] = attribution.get("prev_targets") or frozenset()
        click_attrs["prev_sub"] = attribution.get("prev_subs") or frozenset()

    # v2 Phase A2 — routing_trace (compact, always). The cascade populates it
    # by-reference with scope_walk + candidate/loaded/availability-excluded
    # counts + winning scope, EVEN on a miss — so a non-routed click still
    # records WHY no flow won. Stamped onto attribution unconditionally below.
    cascade_trace: dict[str, Any] = {
        "buyer_enrichment": "ok" if buyer_chain.get("buyer_id") else "absent",
    }
    # v2 LD-F2 / D22 — a VALID `X-Test-Id` (validated + bound by the /decide
    # middleware via `set_test_id`) flips the trace to Mode-B (heavy): the
    # cascade emits the FULL rejected-criteria list with per-flow descriptors;
    # without it the trace stays the compact steady-state form. `get_test_id()`
    # is "" for normal traffic ⇒ light path ⇒ no per-click cost (the cost
    # invariant). The raw header is NEVER read here — only the validated,
    # context-bound value (no SEC-L1 regression).
    diagnostic = bool(get_test_id())
    flow = await cascade.resolve_flow(
        r,
        campaign_id=campaign_id,
        company_id=buyer_chain["company_id"],
        buyer_id=buyer_chain["buyer_id"],
        team_id=buyer_chain["team_id"],
        department_id=buyer_chain["department_id"],
        custom_group_id=buyer_chain["custom_group_id"],
        click_attrs=click_attrs,
        seen_before=seen_before if audience_routing else False,
        audience_routing=audience_routing,
        # v2 Phase A — availability returning-class is gated on
        # returning_routing_enabled, the SAME as the audience partition above
        # (reuse the already-gated value). routing OFF ⇒ everyone is "new" class
        # ⇒ a 'draining' target blocks ALL ⇒ TOTAL byte-identical invariant with
        # no exceptions (the "draining keeps returning" semantic activates
        # together with returning routing — one clean switch, no dual meaning).
        returning_visitor=seen_before if audience_routing else False,
        trace=cascade_trace,
        diagnostic=diagnostic,
    )
    attribution["routing_trace"] = cascade_trace
    if flow is None:
        return None

    # Phase 3/4 — record routing-decision attribution from the winning
    # flow. `traffic_target_id` + `current_version_id` are carried on the
    # flow HASH (sync builder flows.py). Stage 3 / Phase 4 (S1): the flow's
    # CURRENT version is now joined by the builder and stamped here into
    # `flow_version_id` — the CH split-attribution column (previously
    # DEFERRED for lack of a current-version pointer). "0" sentinel → 0.
    attribution["flow_id"] = _to_int(flow.get("_id"))
    attribution["traffic_target_id"] = _to_int(flow.get("traffic_target_id"))
    attribution["flow_version_id"] = _to_int(flow.get("current_version_id"))
    # v2 Phase A2 — base provenance from the winning flow.
    attribution["action_type"] = flow.get("action_type") or ""
    attribution["winning_scope_type"] = flow.get("scope_type") or ""
    attribution["winning_scope_id"] = _to_int(flow.get("scope_id"))
    attribution["audience_pool"] = flow.get("audience") or "first"

    # MODEL V3 — record the EFFECTIVE returning_mode actually in force for a
    # returning visitor under live returning routing: the CAMPAIGN mode
    # (fresh|sticky). The per-flow `returning_mode` override was REMOVED in V3 —
    # the mode is a purely campaign-level concept now (the flow HASH no longer
    # carries it; the DB column is kept dormant). "na" when routing is not live OR
    # this is not a returning visitor (mode is a returning-visitor concept). The
    # campaign mode drives only what is RECORDED + (Phase S) the sticky pin — it
    # never gates the partition (that is existence-driven, see the gate above).
    effective_mode = campaign_mode
    if returning_live and (seen_before or effective_mode == "sticky"):
        attribution["returning_mode"] = effective_mode
    else:
        attribution["returning_mode"] = "na"

    # v2 Phase S — sticky binding. Activates ONLY for an offer/split flow under
    # a live `sticky`-mode campaign with a uid (block/redirect have no offer
    # pick to pin; no uid ⇒ nothing to key on). Replaces the action's offer
    # pick with the validated (uid,campaign)→target pin. FAIL-OPEN end-to-end.
    #
    # MODEL V3 / D35 precedence — a winning RETURNING-audience flow keeps its OWN
    # offer pick; the campaign sticky pin does NOT override it (precedence is
    # returning-flow > sticky pin > fresh). The sticky pin applies ONLY when the
    # fallthrough served a first-pool winner. So suppress sticky when the winner
    # came from the returning pool (`flow.audience == "returning"`).
    uid = attribution.get("uid") or ""
    sticky_active = (
        returning_live
        and effective_mode == "sticky"
        and bool(uid)
        and (flow.get("action_type") or "") in ("offer", "split")
        and (flow.get("audience") or "first") != "returning"
    )
    company_id = buyer_chain["company_id"]
    flow_id_str = str(flow.get("_id")) if flow.get("_id") else None

    result, sticky_status = await _resolve_action_with_sticky(
        r, flow, req, campaign_id,
        source_mappings=source_mappings,
        campaign_mappings=campaign_mappings,
        sticky_active=sticky_active,
        uid=uid,
        company_id=company_id,
        seen_before=seen_before,
        returning_visitor=seen_before,
        flow_id=flow_id_str,
        allowed_avail=allowed_avail,
        # FIX-LD-F1 — thread the returning-user macros so cascade-delivered URLs
        # resolve {uid}/{is_returning}/{is_roaming}/{is_unique}. attribution
        # carries them post-`_build_campaign_attribution`; uid/flags are stable
        # across the later flow-id mutations on this same dict.
        identity_macros=_identity_macros(attribution),
        # v2 LD-F2 — same trace dict the cascade populated; the split executor
        # folds its weights/picked + per-leg availability exclusions into it.
        trace=cascade_trace,
    )
    attribution["sticky_status"] = sticky_status

    # v2 C2 — a matched flow whose delivery returned UNAVAILABLE (its pinned
    # target / every split leg / offer default excluded by the availability
    # floor) must route to terminal_fallback, NOT legacy re-serve. Record it in
    # the trace (shared object with attribution["routing_trace"]) so
    # `_route_via_campaign`'s availability_excluded short-circuit fires, and
    # return None. Byte-identical when all targets active (never produced).
    if result is action_executor.UNAVAILABLE_RESULT:
        cascade_trace["availability_excluded"] = (
            cascade_trace.get("availability_excluded") or 0
        ) + 1
        return None

    # `offer_target_id` = the destination target the action resolved to.
    # Read from a COPY-safe `.get` — never mutate `result` (it may be the
    # shared module-level `BLOCK_RESULT` singleton on a block action).
    if result is not None:
        attribution["offer_target_id"] = _to_int(result.get("target_id"))
        # v2 Phase A2 — how the destination target was resolved
        # (pinned/offer_default/bare_url/split_weighted/sticky). Block → "".
        attribution["target_selection_path"] = result.get("target_selection_path") or ""
    return result


async def _resolve_buyer_chain(
    r,
    slots: dict[str, str | None],
    campaign: dict[str, Any],
) -> dict[str, int | None]:
    """Resolve `buyer_id` → org-hierarchy chain for cascade.

    Returns `{buyer_id, team_id, department_id, custom_group_id, company_id}`
    with int values (or None when absent). The `company_id` ALWAYS
    comes from the campaign — never from the buyer enrichment — because
    the cascade keyspace is tenant-scoped and the campaign is the
    authoritative tenant for THIS click's routing.

    **Cross-tenant defense (Stage 2 hardening, security audit
    2026-04-28 HIGH-001 amplification):** when an attacker on company A
    crafts `?buyer_id=N` where user N belongs to company B,
    `enrich_buyer` returns B's chain. Without this defense the cascade
    would walk `flows:scope:B:*` keys and route A's traffic via B's
    flows — a multi-tenant data leak via PK enumeration. We close it
    by asserting `enriched.company_id == campaign.company_id`. On
    mismatch we discard the entire enrichment chain (drop team /
    department / custom_group / buyer attribution to None) and fall
    back to the campaign's company-scope only. Mismatch fires a HIGH
    activity-log + Sentry warning per `api-security` rule security
    event list.

    The Stage 3 cross-tenant key-shape fix (`user:{company_id}:{user_id}`)
    is still pinned in `docs/roadmap/stage-2-sync-excellence.md` —
    once it ships, `enrich_buyer` will refuse mismatched companies at
    source and this assertion becomes pure defense in depth.
    """
    raw_buyer = (slots or {}).get("buyer_id")
    campaign_company_id = _to_int(campaign.get("company_id"))

    # HIGH-001 (03 §3) — scope the Redis lookup to the CAMPAIGN tenant so
    # a same `buyer_id` registered in another tenant cannot resolve here
    # (structural prevention once the legacy global key is retired; the
    # company-mismatch assertion below stays as defence-in-depth).
    enriched = await enrich_buyer(r, raw_buyer, company_id=campaign_company_id)

    enriched_company_id = _to_int(enriched.get("company_id"))

    # If enrichment yielded a tenant that doesn't match the campaign,
    # treat the click as anonymous — the buyer/team/dept/group context
    # would otherwise leak across tenants. Logged so ops can detect
    # advertiser misconfig vs attacker probing.
    if enriched_company_id is not None and (
        campaign_company_id is None or enriched_company_id != campaign_company_id
    ):
        # Sanitize raw_buyer for log + Sentry — it's a valid digit-only
        # ID (enrich_buyer's isdigit() gate already filtered hostile
        # input), but we cap length to avoid breadcrumb pollution per
        # `observability` rule. Buyer IDs are internal user PKs, not
        # PII per se, but full-length verbatim logging is unnecessary.
        sanitized_buyer = (
            str(raw_buyer)[:16] if raw_buyer is not None else "<missing>"
        )
        logger.warning(
            "cross-tenant buyer_id rejected: campaign_company=%s buyer=%s "
            "enriched_company=%s — falling back to campaign tenant scope",
            campaign_company_id, sanitized_buyer, enriched_company_id,
        )
        # Tag + context for Sentry security event correlation.
        # `set_tag` is queryable in dashboards; `set_context` carries
        # the full mismatch detail for incident investigation.
        # Per `api-security` rule security event list — cross-tenant
        # PK enumeration is a HIGH-severity signal worth alerting on.
        sentry_sdk.set_tag("security_event", "cross_tenant_buyer_rejection")
        sentry_sdk.set_context("cross_tenant_attempt", {
            "campaign_company": campaign_company_id,
            "enriched_company": enriched_company_id,
            "buyer_id_prefix": sanitized_buyer,
        })
        sentry_sdk.capture_message(
            "cross-tenant buyer_id attempt blocked",
            level="warning",
        )
        return {
            "buyer_id": None,
            "team_id": None,
            "department_id": None,
            "custom_group_id": None,
            "company_id": campaign_company_id,
        }

    return {
        "buyer_id": _to_int(raw_buyer),
        "team_id": _to_int(enriched.get("team_id")),
        "department_id": _to_int(enriched.get("department_id")),
        "custom_group_id": _to_int(enriched.get("custom_group_id")),
        # Always use campaign's tenant for keyspace, even when chain
        # matches — defense-in-depth so a future bug in `enrich_buyer`
        # cannot poison the keyspace anchor.
        "company_id": campaign_company_id,
    }


def _to_int(value: Any) -> int | None:
    """Best-effort int parse; returns None on bad / empty input."""
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _safe_id_sort_key(rid: Any) -> tuple[int, int, str]:
    """Sort Redis IDs numerically with stable fallback for malformed input.

    Returns `(bucket, value, original)`:
      - bucket=0 + numeric value when `int(rid)` succeeds
      - bucket=1 + 0 + raw string when parse fails (sorts after numeric;
        original string used as tiebreaker for determinism)

    Mirrors `action_executor._safe_target_sort_key`. Used by source
    enumeration AND (B9, audit 2026-06-03) `resolve_target`'s offer-target
    ordering so the legacy path's priority-tie fallthrough matches the
    Stage-2 cascade path numerically. If a cross-service shared module
    ever lands, fold this + `action_executor._safe_target_sort_key` into
    one home (tracked, low priority — both are identical 3 lines).
    """
    try:
        return (0, int(rid), str(rid))
    except (ValueError, TypeError):
        return (1, 0, str(rid))


# §6 (F.30 security) — cross-service contract key. The admin-api sync
# builder (`app/sync/builders/domains.py`, constant
# `keys.DOMAINS_WILDCARD`) publishes into this SET every base domain
# that has ≥1 active subdomain binding — i.e. every base for which
# admin-api auto-provisioned a `*.{base}` wildcard DNS record (F.30
# A.1). Membership, NOT a naive label count, decides what is a wildcard
# subdomain, so multi-label bases (`tds.adstudy.dev`, `base.co.uk`)
# used directly are never mis-classified as subdomains of their parent.
# 3-deploy safe: an absent / empty set makes every branch below behave
# exactly as the pre-§6 resolver did, so reader (this) and writer
# (admin-api) can deploy in any order.
_WILDCARD_BASES_KEY = "domains:wildcard"


class DomainResolution(NamedTuple):
    """Outcome of domain-binding resolution for one click.

    - matched:  `campaign_id` set, `blocked=False`, binding metadata filled.
    - no match: `campaign_id=None`, `blocked=False` → caller falls through
      to geo targeting.
    - blocked:  `campaign_id=None`, `blocked=True` (§6) → caller emits 404,
      no geo fall-through.
    """
    campaign_id: str | None
    binding_id: int
    binding_alias: str | None
    blocked: bool


_NO_DOMAIN_MATCH = DomainResolution(None, 0, None, False)
_DOMAIN_BLOCKED = DomainResolution(None, 0, None, True)


def _parse_binding_value(raw: str | None) -> tuple[str, int, str | None]:
    """Parse a `domain:...` Redis value → (campaign_id, binding_id, binding_alias).

    F.31 shape is JSON `{"campaign_id","binding_id","binding_alias"}`. A
    legacy bare campaign_id scalar (pre-F.31 sync — the 3-deploy window)
    is a value that does NOT start with `{` and parses to
    `(scalar, 0, None)`.

    A value that DOES start with `{` is unambiguously meant to be the
    F.31 JSON shape; if it fails to parse it is a corrupt write, NOT a
    legacy scalar — return an empty campaign_id (a MISS) so the caller
    fails closed (block on a wildcard base, geo-fall-through otherwise)
    rather than routing to a bogus `campaign:{...` id. Never crashes.
    """
    if not raw:
        return "", 0, None
    s = raw.strip()
    if s[:1] == "{":
        try:
            obj = json.loads(s)
            cid = obj.get("campaign_id")
            bid = obj.get("binding_id") or 0
            alias = obj.get("binding_alias")
            try:
                bid = int(bid)
            except (ValueError, TypeError):
                bid = 0
            return (
                str(cid) if cid is not None else "",
                bid,
                str(alias) if alias is not None else None,
            )
        except (json.JSONDecodeError, TypeError, AttributeError):
            logger.error("corrupt JSON domain-binding value, treating as miss: %r", s[:80])
            return "", 0, None
    return s, 0, None


async def _first_match(r, keys_to_check: list[str]) -> str | None:
    """Batch-GET `keys_to_check` in one pipeline; first non-empty wins."""
    if not keys_to_check:
        return None
    pipe = r.pipeline()
    for key in keys_to_check:
        pipe.get(key)
    results = await pipe.execute()
    for val in results:
        if val:
            return val
    return None


async def resolve_domain_campaign(r, req: ClickRequest) -> DomainResolution:
    """Resolve a campaign + binding from domain bindings in Redis.

    Priority order: subdomain > path > param > root (first match wins).

    Returns a `DomainResolution`:
      - matched  — `campaign_id` set, `blocked=False`, `binding_id` +
        `binding_alias` parsed from the binding value (F.31 JSON, or
        legacy scalar → 0/None).
      - no match — `campaign_id=None`, `blocked=False`; caller falls
        through to geo targeting (legitimate for a non-wildcard host that
        resolved via its own explicit DNS).
      - blocked  — `campaign_id=None`, `blocked=True`; the hostname is an
        UNMATCHED subdomain of a wildcard-enabled base (§6 fail-closed).
        The caller MUST emit a 404 and MUST NOT fall through to geo. See
        the `_WILDCARD_BASES_KEY` contract above and
        `docs/development/F.30-F.31-domain-bindings-plan.md` §6.
    """
    # Normalise the hostname before any key lookup or wildcard-membership
    # check. The CF Worker already emits a lowercased hostname, but the
    # F.30 §6 fail-closed contract depends on the live hostname matching
    # the stored (lowercased) `base_domain` keys + `domains:wildcard`
    # members — so we don't leave that security property silently relying
    # on the edge. Trailing dot (FQDN form `example.com.`) is stripped so
    # `example.com.` resolves identically to `example.com`.
    hostname = (req.hostname or "").strip().rstrip(".").lower()
    if not hostname:
        return _NO_DOMAIN_MATCH

    path = (req.path or "").strip("/")
    first_segment = path.split("/")[0] if path else ""
    # F-PARAM-2 — single source of truth for the binding-selector key (shared
    # with resolution.resolve_slots so it's excluded from extras, not leaked).
    param_c = (req.query_params or {}).get(BINDING_SELECTOR_KEY, "")

    # Split off the first label as the candidate subdomain. A wildcard
    # subdomain needs ≥3 labels (`{label}.{base}` where the base itself
    # is a registrable ≥2-label domain) — `len(parts) >= 3` excludes a
    # bare 2-label base (`adstudy.dev`) from being read as a subdomain
    # of its TLD.
    parts = hostname.split(".")
    sub_label = parts[0] if len(parts) >= 3 else ""
    sub_base = ".".join(parts[1:]) if len(parts) >= 3 else ""

    # §6: is the candidate base a wildcard-enabled base? Only then does
    # the fail-closed discipline apply. SISMEMBER is O(1) and skipped
    # entirely for root-domain (2-label) clicks — the common case keeps
    # its single pipeline round-trip.
    is_wildcard_subdomain = False
    if sub_base:
        try:
            is_wildcard_subdomain = bool(
                await r.sismember(_WILDCARD_BASES_KEY, sub_base)
            )
        except Exception as e:  # pragma: no cover — Redis transient
            # Fail OPEN to the legacy path on a transient Redis error, but
            # LOG it: a deterministic failure here (e.g. WRONGTYPE on the
            # `domains:wildcard` key) would silently disable §6 fail-closed
            # for every subdomain while the wildcard DNS stays live, so ops
            # must be able to see it rather than have it pass unnoticed.
            logger.warning("domains:wildcard membership check failed (§6 fail-open): %s", e)
            is_wildcard_subdomain = False

    if is_wildcard_subdomain:
        # The base has a `*.{base}` wildcard DNS, so this host reaches
        # the edge even though it may have no binding. We must NOT
        # inherit the base's root/path/param keys (that would let
        # `random.{base}` ride the base campaign) and must NOT fall
        # through to geo. Resolution, then fail closed:
        #   1. Exact-hostname bindings — the subdomain is itself a
        #      registered domain in its own right (rare; takes
        #      precedence over the wildcard binding). path > param > root.
        #   2. The wildcard subdomain binding for this label.
        #   3. No match → block (404).
        keys_to_check = []
        if first_segment:
            keys_to_check.append(f"domain:{hostname}:path:{first_segment}")
        if param_c:
            keys_to_check.append(f"domain:{hostname}:param:{param_c}")
        keys_to_check.append(f"domain:{hostname}:root")
        keys_to_check.append(f"domain:{sub_base}:subdomain:{sub_label}")

        raw = await _first_match(r, keys_to_check)
        if raw:
            cid, bid, alias = _parse_binding_value(raw)
            if cid:
                return DomainResolution(cid, bid, alias, False)
        return _DOMAIN_BLOCKED  # §6 fail-closed

    # Non-wildcard host — behaviour identical to the pre-§6 resolver.
    # The legacy subdomain heuristic is retained for any base that has
    # subdomain bindings but no wildcard marker yet (pre-deploy window);
    # geo fall-through on miss is preserved because a non-wildcard
    # 3-label host only reaches the edge via its own explicit DNS.
    subdomain = ""
    base_domain = hostname
    if len(parts) > 2:
        subdomain = parts[0]
        base_domain = ".".join(parts[1:])

    keys_to_check = []
    if subdomain:
        keys_to_check.append(f"domain:{base_domain}:subdomain:{subdomain}")
    if first_segment:
        keys_to_check.append(f"domain:{hostname}:path:{first_segment}")
        if base_domain != hostname:
            keys_to_check.append(f"domain:{base_domain}:path:{first_segment}")
    if param_c:
        keys_to_check.append(f"domain:{hostname}:param:{param_c}")
        if base_domain != hostname:
            keys_to_check.append(f"domain:{base_domain}:param:{param_c}")
    keys_to_check.append(f"domain:{hostname}:root")
    if base_domain != hostname:
        keys_to_check.append(f"domain:{base_domain}:root")

    raw = await _first_match(r, keys_to_check)
    if raw:
        cid, bid, alias = _parse_binding_value(raw)
        if cid:
            return DomainResolution(cid, bid, alias, False)
    return _NO_DOMAIN_MATCH


async def select_offer(r, campaign_id: str) -> dict | None:
    """Select offer from campaign's split configuration."""
    try:
        split = await r.hgetall(f"split:{campaign_id}")
        if not split:
            offers_key = f"campaign:{campaign_id}:offers"
            offer_ids = await r.smembers(offers_key)
            if not offer_ids:
                return None
            offer_id = random.choice(sorted(offer_ids))
            offer = await r.hgetall(f"offer:{offer_id}")
            if not offer:
                return None
            offer["_id"] = offer_id
            return offer

        offer_id = weighted_select_from_dict(split)
        offer = await r.hgetall(f"offer:{offer_id}")
        if not offer:
            return None
        offer["_id"] = offer_id
        return offer
    except Exception as e:
        logger.error("select_offer failed: %s", e)
        return None


async def resolve_target(
    r, offer: dict, req: ClickRequest, allowed_avail=frozenset({"active"}),
) -> str | None:
    """Resolve the best matching offer target URL for the click's attributes.

    If offer has targets (has_targets=1):
      1. Load all target IDs from offer:{offer_id}:targets SET
      2. For each target (sorted by priority DESC), check criteria match
      3. First matching target's url_template wins
      4. Fallback: is_default=1 target
    If no targets → return None (caller uses offer.url_template)
    """
    if offer.get("has_targets") != "1":
        return None

    offer_id = offer.get("_id", "")
    target_ids = await r.smembers(f"offer:{offer_id}:targets")
    if not target_ids:
        return None

    # Load all targets in one pipeline. B9 (audit 2026-06-03): sort
    # target_ids NUMERICALLY (via `_safe_id_sort_key`) so the
    # priority-tie fallthrough order matches the Stage-2 path
    # (`action_executor._safe_target_sort_key`). Pre-fix this used a
    # plain `sorted()` = LEXICOGRAPHIC ("10" before "2"), so the same
    # offer could pick a different target on the legacy vs cascade path
    # at equal priority. Both iterations must use the SAME key so the
    # zip below stays aligned.
    pipe = r.pipeline()
    sorted_target_ids = sorted(target_ids, key=_safe_id_sort_key)
    for tid in sorted_target_ids:
        pipe.hgetall(f"offer_target:{tid}")
    targets = await pipe.execute()

    # Sort by priority DESC
    target_list = []
    for tid, t in zip(sorted_target_ids, targets):
        if t:
            t["_id"] = tid
            t["_priority"] = safe_int(t.get("priority"), 0)
            target_list.append(t)
    target_list.sort(key=lambda x: x["_priority"], reverse=True)

    # F.17 (2026-05-03) + CF-3 (2026-06-07): legacy offer-target picker — same
    # 10-dim base click_attrs as the cascade path above. Inline matcher mirrors
    # `cascade._CASE_PRESERVE` for the 4 dims that preserve case
    # (geo / region / browser / language); the rest are lowercased
    # both sides. Drift between this matcher and `cascade._criteria_match`
    # is a silent foot-gun — keep both in lockstep on any case rule
    # change.
    click_attrs = {
        "geo": (req.country or "").upper(),
        "os": parse_os(req.user_agent).lower(),
        "device_type": parse_device_type(req.user_agent).lower(),
        "browser": parse_browser(req.user_agent),
        "region": req.region or "",
        "city": (req.city or "").lower(),
        "language": parse_accept_language(req.accept_language),
        # CF-3 (2026-06-07): kept in lockstep with the cascade builder above —
        # populate isp_asn / time_of_day / day_of_week so an offer_target
        # criterion on them evaluates instead of silently reading "".
        **_extra_click_dims(req),
    }

    default_url = None

    # Mirrors `cascade._CASE_PRESERVE`. Kept inline — moving to a
    # shared module would force a circular import (router imports
    # from cascade for the Stage 2 path; cascade can't import back).
    case_preserve_dims = {"geo", "region", "browser", "language"}

    for t in target_list:
        # v2 C2 — availability floor: an unavailable target for the click's
        # class is never served on the legacy path (skipped for BOTH criteria
        # match AND the is_default fallback). Fail-open 'active' default →
        # byte-identical when nothing drained/closed.
        if (t.get("availability") or "active") not in allowed_avail:
            continue
        # Check if this is the default fallback
        if t.get("is_default") == "1":
            default_url = t.get("url", "")

        # Parse criteria JSON
        criteria_raw = t.get("criteria", "[]")
        try:
            criteria = json.loads(criteria_raw) if isinstance(criteria_raw, str) else criteria_raw
        except (json.JSONDecodeError, TypeError):
            logger.warning("Malformed criteria for offer_target %s, skipping", t.get("_id"))
            continue  # Skip targets with corrupted criteria (don't treat as match-all)

        # Empty criteria = matches all traffic
        if not criteria:
            return t.get("url", "")

        # Check each criterion
        match = True
        for c in criteria:
            dim = c.get("type", "")
            op = c.get("op", "in")
            raw_values = c.get("values", [])
            if dim in case_preserve_dims:
                values = [v for v in raw_values if isinstance(v, str)]
            else:
                values = [v.lower() if isinstance(v, str) else v for v in raw_values]
            click_val = click_attrs.get(dim, "")

            # CF-3 (2026-06-07): fail-CLOSED on an unknown/unevaluated dim — kept
            # in lockstep with `cascade._first_failing_criterion` so a `not_in`
            # exclusion on an unimplemented dim cannot silently pass for all
            # traffic (fail-open). An unknown dim drops this target.
            if dim not in cascade.KNOWN_EVALUATED_DIMS:
                match = False
                break

            if op == "in" and click_val not in values:
                match = False
                break
            elif op == "not_in" and click_val in values:
                match = False
                break

        if match:
            return t.get("url", "")

    # No criteria match — use default target if exists
    return default_url


# Defensive cap on per-click source enumeration. Realistic campaigns
# have 1-5 linked sources; a campaign with hundreds is either a
# misconfiguration or admin-led DoS. The cap keeps the hot path
# under its 10ms latency budget regardless of input. See security
# audit 2026-04-28 (HIGH-003) — pending follow-up: add an O(1)
# `campaign:{id}:source_by_slug:{slug}` index in the sync builder
# so enumeration disappears entirely.
_MAX_SOURCES_PER_CAMPAIGN_AT_CLICK = 100


async def _resolve_source_for_click(
    r,
    campaign_id: str,
    query_params: dict[str, str],
) -> dict[str, Any]:
    """Look up the source matching this click — Stage 2 / Vector 2.8.

    Resolution: `?source=<slug>` query param → match against sources
    linked to the campaign via `campaign:{id}:sources` SET. Returns
    the source HASH (with `_id`) or `{}` if no match.

    Slug comparison is case-insensitive — admin-api `_slugify` lower-
    cases on write but we normalise on read defensively in case
    legacy rows ever drift.

    Company-default-source fallback (when `?source` is absent or no
    match) is intentionally deferred — sync builder doesn't yet emit
    the company-default index. Until then, no-source means
    `resolve_slots` falls back to campaign-only mapping resolution,
    which is the design-doc-correct behavior.
    """
    src_slug_raw = query_params.get("source") if query_params else None
    if not src_slug_raw:
        return {}
    src_slug = src_slug_raw.strip().lower()
    if not src_slug:
        return {}

    source_ids = await r.smembers(f"campaign:{campaign_id}:sources")
    if not source_ids:
        return {}

    # Cap enumeration to bound hot-path Redis pipeline length. Sort
    # numerically (not lexicographic) so "lower-numbered" actually
    # means lowest int — `sorted({"10","100","2"})` lexicographic
    # gives `["10","100","2"]` while attackers can craft slug
    # collisions that crowd out legit sources at the lex prefix.
    # Numeric sort matches admin-api PG SERIAL allocation order
    # (security audit 2026-04-28 HIGH-004). Mirror of
    # `_safe_target_sort_key` in `action_executor.py`.
    sorted_ids = sorted(source_ids, key=_safe_id_sort_key)
    if len(sorted_ids) > _MAX_SOURCES_PER_CAMPAIGN_AT_CLICK:
        logger.warning(
            "campaign:%s has %d sources (>cap %d); truncating enumeration",
            campaign_id, len(sorted_ids), _MAX_SOURCES_PER_CAMPAIGN_AT_CLICK,
        )
        sentry_sdk.capture_message(
            f"campaign:{campaign_id} source count exceeds cap",
            level="warning",
        )
        sorted_ids = sorted_ids[:_MAX_SOURCES_PER_CAMPAIGN_AT_CLICK]

    pipe = r.pipeline()
    for sid in sorted_ids:
        pipe.hgetall(f"source:{sid}")
    results = await pipe.execute()

    for sid, src in zip(sorted_ids, results):
        if src and (src.get("slug") or "").strip().lower() == src_slug:
            src["_id"] = sid
            return src
    return {}


async def _effective_source_mappings(
    r,
    campaign_id: str,
    source_id: int | None,
    source_global: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Resolve the EFFECTIVE source param mappings for this (campaign, source).

    Per the SOURCE↔CAMPAIGN override contract
    (`docs/development/param-source-campaign-overrides-2026-06-02.md`):

      `effective_source = campaign_sources(C,S).params_override
                          IF that override is a non-null list
                          ELSE S.param_mappings (the source global)`

    The admin-api sync builder writes the per-link override into the
    Redis HASH `campaign:{cid}:source_overrides` (key built by
    `keys.campaign_source_overrides_hash`), field = `str(source_id)`,
    value = `json.dumps({"params_override": [...], "postbacks_override":
    [...]})`. A `null`/absent `params_override` means "inherit the
    source global" (the toggle is per-link). This read was previously
    DEAD — the click-processor ignored the HASH entirely (defect P-DEAD,
    audit 2026-06-02), so per-link overrides never took effect at click
    time.

    Defensive throughout: a malformed HASH field, malformed JSON, or a
    non-list `params_override` all fall back to the source global so a
    bad override never blanks a click's params.
    """
    if source_id is None:
        return source_global

    # Mirror the admin-api key contract — field is the stringified PK.
    raw = await r.hget(f"campaign:{campaign_id}:source_overrides", str(source_id))
    if not raw:
        return source_global

    try:
        override_obj = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        # Drift / corruption — never blank params, inherit the global.
        logger.warning(
            "Malformed source_overrides for campaign:%s source:%s; "
            "inheriting source global",
            campaign_id, source_id,
        )
        return source_global

    if not isinstance(override_obj, dict):
        return source_global

    params_override = override_obj.get("params_override")
    # `null` override ⇒ inherit global; a non-null list ⇒ override.
    # Re-use the same defensive parser the global path uses so the
    # entry-shape guarantees are identical.
    if params_override is None:
        return source_global
    parsed = parse_param_mappings(params_override)
    # `parse_param_mappings([])` returns `[]` — an explicit empty
    # override (admin wiped all per-link mappings) is honoured as
    # "no source mappings", NOT a silent fallback to the global.
    return parsed if isinstance(params_override, list) else source_global


def _source_trusted(src: dict[str, Any]) -> bool:
    """Whether the matched source is flagged trusted for returning-user
    identity (default-closed). A funnel_user_id is only treated as an identity
    signal from a trusted source (anti-poisoning, R4 G6). The admin sync emits
    `source_trusted` ("1"/"0") from `sources.funnel_user_id_trusted` (P5); a
    legacy source HASH without the field → False, so the L2 tier stays dark
    until a source is explicitly marked trusted."""
    return str(src.get("source_trusted", "")).strip().lower() in ("1", "true", "yes")


async def _fetch_resolution_context(
    r,
    campaign_id: str,
    campaign: dict[str, Any],
    query_params: dict[str, Any],
) -> tuple[list[dict[str, Any]] | None, list[dict[str, Any]] | None, int | None, bool]:
    """Resolve which source matched + parse both mapping layers.

    Returns `(source_mappings, campaign_mappings, source_id, source_trusted)`
    ready to pass to `build_url(...)`. `None` for source_mappings indicates
    "no source matched" — `resolve_slots` then drives the campaign-only
    chain. `source_id` (Stage 3 / Phase 3) is the matched source's PK —
    `None` when no `?source=` matched — surfaced so the click record can
    attribute the click to its source without a second lookup (the HASH
    was already fetched here). `source_trusted` (P2) rides along from the
    SAME already-fetched HASH (no extra read) for the returning-user
    identity gate; False when no source matched.

    `source_mappings` is the EFFECTIVE source layer: the per-link
    `campaign_sources.params_override` when set (non-null list), else the
    source's global `param_mappings`. See `_effective_source_mappings`.
    """
    src = await _resolve_source_for_click(r, campaign_id, query_params)
    source_id = _to_int(src.get("_id")) if src else None
    if not src:
        return None, parse_param_mappings(campaign.get("default_param_mappings")), None, False

    source_global = parse_param_mappings(src.get("param_mappings"))
    source_mappings = await _effective_source_mappings(
        r, campaign_id, source_id, source_global,
    )
    campaign_mappings = parse_param_mappings(campaign.get("default_param_mappings"))
    return source_mappings, campaign_mappings, source_id, _source_trusted(src)


def build_url(
    template: str,
    req: ClickRequest,
    campaign_id: str,
    offer_id: str,
    *,
    source_mappings: list[dict[str, Any]] | None = None,
    campaign_mappings: list[dict[str, Any]] | None = None,
    target_id: str | None = None,
    flow_id: str | None = None,
    identity: dict[str, Any] | None = None,
) -> str:
    """Build the redirect URL by substituting macros in `template`.

    Stage 2 / Vector 2.8 + T2.5 — uses the merged source∪campaign
    mapping chain via `resolution.resolve_slots`, then routes through
    `macros.safe_substitute` for safe URL output (path-segment
    collapse, empty-query-param drop, always-encode).

    Resolution order per the SOURCE-WINS contract
    (`docs/development/param-source-campaign-overrides-2026-06-02.md`):
      1. Request URL via merged map (SOURCE alias wins per slot).
      2. effective_source hardcoded `default_value` (source specializes
         the campaign).
      3. Campaign hardcoded `default_value`.
      4. NULL — substituter handles by collapsing the macro position.

    Worker-auto fields (`country`, `city`, `ip`, …), substituted-auto
    fields (`language`, `cost`), UA-parsed fields (`os`, `os_version`,
    `browser`, `browser_version`, `device_type`), and technical slots
    (`click_id`, `campaign_id`, `offer_id`, `visitor_id`,
    `offer_target_id`, `flow_id`) are populated directly from the
    request / route context — they are SYSTEM-fixed macro names that
    cannot be remapped via param_mappings (see `macros-registry.md`).
    This is also why they overwrite any same-named slot value at the
    end of the values dict.

    T2.5 (2026-05-09) closure of the macros-registry contract — six
    macros that previously substituted to empty are now populated:
    `os_version`, `browser_version`, `language`, `cost`,
    `offer_target_id`, `flow_id`. Reaches the canonical 70-macro
    landing-context set documented in
    `docs/roadmap/stage-1a-research/macros-registry.md`.

    Args:
        template: URL template containing `{macro}` placeholders.
        req: ClickRequest (worker fields, UA, accept_language).
        campaign_id: Stringified campaign PK for `{campaign_id}`.
        offer_id: Stringified offer PK for `{offer_id}` ('' if N/A).
        source_mappings: Effective source layer — per-link override or
            source global `param_mappings` (wins per slot, SOURCE-WINS).
        campaign_mappings: Campaign's `default_param_mappings` (fallback
            for any slot the source did not specialize).
        target_id: Offer-target PK (`{offer_target_id}`). NULL for
            `redirect` actions which have no target.
        flow_id: Winning flow PK (`{flow_id}`). NULL when caller
            doesn't have one (legacy split path).
        identity: Per-click returning-user macro projection from the
            resolved attribution (FIX-LD-F1) — the keys `uid`,
            `is_unique`, `is_returning`, `is_roaming`. See
            `_identity_macros`. `None` (the default) → `{uid}` collapses
            to empty and the three flags render `false`, so a DARK /
            anonymous click never leaks a literal `{macro}`.

    Returns:
        Final URL string. Never contains a literal `{macro}` —
        unfilled macros collapse via `safe_substitute`'s cleanup.
    """
    # Step 1 — resolve canonical slots via merged mapping chain.
    slots, _extras = resolve_slots(
        query_params=req.query_params or {},
        source_mappings=source_mappings,
        campaign_mappings=campaign_mappings,
    )

    # Step 2 — build the macro values dict. Layered so system-fixed
    # names always win over slot-resolved ones (a misconfigured
    # mapping cannot accidentally override `{click_id}` etc.).
    values: dict[str, Any] = {}

    # Slot layer (lowest precedence — overwritten by worker/technical
    # for system-reserved macro names).
    for slot, value in slots.items():
        values[slot] = value

    # Worker-auto layer — pull from request fields. Empty strings
    # become None so `safe_substitute` collapses the macro cleanly.
    #
    # Audit closure 2026-05-09 (Agent 4 MAJOR): `colo` was
    # historically present in this tuple but is NOT a member of
    # `WORKER_AUTO_SLOTS` — the canonical macro registry routes
    # CF colo info via `worker_colo`, which `macros-registry.md`
    # decision M1 explicitly EXCLUDES from landing macros (audit-
    # only technical slot, leaks routing infrastructure to
    # advertiser-facing URLs). `colo` removed from this tuple to
    # eliminate the ghost key. If a future use case needs CF
    # colo in landing URLs, promote `worker_colo` in
    # `parameters.py:_TECH_LANDING_MACROS` with an explicit
    # decision entry in `macros-registry.md` first.
    worker_auto_fields = (
        "country", "city", "region", "ip", "continent",
        "timezone", "postal_code", "latitude", "longitude",
        "as_org", "user_agent", "referer", "accept_language",
        "tls_version", "http_protocol", "hostname", "path",
    )
    for key in worker_auto_fields:
        v = getattr(req, key, "")
        values[key] = v if v else None
    values["asn"] = req.asn if req.asn else None

    # UA-parsed layer — full 5-field set per macros-registry.md
    # `UA_PARSED_SLOTS`. T2.5 added os_version + browser_version
    # (already emitted by `parse_ua` since F.17 — just wired here).
    # `device` is the legacy pre-F.17 alias for `device_type`; kept
    # so existing operator templates with `{device}` keep working.
    ua = parse_ua(req.user_agent or "")
    values["os"] = ua.get("os") or None
    values["os_version"] = ua.get("os_version") or None
    values["device_type"] = ua.get("device_type") or None
    values["device"] = ua.get("device_type") or None  # legacy alias
    values["browser"] = ua.get("browser") or None
    values["browser_version"] = ua.get("browser_version") or None

    # Substituted-auto layer (T2.5) — `SUBSTITUTED_AUTO_SLOTS`
    # from admin-api `app/common/parameters.py`.
    #
    # `language`: primary BCP47 tag from Accept-Language (per F.17,
    # only the first listed language counts — secondary q-weighted
    # are ignored). Same parser used elsewhere in router for
    # criterion matching, so substitution + match agree.
    #
    # `cost`: advertiser-supplied per-click cost. Read from
    # `query_params['cost']` directly because cost is NOT in
    # `RESERVED_SLOTS` (so the merged source∪campaign mapping
    # chain doesn't carry it). Empty / unparseable → NULL,
    # collapsed by substituter cleanup.
    #
    # Provenance gap (audit closure 2026-05-09 — Agent 4 MAJOR):
    # `app/common/parameters.py:SUBSTITUTED_AUTO_SLOTS` documents
    # `cost` as "advertiser-supplied OR hardcoded campaign cost".
    # The campaign-hardcoded fallback (read `cost` from the
    # `campaign:{id}` Redis hash when query param absent) is
    # DEFERRED — admin-api has no `campaigns.default_cost` column
    # yet, so there's nothing to read. Tracked separately for the
    # next Stage 1 vector that adds the schema. For now, missing
    # `?cost=` in the click URL → `{cost}` macro collapses cleanly
    # (existing semantics preserved; no new behaviour change).
    parsed_lang = parse_accept_language(req.accept_language)
    values["language"] = parsed_lang or None
    # A2 (audit 2026-06-03) — strict numeric gate; non-numeric ?cost=
    # drops to None so the {cost} macro collapses (never reflects text).
    qp_cost = (req.query_params or {}).get("cost") if req.query_params else None
    values["cost"] = coerce_cost(qp_cost)

    # Technical layer (always wins for system-reserved names).
    # T2.5 added offer_target_id + flow_id — caller now threads
    # them through (action_executor passes pinned_target_id and
    # flow["_id"] respectively).
    values["click_id"] = req.click_id or None
    values["campaign_id"] = str(campaign_id) if campaign_id else None
    values["offer_id"] = str(offer_id) if offer_id else None
    values["offer_target_id"] = str(target_id) if target_id else None
    values["flow_id"] = str(flow_id) if flow_id else None
    values["visitor_id"] = req.visitor_id or None

    # Identity layer (FIX-LD-F1, 2026-06-07) — returning-user macros from the
    # resolved attribution. System-fixed names (like the technical layer): a
    # param-mapping cannot remap them, so they sit AFTER the slot layer and win.
    #   {uid}          — canonical hex uid; '' / None (DARK / anon / resolver
    #                    OFF) → None → collapses cleanly via safe_substitute.
    #   {is_unique} {is_returning} {is_roaming} — booleans, rendered 'true' /
    #                    'false' by macros._coerce_value (the SAME lowercase the
    #                    cascade returning-criterion palette uses). They DEFAULT
    #                    to False when no identity was resolved, so a DARK click
    #                    renders 'false' (sensible default) rather than leaking
    #                    a literal {macro}.
    ident = identity or {}
    values["uid"] = ident.get("uid") or None
    values["is_unique"] = bool(ident.get("is_unique"))
    values["is_returning"] = bool(ident.get("is_returning"))
    values["is_roaming"] = bool(ident.get("is_roaming"))

    # Step 3 — safe substitute (handles NULL collapse + URL encoding).
    return safe_substitute(template, values)


def weighted_select(items: list[dict]) -> dict:
    weights = [safe_int(item.get("weight"), 100) for item in items]
    return random.choices(items, weights=weights, k=1)[0]


def weighted_select_from_dict(d: dict) -> str:
    keys = list(d.keys())
    weights = [safe_int(w, 1) for w in d.values()]
    return random.choices(keys, weights=weights, k=1)[0]


def parse_device_type(ua: str | None) -> str:
    return parse_ua(ua or "")["device_type"]


def parse_os(ua: str | None) -> str:
    return parse_ua(ua or "")["os"]


def parse_browser(ua: str | None) -> str:
    return parse_ua(ua or "")["browser"]


def get_full_ua_info(ua: str | None) -> dict:
    return parse_ua(ua or "")


def parse_accept_language(header: str | None) -> str:
    """Extract the user's PRIMARY BCP47 language tag from an
    `Accept-Language` header.

    Per F.17 user decision (2026-05-03): only the first listed
    language counts for criterion matching. Secondary q-weighted
    languages do not — a user with `Accept-Language: ru-RU,en;q=0.9,uk;q=0.7`
    is a Russian-primary user, even if they nominally understand
    English / Ukrainian. Showing them an `uk`-targeted creative
    burns the impression.

    Returns:
      - `"en-US"` / `"pt-BR"` style when both lang+region are valid
      - `"en"` / `"uk"` when only language is present
      - `""` when header is empty / first tag is unparseable

    Casing follows BCP47: lowercase language, uppercase region. The
    admin-api `language` validator regex matches this same casing
    (`^[a-z]{2}(-[A-Z]{2})?$`), so saved criteria and live emissions
    agree.

    Defensive — never raises. Malformed headers (e.g.
    `Accept-Language: *`, garbage bytes) yield `""` so the criterion
    `op=in` fails closed (no match), while `op=not_in` passes
    everyone (effectively a no-op for that criterion). This mirrors
    the existing missing-data convention for `geo` / `region`.
    """
    if not header:
        return ""
    primary = header.split(",", 1)[0].strip()
    primary = primary.split(";", 1)[0].strip()  # strip ;q=...
    if not primary:
        return ""
    parts = primary.split("-", 1)
    lang = parts[0].lower()
    if not (len(lang) == 2 and lang.isalpha()):
        return ""
    if len(parts) == 2:
        country = parts[1].upper()
        if len(country) == 2 and country.isalpha():
            return f"{lang}-{country}"
    return lang
