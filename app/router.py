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

import json
import logging
import random
import time
from typing import Any

import sentry_sdk
from app import action_executor, cascade
from app.enrichment import enrich_buyer
from app.macros import safe_substitute
from app.models import ClickRequest
from app.redis_client import get_redis
from app.resolution import parse_param_mappings, resolve_slots
from app.ua_parser import parse_ua

logger = logging.getLogger("tds.router")


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

    Returns one of three shapes:
      - Happy path: `{"url": str, "campaign_id": str, "offer_id": str,
        "timing": dict}` — caller emits 302 to `url`.
      - Block sentinel: `{"url": None, "campaign_id": str, "offer_id":
        None, "timing": dict, "blocked": True}` — caller emits 404
        (or worker may serve a block page). Distinguished by
        `result.get("blocked") is True` OR `result.get("url") is None`.
      - `None` — no campaign matched; caller emits the worker's
        default fallback (typically 404 with a generic page).

    Timing dict contains ms-precision breakdown of every routing stage,
    plus a `route_via` tag (`flow_cascade`, `flow_cascade_block`, or
    `legacy_split`) for ops drill-down.
    """
    t_start = time.perf_counter()
    timing = {}

    r = await get_redis()

    # Stage 0: Domain-based campaign resolution (highest priority)
    t0 = time.perf_counter()
    domain_campaign_id = await resolve_domain_campaign(r, req)
    timing["domain_resolve_ms"] = _ms_since(t0)

    if domain_campaign_id:
        # Domain resolved — skip geo targeting, go straight to flow cascade.
        timing["domain_matched"] = True
        timing["campaign_source"] = "domain"

        t0 = time.perf_counter()
        campaign = await r.hgetall(f"campaign:{domain_campaign_id}")
        timing["campaign_fetch_ms"] = _ms_since(t0)

        if campaign:
            campaign["_id"] = domain_campaign_id

            routed = await _route_via_campaign(
                r, campaign, domain_campaign_id, req, timing,
                result_label="domain_matched",
            )
            if routed is not None:
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

    # Stage 5: Cap/frequency filtering — delegates to the shared
    # `_campaign_caps_exceeded` helper so both this branch and the
    # domain-resolved branch (`_route_via_campaign`) honour the same
    # eligibility contract. Per-candidate sequential awaits are
    # acceptable inside the 10ms budget at realistic candidate
    # cardinality (1-50). If the cardinality grows we'd batch via
    # pipeline, but that's premature optimization until profiling
    # shows it.
    t0 = time.perf_counter()
    eligible = []
    for i, campaign in enumerate(campaigns):
        if not campaign:
            continue
        cid = candidates[i]
        campaign["_id"] = cid
        if await _campaign_caps_exceeded(r, cid, campaign, req.visitor_id):
            continue
        eligible.append(campaign)
    timing["filtering_ms"] = _ms_since(t0)
    timing["eligible_count"] = len(eligible)

    if not eligible:
        timing["route_total_ms"] = _ms_since(t_start)
        timing["result"] = "all_capped"
        return None

    # Stage 6: Campaign selection (priority + weight)
    t0 = time.perf_counter()
    eligible.sort(key=lambda c: safe_int(c.get("priority"), 0), reverse=True)
    top_priority = safe_int(eligible[0].get("priority"), 0)
    top_campaigns = [c for c in eligible if safe_int(c.get("priority"), 0) == top_priority]
    winner = weighted_select(top_campaigns)
    timing["selection_ms"] = _ms_since(t0)

    # Stages 6.5-9: flow cascade → action execution → counter increment.
    routed = await _route_via_campaign(
        r, winner, winner["_id"], req, timing, result_label="matched",
    )
    if routed is not None:
        return routed

    # No routing path found (no flow + legacy fallback exhausted).
    timing["route_total_ms"] = _ms_since(t_start)
    timing["result"] = "no_offer"
    return None


async def _route_via_campaign(
    r,
    campaign: dict[str, Any],
    campaign_id: str,
    req: ClickRequest,
    timing: dict[str, Any],
    *,
    result_label: str,
) -> dict[str, Any] | None:
    """Drive the routing tail end for one resolved campaign.

    Encapsulates Stages 6.5-9 so both the domain-resolved branch and the
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
      9   — Cap + frequency counter increment (non-blocking).

    Returns a routing result dict (`{url, campaign_id, offer_id, timing}`)
    when a path is found; None when the click should fall through to the
    next branch (e.g. domain match but no flow + no legacy split).
    """
    t_branch = time.perf_counter()

    # Cap pre-check (security audit 2026-04-28 CRITICAL-001 fix).
    # Stage 5 already filtered geo-branch candidates by caps, so for
    # that path this is a redundant ~0.5ms double-check kept for
    # symmetry. For the domain branch this is the FIRST cap check —
    # without it a domain-bound campaign with daily_cap=N could route
    # unlimited clicks until the next click read a stale counter.
    if await _campaign_caps_exceeded(r, campaign_id, campaign, req.visitor_id):
        timing["route_total_ms"] = _ms_since(t_branch)
        timing["result"] = "campaign_capped"
        return None

    # Resolve param-mapping context once — used by both cascade action
    # execution and legacy fallback for URL substitution.
    source_mappings, campaign_mappings = await _fetch_resolution_context(
        r, campaign_id, campaign, req.query_params or {},
    )

    # Stage 6.5 — flow cascade.
    t0 = time.perf_counter()
    cascade_result = await _try_flow_cascade(
        r, campaign, campaign_id, req,
        source_mappings=source_mappings,
        campaign_mappings=campaign_mappings,
    )
    timing["cascade_ms"] = _ms_since(t0)

    if cascade_result is not None:
        # `block` action: short-circuit with no redirect URL but DO
        # bump counters — a blocked click still routed (to a 404), and
        # cap/freq counters guard against retry-storm abuse where an
        # attacker probes a known-block geo to bypass per-visitor rate
        # limits (security audit 2026-04-28 HIGH-002). Stage 6 alert
        # module consumes `action_config.alert` separately.
        if cascade_result.get("action") == "block":
            await _bump_counters(r, campaign_id, campaign, req, timing)
            timing["route_via"] = "flow_cascade_block"
            timing["route_total_ms"] = _ms_since(t_branch)
            timing["result"] = "blocked_by_flow"
            return {
                "url": None,
                "campaign_id": campaign_id,
                "offer_id": None,
                "timing": timing,
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
        await _bump_counters(r, campaign_id, campaign, req, timing)
        timing["route_total_ms"] = _ms_since(t_branch)
        timing["result"] = result_label
        return {
            "url": url,
            "campaign_id": campaign_id,
            "offer_id": offer_id if offer_id is not None else "",
            "timing": timing,
        }

    # Stage 7 — legacy fallback (no flow matched).
    t0 = time.perf_counter()
    offer = await select_offer(r, campaign_id)
    timing["offer_ms"] = _ms_since(t0)
    if not offer:
        return None

    # Stage 8 — legacy URL build via offer.url / target resolution.
    t0 = time.perf_counter()
    target_url = await resolve_target(r, offer, req)
    url_template = target_url if target_url else offer.get("url", "")
    url = build_url(
        url_template, req, campaign_id, offer.get("_id", ""),
        source_mappings=source_mappings,
        campaign_mappings=campaign_mappings,
    )
    timing["url_build_ms"] = _ms_since(t0)
    timing["target_resolved"] = target_url is not None
    timing["route_via"] = "legacy_split"

    # Stage 9 — counter increment.
    await _bump_counters(r, campaign_id, campaign, req, timing)
    timing["route_total_ms"] = _ms_since(t_branch)
    timing["result"] = result_label

    return {
        "url": url,
        "campaign_id": campaign_id,
        "offer_id": offer.get("_id", ""),
        "timing": timing,
    }


async def _try_flow_cascade(
    r,
    campaign: dict[str, Any],
    campaign_id: str,
    req: ClickRequest,
    *,
    source_mappings,
    campaign_mappings,
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
    slots, _extras = resolve_slots(
        query_params=req.query_params or {},
        source_mappings=source_mappings,
        campaign_mappings=campaign_mappings,
    )
    buyer_chain = await _resolve_buyer_chain(r, slots, campaign)

    flow = await cascade.resolve_flow(
        r,
        campaign_id=campaign_id,
        company_id=buyer_chain["company_id"],
        buyer_id=buyer_chain["buyer_id"],
        team_id=buyer_chain["team_id"],
        department_id=buyer_chain["department_id"],
        custom_group_id=buyer_chain["custom_group_id"],
        # F.17 (2026-05-03): 7-dim click_attrs. Each value's casing
        # matches what admin-api validates — see `cascade._CASE_PRESERVE`
        # for which dims preserve case (geo / region / browser /
        # language) vs lowercase (os / device_type / city). Values
        # that CF or the parser couldn't resolve fall through as `""`
        # — `op=in` fails closed (no match), `op=not_in` passes
        # everyone (no-op for that criterion).
        click_attrs={
            "geo": (req.country or "").upper(),
            "os": parse_os(req.user_agent).lower(),
            "device_type": parse_device_type(req.user_agent).lower(),
            "browser": parse_browser(req.user_agent),  # Title Case verbatim
            "region": req.region or "",                # CF human name verbatim
            "city": (req.city or "").lower(),          # case-insensitive match
            "language": parse_accept_language(req.accept_language),
        },
    )
    if flow is None:
        return None

    return await action_executor.execute_action(
        r, flow, req, campaign_id,
        source_mappings=source_mappings,
        campaign_mappings=campaign_mappings,
        build_url_fn=build_url,
    )


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
    enriched = await enrich_buyer(r, raw_buyer)

    campaign_company_id = _to_int(campaign.get("company_id"))
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


async def _campaign_caps_exceeded(
    r,
    campaign_id: str,
    campaign: dict[str, Any],
    visitor_id: str | None,
) -> bool:
    """Stage-5-equivalent eligibility check for a single campaign.

    Returns True when EITHER:
      - daily_cap > 0 AND `cap:{campaign_id}:daily` ≥ daily_cap, OR
      - frequency_cap > 0 AND visitor_id present AND
        `freq:{campaign_id}:{visitor_id}` ≥ frequency_cap.

    Used by both routing branches:
      - Geo branch: Stage 5 calls this per-candidate to filter out
        capped campaigns BEFORE selection (the original behaviour).
      - Domain branch: `_route_via_campaign` calls this once at entry.
        Pre-Vector 2.4+2.5 the domain branch went straight from
        `resolve_domain_campaign` to `select_offer` and skipped the
        eligibility check entirely — every domain-bound campaign
        could over-deliver beyond `daily_cap` (security audit
        2026-04-28 CRITICAL-001). Hoisting the check into the
        shared orchestrator closes the asymmetry: both branches
        now honour caps before any routing work runs.

    Cost: 1-2 Redis GETs (daily counter + optional freq counter).
    For the geo branch this is at most a redundant double-check
    (Stage 5 already filtered) — kept defensively because the cost
    is far below the 10ms hot-path budget and the symmetry guards
    against future regressions where Stage 5 logic drifts.

    Failure-mode: any Redis error here is treated as "not capped"
    so a transient outage doesn't block routing. The fail-open
    posture matches `_bump_counters` — caps are best-effort, never
    fail the click.
    """
    try:
        daily_cap = safe_int(campaign.get("daily_cap"))
        if daily_cap > 0:
            current = await r.get(f"cap:{campaign_id}:daily")
            if current and safe_int(current) >= daily_cap:
                return True

        freq_cap = safe_int(campaign.get("frequency_cap"))
        if visitor_id and freq_cap > 0:
            visits = await r.get(f"freq:{campaign_id}:{visitor_id}")
            if visits and safe_int(visits) >= freq_cap:
                return True
    except Exception as e:  # pragma: no cover — Redis transient
        logger.warning(
            "cap check failed for campaign:%s — failing open: %s",
            campaign_id, e,
        )
    return False


async def _bump_counters(
    r,
    campaign_id: str,
    campaign: dict[str, Any],
    req: ClickRequest,
    timing: dict[str, Any],
) -> None:
    """Stage 9 — daily cap + per-visitor frequency increment.

    Non-blocking: failures are logged but never fail the click. Counters
    drift in worst case; click still routes.
    """
    t0 = time.perf_counter()
    try:
        pipe = r.pipeline()
        cap_key = f"cap:{campaign_id}:daily"
        pipe.incr(cap_key)
        pipe.expire(cap_key, 86400)
        if req.visitor_id:
            freq_period = safe_int(campaign.get("frequency_period"), 86400)
            freq_key = f"freq:{campaign_id}:{req.visitor_id}"
            pipe.incr(freq_key)
            pipe.expire(freq_key, freq_period if freq_period > 0 else 86400)
        await pipe.execute()
    except Exception as e:
        logger.warning("Counter update failed: %s", e)
    timing["counter_ms"] = _ms_since(t0)


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

    Mirrors `action_executor._safe_target_sort_key`. Promote to a shared
    helper if a third caller needs this — left inline for now per
    `reusability-discipline` "extract at 2nd use".
    """
    try:
        return (0, int(rid), str(rid))
    except (ValueError, TypeError):
        return (1, 0, str(rid))


async def resolve_domain_campaign(r, req: ClickRequest) -> str | None:
    """Resolve campaign_id from domain bindings in Redis.

    Priority order: subdomain > path > param > root (first match wins).
    """
    hostname = req.hostname
    if not hostname:
        return None

    path = (req.path or "").strip("/")
    first_segment = path.split("/")[0] if path else ""
    param_c = (req.query_params or {}).get("c", "")

    # Extract subdomain: if hostname has more parts than 2, the prefix is the subdomain
    # e.g., "gambling-tier-1.tds.adstudy.dev" → subdomain = "gambling-tier-1", base = "tds.adstudy.dev"
    parts = hostname.split(".")
    subdomain = ""
    base_domain = hostname
    if len(parts) > 2:
        # Could be sub.domain.tld or sub.domain.co.uk
        # Try: first part as subdomain, rest as base
        subdomain = parts[0]
        base_domain = ".".join(parts[1:])

    # Build lookup keys in priority order
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

    if not keys_to_check:
        return None

    # Batch lookup — single pipeline round-trip
    pipe = r.pipeline()
    for key in keys_to_check:
        pipe.get(key)
    results = await pipe.execute()

    for val in results:
        if val:
            return val  # First match wins

    return None


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


async def resolve_target(r, offer: dict, req: ClickRequest) -> str | None:
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

    # Load all targets in one pipeline
    pipe = r.pipeline()
    for tid in sorted(target_ids):
        pipe.hgetall(f"offer_target:{tid}")
    targets = await pipe.execute()

    # Sort by priority DESC
    target_list = []
    for tid, t in zip(sorted(target_ids), targets):
        if t:
            t["_id"] = tid
            t["_priority"] = safe_int(t.get("priority"), 0)
            target_list.append(t)
    target_list.sort(key=lambda x: x["_priority"], reverse=True)

    # F.17 (2026-05-03): legacy offer-target picker — same 7-dim
    # click_attrs as the cascade path above. Inline matcher mirrors
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
    }

    default_url = None

    # Mirrors `cascade._CASE_PRESERVE`. Kept inline — moving to a
    # shared module would force a circular import (router imports
    # from cascade for the Stage 2 path; cascade can't import back).
    case_preserve_dims = {"geo", "region", "browser", "language"}

    for t in target_list:
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


async def _fetch_resolution_context(
    r,
    campaign_id: str,
    campaign: dict[str, Any],
    query_params: dict[str, Any],
) -> tuple[list[dict[str, Any]] | None, list[dict[str, Any]] | None]:
    """Resolve which source matched + parse both mapping layers.

    Returns `(source_mappings, campaign_mappings)` ready to pass to
    `build_url(...)`. `None` for source_mappings indicates "no source
    matched" — `resolve_slots` then drives the campaign-only chain.
    """
    src = await _resolve_source_for_click(r, campaign_id, query_params)
    source_mappings = parse_param_mappings(src.get("param_mappings")) if src else None
    campaign_mappings = parse_param_mappings(campaign.get("default_param_mappings"))
    return source_mappings, campaign_mappings


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
) -> str:
    """Build the redirect URL by substituting macros in `template`.

    Stage 2 / Vector 2.8 + T2.5 — uses the merged source∪campaign
    mapping chain via `resolution.resolve_slots`, then routes through
    `macros.safe_substitute` for safe URL output (path-segment
    collapse, empty-query-param drop, always-encode).

    Resolution order per `docs/design/PARAMETER-SYSTEM.md`:
      1. Request URL via merged map (campaign alias wins per slot).
      2. Campaign hardcoded `default_value`.
      3. Source hardcoded `default_value`.
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
        source_mappings: Source's `param_mappings` (resolution chain).
        campaign_mappings: Campaign's overrides (wins per slot).
        target_id: Offer-target PK (`{offer_target_id}`). NULL for
            `redirect` actions which have no target.
        flow_id: Winning flow PK (`{flow_id}`). NULL when caller
            doesn't have one (legacy split path).

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
    qp_cost = (req.query_params or {}).get("cost") if req.query_params else None
    values["cost"] = qp_cost if qp_cost else None

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
