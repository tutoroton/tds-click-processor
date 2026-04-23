"""Routing engine — the core of click-processor.

Reads campaign/offer/rule data from Redis, evaluates targeting conditions,
selects destination URL. All lookups are Redis-only, no SQL.

Every stage is timed to millisecond precision for observability.
"""

import json
import logging
import random
import time
from urllib.parse import quote

import sentry_sdk
from app.models import ClickRequest
from app.redis_client import get_redis
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

    Returns: {"url": ..., "campaign_id": ..., "offer_id": ..., "timing": {...}} or None.
    Timing dict contains ms-precision breakdown of every routing stage.
    """
    t_start = time.perf_counter()
    timing = {}

    r = await get_redis()

    # Stage 0: Domain-based campaign resolution (highest priority)
    t0 = time.perf_counter()
    domain_campaign_id = await resolve_domain_campaign(r, req)
    timing["domain_resolve_ms"] = _ms_since(t0)

    if domain_campaign_id:
        # Domain resolved — skip geo targeting, go straight to offer selection
        timing["domain_matched"] = True
        timing["campaign_source"] = "domain"

        t0 = time.perf_counter()
        campaign = await r.hgetall(f"campaign:{domain_campaign_id}")
        timing["campaign_fetch_ms"] = _ms_since(t0)

        if campaign:
            campaign["_id"] = domain_campaign_id

            # Stage 7: Offer selection
            t0 = time.perf_counter()
            offer = await select_offer(r, domain_campaign_id)
            timing["offer_ms"] = _ms_since(t0)

            if offer:
                # Stage 8: URL building
                t0 = time.perf_counter()
                url = build_url(offer.get("url", ""), req, domain_campaign_id, offer.get("_id", ""))
                timing["url_build_ms"] = _ms_since(t0)

                # Stage 9: Counter increment
                t0 = time.perf_counter()
                try:
                    pipe = r.pipeline()
                    cap_key = f"cap:{domain_campaign_id}:daily"
                    pipe.incr(cap_key)
                    pipe.expire(cap_key, 86400)
                    if req.visitor_id:
                        freq_period = safe_int(campaign.get("frequency_period"), 86400)
                        freq_key = f"freq:{domain_campaign_id}:{req.visitor_id}"
                        pipe.incr(freq_key)
                        pipe.expire(freq_key, freq_period if freq_period > 0 else 86400)
                    await pipe.execute()
                except Exception as e:
                    logger.warning("Counter update failed: %s", e)
                timing["counter_ms"] = _ms_since(t0)

                timing["route_total_ms"] = _ms_since(t_start)
                timing["result"] = "domain_matched"

                return {
                    "url": url,
                    "campaign_id": domain_campaign_id,
                    "offer_id": offer.get("_id", ""),
                    "timing": timing,
                }

        # Domain matched but no campaign/offer in Redis — fall through to geo targeting
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

    # Stage 5: Cap/frequency filtering
    t0 = time.perf_counter()
    eligible = []
    for i, campaign in enumerate(campaigns):
        if not campaign:
            continue
        cid = candidates[i]
        campaign["_id"] = cid

        daily_cap = safe_int(campaign.get("daily_cap"))
        if daily_cap > 0:
            current = await r.get(f"cap:{cid}:daily")
            if current and safe_int(current) >= daily_cap:
                continue

        freq_cap = safe_int(campaign.get("frequency_cap"))
        if req.visitor_id and freq_cap > 0:
            visits = await r.get(f"freq:{cid}:{req.visitor_id}")
            if visits and safe_int(visits) >= freq_cap:
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

    # Stage 7: Offer selection
    t0 = time.perf_counter()
    offer = await select_offer(r, winner["_id"])
    timing["offer_ms"] = _ms_since(t0)

    if not offer:
        timing["route_total_ms"] = _ms_since(t_start)
        timing["result"] = "no_offer"
        return None

    # Stage 8: Target resolution + URL building
    t0 = time.perf_counter()
    target_url = await resolve_target(r, offer, req)
    url_template = target_url if target_url else offer.get("url", "")
    url = build_url(url_template, req, winner["_id"], offer.get("_id", ""))
    timing["url_build_ms"] = _ms_since(t0)
    timing["target_resolved"] = target_url is not None

    # Stage 9: Counter increment (non-blocking)
    t0 = time.perf_counter()
    try:
        pipe = r.pipeline()
        cap_key = f"cap:{winner['_id']}:daily"
        pipe.incr(cap_key)
        pipe.expire(cap_key, 86400)
        if req.visitor_id:
            freq_period = safe_int(winner.get("frequency_period"), 86400)
            freq_key = f"freq:{winner['_id']}:{req.visitor_id}"
            pipe.incr(freq_key)
            pipe.expire(freq_key, freq_period if freq_period > 0 else 86400)
        await pipe.execute()
    except Exception as e:
        logger.warning("Counter update failed: %s", e)
    timing["counter_ms"] = _ms_since(t0)

    timing["route_total_ms"] = _ms_since(t_start)
    timing["result"] = "matched"

    return {
        "url": url,
        "campaign_id": winner["_id"],
        "offer_id": offer.get("_id", ""),
        "timing": timing,
    }


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

    # Build click attributes for matching
    click_attrs = {
        "geo": (req.country or "").upper(),
        "os": parse_os(req.user_agent).lower(),
        "device_type": parse_device_type(req.user_agent).lower(),
    }

    default_url = None

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
            values = [v.lower() if dim != "geo" else v.upper() for v in c.get("values", [])]
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


def build_url(template: str, req: ClickRequest, campaign_id: str, offer_id: str) -> str:
    """Replace macros in offer URL template with actual values."""
    # System macros (cannot be overridden by query params)
    system_macros = {
        "{click_id}", "{campaign_id}", "{offer_id}", "{country}",
        "{city}", "{region}", "{ip}", "{os}", "{device}", "{visitor_id}",
    }
    replacements = {
        "{click_id}": quote(str(req.click_id), safe=""),
        "{campaign_id}": quote(str(campaign_id), safe=""),
        "{offer_id}": quote(str(offer_id), safe=""),
        "{country}": quote(str(req.country), safe=""),
        "{city}": quote(str(req.city), safe=""),
        "{region}": quote(str(req.region), safe=""),
        "{ip}": quote(str(req.ip), safe=""),
        "{os}": quote(parse_os(req.user_agent), safe=""),
        "{device}": quote(parse_device_type(req.user_agent), safe=""),
        "{visitor_id}": quote(str(req.visitor_id or ""), safe=""),
    }
    # Query params as additional macros (system macros take priority)
    for key, value in (req.query_params or {}).items():
        macro = f"{{{key}}}"
        if macro not in system_macros:
            replacements[macro] = quote(str(value), safe="")

    url = template
    for macro, value in replacements.items():
        url = url.replace(macro, value)
    return url


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
