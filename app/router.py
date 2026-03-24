"""Routing engine — the core of click-processor.

Reads campaign/offer/rule data from Redis, evaluates targeting conditions,
selects destination URL. All lookups are Redis-only, no SQL.
"""

import random
import re

import sentry_sdk
from app.models import ClickRequest
from app.redis_client import get_redis


async def route(req: ClickRequest) -> dict | None:
    """Find matching campaign + offer for this click.

    Returns: {"url": "...", "campaign_id": ..., "offer_id": ...} or None.
    """
    r = await get_redis()

    # Step 1: Find campaigns targeting this country + device + OS
    device_type = parse_device_type(req.user_agent)
    os_name = parse_os(req.user_agent)

    # Pipeline: fetch all matching sets in one round-trip
    pipe = r.pipeline()
    pipe.smembers(f"geo:{req.country}")
    pipe.smembers(f"device:{device_type}")
    pipe.smembers(f"os:{os_name}")
    pipe.smembers("campaigns:active")  # all active campaign IDs
    results = await pipe.execute()

    geo_ids = results[0] or set()
    device_ids = results[1] or set()
    os_ids = results[2] or set()
    active_ids = results[3] or set()

    # For each active campaign, check if it matches this request.
    # A campaign matches if:
    # - It's in the geo set for this country, OR it has no geo targeting (not in ANY geo set)
    # - Same logic for device and OS
    #
    # geo_ids = campaigns that explicitly target this country
    # If a campaign has NO geo targeting at all, it's a catch-all for geo dimension
    candidates = set()
    for cid in active_ids:
        # Check geo: campaign is in geo:XX set, OR campaign has no geo targeting
        has_geo_targeting = await r.exists(f"campaign:{cid}:has_geo")
        geo_match = (cid in geo_ids) or (not has_geo_targeting)

        # Check device
        has_device_targeting = await r.exists(f"campaign:{cid}:has_device")
        device_match = (cid in device_ids) or (not has_device_targeting)

        # Check OS
        has_os_targeting = await r.exists(f"campaign:{cid}:has_os")
        os_match = (cid in os_ids) or (not has_os_targeting)

        if geo_match and device_match and os_match:
            candidates.add(cid)

    if not candidates:
        return None

    # Step 2: Fetch candidate campaign details (one round-trip)
    pipe = r.pipeline()
    for cid in candidates:
        pipe.hgetall(f"campaign:{cid}")
    campaigns = await pipe.execute()

    # Step 3: Filter by additional rules (caps, time, frequency)
    eligible = []
    for i, campaign in enumerate(campaigns):
        if not campaign:
            continue
        cid = list(candidates)[i]
        campaign["_id"] = cid

        # Check daily cap
        if campaign.get("daily_cap") and campaign["daily_cap"] != "0":
            cap_key = f"cap:{cid}:daily"
            current = await r.get(cap_key)
            if current and int(current) >= int(campaign["daily_cap"]):
                continue

        # Check frequency cap (per visitor)
        if req.visitor_id and campaign.get("frequency_cap") and campaign["frequency_cap"] != "0":
            freq_key = f"freq:{cid}:{req.visitor_id}"
            visits = await r.get(freq_key)
            if visits and int(visits) >= int(campaign["frequency_cap"]):
                continue

        eligible.append(campaign)

    if not eligible:
        return None

    # Step 4: Select campaign by priority (highest) then weight (random)
    eligible.sort(key=lambda c: int(c.get("priority", "0")), reverse=True)
    top_priority = int(eligible[0].get("priority", "0"))
    top_campaigns = [c for c in eligible if int(c.get("priority", "0")) == top_priority]

    # Weighted random among same-priority campaigns
    winner = weighted_select(top_campaigns)

    # Step 5: Get offer via split
    offer = await select_offer(r, winner["_id"])
    if not offer:
        return None

    # Step 6: Build destination URL
    url = build_url(offer.get("url", ""), req, winner["_id"], offer.get("_id", ""))

    # Step 7: Increment counters (async, non-blocking)
    pipe = r.pipeline()
    cap_key = f"cap:{winner['_id']}:daily"
    pipe.incr(cap_key)
    pipe.expire(cap_key, 86400)
    if req.visitor_id:
        freq_key = f"freq:{winner['_id']}:{req.visitor_id}"
        pipe.incr(freq_key)
        pipe.expire(freq_key, int(winner.get("frequency_period", "86400")))
    await pipe.execute()

    return {
        "url": url,
        "campaign_id": winner["_id"],
        "offer_id": offer.get("_id", ""),
    }


async def select_offer(r, campaign_id: str) -> dict | None:
    """Select offer from campaign's split configuration."""
    split = await r.hgetall(f"split:{campaign_id}")
    if not split:
        # No split — get default offer
        offers_key = f"campaign:{campaign_id}:offers"
        offer_ids = await r.smembers(offers_key)
        if not offer_ids:
            return None
        offer_id = random.choice(list(offer_ids))
        offer = await r.hgetall(f"offer:{offer_id}")
        offer["_id"] = offer_id
        return offer

    # Weighted selection from split
    offer_id = weighted_select_from_dict(split)
    offer = await r.hgetall(f"offer:{offer_id}")
    offer["_id"] = offer_id
    return offer


def build_url(template: str, req: ClickRequest, campaign_id: str, offer_id: str) -> str:
    """Replace macros in offer URL template with actual values."""
    replacements = {
        "{click_id}": req.click_id,
        "{campaign_id}": campaign_id,
        "{offer_id}": offer_id,
        "{country}": req.country,
        "{city}": req.city,
        "{region}": req.region,
        "{ip}": req.ip,
        "{os}": parse_os(req.user_agent),
        "{device}": parse_device_type(req.user_agent),
        "{visitor_id}": req.visitor_id or "",
    }
    # Replace sub params: {sub1}, {sub2}, etc.
    for key, value in req.query_params.items():
        replacements[f"{{{key}}}"] = value

    url = template
    for macro, value in replacements.items():
        url = url.replace(macro, value)

    return url


def weighted_select(items: list[dict]) -> dict:
    """Select item by weight field."""
    weights = [int(item.get("weight", "100")) for item in items]
    return random.choices(items, weights=weights, k=1)[0]


def weighted_select_from_dict(d: dict) -> str:
    """Select key from {key: weight} dict."""
    keys = list(d.keys())
    weights = [int(w) for w in d.values()]
    return random.choices(keys, weights=weights, k=1)[0]


def parse_device_type(ua: str) -> str:
    """Extract device type from User-Agent."""
    ua_lower = ua.lower()
    if any(x in ua_lower for x in ["mobile", "iphone", "android", "phone"]):
        return "mobile"
    if any(x in ua_lower for x in ["tablet", "ipad"]):
        return "tablet"
    return "desktop"


def parse_os(ua: str) -> str:
    """Extract OS from User-Agent."""
    ua_lower = ua.lower()
    if "iphone" in ua_lower or "ipad" in ua_lower or "ios" in ua_lower:
        return "ios"
    if "android" in ua_lower:
        return "android"
    if "windows" in ua_lower:
        return "windows"
    if "mac" in ua_lower:
        return "macos"
    if "linux" in ua_lower:
        return "linux"
    return "other"
