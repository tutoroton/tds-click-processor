"""Click-processor — FastAPI service for TDS routing decisions.

Receives click parameters from CF Worker, evaluates routing rules
against local Redis, returns destination URL for redirect.
"""

import asyncio
import json
import logging
import time
from urllib.parse import quote

import sentry_sdk
from sentry_sdk.integrations.fastapi import FastApiIntegration
from sentry_sdk.integrations.starlette import StarletteIntegration
from fastapi import FastAPI, Header, HTTPException, Request
from contextlib import asynccontextmanager

from app.config import settings
from app.models import ClickRequest, ClickResponse, HealthResponse
from app.redis_client import get_redis, close_redis
from app.router import route, parse_device_type, parse_os, parse_browser, get_full_ua_info
from app.shipper import run_shipper

logger = logging.getLogger("tds.click-processor")

# Sentry initialization — DSN from config, not hardcoded [C1 fix]
if settings.sentry_dsn:
    sentry_sdk.init(
        dsn=settings.sentry_dsn,
        integrations=[
            StarletteIntegration(transaction_style="endpoint"),
            FastApiIntegration(transaction_style="endpoint"),
        ],
        traces_sample_rate=0.1,
        environment=settings.environment,
        release=f"geo-tds-backend@0.1.0",
        server_name=f"{settings.node_id}",
    )

START_TIME = time.time()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup validation [C2 fix]
    if settings.environment == "production" and not settings.tds_secret_key:
        logger.warning("TDS_SECRET_KEY is empty in production — auth disabled!")

    r = await get_redis()
    try:
        await r.ping()
        logger.info("Redis connected")
    except Exception as e:
        logger.critical("Redis unreachable at startup: %s", e)
        raise

    # Start click shipper
    shipper_task = asyncio.create_task(run_shipper(r))

    yield

    # Shutdown
    shipper_task.cancel()
    try:
        await shipper_task
    except asyncio.CancelledError:
        pass
    await close_redis()


app = FastAPI(
    title="Geo-TDS Click Processor",
    version="0.1.0",
    docs_url="/docs" if settings.environment == "development" else None,
    lifespan=lifespan,
)


@app.post("/decide")
async def decide(
    req: ClickRequest,
    x_tds_key: str = Header("", alias="X-TDS-Key"),
):
    """Main routing endpoint. Called by CF Worker for every click."""
    t_endpoint_start = time.perf_counter()

    # Auth check
    if settings.tds_secret_key and x_tds_key != settings.tds_secret_key:
        raise HTTPException(status_code=403, detail="Invalid TDS key")

    # Route the click
    try:
        result = await route(req)
    except Exception as e:
        logger.error("route() failed: %s", e, extra={"click_id": req.click_id})
        sentry_sdk.capture_exception(e)
        return ClickResponse(
            url=f"{settings.fallback_url}?reason=error&click_id={quote(req.click_id, safe='')}",
            status=302,
        )

    if result is None:
        return ClickResponse(
            url=f"{settings.fallback_url}?reason=no_match&click_id={quote(req.click_id, safe='')}",
            status=302,
        )

    # Extract routing timing
    routing_timing = result.get("timing", {})

    # Build enriched click record for storage
    t_record_start = time.perf_counter()
    qp = req.query_params or {}
    ua_info = get_full_ua_info(req.user_agent)
    click_record = {
        "click_id": req.click_id,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "node_id": settings.node_id,
        "campaign_id": result.get("campaign_id"),
        "offer_id": result.get("offer_id"),
        "landing_url": result["url"],
        "ip": req.ip,
        "country": req.country,
        "city": req.city,
        "region": req.region,
        "continent": req.continent,
        "timezone": req.timezone,
        "postal_code": req.postal_code,
        "asn": req.asn,
        "isp": req.as_org,
        "latitude": req.latitude,
        "longitude": req.longitude,
        "device_type": ua_info["device_type"],
        "os": ua_info["os"],
        "os_version": ua_info["os_version"],
        "browser": ua_info["browser"],
        "browser_version": ua_info["browser_version"],
        "device_brand": ua_info["device_brand"],
        "device_model": ua_info["device_model"],
        "user_agent": req.user_agent,
        "accept_language": req.accept_language,
        "visitor_id": req.visitor_id or "",
        "is_returning": req.is_returning,
        "referer": req.referer,
        "sub1": qp.get("source", ""),
        "sub2": qp.get("creative", ""),
        "sub3": qp.get("buyer", ""),
        "sub4": qp.get("campaign_ext", qp.get("utm_campaign", "")),
        "sub5": qp.get("adgroup", ""),
        "sub6": qp.get("adset", ""),
        "sub7": qp.get("app", ""),
        "sub8": qp.get("team", ""),
        "extra_params": {k: v for k, v in qp.items()
                        if k not in ("source", "creative", "buyer", "campaign_ext",
                                     "utm_campaign", "adgroup", "adset", "app", "team", "debug")},
    }
    record_build_ms = round((time.perf_counter() - t_record_start) * 1000, 2)

    # Assemble timing before stream write (stream_write_ms added after)
    timing = {
        **routing_timing,
        "record_build_ms": record_build_ms,
    }

    # Include timing in click record for PG storage
    click_record["timing"] = timing

    # Write to local stream
    t_stream = time.perf_counter()
    try:
        r = await get_redis()
        await r.xadd("stream:clicks", {"data": json.dumps(click_record, default=str)})
    except Exception as e:
        logger.error("Failed to write click to stream: %s", e, extra={"click_id": req.click_id})
        sentry_sdk.capture_exception(e)
    timing["stream_write_ms"] = round((time.perf_counter() - t_stream) * 1000, 2)
    timing["endpoint_total_ms"] = round((time.perf_counter() - t_endpoint_start) * 1000, 2)

    return {"url": result["url"], "status": 302, "timing": timing}


@app.get("/health", response_model=HealthResponse)
async def health():
    """Health check."""
    r = await get_redis()
    try:
        redis_ok = await r.ping()
        campaigns_count = await r.scard("campaigns:active")
    except Exception:
        redis_ok = False
        campaigns_count = 0

    return HealthResponse(
        node_id=settings.node_id,
        region=settings.node_region,
        redis=redis_ok,
        campaigns_loaded=campaigns_count,
        uptime_seconds=round(time.time() - START_TIME, 1),
    )


@app.get("/stats")
async def stats():
    """Quick stats."""
    r = await get_redis()
    try:
        info = await r.info("memory")
        return {
            "node_id": settings.node_id,
            "region": settings.node_region,
            "campaigns_active": await r.scard("campaigns:active"),
            "redis_memory_mb": round(info.get("used_memory", 0) / 1024 / 1024, 2),
            "redis_keys": await r.dbsize(),
        }
    except Exception as e:
        return {"error": str(e), "node_id": settings.node_id}


@app.post("/admin/seed")
async def seed_data(x_tds_key: str = Header("", alias="X-TDS-Key")):
    """Load default routing data into local Redis.

    Protected by TDS_SECRET_KEY. Idempotent — safe to call multiple times.
    This is a stopgap until proper PG→Redis sync is implemented.
    """
    if settings.tds_secret_key and x_tds_key != settings.tds_secret_key:
        raise HTTPException(status_code=403, detail="Invalid key")

    r = await get_redis()
    pipe = r.pipeline()

    # Campaign 1: US Mobile CPA
    pipe.hset("campaign:1", mapping={
        "name": "US Mobile CPA", "status": "active", "priority": "10",
        "weight": "100", "daily_cap": "10000", "frequency_cap": "3", "frequency_period": "86400",
    })
    pipe.sadd("campaign:1:has_geo")
    pipe.sadd("campaign:1:has_device")
    pipe.sadd("geo:US", "1")
    pipe.sadd("device:mobile", "1")
    pipe.sadd("campaign:1:offers", "101", "102")
    pipe.hset("offer:101", mapping={"url": "https://example.com/offer-us-1", "payout": "2.50", "weight": "70"})
    pipe.hset("offer:102", mapping={"url": "https://example.com/offer-us-2", "payout": "1.80", "weight": "30"})
    pipe.hset("split:1", mapping={"101": "70", "102": "30"})

    # Campaign 2: EU Desktop
    pipe.hset("campaign:2", mapping={
        "name": "EU Desktop CPL", "status": "active", "priority": "5",
        "weight": "100", "daily_cap": "5000",
    })
    pipe.sadd("campaign:2:has_geo")
    pipe.sadd("campaign:2:has_os")
    pipe.sadd("geo:DE", "2")
    pipe.sadd("geo:FR", "2")
    pipe.sadd("geo:GB", "2")
    pipe.sadd("os:windows", "2")
    pipe.sadd("campaign:2:offers", "201")
    pipe.hset("offer:201", mapping={"url": "https://example.com/offer-eu", "payout": "1.20", "weight": "100"})
    pipe.hset("split:2", mapping={"201": "100"})

    # Campaign 3: Global catch-all
    pipe.hset("campaign:3", mapping={
        "name": "Global Catch-all", "status": "active", "priority": "1",
        "weight": "100", "daily_cap": "50000",
    })
    pipe.sadd("campaign:3:offers", "301")
    pipe.hset("offer:301", mapping={"url": "https://example.com/offer-global", "payout": "0.50", "weight": "100"})
    pipe.hset("split:3", mapping={"301": "100"})

    # Active campaigns index
    pipe.sadd("campaigns:active", "1", "2", "3")

    # Common targeting indexes
    pipe.sadd("device:tablet", "1")
    pipe.sadd("os:android", "1")
    pipe.sadd("os:ios", "1")
    pipe.sadd("os:other", "1")

    await pipe.execute()

    count = await r.scard("campaigns:active")
    return {"status": "ok", "message": f"Seed data loaded: {count} campaigns", "campaigns_loaded": count}


@app.middleware("http")
async def timing_middleware(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    elapsed = (time.perf_counter() - start) * 1000
    response.headers["X-Process-Time"] = f"{elapsed:.1f}ms"
    return response
