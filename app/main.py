"""Click-processor — FastAPI service for TDS routing decisions.

Receives click parameters from CF Worker, evaluates routing rules
against local Redis, returns destination URL for redirect.
"""

import time

import sentry_sdk
from sentry_sdk.integrations.fastapi import FastApiIntegration
from sentry_sdk.integrations.starlette import StarletteIntegration
from fastapi import FastAPI, Header, HTTPException, Request
from contextlib import asynccontextmanager

from app.config import settings
from app.models import ClickRequest, ClickResponse, HealthResponse
from app.redis_client import get_redis, close_redis
from app.router import route

# Sentry initialization — before FastAPI app creation
sentry_sdk.init(
    dsn="https://227c91acc7ea911e7d270699f11cad71@o4510500844929024.ingest.de.sentry.io/4511099868807248",
    integrations=[
        StarletteIntegration(transaction_style="endpoint"),
        FastApiIntegration(transaction_style="endpoint"),
    ],
    traces_sample_rate=0.1,  # 10% sampling
    environment=settings.environment,
    release=f"geo-tds-backend@0.1.0",
    server_name=f"{settings.node_id}",
)

START_TIME = time.time()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: verify Redis connection
    r = await get_redis()
    await r.ping()
    yield
    # Shutdown
    await close_redis()


app = FastAPI(
    title="Geo-TDS Click Processor",
    version="0.1.0",
    docs_url="/docs" if settings.environment == "development" else None,
    lifespan=lifespan,
)


@app.post("/decide", response_model=ClickResponse)
async def decide(
    req: ClickRequest,
    x_tds_key: str = Header("", alias="X-TDS-Key"),
):
    """Main routing endpoint. Called by CF Worker for every click.

    1. Validate auth
    2. Lookup matching campaigns in Redis
    3. Evaluate rules (caps, frequency, targeting)
    4. Select offer (weighted split)
    5. Build destination URL
    6. Return redirect instruction
    """
    # Auth check (skip in development if no key configured)
    if settings.tds_secret_key and x_tds_key != settings.tds_secret_key:
        raise HTTPException(status_code=403, detail="Invalid TDS key")

    # Route the click
    result = await route(req)

    if result is None:
        # No matching campaign — return fallback
        return ClickResponse(
            url=f"https://adstudy.dev?reason=no_match&click_id={req.click_id}",
            status=302,
        )

    return ClickResponse(url=result["url"], status=302)


@app.get("/health", response_model=HealthResponse)
async def health():
    """Health check — returns node status and Redis connectivity."""
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
    """Quick stats — campaigns, offers, Redis memory."""
    r = await get_redis()
    info = await r.info("memory")
    return {
        "node_id": settings.node_id,
        "region": settings.node_region,
        "campaigns_active": await r.scard("campaigns:active"),
        "redis_memory_mb": round(info.get("used_memory", 0) / 1024 / 1024, 2),
        "redis_keys": await r.dbsize(),
    }


@app.middleware("http")
async def timing_middleware(request: Request, call_next):
    """Add X-Process-Time header to all responses."""
    start = time.perf_counter()
    response = await call_next(request)
    elapsed = (time.perf_counter() - start) * 1000
    response.headers["X-Process-Time"] = f"{elapsed:.1f}ms"
    return response
