"""Click-processor — FastAPI service for TDS routing decisions.

Receives click parameters from CF Worker, evaluates routing rules
against local Redis, returns destination URL for redirect.
"""

import asyncio
import gzip
import hashlib
import hmac
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
from app.diag import (
    before_send as diag_before_send,
    emit_checkpoint,
    emit_obs,
    run_obs_drain,
    set_test_id,
    traces_sampler as diag_traces_sampler,
)
from app.models import ClickRequest, ClickResponse, HealthResponse
from app.redis_client import get_redis, close_redis
from app.router import route, parse_device_type, parse_os, parse_browser, get_full_ua_info
from app.shipper import run_shipper
from app.disk_queue import enqueue_click as enqueue_click_to_disk, run_drainer as run_disk_drainer
from app.observability import run_observability_loop
from app.sync_client import apply_snapshot, start_periodic_pull

logger = logging.getLogger("tds.click-processor")

# Sentry initialization — DSN from config, not hardcoded [C1 fix]
#
# `traces_sampler` (callback) replaces the flat `traces_sample_rate`:
# - Returns 1.0 for requests carrying X-Test-Id when `diag_traces_boost`
#   is on (full chronological span capture for diagnostic probes).
# - Returns 0.1 baseline otherwise (production sample stays unchanged).
# - Returns 0.1 when `diag_traces_boost` is off regardless of header
#   (production safety — toggle is the master switch).
#
# `before_send` redacts sensitive headers + query keys (X-TDS-Key,
# debug=, Authorization). Defense-in-depth backstop independent of
# whether each capture site remembered to scrub.
if settings.sentry_dsn:
    sentry_sdk.init(
        dsn=settings.sentry_dsn,
        integrations=[
            StarletteIntegration(transaction_style="endpoint"),
            FastApiIntegration(transaction_style="endpoint"),
        ],
        traces_sampler=diag_traces_sampler,
        before_send=diag_before_send,
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

    # Start periodic sync pull from central
    sync_task = asyncio.create_task(start_periodic_pull(r, interval=settings.full_sync_interval_seconds))

    # Start disk-queue drainer (T2.2 / G-23). Periodically replays
    # any clicks that landed on disk during a Redis outage back
    # into the stream. Runs unconditionally — even in standalone
    # mode there's no harm in scanning an empty queue every 30s.
    disk_drainer_task = asyncio.create_task(run_disk_drainer(r))

    # Start observability loop (T2.6 partial). Periodically emits
    # `stream.clicks.length` + `disk_queue.size` to Sentry as
    # warn-level breadcrumbs when either approaches its cap. No
    # cap = no alert, but we still log at INFO when non-zero so
    # operators see outage start. Independent of shipper /
    # drainer cadences (60s default vs 30s drain) to keep
    # alert granularity matched to typical Sentry evaluation.
    observability_task = asyncio.create_task(run_observability_loop(r))

    # Diagnostic obs-stream drainer. Started unconditionally — when
    # `TDS_DIAG_OBS_STREAM=false` (production default) the queue
    # stays empty and the drain loop is a no-op every 100ms. Cheap
    # to leave running; avoids a code-path divergence between
    # prod and staging.
    diag_drain_task = asyncio.create_task(run_obs_drain(r))

    yield

    # Shutdown — cancel all background tasks and await each so
    # graceful shutdown completes within the FastAPI lifespan
    # window. CancelledError is the expected exit and is silently
    # absorbed; any other exception propagates.
    shipper_task.cancel()
    sync_task.cancel()
    disk_drainer_task.cancel()
    observability_task.cancel()
    diag_drain_task.cancel()
    try:
        await shipper_task
    except asyncio.CancelledError:
        pass
    try:
        await sync_task
    except asyncio.CancelledError:
        pass
    try:
        await disk_drainer_task
    except asyncio.CancelledError:
        pass
    try:
        await observability_task
    except asyncio.CancelledError:
        pass
    try:
        await diag_drain_task
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
    x_test_id: str = Header("", alias="X-Test-Id"),
):
    """Main routing endpoint. Called by CF Worker for every click."""
    t_endpoint_start = time.perf_counter()

    # Auth check (timing-safe)
    if settings.tds_secret_key and (not x_tds_key or not hmac.compare_digest(x_tds_key, settings.tds_secret_key)):
        raise HTTPException(status_code=403, detail="Invalid TDS key")

    # Traffic simulation framework (Phase 1, 2026-05-09) +
    # diagnostic toolkit (Phase 1, 2026-05-10).
    #
    # Tag Sentry span with test_id so verifier can find every event for
    # one synthetic via `mcp__sentry__search_events tags.test_id:<uuid>`.
    # Echoed back in the response so Worker → debug JSON surfaces the
    # round-trip for assertion. Purely additive — production traffic
    # without the header is unchanged.
    #
    # ALSO bind to the request-scoped context var so emit_checkpoint()
    # calls in route() / cascade() / sync_client.apply_snapshot() etc.
    # can append to obs:test:<id> Redis stream without threading the
    # test_id through every signature. The context var is local to
    # this request — concurrent /decide calls each see their own.
    if x_test_id:
        sentry_sdk.set_tag("test_id", x_test_id)
        set_test_id(x_test_id)
        emit_checkpoint("click.decide_in", {
            "click_id": req.click_id,
            "country": req.country,
            "city": req.city,
            "ip": req.ip,
            "user_agent": req.user_agent[:120],
            "node_id": settings.node_id,
        })

    # Route the click
    try:
        result = await route(req)
    except Exception as e:
        logger.error("route() failed: %s", e, extra={"click_id": req.click_id})
        sentry_sdk.capture_exception(e)
        emit_checkpoint("click.route_failed", {"error": str(e)[:200]})
        return ClickResponse(
            url=f"{settings.fallback_url}?reason=error&click_id={quote(req.click_id, safe='')}",
            status=302,
        )

    if result is None:
        emit_checkpoint("click.no_match", {"click_id": req.click_id})
        return ClickResponse(
            url=f"{settings.fallback_url}?reason=no_match&click_id={quote(req.click_id, safe='')}",
            status=302,
        )

    # Routing decision resolved — emit summary checkpoint with the
    # selected campaign / offer / final URL so the trace timeline
    # shows exactly what the cascade picked. The route() function
    # also emits per-stage checkpoints internally (`click.cascade_*`,
    # `click.macro_*`) for finer detail.
    emit_checkpoint("click.action_resolved", {
        "campaign_id": result.get("campaign_id"),
        "offer_id": result.get("offer_id"),
        "url": result.get("url", "")[:200],
    })

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

    # Write to local stream.
    # T2.1 / G-22: inline `MAXLEN ~ N` enforces a hard ceiling on
    # the stream so a central-collector outage cannot grow it
    # unbounded → Redis OOM → routing degradation + click loss.
    # `approximate=True` (the redis-py `~` modifier) makes the trim
    # O(1) per XADD by trimming whole macro-nodes rather than
    # exact-counting; the cap is honoured to ±10% of target which
    # is more than sufficient for a defense-in-depth ceiling. Under
    # normal operation the shipper.run_shipper loop XTRIMs the
    # stream to ~10k after every successful batch ship, so this cap
    # is rarely exercised — it exists for the failure-mode tail.
    # Cap value is env-configurable (TDS_STREAM_CLICKS_MAXLEN);
    # default 1M ≈ ~500 MB Redis budget at ~500 B/click.
    t_stream = time.perf_counter()
    try:
        r = await get_redis()
        await r.xadd(
            "stream:clicks",
            {"data": json.dumps(click_record, default=str)},
            maxlen=settings.stream_clicks_maxlen,
            approximate=True,
        )
        emit_checkpoint("click.stream_xadd", {
            "click_id": req.click_id,
            "stream_write_ms": round((time.perf_counter() - t_stream) * 1000, 2),
        })
    except Exception as e:
        logger.error("Failed to write click to stream: %s", e, extra={"click_id": req.click_id})
        sentry_sdk.capture_exception(e)
        # T2.2 / G-23 — fall back to disk queue when Redis is
        # unreachable. Without this, every click during a Redis
        # outage was LOST: the log + Sentry capture above record
        # the symptom but the click_record itself never made it
        # to the stream → never to central → never to analytics.
        # The disk file is replayed by the background drainer
        # task once Redis recovers; nothing is lost provided
        # disk space holds (cap = TDS_DISK_QUEUE_MAX_FILES).
        enqueued = await enqueue_click_to_disk(click_record)
        if enqueued:
            logger.info(
                "Click %s queued to disk after Redis failure",
                req.click_id,
            )
        emit_checkpoint("click.disk_queue_fallback", {
            "click_id": req.click_id,
            "enqueued": enqueued,
            "error": str(e)[:200],
        })
        # If `enqueued is False`, enqueue_click_to_disk has
        # already logged + Sentry-captured the cap rejection or
        # write failure. The click is genuinely lost in that
        # path — operator's signal to scale Redis or raise the
        # cap.
    timing["stream_write_ms"] = round((time.perf_counter() - t_stream) * 1000, 2)
    timing["endpoint_total_ms"] = round((time.perf_counter() - t_endpoint_start) * 1000, 2)

    response = {"url": result["url"], "status": 302, "timing": timing}
    if x_test_id:
        response["echoed_test_id"] = x_test_id
        # Final checkpoint with full timing breakdown so the trace
        # timeline closes cleanly — the gap-detector flags requests
        # that emit decide_in but never decide_out as suspect.
        emit_checkpoint("click.decide_out", {
            "click_id": req.click_id,
            "url": result["url"][:200],
            "endpoint_total_ms": timing["endpoint_total_ms"],
            "stream_write_ms": timing.get("stream_write_ms"),
        })
    return response


@app.get("/health", response_model=HealthResponse)
async def health():
    """Health check."""
    r = await get_redis()
    try:
        redis_ok = await r.ping()
        campaigns_count = await r.scard("campaigns:active")
        sync_ver = await r.get("sync:version")
    except Exception:
        redis_ok = False
        campaigns_count = 0
        sync_ver = None

    return HealthResponse(
        node_id=settings.node_id,
        region=settings.node_region,
        redis=redis_ok,
        campaigns_loaded=campaigns_count,
        sync_version=int(sync_ver) if sync_ver else 0,
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


_MAX_COMPRESSED_BYTES = 50 * 1024 * 1024   # 50MB on the wire (zip-bomb gate)
_MAX_UNCOMPRESSED_BYTES = 500 * 1024 * 1024  # 500MB after gunzip — gives admin-api ~10x headroom for the same 50MB-on-wire cap when gzip is on (closes G-16 / T1.2)


@app.post("/admin/sync")
async def receive_sync(
    request: Request,
    x_tds_key: str = Header("", alias="X-TDS-Key"),
    x_tds_body_sig: str = Header("", alias="X-TDS-Body-Sig"),
    x_test_id: str = Header("", alias="X-Test-Id"),
):
    """Receive routing snapshot from central admin-api.

    Called by central after full_sync, or manually.
    Replaces all routing data in local Redis with snapshot.
    Auth required via X-TDS-Key header.

    Body MAY be gzip-compressed when admin-api has
    `TDS_SYNC_PUSH_GZIP_ENABLED=true`. Detected via
    `Content-Encoding: gzip`. Without that header the body is
    parsed as plain JSON (legacy / older admin-api builds).

    Optional body integrity (T2.4): if admin-api ships an
    ``X-TDS-Body-Sig: sha256=<hex>`` header, the on-the-wire
    body bytes are verified against the HMAC-SHA256 of those
    bytes computed with ``tds_secret_key``. Mismatch → 401.
    Older admin-api builds that don't ship the header continue
    to work unchanged (lenient mode for the rolling-deploy
    window). Once the entire fleet of admin-api builds emits
    the sig, operators can ratchet up to "require-sig" via a
    future config flag — but that's a follow-up; today's
    contract is "if present, verify; if absent, don't reject".

    Two-stage size guard (T1.2 / G-16):
      - On-the-wire body capped at 50MB (`_MAX_COMPRESSED_BYTES`).
        Gzip-bomb defense — even a worst-case 1000:1 compression
        ratio caps in-memory expansion at 50GB, which the
        post-decompress cap below catches.
      - After gunzip, decoded JSON capped at 500MB
        (`_MAX_UNCOMPRESSED_BYTES`). This is the EFFECTIVE
        snapshot size cap. With gzip on, admin-api can ship
        ~500MB worth of JSON in ~50MB on the wire (~85-90% ratio
        for routing config payloads).
    """
    # Auth (timing-safe)
    if settings.tds_secret_key and (not x_tds_key or not hmac.compare_digest(x_tds_key, settings.tds_secret_key)):
        raise HTTPException(status_code=403, detail="Invalid key")

    # Diagnostic correlation — when admin-api propagated X-Test-Id (per
    # `_build_push_headers` in admin-api SyncService), bind it so the
    # `node.sync_apply_*` checkpoints land in the same obs:test:<id>
    # stream as the upstream mutation.
    if x_test_id:
        sentry_sdk.set_tag("test_id", x_test_id)
        set_test_id(x_test_id)
        emit_checkpoint("node.sync_apply_start", {
            "node_id": settings.node_id,
            "content_encoding": request.headers.get("content-encoding", ""),
            "content_length": request.headers.get("content-length"),
            "has_body_sig": bool(x_tds_body_sig),
        })

    # Pre-decompress payload size guard.
    content_length = request.headers.get("content-length")
    if content_length and int(content_length) > _MAX_COMPRESSED_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"Payload too large on wire (max {_MAX_COMPRESSED_BYTES // (1024 * 1024)}MB)",
        )

    raw_body = await request.body()

    # T2.4 — body integrity check. Verify BEFORE decompression so
    # a corrupt gzip body fails the sig check (cleaner error
    # surface) rather than the gunzip step. Sig is over the EXACT
    # bytes that arrived on the wire — independent of compression.
    #
    # Lenient on absent header (older admin-api builds + dev mode
    # without tds_secret_key configured). Strict on present-but-
    # mismatched: that's the active-MITM scenario the sig defends
    # against, so 401 with no further processing.
    if x_tds_body_sig and settings.tds_secret_key:
        # Format `sha256=<hex>` mirrors GitHub webhook signature
        # convention. Future algos (sha512) can ship under the
        # same header; today we only accept sha256.
        if not x_tds_body_sig.startswith("sha256="):
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Unsupported X-TDS-Body-Sig algorithm: "
                    f"{x_tds_body_sig.split('=', 1)[0]!r}"
                ),
            )
        provided_hex = x_tds_body_sig[len("sha256="):]
        expected_hex = hmac.new(
            settings.tds_secret_key.encode("utf-8"),
            raw_body,
            hashlib.sha256,
        ).hexdigest()
        # Timing-safe compare — avoids per-byte timing leak that
        # could let an attacker reconstruct a forged sig.
        if not hmac.compare_digest(provided_hex, expected_hex):
            logger.warning(
                "X-TDS-Body-Sig mismatch — rejecting tampered or "
                "forged sync push (T2.4 closure).",
            )
            raise HTTPException(
                status_code=401,
                detail="Body signature mismatch",
            )

    # Optional gzip decompression. The admin-api side gates on
    # `TDS_SYNC_PUSH_GZIP_ENABLED`; this end is dual-decode (always
    # accepts both, additive change). Older admin-api builds that
    # don't set the header continue to work unchanged.
    encoding = request.headers.get("content-encoding", "").strip().lower()
    if encoding == "gzip":
        try:
            decoded = gzip.decompress(raw_body)
        except (OSError, EOFError) as exc:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid gzip body: {exc}",
            )
        # Post-decompress cap — the actual snapshot semantic limit.
        if len(decoded) > _MAX_UNCOMPRESSED_BYTES:
            raise HTTPException(
                status_code=413,
                detail=(
                    f"Snapshot too large after decompression "
                    f"(max {_MAX_UNCOMPRESSED_BYTES // (1024 * 1024)}MB)"
                ),
            )
        raw_body = decoded
    elif encoding and encoding != "identity":
        # Unknown encoding — fail clearly rather than silently
        # treating as plain JSON. `identity` is the spec-permitted
        # "no encoding" value, accept it as a no-op.
        raise HTTPException(
            status_code=415,
            detail=f"Unsupported Content-Encoding: {encoding!r}",
        )

    try:
        snapshot = json.loads(raw_body)
    except (ValueError, json.JSONDecodeError):
        raise HTTPException(status_code=400, detail="Invalid JSON")

    # Version guard: reject downgrades (prevent accidental or malicious rollback)
    r = await get_redis()
    incoming_version = snapshot.get("sync_version", 0)
    current_version_str = await r.get("sync:version")
    current_version = int(current_version_str) if current_version_str else 0

    if current_version > 0 and incoming_version < current_version:
        logger.warning("Rejected sync downgrade: incoming v%d < current v%d", incoming_version, current_version)
        return {
            "status": "rejected",
            "reason": f"version downgrade: incoming v{incoming_version} < current v{current_version}",
            "keys_written": 0,
        }

    stats = await apply_snapshot(r, snapshot)

    logger.info("Sync received: %d keys written (v%d)", stats.get("keys_written", 0), incoming_version)
    if x_test_id:
        emit_checkpoint("node.sync_apply_done", {
            "node_id": settings.node_id,
            "sync_version": incoming_version,
            "keys_written": stats.get("keys_written", 0),
            "stale_removed": stats.get("stale_removed", 0),
            "elapsed_ms": stats.get("elapsed_ms"),
        })
    return stats


@app.post("/admin/seed")
async def seed_data(x_tds_key: str = Header("", alias="X-TDS-Key")):
    """Load default routing data into local Redis.

    DISABLED in production. Only available in development mode.
    """
    if settings.environment == "production":
        raise HTTPException(status_code=403, detail="Seed disabled in production. Use /admin/sync.")

    # T1.13 (G-30 closure 2026-05-08) — `hmac.compare_digest` instead of
    # `!=`. Mirrors `/decide` + `/admin/sync` (lines 100, 266) and rule
    # `sync-protocol` → "hmac.compare_digest for auth". Defense-in-depth:
    # `/admin/seed` is dev-only AND already gated by environment check
    # above, so no production risk today, but the inconsistency was a
    # foot-gun for any future operator copying this block to a new
    # endpoint. Regression-fenced by tests/unit/test_admin_auth_timing_safe.py.
    if settings.tds_secret_key and (not x_tds_key or not hmac.compare_digest(x_tds_key, settings.tds_secret_key)):
        raise HTTPException(status_code=403, detail="Invalid key")

    r = await get_redis()
    pipe = r.pipeline()

    # Campaign 1: US Mobile CPA
    pipe.hset("campaign:1", mapping={
        "name": "US Mobile CPA", "status": "active", "priority": "10",
        "weight": "100", "daily_cap": "10000", "frequency_cap": "3", "frequency_period": "86400",
    })
    pipe.set("campaign:1:has_geo", "1")
    pipe.set("campaign:1:has_device", "1")
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
    pipe.set("campaign:2:has_geo", "1")
    pipe.set("campaign:2:has_os", "1")
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
