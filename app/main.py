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

from app.config import _LOCAL_ENVIRONMENTS, settings
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
from app.shipper import assert_shipper_ready, run_shipper
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

    # F.29 Sprint 1.2 — synchronous shipper-config validation BEFORE
    # task creation. If shipper cannot safely start (empty central_url
    # in non-local env + TDS_REQUIRE_CENTRAL_URL=true) this raises
    # ShipperDisabledError. The exception propagates out of lifespan
    # so uvicorn fails boot with the traceback visible — exactly the
    # behaviour audit 2026-05-16 demanded (the 50-day silent-disable
    # of AU+CA shippers must be impossible to repeat).
    #
    # Defense-in-depth check (the Settings._enforce_central_url_presence
    # validator above already catches the case at config-construction
    # time). Reaching the raise here means env was mutated post-boot
    # OR a test bypassed validation via Settings.model_construct() —
    # in either case fail-closed is correct.
    assert_shipper_ready()

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


# ============================================================================
# Shared X-TDS-Key auth helper — F.24 Phase 1 dual-path + H6 fix
# ============================================================================
#
# SINGLE-PATH PER-WORKER AUTH (F.24 Phase 1, 2026-05-14;
#                              F.25 legacy branch removed 2026-05-16)
# --------------------------------------------------------------------
# Admin-api emits a per-Worker `TDS_SECRET_KEY` (see WorkerService.
# deploy_script + sync builder `workers.py`). The auth path discovers
# which Worker sent a request by hashing the incoming `X-TDS-Key` with
# sha256 and looking up `worker_secret_hash:{hex} → worker_id` in
# local Redis.
#
# The legacy global `settings.tds_secret_key` fallback inside
# `_check_tds_key` was REMOVED in F.25 cleanup. It was the F.24
# Phase 1 dual-window net while Workers carried the old global secret
# baked into their CF script. Migration 057 backfilled EVERY existing
# `workers` row with `tds_secret_key_encrypted` (the global value,
# encrypted) so `sha256(global) → worker_id` is already in the
# per-Worker Redis index for every active+deployed Worker — a Worker
# still presenting the global header authenticates via the per-Worker
# path, NOT a special-case branch. This was verified DETERMINISTICALLY
# on staging 2026-05-16 (every `workers` row has a per-Worker secret;
# 0 active+deployed Workers missing one) — the dual-window's purpose
# was to reach exactly this state, confirmed by DB fact rather than a
# calendar soak (geo-tds is pre-MVP / no production traffic, so a
# time-based soak yields no signal — see
# `docs/roadmap/stage-1a-research/f25-destructive-cleanup-plan.md`).
#
# The global secret itself is NOT removed — it remains the credential
# for the sync push/pull + edge-node channel (admin-api sync auth +
# X-TDS-Body-Sig both directions, click-processor pull client, edge
# provisioning). ONLY the `_check_tds_key` Worker-auth fallback is
# gone. A Worker that genuinely missed rotation now fails CLOSED (403)
# instead of being silently masked by the global secret — the correct
# end-state. Reversible: revert this commit re-adds the branch.
#
# Hot-path cost: one Redis GET (~0.5ms) added to /decide's 10ms budget
# — within the contract documented in `diagnostic-mode` rule. The
# Redis call uses the singleton client (`get_redis()`) so no
# connection allocation overhead per request.

def _hash_secret(plaintext: str) -> str:
    """sha256 hex digest of the secret. Identical to the admin-api
    sync builder's hash (see `app/sync/builders/workers.py`) so the
    two sides agree on the lookup key.
    """
    return hashlib.sha256(plaintext.encode("utf-8")).hexdigest()


async def _check_tds_key(x_tds_key: str) -> int:
    """Validate the `X-TDS-Key` header. Raise 403 on failure.

    Returns: worker_id (int ≥ 1) — the Worker whose per-Worker secret
    matched. Never returns 0/None (the F.24 Phase 1 legacy sentinel
    `0` was removed with the legacy branch in F.25). The return is
    advisory; callers may ignore it today (no caller pattern requires
    forensic attribution yet — F.24 plan §0.2 deferred phase).

    Algorithm (single per-Worker path, F.25 2026-05-16):
      1. Reject empty `x_tds_key` outright (403).
      2. PER-WORKER lookup. Hash `x_tds_key` with sha256, GET
         `worker_secret_hash:{hex}` in local Redis. A hit returns
         the originating Worker id.
      3. ANYTHING ELSE (miss / corrupted index entry / Redis error)
         → raise HTTPException(403). FAIL CLOSED.

    Why no legacy global-secret fallback (F.25):
      The F.24 Phase 1 dual-window fell back to a constant-time
      compare against `settings.tds_secret_key` on per-Worker miss.
      Migration 057 backfilled EVERY `workers` row with the global
      value (encrypted) as its per-Worker secret, so
      `sha256(global) → worker_id` is already in the per-Worker index
      for every active+deployed Worker — a Worker still presenting
      the global header authenticates via step 2, not a branch.
      Verified deterministically on staging 2026-05-16 (all Workers
      carry a per-Worker secret; 0 active+deployed missing). Removing
      the branch makes a genuinely-unrotated Worker fail CLOSED —
      the correct end-state. The global secret itself is NOT removed:
      it remains the sync push/pull + edge-node channel credential
      (admin-api sync auth, X-TDS-Body-Sig both directions, pull
      client, edge provisioning) — see rule `outbound-http-safety`
      "Worker → Backend integrity".

    Fail-closed discipline (H6 fix, 2026-05-11):
      An empty `x_tds_key` is rejected at step 1. Empty/misconfigured
      `settings.tds_secret_key` can no longer auto-authenticate any
      Worker here (the secret is not consulted on this path at all) —
      strictly more fail-closed than the pre-F.25 H6 invert.

    Timing safety:
      No string comparison of the secret happens here anymore — auth
      is a one-way sha256 digest → Redis key lookup. A timing
      observation that reveals "key exists in Redis" does not leak
      the secret (digests are one-way). The `hmac.compare_digest`
      timing discipline still governs the sync-channel auth paths
      (`_admin_sync` body-sig, `sync/router`) — out of scope here.

    Redis-unavailable mode:
      If Redis is unreachable the per-Worker lookup fails → 403
      (fail-closed). `/decide` cannot serve a request without Redis
      anyway (SINTER/SETNX/XADD — `architecture.md` "Click Processor
      → Redis down → Fail-closed"), so failing auth closed here is
      consistent, not a new outage surface.
    """
    provided = x_tds_key or ""
    if not provided:
        raise HTTPException(status_code=403, detail="Invalid TDS key")

    # ── Per-Worker path (F.24 Phase 1) ──────────────────────────────
    try:
        r = await get_redis()
        digest = _hash_secret(provided)
        worker_id_str = await r.get(f"worker_secret_hash:{digest}")
        if worker_id_str:
            # Production redis-py is configured with `decode_responses=True`
            # (see `app/redis_client.py:15`) so every value is already a
            # str. The defensive bytes-decode branch that previously lived
            # here was dead code per the F.24 Phase 1 audit (Agent B MED).
            # If a future redis client config flips that off, the
            # `int(worker_id_str)` cast below will raise TypeError on
            # bytes input and the except branch falls through to legacy.
            try:
                return int(worker_id_str)
            except (TypeError, ValueError):
                # Corrupted index entry — log + fall through to
                # legacy. Don't fail the auth because a sync glitch
                # corrupted a row; the legacy global secret is the
                # safety net.
                logger.warning(
                    "worker_secret_hash:%s → non-integer worker_id=%r; "
                    "falling through to legacy global secret check.",
                    digest, worker_id_str,
                )
    except HTTPException:
        raise
    except Exception as e:
        # Redis unreachable or transient error — FAIL CLOSED (403).
        # The F.24 Phase 1 dual-window fell through to the legacy
        # global secret here; F.25 removed that branch (every Worker
        # carries a per-Worker secret — verified deterministically
        # 2026-05-16). `/decide` cannot serve a request without Redis
        # anyway (SINTER/SETNX/XADD — `architecture.md` fail-closed),
        # so failing auth closed here is consistent, not a new outage
        # surface. Log loud — a persistent Redis outage is a separate
        # incident affecting many other code paths.
        logger.warning(
            "_check_tds_key: per-Worker lookup failed (%s); "
            "failing closed (403) — no legacy fallback (F.25).", e,
        )

    # Per-Worker miss / corrupted index entry / Redis error → 403.
    # No legacy global-secret fallback (removed F.25 — see the
    # SINGLE-PATH PER-WORKER AUTH note above the helper).
    raise HTTPException(status_code=403, detail="Invalid TDS key")


# ============================================================================
# Click event-time resolution (F.24 Phase 5.1b)
# ============================================================================

def _resolve_click_timestamp(click_ts: str | None) -> str:
    """Canonical click instant for the click record / collector created_at.

    Returns the EDGE-generated `click_ts` verbatim when the CF Worker
    supplied it (the F.24-Phase-5.1b path: one value, captured once at
    the edge, serialised once in raceBackends → byte-identical across
    every node in a true-racing fan-out, so the collector's
    `ON CONFLICT (click_id, created_at)` collapses N raced inserts to
    ONE row). Falls back to this node's own UTC second when absent —
    non-Worker callers (smoke scripts, integration tests) and the
    dual-deploy window where an older Worker has not yet rolled out;
    those are not subject to racing fan-out so per-node time is
    acceptable. `or` intentionally treats "" the same as None (a
    Pydantic-stripped empty string must not become the click's time).

    Pure (no I/O) so it is unit-tested directly rather than through the
    full /decide path — mirrors the worker `fetchImpl` testability seam.
    """
    return click_ts or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


# ============================================================================
# Click idempotency gate (H1 fix, 2026-05-11)
# ============================================================================

async def _acquire_click_dedup(click_id: str) -> bool | None:
    """Atomic first-seen check for a click_id via Redis SETNX.

    Returns:
        True  — first time we see this click_id (caller proceeds with XADD).
        False — duplicate detected (caller skips XADD; the click is already
                in the stream from a prior /decide call for the same id).
        None  — dedup unavailable (Redis error, or feature disabled by
                operator). Caller MUST fail-open and proceed with XADD;
                downstream ClickHouse dedup on click_id is the safety net.

    Atomicity: `SET key NX EX ttl` is a single Redis primitive — exactly
    one of N concurrent calls with the same click_id wins. No race window.

    Disabled path: when `settings.click_dedup_ttl_seconds == 0`, the
    function short-circuits to None (no Redis call, no overhead). This is
    the operator's escape hatch for incident response — restore previous
    behaviour without a redeploy.

    Live evidence motivating this gate: Sentry GEO-TDS-WORKER-1 showed
    18 events / 9 users in the last hour before audit 2026-05-11. The
    same click_id was XADD'd from BOTH AU and CA nodes within ~3 seconds
    because the Worker's 2s `AbortSignal.timeout` fires roughly when
    click-processor's response is still on the wire (EU→AU 290ms each
    way + ~1.7s click-processor under burst load).

    SCOPE — this SETNX is NODE-LOCAL (each edge node has its own Redis;
    `settings.redis_url` = the node's own instance). It dedups
    SAME-NODE retries only. It does NOT and never did dedup the SAME
    click_id arriving at DIFFERENT nodes. Pre-F.24-Phase-5 the
    Worker's sequential fallback hit a second node only on a timeout
    (the rare evidence above). F.24 Phase 5 makes the Worker race ALL
    nodes for EVERY click → multi-node-same-click is now the NORM, far
    beyond what a node-local gate can cover. The CROSS-NODE
    idempotency guarantee is therefore the COLLECTOR's
    `ON CONFLICT (click_id, created_at) DO NOTHING` — which only works
    if `created_at` is byte-identical across raced nodes. F.24
    Phase 5.1b makes that hold: `created_at` is sourced from the
    EDGE-generated `click_ts` (req.click_ts → click_record["timestamp"]
    → collector), captured ONCE at the Worker and serialised ONCE in
    raceBackends, so every raced node ships the identical
    (click_id, created_at). This node-local SETNX remains a useful
    fast-path (skips a redundant same-node stream write) but is no
    longer load-bearing for cross-node correctness.
    """
    if settings.click_dedup_ttl_seconds <= 0:
        # Operator-disabled — fail-open to legacy behaviour.
        return None
    try:
        r = await get_redis()
        # `SET key value NX EX ttl` returns truthy on first-set, None on
        # collision. redis-py exposes this via `r.set(..., nx=True, ex=...)`
        # which returns True / None. Normalise None → False here so the
        # caller's branch logic is simple (`is False`).
        acquired = await r.set(
            f"click:seen:{click_id}",
            "1",
            nx=True,
            ex=settings.click_dedup_ttl_seconds,
        )
        return True if acquired else False
    except Exception as exc:
        # Redis hiccup → log + fail-open. Better one duplicate than one
        # lost click. ClickHouse downstream dedup on click_id PK is the
        # eventual safety net. Sentry-capture is intentional — operator
        # signal that dedup is degraded (not silent).
        logger.warning(
            "Click dedup SETNX failed for %s: %s — failing open",
            click_id, exc,
        )
        sentry_sdk.capture_exception(exc)
        return None


@app.post("/decide")
async def decide(
    req: ClickRequest,
    x_tds_key: str = Header("", alias="X-TDS-Key"),
    x_test_id: str = Header("", alias="X-Test-Id"),
):
    """Main routing endpoint. Called by CF Worker for every click."""
    t_endpoint_start = time.perf_counter()

    # Auth check (timing-safe + fail-closed per H6 fix).
    await _check_tds_key(x_tds_key)

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

    # Audit closure 2026-05-10 (Code-reviewer #1 HIGH-1): the previous
    # implementation only emitted `click.decide_out` on the SUCCESS
    # path. Every error / no_match probe failed the gap-detector's
    # open-without-close invariant — the trace CLI flagged exactly the
    # failure cases operators most need to investigate. Fix: emit
    # `click.decide_out` on EVERY exit path with an `outcome` label so
    # the timeline is honest about what happened. We don't use a
    # try/finally wrapper because the function has only three exit
    # points and explicit emits keep each path's payload (route_error
    # carries the exception, no_match carries the click_id, success
    # carries timing) without a central outcome tracker.

    # Route the click
    try:
        result = await route(req)
    except Exception as e:
        logger.error("route() failed: %s", e, extra={"click_id": req.click_id})
        sentry_sdk.capture_exception(e)
        emit_checkpoint("click.route_failed", {"error": str(e)[:200]})
        if x_test_id:
            emit_checkpoint("click.decide_out", {
                "click_id": req.click_id,
                "outcome": "route_error",
                "endpoint_total_ms": round((time.perf_counter() - t_endpoint_start) * 1000, 2),
            })
        return ClickResponse(
            url=f"{settings.fallback_url}?reason=error&click_id={quote(req.click_id, safe='')}",
            status=302,
        )

    if result is None:
        emit_checkpoint("click.no_match", {"click_id": req.click_id})
        if x_test_id:
            emit_checkpoint("click.decide_out", {
                "click_id": req.click_id,
                "outcome": "no_match",
                "endpoint_total_ms": round((time.perf_counter() - t_endpoint_start) * 1000, 2),
            })
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
        # F.24 Phase 5.1b — EDGE-generated canonical click instant (the
        # cross-node dedup anchor for true racing). Full rationale +
        # fallback semantics in `_resolve_click_timestamp`.
        "timestamp": _resolve_click_timestamp(req.click_ts),
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

    # H1 fix (2026-05-11): idempotency gate BEFORE XADD.
    #
    # Without this, a CF Worker retry on its 2s `AbortSignal.timeout`
    # produced a SECOND /decide call for the same click_id. Live
    # evidence (Sentry GEO-TDS-WORKER-1 + audit 2026-05-11): the same
    # click_id (e.g. `019e1407e312e5ba5d38b0f9`) successfully resolved
    # on BOTH AU and CA edge nodes within ~3 seconds, both XADD'd to
    # `stream:clicks` → ClickHouse double-counted the click + the
    # postback chain fired twice for the same advertiser.
    #
    # `SET click:seen:<id> NX EX <ttl>` is atomic — exactly one of
    # the concurrent /decide calls returns `True` (sets the marker),
    # the other returns `False` (marker existed) and skips its XADD.
    # Both calls STILL return the routing URL to the Worker so the
    # user-facing 302 is unaffected — only the analytics-side
    # double-write is prevented. This matches the data-flow.md
    # design intent of `click:{click_id}` TTL 30d and aligns the
    # postback dedup pattern (event:{click_id}:{type} SETNX 30d)
    # already documented in data-flow.md.
    #
    # Fail-open semantics: if the SET call itself fails (Redis
    # impaired), we LOG + skip dedup and proceed with XADD. A
    # duplicate is preferable to a lost click in that case — the
    # central collector + ClickHouse can deduplicate downstream
    # (click_id is the natural primary key per data-flow.md).
    #
    # Operator escape hatch: `click_dedup_ttl_seconds=0` disables
    # the gate entirely (skip both SET and the check). Use only for
    # incident response (Redis OOM, retry-storm during deploy).
    dedup_ok = await _acquire_click_dedup(req.click_id)
    if dedup_ok is False:
        # Duplicate detected — return success without re-writing the
        # stream / disk fallback. Worker user still gets the redirect.
        emit_checkpoint("click.duplicate_skipped", {
            "click_id": req.click_id,
            "outcome": "duplicate",
        })
        timing["endpoint_total_ms"] = round(
            (time.perf_counter() - t_endpoint_start) * 1000, 2,
        )
        response = {"url": result["url"], "status": 302, "timing": timing}
        if x_test_id:
            response["echoed_test_id"] = x_test_id
            emit_checkpoint("click.decide_out", {
                "click_id": req.click_id,
                "outcome": "duplicate",
                "url": result["url"][:200],
                "endpoint_total_ms": timing["endpoint_total_ms"],
            })
        return response
    # dedup_ok is True (first-seen) or None (Redis dedup unavailable
    # — fail-open, proceed with XADD as before).

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
        # Success-path checkpoint with full timing breakdown.
        # Mirrors the early-return decide_out emits above (route_error,
        # no_match) — every exit path emits decide_out so the trace
        # timeline always closes cleanly. Outcome label distinguishes
        # the success path from failure paths in the rendered output.
        emit_checkpoint("click.decide_out", {
            "click_id": req.click_id,
            "outcome": "matched",
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
    # Auth (timing-safe + fail-closed per H6 fix).
    await _check_tds_key(x_tds_key)

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

    DISABLED outside local/development. Only available when the
    `_LOCAL_ENVIRONMENTS` membership check passes.
    """
    # M8 fix (2026-05-11): was `== "production"`, which only blocked
    # the exact string "production". A `TDS_ENVIRONMENT=staging` (or
    # any typo / capitalisation drift) silently passed this gate and
    # then a separate auth issue (H6 fail-open when `tds_secret_key=""`)
    # could let an attacker hit /admin/seed → overwrite Redis routing
    # data with hardcoded campaigns 1/2/3 → real clicks redirected
    # to `https://example.com/offer-*` placeholders. Two independent
    # gate failures compounded to unauthenticated seed on staging.
    #
    # Now uses the same `_LOCAL_ENVIRONMENTS` frozenset that the
    # config's `_enforce_secret_presence` guard uses — single source
    # of truth for "is this a sandbox env that allows dev affordances".
    # If `environment` is not in {"local", "development"}, /admin/seed
    # is 403 regardless of auth header (because seed has no business
    # being callable on staging or production).
    if settings.environment not in _LOCAL_ENVIRONMENTS:
        raise HTTPException(
            status_code=403,
            detail=(
                "Seed disabled outside local/development environments. "
                "Use /admin/sync for staging/production routing data."
            ),
        )

    # Shared `_check_tds_key` helper (H6 fix 2026-05-11 consolidated
    # the per-endpoint inline checks here). F.25 (2026-05-16): the
    # helper is now single-path per-Worker — a sha256(X-TDS-Key) →
    # `worker_secret_hash:{hex}` Redis lookup; miss / Redis-error →
    # 403 FAIL CLOSED. No `hmac.compare_digest` against the global
    # secret anymore (legacy fallback removed) → strictly more
    # fail-closed AND more timing-safe (one-way digest lookup, no
    # secret string-compare). Contract regression-fenced by
    # tests/unit/test_admin_auth_timing_safe.py +
    # test_check_tds_key_h6.py.
    await _check_tds_key(x_tds_key)

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
