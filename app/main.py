"""Click-processor — FastAPI service for TDS routing decisions.

Receives click parameters from CF Worker, evaluates routing rules
against local Redis, returns destination URL for redirect.

Mirror tip: any commit on `services/click-processor/` lands here +
gets auto-mirrored to `tutoroton/tds-click-processor` (CI on stage
merge); admin-api polls the mirror tip every 120s and flags
`update_available=true` for nodes still on an earlier SHA → operator
sees the "Update available" badge in admin-panel → can roll forward
via Deploy CTA.
"""

import asyncio
import gzip
import hashlib
import hmac
import json
import logging
import shutil
import time
from datetime import datetime, timezone
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
from app.shipper_metrics import metrics as shipper_metrics
from app.telemetry import OP_DISK_PRESSURE, capture_op_msg
from app.disk_queue import (
    check_disk_pressure,
    enqueue_click as enqueue_click_to_disk,
    get_queue_size,
    run_drainer as run_disk_drainer,
)
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
        # F.40 — when attached to a tenant Sentry account, provisioning
        # bakes `sentry_environment` = node_id so events split by instance
        # within one shared project. Empty → the deploy environment
        # (TDS_ENVIRONMENT) — unattached / legacy nodes unchanged.
        environment=settings.sentry_environment or settings.environment,
        # F.40 PII hardening (CRIT-001) — never let the SDK auto-attach
        # the visitor IP / request body to events that may now ship to a
        # third-party tenant Sentry. `before_send` additionally truncates
        # any IP the code attaches + strips the request body.
        send_default_pii=False,
        # F.32 — release = the node's running git SHA (settings.code_version,
        # stamped by render-env/update.sh), so Sentry events map to the exact
        # deployed revision per env (was a hardcoded "0.1.0" → every node looked
        # identical in Sentry regardless of code). "unknown" for pre-F.32/local.
        release=f"geo-tds-backend@{settings.code_version}",
        server_name=f"{settings.node_id}",
    )

START_TIME = time.time()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup validation [C2 fix]
    if settings.environment == "production" and not settings.tds_secret_key:
        logger.warning("TDS_SECRET_KEY is empty in production — auth disabled!")

    # F-4 MEDIUM (audit 2026-05-25) — smoke-probe authenticator visibility.
    # When `smoke_probe_secret` is unset in a non-local env, the /decide
    # smoke-test bypass falls back to X-TDS-Key alone (a forge vector — a
    # key-holder or log/stream observer can drive a false activation). The
    # design deliberately does NOT boot-guard this (onboarding-only path,
    # graceful degradation) and already WARNs on each bypass — but surface
    # it ONCE at startup too (log + Sentry) so the misconfig is visible
    # before any probe arrives, without changing the graceful behaviour.
    if settings.environment not in _LOCAL_ENVIRONMENTS and not settings.smoke_probe_secret:
        logger.warning(
            "TDS_SMOKE_PROBE_SECRET is empty in env=%s — /decide smoke bypass "
            "is authenticated by X-TDS-Key alone (forgeable). Configure the "
            "shared smoke-probe secret to close the false-activation vector.",
            settings.environment,
        )
        sentry_sdk.capture_message(
            "smoke-probe secret unset in non-local — /decide smoke bypass "
            "forgeable via X-TDS-Key alone",
            level="warning",
        )

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


def _sync_secret_matches(x_tds_key: str) -> bool:
    """Constant-time check of the static admin→node sync credential.

    The config push on ``/admin/sync`` is authenticated by the node's
    STATIC shared secret (``settings.tds_secret_key``) — exactly the
    credential the ``_check_tds_key`` docstring designates: "the global
    secret … remains the sync push/pull + edge-node channel credential
    (admin-api sync auth …)". This is a DIFFERENT trust model from
    ``/decide`` Worker→backend routing, which resolves a *per-Worker*
    secret via the local ``worker_secret_hash`` index.

    Why this exists (F.33, 2026-05-24): gating ``/admin/sync`` solely on
    ``_check_tds_key`` was a chicken-and-egg. That index is EMPTY on a
    freshly-provisioned node — it is populated *by* a sync (the snapshot
    ships ``worker_secret_hash:*`` keys). So the very first config push,
    the one that would seed the index, was rejected 403, and a fresh node
    could never bootstrap its routing config (it went ``active`` via the
    smoke probe yet stayed config-empty, mis-routing live traffic). The
    node validating the push against the secret it baked at provision
    time — the same value admin-api signs the push with — breaks the
    cycle. Direct analogue of the ``/decide`` smoke-probe bypass.

    Safe re F.25: that change removed the global-secret fallback from
    ``/decide`` ROUTING auth (per-Worker secrets so a leaked global key
    cannot route Worker traffic). The admin→node push channel is a
    distinct threat model where the shared edge-node secret IS the
    legitimate credential — the body-sig verifier (T2.4) already trusts
    the same ``settings.tds_secret_key``.

    Returns True iff the presented key equals the baked secret. Empty
    header or empty/unset secret → False (fail-closed). Constant-time
    compare — no secret-length or content timing leak.
    """
    provided = x_tds_key or ""
    expected = settings.tds_secret_key or ""
    if not provided or not expected:
        return False
    return hmac.compare_digest(provided, expected)


# ============================================================================
# Click event-time resolution (F.24 Phase 5.1b)
# ============================================================================

def _resolve_fallback_url() -> str:
    """Resolve the global fallback URL for blocked / no-match clicks.

    Single chokepoint so the fallback destination is configurable. Today it
    returns the node's env-configured ``settings.fallback_url``. GROUNDWORK
    (F-1, 2026-05-25): this is the one place an admin-pushed override will be
    read from synced config (delivered via the snapshot push) — wiring the
    admin settings UI is a tracked roadmap item. Centralising the three call
    sites (route_error / no_match / blocked) here means that future change is a
    one-function edit with NO behavioural change today.
    """
    return settings.fallback_url


# Plausibility window for a click_id-derived ms epoch: 2020-01-01 ..
# 2100-01-01. A genuine edge click_id always decodes inside this; a
# random/legacy 24-hex string whose prefix happens to be valid hex but
# is NOT an ms epoch almost always lands outside → we fall back to
# node-local time rather than fabricate an absurd created_at.
_CLICK_ID_MS_MIN = 1_577_836_800_000  # 2020-01-01T00:00:00Z
_CLICK_ID_MS_MAX = 4_102_444_800_000  # 2100-01-01T00:00:00Z


def _created_at_from_click_id(click_id: str | None) -> str | None:
    """Reconstruct the edge click instant from a UUIDv7-style click_id.

    The CF Worker's `generateClickId()` encodes `Date.now()` (ms epoch)
    in the first 12 hex chars and 6 random bytes (12 hex) after it — a
    fixed 24-hex-char id, identical across every racing node for one
    click. When the Worker did NOT forward an explicit `click_ts`
    header (an older Worker in the dual-deploy window), every node can
    still derive the SAME instant from the shared click_id, keeping the
    collector's `(click_id, created_at)` PK skew-immune — instead of
    each node stamping its own `gmtime()` and inflating one click into N
    rows.

    Returns an ISO-8601 ms-precision UTC string (matching JS
    `Date.toISOString()`) for a canonical 24-hex click_id whose prefix
    decodes to a plausible epoch; otherwise None so the caller falls
    back to node-local time. `smoke-test-*` ids and any legacy
    non-canonical id are NOT subject to true-racing fan-out, so
    per-node time is acceptable for them.

    Pure (no I/O) — unit-tested directly.
    """
    if click_id is None or len(click_id) != 24:
        return None
    try:
        ms = int(click_id[:12], 16)
    except ValueError:
        return None
    if not (_CLICK_ID_MS_MIN <= ms <= _CLICK_ID_MS_MAX):
        return None
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return f"{dt.strftime('%Y-%m-%dT%H:%M:%S')}.{dt.microsecond // 1000:03d}Z"


def _resolve_click_timestamp(click_ts: str | None, click_id: str | None = None) -> str:
    """Canonical click instant for the click record / collector created_at.

    Resolution order (each step byte-identical across a racing fan-out):
      1. EDGE-generated `click_ts` verbatim when the CF Worker supplied
         it (the F.24-Phase-5.1b path: one value, captured once at the
         edge, serialised once in raceBackends → identical across every
         node, so the collector's `ON CONFLICT (click_id, created_at)`
         collapses N raced inserts to ONE row).
      2. F-4 (audit 2026-05-25): when `click_ts` is absent but the
         click_id is the canonical UUIDv7-style form, derive the instant
         from its shared ms-prefix — still identical across nodes, so a
         click from an older Worker (no header) is ALSO skew-immune
         rather than fanning out into N per-node-`gmtime()` rows.
      3. Last resort — this node's own UTC second. Reached only by
         non-Worker callers (smoke scripts, integration tests) and
         non-canonical ids, which are not subject to racing fan-out so
         per-node time is acceptable.

    `or` intentionally treats "" the same as None (a Pydantic-stripped
    empty string must not become the click's time).

    Pure (no I/O) so it is unit-tested directly rather than through the
    full /decide path — mirrors the worker `fetchImpl` testability seam.
    """
    return (
        click_ts
        or _created_at_from_click_id(click_id)
        or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    )


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


# F.29 Sprint 3.6 (2026-05-23) — smoke-test click_id prefix.
#
# Synthetic clicks emitted by the admin-api ``EdgeNodeService._run_smoke_test``
# carry a ``smoke-test-<hex>`` ``click_id``. The /decide handler short-
# circuits these BEFORE the routing pipeline:
#   * skip campaign matching / Redis lookups / postback queue / dedup
#   * XADD a minimal payload to ``stream:clicks`` so the shipper sends
#     it to central; the central collector's deadletter forward is the
#     only behaviour-sensitive surface for smoke clicks (Sprint 3.6
#     also requires the collector to skip per-tenant deadletter
#     forwarding for this prefix — see services/collector/app/main.py).
#
# Operator-visible analytics (Sprint 4.2 dashboard widgets) MUST filter
# this prefix from real-traffic counters; the convention is pinned here
# and surfaced in skill `provisioning-edge-node`.
_SMOKE_TEST_CLICK_ID_PREFIX = "smoke-test-"

# F.29 Sprint 4.1 (TD-13, 2026-05-23) — HMAC smoke-probe freshness window.
# The admin-api stamps `issued_at` (unix seconds) into the probe header and
# signs it; /decide rejects probes older than this. The smoke-test timeout
# itself is 30s (admin-api side), so 120s gives generous clock-skew + retry
# headroom while bounding how long a captured (smoke_id, sig) pair can be
# replayed. A replay within the window only re-confirms an already-targeted
# node anyway (the signature binds the node_id-embedding smoke_id).
_SMOKE_PROBE_FRESHNESS_SECONDS = 120
# Tolerance for a probe whose issued_at is slightly AHEAD of this node's
# clock (admin-api vs node clock skew). Freshness is a ONE-DIRECTIONAL
# bound on PAST replay — a far-future issued_at must NOT be accepted (an
# `abs()` check would let a future-dated probe live for 2× the window).
_SMOKE_PROBE_CLOCK_SKEW_SECONDS = 5


def _verify_smoke_probe(click_id: str, probe_header: str) -> tuple[bool, str]:
    """Verify the ``X-TDS-Smoke-Probe`` HMAC for a smoke-test click (TD-13).

    Only consulted when ``settings.smoke_probe_secret`` is configured
    (enforce mode). The header format is ``<issued_at>.<hexsig>`` where
    ``hexsig = HMAC-SHA256(smoke_probe_secret, f"{click_id}.{issued_at}")``.

    Binding the signature to ``click_id`` (which embeds the activating
    node_id) means a captured header cannot be retargeted to another node;
    binding ``issued_at`` plus the freshness check bounds replay. The
    comparison is constant-time.

    Returns ``(ok, detail)`` — ``detail`` is a non-sensitive reason string
    safe to log / surface in the 403 body (it never echoes the secret or
    the expected signature).
    """
    secret = settings.smoke_probe_secret
    if not probe_header:
        return False, "missing X-TDS-Smoke-Probe header"
    issued_raw, sep, provided_sig = probe_header.partition(".")
    if not sep or not issued_raw or not provided_sig:
        return False, "malformed probe header (expected '<issued_at>.<sig>')"
    try:
        issued_at = int(issued_raw)
    except ValueError:
        return False, "probe issued_at is not an integer"
    # Asymmetric freshness: reject far-future issued_at (clock-skew bounded)
    # AND anything older than the window. Do NOT use abs() — that would let
    # a future-dated probe stay valid for 2× the window.
    skew = time.time() - issued_at  # > 0 ⇒ probe is in the past
    if skew < -_SMOKE_PROBE_CLOCK_SKEW_SECONDS:
        return False, "probe issued_at is in the future (clock skew / forgery?)"
    if skew > _SMOKE_PROBE_FRESHNESS_SECONDS:
        return False, (
            f"probe expired (age {skew:.0f}s > "
            f"{_SMOKE_PROBE_FRESHNESS_SECONDS}s freshness window)"
        )
    expected_sig = hmac.new(
        secret.encode(),
        f"{click_id}.{issued_at}".encode(),
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(expected_sig, provided_sig):
        return False, "probe signature mismatch"
    return True, "ok"


@app.post("/decide")
async def decide(
    req: ClickRequest,
    x_tds_key: str = Header("", alias="X-TDS-Key"),
    x_test_id: str = Header("", alias="X-Test-Id"),
    x_tds_smoke_probe: str = Header("", alias="X-TDS-Smoke-Probe"),
):
    """Main routing endpoint. Called by CF Worker for every click."""
    t_endpoint_start = time.perf_counter()

    is_smoke = req.click_id.startswith(_SMOKE_TEST_CLICK_ID_PREFIX)

    # ── Auth ────────────────────────────────────────────────────────────
    # F.33 (2026-05-24) — a smoke-test click authenticates via the TD-13
    # X-TDS-Smoke-Probe HMAC, NOT the per-Worker X-TDS-Key index. This is
    # essential: `_check_tds_key` resolves X-TDS-Key against the LOCAL
    # `worker_secret_hash` index, which is EMPTY on a freshly-provisioned node
    # (it's populated by sync/seed — and the activation smoke gate runs BEFORE
    # seed). So a per-Worker check 403s EVERY smoke on a fresh node, making the
    # gate impossible to pass without ?skip_smoke (the chicken-and-egg the
    # F.33 drill surfaced). The probe is a stronger, CONFIG-INDEPENDENT auth
    # (node_id-bound HMAC only admin-api can mint, replay-bounded) and the
    # smoke click short-circuits routing anyway — so authenticating it via the
    # probe and skipping `_check_tds_key` is strictly safer than the prior
    # "bypass on X-TDS-Key alone" path. Real (non-smoke) traffic is unchanged.
    smoke_probe_authed = False
    if is_smoke and settings.smoke_probe_secret:
        probe_ok, probe_detail = _verify_smoke_probe(req.click_id, x_tds_smoke_probe)
        if not probe_ok:
            _fp = hashlib.sha256(req.click_id.encode()).hexdigest()[:8]
            logger.warning(
                "Smoke-probe auth FAILED click_id_fp=%s node_id=%s: %s — REFUSED",
                _fp, settings.node_id, probe_detail,
            )
            sentry_sdk.add_breadcrumb(
                category="smoke.probe", level="warning",
                message="smoke probe authentication failed",
                data={"detail": probe_detail, "node_id": settings.node_id},
            )
            raise HTTPException(
                status_code=403,
                detail=f"smoke probe authentication failed: {probe_detail}",
            )
        smoke_probe_authed = True  # the probe IS the auth — skip per-Worker check
    if not smoke_probe_authed:
        # Normal traffic, AND the legacy smoke-without-probe case (a node or
        # admin-api predating the probe rollout). On a FRESH node the latter
        # still 403s here — that is the documented `?skip_smoke=true` case.
        await _check_tds_key(x_tds_key)

    # F.29 Sprint 3.6 — smoke-test bypass. Synthetic clicks from
    # ``EdgeNodeService._run_smoke_test`` carry a ``smoke-test-<hex>``
    # ``click_id``. We XADD a minimal record to ``stream:clicks`` so the
    # shipper delivers it to central (where the admin-api smoke gate is
    # polling ``stream:clicks-incoming``), then short-circuit the
    # routing path — no campaign matching, no postback, no dedup. The
    # synthetic click is purely a pipeline-liveness probe.
    #
    # Auth is still enforced above (smoke clicks come from operator-
    # invoked tooling against a legitimately deployed edge node with
    # the right X-TDS-Key secret). Length + charset already validated
    # by ClickRequest's regex ``^[a-zA-Z0-9_\\-]+$``.
    if is_smoke:
        # F.29 Sprint 3.7.1 (SEC-H001 leak-surface reduction) — do NOT
        # log the FULL smoke click_id. The admin-api smoke gate matches
        # on the exact `smoke-test-{node_id}-{hex}` value; the 64-bit
        # hex is the only thing standing between a collector-key holder
        # and a forged false-positive activation. Log only a sha256-derived
        # 8-char correlation token — enough to grep-correlate one smoke run
        # across services, but it reveals NONE of the actual hex (one-way).
        smoke_fp = hashlib.sha256(req.click_id.encode()).hexdigest()[:8]

        # The TD-13 X-TDS-Smoke-Probe HMAC was already verified in the auth
        # section above (when `smoke_probe_secret` is set it IS the auth for
        # this click). When the secret is UNSET, this click reached here via
        # `_check_tds_key` on X-TDS-Key alone (legacy / pre-rollout) — WARN so
        # the unauthenticated-bypass gap is visible. Configure
        # TDS_SMOKE_PROBE_SECRET on admin-api + the node to close it AND to let
        # a fresh (unseeded) node pass smoke at all.
        if not settings.smoke_probe_secret:
            logger.warning(
                "Smoke bypass authenticated by X-TDS-Key only (TDS_SMOKE_PROBE_"
                "SECRET unset) click_id_fp=%s node_id=%s — set the probe secret "
                "to close the TD-13 forge vector + let fresh nodes pass smoke",
                smoke_fp, settings.node_id,
            )
        try:
            r = await get_redis()
            smoke_record = {
                "click_id": req.click_id,
                "node_id": settings.node_id,
                "created_at_ms": int(time.time() * 1000),
                "smoke_test": True,
            }
            await r.xadd(
                "stream:clicks",
                {"data": json.dumps(smoke_record)},
                maxlen=settings.stream_clicks_maxlen,
                approximate=True,
            )
            logger.info(
                "Smoke-test click bypassed routing: click_id_fp=%s node_id=%s",
                smoke_fp, settings.node_id,
            )
        except Exception as exc:  # noqa: BLE001
            # Smoke XADD failure is itself a signal — the admin-api
            # smoke gate will time out and report the misconfig. Log
            # + Sentry so operators have both signals. Fingerprint only.
            logger.error(
                "Smoke-test XADD failed for click_id_fp=%s: %s",
                smoke_fp, exc,
            )
            sentry_sdk.capture_exception(exc)
        # Return a benign 302 to the fallback URL. The smoke gate
        # doesn't inspect the response body — only the central stream.
        return ClickResponse(
            url=f"{settings.fallback_url}?reason=smoke_test"
                f"&click_id={quote(req.click_id, safe='')}",
            status=302,
        )

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
            url=f"{_resolve_fallback_url()}?reason=error&click_id={quote(req.click_id, safe='')}",
            status=302,
        )

    # F-1 (2026-05-25): no_match AND blocked clicks are NOT dropped — they
    # become full, RECORDED clicks routed to the (admin-configurable) fallback
    # URL, tagged with their routing outcome, then fall through to the SAME
    # record-build → dedup → XADD → 302 path as a matched click. Previously
    # no_match early-returned (never recorded) and the blocked sentinel
    # (`{"url": None, "blocked": True}` from router.route) crashed at the
    # action_resolved checkpoint below (`None[:200]` → HTTP 500 → Worker
    # "All backends failed" → fallback). The matched path is byte-identical
    # (it never enters this branch). `result is None` is checked first so
    # `result.get("blocked")` is only evaluated on a dict.
    if result is None or result.get("blocked"):
        reason = "no_match" if result is None else "blocked"
        # Copy the timing dict — `setdefault` below would otherwise mutate
        # the object `route()` returned (benign today, fragile on reuse).
        fb_timing = {} if result is None else dict(result.get("timing") or {})
        fb_timing.setdefault("result", reason)
        result = {
            "url": f"{_resolve_fallback_url()}?reason={reason}"
                   f"&click_id={quote(req.click_id, safe='')}",
            "campaign_id": None if result is None else result.get("campaign_id"),
            "offer_id": None,
            "binding_id": 0 if result is None else result.get("binding_id", 0),
            "binding_alias": None if result is None else result.get("binding_alias"),
            "timing": fb_timing,
            # Surfaced into click_record.extra_params below so the fallback
            # click is queryable (extra_params->>'routing_status').
            "routing_status": reason,
        }
        emit_checkpoint(f"click.{reason}", {"click_id": req.click_id})

    # Routing decision resolved — emit summary checkpoint with the
    # selected campaign / offer / final URL so the trace timeline
    # shows exactly what the cascade picked. The route() function
    # also emits per-stage checkpoints internally (`click.cascade_*`,
    # `click.macro_*`) for finer detail.
    emit_checkpoint("click.action_resolved", {
        "campaign_id": result.get("campaign_id"),
        "offer_id": result.get("offer_id"),
        # Defensive: `.get("url", "")` returns None when the key is present
        # with value None (the old block-sentinel crash) — `or ""` guards it.
        "url": (result.get("url") or "")[:200],
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
        "timestamp": _resolve_click_timestamp(req.click_ts, req.click_id),
        "node_id": settings.node_id,
        "campaign_id": result.get("campaign_id"),
        "offer_id": result.get("offer_id"),
        # F.31 — per-binding analytics attribution. Domain-resolved clicks
        # carry the matched binding's id + alias; geo-resolved clicks (no
        # domain binding) default to 0 / "" (the "(default)" bucket).
        "binding_id": result.get("binding_id", 0),
        "binding_alias": result.get("binding_alias") or "",
        # Defensive: every path that reaches here now sets a string url
        # (matched offer OR fallback) — `.get` guards a hypothetical None
        # from becoming a KeyError (it lands as SQL NULL instead).
        "landing_url": result.get("url"),
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
    # F-1: tag fallback (no_match / blocked) clicks so they are queryable
    # (extra_params->>'routing_status'). Only set on the fallback path — a
    # matched click's record is byte-identical to before.
    if result.get("routing_status"):
        click_record["extra_params"]["routing_status"] = result["routing_status"]
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
    # double-write is prevented. F-4 (audit 2026-05-25): this marker's
    # TTL is 24h, decoupled from the 30d `click:{click_id}` /
    # `event:{click_id}:{type}` retention — it only needs to outlive the
    # same-node retry window (seconds), and is fully backstopped by the
    # collector's central dedup + ClickHouse natural-key dedup, so a
    # short bounded TTL costs nothing in correctness while cutting Redis
    # memory ~30×.
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
        # F.29 Sprint 1.5 (2026-05-23) — pre-flight disk-pressure check.
        # Plan §3 G4 closes the "disk full → enqueue OSErrors silently"
        # gap. Pre-F.29 the disk-fallback path called enqueue_click
        # blindly and accepted whatever bool came back; on a truly-full
        # mount the write raised OSError, was caught inside enqueue_click,
        # logged at ERROR + Sentry-captured, and returned False — but
        # /decide kept going and responded 302 to the Worker. The click
        # was "genuinely lost" per the pre-F.29 comment, with NO HTTP
        # signal back to the Worker that storage was saturated.
        #
        # Post-F.29: check_disk_pressure() returns (is_pressured, free).
        # If pressured (free < TDS_DISK_QUEUE_MIN_FREE_BYTES, default 1
        # GiB), refuse the fallback and 503 to the Worker. Worker's
        # AbortSignal fallback URL takes over → user still gets
        # redirected via Worker fallback path → operator sees the 503
        # signal in node logs + Sentry breadcrumb tagged
        # ``op=disk_pressure``.
        is_pressured, free_bytes = check_disk_pressure()
        if is_pressured:
            logger.critical(
                "F.29 Sprint 1.5 — disk-queue under pressure: "
                "%s free bytes < %s threshold on %s. REFUSING fallback "
                "enqueue for click %s. Worker will receive 503 → falls "
                "through to its own fallback URL (graceful degradation).",
                free_bytes,
                settings.disk_queue_min_free_bytes,
                settings.disk_queue_root,
                req.click_id,
            )
            # F.29 Sprint 1.6 — use the canonical telemetry helper +
            # OP_DISK_PRESSURE constant rather than inlining the
            # push_scope + set_tag dance. Single source of truth for
            # the op-tag scheme means Sprint 4.1 alert rules bind to
            # the same value here as in shipper's exception paths.
            capture_op_msg(
                OP_DISK_PRESSURE,
                f"Disk queue under pressure: free={free_bytes} < "
                f"threshold={settings.disk_queue_min_free_bytes} "
                f"on {settings.disk_queue_root}. Click {req.click_id} "
                "refused (503).",
                level="error",
                free_bytes=free_bytes,
                threshold_bytes=settings.disk_queue_min_free_bytes,
                disk_queue_root=settings.disk_queue_root,
                click_id=req.click_id,
            )
            emit_checkpoint("click.disk_pressure_503", {
                "click_id": req.click_id,
                "free_bytes": free_bytes,
                "threshold_bytes": settings.disk_queue_min_free_bytes,
            })
            raise HTTPException(
                status_code=503,
                detail="disk_pressure",
            )

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
    """Health check.

    F.29 Sprint 1.4 (2026-05-23) — extended with shipper + storage
    visibility (plan §3 G5). Pre-F.29 a silently-disabled shipper task
    still produced ``redis=true`` and ``campaigns_loaded>0`` because
    the redis ping itself worked; the fact that clicks were never
    delivered upstream was invisible. The new fields expose:

      * shipper_running / shipper_lag_seconds / last_ship_at /
        last_ship_status / last_batch_size — live from
        ``app.shipper_metrics`` (the shipper task updates it).
      * stream_clicks_length — current XLEN of stream:clicks.
      * disk_queue_size — count of files awaiting drainer replay.
      * disk_free_bytes — free bytes on the disk-queue mountpoint
        (None when the path does not exist).

    Each ancillary read is wrapped — a probe must never crash on a
    component that's only-partially-up, because /health itself is what
    the orchestrator polls to DECIDE whether the service is up.
    """
    r = await get_redis()
    try:
        redis_ok = await r.ping()
        campaigns_count = await r.scard("campaigns:active")
        sync_ver = await r.get("sync:version")
    except Exception:
        redis_ok = False
        campaigns_count = 0
        sync_ver = None

    # F.29 Sprint 1.4 — stream:clicks length. Independent try/except
    # so a Redis blip on XLEN does not cascade to making /health
    # respond 500 (orchestrators interpret that as "service down"
    # and may restart the container, blowing local state).
    try:
        stream_length = await r.xlen("stream:clicks") if redis_ok else 0
    except Exception:
        stream_length = 0

    # F.29 Sprint 1.4 — disk-queue file count. Reads the in-memory
    # counter (no FS scan) — see app.disk_queue. ``get_queue_size`` is
    # imported at module top (Sprint 1.6 — removed the redundant
    # in-function import). Wrapped independently so an early-lifecycle
    # call (before first /decide initialises the counter) returns 0
    # rather than 500'ing the probe.
    try:
        disk_queue_size = await get_queue_size()
    except Exception:
        disk_queue_size = 0

    # F.29 Sprint 1.4 — free bytes on the disk-queue mountpoint.
    # Used by Sprint 4.1 alert "disk_free_bytes < 1GB → warn". Returns
    # None when the root path does not exist (operator opted out of
    # disk fallback via empty TDS_DISK_QUEUE_ROOT, or first-boot before
    # the directory was created).
    disk_free_bytes: int | None = None
    if settings.disk_queue_root:
        try:
            disk_free_bytes = shutil.disk_usage(
                settings.disk_queue_root
            ).free
        except (OSError, FileNotFoundError):
            # Directory may not exist yet on first boot — leave as None.
            disk_free_bytes = None

    return HealthResponse(
        # F.32 Track 1 — drift visibility: the git SHA the node is running.
        code_version=settings.code_version,
        node_id=settings.node_id,
        region=settings.node_region,
        redis=redis_ok,
        campaigns_loaded=campaigns_count,
        sync_version=int(sync_ver) if sync_ver else 0,
        uptime_seconds=round(time.time() - START_TIME, 1),
        # F.29 Sprint 1.4 — shipper visibility (single source of truth
        # in ShipperMetrics dataclass; ``to_health_dict`` ensures any
        # future field added there is wired into the response).
        **shipper_metrics.to_health_dict(),
        stream_clicks_length=stream_length,
        disk_queue_size=disk_queue_size,
        disk_free_bytes=disk_free_bytes,
    )


# Loopback peer addresses for the /stats health-probe carve-out.
# `::ffff:127.0.0.1` is the IPv4-mapped-IPv6 form a dual-stack uvicorn
# bind can report for a localhost connection — included so the deploy
# health.sh probe is not falsely gated on a dual-stack node. All forms
# are kernel-set socket peers; an external client cannot spoof them.
#
# INVARIANT: this carve-out is sound ONLY while click-processor :8100 is
# reached DIRECTLY (no L7 reverse-proxy on the same host). A front proxy
# would make every caller's peer == loopback → unauthenticated /stats.
# Recorded in rule `architecture` ("/stats loopback trust"). Re-evaluate
# before fronting a node with a proxy.
_LOOPBACK_HOSTS = frozenset(
    {"127.0.0.1", "::1", "localhost", "::ffff:127.0.0.1"}
)


@app.get("/stats")
async def stats(
    request: Request,
    x_tds_key: str = Header("", alias="X-TDS-Key"),
):
    """Quick operational stats (node id, region, redis size).

    F-4 MEDIUM (audit 2026-05-25): previously fully unauthenticated,
    leaking node identity + config/memory size to any reachable caller.
    Gated in non-local — but the deploy ``health.sh`` probe curls this
    from LOOPBACK, so loopback stays open (zero-config health check) and
    any OTHER caller must present the node's X-TDS-Key (same credential
    as /admin/sync: static sync secret OR the per-Worker index).
    Local/dev is fully open for convenience.
    """
    if settings.environment not in _LOCAL_ENVIRONMENTS:
        client_host = request.client.host if request.client else ""
        if client_host not in _LOOPBACK_HOSTS:
            # Mirrors the /admin/sync auth ladder: static secret first,
            # per-Worker index fallback (raises 403 on a miss).
            if not _sync_secret_matches(x_tds_key):
                await _check_tds_key(x_tds_key)
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
    # Auth (timing-safe + fail-closed per H6 fix). The sync channel
    # credential is the node's STATIC shared secret — gate on it FIRST,
    # then fall back to the per-Worker index for defense in depth. This
    # is essential for fresh-node bootstrap: the worker_secret_hash index
    # is empty until the first sync populates it, so gating SOLELY on
    # _check_tds_key 403'd the very push meant to seed it (a fresh node
    # went active via the smoke probe yet could never receive config).
    # See _sync_secret_matches for the full rationale + F.25 safety note.
    if not _sync_secret_matches(x_tds_key):
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

    # F-4 HIGH-003 (audit 2026-05-25) — require the sig in non-local.
    # The legacy contract was "verify if present, accept if absent",
    # which let an on-path attacker simply STRIP the header to bypass
    # the body-integrity check entirely. In a non-local env where this
    # node has a tds_secret_key (so the sig CAN be verified), a missing
    # header is now a hard 401. admin-api signs on every push path, so
    # this rejects only tampered/forged (header-stripped) pushes — never
    # a legitimate one. Gated on tds_secret_key so a fresh node that has
    # not yet received its shared secret still bootstraps. Escape hatch:
    # TDS_REQUIRE_BODY_SIG=false reverts to lenient (incident rollback to
    # a non-signing producer). Local/dev is always lenient.
    _non_local = settings.environment not in _LOCAL_ENVIRONMENTS
    if (
        settings.require_body_sig
        and _non_local
        and settings.tds_secret_key
        and not x_tds_body_sig.strip()  # whitespace-only counts as absent
    ):
        logger.warning(
            "X-TDS-Body-Sig ABSENT on /admin/sync in env=%s — rejecting "
            "(HIGH-003 require-sig). Set TDS_REQUIRE_BODY_SIG=false only "
            "for an incident rollback to a non-signing producer.",
            settings.environment,
        )
        raise HTTPException(
            status_code=401,
            detail="Body signature required",
        )

    # T2.4 — body integrity check. Verify BEFORE decompression so
    # a corrupt gzip body fails the sig check (cleaner error
    # surface) rather than the gunzip step. Sig is over the EXACT
    # bytes that arrived on the wire — independent of compression.
    #
    # Lenient on absent header ONLY in local/dev or when require_body_sig
    # is disabled (the enforcement block above handles non-local). Strict
    # on present-but-mismatched: that's the active-MITM scenario the sig
    # defends against, so 401 with no further processing.
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
