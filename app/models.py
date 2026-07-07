"""Request/response models for click-processor."""

from pydantic import BaseModel, Field, field_validator

from app.shipper_metrics import ShipStatus


# Resource-exhaustion caps on query_params (security audit
# 2026-04-28 MEDIUM-004). Module-level constants — Pydantic v2
# would treat class-level int annotations as model fields, not
# ClassVar constants. CF Worker bounds URL length around 8 KB,
# but click-processor needs defense-in-depth so a misconfigured
# Worker or a direct HTTP call to /decide can't blow the hot
# path's memory budget. Both caps are generous: legitimate
# advertiser keys top out around 30-50 per click, individual
# values rarely exceed 256 chars.
_MAX_QUERY_PARAM_KEYS = 100
_MAX_QUERY_PARAM_VALUE_LENGTH = 1024


class ClickRequest(BaseModel):
    """Incoming request from CF Worker."""
    click_id: str = Field(max_length=128, pattern=r'^[a-zA-Z0-9_\-]+$')
    # F.24 Phase 5.1b — canonical click instant, generated ONCE at the
    # CF Worker edge (single source of truth, alongside click_id). It
    # becomes the collector's `clicks.created_at`, which is the
    # cross-node dedup anchor for true-racing fan-out (the clicks PK is
    # (click_id, created_at) on a created_at-partitioned table; both
    # halves must be edge-stable so N raced nodes collapse to one row
    # via ON CONFLICT). Strict ISO-8601 UTC, `Z` suffix, optional
    # millisecond fraction (JS toISOString emits 3; allow 1-6 for other
    # callers). Optional + None default: absent → click-processor falls
    # back to its own gmtime (dual-deploy window + non-Worker callers).
    # Validated at the boundary (api-contracts / data-handling) because
    # the value parameterises a TIMESTAMPTZ AND the collector's
    # `clicks` RANGE-partition routing key (PRIMARY KEY
    # (click_id, created_at) PARTITION BY RANGE(created_at)). The
    # SEMANTIC-range pattern (year 20xx, month 01-12, day 01-31, hour
    # 00-23, min/sec 00-59) is the FIRST defense layer: it rejects the
    # obvious garbage / far-future class (`2099-…`, `…-13-…`,
    # `…-99-…`) at /decide ingress so it never reaches the collector
    # (5.1b security-cycle finding #3 — a shape-valid out-of-range
    # date is a NEW surface vs the pre-5.1b node-`gmtime()` which was
    # always ~now/in-range; an unroutable created_at made
    # `collector.executemany` fail the WHOLE batch → shipper never
    # acks → poison-pill stall + co-batched legit-click loss). The
    # pattern alone cannot bound to the LIVE ±partition window (a
    # regex doesn't know "now"); the load-bearing fix is the
    # `clicks_default` DEFAULT partition in collector init.sql
    # (defense-in-depth — any still-out-of-range value lands there
    # instead of aborting the batch). max_length bounds it.
    click_ts: str | None = Field(
        default=None,
        max_length=40,
        pattern=(
            r'^20\d{2}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])'
            r'T([01]\d|2[0-3]):[0-5]\d:[0-5]\d(\.\d{1,6})?Z$'
        ),
    )
    visitor_id: str | None = Field(default=None, max_length=128, pattern=r'^[a-zA-Z0-9_\-]*$')
    # Signed identity cookie (`_tds_id`) value — Layer-1 RECOGNITION (P2, dark).
    # SoT: docs/development/returning-users-v2/DECISION-edge-identity-architecture.md.
    # The worker forwards the raw cookie value here (P4); the node VERIFIES it
    # in-process (HMAC) to recognize a returning user with ZERO store hit,
    # dual-accepting the legacy `_tds_vid` path. Absent (old worker / P2) ⇒ None
    # ⇒ byte-identical legacy resolution. Bounded; charset is the cookie wire
    # shape `b64url(payload).b64url(sig)` (base64url alphabet + the dot
    # separator). A non-matching value simply fails verify (fail-open), so the
    # pattern is a cheap pre-filter, not a security boundary.
    # SEC-LOW-01 (audit-2 2026-06-07): cap is 1024 to match the worker's
    # `_validIdentityCookie` length guard. The codec's largest valid PAYLOAD is
    # 512 B (`identity_token._MAX_PAYLOAD_BYTES`), which base64url-encodes to
    # ~683 chars for the payload PLUS a 43-char b64 sig + the dot separator
    # (~727 total). A 512 cap could 422 a near-max token at the edge (which would
    # fail verify anyway → fail-open), so 1024 keeps the pre-filter strictly
    # looser than what the codec itself will reject.
    identity_token: str | None = Field(
        default=None, max_length=1024, pattern=r'^[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+$'
    )
    is_returning: bool = False
    # Stage 3 · Phase 4 S2 — edge quality signals from CF Bot Management
    # (fail-open false at the worker on a non-Enterprise zone). Bools so a
    # missing field defaults cleanly; collector maps each to a CH UInt8.
    is_bot: bool = False
    is_proxy: bool = False
    # CF request ray (correlate with CF logs) + a worker-generated edge
    # correlation id, distinct from click_id. Both bounded; charset is not
    # constrained beyond length (cf_ray is CF-shaped hex-dash, request_id a
    # UUID) so a future edge format change does not 422 a live click.
    cf_ray: str = Field(default="", max_length=64)
    request_id: str = Field(default="", max_length=64)
    # Worker edge-arrival instant (OQ-D). Same strict ISO-8601 UTC shape as
    # click_ts; optional → absent (old worker) lands as CH NULL, never now().
    arrival_ts: str | None = Field(
        default=None,
        max_length=40,
        pattern=(
            r'^20\d{2}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])'
            r'T([01]\d|2[0-3]):[0-5]\d:[0-5]\d(\.\d{1,6})?Z$'
        ),
    )
    # Geo
    ip: str = ""
    country: str = ""
    city: str = ""
    region: str = ""
    continent: str = ""
    timezone: str = ""
    postal_code: str = ""
    latitude: str = ""
    longitude: str = ""
    asn: int = 0
    as_org: str = ""
    colo: str = ""
    # HTTP
    user_agent: str = ""
    referer: str = ""
    accept_language: str = ""
    # TLS
    tls_version: str = ""
    http_protocol: str = ""
    # URL
    hostname: str = ""
    path: str = ""
    # Query params from the request URL. Strict `dict[str, str]` —
    # rejects nested structures (lists, dicts, None values) at the
    # boundary so downstream resolution + macro substitution can
    # rely on a flat string-string shape. Per security audit
    # 2026-04-28 (HIGH-001) — closes the type-confusion future
    # foot-gun where a Worker bug could ship list-shaped values.
    query_params: dict[str, str] = Field(default_factory=dict)

    @field_validator("query_params", mode="before")
    @classmethod
    def _coerce_query_params(cls, v):
        """Coerce non-string query values to string at the boundary.

        Tolerates Worker quirks that ship numbers / bools as native
        types while still rejecting collection types (list, dict)
        which would corrupt slot resolution + macro substitution.
        Also bounds length: at most 100 keys, value strings capped
        at 1024 chars (truncated). Long values would otherwise
        amplify into the eventual `clicks.extras` JSONB row at
        Stage 3 storage time.
        """
        if v is None:
            return {}
        if not isinstance(v, dict):
            raise ValueError("query_params must be an object")
        if len(v) > _MAX_QUERY_PARAM_KEYS:
            raise ValueError(
                f"query_params has {len(v)} keys; max {_MAX_QUERY_PARAM_KEYS}"
            )
        out: dict[str, str] = {}
        for k, val in v.items():
            if not isinstance(k, str):
                raise ValueError(f"query_params keys must be strings (got {type(k).__name__})")
            if val is None:
                continue  # skip null values — same as absent key
            if isinstance(val, (list, dict)):
                raise ValueError(
                    f"query_params['{k}'] must be a scalar; got {type(val).__name__}"
                )
            if isinstance(val, bool):
                # bool BEFORE int (isinstance(True, int) is True).
                coerced = "true" if val else "false"
            elif isinstance(val, (int, float, str)):
                coerced = str(val)
            else:
                raise ValueError(
                    f"query_params['{k}'] has unsupported type {type(val).__name__}"
                )
            # Truncate rather than reject — operators sometimes ship
            # legitimately long click ids or marketing context. Cap
            # protects downstream storage without losing the click.
            if len(coerced) > _MAX_QUERY_PARAM_VALUE_LENGTH:
                coerced = coerced[: _MAX_QUERY_PARAM_VALUE_LENGTH]
            out[k] = coerced
        return out


class ClickResponse(BaseModel):
    """Response to CF Worker — where to redirect the user.

    F-2 (2026-06-10) — Worker-owned fallback contract. ``fallback=True`` with
    an empty ``url`` tells the Worker "no route here — redirect to YOUR
    admin-configured FALLBACK_URL", appending ``fallback_reason`` + click_id
    itself. The node carries NO default fallback URL of its own anymore (the
    per-campaign ``campaigns.fallback_url``, when configured, still arrives as
    a normal absolute ``url`` — that one is an admin setting, not a node
    default).
    """
    url: str = ""
    status: int = 302
    fallback: bool = False
    fallback_reason: str | None = None
    # P3 (2026-06-06) — the signed `_tds_id` value the node MINTED/re-stamped for
    # this returning-user identity, for the worker (P4) to emit as a Set-Cookie.
    # The node does NOT set an HTTP cookie header itself — node↔worker is JSON.
    # `None` ⇒ dark / nothing to emit (resolver OFF, codec disabled, or no uid):
    # the field is then OMITTED from the response → byte-identical to legacy.
    set_identity: str | None = None


class HealthResponse(BaseModel):
    """Click-processor health snapshot for operator dashboards.

    F.29 Sprint 1.4 (2026-05-23) extended this with shipper + storage
    visibility (plan §3 G5). Pre-F.29 a shipper task that had crashed
    silently still produced /health=200 with redis=true — the very
    50-day silent-shipper-disable that audit-2026-05-16 caught. New
    fields expose the shipper's live state and the local storage
    capacity so the operator can spot trouble before clicks queue up.

    All new fields are OPTIONAL with safe defaults so:
      * Legacy consumers that don't know about them keep working.
      * Sub-modules unavailable during early lifespan (e.g. /health
        hit during graceful shutdown after Redis closed) can still
        return a coherent shape.

    Field semantics — see ``app.shipper_metrics.ShipperMetrics`` +
    ``app.disk_queue.get_queue_stats`` for the canonical definitions.
    """

    # F.32 Track 1 — running code version (git short SHA from the node .env via
    # settings.code_version). Lets `tds-deploy node status` + the admin node list
    # compare each node against the expected release tip → drift one-glance.
    # "unknown" (pre-F.32 / local) or "local-dirty" (--dev rsync) are valid.
    code_version: str = "unknown"

    # Pre-F.29 fields — preserved verbatim, callers depend on them.
    node_id: str
    region: str
    redis: bool
    campaigns_loaded: int
    sync_version: int = 0
    uptime_seconds: float

    # Returning-users v2 — the EFFECTIVE (post-boot-gate) state of the identity
    # resolver on this node. Reflects `settings.returning_resolver_enabled` AFTER
    # `assert_identity_namespace_safe` ran: True ⇒ resolver armed (identity-redis
    # reachable + noeviction); False ⇒ either the env gate is off OR the boot-gate
    # DEGRADED it (identity store unavailable at startup). Lets an operator (and
    # `deploy/update.sh`) SEE whether returning recognition is actually live on a
    # node — not just whether the .env flag is set. Default False (dark).
    returning_resolver_active: bool = False

    # F.29 Sprint 1.4 shipper visibility ---------------------------------
    # Whether the click shipper task is actively shipping. False on
    # standalone/escape-hatch modes (intentional) AND on silent task
    # crashes (the catastrophic case the operator must see).
    shipper_running: bool = False
    # Seconds since the last ship attempt (None until first attempt).
    # Sprint 4.1 page rule: lag > 5min → page.
    shipper_lag_seconds: float | None = None
    # Wall-clock UNIX timestamp of last ship attempt (None until first).
    last_ship_at: float | None = None
    # Number of clicks in the last batch attempt. 0 = stream was empty.
    last_batch_size: int = 0
    # Outcome literal (see app.shipper_metrics.ShipStatus). "n/a" until
    # first attempt; one of {success, ack_failed, collector_error,
    # unreachable, parse_failed, loop_error, n/a} after.
    #
    # Sprint 1.6 (validation cycle): typed as the canonical
    # ShipStatus Literal rather than bare str. This makes the dict
    # keys returned by ShipperMetrics.to_health_dict() round-trip
    # through Pydantic validation — a stale status value from a
    # future refactor (e.g. "loop-error" vs "loop_error") would now
    # fail validation at /health response build time rather than
    # silently propagating to dashboards as a typo.
    last_ship_status: ShipStatus = "n/a"

    # F.29 Sprint 2.4 (2026-05-23) rolling-window batch success ratio.
    # accepted / (accepted + rejected) over the last 5 minutes per
    # node. None when no outcomes recorded yet OR when the denominator
    # is 0 (e.g. shipper running but stream empty). 1.0 = all delivered;
    # 0.0 = all rejected. Sprint 4.1 alert rule: warn at <0.95 sustained.
    shipper_success_ratio_5m: float | None = None

    # F.29 Sprint 1.4 storage visibility ---------------------------------
    # XLEN of stream:clicks. Steady-state ~0-10k (shipper XTRIMs to 10k
    # after success). Sustained > 50k = shipper failing.
    stream_clicks_length: int = 0
    # P2 (D3, LOSSFIX 2026-07-07) segment-queue depth — REPURPOSED from
    # a per-click file count (pre-P2) to a SEGMENT count (own + adopted
    # + not-yet-migrated legacy `*.json`). Steady-state = 0 (disk
    # fallback fires only on Redis outage / M1 reject / watermark
    # spill). See ``app.disk_queue.get_queue_stats``.
    disk_queue_size: int = 0
    # P2 (D3) — total bytes across every segment/legacy file awaiting
    # drainer replay. Compared against
    # ``settings.disk_segment_max_total_bytes`` for the byte-cap gate.
    disk_queue_bytes: int = 0
    # P2 (D3) — age in seconds of the OLDEST file still awaiting replay.
    # None when the queue is empty. A growing value under a healthy
    # Redis means the drainer itself is stuck, not just an outage.
    disk_queue_oldest_seconds: float | None = None
    # Free bytes on the disk-queue mountpoint. None when the path does
    # not exist (local dev without TDS_DISK_QUEUE_ROOT). Sprint 1.5
    # pre-flight check uses this against
    # ``settings.disk_queue_min_free_bytes``.
    disk_free_bytes: int | None = None
    # CAP-1 (2026-06-10) — identity-store (noeviction Redis) saturation
    # visibility. None when the store is unreachable or maxmemory is
    # unlimited (local dev). The node itself fires throttled Sentry
    # signals at ≥80%/≥95% (see /health); these fields give operators +
    # the admin health snapshot the same numbers.
    identity_store_used_bytes: int | None = None
    identity_store_max_bytes: int | None = None
    identity_store_used_pct: float | None = None

    # GTD-R75 / ADR-0055 — the honest capacity-verification loop. These two
    # report the EFFECTIVE config the running process actually has, not what
    # provisioning INTENDED to inject — admin-api compares this against its
    # own intended values (auto_web_concurrency/auto_pool_size) to stamp
    # capacity_applied (true/false/unverified) rather than trusting a pure
    # computation. web_concurrency is read straight from the process's own
    # environment (the same WEB_CONCURRENCY the Dockerfile CMD's
    # `${WEB_CONCURRENCY:-2}` consumed to pick the worker count) — this
    # process is one of those workers, so its own env IS the value that
    # governed. redis_max_connections is the pydantic-resolved
    # settings.redis_max_connections (already reflects whatever env/default
    # actually took effect).
    web_concurrency: int = 2
    redis_max_connections: int = 128

    # LOSSFIX P3 (2026-07-07, L6) — observability depth. Every field here
    # is a CACHED read (the watermark sampler's in-memory state, the M1
    # observability loop's cached stream-length sample) — never a new
    # per-request Redis round-trip, mirroring the existing D3/CAP-1
    # discipline above. Lets an operator (and the P4 abort-guard) read a
    # node's spill/backpressure/dedup posture from ONE /health call.
    #
    # P2 c3 watermark state — used_memory% and whether new real clicks
    # are currently diverting to the disk-segment fallback.
    watermark_spill_mode: bool = False
    watermark_used_memory_pct: float = 0.0
    # None = never sampled yet (fresh boot, within the boot grace).
    watermark_sample_age_seconds: float | None = None
    # M1 — the entry-count reject threshold + whether the CACHED signal
    # is currently at/over it (mirrors `main._check_stream_backpressure`
    # exactly, so this reflects the SAME decision the hot path makes,
    # not a possibly-fresher-but-different live XLEN).
    stream_clicks_reject_threshold: int = 0
    stream_backpressure_active: bool = False
    # The live click_dedup_ttl_seconds value in effect (config
    # passthrough — zero-cost, "dedup-key pressure if cheaply
    # available" per the P3 brief; a live SCAN/DBSIZE count would NOT
    # be cheap, so this is the pressure signal actually exposed).
    click_dedup_ttl_seconds: int = 0
