"""Click-processor configuration. All settings from environment variables."""

from pydantic import model_validator
from pydantic_settings import BaseSettings


# Environments where missing secrets are TOLERATED — local dev,
# unit tests, ad-hoc explorer scripts. Mirrors the admin-api
# `_LOCAL_ENVIRONMENTS` constant in `services/admin-api/app/config.py`.
# Any environment NOT in this set must boot WITH a non-empty
# `tds_secret_key` (audit closure 2026-05-09 — Agent 2 HIGH-1).
_LOCAL_ENVIRONMENTS = frozenset({"local", "development"})


class Settings(BaseSettings):
    # Service
    node_id: str = "local"
    node_region: str = "eu"
    environment: str = "development"
    port: int = 8100

    # F.32 Track 1 — running code version (git short SHA), stamped into the node
    # .env at provision/update time (deploy/render-env.sh + deploy/update.sh) and
    # surfaced in /health so operators can spot drift between nodes at a glance
    # (the "pereviryty ne mozhemo" pain). "unknown" = local dev or a node
    # provisioned before F.32; "local-dirty" = a tds-deploy --dev rsync (WIP, not
    # a clean git ref).
    code_version: str = "unknown"

    # Redis (local, same machine/container)
    redis_url: str = "redis://redis:6379/0"

    # F4 (GTD-R173, 2026-07-05) — routing-Redis connection-pool sizing +
    # bounded-wait. The pre-F4 pool was the default NON-blocking
    # `ConnectionPool(max_connections=20)` with NO timeouts (redis_client.py).
    # redis-py 5.2.1's default pool RAISES `ConnectionError("Too many
    # connections")` SYNCHRONOUSLY the instant `in_use >= 20` and no idle
    # connection exists — before any socket op. Under a concurrency burst the
    # per-worker pool exhausted and whichever routing stage was acquiring a
    # connection fail-opened; the audit caught the flow-candidate read
    # (`except → []` → "no flow" → offer-miss). Fix = a `BlockingConnectionPool`
    # sized >= peak concurrency that WAITS (a connection frees in ~0.2 ms)
    # instead of raising. Deadlock-free here: the hot path holds <=1 connection
    # at any instant (every pipeline is buffer-then-execute, released in a
    # `finally`; no gather/TaskGroup/watch/lock; identity is a separate pool).
    #
    # `redis_pool_timeout_seconds` is the PER-ACQUIRE wait (redis-py source +
    # context7), NOT per-request — a `/decide` makes ~12-18 sequential acquires,
    # so 0.12 s bounds the worst case (~15 x 0.12 < 1.8 s) safely under the CF
    # 2 s AbortSignal. Sizing N >= peak means normal load never blocks; the
    # timeout only bites on a pathological over-subscription, where the request
    # load-sheds to a RECORDED honest worker fallback (Layer 2), never a silent
    # misroute. All four are TDS_-prefixed and tunable WITHOUT a rebuild.
    #
    # Server headroom: routing Redis `maxclients` default 10000; 128/worker x 2
    # uvicorn workers (+ identity 128 x 2 when the resolver is live) ~= 512
    # conns/node << 10000, and ~512 idle conns cost a few MB (`maxmemory 256mb`
    # bounds DATA, not connections). SoT: FIX-DESIGN-F4.md, FIX-PLAN.md §1.
    redis_max_connections: int = 128
    redis_pool_timeout_seconds: float = 0.12
    redis_socket_timeout_seconds: float = 1.0
    redis_socket_connect_timeout_seconds: float = 1.0

    # Auth (shared secret with CF Worker)
    tds_secret_key: str = ""

    # F-4 HIGH-003 (audit 2026-05-25) — require the X-TDS-Body-Sig header
    # on /admin/sync in non-local envs (defense-in-depth vs an on-path
    # attacker who keeps the valid X-TDS-Key but tampers with the snapshot
    # body). Enforcement is GATED on the node also having a tds_secret_key
    # (without it the sig cannot be verified, and a fresh node mid-bootstrap
    # may legitimately lack it). admin-api signs on BOTH push paths
    # (SyncService._build_push_headers + the seed_data fallback), so this
    # never rejects a legitimate push. Escape hatch: set
    # TDS_REQUIRE_BODY_SIG=false to fall back to lenient verify-if-present —
    # for an incident where a non-signing producer (e.g. a rolled-back
    # admin-api) must push. Mismatched sigs are ALWAYS rejected regardless.
    require_body_sig: bool = True

    # F.29 Sprint 4.1 (TD-13, 2026-05-23) — HMAC smoke-probe authenticator.
    #
    # Dedicated secret shared between admin-api and every edge node, but
    # DISTINCT from `tds_secret_key` (X-TDS-Key) and the collector api key.
    # When set, the /decide smoke-test bypass (the `smoke-test-` prefix
    # short-circuit) REQUIRES a valid `X-TDS-Smoke-Probe` HMAC header that
    # only admin-api can produce — closing the forge vector where a holder
    # of X-TDS-Key (or the 64-bit hex observable in logs / the central
    # stream) drives a false-positive activation of another tenant's node.
    #
    # Opt-in / fail-closed-when-set: empty (default) preserves the
    # pre-Sprint-4.1 behaviour (bypass on X-TDS-Key alone) so existing
    # nodes don't break before the secret is rolled out — a WARN fires on
    # every unauthenticated bypass so operators know to configure it. Once
    # set, a missing/invalid/expired probe is REFUSED (403) — the smoke
    # gate then surfaces it as a clear "node /decide returned HTTP 403".
    # No boot guard: smoke probing is an onboarding-only path, so an
    # unset secret degrades gracefully rather than refusing service.
    smoke_probe_secret: str = ""

    # Central server (for sync + click shipping).
    #
    # F.29 Sprint 1.1 (2026-05-22) — central_url is no longer "optional"
    # in non-local environments. See `_enforce_central_url_presence` for
    # full incident context.
    #
    # F.29 Sprint 2.7b (2026-05-23) — HTTPS enforcement layer. Edge nodes
    # are deployed across WAN (Sydney + Toronto staging today; future
    # tenants will deploy across geographic regions). Plain HTTP exposes
    # the click pipeline to MITM attacks: an on-path attacker can
    # intercept request bodies (PII — IP/geo/UA) AND downgrade response
    # to the pre-F.29 legacy shape (`{"received":N,"queued":N}` with
    # status 200) → Sprint 2.5 backwards-compat shim activates → shipper
    # ACKs all clicks → SILENT LOSS. Sprint 2 validation cycle (Agent 2
    # HIGH S2-002, 2026-05-23) caught this load-bearing path.
    #
    # `require_central_url_https` (default True in non-local env) refuses
    # boot when TDS_CENTRAL_URL doesn't start with "https://" — closing
    # the MITM-shim attack surface. Local env exempt (HTTP fine for
    # localhost dev). Operator escape hatch:
    # TDS_REQUIRE_CENTRAL_URL_HTTPS=false to revert (NOT recommended;
    # use only for transitional rolling deploy of TLS termination).
    central_url: str = ""
    central_api_key: str = ""
    require_central_url: bool = True
    require_central_url_https: bool = True

    # F.32 Track 2 — config-snapshot pull URL, SEPARATE from `central_url`.
    #
    # `central_url` is the COLLECTOR (the F.29 shipper POSTs clicks to
    # `{central_url}/api/clicks/batch`, X-Node-Key auth). The periodic
    # config-snapshot pull (`sync_client.pull_from_central`) hits a
    # DIFFERENT service — the admin-api `{sync_url}/api/system/sync/snapshot`
    # (X-TDS-Key auth). Before this split the pull reused `central_url`, so
    # once F.29 pointed `central_url` at the collector the pull 403'd every
    # cycle (the collector has no snapshot endpoint). Config still flows via
    # PUSH, so the pull is a best-effort safety net: empty `sync_url` ⇒ pull
    # disabled (push-only); set it to the admin-api host to re-enable.
    sync_url: str = ""

    # F-2 (2026-06-10): the node-level default fallback URL is GONE. The
    # Worker is the single fallback owner (`workers.settings.fallback_url`,
    # admin-configured, deployed to CF as FALLBACK_URL): a no-route /decide
    # answers `{"fallback": true, "fallback_reason": ...}` and the Worker
    # builds the destination itself. Per-campaign `campaigns.fallback_url`
    # (admin setting) still produces a normal absolute redirect on the node.

    # Sentry (from env, never hardcode)
    sentry_dsn: str = ""
    # F.40 — per-instance Sentry environment tag. When a node is attached
    # to a tenant Sentry account, provisioning bakes this = the node_id
    # (e.g. "fra-myedge") so the tenant filters their fleet by environment
    # in one shared project. Empty → fall back to `environment`
    # (TDS_ENVIRONMENT) so unattached / legacy nodes are unchanged.
    sentry_environment: str = ""

    # Sync
    sync_interval_seconds: int = 30
    full_sync_interval_seconds: int = 60

    # T2.1 / G-22, REPURPOSED by M1 (LOSSFIX P1b, 2026-07-07) —
    # `stream:clicks` entry-count REJECT threshold. Pre-fix this was an
    # inline `MAXLEN ~ N` on every XADD, which SILENTLY TRIMMED the
    # oldest UNCONSUMED entries once the stream grew past it during an
    # extended central-collector outage (the M-TRIM pathology — masked
    # by the fact that the stream rarely got that large before OTHER
    # limits bit). No XADD carries MAXLEN anymore: `/decide` instead
    # checks this threshold against a CACHED XLEN sample
    # (`app.observability.get_cached_stream_clicks_length`, ~60s
    # cadence — never a per-click round-trip) BEFORE attempting the
    # real-click XADD, and diverts to the existing disk-fallback queue
    # when at/over it — reject, not trim (see `main._check_stream_
    # backpressure`). The smoke-probe XADD rejects outright (503) at
    # the same threshold, gating node activation.
    #
    # A2 (MUST, LOSSFIX P1b) — default is 300_000, NOT the old 1_000_000.
    # Edge routing Redis is provisioned at 256 MB (`docker-compose.yml`
    # `--maxmemory 256mb`); at ~500-600 B/entry, 1,000,000 entries ≈
    # 500-600 MB — comfortably ABOVE the 256 MB budget, so Redis would
    # OOM long before XLEN could ever reach that count, making the
    # reject path dead code. 300,000 ≈ 70% of 256 MB ÷ ~600 B/entry —
    # large enough to absorb hours of a collector outage at normal
    # click rates while staying inside the actual memory budget.
    # Tunable per-environment via TDS_STREAM_CLICKS_MAXLEN (NOT pinned
    # by `deploy/render-env.sh`, so this new default takes effect
    # fleet-wide on next deploy).
    stream_clicks_maxlen: int = 300_000

    # H1 fix (2026-05-11): TTL for the per-click idempotency marker
    # `click:seen:<click_id>` set by `acquire_click_dedup` in main.py.
    # This is the NODE-LOCAL gate — it only suppresses a redundant XADD
    # for a SAME-NODE retry (the Worker's sequential fallback re-hitting
    # this node within its 2s AbortSignal window). A genuine duplicate
    # therefore arrives within SECONDS, not days; the shipper has long
    # deadlettered anything older. The marker is also write-only (a
    # SETNX flag, never read back). Cross-node / late duplicates are
    # independently caught by the COLLECTOR's central dedup, so this
    # expiring early is fully backstopped.
    #
    # `click:shipped:<click_id>` (P2, LOSSFIX 2026-07-07 — set ONLY
    # after a CONFIRMED-successful XADD, see app/disk_queue.py) SHARES
    # this SAME TTL — both markers shrink together.
    #
    # LOSSFIX P3 (2026-07-07) — SHRUNK 86400s (24h) -> 600s (10min), the
    # edge-side half of the M-OOM keyspace relief (collector's central
    # `click:central_seen` mirrors this shrink to 1800s — see
    # services/collector/app/config.py for that side's sizing table).
    # See ``docs/development/lossfix-p3-2026-07-07/DEPLOY-SEQUENCING.md``
    # before ever rolling this to a node with an existing disk backlog.
    #
    # SIZING (A1, MUST): must EXCEED the max realistic dedup-defeating
    # window = max(CF-Worker-retry window, the shipper reclaim window
    # ~95s [F-6: min_idle 60s + interval 30s + one loop pass], cross-
    # node arrival skew). 600s comfortably clears that ~95s bound.
    #
    # Keyspace at STRESS rates (dedup_keyspace ~= rate * TTL * ~150B/
    # key): @600s, 800rps (the grid soak target) -> 72MB of the edge
    # routing Redis's 256MB budget (28%) -- volatile-lru MAY evict
    # markers early under memory pressure, which only shifts a replay
    # toward the DUP direction (safe by A2 below, never loss).
    #
    # spilled=first-arrival insight: a DIVERTED/spilled click does NOT
    # depend on THIS marker at any TTL for its own correctness -- it is
    # captured once via segment replay gated on the SEPARATE
    # `click:shipped` LOCAL check (disk_queue.py), and central dedup
    # (collector's `click:central_seen`) is the cross-node/CF-retry
    # backstop. The TTL shrink does NOT weaken spill-path correctness —
    # don't over-worry the spill path reading this.
    #
    # INVARIANT (A2, MUST): the shrink can only ever increase BOUNDED
    # DUP, never loss. `click:seen`/`click:shipped` only fail toward
    # "not seen" on expiry (false-negative -> re-ship -> the
    # collector's central SETNX / CH uniq-by-click_id absorbs it); a
    # false-positive "already shipped" CANNOT arise from expiry --
    # `click:shipped` is only SET after a CONFIRMED XADD, never before
    # one is attempted.
    #
    # Operator override: env `TDS_CLICK_DEDUP_TTL_SECONDS`; 0 DISABLES
    # dedup entirely (escape hatch for Redis OOM / extreme retry-storm
    # during a deploy).
    click_dedup_ttl_seconds: int = 600  # 10 minutes, 0 = disabled (LOSSFIX P3, was 86400/24h)

    # T2.2 / G-23, REDESIGNED by P2 (LOSSFIX, 2026-07-07) — disk fallback
    # queue for clicks when XADD fails (or the M1/watermark gates divert
    # here pre-emptively). Every worker appends NDJSON lines to its OWN
    # `{boot_epoch}-{pid}-{seq}.ndjson` segment (group-commit fsync, see
    # `app/disk_queue.py`) instead of one file per click — the old
    # per-click-file design meant an outage produced one `rglob` entry
    # per click (inode exhaustion risk) and ~1 fsync per click. A
    # background drainer replays each finalized segment, deleting it
    # (and its replay-offset sidecar) once fully drained.
    #
    # M7 fix (2026-05-11), unchanged by P2: absolute-path requirement.
    #
    # Was: `var/click-queue` — RELATIVE to process CWD at runtime.
    # If uvicorn was launched with `cwd=/` (some container configs,
    # systemd ExecStart with `WorkingDirectory=/`, or operator typo),
    # queued click files landed at `/var/click-queue/...` — a
    # system-wide path the service may not own. Worst case: a
    # hostile co-tenant on the same node creates `/var/click-queue`
    # with world-readable perms BEFORE the service starts; service
    # writes click PII files (IP, geo, lat/lon, full UA) into the
    # attacker-readable directory; the defensive `chmod(0o700)` then
    # swallows OSError silently, masking the misconfig.
    #
    # Now: default to absolute `/var/tds/click-queue` and a field
    # validator (below) refuses to construct Settings with a
    # non-absolute value. Loud failure at startup > silent data
    # loss at runtime.
    disk_queue_root: str = "/var/tds/click-queue"
    disk_queue_drain_interval_seconds: int = 30

    # P2 c1 — segment rotation. A segment finalizes (closes, renames off
    # its `.wip` suffix, dir-fsynced — D2) once EITHER threshold is hit,
    # whichever first. 1-5 MB / ~1s per the design brief; defaults sit at
    # the low end so a drainer cycle never waits long to see fresh data.
    disk_segment_max_bytes: int = 2_000_000
    disk_segment_max_age_seconds: float = 1.0

    # P2 c1 — group-commit linger window. Concurrent `enqueue_click`
    # awaiters that land within this window share ONE fsync (batches
    # writes under load instead of ~1 fsync/click); bounds the added
    # latency on the (already-degraded, spill/fallback-only) path to
    # this value plus a few ms of fsync time — comfortably under the
    # brief's <=100ms target and far under the CF Worker's 2000ms
    # AbortSignal race deadline.
    disk_segment_group_commit_ms: float = 20.0

    # P2 c1/c2 — global byte-cap across ALL workers' segments (own +
    # adopted orphans + any not-yet-migrated legacy `*.json` files),
    # replacing the old per-click-file-count cap (`disk_queue_max_files`,
    # removed — meaningless once clicks are batched into segments). A
    # CHEAP periodic scan (`disk_queue_stats_scan_interval_seconds`)
    # caches {segments, bytes, oldest_seconds} for both this gate and
    # `/health`; `enqueue_click` reads ONLY the cache (A3-style hot-path
    # discipline — never a live scan on `/decide`). At/over the cap, new
    # clicks are REJECTED (visible 503 via the existing L1 uncaptured
    # path) rather than silently rotating the oldest segment. 0/negative
    # disables the cap (unbounded, operator opt-in).
    disk_segment_max_total_bytes: int = 5_000_000_000  # 5 GiB
    disk_queue_stats_scan_interval_seconds: float = 5.0

    # P2 c2 (B1) — orphan-adoption age floor. On EVERY adoption attempt
    # (boot + each periodic drainer cycle, gate-E fix 2026-07-07 — was
    # one-shot-at-boot only), any segment prefix `{epoch}-{pid}` that
    # isn't THIS worker's own is a candidate orphan (its writer died —
    # crash, restart, or a full-node reboot). Prefixes younger than
    # this floor are assumed to belong to a SIBLING worker of the SAME
    # boot generation that just hasn't written its first segment yet
    # (uvicorn's `--workers N` start within a couple seconds of each
    # other) and are left alone THIS attempt — a genuinely dead
    # worker's segments are still there (and get adopted) on the NEXT
    # periodic retry, at most `disk_queue_drain_interval_seconds`
    # later. 30s is comfortably above any realistic multi-worker boot
    # stagger.
    disk_orphan_adopt_min_age_seconds: int = 30

    # gate-E round 2 CRITICAL fix (2026-07-07) — MECHANICAL liveness for
    # orphan adoption. `disk_orphan_adopt_min_age_seconds` above is a cheap
    # AGE pre-filter only (guards a same-boot sibling still starting up); it
    # is NOT proof of death, because the epoch in a segment's name is the
    # writer's BOOT time, not the file's age — so ANY live sibling older
    # than that floor (i.e. every sibling past its first ~30s of life)
    # looked "dead" to age alone. Combined with the periodic-retry HIGH fix
    # (round 1), that became CONTINUOUS mass live-sibling theft under
    # sustained WC=8 spill — including OPEN `.wip` theft (silent loss
    # TWICE: a torn-tail truncate mid-flight on a file the owner is still
    # actively writing, then the owner's own subsequent appends vanishing
    # into an unlinked inode once the thief drains+deletes the file).
    #
    # Every worker refreshes its own `{prefix}.alive` heartbeat file's mtime
    # every `run_drainer` cycle AND at the moment it opens its FIRST
    # segment — a candidate orphan prefix is adopted only once its
    # heartbeat is missing (a pre-this-fix orphan that already cleared the
    # age floor — a live current-code worker can never be in that state)
    # OR older than `THIS multiplier * disk_queue_drain_interval_seconds`.
    # A multiplier (not an absolute seconds value) so a change to the drain
    # interval can never accidentally starve a live worker of enough time
    # to refresh before looking stale.
    disk_orphan_heartbeat_stale_multiplier: float = 3.0

    # P2 c1 (B3, gate-E perf fix 2026-07-07) — replay-offset batching.
    # Persisting the offset sidecar after EVERY line (one full
    # open+write+fsync+close+rename per line) is correct but slow under
    # a large backlog — it can slow the drain enough to push toward the
    # byte-cap sooner. Batching to every N lines cuts the fsync count
    # ~Nx while still bounding a CRASH mid-replay to at most N
    # re-replayed lines (backstopped by the click:shipped dedup check +
    # the collector's central dedup, same "duplicate over loss"
    # trade-off already accepted elsewhere in this module). A Redis-
    # impairment break (the process stays alive, just stops) still
    # flushes the offset immediately — only a hard crash accepts the
    # bounded-batch window.
    disk_replay_offset_batch_lines: int = 50

    # P2 c3 (D5, LOSSFIX, 2026-07-07) — edge used_memory% watermark.
    #
    # Ported from the collector's `app/watermark.py` (LOSSFIX P1a) — same
    # state machine, same defaults, same fail-open/hysteresis semantics.
    # The ONE difference: the collector SHEDS (503) on trip; the edge
    # SPILLS (diverts new real clicks to the disk-segment fallback
    # instead of XADD-ing) because routing-cache HSETs live on the SAME
    # Redis instance (`docker-compose.node.yml`'s `redis` service, 256
    # MB volatile-lru — confirmed shared with `stream:clicks`, not a
    # separate instance) and must keep succeeding even under click-
    # traffic memory pressure (empirical: campaign-sync HSET threw
    # OutOfMemoryError at >=650rps once click XADDs filled the same
    # instance). A dedicated ~1s sampler (`app/watermark.py`) reads
    # `used_memory%` of THIS SAME instance; `main.py`'s real-click path
    # reads only the cached `should_spill()` decision — never a per-click
    # INFO round-trip.
    watermark_shed_pct: float = 85.0
    watermark_resume_pct: float = 70.0
    watermark_sample_interval_sec: float = 1.0
    watermark_staleness_sec: float = 10.0
    watermark_boot_grace_sec: float = 60.0

    # F.29 Sprint 4.1 (2026-05-23) — shipper-health alert thresholds.
    #
    # The observability loop (`emit_shipper_health`) runs on its OWN task,
    # independent of the shipper coroutine, so it can detect the shipper
    # being WEDGED/dead — the audit-2026-05-16 50-day blackout case the
    # shipper loop cannot self-report. On breach it emits a Sentry
    # capture_message (error=page, warning=warn) that Sentry issue-alert
    # rules fire on (rule configs: docs/development/capacity-validation-
    # 1000rps.md alert runbook — Sentry MCP cannot create alert rules).
    #
    # - lag > 300s (5 min) while a batch HAS shipped → page (the click
    #   pipeline has stalled — the canonical F.29 G5 alert).
    # - success_ratio_5m < 0.95 with a meaningful sample → warn.
    # "Sustained" is enforced by the Sentry alert rule (fires when the
    # condition recurs over its evaluation window), not here — this loop
    # just emits the per-tick signal.
    shipper_lag_alert_seconds: int = 300
    shipper_success_ratio_alert_min: float = 0.95
    # Minimum window sample before the success-ratio alert can fire —
    # avoids paging on a single rejected click in an otherwise-quiet
    # window (which would read as ratio=0.0).
    shipper_success_ratio_alert_min_sample: int = 20

    # F.29 Sprint 1.5 (2026-05-23) — pre-flight disk-pressure threshold.
    #
    # Closes plan §3 G4: pre-F.29 the disk-queue fallback at
    # main.py:659-674 would fall through to enqueue_click whenever the
    # XADD path failed. The cap-by-file-count guard
    # (``disk_queue_max_files``) bounds queue *cardinality*, but a
    # disk-FULL condition fires BEFORE the file-count cap is hit
    # (each click is ~500 B; 100k files × 500 B ≈ 50 MB, but if the
    # mount has 0 free bytes for any other reason — log rotation,
    # /var/lib runaway, etc. — the enqueue OSErrors and the click is
    # "genuinely lost" per the pre-F.29 comment at main.py:674).
    #
    # The pre-flight check (``app.disk_queue.check_disk_pressure``)
    # compares free bytes against THIS threshold before attempting
    # the write. If under pressure:
    #   - CRITICAL log + Sentry capture
    #   - /decide returns 503 disk_pressure to the CF Worker
    #   - Worker falls through to its own fallback URL → user still
    #     gets redirected; click is recorded as visibly lost rather
    #     than silently lost
    #
    # 1 GiB default is generous: at ~500 B/click the disk-queue would
    # need to absorb 2M backlogged clicks to push the threshold —
    # well above the file-count cap (100k). It's a SECOND-LINE
    # defense against non-shipper disk consumers competing for the
    # same mount (logs, sync_client downloads, ad-hoc files).
    #
    # Local env (TDS_ENVIRONMENT in {local, development}) skips the
    # check — engineers may have small dev partitions and the disk
    # fallback path isn't exercised in dev anyway.
    disk_queue_min_free_bytes: int = 1_073_741_824  # 1 GiB

    # F.29 Sprint 2.2 (2026-05-23) — shipper retry policy for clicks
    # the central collector REJECTED (per-click verdict in BatchResponse,
    # Sprint 2.1). Each rejected click_id increments a Redis counter
    # ``click:retry:{click_id}`` (24-hour TTL). When the counter reaches
    # this maximum, the click is deadlettered (Sprint 2.3) — moved out
    # of the shipper's retry rotation into a central deadletter stream
    # for operator inspection.
    #
    # Default 5 chosen to balance:
    #   * Tolerate transient collector blips (1-2 quick retries).
    #   * Catch persistent rejects (validation failures, schema
    #     regressions) within ~5 batch cycles ≈ 10s at default
    #     2s batch timeout.
    #   * Bound retry storms — a misconfigured collector returning
    #     100% rejected won't keep ~1M clicks in PEL forever.
    #
    # Operator override:
    #   TDS_SHIPPER_MAX_RETRY_ATTEMPTS=2 → quicker deadlettering
    #     during a sustained incident (sacrifices some legitimate
    #     transient retries for operator triage speed).
    #   TDS_SHIPPER_MAX_RETRY_ATTEMPTS=10 → more patience for slow
    #     central recovery (longer retry tail, larger backlog).
    shipper_max_retry_attempts: int = 5

    # F.29 Sprint 2.2 — retry counter TTL. 24h is plenty for a click
    # to either deadletter or eventually succeed; setting too low
    # would reset the counter mid-incident and let a stuck click
    # retry forever.
    shipper_retry_ttl_seconds: int = 86400  # 24 hours

    # C3 (audit 2026-06-03) — edge-shipper orphaned-PEL reclaim. Mirror of
    # the central writer's reclaim knobs (writer_reclaim_*). CONSUMER_NAME
    # embeds os.getpid(), so a shipper crash/restart orphans the dead
    # consumer's PEL entries (XREADGROUP-read but never XACKed because the
    # process died between read and ship+ack). The main loop reads only
    # `>` (new), never the dead consumer's PEL → silent click loss. The
    # central writer already guards this (writer._reclaim_pending); these
    # knobs drive the mirrored edge-shipper reclaim loop.
    #   * interval_sec — how often the loop runs reclaim between drains.
    #   * min_idle_ms  — only claim entries idle PAST this (never race the
    #                    live consumer; > a normal ship+ack round-trip).
    #   * max_per_cycle — bound the reclaim hot loop per tick.
    shipper_reclaim_interval_sec: float = 30.0
    shipper_reclaim_min_idle_ms: int = 60_000
    shipper_reclaim_max_per_cycle: int = 5_000

    # AUD-B F1 (2026-06-12) — processed-history trim cadence. The shipper
    # XTRIMs `stream:clicks` with MINID (oldest pending id, else the
    # group's last-delivered-id) on this interval — replacing the old
    # per-batch `XTRIM MAXLEN ~10000` that silently destroyed outage
    # backlog on recovery (the first successful ship after a central
    # outage trimmed every un-shipped entry older than the newest 10k).
    # Mirror of process-service `consumer_trim_interval_sec`.
    shipper_trim_interval_sec: float = 60.0

    # ------------------------------------------------------------------
    # Diagnostic mode toggles. All three default `False` — production
    # safety-first. Operator flips per-environment via `.env`:
    #   staging: all True (full diagnostic visibility for calibration)
    #   production: all False; flip individually for incident response
    #
    # The toggles gate behaviour ONLY when the request also carries the
    # `X-Test-Id` header — diagnostic mode is "deep observation for
    # tagged requests", never a global verbosity boost. Production
    # traffic without the header is unaffected by any toggle value.
    #
    # Granularity rationale (3 toggles vs 1):
    #   - traces_boost: Sentry tracesSampler returns 1.0 for tagged
    #     requests. Cost: extra Sentry quota for a small fraction of
    #     traffic. Cheapest to enable.
    #   - obs_stream: emits per-checkpoint events to local Redis
    #     `obs:test:<id>` for trace-CLI aggregation. Cost: ~0.1 ms
    #     per request via background drain (zero impact on /decide
    #     latency budget). Most useful for full chronology.
    #   - verbose_logs: structured-log INFO at every checkpoint
    #     (otherwise DEBUG/dropped). Cost: log-volume increase for
    #     tagged requests. Useful when Sentry/obs stream are
    #     unreachable and the operator only has docker logs.
    #
    # Full discipline: rule `diagnostic-mode`, skill
    # `diagnostic-tracing`.
    # ------------------------------------------------------------------
    diag_traces_boost: bool = False
    diag_obs_stream: bool = False
    diag_verbose_logs: bool = False

    # Bounded queue for the obs-stream background drain. Caps the
    # in-memory backlog if Redis is briefly unreachable — drops oldest
    # rather than blocking the request path. Sized for ~10s of dense
    # tagged traffic at typical checkpoint counts.
    diag_obs_queue_max: int = 10_000
    diag_obs_drain_interval_ms: int = 100
    # Per-test stream hard cap (XADD MAXLEN ~). Bounds Redis growth if
    # an operator pumps a very large probe set through one test_id.
    diag_obs_stream_maxlen: int = 10_000
    # TTL on the obs:test:<id> key. 1h is enough for the trace CLI to
    # consume; trace runs typically happen seconds-to-minutes after
    # the probe.
    diag_obs_stream_ttl_seconds: int = 3600

    # ------------------------------------------------------------------
    # Returning-user identity resolver (P2, 2026-06-05). DARK by default.
    #
    # `returning_resolver_enabled` is the GLOBAL master kill-switch and the
    # gate-#1 toggle (R4 audit): the router checks this cached bool FIRST,
    # before ANY identity Redis I/O — OFF ⇒ instant skip, zero new
    # round-trips, click_attrs / is_unique / is_returning computed exactly
    # as today (byte-identical). A finer per-company gate rides on the
    # synced campaign HASH (`returning_resolver` field, default closed,
    # wired by admin sync in P4) so a tenant opts in individually.
    #
    # `identity_redis_url` (gate #2, R4 + v2 P0.3): identity keys MUST live on
    # a dedicated, `noeviction` Redis instance — an LRU eviction of an
    # identity key silently degrades a returning user back to "new", drops
    # sticky pins, AND competes with the routing cache for memory. The boot
    # gate (`identity.assert_identity_namespace_safe`, run when the resolver
    # is enabled) enforces this — and after the 2026-06-06 incident it DEGRADES
    # rather than refusing, so a misconfigured store can never down a node:
    #   * non-local + empty ⇒ DEGRADE (disable the resolver in-memory + LOUD
    #     CRITICAL/Sentry, then boot legacy — we will NOT silently reuse the
    #     evictable routing Redis, and we will NOT take the node offline);
    #   * non-local + set but unreachable / not `noeviction` ⇒ DEGRADE likewise;
    #   * local ⇒ STRICT (raise) — dev must notice misconfig; reuse of the
    #     routing Redis is acceptable for local dev only.
    # Point this at a separate instance/DB before enabling the resolver for a
    # tenant in any non-local environment.
    #
    # `returning_uid_ttl_seconds` — sliding TTL (refreshed on every visit)
    # so memory tracks the ACTIVE returning audience, not all-time uids.
    # 180 days (R3 §6 recommendation; 1-year ceiling is a per-tenant
    # opt-in handled in admin config later, not here).
    # Default TRUE: the feature is important-by-default, and the deploy tooling
    # (deploy/render-env.sh) already forces `true`, so the bare code default
    # matches — a node/env that doesn't set the var still gets the feature.
    # SAFE: behaviour stays gated by the PER-COMPANY check in router.py
    # (`_company_returning_enabled`, reads `returning_resolver` from the campaign
    # HASH, default closed) — env-true alone changes NO routing until a company
    # opts in. The boot gate (app/identity.py) DEGRADES-not-crashes if the
    # identity-redis store is absent in a non-local env, and warns-only in a
    # local-class env (`local`/`development`) — so local boot stays quiet.
    returning_resolver_enabled: bool = True
    identity_redis_url: str = ""
    returning_uid_ttl_seconds: int = 15_552_000  # 180 days

    # ------------------------------------------------------------------
    # Signed identity cookie (`_tds_id`) — Layer-1 RECOGNITION (P2, DARK).
    # SoT: docs/development/returning-users-v2/DECISION-edge-identity-architecture.md
    #
    # A signed cookie carries the immutable WHO of a returning user so any edge
    # node recognizes them with a single in-process HMAC verify and ZERO store
    # hit (no cross-node replication ⇒ gap-free). The codec lives in
    # `app/identity_token.py`; P2 only VERIFIES (dual-accept with legacy
    # `_tds_vid`), minting is P3.
    #
    # `identity_cookie_keys` — the signing key RING, form `kid:secret,kid:secret`
    # (kid = small non-negative int; secret = raw key, UTF-8). This is a
    # DEDICATED identity key, explicitly NOT `tds_secret_key` (X-TDS-Key): so
    # rotating it never makes the fleet "look new", and a routing-secret leak
    # cannot forge identity. Empty ⇒ codec disabled ⇒ verify returns None
    # (fail-open to legacy) ⇒ byte-identical to pre-P2.
    #
    # ROTATION: add a NEW kid as `identity_cookie_active_kid` (used to sign in
    # P3) while keeping the OLD kid(s) in `identity_cookie_keys` for the overlap
    # window, so cookies signed by the old key still VERIFY. Drop a kid from the
    # ring only after every cookie it signed has expired (180d max-age).
    #
    # `identity_cookie_active_kid` — the kid used for signing/re-stamping (P3).
    # Must be present in `identity_cookie_keys`. Verify accepts ANY kid in the
    # ring regardless of this value (that is what makes rotation gap-free).
    identity_cookie_keys: str = ""
    identity_cookie_active_kid: str = ""

    # P4 (2026-06-05) — returning-user SEGMENTED ROUTING. Separate gate from the
    # resolver so identity can be computed/observed (P2/P3) WITHOUT changing
    # routing selection. DARK by default: OFF ⇒ the cascade is single-pass,
    # byte-identical to today (no audience partition, seen_before ignored).
    # ON ⇒ a returning visitor (seen_before) evaluates audience='returning'
    # flows first, falling through to 'first' flows on no match; new visitors
    # see 'first' flows only. Requires returning_resolver_enabled to be ON too
    # (seen_before / prev_* come from the resolver); if routing is ON but the
    # resolver is OFF, seen_before is never true → first-pool only (fail-safe).
    #
    # Default TRUE for the same reason as `returning_resolver_enabled` above:
    # the deploy tooling already forces `true`, and segmented routing stays
    # gated by the PER-COMPANY `_company_routing_enabled` check (router.py,
    # reads `returning_routing` from the campaign HASH, default closed) — so
    # env-true alone changes no routing until a company opts in.
    returning_routing_enabled: bool = True

    model_config = {"env_prefix": "TDS_"}

    @model_validator(mode="after")
    def _enforce_secret_presence(self) -> "Settings":
        """Refuse to boot in non-local environments without a
        valid `tds_secret_key`. Mirrors admin-api's startup guard
        (services/admin-api/app/config.py).

        Audit closure 2026-05-09 (Agent 2 HIGH-1): without this
        guard, a click-processor node deployed with
        `TDS_SECRET_KEY=""` would silently no-op BOTH the
        X-TDS-Key auth check (`if settings.tds_secret_key and
        not hmac.compare_digest(...)`) AND the T2.4 X-TDS-Body-Sig
        verifier (`if x_tds_body_sig and settings.tds_secret_key`).
        Both defenses short-circuit on the falsy `and`, leaving
        the `/admin/sync` endpoint open to unauthenticated +
        un-integrity-checked snapshot pushes from any caller —
        full multi-tenant routing tampering.

        The check fires AFTER all field defaults apply so the
        `tds_secret_key: str = ""` default still works in local
        dev (environment="development" → guard short-circuits at
        the first `if`).

        Length floor `32` matches admin-api's `tds_secret_key`
        guard and rule `api-security` "≥256 bits per HS256".
        """
        if self.environment in _LOCAL_ENVIRONMENTS:
            return self

        if not self.tds_secret_key:
            raise ValueError(
                f"TDS_SECRET_KEY must be set when TDS_ENVIRONMENT="
                f"'{self.environment}'. Empty secret silently "
                f"disables BOTH X-TDS-Key auth AND X-TDS-Body-Sig "
                f"integrity verification on /admin/sync, leaving "
                f"the snapshot-apply path open to MITM tampering. "
                f"Mirrors the admin-api guard for the same secret."
            )

        if len(self.tds_secret_key) < 32:
            raise ValueError(
                f"TDS_SECRET_KEY must be at least 32 characters "
                f"(≥256 bits per `api-security.md` HS256 target; "
                f"same length floor as admin-api). Current length: "
                f"{len(self.tds_secret_key)}."
            )

        return self

    @model_validator(mode="after")
    def _enforce_central_url_presence(self) -> "Settings":
        """Refuse to boot in non-local environments without a non-empty
        ``central_url`` when ``require_central_url`` is True.

        F.29 Sprint 1.1 (2026-05-23). Closes the catastrophic silent-disable
        path surfaced by audit 2026-05-16: AU+CA edge nodes had
        ``TDS_CENTRAL_URL=""`` → shipper silently returned at startup
        (services/click-processor/app/shipper.py:34-36 pre-F.29) →
        50-day click-persistence blackout. The shipper accepted clicks,
        wrote to local stream:clicks (4637 + 271 stockpiled), but never
        delivered to central. Central PG ``clicks`` table grew by ONE row
        in that 50-day window. Catastrophic revenue/analytics blind spot
        without any error signal — the service responded healthy on
        ``/health`` the whole time.

        Mirror of ``_enforce_secret_presence`` (above) — same shape, same
        env-tolerance carve-out, same loud-on-fail discipline. Two
        independent guards because the secret and the central URL are
        independent failure modes (you can configure one without the
        other; both must be present in non-local env).

        Gate semantics:

        * ``environment ∈ _LOCAL_ENVIRONMENTS`` → always pass (preserves
          ``make dev`` workflow + standalone-mode click-processor where
          the operator intentionally runs without a central collector,
          e.g. for an isolated load-test rig).
        * ``require_central_url == False`` → always pass (operator
          escape hatch; emits a startup warning at lifespan boot, not
          here in the validator — Pydantic validators run too early in
          the import chain to reach the configured logger).
        * Otherwise → ``central_url`` MUST be non-empty.

        Error message names the EXACT remediation path so an operator
        reading a boot-failure log can fix it without digging through
        source. Length / scheme validation is intentionally NOT added
        here — the shipper's ``httpx.AsyncClient.post`` will surface
        any URL-format issues with their own specific errors, and we
        don't want to forbid future operator-deployed proxy schemes
        (e.g. unix sockets via httpx.URL parser extensions).
        """
        if self.environment in _LOCAL_ENVIRONMENTS:
            return self

        if not self.require_central_url:
            # Operator opted out — legacy silent-disable behaviour.
            # No validator error; the shipper's runtime check in
            # ``run_shipper`` logs a WARNING when ``central_url`` is
            # empty (see F.29 Sprint 1.2 in shipper.py).
            return self

        if not self.central_url:
            raise ValueError(
                f"TDS_CENTRAL_URL must be set when TDS_ENVIRONMENT="
                f"'{self.environment}' (and TDS_REQUIRE_CENTRAL_URL is "
                f"True, which is the default per F.29 Sprint 1.1). "
                f"Empty central_url silently disabled the click shipper "
                f"for 50 days on AU+CA nodes (audit 2026-05-16) — "
                f"thousands of clicks accumulated on edge Redis with "
                f"zero delivery to central. Remediation: set "
                f"TDS_CENTRAL_URL=https://<central-host>:8200 in the "
                f"node's .env file and restart. Emergency rollback: "
                f"TDS_REQUIRE_CENTRAL_URL=false to revert to legacy "
                f"silent-disable (not recommended — fix the misconfig "
                f"instead)."
            )

        # F.29 Sprint 2.7b (2026-05-23) — HTTPS enforcement.
        # Plain http:// over WAN exposes the Sprint 2.5 shim path to
        # MITM downgrade: attacker intercepts response, replaces with
        # legacy shape, shipper ACKs all → silent click loss.
        # ``require_central_url_https`` defaults True; operator escape
        # hatch for transitional TLS rollout sets it to False.
        if self.require_central_url_https and not self.central_url.startswith("https://"):
            raise ValueError(
                f"TDS_CENTRAL_URL must use HTTPS in env="
                f"'{self.environment}' (got: {self.central_url!r}). "
                f"Plain HTTP exposes the click pipeline to MITM attacks: "
                f"an on-path attacker can downgrade the response to the "
                f"pre-F.29 legacy shape and trigger the Sprint 2.5 "
                f"backwards-compat shim → shipper ACKs all clicks as "
                f"delivered → SILENT LOSS. Remediation: configure HTTPS "
                f"termination at the central collector (reverse proxy or "
                f"direct cert) and set TDS_CENTRAL_URL=https://... "
                f"Emergency rollback for TLS-rollout transition: "
                f"TDS_REQUIRE_CENTRAL_URL_HTTPS=false (security-degraded "
                f"mode; track rolling-deploy completion in operator "
                f"runbook and flip back ASAP)."
            )

        return self

    @model_validator(mode="after")
    def _enforce_disk_queue_root_absolute(self) -> "Settings":
        """M7 fix (2026-05-11): refuse to boot with a relative
        `disk_queue_root`.

        Was a silent-data-loss footgun: if uvicorn started with
        `cwd=/`, queued click files landed at `/var/click-queue/...`
        — a path the service likely doesn't own. `chmod(0o700)`
        failures were silently swallowed, so the misconfig was
        invisible until you went looking for the lost clicks. The
        drainer's sorted-glob scan would then find no files and
        silently skip replay.

        Loud startup failure > silent runtime data loss. Operator
        sees the error message on `docker compose up` and fixes it
        before any traffic flows.

        Empty value is allowed (turns the disk-fallback feature off
        cleanly — drainer becomes a no-op). This preserves existing
        local-dev behaviour where operators don't want a system
        path created. Only non-empty + relative raises.
        """
        if self.disk_queue_root and not self.disk_queue_root.startswith("/"):
            raise ValueError(
                f"TDS_DISK_QUEUE_ROOT must be an absolute path "
                f"(starts with '/'). Got '{self.disk_queue_root}'. "
                f"Relative paths resolve against the process CWD at "
                f"runtime, which is unreliable across container / "
                f"systemd configs and has caused silent data loss "
                f"during Redis-outage fallback. Use e.g. "
                f"'/var/tds/click-queue' (default) or set to '' to "
                f"disable disk fallback entirely."
            )
        return self


settings = Settings()
