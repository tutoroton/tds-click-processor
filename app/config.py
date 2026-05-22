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

    # Redis (local, same machine/container)
    redis_url: str = "redis://redis:6379/0"

    # Auth (shared secret with CF Worker)
    tds_secret_key: str = ""

    # Central server (for sync + click shipping).
    #
    # F.29 Sprint 1.1 (2026-05-22) — central_url is no longer "optional"
    # in non-local environments. The audit 2026-05-16 caught AU+CA edge
    # nodes deployed with TDS_CENTRAL_URL="" for 50 days: the shipper
    # silently returned at startup (services/click-processor/app/
    # shipper.py:34-36 pre-F.29), accepted clicks into local Redis, but
    # never delivered them upstream. Central PG `clicks` grew by 1 row
    # in that window while edge stream:clicks accumulated thousands.
    #
    # `require_central_url` (gate flag, default True per F.29 plan §7.1)
    # promotes the misconfig from silent-disable to boot-time fail-closed
    # in staging / production / any non-local env. Mirrors the proven
    # `_enforce_secret_presence` pattern (line ~168 below) — same shape,
    # same env-tolerance carve-out, same loud-on-fail discipline.
    # Operator escape hatch: set TDS_REQUIRE_CENTRAL_URL=false to revert
    # to legacy silent-disable for emergency rollback (NOT recommended;
    # use only if the boot-time refusal blocks a known-good node during
    # incident recovery, then fix and flip back).
    central_url: str = ""
    central_api_key: str = ""
    require_central_url: bool = True

    # Fallback URL when routing fails
    fallback_url: str = "https://adstudy.dev"

    # Sentry (from env, never hardcode)
    sentry_dsn: str = ""

    # Sync
    sync_interval_seconds: int = 30
    full_sync_interval_seconds: int = 60

    # T2.1 / G-22 — `stream:clicks` inline MAXLEN cap (zero-loss
    # foundation). The shipper task XTRIMs the stream to ~10k after
    # every successful batch ship to central; without this hard cap
    # a central-collector outage would let `/decide`'s XADD path
    # grow the stream unbounded → Redis OOM → routing degradation
    # + click loss (`noeviction` policy on edge Redis means writes
    # eventually start failing). The inline `MAXLEN ~ N` on every
    # XADD enforces a worst-case ceiling that's:
    #   - large enough to absorb collector outages of hours under
    #     normal click rates (1M @ ~500 B/click ≈ 500 MB Redis budget,
    #     well below typical 4 GB node provisioning)
    #   - small enough that even at saturation the stream stays
    #     bounded
    # `~` (approximate) trim is O(1) per XADD vs O(N) exact trim;
    # at the cost of the cap being honoured to ±10% of the target
    # rather than exact — fine for a defense-in-depth ceiling.
    # Tunable per-environment via TDS_STREAM_CLICKS_MAXLEN.
    stream_clicks_maxlen: int = 1_000_000

    # H1 fix (2026-05-11): TTL for the per-click idempotency marker
    # `click:seen:<click_id>` set by `acquire_click_dedup` in main.py.
    # The marker MUST outlive the realistic Worker→edge retry window
    # but eventually expire to bound Redis memory growth. 30 days
    # aligns with `data-flow.md`'s `click:{click_id}` TTL — the click
    # record itself is referenced by postbacks up to 30d after the
    # initial 302, so any genuine duplicate within that window is a
    # retry / replay, not a fresh click. After 30d the same click_id
    # is practically guaranteed to be a new generation (UUID v7-class
    # rollover bound is in the centuries; collision is impossible at
    # observed traffic). Operator override path: env var
    # `TDS_CLICK_DEDUP_TTL_SECONDS`. Set to 0 to DISABLE dedup
    # entirely (operator escape hatch for Redis OOM or extreme
    # retry-storm incidents during a deploy).
    click_dedup_ttl_seconds: int = 86400 * 30  # 30 days, 0 = disabled

    # T2.2 / G-23 — disk fallback queue for clicks when XADD fails.
    # The MAXLEN cap above defends against unbounded growth, but
    # cannot help when Redis itself is unreachable (container OOM,
    # restart, brief network partition). Without this fallback,
    # every click during a Redis outage is LOST — log + Sentry
    # capture, but the click never lands in stream:clicks, never
    # ships to central, never appears in analytics. Revenue blind
    # spot.
    #
    # On XADD failure, /decide writes the click record to a JSON
    # file under `disk_queue_root` (atomic write — .tmp + rename).
    # A background drainer task scans the queue every
    # `disk_queue_drain_interval_seconds` and replays files back
    # into Redis once it recovers. Drained files are unlinked.
    #
    # Cap (`disk_queue_max_files`) bounds the disk usage during
    # prolonged outages — at 100k files * ~500 B = ~50 MB budget.
    # Exceeding the cap CRITICAL-logs and rejects the enqueue
    # (loud failure) rather than silently rotating oldest. If your
    # incident response can't recover Redis within the cap window,
    # the operator's options are: scale Redis, raise the cap,
    # accept loss for new clicks. We never silently drop the
    # OLDEST click — that's revenue we already earned.
    # M7 fix (2026-05-11): absolute-path requirement.
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
    disk_queue_max_files: int = 100_000
    disk_queue_drain_interval_seconds: int = 30

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
                f"TDS_CENTRAL_URL=http://<central-host>:8200 in the "
                f"node's .env file and restart. Emergency rollback: "
                f"TDS_REQUIRE_CENTRAL_URL=false to revert to legacy "
                f"silent-disable (not recommended — fix the misconfig "
                f"instead)."
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
