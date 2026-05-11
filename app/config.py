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

    # Central server (for sync, optional for standalone mode)
    central_url: str = ""
    central_api_key: str = ""

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
    disk_queue_root: str = "var/click-queue"
    disk_queue_max_files: int = 100_000
    disk_queue_drain_interval_seconds: int = 30

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


settings = Settings()
