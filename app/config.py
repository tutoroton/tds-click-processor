"""Click-processor configuration. All settings from environment variables."""

from pydantic_settings import BaseSettings


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

    model_config = {"env_prefix": "TDS_"}


settings = Settings()
