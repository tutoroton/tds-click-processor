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

    model_config = {"env_prefix": "TDS_"}


settings = Settings()
