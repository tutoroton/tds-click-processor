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

    # Sync
    sync_interval_seconds: int = 30
    full_sync_interval_seconds: int = 60

    model_config = {"env_prefix": "TDS_"}


settings = Settings()
