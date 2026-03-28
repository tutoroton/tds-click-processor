"""Request/response models for click-processor."""

from pydantic import BaseModel, Field


class ClickRequest(BaseModel):
    """Incoming request from CF Worker."""
    click_id: str = Field(max_length=128, pattern=r'^[a-zA-Z0-9_\-]+$')
    visitor_id: str | None = Field(default=None, max_length=128, pattern=r'^[a-zA-Z0-9_\-]*$')
    is_returning: bool = False
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
    path: str = ""
    query_params: dict = {}


class ClickResponse(BaseModel):
    """Response to CF Worker — where to redirect the user."""
    url: str
    status: int = 302


class HealthResponse(BaseModel):
    node_id: str
    region: str
    redis: bool
    campaigns_loaded: int
    uptime_seconds: float
