"""Request/response models for click-processor."""

from pydantic import BaseModel, Field, field_validator


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
    """Response to CF Worker — where to redirect the user."""
    url: str
    status: int = 302


class HealthResponse(BaseModel):
    node_id: str
    region: str
    redis: bool
    campaigns_loaded: int
    sync_version: int = 0
    uptime_seconds: float
