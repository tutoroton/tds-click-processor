"""Async Redis connection pool."""

import redis.asyncio as redis
from redis.asyncio.connection import BlockingConnectionPool

from app.config import settings

pool: redis.Redis | None = None


async def get_redis() -> redis.Redis:
    global pool
    if pool is None:
        # F4 (GTD-R173) — a sized, bounded-wait BlockingConnectionPool replaces
        # the default non-blocking `ConnectionPool(max_connections=20)`. The old
        # pool raised `ConnectionError("Too many connections")` synchronously on
        # exhaustion under a concurrency burst → the routing stage acquiring a
        # connection fail-opened (offer-miss under load). The blocking pool WAITS
        # up to `timeout` (per-acquire) for a connection to free instead of
        # raising; deadlock-free because the hot path holds <=1 conn at any
        # instant. `socket_timeout`/`socket_connect_timeout` also bound a hung
        # (not-down) Redis op. All four are env-tunable (config.py). See
        # FIX-DESIGN-F4.md / FIX-PLAN.md §1.
        pool = redis.Redis(connection_pool=BlockingConnectionPool.from_url(
            settings.redis_url,
            decode_responses=True,
            max_connections=settings.redis_max_connections,
            timeout=settings.redis_pool_timeout_seconds,
            socket_timeout=settings.redis_socket_timeout_seconds,
            socket_connect_timeout=settings.redis_socket_connect_timeout_seconds,
        ))
    return pool


async def close_redis():
    global pool
    if pool:
        await pool.aclose()
        pool = None


# Returning-user identity store (P2, 2026-06-05). A SEPARATE client so the
# identity keyspace lives on a dedicated `noeviction` Redis instance
# (DECISION-edge-identity-architecture.md L2; D30) via TDS_IDENTITY_REDIS_URL.
# In production/staging that URL is a compose-literal pointing at the
# `identity-redis` service (noeviction). Empty ⇒ reuse the routing pool — a
# LOCAL-DEV fallback ONLY: the edge routing Redis EVICTS (volatile-lru), so
# reusing it for identity would silently degrade returning users back to "new"
# and drop sticky pins. The boot gate refuses/degrades in non-local when the
# dedicated store is absent, so this fallback is never taken on a real node.
identity_pool: redis.Redis | None = None


async def get_identity_redis() -> redis.Redis:
    """Client for the company-scoped `id:*` identity keyspace.

    Opens a dedicated pool when `identity_redis_url` is set (the production
    path — a separate noeviction instance); reuses the routing pool only when
    empty (local dev). Distinct from `get_redis()` so the two keyspaces are
    physically separated without touching the hot routing path.
    """
    global identity_pool
    if not settings.identity_redis_url:
        # Shared instance — reuse the routing pool (one connection set).
        return await get_redis()
    if identity_pool is None:
        # F4 (GTD-R173) — identical BlockingConnectionPool treatment on the
        # SEPARATE identity pool (same env knobs) so a fail-open in the
        # returning-user identity keyspace can't recur under load either. A
        # distinct pool means identity acquires never contend with the routing
        # pool (no cross-pool wait → the deadlock-freedom argument still holds).
        identity_pool = redis.Redis(connection_pool=BlockingConnectionPool.from_url(
            settings.identity_redis_url,
            decode_responses=True,
            max_connections=settings.redis_max_connections,
            timeout=settings.redis_pool_timeout_seconds,
            socket_timeout=settings.redis_socket_timeout_seconds,
            socket_connect_timeout=settings.redis_socket_connect_timeout_seconds,
        ))
    return identity_pool


async def close_identity_redis():
    global identity_pool
    if identity_pool:
        await identity_pool.aclose()
        identity_pool = None


# Click shipper drainer (TDSP-E20, 2026-07-27). A DEDICATED pool so the
# shipper's blocking `XREADGROUP ... BLOCK=BATCH_TIMEOUT_MS` (app/shipper.py)
# never races the ROUTING pool's `socket_timeout` above (sized short, for
# `/decide` — see config.py's `redis_shipper_*` comment for the full
# incident writeup: a shared timeout shorter than BLOCK made every idle
# poll throw `TimeoutError`, ~592K duplicate Sentry events). Unlike
# `identity_pool`, this is the SAME physical Redis instance/URL as routing
# — just an isolated connection pool with shipper-appropriate timeouts, not
# a separate deployment.
shipper_pool: redis.Redis | None = None


async def get_shipper_redis() -> redis.Redis:
    """Client for the click-shipper's local-stream drainer.

    Isolated from `get_redis()` purely on timeout sizing: the shipper's
    `BLOCK` wait needs a client-side `socket_timeout` WITH MARGIN above its
    own BLOCK value, while the routing pool's short timeout is correctly
    sized for the `/decide` hot path. Sharing a pool forces one of the two
    to be wrong.
    """
    global shipper_pool
    if shipper_pool is None:
        shipper_pool = redis.Redis(connection_pool=BlockingConnectionPool.from_url(
            settings.redis_url,
            decode_responses=True,
            max_connections=settings.redis_shipper_max_connections,
            timeout=settings.redis_shipper_pool_timeout_seconds,
            socket_timeout=settings.redis_shipper_socket_timeout_seconds,
            socket_connect_timeout=settings.redis_shipper_socket_connect_timeout_seconds,
        ))
    return shipper_pool


async def close_shipper_redis():
    global shipper_pool
    if shipper_pool:
        await shipper_pool.aclose()
        shipper_pool = None
