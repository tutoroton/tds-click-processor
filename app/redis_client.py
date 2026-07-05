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
