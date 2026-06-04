"""Async Redis connection pool."""

import redis.asyncio as redis

from app.config import settings

pool: redis.Redis | None = None


async def get_redis() -> redis.Redis:
    global pool
    if pool is None:
        pool = redis.from_url(
            settings.redis_url,
            decode_responses=True,
            max_connections=20,
        )
    return pool


async def close_redis():
    global pool
    if pool:
        await pool.aclose()
        pool = None


# Returning-user identity store (P2, 2026-06-05). A SEPARATE client so the
# identity keyspace can be pointed at a dedicated `noeviction` Redis instance
# (gate #2, R4 audit) via TDS_IDENTITY_REDIS_URL. Empty ⇒ reuse the routing
# pool (safe at small scale: the edge routing Redis is already noeviction).
identity_pool: redis.Redis | None = None


async def get_identity_redis() -> redis.Redis:
    """Client for the company-scoped `id:*` identity keyspace.

    Reuses the routing pool when `identity_redis_url` is empty, else opens a
    dedicated pool. Distinct from `get_redis()` so the two keyspaces can be
    physically separated without touching the hot routing path.
    """
    global identity_pool
    if not settings.identity_redis_url:
        # Shared instance — reuse the routing pool (one connection set).
        return await get_redis()
    if identity_pool is None:
        identity_pool = redis.from_url(
            settings.identity_redis_url,
            decode_responses=True,
            max_connections=20,
        )
    return identity_pool


async def close_identity_redis():
    global identity_pool
    if identity_pool:
        await identity_pool.aclose()
        identity_pool = None
