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
