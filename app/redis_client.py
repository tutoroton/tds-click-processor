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
