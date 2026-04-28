"""Buyer-id attribution enrichment — Stage 2 / Vector 2.10.

Stage 3's `clicks` table will carry the canonical attribution chain
columns (`team_id`, `department_id`, `custom_group_id`, `company_id`)
populated from `clicks.buyer_id`. The chain is pre-resolved at sync
time by `services/admin-api/app/sync/builders/users.py`, so
enrichment at click time is a single Redis HGETALL — no chain
walking, no follow-up queries, fits well inside the 10ms hot-path
budget.

Design pin: `docs/design/PARAMETER-SYSTEM.md` + Round 2 single-
canonical-buyer-id rule. `clicks.buyer_id` is the ONLY attribution
input — `created_by` is access rights only, never attribution.

This module is the consumption side of Vector 2.10. The actual
click-row population (writing the resolved fields to the click row)
lives in Stage 3 (`clicks` table migration not yet shipped). For
Stage 2 this helper exists primarily for testing the sync contract:
admin-api emits → click-processor reads → enrichment produces the
expected chain.
"""

from __future__ import annotations

import logging
import types
from typing import Any

# Narrow Redis exception types — see `enrich_buyer` rationale below.
# `redis.asyncio` raises `redis.exceptions.RedisError` (parent class
# covers ConnectionError / TimeoutError / ResponseError / etc.).
# Importing lazily so the helper still works in test environments
# that don't install the redis client (mocks supply their own).
try:
    from redis.exceptions import RedisError  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover — redis installed in prod
    RedisError = Exception  # fallback that's still safer than bare `Exception`


logger = logging.getLogger("tds.enrichment")


__all__ = ["enrich_buyer", "EMPTY_ENRICHMENT"]


# Empty result returned when buyer_id is missing, malformed, or
# resolves to no Redis row. Same keys as a successful resolution so
# downstream consumers can treat it as a uniform shape.
#
# Frozen via `MappingProxyType` so callers cannot accidentally
# mutate the module-level constant — `result["team_id"] = "5"`
# raises `TypeError` instead of corrupting future enrichment calls
# (per code-review MEDIUM 2026-04-28).
EMPTY_ENRICHMENT: types.MappingProxyType[str, str | None] = types.MappingProxyType({
    "team_id": None,
    "department_id": None,
    "custom_group_id": None,
    "company_id": None,
})


async def enrich_buyer(redis, buyer_id: Any) -> dict[str, str | None]:
    """Resolve `clicks.buyer_id` to the canonical attribution chain.

    Args:
        redis: Async Redis client (`redis.asyncio.Redis`).
        buyer_id: The buyer_id value from the click — typically a
            string (resolved via `resolve_slots` for the `buyer_id`
            slot). Numeric inputs are coerced to string.

    Returns:
        `{team_id, department_id, custom_group_id, company_id}` —
        all values are strings (Redis storage shape) or `None` when
        the chain can't be resolved. Callers writing to the eventual
        `clicks` row should treat `None` as SQL NULL.

    Behavior:
      - Missing / empty buyer_id → `EMPTY_ENRICHMENT`.
      - User hash exists but status != 'active' → `EMPTY_ENRICHMENT`
        (paused / archived users shouldn't enrich live traffic).
      - User hash exists with active status → resolved chain. Any
        chain link the user builder couldn't resolve (e.g., user
        has no team) lands as `None`.
      - Redis error → log warning + return `EMPTY_ENRICHMENT` (fail-
        open: enrichment is best-effort, never block routing).
    """
    if buyer_id is None:
        return dict(EMPTY_ENRICHMENT)
    bid_str = str(buyer_id).strip()
    if not bid_str:
        return dict(EMPTY_ENRICHMENT)

    # Numeric-only validation closes a Redis-key-injection class
    # (LOW-002) and protects against malformed slot resolution
    # leaking exotic strings into the keyspace. `users.id` is the
    # canonical FK target — `clicks.buyer_id` (per Round 2 single-
    # canonical-buyer-id rule) is REQUIRED to be the numeric user
    # PK. Non-digit input is never legitimate; it indicates either
    # (a) advertiser misconfiguration that should be caught upstream
    # (admin-api param_mappings validator), or (b) a malicious
    # crafted `?buyer_id=...` value attempting injection. Either
    # way, returning EMPTY_ENRICHMENT is the correct fail-safe —
    # the click still routes (this enrichment is post-decision
    # context, not routing-critical), just without attribution.
    # Architecture audit 2026-04-28 confirmed this is by-design
    # behavior, not silent data loss.
    if not bid_str.isdigit():
        return dict(EMPTY_ENRICHMENT)

    # Re-bind to a clearly-named variable AFTER the digit-only gate so
    # the Redis key construction below is obviously safe. Future editors
    # who don't read the long comment above won't accidentally move the
    # key construction before the validation step (architecture audit
    # 2026-04-28 HIGH: readability hardening for security-critical guard).
    validated_buyer_id = bid_str

    try:
        user = await redis.hgetall(f"user:{validated_buyer_id}")
    except (RedisError, ConnectionError, TimeoutError, OSError) as exc:
        # Narrow catch — log Redis-layer faults, let programming
        # errors (`AttributeError`, `TypeError`, etc.) propagate so
        # they hit Sentry instead of silently degrading enrichment.
        # Per code-review HIGH 2026-04-28 + `resilience-patterns`
        # rule's fail-open policy for non-routing-critical paths.
        logger.warning(
            "enrich_buyer: Redis lookup failed for buyer_id=%s: %s",
            bid_str, exc,
        )
        return dict(EMPTY_ENRICHMENT)

    if not user:
        # No user row in Redis — buyer_id either doesn't exist or
        # references a non-active user (paused / archived users get
        # filtered out by the sync builder's `WHERE status='active'`).
        return dict(EMPTY_ENRICHMENT)

    # Defense in depth — the builder filters by status='active', but
    # if a stale build snuck a non-active row through, refuse to
    # enrich. Log a warning so ops sees the sync drift rather than
    # silently NULL'ing attribution (per security audit MEDIUM-001).
    if user.get("status") != "active":
        logger.warning(
            "enrich_buyer: user:%s in Redis has status=%r — sync drift?",
            bid_str, user.get("status"),
        )
        return dict(EMPTY_ENRICHMENT)

    return {
        "team_id": _empty_to_none(user.get("team_id")),
        "department_id": _empty_to_none(user.get("department_id")),
        "custom_group_id": _empty_to_none(user.get("custom_group_id")),
        "company_id": _empty_to_none(user.get("company_id")),
    }


def _empty_to_none(value: Any) -> str | None:
    """Empty string in Redis → `None` for SQL NULL semantics.

    The user builder writes `""` for unresolved chain links (Redis
    can't store a Python None in a HASH field). We invert here so
    callers write SQL NULL into the eventual click row instead of
    propagating an empty string into queries.
    """
    if value is None or value == "":
        return None
    return str(value)
