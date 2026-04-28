"""Tests for `app.enrichment` — Vector 2.10 buyer-id chain resolution.

Coverage:
  - Happy path: buyer_id resolves to full chain.
  - Missing user hash → EMPTY_ENRICHMENT.
  - Inactive user hash → EMPTY_ENRICHMENT (defense in depth).
  - Empty chain links → None (NULL semantics for SQL).
  - Missing / empty / None buyer_id → EMPTY_ENRICHMENT.
  - Redis errors → fail-open with EMPTY_ENRICHMENT (best-effort).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.enrichment import EMPTY_ENRICHMENT, enrich_buyer


def _redis_with(hash_data: dict | None) -> MagicMock:
    """Mock Redis whose hgetall returns `hash_data` (or {} if None)."""
    redis = MagicMock()
    redis.hgetall = AsyncMock(return_value=hash_data or {})
    return redis


def _redis_raises(exc: Exception) -> MagicMock:
    redis = MagicMock()
    redis.hgetall = AsyncMock(side_effect=exc)
    return redis


# ============================================================
# Happy path
# ============================================================


class TestEnrichBuyerHappyPath:
    @pytest.mark.asyncio
    async def test_full_chain(self):
        redis = _redis_with({
            "id": "42",
            "company_id": "7",
            "team_id": "3",
            "department_id": "5",
            "custom_group_id": "11",
            "status": "active",
        })
        result = await enrich_buyer(redis, "42")

        assert result == {
            "team_id": "3",
            "department_id": "5",
            "custom_group_id": "11",
            "company_id": "7",
        }

    @pytest.mark.asyncio
    async def test_partial_chain_only_company(self):
        # User has no team/department/custom_group — chain links are
        # empty strings in Redis (builder writes "" for unresolved).
        # Enrichment translates "" → None for SQL NULL semantics.
        redis = _redis_with({
            "id": "1",
            "company_id": "7",
            "team_id": "",
            "department_id": "",
            "custom_group_id": "",
            "status": "active",
        })
        result = await enrich_buyer(redis, "1")

        assert result == {
            "team_id": None,
            "department_id": None,
            "custom_group_id": None,
            "company_id": "7",
        }

    @pytest.mark.asyncio
    async def test_buyer_id_int_coerced_to_string(self):
        # Slot resolution emits strings, but defensive: int input
        # should still resolve via str() coercion.
        redis = _redis_with({
            "id": "42",
            "company_id": "7",
            "team_id": "3",
            "department_id": "",
            "custom_group_id": "",
            "status": "active",
        })
        result = await enrich_buyer(redis, 42)

        # hgetall called with string-formed key.
        redis.hgetall.assert_awaited_once_with("user:42")
        assert result["team_id"] == "3"

    @pytest.mark.asyncio
    async def test_buyer_id_with_whitespace(self):
        redis = _redis_with({
            "id": "5",
            "company_id": "7",
            "team_id": "",
            "department_id": "",
            "custom_group_id": "",
            "status": "active",
        })
        result = await enrich_buyer(redis, "  5  ")

        redis.hgetall.assert_awaited_once_with("user:5")
        assert result["company_id"] == "7"


# ============================================================
# Missing / inactive / malformed
# ============================================================


class TestEnrichBuyerMissingPaths:
    @pytest.mark.asyncio
    async def test_no_redis_row(self):
        redis = _redis_with(None)
        result = await enrich_buyer(redis, "999")

        assert result == EMPTY_ENRICHMENT

    @pytest.mark.asyncio
    async def test_inactive_user_returns_empty(self):
        # Builder filters by status='active', but defense-in-depth:
        # if a stale build snuck a paused user through, refuse to
        # enrich. Don't attribute traffic to a paused account.
        redis = _redis_with({
            "id": "42",
            "company_id": "7",
            "team_id": "3",
            "department_id": "5",
            "custom_group_id": "11",
            "status": "paused",
        })
        result = await enrich_buyer(redis, "42")

        assert result == EMPTY_ENRICHMENT

    @pytest.mark.asyncio
    async def test_archived_user_returns_empty(self):
        redis = _redis_with({
            "id": "42",
            "company_id": "7",
            "team_id": "3",
            "department_id": "5",
            "custom_group_id": "11",
            "status": "archived",
        })
        result = await enrich_buyer(redis, "42")

        assert result == EMPTY_ENRICHMENT


# ============================================================
# Input validation
# ============================================================


class TestEnrichBuyerInputs:
    @pytest.mark.asyncio
    async def test_none_buyer_id(self):
        redis = _redis_with(None)
        result = await enrich_buyer(redis, None)

        assert dict(result) == dict(EMPTY_ENRICHMENT)
        redis.hgetall.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_empty_string_buyer_id(self):
        redis = _redis_with(None)
        result = await enrich_buyer(redis, "")

        assert dict(result) == dict(EMPTY_ENRICHMENT)
        redis.hgetall.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_whitespace_only_buyer_id(self):
        redis = _redis_with(None)
        result = await enrich_buyer(redis, "   ")

        assert dict(result) == dict(EMPTY_ENRICHMENT)
        redis.hgetall.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_non_numeric_buyer_id_returns_empty(self):
        # `users.id` is always a positive integer FK — a non-digit
        # buyer_id is never legitimate. Defensive: closes Redis-key
        # injection class (security audit LOW-002) and protects
        # against malformed slot resolution leaking into the keyspace.
        redis = _redis_with(None)
        result = await enrich_buyer(redis, "1; DROP TABLE users")

        assert dict(result) == dict(EMPTY_ENRICHMENT)
        redis.hgetall.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_buyer_id_with_newlines_rejected(self):
        # Even if redis-py is RESP-safe, exotic shapes shouldn't
        # land in the namespace. isdigit() catches them.
        redis = _redis_with(None)
        result = await enrich_buyer(redis, "42\nFLUSHALL")

        assert dict(result) == dict(EMPTY_ENRICHMENT)
        redis.hgetall.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_buyer_id_path_traversal_rejected(self):
        redis = _redis_with(None)
        result = await enrich_buyer(redis, "../admin")

        assert dict(result) == dict(EMPTY_ENRICHMENT)
        redis.hgetall.assert_not_awaited()


# ============================================================
# Failure modes — fail-open
# ============================================================


class TestEnrichBuyerFailures:
    @pytest.mark.asyncio
    async def test_redis_error_returns_empty(self):
        # Connection drop, timeout, anything — enrichment is best-
        # effort and a Redis blip must not break routing.
        redis = _redis_raises(ConnectionError("Redis down"))
        result = await enrich_buyer(redis, "42")

        assert result == EMPTY_ENRICHMENT

    @pytest.mark.asyncio
    async def test_redis_timeout_returns_empty(self):
        redis = _redis_raises(TimeoutError("hgetall timeout"))
        result = await enrich_buyer(redis, "42")

        assert result == EMPTY_ENRICHMENT


# ============================================================
# Stage 3 storage shape — None NOT empty string
# ============================================================


class TestEnrichmentSqlNullSemantics:
    @pytest.mark.asyncio
    async def test_none_not_empty_string_for_unresolved(self):
        """Stage 3 clicks table will store these as nullable INT
        columns. Empty strings would break INSERT casts; the helper
        translates Redis "" → None so writes can use NULL directly.
        """
        redis = _redis_with({
            "id": "1",
            "company_id": "7",
            "team_id": "",
            "department_id": "",
            "custom_group_id": "",
            "status": "active",
        })
        result = await enrich_buyer(redis, "1")

        for key in ("team_id", "department_id", "custom_group_id"):
            assert result[key] is None
            assert result[key] != ""

    @pytest.mark.asyncio
    async def test_returns_immutable_shape_default(self):
        # EMPTY_ENRICHMENT exposed for callers who want a pre-built
        # default; verify it has all canonical keys.
        assert set(EMPTY_ENRICHMENT.keys()) == {
            "team_id", "department_id", "custom_group_id", "company_id",
        }
        assert all(v is None for v in EMPTY_ENRICHMENT.values())

    def test_empty_enrichment_constant_is_frozen(self):
        # `MappingProxyType` makes the module constant read-only —
        # callers can't accidentally corrupt it via in-place mutation
        # (per code-review MEDIUM 2026-04-28).
        with pytest.raises(TypeError):
            EMPTY_ENRICHMENT["team_id"] = "5"  # type: ignore[index]

    @pytest.mark.asyncio
    async def test_returned_dict_is_independent_copy(self):
        # Mutating the returned dict from one call must not affect
        # subsequent calls. enrich_buyer returns a fresh dict via
        # `dict(EMPTY_ENRICHMENT)`.
        redis = _redis_with(None)
        first = await enrich_buyer(redis, "999")
        first["team_id"] = "tampered"
        second = await enrich_buyer(redis, "999")
        assert second["team_id"] is None


# ============================================================
# Operator visibility — log signals
# ============================================================


class TestEnrichBuyerLogging:
    @pytest.mark.asyncio
    async def test_stale_status_logs_warning(self, caplog):
        # Per security audit MEDIUM-001: stale-status defense should
        # emit a warning so ops sees sync drift rather than silently
        # NULL'ing attribution.
        import logging
        redis = _redis_with({
            "id": "42",
            "company_id": "7",
            "team_id": "3",
            "department_id": "5",
            "custom_group_id": "11",
            "status": "paused",
        })
        with caplog.at_level(logging.WARNING, logger="tds.enrichment"):
            await enrich_buyer(redis, "42")
        assert any("sync drift" in rec.message for rec in caplog.records)

    @pytest.mark.asyncio
    async def test_redis_error_logs_warning(self, caplog):
        import logging
        redis = _redis_raises(ConnectionError("Redis down"))
        with caplog.at_level(logging.WARNING, logger="tds.enrichment"):
            await enrich_buyer(redis, "42")
        assert any("Redis lookup failed" in rec.message for rec in caplog.records)
