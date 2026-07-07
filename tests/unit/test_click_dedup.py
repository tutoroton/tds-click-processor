"""Tests for the click idempotency gate (H1 fix, 2026-05-11).

Live evidence motivating the fix: Sentry GEO-TDS-WORKER-1 showed 18
events / 9 users in 1 hour before audit 2026-05-11. Same click_id
(e.g. `019e1407e312e5ba5d38b0f9`) succeeded on BOTH AU and CA edge
nodes within ~3 seconds because the Worker's 2s `AbortSignal.timeout`
fires roughly when click-processor's response is still on the wire
(EU→AU 290ms each way + ~1.7s click-processor under burst load).

Without an idempotency gate, both `/decide` calls XADD to
`stream:clicks` → ClickHouse double-counts the click + postbacks
fire twice for the same advertiser. The SETNX gate at the start of
the success path ensures exactly one of the concurrent calls writes
to the stream.

Three layers of defense covered here:

  1. Settings field exists and defaults to a 24-hour TTL (F-4, audit
     2026-05-25; lowered from 30d — node-local marker only needs to
     cover the same-node retry window of seconds).
  2. The `_acquire_click_dedup` helper returns the documented
     three-state result (True / False / None).
  3. Disabled path (`click_dedup_ttl_seconds=0`) short-circuits
     without touching Redis — operator escape hatch.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import pytest


# ---------------------------------------------------------------------------
# Source-pin: settings field
# ---------------------------------------------------------------------------


class TestSettingsField:
    def test_click_dedup_ttl_seconds_is_named_field(self):
        """The TTL MUST be a named Settings field (not a hardcoded
        literal at the callsite) so operators can flip via
        TDS_CLICK_DEDUP_TTL_SECONDS without rebuilding."""
        from app.config import Settings

        assert "click_dedup_ttl_seconds" in Settings.model_fields, (
            "Settings.click_dedup_ttl_seconds must be defined so "
            "operators can override via TDS_CLICK_DEDUP_TTL_SECONDS "
            "(H1 escape hatch)."
        )

    def test_default_is_24_hours(self):
        """Default 24h (F-4, audit 2026-05-25, lowered from 30d). This
        NODE-LOCAL marker only suppresses a same-node retry — which
        arrives within SECONDS (the Worker's 2s AbortSignal window), not
        days. It is also write-only and fully backstopped by the
        collector's central dedup, so a generous 24h amply covers the
        real window while bounding Redis memory ~30×."""
        from app.config import settings

        assert settings.click_dedup_ttl_seconds == 86400, (
            "Default dedup TTL should be 24 hours (86400 seconds) per "
            "F-4 (audit 2026-05-25)."
        )


# ---------------------------------------------------------------------------
# _acquire_click_dedup helper behaviour
# ---------------------------------------------------------------------------


class TestAcquireClickDedup:
    """The helper returns three documented states.

    True  → first-seen (proceed with XADD)
    False → duplicate detected (skip XADD)
    None  → unavailable (fail-open, proceed with XADD)
    """

    @pytest.mark.asyncio
    async def test_first_seen_returns_true(self):
        """SETNX returns truthy on first-set → helper returns True."""
        from app import main

        mock_redis = AsyncMock()
        mock_redis.set = AsyncMock(return_value=True)
        with patch("app.main.get_redis", AsyncMock(return_value=mock_redis)):
            result = await main._acquire_click_dedup("test-click-001")
        assert result is True
        # Pin the redis-py call shape: NX + EX TTL.
        mock_redis.set.assert_awaited_once_with(
            "click:seen:test-click-001",
            "1",
            nx=True,
            ex=86400,  # default TTL — 24h (F-4, lowered from 30d)
        )

    @pytest.mark.asyncio
    async def test_duplicate_returns_false(self):
        """SETNX returns None on collision (key existed) → helper
        normalises to False so caller's `is False` branch fires."""
        from app import main

        mock_redis = AsyncMock()
        mock_redis.set = AsyncMock(return_value=None)
        with patch("app.main.get_redis", AsyncMock(return_value=mock_redis)):
            result = await main._acquire_click_dedup("test-click-002")
        assert result is False

    @pytest.mark.asyncio
    async def test_redis_error_returns_none_fail_open(self):
        """Redis impaired → helper returns None (fail-open). Caller
        proceeds with XADD; downstream ClickHouse dedup is the
        safety net. Better one duplicate than one lost click."""
        from app import main

        mock_redis = AsyncMock()
        mock_redis.set = AsyncMock(side_effect=Exception("redis down"))
        with patch("app.main.get_redis", AsyncMock(return_value=mock_redis)):
            result = await main._acquire_click_dedup("test-click-003")
        assert result is None

    @pytest.mark.asyncio
    async def test_disabled_short_circuits_without_redis_call(self):
        """When ttl=0 (operator escape hatch), the helper MUST NOT
        touch Redis — no SET call, no overhead."""
        from app import main

        mock_redis = AsyncMock()
        mock_redis.set = AsyncMock()
        with patch("app.main.settings.click_dedup_ttl_seconds", 0), \
             patch("app.main.get_redis", AsyncMock(return_value=mock_redis)):
            result = await main._acquire_click_dedup("test-click-004")
        assert result is None
        mock_redis.set.assert_not_called()

    @pytest.mark.asyncio
    async def test_atomicity_concurrent_calls_one_winner(self):
        """Simulate N concurrent calls with the same click_id —
        exactly one MUST receive True (the SETNX winner); the rest
        get False. This validates the contract; the real atomicity
        is the Redis SET NX primitive (which is itself unit-tested
        upstream)."""
        from app import main

        # Fake Redis state — first call wins, all subsequent see
        # the marker as set. Using a mutable closure to simulate
        # the SETNX semantics in a single-threaded test.
        state = {"acquired": False}

        async def fake_set(*args, **kwargs):
            if state["acquired"]:
                return None  # collision
            state["acquired"] = True
            return True

        mock_redis = AsyncMock()
        mock_redis.set = fake_set
        with patch("app.main.get_redis", AsyncMock(return_value=mock_redis)):
            results = await asyncio.gather(
                *[main._acquire_click_dedup("burst-click") for _ in range(10)]
            )

        winners = [r for r in results if r is True]
        losers = [r for r in results if r is False]
        assert len(winners) == 1, (
            f"Exactly one concurrent call should win (got {len(winners)})"
        )
        assert len(losers) == 9


# ---------------------------------------------------------------------------
# Source-pins: code shape (so refactors can't silently disable the gate)
# ---------------------------------------------------------------------------


class TestSourcePins:
    """Read main.py source and assert key shapes — defends against
    a refactor that drops the dedup call from the /decide path."""

    def test_helper_function_exists(self):
        from app import main

        assert hasattr(main, "_acquire_click_dedup"), (
            "`_acquire_click_dedup` is the canonical dedup helper. "
            "If renamed, update tests + skill `routing-stress-audit`."
        )
        assert asyncio.iscoroutinefunction(main._acquire_click_dedup), (
            "Helper must be async (it awaits Redis)."
        )

    def test_decide_handler_calls_dedup_before_xadd(self):
        """The `/decide` source MUST call `_acquire_click_dedup`
        BEFORE the REAL click's `r.xadd("stream:clicks", ...)` line. A
        future refactor that moves the dedup AFTER the XADD defeats the
        whole purpose — would still double-write before checking.

        LOSSFIX P1b (2026-07-07) — anchored on `json.dumps(click_record`
        rather than a bare `xadd(` search: `/decide` has a SECOND,
        EARLIER `xadd(` call for the synthetic smoke-probe click (which
        deliberately does NOT participate in dedup — it's a pipeline-
        liveness probe, not real traffic), so a whitespace-insensitive
        "first xadd(" search would false-positive against the smoke
        path once M1 changed the real XADD's indentation. Searching for
        the `click_record` JSON-encode instead targets the REAL click's
        XADD specifically (smoke encodes `smoke_record`, never
        `click_record`).
        """
        import inspect
        from app import main

        src = inspect.getsource(main.decide)
        assert "_acquire_click_dedup" in src, (
            "/decide MUST call _acquire_click_dedup() in its body."
        )
        # Position check: the dedup call must precede the REAL click's xadd.
        dedup_pos = src.find("_acquire_click_dedup")
        xadd_pos = src.find("json.dumps(click_record")
        assert xadd_pos > 0, (
            "Expected to find the real click's XADD payload "
            "(json.dumps(click_record, ...)) in /decide's source."
        )
        assert dedup_pos < xadd_pos, (
            "Dedup call must precede the XADD — otherwise a duplicate "
            "is written to the stream before we know it's a duplicate."
        )

    def test_drainer_path_has_dedup_branch(self):
        """The disk drainer at `disk_queue.drain_to_redis` must also
        gate on dedup — otherwise a Redis-outage recovery replay
        creates duplicate stream entries for any click that ALSO
        succeeded on a sibling node during the outage window."""
        import inspect
        from app import disk_queue

        src = inspect.getsource(disk_queue.drain_to_redis)
        assert "click:seen:" in src or "click_dedup_ttl_seconds" in src, (
            "drain_to_redis MUST gate the XADD replay on dedup so "
            "Redis-recovery doesn't create stream duplicates "
            "(H1 fix companion)."
        )
