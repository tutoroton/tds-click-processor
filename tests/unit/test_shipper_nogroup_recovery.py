"""Regression tests for shipper NOGROUP auto-heal (audit 2026-05-27).

## Background

The collector-side writer ``services/collector/app/writer.py`` exhibited a
catastrophic NOGROUP infinite-loop pathology in staging (Sentry
GEO-TDS-ADMIN-API-1X / 1W — 1500 events / 2h). The shipper-side
:func:`_drain_batch_from_stream` in this module had the SAME latent bug
on its local stream ``stream:clicks``: if the local consumer group
``shippers`` disappeared mid-flight (operator XGROUP DESTROY, Redis
restart with corrupt AOF, FLUSHDB), every XREADGROUP would raise
``ResponseError: NOGROUP …``, propagate to
:func:`_handle_shipper_loop_error`, sleep 2s, retry — and immediately
NOGROUP again, ad infinitum.

This patch fixes the latent bug preemptively (mirror of the writer fix).

## Post-fix invariants (this test suite pins)

1. **NOGROUP from xreadgroup is caught INSIDE _drain_batch_from_stream**
   and the consumer group is recreated via the new
   :func:`_ensure_local_consumer_group` helper.

2. **xreadgroup is retried exactly ONCE after recovery**; a second
   consecutive NOGROUP (or any other Redis error during retry) is
   propagated to the outer ``_handle_shipper_loop_error`` for the
   normal 2-second back-off — prevents tight-loop on pathological
   recovery states.

3. **Non-NOGROUP ResponseErrors propagate unchanged** — the new
   ``except RedisResponseError`` branch must NOT catch unrelated
   protocol failures (WRONGTYPE, NOAUTH, etc.); those go to the outer
   catch-all where they were already handled.

4. **:func:`_ensure_local_consumer_group` is idempotent** — used in
   both the startup path (via :func:`run_shipper`) and the in-loop
   recovery path. BUSYGROUP is silently absorbed; other errors raise.

5. **:func:`_is_nogroup_error` classifies correctly** — narrow to
   ``RedisResponseError`` to avoid false positives on
   ConnectionError / TimeoutError / arbitrary Exception types that
   should NOT trigger an xgroup_create round-trip.

6. **Regression: existing happy path** — when NOGROUP does NOT occur,
   xreadgroup is called exactly once, behaviour identical to pre-fix.

7. **Sentry signal at level=error** for the recovery event (alerting
   threshold).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from redis.exceptions import ResponseError as RedisResponseError

from app import shipper as shipper_module
from app.shipper import (
    BATCH_SIZE,
    BATCH_TIMEOUT_MS,
    GROUP_NAME,
    STREAM_KEY,
    _drain_batch_from_stream,
    _ensure_local_consumer_group,
    _is_nogroup_error,
)


# ============================================================================
#                       _is_nogroup_error classification
# ============================================================================


class TestIsNogroupError:
    """Mirror of the writer's classification helper — pins the same
    narrow semantics."""

    def test_classifies_response_error_with_nogroup(self):
        exc = RedisResponseError(
            "NOGROUP No such key 'stream:clicks' or consumer "
            "group 'shippers' in XREADGROUP with GROUP option"
        )
        assert _is_nogroup_error(exc) is True

    def test_does_not_classify_busygroup(self):
        exc = RedisResponseError("BUSYGROUP Consumer Group name already exists")
        assert _is_nogroup_error(exc) is False

    def test_does_not_classify_wrongtype(self):
        exc = RedisResponseError("WRONGTYPE Operation against a key holding the wrong kind of value")
        assert _is_nogroup_error(exc) is False

    def test_does_not_classify_connection_error(self):
        assert _is_nogroup_error(ConnectionError("conn lost")) is False
        assert _is_nogroup_error(TimeoutError("timeout")) is False

    def test_does_not_classify_arbitrary_exception(self):
        assert _is_nogroup_error(RuntimeError("rt")) is False
        assert _is_nogroup_error(ValueError("v")) is False


# ============================================================================
#                       _ensure_local_consumer_group helper
# ============================================================================


class TestEnsureLocalConsumerGroup:
    """Pin the helper's idempotent contract — used at startup AND
    recovery."""

    @pytest.mark.asyncio
    async def test_creates_group_with_mkstream(self):
        r = MagicMock()
        r.xgroup_create = AsyncMock()
        await _ensure_local_consumer_group(r)
        r.xgroup_create.assert_awaited_once_with(
            STREAM_KEY, GROUP_NAME, id="0", mkstream=True,
        )

    @pytest.mark.asyncio
    async def test_silently_absorbs_busygroup(self):
        r = MagicMock()
        r.xgroup_create = AsyncMock(
            side_effect=Exception("BUSYGROUP Consumer Group name already exists")
        )
        # Must not raise — BUSYGROUP means the group already exists,
        # which is the desired post-condition.
        await _ensure_local_consumer_group(r)

    @pytest.mark.asyncio
    async def test_propagates_other_errors(self):
        """NOAUTH, permission-denied, etc. must surface to the caller —
        silent absorption would mask configuration drift."""
        r = MagicMock()
        r.xgroup_create = AsyncMock(
            side_effect=Exception("NOAUTH Authentication required")
        )
        with pytest.raises(Exception, match="NOAUTH"):
            await _ensure_local_consumer_group(r)


# ============================================================================
#                  _drain_batch_from_stream NOGROUP recovery
# ============================================================================


def _make_nogroup_error() -> RedisResponseError:
    return RedisResponseError(
        "NOGROUP No such key 'stream:clicks' or consumer "
        "group 'shippers' in XREADGROUP with GROUP option"
    )


class TestDrainNogroupRecovery:
    """Integration-style tests for the in-loop NOGROUP recovery in
    :func:`_drain_batch_from_stream`."""

    @pytest.mark.asyncio
    async def test_nogroup_triggers_recovery_and_retry(self):
        """The canonical recovery path: NOGROUP → recover → retry
        xreadgroup → succeed."""
        r = MagicMock()
        calls = {"n": 0}

        async def _xreadgroup(*args, **kwargs):
            calls["n"] += 1
            if calls["n"] == 1:
                raise _make_nogroup_error()
            # Second call (after recovery) — return one click.
            return [
                (
                    STREAM_KEY,
                    [
                        ("1-0", {"data": '{"click_id": "abc"}'}),
                    ],
                )
            ]

        r.xreadgroup = AsyncMock(side_effect=_xreadgroup)
        r.xgroup_create = AsyncMock()

        with patch.object(shipper_module, "_capture_op_msg") as mock_msg:
            clicks, msg_ids = await _drain_batch_from_stream(r)

        # Two xreadgroup calls — initial NOGROUP + post-recovery retry.
        assert calls["n"] == 2
        # ensure_local_consumer_group recreated the group.
        r.xgroup_create.assert_awaited_once_with(
            STREAM_KEY, GROUP_NAME, id="0", mkstream=True,
        )
        # Sentry signal emitted at level=error.
        mock_msg.assert_called_once()
        _args, kwargs = mock_msg.call_args
        assert kwargs.get("level") == "error"
        # Click was returned post-recovery.
        assert clicks == [{"click_id": "abc"}]
        assert msg_ids == ["1-0"]

    @pytest.mark.asyncio
    async def test_double_nogroup_propagates_to_outer_handler(self):
        """If recovery succeeds but the immediately-following xreadgroup
        ALSO NOGROUPs (pathological state: e.g. cluster slot migration
        mid-recovery, xgroup_create silently failing), propagate so the
        outer :func:`_handle_shipper_loop_error` applies its 2s
        back-off rather than tight-spinning in this helper."""
        r = MagicMock()

        async def _xreadgroup(*args, **kwargs):
            raise _make_nogroup_error()

        r.xreadgroup = AsyncMock(side_effect=_xreadgroup)
        r.xgroup_create = AsyncMock()

        with patch.object(shipper_module, "_capture_op_msg"):
            with pytest.raises(RedisResponseError, match="NOGROUP"):
                await _drain_batch_from_stream(r)

        # Recovery was attempted exactly once, NOT in a tight loop.
        r.xgroup_create.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_non_nogroup_response_error_propagates_unchanged(self):
        """WRONGTYPE / NOAUTH / arbitrary ResponseError must NOT take
        the recovery path — they propagate to the outer catch-all
        immediately."""
        r = MagicMock()
        wrongtype = RedisResponseError("WRONGTYPE")
        r.xreadgroup = AsyncMock(side_effect=wrongtype)
        r.xgroup_create = AsyncMock()

        with pytest.raises(RedisResponseError, match="WRONGTYPE"):
            await _drain_batch_from_stream(r)

        # No recovery attempt.
        r.xgroup_create.assert_not_called()

    @pytest.mark.asyncio
    async def test_non_redis_exception_propagates_unchanged(self):
        """A non-Redis exception (e.g. RuntimeError from a bug) must
        propagate without triggering recovery. The outer catch-all in
        ``_handle_shipper_loop_error`` will record it under
        OP_LOOP_ITERATION."""
        r = MagicMock()
        r.xreadgroup = AsyncMock(side_effect=RuntimeError("boom"))
        r.xgroup_create = AsyncMock()

        with pytest.raises(RuntimeError, match="boom"):
            await _drain_batch_from_stream(r)

        r.xgroup_create.assert_not_called()

    @pytest.mark.asyncio
    async def test_recovery_failure_propagates(self):
        """If the recovery call itself raises (e.g. NOAUTH on
        xgroup_create), propagate so the outer catch-all reports it.
        Better to surface a permissions/config problem to operators
        than to tight-loop attempting recovery."""
        r = MagicMock()
        r.xreadgroup = AsyncMock(side_effect=_make_nogroup_error())
        r.xgroup_create = AsyncMock(
            side_effect=Exception("NOAUTH Authentication required")
        )

        with patch.object(shipper_module, "_capture_op_msg"):
            with pytest.raises(Exception, match="NOAUTH"):
                await _drain_batch_from_stream(r)


# ============================================================================
#                       Happy path regression
# ============================================================================


class TestDrainHappyPathRegression:
    """Pin that the auto-heal addition did NOT change the existing
    happy-path behaviour — single xreadgroup call, no recovery
    overhead."""

    @pytest.mark.asyncio
    async def test_happy_path_single_xreadgroup_call(self):
        r = MagicMock()
        r.xreadgroup = AsyncMock(
            return_value=[
                (
                    STREAM_KEY,
                    [
                        ("1-0", {"data": '{"click_id": "a"}'}),
                        ("2-0", {"data": '{"click_id": "b"}'}),
                    ],
                )
            ]
        )
        r.xgroup_create = AsyncMock()
        r.xack = AsyncMock()

        clicks, msg_ids = await _drain_batch_from_stream(r)

        # Exactly one xreadgroup call — recovery overhead absent in
        # the happy path.
        r.xreadgroup.assert_awaited_once_with(
            GROUP_NAME, shipper_module.CONSUMER_NAME,
            {STREAM_KEY: ">"},
            count=BATCH_SIZE, block=BATCH_TIMEOUT_MS,
        )
        # No recovery group-recreation.
        r.xgroup_create.assert_not_called()
        # Both clicks returned.
        assert clicks == [{"click_id": "a"}, {"click_id": "b"}]
        assert msg_ids == ["1-0", "2-0"]

    @pytest.mark.asyncio
    async def test_happy_path_empty_results(self):
        """Empty xreadgroup result (BATCH_TIMEOUT_MS elapsed with no
        new messages) — must return ``([], [])`` and NOT trigger
        any recovery attempt."""
        r = MagicMock()
        r.xreadgroup = AsyncMock(return_value=[])
        r.xgroup_create = AsyncMock()

        clicks, msg_ids = await _drain_batch_from_stream(r)

        assert clicks == []
        assert msg_ids == []
        r.xgroup_create.assert_not_called()
