"""G-LOW-1 (SEC-M1) — the returning-user WRITE-fail swallow paths now emit a
throttled Sentry capture instead of being silent. Fail-open is preserved: the
capture never raises and the click is already routed/XADD'd. Read failures stay
silent by design (a missed pin → normal selection).

Covers: identity.persist_identity, sticky.set_sticky_nx, sticky.repin.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import fakeredis.aioredis
import pytest

from app import identity, sticky
from app.telemetry import OP_IDENTITY_PERSIST, OP_STICKY_WRITE

pytestmark = pytest.mark.asyncio


def _failing_redis():
    """A redis whose pipeline().execute() AND .set() raise — to drive the
    swallow-except path of every write helper."""
    r = MagicMock()
    pipe = MagicMock()
    # queue ops are sync no-ops; the awaited execute raises.
    pipe.set = MagicMock()
    pipe.sadd = MagicMock()
    pipe.hsetnx = MagicMock()
    pipe.expire = MagicMock()
    pipe.execute = AsyncMock(side_effect=RuntimeError("redis down"))
    r.pipeline = MagicMock(return_value=pipe)
    r.set = AsyncMock(side_effect=RuntimeError("redis down"))
    r.get = AsyncMock(side_effect=RuntimeError("redis down"))
    r.expire = AsyncMock(side_effect=RuntimeError("redis down"))
    r.scard = AsyncMock(side_effect=RuntimeError("redis down"))
    return r


class TestPersistIdentityWriteFail:
    async def test_capture_on_write_fail_and_fail_open(self):
        with patch.object(identity, "capture_op_msg_throttled") as cap:
            # must NOT raise (fire-and-forget, fail-open)
            await identity.persist_identity(
                _failing_redis(), company_id=7, uid="a" * 32,
                funnel_user_id=None, visitor_id="V", campaign_id="10",
                source_trusted=False, ttl=1000,
            )
        assert cap.called
        assert cap.call_args.args[0] == OP_IDENTITY_PERSIST
        # dedup key is the company → throttle is per-tenant
        assert cap.call_args.args[1] == 7

    async def test_no_capture_on_happy_path(self):
        r = fakeredis.aioredis.FakeRedis(decode_responses=True)
        with patch.object(identity, "capture_op_msg_throttled") as cap:
            await identity.persist_identity(
                r, company_id=7, uid="a" * 32, funnel_user_id=None,
                visitor_id="V", campaign_id="10", source_trusted=False, ttl=1000,
            )
        assert not cap.called  # byte-identical: no error → no Sentry


class TestStickyWriteFail:
    async def test_set_nx_capture_on_write_fail_and_fail_open(self):
        with patch.object(sticky, "get_identity_redis",
                          AsyncMock(return_value=_failing_redis())), \
             patch.object(sticky, "capture_op_msg_throttled") as cap:
            await sticky.set_sticky_nx(7, "a" * 32, "10", "99", 1000)  # no raise
        assert cap.called
        assert cap.call_args.args[0] == OP_STICKY_WRITE
        assert cap.call_args.args[1] == 7

    async def test_repin_capture_on_write_fail_and_fail_open(self):
        with patch.object(sticky, "get_identity_redis",
                          AsyncMock(return_value=_failing_redis())), \
             patch.object(sticky, "capture_op_msg_throttled") as cap:
            await sticky.repin(7, "a" * 32, "10", "99", 1000)  # no raise
        assert cap.called
        assert cap.call_args.args[0] == OP_STICKY_WRITE

    async def test_no_capture_on_happy_path(self):
        r = fakeredis.aioredis.FakeRedis(decode_responses=True)
        with patch.object(sticky, "get_identity_redis", AsyncMock(return_value=r)), \
             patch.object(sticky, "capture_op_msg_throttled") as cap:
            await sticky.set_sticky_nx(7, "a" * 32, "10", "99", 1000)
            await sticky.repin(7, "a" * 32, "10", "98", 1000)
        assert not cap.called  # byte-identical: no error → no Sentry

    async def test_read_fail_stays_silent(self):
        # get_sticky is a READ — a failure degrades to "no pin" with NO Sentry
        # (by design; the click still routes via normal selection).
        with patch.object(sticky, "get_identity_redis",
                          AsyncMock(return_value=_failing_redis())), \
             patch.object(sticky, "capture_op_msg_throttled") as cap:
            res = await sticky.get_sticky(7, "a" * 32, "10", 1000)
        assert res is None and not cap.called
