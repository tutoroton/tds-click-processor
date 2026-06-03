"""C3 (audit 2026-06-03) — edge-shipper orphaned-PEL reclaim.

Pins `_reclaim_shipper_pending` (mirror of the central writer's
`_reclaim_pending`). The shipper CONSUMER_NAME embeds os.getpid(), so a
crash/restart orphans the dead consumer's PEL entries (XREADGROUP-read
but never XACKed). The main loop reads only `>` (new), so without
XAUTOCLAIM those clicks are lost forever. These tests pin:

  1. orphaned PEL entries are reclaimed (XAUTOCLAIM) and re-shipped
     through the same post→verdict→ack path → accepted clicks ACKed;
  2. the idle threshold (`shipper_reclaim_min_idle_ms`) is honored so
     reclaim never races the live consumer;
  3. a re-ship FAILURE leaves the claimed entries pending (not ACKed,
     not lost) for the next reclaim tick;
  4. a poison (undecodable) entry is ACKed in place, not wedged;
  5. reclaim NEVER raises (a fault must not break the main ship loop),
     and self-heals NOGROUP.

All mutation-checked — see P3-results.md.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from app import shipper
from app.config import settings
from app.shipper import _reclaim_shipper_pending


class FakeRedis:
    """Minimal async Redis double for the reclaim path: xgroup_create +
    xautoclaim (returns queued batches) + xack/xadd/xtrim spies."""

    def __init__(self, autoclaim_batches):
        # Each element: (next_cursor, [(msg_id, {"data": json_str}), ...]).
        # The loop stops when next_cursor == "0-0".
        self._batches = list(autoclaim_batches)
        self.xgroup_create = AsyncMock()
        self.xack = AsyncMock()
        self.xadd = AsyncMock()
        self.xtrim = AsyncMock()
        self.xautoclaim_calls: list[dict] = []

    async def xautoclaim(self, stream, group, consumer, *, min_idle_time,
                         start_id, count):
        self.xautoclaim_calls.append(
            {"min_idle_time": min_idle_time, "start_id": start_id}
        )
        if self._batches:
            cursor, messages = self._batches.pop(0)
            return [cursor, messages, []]
        return ["0-0", [], []]


def _msg(msg_id: str, click_id: str):
    return (msg_id, {"data": json.dumps({"click_id": click_id})})


def _resp(status: int, text: str) -> httpx.Response:
    return httpx.Response(status_code=status, text=text)


@pytest.mark.asyncio
async def test_orphaned_pel_reclaimed_and_shipped():
    redis = FakeRedis([("0-0", [_msg("1-0", "c1"), _msg("2-0", "c2")])])
    client = AsyncMock()
    accepted = _resp(200, '{"accepted":["c1","c2"],"rejected":[],"duplicates":[]}')

    with patch.object(shipper, "_post_batch_to_central",
                      new=AsyncMock(return_value=accepted)), \
         patch.object(shipper, "_process_new_shape_batch",
                      new=AsyncMock()) as proc:
        counts = await _reclaim_shipper_pending(redis, client)

    assert counts == {"claimed": 2, "shipped_batches": 1}
    # The reclaimed batch was re-driven through the per-click verdict path
    # with the orphaned msg_ids (so accepted clicks get ACKed there).
    proc.assert_awaited_once()
    _, _, _, _, clicks_arg, msg_ids_arg = proc.await_args.args
    assert [c["click_id"] for c in clicks_arg] == ["c1", "c2"]
    assert msg_ids_arg == ["1-0", "2-0"]


@pytest.mark.asyncio
async def test_idle_threshold_is_respected():
    redis = FakeRedis([("0-0", [_msg("1-0", "c1")])])
    client = AsyncMock()
    accepted = _resp(200, '{"accepted":["c1"],"rejected":[],"duplicates":[]}')

    with patch.object(shipper, "_post_batch_to_central",
                      new=AsyncMock(return_value=accepted)), \
         patch.object(shipper, "_process_new_shape_batch", new=AsyncMock()):
        await _reclaim_shipper_pending(redis, client)

    assert redis.xautoclaim_calls, "XAUTOCLAIM must be issued"
    # Never race the live consumer — only claim entries idle past the knob.
    assert (
        redis.xautoclaim_calls[0]["min_idle_time"]
        == settings.shipper_reclaim_min_idle_ms
    )


@pytest.mark.asyncio
async def test_reship_failure_leaves_entries_pending():
    redis = FakeRedis([("0-0", [_msg("1-0", "c1"), _msg("2-0", "c2")])])
    client = AsyncMock()
    server_error = _resp(500, "upstream boom")

    with patch.object(shipper, "_post_batch_to_central",
                      new=AsyncMock(return_value=server_error)), \
         patch.object(shipper, "_process_new_shape_batch",
                      new=AsyncMock()) as proc:
        counts = await _reclaim_shipper_pending(redis, client)

    # Claimed but NOT shipped — the entries stay in OUR PEL (no ACK path
    # ran) and are retried on the next reclaim tick. Nothing lost.
    assert counts == {"claimed": 2, "shipped_batches": 0}
    proc.assert_not_awaited()
    redis.xack.assert_not_called()


@pytest.mark.asyncio
async def test_central_unreachable_is_non_fatal_and_leaves_pending():
    redis = FakeRedis([("0-0", [_msg("1-0", "c1")])])
    client = AsyncMock()

    with patch.object(shipper, "_post_batch_to_central",
                      new=AsyncMock(side_effect=httpx.ConnectError("down"))), \
         patch.object(shipper, "_process_new_shape_batch",
                      new=AsyncMock()) as proc:
        counts = await _reclaim_shipper_pending(redis, client)

    assert counts == {"claimed": 1, "shipped_batches": 0}
    proc.assert_not_awaited()
    redis.xack.assert_not_called()


@pytest.mark.asyncio
async def test_poison_entry_acked_in_place_not_wedged():
    poison = ("9-0", {"data": "{garbled<not-json"})
    redis = FakeRedis([("0-0", [poison, _msg("2-0", "c2")])])
    client = AsyncMock()
    accepted = _resp(200, '{"accepted":["c2"],"rejected":[],"duplicates":[]}')

    with patch.object(shipper, "_post_batch_to_central",
                      new=AsyncMock(return_value=accepted)), \
         patch.object(shipper, "_process_new_shape_batch", new=AsyncMock()):
        counts = await _reclaim_shipper_pending(redis, client)

    # Only the good click counts as claimed; the poison was ACKed in place
    # (so it leaves the PEL and never wedges the reclaim cursor).
    assert counts["claimed"] == 1
    redis.xack.assert_awaited()  # poison ACK


@pytest.mark.asyncio
async def test_reclaim_never_raises_on_redis_fault():
    redis = FakeRedis([])
    # XAUTOCLAIM itself blows up with a non-NOGROUP error.
    redis.xautoclaim = AsyncMock(side_effect=RuntimeError("redis exploded"))
    client = AsyncMock()

    # Must not raise — a reclaim fault cannot be allowed to break the loop.
    counts = await _reclaim_shipper_pending(redis, client)
    assert counts == {"claimed": 0, "shipped_batches": 0}


@pytest.mark.asyncio
async def test_reclaim_self_heals_nogroup():
    from redis.exceptions import ResponseError

    redis = FakeRedis([])
    redis.xautoclaim = AsyncMock(
        side_effect=ResponseError("NOGROUP No such key or consumer group")
    )
    client = AsyncMock()

    counts = await _reclaim_shipper_pending(redis, client)
    assert counts == {"claimed": 0, "shipped_batches": 0}
    # ensure_local_consumer_group called at entry AND on NOGROUP recovery.
    assert redis.xgroup_create.await_count >= 2
