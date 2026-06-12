"""AUD-B F1 (2026-06-12) — shipper processed-history trim is MINID-based.

Pre-fix the shipper ran ``XTRIM stream:clicks MAXLEN ~10000`` after EVERY
successful batch ship (``_ack_and_trim_shipped_batch``), capping the
SURVIVABLE outage backlog at 10k: during a central-collector outage clicks
pile up in the stream (the XADD-side cap is 1M), and the FIRST successful
batch after recovery trimmed everything older than the newest 10k —
silently destroying the un-shipped backlog, including PEL-referenced
entries (XAUTOCLAIM then drops them via the deleted-ids array). At
10 clicks/s that was a 17-minute outage budget.

The fix mirrors the proven process-service pattern
(``app/events/consumer.py`` ``trim_processed``): trim with MINID to the
oldest PENDING id (PEL non-empty) or the group's last-delivered-id (PEL
empty) — entries that are undelivered or pending are NEVER cut. Pins:

  1. outage backlog (undelivered entries, even beyond the old 10k
     window) survives the trim;
  2. processed (delivered + ACKed) history IS trimmed;
  3. PEL entries survive and stay reclaimable (C3 crash re-drive);
  4. post-recovery partial ship trims only the acked prefix;
  5. trim is best-effort (never raises) and skips when nothing is
     provably processed;
  6. wiring: run_shipper owns the trim clock; _ack_shipped_batch no
     longer trims at all.
"""

from __future__ import annotations

import inspect

import fakeredis.aioredis
import pytest

from app import shipper
from app.shipper import (
    GROUP_NAME,
    STREAM_KEY,
    _ack_shipped_batch,
    _trim_processed_history,
)


@pytest.fixture()
def fake_redis():
    return fakeredis.aioredis.FakeRedis(decode_responses=True)


async def _seed(r, n: int, *, group: bool = True) -> list[str]:
    if group:
        await r.xgroup_create(STREAM_KEY, GROUP_NAME, id="0", mkstream=True)
    return [await r.xadd(STREAM_KEY, {"data": "{}"}) for _ in range(n)]


async def _read(r, count: int) -> list[str]:
    resp = await r.xreadgroup(GROUP_NAME, "t", {STREAM_KEY: ">"}, count=count)
    return [mid for mid, _ in resp[0][1]] if resp else []


@pytest.mark.asyncio
async def test_outage_backlog_beyond_old_10k_window_survives():
    """THE F1 regression pin: an un-shipped backlog larger than the old
    hardcoded 10k MAXLEN window is untouched by the trim — nothing was
    delivered, so nothing is provably processed."""
    r = fakeredis.aioredis.FakeRedis(decode_responses=True)
    await _seed(r, 10_500)

    removed = await _trim_processed_history(r)

    assert removed == 0
    assert await r.xlen(STREAM_KEY) == 10_500


@pytest.mark.asyncio
async def test_recovery_ship_trims_only_acked_prefix(fake_redis):
    """Outage-recovery shape: backlog accumulated, shipper recovers and
    ships+ACKs the first batch. Old behavior: trim-to-newest-10k destroyed
    the rest of the backlog. New behavior: only the acked prefix below
    the last-delivered-id goes; every undelivered entry survives."""
    ids = await _seed(fake_redis, 10_200)
    batch = await _read(fake_redis, 500)  # shipper drains first batch
    await fake_redis.xack(STREAM_KEY, GROUP_NAME, *batch)

    removed = await _trim_processed_history(fake_redis)

    # XTRIM MINID removes ids < minid — boundary entry (500th) is kept.
    assert removed == 499
    remaining = [m for m, _ in await fake_redis.xrange(STREAM_KEY)]
    assert remaining == ids[499:]  # 9,701 undelivered entries intact


@pytest.mark.asyncio
async def test_processed_history_trimmed(fake_redis):
    """Steady-state hygiene: fully delivered + ACKed history is removed
    (all but the boundary entry) so XLEN stops growing toward MAXLEN."""
    ids = await _seed(fake_redis, 5)
    for mid in await _read(fake_redis, 5):
        await fake_redis.xack(STREAM_KEY, GROUP_NAME, mid)

    removed = await _trim_processed_history(fake_redis)

    assert removed == 4
    remaining = [m for m, _ in await fake_redis.xrange(STREAM_KEY)]
    assert remaining == [ids[-1]]


@pytest.mark.asyncio
async def test_pel_entries_survive_and_stay_reclaimable(fake_redis):
    """Safe point = oldest PENDING id: delivered-but-unACKed entries (a
    crashed shipper's PEL) survive the trim and remain XAUTOCLAIM-able —
    the C3 orphaned-PEL reclaim depends on the entry DATA still existing."""
    ids = await _seed(fake_redis, 5)
    mids = await _read(fake_redis, 3)                       # deliver e1..e3
    await fake_redis.xack(STREAM_KEY, GROUP_NAME, mids[0])  # ACK only e1

    removed = await _trim_processed_history(fake_redis)

    assert removed == 1  # only e1 (< oldest pending)
    remaining = [m for m, _ in await fake_redis.xrange(STREAM_KEY)]
    assert remaining == ids[1:]
    info = await fake_redis.xpending(STREAM_KEY, GROUP_NAME)
    assert info["pending"] == 2
    _cur, claimed, _del = await fake_redis.xautoclaim(
        STREAM_KEY, GROUP_NAME, "rescuer", min_idle_time=0, start_id="0-0"
    )
    assert [m for m, _ in claimed] == mids[1:]


@pytest.mark.asyncio
async def test_nothing_delivered_yet_skips(fake_redis):
    """last-delivered-id == 0-0 → nothing provably processed → no trim."""
    await _seed(fake_redis, 3)
    assert await _trim_processed_history(fake_redis) == 0
    assert await fake_redis.xlen(STREAM_KEY) == 3


@pytest.mark.asyncio
async def test_missing_group_never_raises(fake_redis):
    """Best-effort: no group/stream → 0, no exception out of the helper."""
    assert await _trim_processed_history(fake_redis) == 0


@pytest.mark.asyncio
async def test_trim_failure_never_raises(fake_redis, monkeypatch):
    """A Redis blip in XTRIM must never break the ship loop."""
    await _seed(fake_redis, 2)
    for mid in await _read(fake_redis, 2):
        await fake_redis.xack(STREAM_KEY, GROUP_NAME, mid)

    async def boom(*a, **kw):
        raise ConnectionError("redis blip")

    monkeypatch.setattr(fake_redis, "xtrim", boom)
    assert await _trim_processed_history(fake_redis) == 0


@pytest.mark.asyncio
async def test_ack_helper_no_longer_trims():
    """_ack_shipped_batch only ACKs — the destructive per-batch MAXLEN
    trim is gone from the ship path (and from the reclaim path, which
    ships through the same helper)."""
    from unittest.mock import AsyncMock

    redis = AsyncMock()
    assert await _ack_shipped_batch(
        redis, {"1-0"}, batch_size=1, collector_status=202,
    ) is True
    redis.xack.assert_awaited_once()
    redis.xtrim.assert_not_awaited()


def test_run_shipper_owns_the_trim_clock():
    """Source pin (idiom: test_shipper_exception_tagging): the loop fires
    _trim_processed_history on the shipper_trim_interval_sec clock, and
    no MAXLEN-based XTRIM survives anywhere in the module."""
    loop_src = inspect.getsource(shipper.run_shipper)
    assert "_trim_processed_history" in loop_src
    assert "shipper_trim_interval_sec" in loop_src

    module_src = inspect.getsource(shipper)
    assert "maxlen=10000" not in module_src
    # The only xtrim on stream:clicks is the MINID one inside the helper.
    helper_src = inspect.getsource(shipper._trim_processed_history)
    assert "minid=minid" in helper_src
