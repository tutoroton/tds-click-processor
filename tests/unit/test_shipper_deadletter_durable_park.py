"""LOSSFIX-4 (2026-08-15) — a deadlettered click must survive its own ACK.

The shipper used to XADD the click into `stream:clicks-deadletter`
(`maxlen=10_000, approximate=True` — an evicting ring) and then ACK it off
`stream:clicks` **regardless of outcome**. The ring was therefore the click's
only copy, and it was NOT "already lost" on arrival, as the recorded
exemption claimed (F-DL-1/GTD-R196) — it became lost when the ring rotated.

Same mechanism as the 19 clicks proven lost in the collector (ANCHOR §182),
one hop upstream, firing on TRANSIENT faults: `counter_error:` is the edge's
own Redis stuttering on an INCR, not a bad click.

The decisive test here is `test_a_failed_park_forbids_the_ack`: it is the
only one that distinguishes "we wrote a copy somewhere" from "a copy
survives the ACK", and it is the assertion the old code fails.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import pytest

from app import disk_queue, shipper
from app.shipper import DEADLETTER_STREAM_KEY, _deadletter_click


@pytest.fixture(autouse=True)
def _park_root(tmp_path, monkeypatch):
    monkeypatch.setattr(disk_queue.settings, "disk_queue_root", str(tmp_path))
    return tmp_path


def _park_lines(root) -> list[dict]:
    park_dir = root / disk_queue._PARK_DIRNAME
    if not park_dir.exists():
        return []
    out = []
    for f in park_dir.glob("*.ndjson"):
        out += [json.loads(line) for line in f.read_text().splitlines() if line]
    return out


class TestTheParkIsDurableAndFirst:
    @pytest.mark.asyncio
    async def test_a_deadlettered_click_lands_on_disk(self, _park_root):
        r = AsyncMock()
        ok = await _deadletter_click(
            r, {"click_id": "c1", "geo": "UA"}, attempt=3, reason="queue_failure")

        assert ok is True
        parked = _park_lines(_park_root)
        assert len(parked) == 1
        assert parked[0]["click_id"] == "c1"
        # The FULL click survives, not a summary — a park you cannot replay
        # faithfully is not a park.
        assert json.loads(parked[0]["data"])["geo"] == "UA"

    @pytest.mark.asyncio
    async def test_the_disk_park_happens_before_the_evicting_ring(self, _park_root):
        """Order is the whole contract: the ring may fail, be full, or evict
        — none of that may happen before a durable copy exists."""
        order: list[str] = []
        r = AsyncMock()

        async def _xadd(key, *a, **kw):
            order.append(f"ring:{key}")
            return "1-0"

        r.xadd = _xadd
        real_park = disk_queue.enqueue_parked_click

        async def _park(record):
            order.append("disk_park")
            return await real_park(record)

        with patch.object(shipper, "enqueue_parked_click", new=_park):
            await _deadletter_click(r, {"click_id": "c1"}, attempt=3, reason="x")

        assert order[0] == "disk_park", order
        assert order[1] == f"ring:{DEADLETTER_STREAM_KEY}"

    @pytest.mark.asyncio
    async def test_a_failed_park_means_not_safe_to_ack(self):
        r = AsyncMock()
        with patch.object(shipper, "enqueue_parked_click",
                          new=AsyncMock(return_value=False)):
            ok = await _deadletter_click(
                r, {"click_id": "c1"}, attempt=3, reason="x")
        assert ok is False
        r.xadd.assert_not_awaited()  # no ring write either — nothing is retired


class TestTheAckIsGatedOnTheCopy:
    """🔴 The decisive pair. Everything above proves a copy was WRITTEN;
    only these prove a copy SURVIVES THE ACK, which is the property the old
    code lacked and the exemption's premise assumed."""

    @staticmethod
    async def _run(park_ok: bool) -> set[str]:
        ack_msg_ids: set[str] = set()
        clicks = [{"click_id": "c1"}]
        with patch.object(shipper, "_handle_rejected_click",
                          new=AsyncMock(return_value=(False, park_ok))):
            await shipper._handle_rejected_in_batch(
                AsyncMock(), AsyncMock(),
                rejected_items=[{"click_id": "c1", "reason": "queue_failure"}],
                clicks=clicks,
                click_id_to_msg_id={"c1": "5-0"},
                ack_msg_ids=ack_msg_ids,
            )
        return ack_msg_ids

    @pytest.mark.asyncio
    async def test_a_failed_park_forbids_the_ack(self):
        """Against the pre-fix code this returns {"5-0"}: the click is ACKed
        off `stream:clicks` with nothing but an evicting ring holding it."""
        assert await self._run(park_ok=False) == set(), (
            "the shipper ACKed a click off the durable stream while its only "
            "copy was a 10k ring that evicts — this is the §182 mechanism"
        )

    @pytest.mark.asyncio
    async def test_the_control_a_successful_park_does_allow_the_ack(self):
        """Without this the test above could pass by never ACKing anything,
        which would wedge the shipper instead of fixing it."""
        assert await self._run(park_ok=True) == {"5-0"}


class TestTransientFaultsAreTheCommonCase:
    """The exemption read as reasonable partly because "deadletter" sounds
    like "bad data". Three of the four call sites are infrastructure hiccups
    on OUR side, holding a perfectly good click."""

    @pytest.mark.asyncio
    async def test_an_edge_redis_hiccup_still_preserves_the_click(self, _park_root):
        r = AsyncMock()
        pipe = AsyncMock()
        pipe.execute = AsyncMock(side_effect=RuntimeError("edge redis down"))
        r.pipeline = lambda: pipe

        retried, safe_to_ack = await shipper._handle_rejected_click(
            r, {"click_id": "good-click", "geo": "PL"}, "queue_failure")

        assert (retried, safe_to_ack) == (False, True)
        parked = _park_lines(_park_root)
        assert [p["click_id"] for p in parked] == ["good-click"]
        assert parked[0]["last_rejection_reason"].startswith("counter_error:")
