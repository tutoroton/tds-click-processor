"""LOSSFIX P1b (2026-07-07) — L1: the silent-302 leak.

Pre-fix, `/decide`'s disk-fallback branch called `enqueue_click_to_disk`
and fell through to an unconditional 302 REGARDLESS of whether the
enqueue succeeded — a click that failed BOTH the stream XADD and the
disk fallback was "genuinely lost" (per the old comment) with no signal
to the Worker. Now: 503, mirroring the existing disk-pressure-503
block, so the Worker's AbortSignal fallback (or sibling-race recovery)
takes over instead of silently telling the user "success" for a click
that landed nowhere durable.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.telemetry import OP_CLICK_UNCAPTURED


@pytest.fixture
def client():
    from app.main import app
    return TestClient(app)


@pytest.fixture
def patched_auth():
    with patch("app.main._check_tds_key", new_callable=AsyncMock):
        yield


def _payload(click_id: str = "019e5be83c8179896a0859dd") -> dict:
    return {
        "click_id": click_id,
        "ip": "1.2.3.4",
        "country": "DE",
        "user_agent": "geo-tds-test/1.0",
    }


_MATCHED = {
    "url": "https://offer.example.com/track?cid=1",
    "campaign_id": "camp-9",
    "offer_id": "offer-9",
    "binding_id": 0,
    "binding_alias": None,
    "timing": {"result": "flow_cascade"},
}


def _fake_redis(xadd_side_effect=None) -> MagicMock:
    r = MagicMock()
    r.set = AsyncMock(return_value=True)  # node-local dedup: first-seen
    if xadd_side_effect is not None:
        r.xadd = AsyncMock(side_effect=xadd_side_effect)
    else:
        r.xadd = AsyncMock(return_value="1-0")
    return r


def _capturing_op_msg(sink: list):
    def _capture(op_name, message, level="warning", **extras):
        sink.append((op_name, level, extras))
    return _capture


# ---------------------------------------------------------------------------
# L1: stream XADD fails AND disk fallback ALSO fails -> 503, not 302
# ---------------------------------------------------------------------------


def test_stream_fail_and_disk_fail_returns_503_not_302(client, patched_auth):
    """The headline L1 fix. Pre-fix this fell through to an unconditional
    302 with the click 'genuinely lost'. Now: 503, and the op-tag fires."""
    fake_redis = _fake_redis(xadd_side_effect=RuntimeError("redis down"))
    fake_route = AsyncMock(return_value=_MATCHED)
    fake_enqueue = AsyncMock(return_value=False)  # disk fallback ALSO fails
    captured: list = []

    with patch("app.main.get_redis", new=AsyncMock(return_value=fake_redis)), \
         patch("app.main.route", new=fake_route), \
         patch("app.main.enqueue_click_to_disk", new=fake_enqueue), \
         patch("app.main.check_disk_pressure", return_value=(False, 10**9)), \
         patch("app.main.capture_op_msg", new=_capturing_op_msg(captured)):
        r = client.post("/decide", json=_payload(), headers={"X-TDS-Key": "x"})

    assert r.status_code == 503, (
        f"Expected 503 (click_uncaptured), got {r.status_code}: {r.text}"
    )
    assert r.json()["detail"] == "click_uncaptured"
    assert r.status_code != 302
    assert "302" not in json.dumps(r.json())
    fake_enqueue.assert_awaited_once()
    ops = [c[0] for c in captured]
    assert OP_CLICK_UNCAPTURED in ops, (
        f"Expected the OP_CLICK_UNCAPTURED op-tag to fire; captured={captured}"
    )


def test_stream_fail_but_disk_succeeds_still_302(client, patched_auth):
    """Sanity counterpart — the disk fallback SUCCEEDING must still 302
    (the click IS captured, just via disk instead of the stream)."""
    fake_redis = _fake_redis(xadd_side_effect=RuntimeError("redis down"))
    fake_route = AsyncMock(return_value=_MATCHED)
    fake_enqueue = AsyncMock(return_value=True)

    with patch("app.main.get_redis", new=AsyncMock(return_value=fake_redis)), \
         patch("app.main.route", new=fake_route), \
         patch("app.main.enqueue_click_to_disk", new=fake_enqueue), \
         patch("app.main.check_disk_pressure", return_value=(False, 10**9)):
        r = client.post("/decide", json=_payload(), headers={"X-TDS-Key": "x"})

    assert r.status_code == 200
    assert r.json()["status"] == 302
    fake_enqueue.assert_awaited_once()
