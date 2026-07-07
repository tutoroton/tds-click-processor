"""LOSSFIX P1b (2026-07-07) — edge-side no-silent-loss end-to-end tests.

Closes two loss mechanisms on `services/click-processor` (mirroring the
central collector's LOSSFIX P1a, with the edge's existing disk-fallback
machinery):

  * **L1** — the silent-302 leak. Pre-fix, `/decide`'s disk-fallback
    branch called `enqueue_click_to_disk` and fell through to an
    unconditional 302 REGARDLESS of whether the enqueue succeeded — a
    click that failed BOTH the stream XADD and the disk fallback was
    "genuinely lost" (per the old comment) with no signal to the
    Worker. Now: 503, mirroring the existing disk-pressure-503 block.

  * **M1 (edge)** — MAXLEN → entry-count reject. The real-click XADD no
    longer carries a trimming MAXLEN cap; instead a CACHED stream-length
    gate (`main._check_stream_backpressure`, never a per-click round-
    trip) diverts an over-threshold click to the SAME disk-fallback path
    — reject, not trim. The smoke probe rejects outright (503,
    reject-only, no disk fallback) so a saturated node can't pass
    activation green.

Covers the brief's OBSERVABLE DONE items 1, 2, 5 (T7), 6 (T8) — item 3
(no MAXLEN on the real XADD), 4/T6 (all four cap-carriers), and 7/T9
(the 300_000 default) are pinned in test_stream_clicks_maxlen.py.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.config import settings
from app.telemetry import OP_CLICK_UNCAPTURED, OP_STREAM_ENTRY_LIMIT


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


def _smoke_payload(click_id: str = "smoke-test-deadbeef") -> dict:
    return {
        "click_id": click_id,
        "ip": "127.0.0.1",
        "country": "ZZ",
        "user_agent": "geo-tds-smoke-test/1.0",
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
# 1 — L1: stream XADD fails AND disk fallback ALSO fails -> 503, not 302
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


# ---------------------------------------------------------------------------
# 2 — M1: cached gate trips -> diverted to disk fallback (not trimmed,
#     not silently 302'd); 503 only if disk ALSO fails
# ---------------------------------------------------------------------------


def test_gate_tripped_skips_xadd_diverts_to_disk_still_302_on_success(
    client, patched_auth,
):
    fake_redis = _fake_redis()  # would succeed if XADD were even attempted
    fake_route = AsyncMock(return_value=_MATCHED)
    fake_enqueue = AsyncMock(return_value=True)

    with patch("app.main.get_redis", new=AsyncMock(return_value=fake_redis)), \
         patch("app.main.route", new=fake_route), \
         patch("app.main.enqueue_click_to_disk", new=fake_enqueue), \
         patch(
             "app.main.get_cached_stream_clicks_length",
             return_value=settings.stream_clicks_maxlen,
         ):
        r = client.post("/decide", json=_payload(), headers={"X-TDS-Key": "x"})

    assert r.status_code == 200
    assert r.json()["status"] == 302
    # The gate diverted BEFORE ever attempting the real XADD.
    fake_redis.xadd.assert_not_awaited()
    fake_enqueue.assert_awaited_once()


def test_gate_tripped_and_disk_fails_returns_503(client, patched_auth):
    fake_redis = _fake_redis()
    fake_route = AsyncMock(return_value=_MATCHED)
    fake_enqueue = AsyncMock(return_value=False)
    captured: list = []

    with patch("app.main.get_redis", new=AsyncMock(return_value=fake_redis)), \
         patch("app.main.route", new=fake_route), \
         patch("app.main.enqueue_click_to_disk", new=fake_enqueue), \
         patch("app.main.check_disk_pressure", return_value=(False, 10**9)), \
         patch(
             "app.main.get_cached_stream_clicks_length",
             return_value=settings.stream_clicks_maxlen + 1,
         ), \
         patch("app.main.capture_op_msg", new=_capturing_op_msg(captured)):
        r = client.post("/decide", json=_payload(), headers={"X-TDS-Key": "x"})

    assert r.status_code == 503
    assert r.json()["detail"] == "click_uncaptured"
    fake_redis.xadd.assert_not_awaited()
    ops = [c[0] for c in captured]
    # Both tags are meaningful here: entry-limit (why we diverted) and
    # click-uncaptured (the terminal failure once disk also failed).
    assert OP_STREAM_ENTRY_LIMIT in ops
    assert OP_CLICK_UNCAPTURED in ops


def test_gate_not_tripped_below_threshold_normal_xadd(client, patched_auth):
    """Sanity counterpart — a cached length comfortably under the
    threshold must not perturb the normal XADD happy path."""
    fake_redis = _fake_redis()
    fake_route = AsyncMock(return_value=_MATCHED)

    with patch("app.main.get_redis", new=AsyncMock(return_value=fake_redis)), \
         patch("app.main.route", new=fake_route), \
         patch("app.main.get_cached_stream_clicks_length", return_value=10):
        r = client.post("/decide", json=_payload(), headers={"X-TDS-Key": "x"})

    assert r.status_code == 200
    assert r.json()["status"] == 302
    fake_redis.xadd.assert_awaited_once()


# ---------------------------------------------------------------------------
# T7 (A4) — smoke probe over threshold -> 503, reject-only, no disk file
# ---------------------------------------------------------------------------


def test_smoke_over_threshold_returns_503_no_xadd_no_disk(client, patched_auth):
    fake_redis = _fake_redis()
    fake_enqueue = AsyncMock()
    captured: list = []

    with patch("app.main.get_redis", new=AsyncMock(return_value=fake_redis)), \
         patch("app.main.enqueue_click_to_disk", new=fake_enqueue), \
         patch(
             "app.main.get_cached_stream_clicks_length",
             return_value=settings.stream_clicks_maxlen,
         ), \
         patch("app.main.capture_op_msg", new=_capturing_op_msg(captured)):
        r = client.post(
            "/decide",
            json=_smoke_payload(),
            headers={"X-TDS-Key": "x"},
        )

    assert r.status_code == 503
    assert r.json()["detail"] == "stream_entry_limit"
    # Reject-only: no XADD attempt (synthetic click, nothing to preserve)...
    fake_redis.xadd.assert_not_awaited()
    # ...and no disk fallback either.
    fake_enqueue.assert_not_awaited()
    ops = [c[0] for c in captured]
    assert OP_STREAM_ENTRY_LIMIT in ops


def test_smoke_below_threshold_still_xadds_and_302s(client, patched_auth):
    """Sanity counterpart — a healthy node's smoke probe is unaffected."""
    fake_redis = _fake_redis()

    with patch("app.main.get_redis", new=AsyncMock(return_value=fake_redis)), \
         patch("app.main.get_cached_stream_clicks_length", return_value=0):
        r = client.post(
            "/decide",
            json=_smoke_payload(),
            headers={"X-TDS-Key": "x"},
        )

    assert r.status_code == 200
    assert r.json()["status"] == 302
    fake_redis.xadd.assert_awaited_once()


# ---------------------------------------------------------------------------
# T8 (A3) — a stale/failed cached signal FAILS OPEN to the XADD attempt
# ---------------------------------------------------------------------------


def test_never_sampled_cache_fails_open_to_xadd_attempt(client, patched_auth):
    """`None` (never sampled, or the sampler's last attempt failed) must
    NOT gate ingest — the gate can never itself become a new hot-path
    failure mode."""
    fake_redis = _fake_redis()
    fake_route = AsyncMock(return_value=_MATCHED)

    with patch("app.main.get_redis", new=AsyncMock(return_value=fake_redis)), \
         patch("app.main.route", new=fake_route), \
         patch("app.main.get_cached_stream_clicks_length", return_value=None):
        r = client.post("/decide", json=_payload(), headers={"X-TDS-Key": "x"})

    assert r.status_code == 200
    assert r.json()["status"] == 302
    fake_redis.xadd.assert_awaited_once()


def test_smoke_never_sampled_cache_fails_open(client, patched_auth):
    fake_redis = _fake_redis()

    with patch("app.main.get_redis", new=AsyncMock(return_value=fake_redis)), \
         patch("app.main.get_cached_stream_clicks_length", return_value=None):
        r = client.post(
            "/decide",
            json=_smoke_payload(),
            headers={"X-TDS-Key": "x"},
        )

    assert r.status_code == 200
    fake_redis.xadd.assert_awaited_once()


# ---------------------------------------------------------------------------
# _check_stream_backpressure — direct boundary pin (pure function)
# ---------------------------------------------------------------------------


class TestCheckStreamBackpressureBoundary:
    def test_none_cache_fails_open(self):
        from app.main import _check_stream_backpressure

        with patch("app.main.get_cached_stream_clicks_length", return_value=None):
            assert _check_stream_backpressure() is False

    def test_below_threshold_does_not_trip(self):
        from app.main import _check_stream_backpressure

        with patch(
            "app.main.get_cached_stream_clicks_length",
            return_value=settings.stream_clicks_maxlen - 1,
        ):
            assert _check_stream_backpressure() is False

    def test_at_threshold_trips(self):
        """Boundary: AT the threshold trips (>=, not >) — matches the
        collector's M1 semantics for consistency."""
        from app.main import _check_stream_backpressure

        with patch(
            "app.main.get_cached_stream_clicks_length",
            return_value=settings.stream_clicks_maxlen,
        ):
            assert _check_stream_backpressure() is True

    def test_over_threshold_trips(self):
        from app.main import _check_stream_backpressure

        with patch(
            "app.main.get_cached_stream_clicks_length",
            return_value=settings.stream_clicks_maxlen + 1,
        ):
            assert _check_stream_backpressure() is True
