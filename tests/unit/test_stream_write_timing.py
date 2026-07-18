"""GTD-R218/PERF-2 (GTD-V23, 2026-07-17) — the click record shipped to
`stream:clicks` (and from there, durably, to ClickHouse `tds.events`)
must carry a NON-zero, honestly-computed pre-stream-write timing field,
and the always-on `stream_write_metrics` rolling window (the "equivalent
durable metric" the finding's Verified clause allows for the round-trip
itself, which cannot self-embed) must be reachable via `/health`.

Regression target: pre-fix, `timing["endpoint_total_ms"]` /
`timing["stream_write_ms"]` were set on the SAME `timing` dict object
AFTER `json.dumps(click_record, ...)` had already frozen a snapshot into
the XADD payload — so every stored click carried those fields as 0/absent
(100% over 7d/22k clicks per the finding). `pre_stream_ms` is computed
and inserted into `timing` BEFORE that serialisation, so it must actually
appear, non-zero, in the exact bytes handed to `redis.xadd`.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app import stream_write_metrics


@pytest.fixture
def client():
    from app.main import app
    return TestClient(app)


@pytest.fixture
def patched_auth():
    with patch("app.main._check_tds_key", new_callable=AsyncMock):
        yield


@pytest.fixture(autouse=True)
def _reset_stream_write_window():
    stream_write_metrics._reset_for_tests()
    yield
    stream_write_metrics._reset_for_tests()


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


def _fake_redis() -> MagicMock:
    r = MagicMock()
    r.set = AsyncMock(return_value=True)  # node-local dedup: first-seen
    r.xadd = AsyncMock(return_value="1-0")
    return r


def _decide(client, patched_auth, fake_redis):
    fake_route = AsyncMock(return_value=_MATCHED)
    with patch("app.main.get_redis", new=AsyncMock(return_value=fake_redis)), \
         patch("app.main.route", new=fake_route), \
         patch("app.main.get_cached_stream_clicks_length", return_value=10):
        return client.post("/decide", json=_payload(), headers={"X-TDS-Key": "x"})


class TestPreStreamMsReachesStoredPayload:
    def test_pre_stream_ms_present_and_nonnegative_in_xadd_payload(
        self, client, patched_auth
    ):
        fake_redis = _fake_redis()
        resp = _decide(client, patched_auth, fake_redis)

        assert resp.status_code == 200
        fake_redis.xadd.assert_awaited_once()
        (_stream_key, fields), _kwargs = fake_redis.xadd.call_args
        stored = json.loads(fields["data"])

        assert "pre_stream_ms" in stored["timing"]
        assert isinstance(stored["timing"]["pre_stream_ms"], (int, float))
        assert stored["timing"]["pre_stream_ms"] >= 0

    def test_endpoint_total_ms_and_stream_write_ms_absent_from_stored_payload(
        self, client, patched_auth
    ):
        """Documents the STILL-TRUE structural limit (not a regression): a
        click's own `stream_write_ms`/`endpoint_total_ms` genuinely cannot
        ride in its own XADD payload — they are computed after it. If this
        ever starts passing, `timing` assembly moved past the XADD call
        and `pre_stream_ms` computation should be re-examined too."""
        fake_redis = _fake_redis()
        _decide(client, patched_auth, fake_redis)

        (_stream_key, fields), _kwargs = fake_redis.xadd.call_args
        stored = json.loads(fields["data"])

        assert "stream_write_ms" not in stored["timing"]
        assert "endpoint_total_ms" not in stored["timing"]

    def test_response_still_carries_full_timing_incl_post_xadd_fields(
        self, client, patched_auth
    ):
        """The HTTP response to the Worker is built AFTER the XADD, so it
        legitimately carries the full timing dict incl. the post-XADD
        fields — unaffected by this fix (belt-and-suspenders pin)."""
        fake_redis = _fake_redis()
        resp = _decide(client, patched_auth, fake_redis)

        timing = resp.json()["timing"]
        assert "pre_stream_ms" in timing
        assert "endpoint_total_ms" in timing
        assert "stream_write_ms" in timing


class TestStreamWriteMetricsWindow:
    def test_no_samples_yet_health_reports_none_and_zero_count(self, client):
        resp = client.get("/health")
        body = resp.json()
        assert body["stream_write_sample_count"] == 0
        assert body["stream_write_p50_ms"] is None
        assert body["stream_write_p95_ms"] is None
        assert body["stream_write_max_ms"] is None

    def test_successful_xadd_feeds_the_rolling_window(self, client, patched_auth):
        fake_redis = _fake_redis()
        _decide(client, patched_auth, fake_redis)

        stats = stream_write_metrics.stream_write_stats()
        assert stats["stream_write_sample_count"] == 1
        assert stats["stream_write_p50_ms"] is not None
        assert stats["stream_write_p50_ms"] >= 0

    def test_health_endpoint_surfaces_the_window(self, client, patched_auth):
        fake_redis = _fake_redis()
        _decide(client, patched_auth, fake_redis)

        body = client.get("/health").json()
        assert body["stream_write_sample_count"] == 1
        assert body["stream_write_p50_ms"] is not None
        assert body["stream_write_max_ms"] is not None
