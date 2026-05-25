"""F-1 (audit 2026-05-25) — blocked + no_match clicks become RECORDED
fallback clicks instead of crashing / being dropped.

Before this fix:
  * the block sentinel from `router.route` (`{"url": None, "blocked": True}`)
    reached the action_resolved checkpoint `result.get("url", "")[:200]` →
    `None[:200]` → TypeError → HTTP 500 → Worker "All backends failed" →
    fallback. (Sentry GEO-TDS-BACKEND-11.)
  * no_match (`result is None`) early-returned to the fallback URL but was
    NEVER recorded as a click.

After: both fall through to the SAME record-build → dedup → XADD → 302 path as
a matched click, routed to the (admin-configurable) fallback URL and tagged
`extra_params.routing_status` so they are queryable. The matched path is
unchanged.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient


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


def _fake_redis() -> MagicMock:
    r = MagicMock()
    r.set = AsyncMock(return_value=True)        # node-local dedup: first-seen
    r.xadd = AsyncMock(return_value="1-0")      # stream write
    return r


def _post(client, route_return):
    fake_redis = _fake_redis()
    fake_route = AsyncMock(return_value=route_return)
    with patch("app.main.get_redis", new=AsyncMock(return_value=fake_redis)), \
         patch("app.main.route", new=fake_route):
        r = client.post("/decide", json=_payload(), headers={"X-TDS-Key": "x"})
    return r, fake_redis


_BLOCK_SENTINEL = {
    "url": None,
    "campaign_id": "camp-1",
    "offer_id": None,
    "binding_id": 0,
    "binding_alias": None,
    "timing": {"result": "blocked_by_flow"},
    "blocked": True,
}


def test_blocked_click_does_not_crash_and_is_recorded(client, patched_auth):
    r, fake_redis = _post(client, _BLOCK_SENTINEL)

    # NOT a 500 (the old None[:200] crash).
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == 302
    assert "reason=blocked" in body["url"]
    assert "click_id=019e5be83c81" in body["url"]  # url-encoded click_id prefix

    # Recorded as a full click (XADD fired).
    fake_redis.xadd.assert_awaited_once()
    data = json.loads(fake_redis.xadd.await_args.args[1]["data"])
    assert data["offer_id"] is None
    assert "reason=blocked" in data["landing_url"]
    assert data["extra_params"]["routing_status"] == "blocked"


def test_no_match_click_is_recorded_as_fallback(client, patched_auth):
    r, fake_redis = _post(client, None)

    assert r.status_code == 200
    body = r.json()
    assert body["status"] == 302
    assert "reason=no_match" in body["url"]

    fake_redis.xadd.assert_awaited_once()
    data = json.loads(fake_redis.xadd.await_args.args[1]["data"])
    assert data["campaign_id"] is None
    assert data["offer_id"] is None
    assert data["extra_params"]["routing_status"] == "no_match"


def test_matched_click_happy_path_unchanged(client, patched_auth):
    """Regression fence: a matched result must NOT be tagged routing_status
    and must redirect to the offer URL (happy path byte-identical)."""
    matched = {
        "url": "https://offer.example.com/track?cid=1",
        "campaign_id": "camp-9",
        "offer_id": "offer-9",
        "binding_id": 0,
        "binding_alias": None,
        "timing": {"result": "flow_cascade"},
    }
    r, fake_redis = _post(client, matched)

    assert r.status_code == 200
    body = r.json()
    assert body["status"] == 302
    assert body["url"] == "https://offer.example.com/track?cid=1"

    fake_redis.xadd.assert_awaited_once()
    data = json.loads(fake_redis.xadd.await_args.args[1]["data"])
    assert data["offer_id"] == "offer-9"
    assert data["landing_url"] == "https://offer.example.com/track?cid=1"
    assert "routing_status" not in data["extra_params"]


def test_resolve_fallback_url_returns_configured_default():
    from app.main import _resolve_fallback_url
    from app.config import settings
    assert _resolve_fallback_url() == settings.fallback_url
