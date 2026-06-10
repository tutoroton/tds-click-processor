"""F-1 (audit 2026-05-25) + F-2 (2026-06-10) — blocked + no_match clicks are
RECORDED and signal the WORKER-OWNED fallback.

F-1: both the block sentinel and no_match fall through to the SAME
record-build → dedup → XADD → 302 path as a matched click (previously: the
None[:200] crash / silent drop — Sentry GEO-TDS-BACKEND-11).

F-2: the node-level default fallback URL is GONE. A no-route click answers
`{"url": "", "fallback": true, "fallback_reason": <reason>}` — the Worker
redirects to its admin-configured FALLBACK_URL, appending reason + click_id
itself. The click is still fully recorded (landing_url = "") and tagged
`extra_params.routing_status` so it stays queryable. A per-campaign
`campaigns.fallback_url` (admin setting) still produces a normal absolute
url on the node — covered in test_router_cascade.py. The matched path is
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
    # F-2: no node-built URL — the Worker owns the fallback destination.
    assert body["url"] == ""
    assert body["fallback"] is True
    assert body["fallback_reason"] == "blocked"

    # Recorded as a full click (XADD fired).
    fake_redis.xadd.assert_awaited_once()
    data = json.loads(fake_redis.xadd.await_args.args[1]["data"])
    assert data["offer_id"] is None
    assert data["landing_url"] == ""
    assert data["extra_params"]["routing_status"] == "blocked"


def test_no_match_click_is_recorded_as_fallback(client, patched_auth):
    r, fake_redis = _post(client, None)

    assert r.status_code == 200
    body = r.json()
    assert body["status"] == 302
    assert body["url"] == ""
    assert body["fallback"] is True
    assert body["fallback_reason"] == "no_match"

    fake_redis.xadd.assert_awaited_once()
    data = json.loads(fake_redis.xadd.await_args.args[1]["data"])
    assert data["campaign_id"] is None
    assert data["offer_id"] is None
    assert data["extra_params"]["routing_status"] == "no_match"


def test_campaign_fallback_url_still_serves_absolute_redirect(client, patched_auth):
    """F-2 boundary: a per-campaign fallback_url (admin setting carried by the
    non_routed sentinel) still produces a normal absolute redirect on the node
    — the worker-fallback flag must be ABSENT/false on that path."""
    sentinel = {
        "url": None,
        "campaign_id": "camp-7",
        "offer_id": None,
        "binding_id": 0,
        "binding_alias": None,
        "timing": {"result": "no_candidates"},
        "non_routed": True,
        "routing_status": "no_candidates",
        "fallback_url": "https://camp-lander.example/lp?src=tds",
    }
    r, fake_redis = _post(client, sentinel)

    assert r.status_code == 200
    body = r.json()
    assert body["status"] == 302
    assert body["url"].startswith("https://camp-lander.example/lp?src=tds&reason=no_candidates")
    assert "click_id=019e5be83c81" in body["url"]
    assert body.get("fallback") is not True

    fake_redis.xadd.assert_awaited_once()
    data = json.loads(fake_redis.xadd.await_args.args[1]["data"])
    assert data["landing_url"].startswith("https://camp-lander.example/lp")
    assert data["extra_params"]["routing_status"] == "no_candidates"


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


def test_no_node_level_fallback_url_remains():
    """F-2 pin: the node carries NO default fallback URL — neither the old
    settings field nor the old resolver chokepoint may come back silently."""
    import app.main as main_module
    from app.config import settings
    assert not hasattr(settings, "fallback_url")
    assert not hasattr(main_module, "_resolve_fallback_url")
