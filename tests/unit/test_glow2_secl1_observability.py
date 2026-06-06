"""G-LOW-2 (route() catch-all is tagged) + SEC-L1 (X-Test-Id whitelist-gated
before the Sentry tag / response echo). Both exercised via the /decide handler;
the route-error branch is convenient because `set_tag(test_id)` runs BEFORE
route() and the branch returns a 302 without needing the full success path.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.telemetry import OP_ROUTE_ERROR

VALID_TID = "12345678-1234-1234-1234-123456789abc"   # uuid4 → passes _VALID_TEST_ID
BAD_TID_CHARS = "evil_<script>"                        # out-of-charset
BAD_TID_LONG = "a" * 80                                # > 64


@pytest.fixture
def client():
    from app.main import app
    return TestClient(app)


def _payload(cid="019e5be83c8179896a0859dd"):
    return {"click_id": cid, "ip": "1.2.3.4", "country": "US",
            "user_agent": "geo-tds-test/1.0"}


def _post(client, headers):
    """POST /decide with route() RAISING (auth + redis patched). Returns
    (response, capture_op_exc_mock, set_tag_mock)."""
    fake_redis = MagicMock()
    fake_redis.set = AsyncMock(return_value=True)
    fake_redis.xadd = AsyncMock(return_value="1-0")
    with patch("app.main._check_tds_key", new_callable=AsyncMock), \
         patch("app.main.get_redis", new=AsyncMock(return_value=fake_redis)), \
         patch("app.main.route", new=AsyncMock(side_effect=RuntimeError("boom"))), \
         patch("app.main.capture_op_exc") as cap, \
         patch("app.main.sentry_sdk.set_tag") as set_tag:
        r = client.post("/decide", json=_payload(), headers=headers)
    return r, cap, set_tag


class TestGLow2RouteErrorTagged:
    def test_route_error_capture_is_tagged_with_op_and_context(self):
        r, cap, _ = _post(self_client(), {"X-TDS-Key": "x"})
        assert r.status_code == 200  # route-error → 302 fallback ClickResponse
        assert cap.called
        assert cap.call_args.args[0] == OP_ROUTE_ERROR          # op tag (was untagged)
        kw = cap.call_args.kwargs
        # returning context present so the issue is filterable + diagnosable
        assert "resolver_enabled" in kw and "routing_enabled" in kw
        assert kw["click_id"] == _payload()["click_id"]


class TestSecL1TestIdGate:
    def test_valid_test_id_is_tagged(self):
        _, _, set_tag = _post(self_client(), {"X-TDS-Key": "x", "X-Test-Id": VALID_TID})
        calls = [c for c in set_tag.call_args_list if c.args and c.args[0] == "test_id"]
        assert calls and calls[0].args[1] == VALID_TID

    def test_invalid_charset_test_id_not_tagged(self):
        _, _, set_tag = _post(self_client(), {"X-TDS-Key": "x", "X-Test-Id": BAD_TID_CHARS})
        assert not [c for c in set_tag.call_args_list
                    if c.args and c.args[0] == "test_id"]

    def test_oversized_test_id_not_tagged(self):
        _, _, set_tag = _post(self_client(), {"X-TDS-Key": "x", "X-Test-Id": BAD_TID_LONG})
        assert not [c for c in set_tag.call_args_list
                    if c.args and c.args[0] == "test_id"]


# TestClient is process-global-cheap; build one per call to keep tests isolated.
def self_client():
    from app.main import app
    return TestClient(app)
