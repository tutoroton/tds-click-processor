"""C2/V9 — the bot verdict this service COMPUTES must reach the click record.

`ua_parser.parse_ua` has always returned a device_detector `is_bot` verdict and
the click record has always thrown it away, writing `req.is_bot` -- the EDGE
(CF Bot Management) flag -- instead.

WHY THAT WAS WRONG, measured on live staging ClickHouse 2026-08-25 over
`tds.events` (19 788 924 clicks, 5 606 distinct user agents):

  * We hold no CF Bot Management subscription, so the edge verdict is False on
    every real click. The only 1s ever recorded (379 539 rows) came from the
    synthetic load generator, stopped dead on 2026-07-08, and were ~2% RANDOM
    NOISE: the SAME three ordinary browser UA strings appear on both sides of
    the flag (1.97% / 2.00% / 1.99% bot). A UA-derived verdict is deterministic
    per UA, so a ~2% split across identical UAs proves the flag was not derived
    from anything.
  * Meanwhile the genuine crawlers were all recorded as HUMAN:
    TLM-Audit-Scanner/1.0 (12 359 clicks, 0 flagged), WordPress-install
    scanner probes (1 364, 0 flagged), and every AI/search crawler below.
  * Running the real `parse_ua` over every live UA weighted by click count:
    266 of 5 606 UAs are bots -> 14.78% of real post-load-generator traffic
    (32 795 of 221 861), 0.254% across the whole synthetic-dominated table.

These tests exercise the FULL `/decide` path and assert on the click record
actually handed to the stream, not on the helper in isolation -- the helper
being right proves nothing if the call site never passes the verdict.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

# Verbatim from live staging: the top bot UAs by click volume, and a control
# that is the single highest-volume human UA in the same table.
UA_CLAUDEBOT = ("Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko); "
                "compatible; ClaudeBot/1.0; +claudebot@anthropic.com)")
UA_AMAZONBOT = ("Mozilla/5.0 (compatible; Amazonbot/0.1; "
                "+https://developer.amazon.com/support/amazonbot)")
UA_CURL = "curl/8.5.0"
UA_GO = "Go-http-client/2.0"
UA_TLM = "TLM-Audit-Scanner/1.0"
UA_HUMAN = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")


@pytest.fixture
def client():
    from app.main import app
    return TestClient(app)


@pytest.fixture
def patched_auth():
    with patch("app.main._check_tds_key", new_callable=AsyncMock):
        yield


def _record(client, user_agent: str, *, edge_is_bot: bool = False) -> dict:
    """POST /decide with `user_agent` and return the recorded click record."""
    fake_redis = MagicMock()
    fake_redis.set = AsyncMock(return_value=True)
    fake_redis.xadd = AsyncMock(return_value="1-0")
    payload = {
        "click_id": "019e5be83c8179896a0859dd",
        "ip": "1.2.3.4",
        "country": "DE",
        "user_agent": user_agent,
        "is_bot": edge_is_bot,
    }
    with patch("app.main.get_redis", new=AsyncMock(return_value=fake_redis)), \
         patch("app.main.route", new=AsyncMock(return_value=None)):
        r = client.post("/decide", json=payload, headers={"X-TDS-Key": "x"})
    assert r.status_code == 200, r.text
    fake_redis.xadd.assert_awaited_once()
    return json.loads(fake_redis.xadd.await_args.args[1]["data"])


@pytest.mark.parametrize("ua", [UA_CLAUDEBOT, UA_AMAZONBOT, UA_TLM])
def test_declared_crawler_is_recorded_as_a_bot(client, patched_auth, ua):
    """THE FIX. Every one of these was recorded as human on live staging."""
    assert _record(client, ua)["is_bot"] is True


def test_ordinary_browser_is_not_a_bot(client, patched_auth):
    """CONTROL — without it the parametrized test above passes on a stuck True."""
    assert _record(client, UA_HUMAN)["is_bot"] is False


def test_edge_verdict_still_wins_on_a_browser_ua(client, patched_auth):
    """UNION, not replacement: a CF verdict on a browser UA is still honoured,
    so the column stays correct if a Bot Management subscription is added."""
    assert _record(client, UA_HUMAN, edge_is_bot=True)["is_bot"] is True


def test_empty_user_agent_is_not_a_bot(client, patched_auth):
    """373 257 live clicks carry no UA at all -- they must not become bots."""
    assert _record(client, "")["is_bot"] is False


@pytest.mark.parametrize("ua", [UA_CURL, UA_GO])
def test_http_library_clients_are_deliberately_not_bots(client, patched_auth, ua):
    """A NAMED LIMIT, pinned so it stays a decision instead of becoming a
    surprise. device_detector separates a `library` client (curl, Wget,
    Go-http-client, Python Requests, aiohttp, Axios) from a `bot`, and this fix
    adopts that taxonomy verbatim rather than inventing its own -- so the
    14.78% figure above and the shipped behaviour are the same measurement.

    Measured on the same live window: library clients are a further 3.03% of
    real traffic (6 723 of 221 861 -- curl 5 452, Go-http-client 665, Python
    Requests 504, aiohttp 60, Axios 40, Java 1) and remain `is_bot = 0`.

    Whether an HTTP library should count as a bot is a PRODUCT question, not a
    defect: an aiohttp caller may be a legitimate API consumer. Widening it
    would be a separate, deliberate change -- and would move this test first.
    """
    assert _record(client, ua)["is_bot"] is False
