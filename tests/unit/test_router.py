"""Unit tests for the routing engine.

Tests cover: device/OS/browser parsing, URL building, weighted selection,
parameter mapping, edge cases, and malicious inputs.
"""

import pytest
from app.router import (
    parse_device_type,
    parse_os,
    parse_browser,
    build_url,
    weighted_select,
    weighted_select_from_dict,
)
from app.models import ClickRequest


# ============================================================
# Device Type Parsing
# ============================================================

class TestParseDeviceType:
    def test_iphone(self):
        assert parse_device_type("Mozilla/5.0 (iPhone; CPU iPhone OS 17_2)") == "mobile"

    def test_android_phone(self):
        assert parse_device_type("Mozilla/5.0 (Linux; Android 14; SM-S928B)") == "mobile"

    def test_ipad(self):
        assert parse_device_type("Mozilla/5.0 (iPad; CPU OS 17_2)") == "tablet"

    def test_windows_desktop(self):
        assert parse_device_type("Mozilla/5.0 (Windows NT 10.0; Win64; x64)") == "desktop"

    def test_mac_desktop(self):
        assert parse_device_type("Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2)") == "desktop"

    def test_linux_desktop(self):
        assert parse_device_type("Mozilla/5.0 (X11; Linux x86_64)") == "desktop"

    def test_empty_ua(self):
        assert parse_device_type("") == "desktop"

    def test_none_like(self):
        """Should not crash on unusual input."""
        assert parse_device_type("null") == "desktop"

    def test_bot_ua(self):
        assert parse_device_type("Googlebot/2.1") == "desktop"

    def test_curl(self):
        assert parse_device_type("curl/8.5.0") == "desktop"

    def test_mobile_keyword_generic(self):
        assert parse_device_type("SomeBrowser/1.0 Mobile") == "mobile"


# ============================================================
# OS Parsing
# ============================================================

class TestParseOS:
    def test_ios_iphone(self):
        assert parse_os("Mozilla/5.0 (iPhone; CPU iPhone OS 17_2)") == "ios"

    def test_ios_ipad(self):
        assert parse_os("Mozilla/5.0 (iPad; CPU OS 17_2)") == "ipados"

    def test_android(self):
        assert parse_os("Mozilla/5.0 (Linux; Android 14; SM-S928B)") == "android"

    def test_windows(self):
        assert parse_os("Mozilla/5.0 (Windows NT 10.0; Win64; x64)") == "windows"

    def test_macos(self):
        assert parse_os("Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2)") == "mac"

    def test_linux(self):
        assert parse_os("Mozilla/5.0 (X11; Linux x86_64)") == "gnu/linux"

    def test_empty(self):
        assert parse_os("") == "other"

    def test_unknown(self):
        assert parse_os("SomeRandomBot/1.0") == "other"


# ============================================================
# Browser Parsing
# ============================================================

class TestParseBrowser:
    def test_chrome(self):
        assert parse_browser("Mozilla/5.0 Chrome/120.0.6099.130 Safari/537.36") == "chrome"

    def test_safari(self):
        assert parse_browser("Mozilla/5.0 (Macintosh) AppleWebKit/605.1.15 Safari/605.1") == "safari"

    def test_firefox(self):
        assert parse_browser("Mozilla/5.0 (Windows; rv:121.0) Gecko/20100101 Firefox/121.0") == "firefox"

    def test_edge(self):
        assert parse_browser("Mozilla/5.0 Chrome/120.0 Safari/537.36 Edg/120.0") == "microsoft edge"

    def test_opera(self):
        assert parse_browser("Mozilla/5.0 Chrome/120.0 Safari/537.36 OPR/106.0") == "opera"

    def test_empty(self):
        assert parse_browser("") == "other"

    def test_bot(self):
        assert parse_browser("Googlebot/2.1") == "googlebot"


# ============================================================
# URL Building
# ============================================================

class TestBuildUrl:
    def _make_request(self, **kwargs):
        defaults = {
            "click_id": "test-click-123",
            "country": "US",
            "city": "New York",
            "region": "NY",
            "ip": "1.2.3.4",
            "user_agent": "iPhone",
            "query_params": {"source": "fb", "sub1": "creative_v1"},
        }
        defaults.update(kwargs)
        return ClickRequest(**defaults)

    def test_basic_macros(self):
        req = self._make_request()
        url = build_url(
            "https://example.com/offer?cid={click_id}&c={country}",
            req, "1", "101"
        )
        assert "cid=test-click-123" in url
        assert "c=US" in url

    def test_sub_params_from_query(self):
        req = self._make_request(query_params={"source": "fb", "creative": "vid_01"})
        url = build_url(
            "https://example.com/?src={source}&cr={creative}",
            req, "1", "101"
        )
        assert "src=fb" in url
        assert "cr=vid_01" in url

    def test_empty_macro_not_crash(self):
        req = self._make_request(visitor_id=None)
        url = build_url(
            "https://example.com/?vid={visitor_id}",
            req, "1", "101"
        )
        assert "vid=" in url  # empty but no crash

    def test_no_macros(self):
        req = self._make_request()
        url = build_url("https://example.com/static-page", req, "1", "101")
        assert url == "https://example.com/static-page"

    def test_special_characters_in_params(self):
        """Params with special chars must be URL-encoded to prevent injection."""
        req = self._make_request(query_params={"source": "a&b=c", "sub1": "test<script>"})
        url = build_url("https://example.com/?s={source}", req, "1", "101")
        # URL-encoded: & → %26, = → %3D (prevents URL parameter injection)
        assert "a%26b%3Dc" in url
        assert "<script>" not in url  # XSS chars encoded


# ============================================================
# Weighted Selection
# ============================================================

class TestWeightedSelect:
    def test_single_item(self):
        items = [{"weight": "100", "name": "a"}]
        result = weighted_select(items)
        assert result["name"] == "a"

    def test_zero_weight_fallback(self):
        """Items with 0 weight should still not crash."""
        items = [{"weight": "0", "name": "a"}, {"weight": "100", "name": "b"}]
        # random.choices handles 0 weight correctly
        result = weighted_select(items)
        assert result["name"] == "b"

    def test_distribution(self):
        """Over many runs, 70/30 split should approximate 70/30."""
        items = [{"weight": "70", "name": "a"}, {"weight": "30", "name": "b"}]
        counts = {"a": 0, "b": 0}
        for _ in range(10000):
            r = weighted_select(items)
            counts[r["name"]] += 1
        # Allow 5% tolerance
        assert 6500 < counts["a"] < 7500
        assert 2500 < counts["b"] < 3500


class TestWeightedSelectFromDict:
    def test_single(self):
        assert weighted_select_from_dict({"101": "100"}) == "101"

    def test_distribution(self):
        d = {"101": "70", "102": "30"}
        counts = {"101": 0, "102": 0}
        for _ in range(10000):
            counts[weighted_select_from_dict(d)] += 1
        assert 6500 < counts["101"] < 7500


# ============================================================
# Edge Cases & Malicious Input
# ============================================================

class TestEdgeCases:
    def test_very_long_ua(self):
        """UA strings can be 500+ chars. Should not crash or slow down."""
        ua = "A" * 10000
        assert parse_device_type(ua) == "desktop"
        assert parse_os(ua) == "other"
        assert parse_browser(ua) == "other"

    def test_unicode_in_ua(self):
        ua = "Mozilla/5.0 (Windows) 日本語テスト Chrome/120"
        assert parse_device_type(ua) == "desktop"
        # device_detector may not parse Chrome from this unusual UA
        assert isinstance(parse_browser(ua), str)

    def test_null_bytes_in_ua(self):
        ua = "Mozilla/5.0\x00(iPhone)"
        # Should not crash
        result = parse_device_type(ua)
        assert isinstance(result, str)

    def test_empty_query_params(self):
        req = ClickRequest(click_id="test", query_params={})
        url = build_url("https://example.com/?s={source}", req, "1", "101")
        assert "s={source}" in url  # unreplaced macro stays

    def test_huge_query_params(self):
        """Thousands of query params should not crash."""
        params = {f"param_{i}": f"value_{i}" for i in range(1000)}
        req = ClickRequest(click_id="test", query_params=params)
        url = build_url("https://example.com/", req, "1", "101")
        assert "example.com" in url
