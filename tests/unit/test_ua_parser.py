"""Unit tests for UA parsing via device_detector.

Tests cover: device type normalization, OS detection, browser detection,
device brand/model extraction, bot detection, caching, edge cases.
"""

import pytest
from app.ua_parser import parse_ua


# ============================================================
# Device Type (normalized to mobile/tablet/desktop)
# ============================================================

class TestDeviceType:
    def test_iphone(self):
        r = parse_ua("Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15")
        assert r["device_type"] == "mobile"

    def test_android_phone(self):
        r = parse_ua("Mozilla/5.0 (Linux; Android 14; SM-S928B) AppleWebKit/537.36 Chrome/120.0 Mobile Safari/537.36")
        assert r["device_type"] == "mobile"

    def test_ipad(self):
        r = parse_ua("Mozilla/5.0 (iPad; CPU OS 17_2 like Mac OS X) AppleWebKit/605.1.15")
        assert r["device_type"] == "tablet"

    def test_windows_desktop(self):
        r = parse_ua("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36")
        assert r["device_type"] == "desktop"

    def test_mac_desktop(self):
        r = parse_ua("Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2) AppleWebKit/605.1.15 Safari/605.1")
        assert r["device_type"] == "desktop"

    def test_empty_ua(self):
        r = parse_ua("")
        assert r["device_type"] == "desktop"

    def test_none_handled(self):
        """parse_ua receives empty string from router wrappers when UA is None."""
        r = parse_ua("")
        assert r["device_type"] == "desktop"


# ============================================================
# OS Detection
# ============================================================

class TestOS:
    def test_ios(self):
        r = parse_ua("Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X)")
        assert r["os"] == "ios"

    def test_android(self):
        r = parse_ua("Mozilla/5.0 (Linux; Android 14; SM-S928B)")
        assert r["os"] == "android"

    def test_windows(self):
        r = parse_ua("Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0")
        assert r["os"] == "windows"

    def test_macos(self):
        r = parse_ua("Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2) Safari/605.1")
        assert r["os"] == "mac"

    def test_linux(self):
        r = parse_ua("Mozilla/5.0 (X11; Linux x86_64) Chrome/120.0")
        assert r["os"] == "gnu/linux"

    def test_empty(self):
        r = parse_ua("")
        assert r["os"] == "other"

    def test_os_version_populated(self):
        r = parse_ua("Mozilla/5.0 (Linux; Android 14; SM-S928B) Chrome/120.0 Mobile Safari/537.36")
        assert r["os_version"] == "14"


# ============================================================
# Browser Detection
# ============================================================

class TestBrowser:
    def test_chrome(self):
        r = parse_ua("Mozilla/5.0 (Windows NT 10.0) Chrome/120.0.6099.130 Safari/537.36")
        assert r["browser"] == "chrome"

    def test_safari(self):
        r = parse_ua("Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2) AppleWebKit/605.1.15 Version/17.2 Safari/605.1")
        assert r["browser"] == "safari"

    def test_firefox(self):
        r = parse_ua("Mozilla/5.0 (Windows; rv:121.0) Gecko/20100101 Firefox/121.0")
        assert r["browser"] == "firefox"

    def test_edge(self):
        r = parse_ua("Mozilla/5.0 Chrome/120.0 Safari/537.36 Edg/120.0")
        assert r["browser"] == "microsoft edge"

    def test_empty(self):
        r = parse_ua("")
        assert r["browser"] == "other"

    def test_browser_version_populated(self):
        r = parse_ua("Mozilla/5.0 (Windows) Chrome/120.0.6099.130 Safari/537.36")
        assert "120" in r["browser_version"]


# ============================================================
# Device Brand & Model
# ============================================================

class TestDeviceBrandModel:
    def test_iphone_brand(self):
        r = parse_ua("Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15")
        assert r["device_brand"] == "Apple"
        assert r["device_model"] == "iPhone"

    def test_samsung_model(self):
        r = parse_ua("Mozilla/5.0 (Linux; Android 14; SM-S928B) AppleWebKit/537.36 Chrome/120.0 Mobile Safari/537.36")
        assert r["device_brand"] == "Samsung"
        assert "Galaxy" in r["device_model"]

    def test_desktop_no_brand(self):
        r = parse_ua("Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0 Safari/537.36")
        assert r["device_brand"] == ""

    def test_ipad_brand(self):
        r = parse_ua("Mozilla/5.0 (iPad; CPU OS 17_2 like Mac OS X) AppleWebKit/605.1.15")
        assert r["device_brand"] == "Apple"
        assert r["device_model"] == "iPad"


# ============================================================
# Bot Detection
# ============================================================

class TestBotDetection:
    def test_googlebot(self):
        r = parse_ua("Googlebot/2.1 (+http://www.google.com/bot.html)")
        assert r["is_bot"] is True

    def test_curl(self):
        r = parse_ua("curl/8.5.0")
        assert r["is_bot"] is False

    def test_normal_browser_not_bot(self):
        r = parse_ua("Mozilla/5.0 (Windows NT 10.0) Chrome/120.0 Safari/537.36")
        assert r["is_bot"] is False


# ============================================================
# Edge Cases & Performance
# ============================================================

class TestEdgeCases:
    def test_very_long_ua(self):
        ua = "A" * 10000
        r = parse_ua(ua)
        assert isinstance(r["device_type"], str)

    def test_unicode_in_ua(self):
        r = parse_ua("Mozilla/5.0 (Windows) 日本語テスト Chrome/120")
        assert isinstance(r["device_type"], str)

    def test_null_bytes_in_ua(self):
        r = parse_ua("Mozilla/5.0\x00(iPhone)")
        assert isinstance(r["device_type"], str)

    def test_cache_returns_same_result(self):
        """Identical UA strings should return cached result."""
        ua = "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2)"
        r1 = parse_ua(ua)
        r2 = parse_ua(ua)
        assert r1 is r2  # Same object from cache

    def test_all_fields_present(self):
        """Every result must have all expected keys."""
        r = parse_ua("Mozilla/5.0 Chrome/120")
        expected_keys = {"device_type", "device_type_raw", "os", "os_version",
                         "browser", "browser_version", "device_brand", "device_model", "is_bot"}
        assert set(r.keys()) == expected_keys
