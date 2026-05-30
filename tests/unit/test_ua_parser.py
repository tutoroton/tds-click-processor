"""Unit tests for UA parsing via device_detector.

Tests cover: device type normalization, OS detection, browser detection,
device brand/model extraction, bot detection, caching, edge cases.
"""

import pytest
from app.ua_parser import parse_ua, warmup, _WARMUP_UAS, _UA_CACHE_SIZE


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
    # F.17 (2026-05-03): browser values are now device_detector's
    # canonical Title Case names (`Chrome`, `Microsoft Edge`,
    # `Mobile Safari`) instead of the previous `.lower()` shorthand.
    # Storage vocabulary in admin-api's `browser` criterion enum is
    # the same taxonomy — drift here breaks live-click matching.
    def test_chrome(self):
        r = parse_ua("Mozilla/5.0 (Windows NT 10.0) Chrome/120.0.6099.130 Safari/537.36")
        assert r["browser"] == "Chrome"

    def test_safari(self):
        r = parse_ua("Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2) AppleWebKit/605.1.15 Version/17.2 Safari/605.1")
        assert r["browser"] == "Safari"

    def test_firefox(self):
        r = parse_ua("Mozilla/5.0 (Windows; rv:121.0) Gecko/20100101 Firefox/121.0")
        assert r["browser"] == "Firefox"

    def test_edge(self):
        r = parse_ua("Mozilla/5.0 Chrome/120.0 Safari/537.36 Edg/120.0")
        assert r["browser"] == "Microsoft Edge"

    def test_mobile_safari(self):
        # iOS Safari yields "Mobile Safari" (not just "Safari") —
        # important contrast for criterion targeting.
        r = parse_ua("Mozilla/5.0 (iPhone; CPU iPhone OS 17_0) AppleWebKit/605.1.15 Version/17.0 Mobile/15E148 Safari/604.1")
        assert r["browser"] == "Mobile Safari"

    def test_samsung_browser(self):
        r = parse_ua("Mozilla/5.0 (Linux; Android 13; SM-S918B) SamsungBrowser/22.0 Chrome/115.0.0.0 Mobile Safari/537.36")
        assert r["browser"] == "Samsung Browser"

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


# ============================================================
# F.41 — Warm-up + cache (cold-start latency fix)
# ============================================================

class TestWarmup:
    """warmup() pre-loads device_detector's lazy regex bundles at service
    startup so the first production click parses warm (~ms) instead of paying
    the ~720ms cold load that crossed the worker's 2000ms deadline (F1)."""

    def test_warmup_parses_every_entry(self):
        # All representative UAs parse without error → return == set size.
        assert warmup() == len(_WARMUP_UAS)

    def test_warmup_uas_are_distinct(self):
        # Duplicates would waste warm-up work and mislead the cache assertion.
        assert len(set(_WARMUP_UAS)) == len(_WARMUP_UAS)

    def test_warmup_populates_cache(self):
        parse_ua.cache_clear()
        warmup()
        # Every distinct warm-up UA is now resident in the LRU.
        assert parse_ua.cache_info().currsize == len(_WARMUP_UAS)

    def test_warmup_is_idempotent(self):
        # Safe to call repeatedly (e.g. per uvicorn worker) — stable count.
        assert warmup() == warmup()

    def test_warmup_never_raises_on_bad_entry(self, monkeypatch):
        # Defense-in-depth: even if a single UA blows up, warm-up continues.
        import app.ua_parser as m
        real_parse = m.parse_ua

        def flaky(ua):
            if "BOOM" in ua:
                raise RuntimeError("simulated parse failure")
            return real_parse(ua)

        monkeypatch.setattr(m, "parse_ua", flaky)
        monkeypatch.setattr(m, "_WARMUP_UAS", ("good-ua", "BOOM-ua", "another-good"))
        # 2 good + 1 raising → returns 2, no exception propagates.
        assert m.warmup() == 2


class TestCacheSizing:
    def test_lru_maxsize_env_driven(self):
        # The LRU maxsize is driven by TDS_UA_CACHE_SIZE (default 32768),
        # replacing the old hardcoded 4096. Larger cache keeps more hot UAs
        # resident under real traffic — output values are unchanged.
        assert parse_ua.cache_info().maxsize == _UA_CACHE_SIZE
        assert _UA_CACHE_SIZE >= 4096


class TestOutputIdentityGuard:
    """F.41 — lock parsed VALUES at unit granularity so the warm-up/cache
    change can never silently alter routing-relevant output. Mirrors the full
    37,654-UA golden diff (G3/S1). Values verified against device_detector
    6.2.0 (note iPad → os 'ipados', Android Chrome → 'Chrome Mobile')."""

    @pytest.mark.parametrize("ua,device_type,os_name,browser", [
        ("Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 "
         "(KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
         "mobile", "ios", "Mobile Safari"),
        ("Mozilla/5.0 (Linux; Android 14; SM-S928B) AppleWebKit/537.36 "
         "(KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
         "mobile", "android", "Chrome Mobile"),
        ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
         "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
         "desktop", "windows", "Chrome"),
        ("Mozilla/5.0 (iPad; CPU OS 17_2 like Mac OS X) AppleWebKit/605.1.15 "
         "(KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
         "tablet", "ipados", "Mobile Safari"),
        ("Mozilla/5.0 (Linux; Android 12; RMX3085) AppleWebKit/537.36 "
         "(KHTML, like Gecko) Chrome/108.0.0.0 YaBrowser/23.1.1.100 Mobile Safari/537.36",
         "mobile", "android", "Yandex Browser"),
    ])
    def test_known_ua_values_locked(self, ua, device_type, os_name, browser):
        r = parse_ua(ua)
        assert r["device_type"] == device_type
        assert r["os"] == os_name
        assert r["browser"] == browser
