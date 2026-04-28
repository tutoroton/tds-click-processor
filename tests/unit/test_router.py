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
    """Vector 2.8 — build_url integrates resolve_slots + safe_substitute.

    Macros split into three layers (precedence ascending):
      1. Slot layer — sub1..sub20 + 19 reserved slots resolved via
         the merged source∪campaign mapping chain.
      2. Worker-auto layer — country, city, ip, user_agent, ...
         (system-fixed names, populated from request).
      3. Technical layer — click_id, campaign_id, offer_id,
         visitor_id (system-fixed, always wins).

    These tests verify the integration. Per-priority semantics of
    the slot layer are exhaustively covered in `test_resolution.py`.
    """

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

    def test_technical_macros_always_resolve(self):
        req = self._make_request()
        url = build_url(
            "https://example.com/offer?cid={click_id}&oid={offer_id}",
            req, "1", "101",
        )
        assert "cid=test-click-123" in url
        assert "oid=101" in url

    def test_worker_auto_macros_always_resolve(self):
        req = self._make_request()
        url = build_url(
            "https://example.com/?c={country}&city={city}",
            req, "1", "101",
        )
        # `country` and `city` come from the worker-auto layer — no
        # mapping required.
        assert "c=US" in url
        assert "city=New%20York" in url  # URL-encoded space

    def test_slot_macro_via_source_mapping(self):
        """Source mapping binds incoming `creative` → slot `sub1`.

        The url_template uses `{sub1}` (canonical slot name), NOT
        `{creative}`. That's the Vector 2.8 contract — macros refer
        to canonical slots, mapping aliases the incoming key.
        """
        req = self._make_request(query_params={"creative": "vid_01"})
        url = build_url(
            "https://example.com/?cr={sub1}", req, "1", "101",
            source_mappings=[{"slot": "sub1", "alias": "creative"}],
        )
        assert "cr=vid_01" in url

    def test_slot_falls_back_to_hardcoded(self):
        # No request value, source has hardcoded default → applies.
        req = self._make_request(query_params={})
        url = build_url(
            "https://example.com/?p={pixel_id}", req, "1", "101",
            source_mappings=[{"slot": "pixel_id", "default_value": "fb_pixel_42"}],
        )
        assert "p=fb_pixel_42" in url

    def test_unmapped_slot_collapses_to_empty(self):
        """When mapping doesn't bind a slot, `{slot}` macro drops out.

        New behavior — old `build_url` left `{macro}` literal in the
        URL. `safe_substitute` cleans up empty query params, so the
        macro position vanishes entirely.
        """
        req = self._make_request(query_params={})
        url = build_url(
            "https://example.com/?s={sub1}&keep=1", req, "1", "101",
            source_mappings=None,
            campaign_mappings=None,
        )
        # `{sub1}` had no mapping → empty → query cleanup drops `s=`.
        assert "s=" not in url
        assert "keep=1" in url

    def test_no_macros(self):
        req = self._make_request()
        url = build_url("https://example.com/static-page", req, "1", "101")
        assert url == "https://example.com/static-page"

    def test_url_encoding_via_safe_substitute(self):
        """Special characters in slot values get URL-encoded by `safe_substitute`.

        Verifies the `safe_substitute` integration — encoding is
        always-on regardless of which layer the value came from.
        """
        req = self._make_request(query_params={"creative": "a&b=c<script>"})
        url = build_url(
            "https://example.com/?s={sub1}", req, "1", "101",
            source_mappings=[{"slot": "sub1", "alias": "creative"}],
        )
        # `&` → %26, `=` → %3D, `<` → %3C, `>` → %3E.
        assert "a%26b%3Dc%3Cscript%3E" in url
        assert "<script>" not in url
        assert "a&b=c" not in url

    def test_visitor_id_none_drops_macro(self):
        """`visitor_id=None` → safe_substitute drops the empty query param."""
        req = self._make_request(visitor_id=None)
        url = build_url(
            "https://example.com/?vid={visitor_id}&kept=1", req, "1", "101",
        )
        # Empty visitor_id query param dropped.
        assert "vid=" not in url
        assert "kept=1" in url

    def test_empty_query_params_macro_collapses(self):
        """`{source}` with no mapping no longer leaves a literal."""
        req = self._make_request(query_params={})
        url = build_url(
            "https://example.com/?s={source}", req, "1", "101",
        )
        # New behavior — literal `{source}` does NOT remain.
        assert "{source}" not in url
        # Empty query param was cleaned up.
        assert "s=" not in url

    def test_campaign_overrides_source_alias(self):
        """Campaign mapping wins per-slot for the URL-key lookup."""
        req = self._make_request(query_params={"gclid": "g123", "fbclid": "fb456"})
        url = build_url(
            "https://example.com/?clk={source_click_id}", req, "1", "101",
            source_mappings=[{"slot": "source_click_id", "alias": "fbclid"}],
            campaign_mappings=[{"slot": "source_click_id", "alias": "gclid"}],
        )
        # Campaign alias wins → `gclid` value lands in the slot.
        assert "clk=g123" in url
        assert "fb456" not in url


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

    def test_empty_query_params_no_literal_left(self):
        # Vector 2.8 — `{source}` macro with no mapping resolves to
        # NULL; safe_substitute drops the empty query param.
        req = ClickRequest(click_id="test", query_params={})
        url = build_url("https://example.com/?s={source}", req, "1", "101")
        assert "{source}" not in url
        assert "s=" not in url
        assert "example.com" in url

    def test_huge_query_params_rejected_at_boundary(self):
        """Resource-exhaustion cap (security audit MEDIUM-004) — the
        Pydantic boundary now rejects > 100 keys, so the legacy
        "thousands shouldn't crash" promise is enforced as
        boundary rejection rather than tolerant downstream handling.
        """
        from pydantic import ValidationError
        params = {f"param_{i}": f"value_{i}" for i in range(1000)}
        with pytest.raises(ValidationError):
            ClickRequest(click_id="test", query_params=params)

    def test_max_allowed_query_params_pass_through(self):
        """At the 100-key cap, build_url processes without crashing."""
        params = {f"param_{i}": f"value_{i}" for i in range(100)}
        req = ClickRequest(click_id="test", query_params=params)
        url = build_url("https://example.com/", req, "1", "101")
        assert "example.com" in url
