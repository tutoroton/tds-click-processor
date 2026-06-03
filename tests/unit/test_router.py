"""Unit tests for the routing engine.

Tests cover: device/OS/browser parsing, URL building, weighted selection,
parameter mapping, edge cases, and malicious inputs.
"""

import pytest
from unittest.mock import MagicMock

from app.router import (
    parse_device_type,
    parse_os,
    parse_browser,
    build_url,
    resolve_target,
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
    # F.17 (2026-05-03): browser values are now device_detector's
    # canonical Title Case names — same vocabulary admin-api validates
    # against (`KNOWN_BROWSERS` from the bundled taxonomy).
    def test_chrome(self):
        assert parse_browser("Mozilla/5.0 Chrome/120.0.6099.130 Safari/537.36") == "Chrome"

    def test_safari(self):
        assert parse_browser("Mozilla/5.0 (Macintosh) AppleWebKit/605.1.15 Safari/605.1") == "Safari"

    def test_firefox(self):
        assert parse_browser("Mozilla/5.0 (Windows; rv:121.0) Gecko/20100101 Firefox/121.0") == "Firefox"

    def test_edge(self):
        assert parse_browser("Mozilla/5.0 Chrome/120.0 Safari/537.36 Edg/120.0") == "Microsoft Edge"

    def test_opera(self):
        assert parse_browser("Mozilla/5.0 Chrome/120.0 Safari/537.36 OPR/106.0") == "Opera"

    def test_empty(self):
        assert parse_browser("") == "other"

    def test_bot(self):
        # Bots still flow through the same parser; they emit their
        # canonical name (e.g. `Googlebot`). is_bot=True in parse_ua
        # is the discriminator for downstream consumers that care.
        assert parse_browser("Googlebot/2.1") == "Googlebot"


# ============================================================
# F.17 — Accept-Language Parsing (parse_accept_language)
# ============================================================


class TestParseAcceptLanguage:
    """Per-F.17 user decision (2026-05-03): only the PRIMARY language
    counts for criterion matching. Secondary q-weighted languages do
    not — a Russian-primary user with `Accept-Language: ru-RU,en;q=0.9,uk;q=0.7`
    is RU even if they nominally read English / Ukrainian.

    Output casing follows BCP47: lowercase language, uppercase region.
    """

    # `pytest` import is already at the top of the file via parametrize
    # — keeping these as plain methods to match the surrounding test
    # style (no parametrize used elsewhere in the parsing tests).

    def test_simple_lang_only(self):
        from app.router import parse_accept_language
        assert parse_accept_language("en") == "en"

    def test_lang_with_region(self):
        from app.router import parse_accept_language
        assert parse_accept_language("en-US") == "en-US"

    def test_primary_wins_over_secondary(self):
        from app.router import parse_accept_language
        # Per the F.17 contract — Russian is primary, not Ukrainian.
        assert parse_accept_language("ru-RU,en;q=0.9,uk;q=0.7") == "ru-RU"

    def test_strips_q_weight_on_primary(self):
        from app.router import parse_accept_language
        assert parse_accept_language("uk-UA;q=1.0,en;q=0.9") == "uk-UA"

    def test_normalizes_casing(self):
        from app.router import parse_accept_language
        # Browsers occasionally ship `EN-us` / `en_US` style — we
        # canonicalize lower-Upper for the language tag, anything
        # else falls through.
        assert parse_accept_language("EN-us") == "en-US"

    def test_lang_only_when_region_malformed(self):
        from app.router import parse_accept_language
        # 3-letter region per BCP47 is rare-but-valid (e.g. spanish
        # `es-419` for Latin America). Our criterion validator
        # accepts only 2-letter regions, so we degrade to lang-only
        # for now — operator can target `es` and catch the parent.
        assert parse_accept_language("es-419") == "es"

    def test_empty_header(self):
        from app.router import parse_accept_language
        assert parse_accept_language("") == ""

    def test_none_header(self):
        from app.router import parse_accept_language
        assert parse_accept_language(None) == ""

    def test_garbage_yields_empty(self):
        from app.router import parse_accept_language
        assert parse_accept_language("*") == ""
        assert parse_accept_language("a") == ""        # 1-char lang
        assert parse_accept_language("english") == ""  # non-2-char


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

    def test_source_overrides_campaign_alias(self):
        """SOURCE-WINS (2026-06-02): source mapping wins per-slot for the
        URL-key lookup — the source specializes the campaign."""
        req = self._make_request(query_params={"gclid": "g123", "fbclid": "fb456"})
        url = build_url(
            "https://example.com/?clk={source_click_id}", req, "1", "101",
            source_mappings=[{"slot": "source_click_id", "alias": "fbclid"}],
            campaign_mappings=[{"slot": "source_click_id", "alias": "gclid"}],
        )
        # Source alias wins → `fbclid` value lands in the slot.
        assert "clk=fb456" in url
        assert "g123" not in url


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


# ============================================================
# Audit 2026-06-03 regression fence — legacy resolve_target
# ============================================================
#
# `resolve_target` is the offer-target picker on the cascade-MISS
# (legacy) path: when `cascade.resolve_flow` returns None, route()
# falls to `select_offer` → `resolve_target`. It was entirely
# unit-untested before this fence. Pins: no-targets passthrough,
# empty-criteria match-all, criteria selection, is_default fallback,
# the B4 "no match + no default → None" gap, and the B9 priority
# tie-break (priority DESC, then LEXICOGRAPHIC offer_target id —
# NOT numeric, unlike `action_executor._safe_target_sort_key`; the
# two paths intentionally diverge here, documented in P2-results.md).
# Mutation-checked — see P2-results.md.


def _ot_redis(target_sets: dict[str, set], target_hashes: dict[str, dict]) -> MagicMock:
    """Mock Redis for `resolve_target`: smembers + pipeline.hgetall."""

    class FakePipeline:
        def __init__(self):
            self._keys: list[str] = []

        def hgetall(self, key):
            self._keys.append(key)

        async def execute(self):
            return [dict(target_hashes.get(k, {})) for k in self._keys]

    async def _smembers(key):
        return set(target_sets.get(key, set()))

    redis = MagicMock()
    redis.smembers = _smembers
    redis.pipeline = lambda: FakePipeline()
    return redis


def _ot_click(country="US", region="", city="", ua="Mozilla/5.0 (iPhone)", al="en-US"):
    return ClickRequest(
        click_id="t-1", country=country, region=region, city=city,
        user_agent=ua, accept_language=al, query_params={},
    )


class TestResolveTargetLegacy:
    @pytest.mark.asyncio
    async def test_offer_without_targets_returns_none(self):
        offer = {"_id": "5", "has_targets": "0"}
        url = await resolve_target(_ot_redis({}, {}), offer, _ot_click())
        assert url is None

    @pytest.mark.asyncio
    async def test_empty_targets_set_returns_none(self):
        offer = {"_id": "5", "has_targets": "1"}
        r = _ot_redis({"offer:5:targets": set()}, {})
        assert await resolve_target(r, offer, _ot_click()) is None

    @pytest.mark.asyncio
    async def test_empty_criteria_matches_all(self):
        offer = {"_id": "5", "has_targets": "1"}
        r = _ot_redis(
            {"offer:5:targets": {"7"}},
            {"offer_target:7": {"url": "https://match-all", "criteria": "[]",
                                "priority": "0", "is_default": "0"}},
        )
        assert await resolve_target(r, offer, _ot_click()) == "https://match-all"

    @pytest.mark.asyncio
    async def test_criteria_selects_matching_target(self):
        offer = {"_id": "5", "has_targets": "1"}
        r = _ot_redis(
            {"offer:5:targets": {"7", "8"}},
            {
                "offer_target:7": {
                    "url": "https://us", "priority": "10", "is_default": "0",
                    "criteria": '[{"type":"geo","op":"in","values":["US"]}]',
                },
                "offer_target:8": {
                    "url": "https://ru", "priority": "5", "is_default": "0",
                    "criteria": '[{"type":"geo","op":"in","values":["RU"]}]',
                },
            },
        )
        # US click → target 7 matches (and is higher priority anyway).
        assert await resolve_target(r, offer, _ot_click(country="US")) == "https://us"

    @pytest.mark.asyncio
    async def test_no_criteria_match_uses_is_default(self):
        offer = {"_id": "5", "has_targets": "1"}
        r = _ot_redis(
            {"offer:5:targets": {"7", "9"}},
            {
                "offer_target:7": {
                    "url": "https://ru-only", "priority": "10", "is_default": "0",
                    "criteria": '[{"type":"geo","op":"in","values":["RU"]}]',
                },
                "offer_target:9": {
                    "url": "https://default", "priority": "1", "is_default": "1",
                    "criteria": '[{"type":"geo","op":"in","values":["DE"]}]',
                },
            },
        )
        # US click matches neither geo criterion → fall to is_default target.
        assert await resolve_target(r, offer, _ot_click(country="US")) == "https://default"

    @pytest.mark.asyncio
    async def test_b4_no_match_no_default_returns_none(self):
        # B4 (legacy path): targets exist, none match the click, and NONE
        # is is_default → resolve_target returns None. route() then falls
        # back to `offer.get("url")`; if that's empty too the click has no
        # target. Pins that a non-matching, non-default target is never
        # returned as a silent fallback.
        offer = {"_id": "5", "has_targets": "1"}
        r = _ot_redis(
            {"offer:5:targets": {"7"}},
            {"offer_target:7": {
                "url": "https://ru-only", "priority": "10", "is_default": "0",
                "criteria": '[{"type":"geo","op":"in","values":["RU"]}]',
            }},
        )
        assert await resolve_target(r, offer, _ot_click(country="US")) is None

    @pytest.mark.asyncio
    async def test_b9_priority_then_lexicographic_id_tiebreak(self):
        # B9: two equal-priority match-all targets with ids "2" and "10".
        # Input order is `sorted(target_ids)` = lexicographic = ["10","2"];
        # the stable priority-DESC sort preserves it, so the FIRST
        # match-all target iterated ("10") wins. This pins the
        # lexicographic (NOT numeric) tie-break: a future change to
        # numeric ordering (which would pick "2") fails this test.
        offer = {"_id": "5", "has_targets": "1"}
        r = _ot_redis(
            {"offer:5:targets": {"2", "10"}},
            {
                "offer_target:2": {"url": "https://id-2", "priority": "5",
                                   "is_default": "0", "criteria": "[]"},
                "offer_target:10": {"url": "https://id-10", "priority": "5",
                                    "is_default": "0", "criteria": "[]"},
            },
        )
        assert await resolve_target(r, offer, _ot_click()) == "https://id-10"
