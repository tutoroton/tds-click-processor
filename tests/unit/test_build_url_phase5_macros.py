"""Tests for the T2.5 / Phase 5 macro wiring closures.

Closes the gap between the canonical 70-macro landing-context list
documented in `docs/roadmap/stage-1a-research/macros-registry.md`
and what `router.build_url` actually populated. Six macros that
previously substituted to empty are now populated:

  * `os_version` — UA-parsed (already emitted by `parse_ua`,
    just wired into the values dict)
  * `browser_version` — UA-parsed (same)
  * `language` — primary BCP47 tag from Accept-Language header
  * `cost` — advertiser-supplied per-click cost from query_params
  * `offer_target_id` — pinned/fallback target id from action_executor
  * `flow_id` — winning flow id from cascade.resolve_flow

Reference: `docs/roadmap/stage-1a-research/macros-registry.md`,
action-items.md T2.5, open-questions.md G-05.
"""

from __future__ import annotations

import inspect

import pytest

from app.models import ClickRequest
from app.router import build_url


def _req(**overrides) -> ClickRequest:
    """Build a sensible-defaults ClickRequest for macro tests.

    Only override what the specific test cares about — every other
    field gets a non-empty value so a missing-field bug surfaces
    as the wrong macro substitution, not a None deref."""
    defaults = dict(
        click_id="click-abc",
        country="US",
        city="New York",
        region="NY",
        ip="1.2.3.4",
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        accept_language="uk-UA,uk;q=0.9,en;q=0.8",
        query_params={"source": "fb", "cost": "0.42"},
    )
    defaults.update(overrides)
    return ClickRequest(**defaults)


# ---------------------------------------------------------------------------
# UA-parsed: os_version + browser_version
# ---------------------------------------------------------------------------


class TestUaVersionMacros:
    """`{os_version}` + `{browser_version}` complete the
    UA_PARSED_SLOTS frozenset (5 names: os, os_version, browser,
    browser_version, device_type). `parse_ua` already emits these
    fields — T2.5 just wires them into the values dict."""

    def test_os_version_substitutes(self):
        # Chrome on Windows 10 — `parse_ua` returns os_version="10".
        url = build_url(
            "https://x.example/?osver={os_version}",
            _req(),
            "1", "101",
        )
        # Pin only the prefix — minor device_detector versions may
        # tweak the trailing zeros.
        assert "osver=10" in url

    def test_browser_version_substitutes(self):
        url = build_url(
            "https://x.example/?bver={browser_version}",
            _req(),
            "1", "101",
        )
        # device_detector returns "120.0.0.0" for the UA above.
        assert "bver=120" in url

    def test_unknown_ua_collapses_versions_cleanly(self):
        """An empty / unknown UA → parse_ua returns "" for versions
        → values dict gets None → safe_substitute drops the param."""
        url = build_url(
            "https://x.example/?osver={os_version}&keep=1",
            _req(user_agent=""),
            "1", "101",
        )
        # `osver=` should be cleaned up, `keep=1` survives.
        assert "osver=" not in url
        assert "keep=1" in url


# ---------------------------------------------------------------------------
# Substituted-auto: language + cost
# ---------------------------------------------------------------------------


class TestLanguageMacro:
    def test_uk_primary_substitutes(self):
        url = build_url(
            "https://x.example/?lang={language}",
            _req(accept_language="uk-UA,uk;q=0.9,en;q=0.8"),
            "1", "101",
        )
        # Per F.17: only the FIRST listed language counts. q-weighted
        # secondaries are ignored.
        assert "lang=uk-UA" in url

    def test_no_accept_language_drops_param(self):
        url = build_url(
            "https://x.example/?lang={language}&keep=1",
            _req(accept_language=""),
            "1", "101",
        )
        assert "lang=" not in url
        assert "keep=1" in url

    def test_language_only_no_region(self):
        url = build_url(
            "https://x.example/?lang={language}",
            _req(accept_language="en;q=0.9"),
            "1", "101",
        )
        assert "lang=en" in url


class TestCostMacro:
    def test_cost_from_query_params(self):
        url = build_url(
            "https://x.example/?c={cost}",
            _req(query_params={"cost": "0.85"}),
            "1", "101",
        )
        # Decimal-string passes through verbatim — encoder leaves
        # ASCII digits alone.
        assert "c=0.85" in url

    def test_cost_missing_drops_param(self):
        """No `?cost=` in incoming URL → macro collapses, query
        cleanup drops the empty `c=` pair."""
        url = build_url(
            "https://x.example/?c={cost}&keep=1",
            _req(query_params={}),
            "1", "101",
        )
        assert "c=" not in url
        assert "keep=1" in url

    def test_cost_empty_string_drops_param(self):
        """An explicit `?cost=` (empty value) is treated the same
        as absent — empty string → None at the values dict, dropped
        by safe_substitute."""
        url = build_url(
            "https://x.example/?c={cost}&keep=1",
            _req(query_params={"cost": ""}),
            "1", "101",
        )
        assert "c=" not in url
        assert "keep=1" in url

    def test_cost_special_chars_url_encoded(self):
        """Even if cost is a malformed string with special chars,
        it must be URL-encoded — never break the URL syntactically.
        Defense vs misbehaving traffic source.

        Audit fix 2026-05-09 (Agent 4 minor): the prior assertion
        used `or "%3D" in url` which made it pass on any URL
        containing `%3D` anywhere — even if the cost segment
        itself was un-encoded. Replaced with two unconditional
        per-segment checks.
        """
        url = build_url(
            "https://x.example/?c={cost}",
            _req(query_params={"cost": "a&b=evil"}),
            "1", "101",
        )
        # & and = should be percent-encoded; payload becomes
        # `c=a%26b%3Devil`. Slice the cost segment and verify
        # the dangerous shape is GONE from THAT segment, not
        # from any random part of the URL.
        cost_segment = url.split("c=")[1].split("&")[0]
        assert "%3D" in cost_segment, (
            f"`=` in cost value MUST be URL-encoded as %3D in the "
            f"cost segment specifically; got {cost_segment!r}"
        )
        assert "%26" in cost_segment, (
            f"`&` in cost value MUST be URL-encoded as %26 in the "
            f"cost segment specifically; got {cost_segment!r}"
        )
        # The full encoded value as a positive sanity check.
        assert "c=a%26b%3Devil" in url


# ---------------------------------------------------------------------------
# Technical: offer_target_id + flow_id
# ---------------------------------------------------------------------------


class TestTargetAndFlowIdMacros:
    def test_offer_target_id_substitutes(self):
        url = build_url(
            "https://x.example/?ot={offer_target_id}",
            _req(),
            "1", "101",
            target_id="42",
        )
        assert "ot=42" in url

    def test_offer_target_id_missing_drops(self):
        url = build_url(
            "https://x.example/?ot={offer_target_id}&keep=1",
            _req(),
            "1", "101",
            # target_id omitted (default None — redirect-action path)
        )
        assert "ot=" not in url
        assert "keep=1" in url

    def test_flow_id_substitutes(self):
        url = build_url(
            "https://x.example/?fid={flow_id}",
            _req(),
            "1", "101",
            flow_id="7",
        )
        assert "fid=7" in url

    def test_flow_id_missing_drops(self):
        url = build_url(
            "https://x.example/?fid={flow_id}&keep=1",
            _req(),
            "1", "101",
        )
        assert "fid=" not in url
        assert "keep=1" in url

    def test_int_target_id_coerced_to_str(self):
        """Caller may pass the id as int (e.g. directly from a
        Redis HASH that wasn't pre-stringified). build_url
        defensively str()s — no TypeError downstream."""
        url = build_url(
            "https://x.example/?ot={offer_target_id}",
            _req(),
            "1", "101",
            target_id=42,  # int, not str
        )
        assert "ot=42" in url


# ---------------------------------------------------------------------------
# Cumulative pin: full 70-macro contract
# ---------------------------------------------------------------------------


class TestFullSetIntegration:
    """One template that exercises ALL 6 newly-wired macros at once.
    A regression that drops any one of them surfaces here as a
    missing query param — clear signal."""

    def test_all_t25_macros_in_one_url(self):
        url = build_url(
            "https://x.example/?osver={os_version}&bver={browser_version}"
            "&lang={language}&cost={cost}"
            "&ot={offer_target_id}&fid={flow_id}",
            _req(),
            "1", "101",
            target_id="55",
            flow_id="9",
        )
        for needed in (
            "osver=10",       # UA → os_version
            "bver=120",       # UA → browser_version
            "lang=uk-UA",     # accept_language primary tag
            "cost=0.42",      # query_params['cost']
            "ot=55",          # threaded target_id
            "fid=9",          # threaded flow_id
        ):
            assert needed in url, f"Missing {needed!r} in {url!r}"

    def test_pre_existing_macros_unbroken(self):
        """Defense vs regression — the pre-T2.5 macros (click_id,
        campaign_id, country, city, sub1, etc.) MUST still
        substitute correctly. T2.5 is additive only."""
        url = build_url(
            "https://x.example/?cid={click_id}&camp={campaign_id}"
            "&oid={offer_id}&country={country}&city={city}"
            "&os={os}&browser={browser}",
            _req(),
            "1", "101",
        )
        for needed in (
            "cid=click-abc",
            "camp=1",
            "oid=101",
            "country=US",
            "city=New%20York",
            "os=windows",
            "browser=Chrome",
        ):
            assert needed in url, f"Missing {needed!r} in {url!r}"


# ---------------------------------------------------------------------------
# Source-pin: build_url signature
# ---------------------------------------------------------------------------


class TestSignaturePin:
    """If a future refactor drops or renames target_id / flow_id
    kwargs, action_executor's call shape silently breaks here. Pin
    the signature."""

    def test_build_url_accepts_target_and_flow_kwargs(self):
        sig = inspect.signature(build_url)
        params = sig.parameters
        assert "target_id" in params, (
            "build_url MUST accept `target_id` kwarg (T2.5)."
        )
        assert "flow_id" in params, (
            "build_url MUST accept `flow_id` kwarg (T2.5)."
        )
        # Both must default to None — caller may omit (redirect
        # action has no offer/target).
        assert params["target_id"].default is None
        assert params["flow_id"].default is None
