"""Unit tests for `app.macros.safe_substitute` — the URL builder
that gives M4 (`docs/roadmap/stage-1a-research/macros-registry.md`)
its safe-output guarantee.

User constraint (verbatim): "потрібно безпечне рішення, яке не буде
ламати маршрутизацію". The substituter MUST emit a syntactically
valid URL even when macros resolve to NULL. These tests pin every
worked example from the macros stub plus edge cases that surfaced
while writing the helper.
"""

from __future__ import annotations

import pytest

from app.macros import safe_substitute, _cleanup_url


# ============================================================
# Worked examples from macros-registry.md
# ============================================================

class TestWorkedExamplesFromResearchStub:
    """Each test mirrors an example block in `macros-registry.md` —
    if the doc and the code drift, these tests fire.
    """

    def test_example_1_query_string_drops_nulls(self):
        url = safe_substitute(
            "https://landing.com/?gclid={source_click_id}&geo={country}&buyer={buyer_id}",
            {"country": "US", "source_click_id": None, "buyer_id": None},
        )
        assert url == "https://landing.com/?geo=US"

    def test_example_2_path_collapses_null_segment(self):
        url = safe_substitute(
            "https://landing.com/{country}/{funnel_type}/page",
            {"country": "US", "funnel_type": None},
        )
        assert url == "https://landing.com/US/page"

    def test_example_3_multiple_null_path_segments(self):
        url = safe_substitute(
            "https://landing.com/{country}/{region}/{city}/checkout",
            {"country": "US", "region": None, "city": None},
        )
        assert url == "https://landing.com/US/checkout"

    def test_example_4_trailing_path_macro_dropped(self):
        url = safe_substitute(
            "https://landing.com/{country}/{funnel_type}",
            {"country": "US"},
        )
        assert url == "https://landing.com/US"

    def test_example_5_scheme_double_slash_preserved(self):
        url = safe_substitute(
            "https://{host}/{country}/page",
            {"host": "landing.com", "country": "US"},
        )
        assert url == "https://landing.com/US/page"


# ============================================================
# URL encoding (M5 contract)
# ============================================================

class TestUrlEncoding:
    def test_special_chars_in_value_are_encoded(self):
        url = safe_substitute(
            "https://landing.com/?q={keyword}",
            {"keyword": "Smith & Co"},
        )
        assert url == "https://landing.com/?q=Smith%20%26%20Co"

    def test_path_separator_in_value_encoded(self):
        # `safe=''` to `quote` means even `/` is escaped — required
        # because `sub` and `extras` are arbitrary user input that
        # would otherwise break URL structure.
        url = safe_substitute(
            "https://landing.com/?ref={referrer}",
            {"referrer": "path/to/page"},
        )
        assert url == "https://landing.com/?ref=path%2Fto%2Fpage"

    def test_unicode_value_encoded(self):
        url = safe_substitute(
            "https://landing.com/?q={keyword}",
            {"keyword": "привіт"},
        )
        assert url == (
            "https://landing.com/?q=%D0%BF%D1%80%D0%B8%D0%B2%D1%96%D1%82"
        )

    def test_numeric_value_pass_through(self):
        url = safe_substitute(
            "https://landing.com/?id={click_id}",
            {"click_id": 12345},
        )
        assert url == "https://landing.com/?id=12345"


# ============================================================
# Empty / unknown macros
# ============================================================

class TestNullSemantics:
    def test_explicit_none_drops_query_param(self):
        url = safe_substitute(
            "https://landing.com/?a={a}&b={b}",
            {"a": "1", "b": None},
        )
        assert url == "https://landing.com/?a=1"

    def test_empty_string_drops_query_param(self):
        # Empty string treated identically to None — there's no
        # meaningful "empty value" use case in adtech URL templates.
        url = safe_substitute(
            "https://landing.com/?a={a}&b={b}",
            {"a": "1", "b": ""},
        )
        assert url == "https://landing.com/?a=1"

    def test_missing_key_drops_query_param(self):
        # Macro present in template but key missing from values
        # dict — same NULL semantics.
        url = safe_substitute(
            "https://landing.com/?a={a}&b={b}",
            {"a": "1"},  # `b` not provided
        )
        assert url == "https://landing.com/?a=1"

    def test_all_query_params_null_strips_question_mark(self):
        url = safe_substitute(
            "https://landing.com/?a={a}&b={b}",
            {"a": None, "b": None},
        )
        assert url == "https://landing.com/"

    def test_no_macros_in_template_unchanged(self):
        url = safe_substitute(
            "https://landing.com/static/page?x=1",
            {"unused": "value"},
        )
        assert url == "https://landing.com/static/page?x=1"


# ============================================================
# Path cleanup edge cases
# ============================================================

class TestPathCleanup:
    def test_consecutive_path_slashes_collapsed(self):
        # Direct `_cleanup_url` test — same logic safe_substitute
        # applies, but exercised on a pre-substituted URL.
        assert _cleanup_url("https://landing.com//foo//bar") == \
               "https://landing.com/foo/bar"

    def test_triple_slash_collapsed(self):
        assert _cleanup_url("https://landing.com///foo") == \
               "https://landing.com/foo"

    def test_root_path_preserved(self):
        # "https://host/" should NOT lose the root slash — landings
        # often configure that as the homepage route.
        assert _cleanup_url("https://landing.com/") == \
               "https://landing.com/"

    def test_bare_host_no_slash(self):
        # Pre-substituted URL with no path — leave alone.
        assert _cleanup_url("https://landing.com") == \
               "https://landing.com"

    def test_query_string_not_affected_by_path_cleanup(self):
        # `//` inside query string should NOT collapse (it's a
        # value, not a path separator).
        assert _cleanup_url("https://landing.com/?url=https%3A%2F%2Fother.com") == \
               "https://landing.com/?url=https%3A%2F%2Fother.com"


# ============================================================
# Query cleanup edge cases
# ============================================================

class TestQueryCleanup:
    def test_leading_empty_param(self):
        # `?a=&b=2` → `?b=2`
        assert _cleanup_url("https://h.com/?a=&b=2") == "https://h.com/?b=2"

    def test_trailing_empty_param(self):
        # `?a=1&b=` → `?a=1`
        assert _cleanup_url("https://h.com/?a=1&b=") == "https://h.com/?a=1"

    def test_middle_empty_param(self):
        # `?a=1&b=&c=3` → `?a=1&c=3`
        assert _cleanup_url("https://h.com/?a=1&b=&c=3") == \
               "https://h.com/?a=1&c=3"

    def test_consecutive_empty_params(self):
        # `?a=&b=&c=3` → `?c=3` (loop until stable)
        assert _cleanup_url("https://h.com/?a=&b=&c=3") == \
               "https://h.com/?c=3"

    def test_only_empty_params(self):
        # `?a=&b=&c=` → strip query entirely.
        assert _cleanup_url("https://h.com/?a=&b=&c=") == "https://h.com/"

    def test_fragment_preserved(self):
        url = safe_substitute(
            "https://landing.com/?a={a}#section1",
            {"a": "1"},
        )
        assert url == "https://landing.com/?a=1#section1"

    def test_fragment_with_dropped_query(self):
        url = safe_substitute(
            "https://landing.com/?a={a}#section1",
            {"a": None},
        )
        assert url == "https://landing.com/#section1"


# ============================================================
# Robustness — never leak `{macro}` literals
# ============================================================

class TestNoLiteralLeak:
    @pytest.mark.parametrize("template,values", [
        ("https://h.com/?x={a}", {}),
        ("https://h.com/?x={a}", {"a": None}),
        ("https://h.com/{a}/{b}/{c}", {}),
        ("https://h.com/{a}/{b}/{c}", {"a": None, "b": "", "c": None}),
        ("https://h.com/?x={a}&y={b}&z={c}", {}),
    ])
    def test_no_braces_in_output(self, template: str, values: dict):
        url = safe_substitute(template, values)
        assert "{" not in url
        assert "}" not in url

    def test_template_without_macros_unchanged(self):
        # Edge case: template has braces from JSON / templating that
        # aren't valid macros (uppercase, special chars). The macro
        # regex only matches `{lowercase_with_digits_and_underscores}`,
        # so JSON literals stay intact.
        url = safe_substitute(
            "https://h.com/?config=%7B%22key%22%3A%22value%22%7D",
            {},
        )
        # Encoded braces (`%7B`/`%7D`) untouched.
        assert "%7B" in url
        assert "%7D" in url


# ============================================================
# Realistic landing URL examples
# ============================================================

class TestRealisticLandings:
    def test_full_landing_with_all_macros_filled(self):
        url = safe_substitute(
            "https://offer.com/?cid={click_id}&src={source}"
            "&geo={country}&os={os}&kw={keyword}",
            {
                "click_id": "abc123",
                "source": "facebook",
                "country": "US",
                "os": "ios",
                "keyword": "casino",
            },
        )
        assert url == (
            "https://offer.com/?cid=abc123&src=facebook"
            "&geo=US&os=ios&kw=casino"
        )

    def test_landing_with_partial_resolution(self):
        # Realistic: advertiser-supplied `gclid` missing because click
        # came from a non-Google source; we still want a clean URL.
        url = safe_substitute(
            "https://offer.com/?cid={click_id}&gclid={source_click_id}"
            "&fbclid={source_click_id}&geo={country}",
            {
                "click_id": "abc123",
                "source_click_id": None,
                "country": "US",
            },
        )
        assert url == "https://offer.com/?cid=abc123&geo=US"

    def test_postback_url(self):
        # Outbound postback (Stage 4 reuses safe_substitute) — same
        # logic, different macro set.
        url = safe_substitute(
            "https://network.com/postback?cid={click_id}"
            "&payout={payout}&txid={transaction_id}",
            {
                "click_id": "abc123",
                "payout": "12.50",
                "transaction_id": "tx_42",
            },
        )
        assert url == (
            "https://network.com/postback?cid=abc123"
            "&payout=12.50&txid=tx_42"
        )

    def test_postback_url_no_transaction_id(self):
        # Some networks omit transaction_id — postback should still
        # fire with cleaned URL.
        url = safe_substitute(
            "https://network.com/postback?cid={click_id}"
            "&payout={payout}&txid={transaction_id}",
            {"click_id": "abc123", "payout": "12.50", "transaction_id": None},
        )
        assert url == "https://network.com/postback?cid=abc123&payout=12.50"


# ============================================================
# Determinism
# ============================================================

class TestDeterminism:
    def test_same_input_same_output(self):
        template = "https://h.com/{a}?b={c}&d={e}"
        values = {"a": "x", "c": "y", "e": None}
        assert safe_substitute(template, values) == safe_substitute(template, values)


# ============================================================
# Type guard — non-scalar values rejected (security regression)
# ============================================================

class TestNonScalarValuesRejected:
    """Per security audit + code review 2026-04-28 — list / dict /
    bytes values must NOT silently coerce via `str()` because that
    leaks Python repr (`"[1, 2, 3]"`, `"b'abc'"`) into URLs.
    """

    @pytest.mark.parametrize("bad_value", [
        ["US", "CA"],          # list
        {"k": "v"},            # dict
        b"bytes",              # bytes
        ("a", "b"),            # tuple
        object(),              # arbitrary object
    ])
    def test_raises_type_error(self, bad_value):
        with pytest.raises(TypeError):
            safe_substitute("?x={a}", {"a": bad_value})

    def test_int_passes(self):
        assert safe_substitute("?id={click_id}", {"click_id": 12345}) == \
               "?id=12345"

    def test_float_passes(self):
        assert safe_substitute("?p={payout}", {"payout": 12.5}) == \
               "?p=12.5"

    def test_bool_normalised(self):
        # bool is a subclass of int — must be handled BEFORE int
        # check or `True` would become `"1"` instead of `"true"`.
        assert safe_substitute("?f={flag}", {"flag": True}) == "?f=true"
        assert safe_substitute("?f={flag}", {"flag": False}) == "?f=false"

    def test_zero_value_substituted(self):
        # `0` is not None, not "" — must substitute as "0", NOT drop.
        assert safe_substitute("?p={payout}", {"payout": 0}) == "?p=0"


# ============================================================
# Adversarial input — bounded macro length + ReDoS safety
# ============================================================

class TestAdversarialInput:
    def test_long_macro_name_rejected_silently(self):
        # `_MACRO_RE` quantifier is `[a-z0-9_]{0,99}` — a 200-char
        # macro name does NOT match the regex, so it leaks through
        # the template unchanged. That's safe (no substitution
        # attempted) but signals operator error.
        long_name = "a" * 200
        url = safe_substitute(f"https://h.com/?x={{{long_name}}}", {})
        # The template's literal `{aaaa...}` is preserved (uppercase
        # rules don't apply because all `a` lowercase, but the
        # length cap rejects the whole macro pattern). Output
        # contains the literal braces.
        assert "{" in url and "}" in url

    def test_uppercase_macro_not_substituted(self):
        # Macros must be lowercase (registry convention).
        url = safe_substitute("https://h.com/{COUNTRY}", {"COUNTRY": "US"})
        # Literal `{COUNTRY}` survives because regex requires `[a-z]`.
        assert url == "https://h.com/{COUNTRY}"

    def test_macro_starting_with_digit_not_substituted(self):
        url = safe_substitute("https://h.com/{1bad}", {"1bad": "value"})
        assert url == "https://h.com/{1bad}"


# ============================================================
# Realistic edge cases — host:port, IPv6, empty template
# ============================================================

class TestRealisticEdgeCases:
    def test_host_with_port(self):
        # Tracking endpoints frequently use non-standard ports.
        url = safe_substitute(
            "https://tracking.net:8080/cb?cid={click_id}",
            {"click_id": None},
        )
        assert url == "https://tracking.net:8080/cb"

    def test_ipv6_literal(self):
        # Internal node-local URLs may use IPv6.
        url = safe_substitute(
            "https://[::1]:8100/decide?cid={click_id}",
            {"click_id": "abc"},
        )
        assert url == "https://[::1]:8100/decide?cid=abc"

    def test_empty_template(self):
        # Defensive — caller should validate non-empty, but the
        # helper must not crash.
        assert safe_substitute("", {"a": "1"}) == ""

    def test_duplicate_macro_both_dropped_when_null(self):
        # Same macro reused → both query params drop on NULL.
        url = safe_substitute(
            "?gclid={src_click_id}&fbclid={src_click_id}",
            {"src_click_id": None},
        )
        assert url == ""

    def test_duplicate_macro_both_substituted_when_present(self):
        url = safe_substitute(
            "?gclid={src_click_id}&fbclid={src_click_id}",
            {"src_click_id": "abc"},
        )
        assert url == "?gclid=abc&fbclid=abc"


class TestMacroValueLengthCap:
    """Defense-in-depth — security audit 2026-04-28 (MEDIUM-002).

    A single macro value over 4 KB is truncated rather than embedded
    in the redirect URL. Real RESERVED_SLOTS use cases stay well
    under this cap (gclid ~50, fbclid ~150, keyword free-text ~200).
    """

    def test_value_under_cap_unchanged(self):
        from app.macros import _MAX_MACRO_VALUE_LENGTH
        value = "a" * (_MAX_MACRO_VALUE_LENGTH - 1)
        url = safe_substitute("?x={k}", {"k": value})
        # The value is URL-safe ASCII so encoded length == raw length.
        assert f"x={value}" in url

    def test_value_at_cap_unchanged(self):
        from app.macros import _MAX_MACRO_VALUE_LENGTH
        value = "a" * _MAX_MACRO_VALUE_LENGTH
        url = safe_substitute("?x={k}", {"k": value})
        assert f"x={value}" in url

    def test_value_over_cap_truncated(self):
        from app.macros import _MAX_MACRO_VALUE_LENGTH
        value = "a" * (_MAX_MACRO_VALUE_LENGTH + 1000)
        url = safe_substitute("?x={k}", {"k": value})
        truncated = "a" * _MAX_MACRO_VALUE_LENGTH
        assert f"x={truncated}" in url
        # Original (longer) value MUST NOT appear in full.
        assert f"x={value}" not in url

    def test_huge_value_doesnt_crash(self):
        # 10 MB string — must not blow stack or eat 100ms.
        from app.macros import _MAX_MACRO_VALUE_LENGTH
        value = "x" * (10 * 1024 * 1024)
        url = safe_substitute("?k={macro}", {"macro": value})
        # Result is bounded to ~4 KB even though input was 10 MB.
        assert len(url) < _MAX_MACRO_VALUE_LENGTH + 200
