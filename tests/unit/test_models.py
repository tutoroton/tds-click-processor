"""Unit tests for Pydantic models — input validation and edge cases."""

import pytest
from pydantic import ValidationError
from app.models import ClickRequest, ClickResponse


class TestClickRequest:
    def test_minimal_valid(self):
        """Minimum required field: click_id."""
        req = ClickRequest(click_id="abc123")
        assert req.click_id == "abc123"
        assert req.country == ""
        assert req.is_returning is False

    def test_full_params(self):
        req = ClickRequest(
            click_id="019d1f413a0c",
            country="US",
            city="New York",
            ip="1.2.3.4",
            user_agent="iPhone Safari",
            query_params={"source": "fb", "sub1": "test"},
            visitor_id="vid123",
            is_returning=True,
        )
        assert req.country == "US"
        assert req.query_params["source"] == "fb"
        assert req.is_returning is True

    def test_null_visitor_id(self):
        """visitor_id can be None (new visitor)."""
        req = ClickRequest(click_id="test", visitor_id=None)
        assert req.visitor_id is None

    def test_empty_strings(self):
        req = ClickRequest(click_id="test", country="", city="", user_agent="")
        assert req.country == ""

    def test_special_chars_in_params(self):
        req = ClickRequest(
            click_id="test",
            query_params={"key": "<script>alert('xss')</script>", "sql": "'; DROP TABLE--"},
        )
        assert "<script>" in req.query_params["key"]


class TestClickIdValidation:
    """H4 fix: click_id field validation."""

    def test_valid_click_id_alphanumeric(self):
        req = ClickRequest(click_id="abc123")
        assert req.click_id == "abc123"

    def test_valid_click_id_with_hyphens_underscores(self):
        req = ClickRequest(click_id="019d1f41-3a0c-7b2e_test")
        assert req.click_id == "019d1f41-3a0c-7b2e_test"

    def test_valid_click_id_hex(self):
        """Typical hex click_id from Worker."""
        req = ClickRequest(click_id="019d1f413a0c7b2eaf3c")
        assert req.click_id == "019d1f413a0c7b2eaf3c"

    def test_click_id_max_length_128(self):
        req = ClickRequest(click_id="a" * 128)
        assert len(req.click_id) == 128

    def test_click_id_exceeds_max_length(self):
        with pytest.raises(ValidationError):
            ClickRequest(click_id="a" * 129)

    def test_click_id_empty_rejected(self):
        """Empty click_id should fail pattern validation."""
        with pytest.raises(ValidationError):
            ClickRequest(click_id="")

    def test_click_id_special_chars_rejected(self):
        """click_id with injection chars must be rejected."""
        with pytest.raises(ValidationError):
            ClickRequest(click_id="test<script>alert(1)</script>")

    def test_click_id_spaces_rejected(self):
        with pytest.raises(ValidationError):
            ClickRequest(click_id="test click")

    def test_click_id_url_chars_rejected(self):
        with pytest.raises(ValidationError):
            ClickRequest(click_id="test?param=value&x=1")


class TestVisitorIdValidation:
    """H4 fix: visitor_id field validation."""

    def test_valid_visitor_id(self):
        req = ClickRequest(click_id="test", visitor_id="vid_abc-123")
        assert req.visitor_id == "vid_abc-123"

    def test_visitor_id_none(self):
        req = ClickRequest(click_id="test", visitor_id=None)
        assert req.visitor_id is None

    def test_visitor_id_empty_string(self):
        """Empty string is valid for visitor_id (matches pattern)."""
        req = ClickRequest(click_id="test", visitor_id="")
        assert req.visitor_id == ""

    def test_visitor_id_max_length_128(self):
        req = ClickRequest(click_id="test", visitor_id="v" * 128)
        assert len(req.visitor_id) == 128

    def test_visitor_id_exceeds_max_length(self):
        with pytest.raises(ValidationError):
            ClickRequest(click_id="test", visitor_id="v" * 129)

    def test_visitor_id_special_chars_rejected(self):
        with pytest.raises(ValidationError):
            ClickRequest(click_id="test", visitor_id="vid<script>")


class TestIdentityTokenValidation:
    """SEC-LOW-01 (audit-2 2026-06-07): `identity_token` max_length raised
    512 → 1024 to match the worker `_validIdentityCookie` guard and stay
    strictly looser than the codec's largest verifiable token (~727 chars)."""

    @staticmethod
    def _tok(payload_len: int) -> str:
        # b64url(payload).b64url(sig) shape; sig is the trailing 43 chars (a
        # SHA-256 b64url-nopad sig is always 43). payload fills the rest minus
        # the dot separator. Charset matches the field pattern.
        sig = "s" * 43
        payload = "p" * (payload_len - 43 - 1)
        return f"{payload}.{sig}"

    def test_token_512_to_1024_now_passes(self):
        # 600-char token (512 < len <= 1024) — would have 422'd at the old 512
        # cap; now validates (matches the field pattern, under the 1024 bound).
        tok = self._tok(600)
        assert len(tok) == 600
        req = ClickRequest(click_id="test", identity_token=tok)
        assert req.identity_token == tok

    def test_token_at_1024_passes(self):
        tok = self._tok(1024)
        assert len(tok) == 1024
        req = ClickRequest(click_id="test", identity_token=tok)
        assert len(req.identity_token) == 1024

    def test_token_exceeds_1024_rejected(self):
        with pytest.raises(ValidationError):
            ClickRequest(click_id="test", identity_token=self._tok(1025))

    def test_token_none_ok(self):
        req = ClickRequest(click_id="test", identity_token=None)
        assert req.identity_token is None


class TestQueryParamsValidation:
    """Vector 2.8 — strict `dict[str, str]` query_params at the
    Pydantic boundary. Closes type-confusion class of bugs before
    they reach `resolve_slots` / `safe_substitute` (security audit
    2026-04-28 HIGH-001).
    """

    def test_string_values_pass(self):
        req = ClickRequest(
            click_id="t",
            query_params={"a": "1", "b": "two"},
        )
        assert req.query_params == {"a": "1", "b": "two"}

    def test_int_value_coerced(self):
        req = ClickRequest(click_id="t", query_params={"age": 42})
        assert req.query_params == {"age": "42"}

    def test_bool_value_lowercased(self):
        # Mirrors `macros._coerce_value` behaviour for cross-layer consistency.
        req = ClickRequest(click_id="t", query_params={"active": True})
        assert req.query_params == {"active": "true"}

    def test_float_value_coerced(self):
        req = ClickRequest(click_id="t", query_params={"price": 3.14})
        assert req.query_params == {"price": "3.14"}

    def test_none_value_dropped(self):
        # Treat null as absent — same as not sending the key.
        req = ClickRequest(click_id="t", query_params={"a": "1", "b": None})
        assert req.query_params == {"a": "1"}

    def test_list_value_rejected(self):
        with pytest.raises(ValidationError):
            ClickRequest(click_id="t", query_params={"k": ["a", "b"]})

    def test_dict_value_rejected(self):
        with pytest.raises(ValidationError):
            ClickRequest(click_id="t", query_params={"k": {"nested": "v"}})

    def test_non_string_key_rejected(self):
        with pytest.raises(ValidationError):
            ClickRequest(click_id="t", query_params={42: "v"})

    def test_empty_dict_default(self):
        req = ClickRequest(click_id="t")
        assert req.query_params == {}

    def test_null_query_params_becomes_empty(self):
        req = ClickRequest(click_id="t", query_params=None)
        assert req.query_params == {}

    def test_too_many_keys_rejected(self):
        # Resource-exhaustion cap: > 100 keys raises (security audit
        # MEDIUM-004). CF Worker / legitimate advertisers stay well
        # under 50 keys; 101+ is misconfiguration or DoS attempt.
        params = {f"k{i}": str(i) for i in range(101)}
        with pytest.raises(ValidationError):
            ClickRequest(click_id="t", query_params=params)

    def test_max_keys_accepted(self):
        # Exactly at cap: accepted.
        params = {f"k{i}": str(i) for i in range(100)}
        req = ClickRequest(click_id="t", query_params=params)
        assert len(req.query_params) == 100

    def test_oversized_value_truncated(self):
        # Long value truncated rather than rejected — preserves the
        # click while bounding storage cost.
        big = "a" * 5000
        req = ClickRequest(click_id="t", query_params={"k": big})
        assert len(req.query_params["k"]) == 1024
        assert req.query_params["k"] == "a" * 1024


class TestClickResponse:
    def test_basic(self):
        resp = ClickResponse(url="https://example.com/offer?cid=123")
        assert resp.status == 302
        assert "example.com" in resp.url

    def test_custom_status(self):
        resp = ClickResponse(url="https://example.com", status=301)
        assert resp.status == 301
