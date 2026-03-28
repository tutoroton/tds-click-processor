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


class TestClickResponse:
    def test_basic(self):
        resp = ClickResponse(url="https://example.com/offer?cid=123")
        assert resp.status == 302
        assert "example.com" in resp.url

    def test_custom_status(self):
        resp = ClickResponse(url="https://example.com", status=301)
        assert resp.status == 301
