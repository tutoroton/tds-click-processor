"""Tests for H6 fix — `_check_tds_key` fails closed.

Legacy pattern: `if settings.tds_secret_key and (not x_tds_key or
not hmac.compare_digest(...)): raise`. The leading `and` short-
circuits to "no auth check" when `settings.tds_secret_key == ""`.

In production this was unreachable (startup guard refuses empty
key in non-local environments). But in local/development, an
operator running on a non-isolated network was exposed.

H6 fix: invert to `if not (provided and stored and compare_digest):
raise`. Empty stored secret now means EVERY request is rejected.

Tests:
  - Valid key passes
  - Wrong key 403
  - Missing header (empty x_tds_key) 403 even when stored is set
  - Empty stored secret 403 even when client sends a value (H6 closure)
  - Empty BOTH 403 (no surprise edge case)
  - Timing-safe via hmac.compare_digest (regression fence in
    test_admin_auth_timing_safe.py covers the source pin; this
    file covers behaviour).
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi import HTTPException


class TestCheckTdsKey:
    """Behaviour tests for the shared auth helper."""

    def test_valid_key_passes(self):
        from app import main

        with patch.object(main.settings, "tds_secret_key", "the-shared-secret-32chars-long-aa"):
            # Should not raise.
            main._check_tds_key("the-shared-secret-32chars-long-aa")

    def test_wrong_key_403(self):
        from app import main

        with patch.object(main.settings, "tds_secret_key", "stored-secret"):
            with pytest.raises(HTTPException) as exc_info:
                main._check_tds_key("wrong-secret")
            assert exc_info.value.status_code == 403

    def test_missing_header_403_when_stored_set(self):
        """Empty incoming `X-TDS-Key` header MUST 403 even when
        the stored secret is set (pre-H6 case — already worked but
        we pin it as a regression fence)."""
        from app import main

        with patch.object(main.settings, "tds_secret_key", "stored-secret"):
            with pytest.raises(HTTPException) as exc_info:
                main._check_tds_key("")
            assert exc_info.value.status_code == 403

    def test_empty_stored_secret_403_h6_closure(self):
        """H6 CORE CLOSURE — empty stored secret means EVERY
        request rejected, regardless of what the client sends.

        Pre-fix: this was 200 (fail-open) because the `if
        settings.tds_secret_key and ...` guard short-circuited.
        Post-fix: 403 because `not (provided and stored and ...)`
        is True when `stored == ""`."""
        from app import main

        with patch.object(main.settings, "tds_secret_key", ""):
            with pytest.raises(HTTPException) as exc_info:
                main._check_tds_key("anything-client-sends")
            assert exc_info.value.status_code == 403

    def test_empty_both_sides_403(self):
        """No surprise edge case — empty + empty still 403."""
        from app import main

        with patch.object(main.settings, "tds_secret_key", ""):
            with pytest.raises(HTTPException):
                main._check_tds_key("")

    def test_helper_uses_compare_digest_not_double_equals(self):
        """Time-safety regression fence — assert the helper body
        contains the `compare_digest` call, not `==`. (Source-level
        pin in test_admin_auth_timing_safe.py also exists; this is
        the behavioural angle.)"""
        import inspect
        from app import main

        src = inspect.getsource(main._check_tds_key)
        assert "hmac.compare_digest" in src
        # No naive equality against stored secret allowed.
        forbidden = [
            "x_tds_key == settings.tds_secret_key",
            "settings.tds_secret_key == x_tds_key",
            "provided == stored",
        ]
        for pat in forbidden:
            assert pat not in src, (
                f"Found timing-attack-prone pattern in helper: {pat!r}"
            )
