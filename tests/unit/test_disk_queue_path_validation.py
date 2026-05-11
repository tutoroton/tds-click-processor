"""Tests for M7 fix — disk_queue_root absolute-path validator.

Was a silent-data-loss footgun: if uvicorn launched with `cwd=/`,
queued click files landed at `/var/click-queue/...` — a system-wide
path the service may not own. The defensive `chmod(0o700)` failures
were silently swallowed, so the misconfig was invisible until you
went looking for the lost clicks. The drainer's sorted-glob scan
would then find no files and skip replay.

M7 fix:
  1. Default changed from `var/click-queue` (relative) to
     `/var/tds/click-queue` (absolute).
  2. New `_enforce_disk_queue_root_absolute` model validator
     refuses to construct Settings when a relative path is set
     (loud failure at startup > silent runtime data loss).
  3. Empty string still allowed (cleanly disables disk fallback).
  4. The silent `except OSError: pass` on chmod replaced with
     logger.warning + sentry_sdk.capture_message so operators
     see directory-perm drift.
"""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest


class TestSettingsDefault:
    def test_default_is_absolute(self):
        """The default path MUST be absolute so a default-config
        deploy can't silent-fail in a `cwd=/` container."""
        from app.config import Settings

        assert Settings.model_fields["disk_queue_root"].default == "/var/tds/click-queue", (
            "Default disk_queue_root should be the absolute "
            "`/var/tds/click-queue` per M7 fix design."
        )


class TestValidator:
    def _make_settings_kwargs(self, **overrides):
        """Build a kwargs dict that satisfies the OTHER validators
        (secret-presence guard) so we isolate the disk_queue_root
        check."""
        base = {
            "environment": "local",  # short-circuits secret guard
            "tds_secret_key": "",  # allowed in local
        }
        base.update(overrides)
        return base

    def test_absolute_default_path_accepted(self):
        """The configured default must not fail its own validator."""
        from app.config import Settings

        kwargs = self._make_settings_kwargs()
        # Should not raise.
        s = Settings(**kwargs)
        assert s.disk_queue_root == "/var/tds/click-queue"

    def test_relative_path_rejected(self):
        """A relative path raises ValueError at construction time
        — loud failure, operator sees it on `docker compose up`."""
        from app.config import Settings

        kwargs = self._make_settings_kwargs(disk_queue_root="var/click-queue")
        with pytest.raises(ValueError, match=r"absolute path"):
            Settings(**kwargs)

    def test_bare_relative_dotpath_rejected(self):
        from app.config import Settings

        kwargs = self._make_settings_kwargs(disk_queue_root="./local")
        with pytest.raises(ValueError, match=r"absolute path"):
            Settings(**kwargs)

    def test_empty_string_allowed_disables_fallback(self):
        """Empty value is explicitly allowed as 'feature off'.
        Use case: local-dev runs that don't want a system path
        created at startup. The drainer becomes a no-op."""
        from app.config import Settings

        kwargs = self._make_settings_kwargs(disk_queue_root="")
        s = Settings(**kwargs)
        assert s.disk_queue_root == ""

    def test_other_absolute_paths_accepted(self):
        from app.config import Settings

        for path in ("/tmp/clicks", "/srv/tds/queue", "/data/cp/q"):
            kwargs = self._make_settings_kwargs(disk_queue_root=path)
            s = Settings(**kwargs)
            assert s.disk_queue_root == path


class TestChmodLoudFailure:
    """The defensive chmod on the parent directory MUST emit a
    log + Sentry message on failure, not silently pass. Operators
    need to see directory-perm drift."""

    def test_chmod_failure_logs_warning(self, tmp_path, caplog):
        """Run the real `_write_file_sync` against a path whose
        parent we make un-chmod-able by mocking `os.chmod` to
        raise OSError. Assert that a WARN log was emitted."""
        import logging
        from app import disk_queue

        target = tmp_path / "out" / "x.json"

        def fail_chmod(*args, **kwargs):
            raise OSError("Operation not permitted")

        caplog.set_level(logging.WARNING, logger="app.disk_queue")
        with patch("app.disk_queue.os.chmod", side_effect=fail_chmod), \
             patch("app.disk_queue.sentry_sdk.capture_message") as mock_capture:
            # `_write_file_sync` is the internal that contains the
            # chmod block. Calling through the public API requires
            # async setup; the unit-level test below targets the
            # function directly.
            disk_queue._write_file_sync(target, b'{"click_id":"x"}')

        # File still written despite chmod failure.
        assert target.exists()
        assert target.read_bytes() == b'{"click_id":"x"}'

        # Warning was logged (not silent).
        assert any(
            "chmod 0o700 failed" in rec.message and "world-readable" in rec.message
            for rec in caplog.records
        ), f"Expected chmod-failure warning, got: {[r.message for r in caplog.records]}"

        # Sentry capture fired.
        mock_capture.assert_called_once()
        capture_args = mock_capture.call_args
        assert "chmod failed" in capture_args.args[0]
        assert capture_args.kwargs.get("level") == "warning"
