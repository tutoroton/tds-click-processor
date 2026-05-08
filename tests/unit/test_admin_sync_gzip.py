"""Tests for the click-processor side of T1.2 / G-16 — dual-decode
gzip support on `/admin/sync`.

The admin-api side gzips the snapshot when
`TDS_SYNC_PUSH_GZIP_ENABLED=true` and adds `Content-Encoding: gzip`.
This endpoint must:

  1. Continue to accept plain JSON bodies (backward compat with
     pre-T1.2 admin-api builds + when the flag is OFF)
  2. Detect `Content-Encoding: gzip` and gunzip before parsing
  3. Reject unknown encodings with 415 (don't silently treat as
     plain JSON — operators get a clear signal that the deploy
     order is wrong)
  4. Cap on-the-wire body at 50MB (zip-bomb gate)
  5. Cap post-decompress body at 500MB (semantic snapshot limit)

Source-level pin — full behavioral test would require booting the
FastAPI app + Redis fixture + auth headers. The contract pins
here catch the most likely regression: a refactor that drops the
gunzip branch, the size caps, or the 415 reject.

Reference: rule `sync-protocol` "Optional gzip on push",
`docs/roadmap/stage-2-sync-coverage-action-items.md` T1.2,
`docs/roadmap/stage-2-sync-coverage-open-questions.md` G-16.
"""

from __future__ import annotations

import inspect

import pytest


# Lazy import so a broken `app.main` doesn't fail the test file
# at collection time — explicit import inside each test surfaces
# precisely the assertion that broke.


class TestImports:
    def test_gzip_module_imported(self):
        from app import main as click_main

        source = inspect.getsource(click_main)
        # `import gzip` must be present at module level.
        assert "import gzip" in source.splitlines()[0:20] or any(
            line.strip() == "import gzip"
            for line in source.splitlines()
        )


class TestSizeCaps:
    """The two-stage size guard MUST be present + the constants
    named so a future bump or rename is trivially auditable."""

    def test_compressed_cap_constant(self):
        from app.main import _MAX_COMPRESSED_BYTES

        # 50MB — same as the legacy single-stage cap. Zip-bomb gate.
        assert _MAX_COMPRESSED_BYTES == 50 * 1024 * 1024

    def test_uncompressed_cap_constant(self):
        from app.main import _MAX_UNCOMPRESSED_BYTES

        # 500MB — semantic snapshot cap. Gives admin-api ~10x
        # headroom over the on-the-wire cap once gzip is on.
        assert _MAX_UNCOMPRESSED_BYTES == 500 * 1024 * 1024

    def test_caps_ordering(self):
        """Compressed cap must be smaller than uncompressed cap —
        otherwise the two-stage check is degenerate."""
        from app.main import _MAX_COMPRESSED_BYTES, _MAX_UNCOMPRESSED_BYTES

        assert _MAX_COMPRESSED_BYTES < _MAX_UNCOMPRESSED_BYTES


class TestReceiveSyncSource:
    """Source-level pin on the /admin/sync handler."""

    def _source(self):
        from app.main import receive_sync

        return inspect.getsource(receive_sync)

    def test_pre_decompress_size_check_present(self):
        source = self._source()
        # The handler still consults Content-Length before doing
        # any work — defends vs an attacker streaming a multi-GB
        # body just to OOM the worker.
        assert "_MAX_COMPRESSED_BYTES" in source
        assert "content-length" in source.lower()
        assert "413" in source

    def test_gzip_branch_present(self):
        source = self._source()
        # The dual-decode branch checks Content-Encoding and
        # decompresses with `gzip.decompress`.
        assert 'content-encoding' in source.lower()
        assert "gzip.decompress" in source

    def test_post_decompress_cap_enforced(self):
        source = self._source()
        # After gunzip, the decoded size must be checked against
        # the uncompressed cap. Without this, a 50MB-on-wire gzip
        # bomb could expand to many GB and OOM the worker.
        assert "_MAX_UNCOMPRESSED_BYTES" in source
        # The actual size CHECK (not the docstring mention) must
        # sit AFTER `gzip.decompress`. Search for the canonical
        # comparison shape `> _MAX_UNCOMPRESSED_BYTES` — that's
        # the code-side reference; the docstring mention earlier
        # in the source uses parentheses + backticks.
        idx_decompress = source.index("gzip.decompress")
        idx_uncomp_check = source.index("> _MAX_UNCOMPRESSED_BYTES")
        assert idx_decompress < idx_uncomp_check

    def test_unknown_encoding_raises_415(self):
        source = self._source()
        # Spec-permitted `identity` is allowed as a no-op; anything
        # else MUST 415 so the operator sees the deploy-order
        # mistake instead of silently parsing bytes as JSON.
        assert "415" in source
        assert "identity" in source

    def test_invalid_gzip_raises_400(self):
        """A malformed gzip body (truncated, wrong magic) should
        return 400, not 500. Pin the OSError/EOFError handler."""
        source = self._source()
        assert "OSError" in source or "BadGzipFile" in source
        assert "400" in source

    def test_legacy_plain_json_path_preserved(self):
        """Without `Content-Encoding: gzip`, the body is parsed as
        plain JSON — backward compat with pre-T1.2 admin-api builds
        + the default-OFF case. Pin that the json.loads call still
        runs after the encoding branch.
        """
        source = self._source()
        # The JSON parse must be reachable without going through
        # the gzip branch — i.e., it sits AFTER the if/elif and
        # operates on `raw_body` (which is either the original
        # body or the decoded one).
        assert "json.loads(raw_body)" in source
        # Defense vs a future refactor that moves the JSON parse
        # INSIDE the gzip branch — would break legacy admin-api.
        idx_json_loads = source.index("json.loads(raw_body)")
        idx_encoding = source.index("encoding")
        assert idx_encoding < idx_json_loads
