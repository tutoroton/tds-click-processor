"""F.33 (2026-05-24) — fresh-node config-push auth on ``/admin/sync``.

Root cause this pins against: the handler authenticated SOLELY via
``_check_tds_key``, which resolves the ``X-TDS-Key`` against the LOCAL
``worker_secret_hash`` per-Worker index and fail-closes (403) on a miss.
That index is EMPTY on a freshly-provisioned node — it is populated *by*
a sync (the snapshot ships ``worker_secret_hash:*`` keys). So the very
first config push, the one that would seed the index, was rejected 403,
and a fresh node could never bootstrap its routing config: it reached
``active`` via the smoke probe yet stayed config-empty and mis-routed
live traffic. Same chicken-and-egg class as the smoke-403 (#40) — but on
``/admin/sync``, which the #55 ``/decide`` smoke-probe bypass did not
cover.

Fix: ``/admin/sync`` authenticates the push against the node's STATIC
shared secret (``settings.tds_secret_key`` — the value baked at provision
time and the one admin-api signs the push with) via a constant-time
compare, falling back to the per-Worker index only for defense in depth.
This is exactly the credential the ``_check_tds_key`` docstring already
designates for "admin-api sync auth", and is safe re F.25 (which removed
the global-secret fallback from ``/decide`` ROUTING auth, a different
threat model).

Coverage:
  * Behavioural — ``_sync_secret_matches`` pure-function contract.
  * Source-pin — ``receive_sync`` gates on ``_sync_secret_matches`` FIRST
    and only falls back to ``_check_tds_key`` on a static miss.

Reference: rule `outbound-http-safety` "Worker → Backend integrity",
rule `sync-protocol`, F.33 plan, sibling test_decide_smoke_bypass.py.
"""

from __future__ import annotations

import inspect

from unittest.mock import patch

from app.config import settings
from app.main import _sync_secret_matches, receive_sync


_SECRET = "x" * 40  # ≥32 chars, mirrors a real baked edge-node secret


# ---------------------------------------------------------------------------
# Behavioural — _sync_secret_matches pure-function contract
# ---------------------------------------------------------------------------


class TestSyncSecretMatches:
    def test_exact_match_authenticates(self):
        """The push carries the node's baked secret → authenticated.

        This is the fresh-node bootstrap path: the per-Worker index is
        empty, but the static secret matches, so the first config push
        is accepted and seeds the index for every subsequent request.
        """
        with patch.object(settings, "tds_secret_key", _SECRET):
            assert _sync_secret_matches(_SECRET) is True

    def test_wrong_secret_rejected(self):
        with patch.object(settings, "tds_secret_key", _SECRET):
            assert _sync_secret_matches("y" * 40) is False

    def test_empty_header_fails_closed(self):
        with patch.object(settings, "tds_secret_key", _SECRET):
            assert _sync_secret_matches("") is False

    def test_unset_secret_fails_closed(self):
        """Empty/unset node secret must never auto-authenticate — even
        an empty presented key. Mirrors the H6 fail-closed discipline."""
        with patch.object(settings, "tds_secret_key", ""):
            assert _sync_secret_matches("") is False
            assert _sync_secret_matches(_SECRET) is False

    def test_uses_constant_time_compare(self):
        """A naive ``==`` on the secret leaks per-byte timing. The helper
        MUST use ``hmac.compare_digest``."""
        source = inspect.getsource(_sync_secret_matches)
        assert "compare_digest" in source, (
            "_sync_secret_matches must use hmac.compare_digest "
            "(timing-safe). See rule outbound-http-safety."
        )
        assert "x_tds_key ==" not in source, (
            "Forbidden timing-leaky equality compare of the secret."
        )


# ---------------------------------------------------------------------------
# Source-pin — receive_sync auth ordering
# ---------------------------------------------------------------------------


class TestReceiveSyncAuthOrdering:
    def _source(self) -> str:
        return inspect.getsource(receive_sync)

    def test_static_secret_checked_first(self):
        """The static-secret gate must run BEFORE the per-Worker index
        fallback — otherwise a fresh node (empty index) 403s before the
        static path is ever consulted (the bug this fixes)."""
        source = self._source()
        # Pin the actual CALL expressions, not bare name mentions —
        # the explanatory comment references `_check_tds_key` before
        # the call, so matching bare names would be brittle.
        idx_static = source.find("_sync_secret_matches(x_tds_key)")
        idx_index = source.find("await _check_tds_key(x_tds_key)")
        assert idx_static != -1, (
            "receive_sync must gate on _sync_secret_matches(x_tds_key) "
            "(static edge-node secret) — the fresh-node bootstrap credential."
        )
        assert idx_index != -1, (
            "per-Worker fallback `await _check_tds_key(x_tds_key)` missing"
        )
        assert idx_static < idx_index, (
            "Static-secret check must precede the _check_tds_key "
            "per-Worker fallback so a fresh node (empty index) "
            "authenticates via its baked secret."
        )

    def test_index_is_a_fallback_not_sole_gate(self):
        """``_check_tds_key`` must be guarded by a static-miss branch,
        not called unconditionally (the pre-fix shape that 403'd fresh
        nodes)."""
        source = self._source()
        assert "if not _sync_secret_matches(x_tds_key):" in source, (
            "_check_tds_key must be reached only on a static-secret "
            "miss: `if not _sync_secret_matches(x_tds_key): "
            "await _check_tds_key(...)`."
        )
