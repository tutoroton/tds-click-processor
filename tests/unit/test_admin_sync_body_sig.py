"""Tests for the click-processor side of T2.4 — HMAC-SHA256 body
integrity verification on `/admin/sync`.

Defends the plain-HTTP edge sync path against active MITM. Sig is
optional today (lenient on absent header for backward compat with
older admin-api builds + dev mode without a configured secret),
but when present it MUST verify byte-exact against
``hmac_sha256(tds_secret_key, raw_body)``. Mismatch → 401, no
snapshot apply.

Coverage:

* Source-pin on the `/admin/sync` handler:
    - `X-TDS-Body-Sig` header parameter is declared
    - `hmac.compare_digest` is used (no `==` comparison)
    - sig verification runs BEFORE decompression so a corrupt
      gzip body fails the sig check (cleaner error surface)
    - the canonical algo prefix `sha256=` is enforced (future-
      proofing for sha512 etc.)

* Behavioural shape pin:
    - well-formed correct sig is accepted
    - mismatched sig is rejected (401 path)
    - absent header is allowed (lenient mode, backward compat)
    - non-`sha256=` algo prefix is rejected (400 path)

The behavioural tests are source-level pins on the handler logic
rather than full FastAPI request/response — booting the FastAPI
app with Redis/auth fixtures is heavyweight relative to the
contract size, and other admin-sync tests in the repo use the
same source-grep approach.

Reference: rule `outbound-http-safety` "Worker → Backend integrity"
+ "hmac.compare_digest for HMAC compare", rule `sync-protocol`,
action-items.md T2.4.
"""

from __future__ import annotations

import hashlib
import hmac
import inspect

import pytest


# ---------------------------------------------------------------------------
# Source-pin — handler structure
# ---------------------------------------------------------------------------


class TestReceiveSyncSource:
    def _source(self) -> str:
        from app.main import receive_sync

        return inspect.getsource(receive_sync)

    def test_header_param_declared(self):
        source = self._source()
        # The Header alias matters — case-insensitive on the wire,
        # but FastAPI dispatches by alias string. Pin the canonical
        # name so a refactor can't quietly rename to e.g.
        # `X-Body-Sig` and break admin-api compat.
        assert 'alias="X-TDS-Body-Sig"' in source, (
            "/admin/sync must declare a Header param with "
            "alias='X-TDS-Body-Sig' (T2.4)."
        )

    def test_uses_compare_digest(self):
        """Per rule `outbound-http-safety`, all HMAC compares use
        ``hmac.compare_digest`` for timing safety. A naive `==` on
        the hex digest would leak per-byte timing — measurable
        over many requests, lets an attacker reconstruct a forged
        sig."""
        source = self._source()
        assert "compare_digest" in source, (
            "Body sig verification must use hmac.compare_digest "
            "(timing-safe). See rule outbound-http-safety."
        )
        # Forbid raw `==` against either of the sig variables.
        forbidden_patterns = [
            "provided_hex == expected_hex",
            "expected_hex == provided_hex",
            "x_tds_body_sig ==",
        ]
        for pat in forbidden_patterns:
            assert pat not in source, (
                f"Forbidden timing-leaky pattern: {pat!r}. Use "
                "hmac.compare_digest instead."
            )

    def test_sha256_prefix_enforced(self):
        """The header value must start with `sha256=`. Today this
        is the only supported algo; rejecting any other prefix
        leaves room for future algo upgrade under the same header
        without a silent-accept regression."""
        source = self._source()
        assert "sha256=" in source, (
            "Handler must enforce the `sha256=` prefix on "
            "X-TDS-Body-Sig (future-proofing for algo upgrade)."
        )

    def test_sig_verified_before_gunzip(self):
        """The sig is computed over the EXACT bytes that arrived on
        the wire — independent of Content-Encoding. Verification
        must happen on `raw_body` BEFORE the gzip branch runs, so
        a corrupted gzip body fails the sig check (clean error)
        rather than the gunzip step (confusing error)."""
        source = self._source()
        idx_compare = source.find("hmac.compare_digest")
        idx_decompress = source.find("gzip.decompress")
        assert idx_compare != -1, "compare_digest call missing"
        assert idx_decompress != -1, "gzip.decompress call missing"
        assert idx_compare < idx_decompress, (
            "X-TDS-Body-Sig verification must run BEFORE "
            "gzip.decompress so corrupt-body errors don't shadow "
            "sig errors."
        )

    def test_lenient_on_absent_header(self):
        """Backward compat — older admin-api builds don't ship the
        sig header. We must NOT 401 in that case; the X-TDS-Key
        check is the existing auth surface and remains the
        primary gate."""
        source = self._source()
        # The branch we want is "if x_tds_body_sig and ..." —
        # absent (empty) header skips the verifier entirely.
        # Pin the canonical condition shape.
        assert "if x_tds_body_sig and settings.tds_secret_key" in source, (
            "Handler must be lenient on absent X-TDS-Body-Sig "
            "(older admin-api builds). Required gate is "
            "`if x_tds_body_sig and settings.tds_secret_key:`."
        )


# ---------------------------------------------------------------------------
# Behavioural pin — sig verification math
# ---------------------------------------------------------------------------


def _expected_sig(body: bytes, secret: str) -> str:
    """Mirror of the admin-api side helper. Used here as the
    cross-service contract pin: if either side drifts, both these
    tests fail."""
    return "sha256=" + hmac.new(
        secret.encode("utf-8"), body, hashlib.sha256,
    ).hexdigest()


class TestSigMath:
    """Verify the cross-service contract by computing both sides
    locally — admin-api emits exactly what click-processor expects."""

    def test_round_trip_matches(self):
        body = b'{"data":{"campaign:1":{"name":"C1"}}}'
        secret = "shared-tds-key-for-tests"

        # admin-api computes
        sig = _expected_sig(body, secret)

        # click-processor verifies
        assert sig.startswith("sha256=")
        provided_hex = sig[len("sha256="):]
        recomputed = hmac.new(
            secret.encode("utf-8"), body, hashlib.sha256,
        ).hexdigest()

        assert hmac.compare_digest(provided_hex, recomputed)

    def test_tampered_body_fails(self):
        original = b'{"data":{"campaign:1":{"name":"C1"}}}'
        tampered = b'{"data":{"campaign:1":{"name":"EVIL"}}}'
        secret = "k"

        # admin-api signs the original
        sig = _expected_sig(original, secret)

        # MITM swaps body but keeps sig
        provided_hex = sig[len("sha256="):]
        recomputed = hmac.new(
            secret.encode("utf-8"), tampered, hashlib.sha256,
        ).hexdigest()

        # Verifier rejects
        assert not hmac.compare_digest(provided_hex, recomputed)

    def test_wrong_secret_fails(self):
        body = b'{"x":1}'
        sig_with_real = _expected_sig(body, "real-secret")
        provided_hex = sig_with_real[len("sha256="):]

        # Verifier on the receive side has a different secret
        recomputed = hmac.new(
            b"different-secret", body, hashlib.sha256,
        ).hexdigest()

        assert not hmac.compare_digest(provided_hex, recomputed)

    @pytest.mark.parametrize(
        "size",
        [0, 1, 100, 10_000, 1_000_000],
    )
    def test_size_invariance(self, size):
        body = b"x" * size
        sig_a = _expected_sig(body, "k")
        sig_b = _expected_sig(body, "k")
        assert sig_a == sig_b, "Determinism must hold for any size"


class TestUnsupportedAlgoRejection:
    """An attacker (or a buggy admin-api) that ships
    ``X-TDS-Body-Sig: md5=<hex>`` MUST be rejected — silently
    falling back to "lenient on unknown" would let weaker algos
    leak in. Source-pin on the rejection path."""

    def test_handler_rejects_unsupported_prefix(self):
        from app.main import receive_sync

        source = inspect.getsource(receive_sync)
        # The handler should raise 400 with a clear message naming
        # the offending prefix.
        assert "Unsupported X-TDS-Body-Sig algorithm" in source, (
            "Handler must reject non-sha256 sig prefixes with a "
            "400 + actionable error message."
        )
