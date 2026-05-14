"""Tests for `_check_tds_key` — H6 fail-closed (2026-05-11) + F.24
Phase 1 dual-path auth (2026-05-14).

## H6 — Fail-closed (preserved)

Legacy pattern: `if settings.tds_secret_key and (not x_tds_key or
not hmac.compare_digest(...)): raise`. The leading `and` short-
circuits to "no auth check" when `settings.tds_secret_key == ""`.

In production this was unreachable (startup guard refuses empty
key in non-local environments). But in local/development, an
operator running on a non-isolated network was exposed.

H6 fix: invert to `if not (provided and stored and compare_digest):
raise`. Empty stored secret now means EVERY request is rejected.

## F.24 Phase 1 — Per-Worker dual-path

Admin-api emits a per-Worker `TDS_SECRET_KEY` and a sync builder
pushes `worker_secret_hash:{sha256_hex} → worker_id` to local
Redis. `_check_tds_key` now:
  1. Hashes the incoming X-TDS-Key with sha256.
  2. GETs `worker_secret_hash:{hex}` — on hit, returns the int
     `worker_id` (positive — used by future forensics paths).
  3. On miss, falls back to legacy `settings.tds_secret_key`
     constant-time compare — on hit, returns sentinel `0`.
  4. Otherwise raises 403.

Tests pin:
  - H6 closure (preserved) — empty stored secret 403s every request
  - per-Worker path — hash matches Redis key → returns worker_id
  - legacy fallback — Redis miss + compare_digest match → returns 0
  - 403 path — unknown secret + Redis miss
  - timing-safe (regression fence on `compare_digest` usage)
"""

from __future__ import annotations

import hashlib
import inspect
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException


def _sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


class _FakeRedisEmpty:
    """Minimal stub of the asyncio Redis client surface that
    `_check_tds_key` calls. Returns None on every `get` — forces
    the legacy fallback path on every test that uses it.
    """

    async def get(self, key: str):
        return None


class _FakeRedisHit:
    """Stub that returns `worker_id_str` ONLY when `get(key)` matches
    the pre-configured key. Mirrors the production semantics where
    the sync builder only writes hashes for active Workers.
    """

    def __init__(self, expected_key: str, worker_id_str: str):
        self._expected_key = expected_key
        self._worker_id_str = worker_id_str

    async def get(self, key: str):
        return self._worker_id_str if key == self._expected_key else None


class _FakeRedisError:
    """Stub that raises on `get` — simulates a transient Redis outage.
    `_check_tds_key` must catch + fall through to legacy.
    """

    async def get(self, key: str):
        raise RuntimeError("Redis unreachable (simulated)")


@pytest.fixture
def patch_redis_empty():
    """`get_redis()` returns a fake that always misses → legacy path."""
    from app import main

    async def _empty():
        return _FakeRedisEmpty()

    with patch.object(main, "get_redis", _empty):
        yield


# ---------------------------------------------------------------------------
# H6 closure — fail-closed semantics with the dual-path active
# ---------------------------------------------------------------------------


class TestH6FailClosed:
    """Behaviour pin for the H6 fix — preserved through the F.24
    Phase 1 dual-path refactor."""

    @pytest.mark.asyncio
    async def test_valid_key_passes(self, patch_redis_empty):
        from app import main

        with patch.object(main.settings, "tds_secret_key", "the-shared-secret-32chars-long-aa"):
            # Returns sentinel 0 (legacy path matched).
            result = await main._check_tds_key("the-shared-secret-32chars-long-aa")
            assert result == 0

    @pytest.mark.asyncio
    async def test_wrong_key_403(self, patch_redis_empty):
        from app import main

        with patch.object(main.settings, "tds_secret_key", "stored-secret"):
            with pytest.raises(HTTPException) as exc_info:
                await main._check_tds_key("wrong-secret")
            assert exc_info.value.status_code == 403

    @pytest.mark.asyncio
    async def test_missing_header_403_when_stored_set(self, patch_redis_empty):
        """Empty incoming `X-TDS-Key` header MUST 403 even when
        the stored secret is set."""
        from app import main

        with patch.object(main.settings, "tds_secret_key", "stored-secret"):
            with pytest.raises(HTTPException) as exc_info:
                await main._check_tds_key("")
            assert exc_info.value.status_code == 403

    @pytest.mark.asyncio
    async def test_empty_stored_secret_403_h6_closure(self, patch_redis_empty):
        """H6 CORE CLOSURE — empty stored secret means EVERY
        request rejected, regardless of what the client sends.

        Pre-fix: this was 200 (fail-open) because the `if
        settings.tds_secret_key and ...` guard short-circuited.
        Post-fix: 403 because `not (provided and stored and ...)`
        is True when `stored == ""`."""
        from app import main

        with patch.object(main.settings, "tds_secret_key", ""):
            with pytest.raises(HTTPException) as exc_info:
                await main._check_tds_key("anything-client-sends")
            assert exc_info.value.status_code == 403

    @pytest.mark.asyncio
    async def test_empty_both_sides_403(self, patch_redis_empty):
        """No surprise edge case — empty + empty still 403."""
        from app import main

        with patch.object(main.settings, "tds_secret_key", ""):
            with pytest.raises(HTTPException):
                await main._check_tds_key("")

    def test_helper_uses_compare_digest_not_double_equals(self):
        """Time-safety regression fence — assert the helper body
        contains the `compare_digest` call, not `==`. Source-level
        pin in `test_admin_auth_timing_safe.py` also exists; this is
        the behavioural angle.
        """
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


# ---------------------------------------------------------------------------
# F.24 Phase 1 — Per-Worker dual-path
# ---------------------------------------------------------------------------


class TestF24DualPath:
    """Behaviour pin for the F.24 Phase 1 per-Worker auth path."""

    @pytest.mark.asyncio
    async def test_per_worker_hit_returns_worker_id(self):
        """When the incoming X-TDS-Key's sha256 hash matches a Redis
        `worker_secret_hash:*` key, the helper returns the int
        `worker_id` — NOT the sentinel 0."""
        from app import main

        secret = "tdssec_per_worker_value_43_chars_xxxxxxxxxx"
        expected_key = f"worker_secret_hash:{_sha256(secret)}"
        fake_redis = _FakeRedisHit(expected_key, worker_id_str="42")

        async def _fake_get_redis():
            return fake_redis

        with patch.object(main, "get_redis", _fake_get_redis), \
             patch.object(main.settings, "tds_secret_key", "something-completely-different-32chars-x"):
            result = await main._check_tds_key(secret)
            assert result == 42, (
                "Per-Worker hash hit must return the matched worker_id; "
                "the legacy global secret in this test is intentionally "
                "different so the test cannot accidentally pass via the "
                "legacy fallback."
            )

    @pytest.mark.asyncio
    async def test_legacy_fallback_returns_sentinel_zero(self, patch_redis_empty):
        """Redis miss + global secret match → returns sentinel `0`.
        Distinguishes "matched via legacy" from "matched via per-Worker".
        The dual-window discipline relies on this fallback during the
        24h migration window."""
        from app import main

        with patch.object(main.settings, "tds_secret_key", "legacy-global-32chars-aaaaaaaaaaa"):
            result = await main._check_tds_key("legacy-global-32chars-aaaaaaaaaaa")
            assert result == 0

    @pytest.mark.asyncio
    async def test_redis_outage_falls_through_to_legacy(self):
        """If Redis is unreachable, the per-Worker lookup must NOT
        block the auth path. The helper catches the exception and
        falls through to the legacy global compare_digest."""
        from app import main

        async def _err_redis():
            return _FakeRedisError()

        with patch.object(main, "get_redis", _err_redis), \
             patch.object(main.settings, "tds_secret_key", "global-secret-32chars-bbbbbbbbb"):
            # Legacy succeeds → return sentinel 0
            result = await main._check_tds_key("global-secret-32chars-bbbbbbbbb")
            assert result == 0

    @pytest.mark.asyncio
    async def test_unknown_secret_with_no_legacy_match_403(self, patch_redis_empty):
        """Cold path — Redis miss AND legacy compare_digest miss → 403.
        Neither path matched, the request is unauthorised."""
        from app import main

        with patch.object(main.settings, "tds_secret_key", "legacy-32chars-cccccccccccccccccc"):
            with pytest.raises(HTTPException) as exc_info:
                await main._check_tds_key("attacker-supplied-32chars-xxxxxx")
            assert exc_info.value.status_code == 403

    @pytest.mark.asyncio
    async def test_corrupted_worker_id_falls_through(self):
        """Defensive — if a sync bug ever wrote a non-numeric value
        under `worker_secret_hash:*`, the helper logs + falls through
        to legacy instead of crashing the auth path."""
        from app import main

        secret = "tdssec_value_that_will_be_hashed_xxxxxx"
        expected_key = f"worker_secret_hash:{_sha256(secret)}"
        # Intentionally non-numeric value — must not crash.
        fake_redis = _FakeRedisHit(expected_key, worker_id_str="not_an_int")

        async def _fake_get_redis():
            return fake_redis

        with patch.object(main, "get_redis", _fake_get_redis), \
             patch.object(main.settings, "tds_secret_key", secret):
            # Legacy fallback succeeds (the same string matches itself).
            result = await main._check_tds_key(secret)
            assert result == 0

    def test_uses_full_sha256_hex_not_prefix(self):
        """Source-level pin — `_hash_secret` returns the full hex
        digest, NOT a prefix. A prefix-based lookup would introduce
        a class of collision bugs at Worker counts in the millions
        and divergence between admin-api / click-processor hashing
        rules would silently break auth."""
        from app import main

        src = inspect.getsource(main._hash_secret)
        # The full hex digest is 64 chars. A prefix-style impl would
        # call `.hexdigest()[:N]`. Pin against that.
        assert ".hexdigest()" in src
        assert ".hexdigest()[" not in src, (
            "_hash_secret appears to slice the hex digest (prefix). "
            "Must use the full 64-char hex per the F.24 Phase 1 "
            "design — see comments in `app/sync/builders/workers.py`."
        )
