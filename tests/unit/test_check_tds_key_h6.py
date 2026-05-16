"""Tests for `_check_tds_key` — single-path per-Worker auth.

## History

- H6 fail-closed (2026-05-11): the legacy
  `if settings.tds_secret_key and not compare_digest(...)` pattern
  failed OPEN when the stored secret was empty. H6 inverted it to
  fail CLOSED.
- F.24 Phase 1 (2026-05-14): added the per-Worker path
  (`worker_secret_hash:{sha256_hex} → worker_id` in local Redis) in
  FRONT of the legacy global-secret fallback (dual-window). Legacy
  match returned sentinel `0`.
- **F.25 (2026-05-16): the legacy global-secret fallback was
  REMOVED.** Migration 057 backfilled every `workers` row with the
  global value (encrypted) as its per-Worker secret, so
  `sha256(global) → worker_id` is already in the per-Worker Redis
  index for every active+deployed Worker — a Worker still presenting
  the global header authenticates via the per-Worker path, not a
  special branch. Verified deterministically on staging (all Workers
  carry a per-Worker secret; 0 active+deployed missing). The global
  secret itself is NOT removed — it stays the sync/edge-channel
  credential — only the `_check_tds_key` fallback is gone.

## Post-F.25 contract (what these tests pin)

  1. Empty `x_tds_key` → 403 (step 1).
  2. Per-Worker hash hit → return the int `worker_id` (≥ 1).
  3. Per-Worker MISS → 403. `settings.tds_secret_key` is NOT
     consulted (no legacy fallback, no sentinel `0`).
  4. Redis error / corrupted index entry → 403 (FAIL CLOSED). The
     pre-F.25 behaviour (fall through to legacy → sentinel 0) is
     GONE — this is the key behaviour-change pin.
  5. Fail-closed is strictly stronger than the pre-F.25 H6 invert:
     an empty/misconfigured global secret can no longer
     auto-authenticate any Worker here.
"""

from __future__ import annotations

import hashlib
import inspect
from unittest.mock import patch

import pytest
from fastapi import HTTPException


def _sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


class _FakeRedisEmpty:
    """Stub: every `get` misses → per-Worker MISS path (now → 403)."""

    async def get(self, key: str):
        return None


class _FakeRedisHit:
    """Stub: returns `worker_id_str` only for the pre-configured key
    (mirrors the sync builder writing hashes only for active Workers).
    """

    def __init__(self, expected_key: str, worker_id_str: str):
        self._expected_key = expected_key
        self._worker_id_str = worker_id_str

    async def get(self, key: str):
        return self._worker_id_str if key == self._expected_key else None


class _FakeRedisError:
    """Stub: raises on `get` — simulates a transient Redis outage.
    Post-F.25 the helper must FAIL CLOSED (403), not fall to legacy.
    """

    async def get(self, key: str):
        raise RuntimeError("Redis unreachable (simulated)")


@pytest.fixture
def patch_redis_empty():
    """`get_redis()` → a fake that always misses (per-Worker MISS)."""
    from app import main

    async def _empty():
        return _FakeRedisEmpty()

    with patch.object(main, "get_redis", _empty):
        yield


# ---------------------------------------------------------------------------
# Fail-closed semantics (single per-Worker path)
# ---------------------------------------------------------------------------


class TestFailClosed:
    @pytest.mark.asyncio
    async def test_per_worker_miss_403_even_with_matching_global(
        self, patch_redis_empty
    ):
        """KEY F.25 BEHAVIOUR CHANGE: a per-Worker MISS now 403s even
        when the incoming key EQUALS the global `settings.tds_secret_key`.
        Pre-F.25 this returned sentinel `0` via the legacy fallback;
        the fallback is gone — the global secret is no longer honoured
        by this helper at all."""
        from app import main

        with patch.object(
            main.settings, "tds_secret_key",
            "the-shared-secret-32chars-long-aa",
        ):
            with pytest.raises(HTTPException) as exc:
                await main._check_tds_key("the-shared-secret-32chars-long-aa")
            assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_wrong_key_403(self, patch_redis_empty):
        from app import main

        with patch.object(main.settings, "tds_secret_key", "stored-secret"):
            with pytest.raises(HTTPException) as exc:
                await main._check_tds_key("wrong-secret")
            assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_missing_header_403(self, patch_redis_empty):
        """Empty incoming `X-TDS-Key` → 403 at step 1 (before any
        Redis lookup)."""
        from app import main

        with patch.object(main.settings, "tds_secret_key", "stored-secret"):
            with pytest.raises(HTTPException) as exc:
                await main._check_tds_key("")
            assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_empty_stored_secret_403(self, patch_redis_empty):
        """Empty global secret → still 403 on per-Worker miss. The
        secret is not consulted by this helper post-F.25, so an
        empty/misconfigured global secret CANNOT auto-authenticate —
        strictly more fail-closed than the pre-F.25 H6 invert."""
        from app import main

        with patch.object(main.settings, "tds_secret_key", ""):
            with pytest.raises(HTTPException) as exc:
                await main._check_tds_key("anything-client-sends")
            assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_empty_both_sides_403(self, patch_redis_empty):
        from app import main

        with patch.object(main.settings, "tds_secret_key", ""):
            with pytest.raises(HTTPException):
                await main._check_tds_key("")

    def test_helper_does_not_compare_global_secret(self):
        """Behavioural-angle source pin (file-level pin lives in
        `test_admin_auth_timing_safe.py`): the helper CODE must NOT
        reference `settings.tds_secret_key` (legacy fallback removed)
        and must NOT carry a timing-attack-prone direct equality.

        The docstring is stripped first — it legitimately *describes*
        the removed pattern + the secret's remaining sync role, which
        must not trip a `not in` code assertion."""
        from app import main

        full = inspect.getsource(main._check_tds_key)
        parts = full.split('"""')
        src = parts[0] + '"""'.join(parts[2:]) if len(parts) >= 3 else full
        assert "settings.tds_secret_key" not in src, (
            "`_check_tds_key` CODE must not consult the global secret "
            "post-F.25 — the legacy fallback was removed."
        )
        assert "hmac.compare_digest" not in src, (
            "`_check_tds_key` CODE must not do any secret string "
            "compare post-F.25 — auth is a one-way sha256 → Redis "
            "digest lookup (no timing surface)."
        )
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
# Per-Worker path (the SOLE auth path post-F.25)
# ---------------------------------------------------------------------------


class TestF24SinglePath:
    @pytest.mark.asyncio
    async def test_per_worker_hit_returns_worker_id(self):
        """X-TDS-Key sha256 hash matches a Redis `worker_secret_hash:*`
        key → returns the int `worker_id`."""
        from app import main

        secret = "tdssec_per_worker_value_43_chars_xxxxxxxxxx"
        expected_key = f"worker_secret_hash:{_sha256(secret)}"
        fake_redis = _FakeRedisHit(expected_key, worker_id_str="42")

        async def _fake_get_redis():
            return fake_redis

        with patch.object(main, "get_redis", _fake_get_redis), \
             patch.object(
                 main.settings, "tds_secret_key",
                 "something-completely-different-32chars-x",
             ):
            result = await main._check_tds_key(secret)
            assert result == 42

    @pytest.mark.asyncio
    async def test_per_worker_miss_with_matching_global_now_403(
        self, patch_redis_empty
    ):
        """Redis miss + incoming key == global secret → 403 (NOT the
        old sentinel `0`). The dual-window legacy fallback is gone;
        this is the canonical F.25 regression pin."""
        from app import main

        with patch.object(
            main.settings, "tds_secret_key",
            "legacy-global-32chars-aaaaaaaaaaa",
        ):
            with pytest.raises(HTTPException) as exc:
                await main._check_tds_key("legacy-global-32chars-aaaaaaaaaaa")
            assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_redis_outage_fails_closed_403(self):
        """Redis unreachable → 403 (FAIL CLOSED). Pre-F.25 this fell
        through to the legacy global compare (→ sentinel 0). Post-F.25
        there is no fallback: `/decide` cannot serve without Redis
        anyway (`architecture.md` fail-closed), so failing auth closed
        here is consistent, not a new outage surface."""
        from app import main

        async def _err_redis():
            return _FakeRedisError()

        with patch.object(main, "get_redis", _err_redis), \
             patch.object(
                 main.settings, "tds_secret_key",
                 "global-secret-32chars-bbbbbbbbb",
             ):
            with pytest.raises(HTTPException) as exc:
                await main._check_tds_key("global-secret-32chars-bbbbbbbbb")
            assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_unknown_secret_403(self, patch_redis_empty):
        """Redis miss + unknown key → 403."""
        from app import main

        with patch.object(
            main.settings, "tds_secret_key",
            "legacy-32chars-cccccccccccccccccc",
        ):
            with pytest.raises(HTTPException) as exc:
                await main._check_tds_key("attacker-supplied-32chars-xxxxxx")
            assert exc.value.status_code == 403

    @pytest.mark.asyncio
    async def test_corrupted_worker_id_fails_closed_403(self):
        """Defensive: a sync bug writing a non-numeric value under
        `worker_secret_hash:*` must NOT crash the auth path AND must
        NOT auto-authenticate. Pre-F.25 it fell through to legacy
        (→ 0); post-F.25 it FAILS CLOSED (403) — there is no legacy
        net, and a corrupted index entry is never a valid auth."""
        from app import main

        secret = "tdssec_value_that_will_be_hashed_xxxxxx"
        expected_key = f"worker_secret_hash:{_sha256(secret)}"
        fake_redis = _FakeRedisHit(expected_key, worker_id_str="not_an_int")

        async def _fake_get_redis():
            return fake_redis

        with patch.object(main, "get_redis", _fake_get_redis), \
             patch.object(main.settings, "tds_secret_key", secret):
            with pytest.raises(HTTPException) as exc:
                await main._check_tds_key(secret)
            assert exc.value.status_code == 403

    def test_uses_full_sha256_hex_not_prefix(self):
        """Source pin — `_hash_secret` returns the FULL hex digest,
        not a prefix. A prefix lookup introduces collision bugs at
        scale and divergence vs the admin-api sync builder."""
        from app import main

        src = inspect.getsource(main._hash_secret)
        assert ".hexdigest()" in src
        assert ".hexdigest()[" not in src, (
            "_hash_secret appears to slice the hex digest (prefix). "
            "Must use the full 64-char hex per F.24 Phase 1 — see "
            "`app/sync/builders/workers.py`."
        )
