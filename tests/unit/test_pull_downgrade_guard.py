"""A-4 — the periodic PULL path must enforce sync-version monotonicity,
in parity with the PUSH handler (`/admin/sync`).

Background (RV-C3.md §A-4): the monotonic-version guard lived ONLY in
the push HTTP handler (`main.py`). `pull_from_central` called
`apply_snapshot` directly, and `apply_snapshot` writes `sync:version`
UNCONDITIONALLY — so a periodic pull that received an older snapshot
(e.g. central serving a stale cached `_last_snapshot` mid-rebuild) would
roll the node's routing version backwards with no guard. The fix mirrors
the push handler's guard inside `pull_from_central` (left
`apply_snapshot` and the push handler untouched).

Three correctness cases pinned:
  * strict downgrade (incoming < current) → rejected, apply_snapshot NOT called
  * equal version (idempotent re-apply) → apply_snapshot CALLED
  * fresh node (no sync:version yet) → first pull applies

These are behavioural — httpx + redis are mocked; no network, no PG.
"""

from __future__ import annotations

import inspect

import pytest


# ---------------------------------------------------------------------------
# Source-level pin — the guard must live in pull_from_central, not be
# folded into apply_snapshot (which stays a load-bearing, source-pinned,
# version-arbitrary helper).
# ---------------------------------------------------------------------------


def test_guard_lives_in_pull_not_apply_snapshot():
    from app.sync_client import pull_from_central, apply_snapshot

    pull_src = inspect.getsource(pull_from_central)
    assert 'await redis.get("sync:version")' in pull_src, (
        "pull_from_central must read the node's current sync:version "
        "before applying (A-4 monotonicity guard)."
    )
    assert '"reason": "version downgrade"' in pull_src, (
        "pull_from_central must reject a strict downgrade with the same "
        "shape the push handler uses."
    )
    # apply_snapshot must NOT gain a version guard — it stays unconditional
    # (the push handler + pull path each guard before calling it).
    apply_src = inspect.getsource(apply_snapshot)
    assert "version downgrade" not in apply_src, (
        "apply_snapshot must remain guard-free (A-4 localises the guard to "
        "the pull path; centralising it is a deferred, riskier change)."
    )


# ---------------------------------------------------------------------------
# Behavioural — drive pull_from_central with mocked httpx + redis.
# ---------------------------------------------------------------------------


def _install_httpx(monkeypatch, snapshot: dict):
    """Make httpx.AsyncClient return `snapshot` from the snapshot GET."""
    import httpx

    class _Resp:
        def raise_for_status(self):
            return None

        def json(self):
            return snapshot

    class _Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def get(self, *a, **k):
            return _Resp()

    monkeypatch.setattr(httpx, "AsyncClient", lambda *a, **k: _Client())


class _Redis:
    """Minimal async redis stub exposing only `get` (the guard's read)."""

    def __init__(self, version: str | None):
        self._version = version
        self.get_calls: list[str] = []

    async def get(self, key):
        self.get_calls.append(key)
        return self._version


def _snapshot(sync_version: int) -> dict:
    return {
        "version": 1,
        "sync_version": sync_version,
        "timestamp": "2026-06-08T00:00:00Z",
        "key_count": 1,
        "data": {"campaign:1": {"name": "C1"}},
        "types": {"campaign:1": "hash"},
    }


@pytest.mark.asyncio
async def test_strict_downgrade_rejected_apply_not_called(monkeypatch):
    """incoming v5 < current v9 → rejected, apply_snapshot never called."""
    from app import sync_client
    from app.config import settings

    monkeypatch.setattr(settings, "sync_url", "http://central:8101")
    _install_httpx(monkeypatch, _snapshot(sync_version=5))

    applied = []

    async def _fake_apply(redis, snap):
        applied.append(snap)
        return {"status": "ok", "keys_written": 1}

    monkeypatch.setattr(sync_client, "apply_snapshot", _fake_apply)

    redis = _Redis(version="9")
    result = await sync_client.pull_from_central(redis)

    assert result == {
        "status": "rejected",
        "reason": "version downgrade",
        "keys_written": 0,
    }
    assert applied == [], "apply_snapshot MUST NOT run on a downgrade pull"
    assert redis.get_calls == ["sync:version"]


@pytest.mark.asyncio
async def test_equal_version_re_applies(monkeypatch):
    """incoming v5 == current v5 → idempotent re-apply (guard is `<`)."""
    from app import sync_client
    from app.config import settings

    monkeypatch.setattr(settings, "sync_url", "http://central:8101")
    _install_httpx(monkeypatch, _snapshot(sync_version=5))

    applied = []

    async def _fake_apply(redis, snap):
        applied.append(snap)
        return {"status": "ok", "keys_written": 1}

    monkeypatch.setattr(sync_client, "apply_snapshot", _fake_apply)

    result = await sync_client.pull_from_central(_Redis(version="5"))

    assert result == {"status": "ok", "keys_written": 1}
    assert len(applied) == 1, "equal version must still re-apply (idempotent)"


@pytest.mark.asyncio
async def test_upgrade_applies(monkeypatch):
    """incoming v12 > current v9 → applies normally."""
    from app import sync_client
    from app.config import settings

    monkeypatch.setattr(settings, "sync_url", "http://central:8101")
    _install_httpx(monkeypatch, _snapshot(sync_version=12))

    applied = []

    async def _fake_apply(redis, snap):
        applied.append(snap)
        return {"status": "ok", "keys_written": 1}

    monkeypatch.setattr(sync_client, "apply_snapshot", _fake_apply)

    result = await sync_client.pull_from_central(_Redis(version="9"))
    assert result == {"status": "ok", "keys_written": 1}
    assert len(applied) == 1


@pytest.mark.asyncio
async def test_fresh_node_applies_first_pull(monkeypatch):
    """current==0 (no sync:version key yet) → guard skipped, first pull
    applies. A brand-new node must converge from its very first pull."""
    from app import sync_client
    from app.config import settings

    monkeypatch.setattr(settings, "sync_url", "http://central:8101")
    _install_httpx(monkeypatch, _snapshot(sync_version=1))

    applied = []

    async def _fake_apply(redis, snap):
        applied.append(snap)
        return {"status": "ok", "keys_written": 1}

    monkeypatch.setattr(sync_client, "apply_snapshot", _fake_apply)

    # redis.get returns None → current = 0 → guard skipped.
    result = await sync_client.pull_from_central(_Redis(version=None))
    assert result == {"status": "ok", "keys_written": 1}
    assert len(applied) == 1, "fresh node's first pull must apply"
