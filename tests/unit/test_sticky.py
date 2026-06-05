"""Sticky binding (v2 Phase S) — sticky.py primitive tests.

First-write-wins NX, explicit re-pin overwrite, sliding TTL on read,
company-scoped key, empty-uid no-op, and fail-open on Redis error.
"""

from __future__ import annotations

import fakeredis.aioredis
import pytest

from app import sticky

pytestmark = pytest.mark.asyncio
TTL = 1000


def _fr():
    return fakeredis.aioredis.FakeRedis(decode_responses=True)


def _patch(monkeypatch, r):
    async def _gir():
        return r
    monkeypatch.setattr(sticky, "get_identity_redis", _gir)


class TestStickyPrimitives:
    async def test_set_nx_first_write_wins(self, monkeypatch):
        r = _fr(); _patch(monkeypatch, r)
        await sticky.set_sticky_nx(1, "U", 10, "7", TTL)
        await sticky.set_sticky_nx(1, "U", 10, "9", TTL)  # NX — ignored
        assert await sticky.get_sticky(1, "U", 10, TTL) == "7"

    async def test_repin_overwrites(self, monkeypatch):
        r = _fr(); _patch(monkeypatch, r)
        await sticky.set_sticky_nx(1, "U", 10, "7", TTL)
        await sticky.repin(1, "U", 10, "9", TTL)  # explicit overwrite
        assert await sticky.get_sticky(1, "U", 10, TTL) == "9"

    async def test_get_none_when_absent(self, monkeypatch):
        r = _fr(); _patch(monkeypatch, r)
        assert await sticky.get_sticky(1, "U", 10, TTL) is None

    async def test_noop_on_empty_uid(self, monkeypatch):
        r = _fr(); _patch(monkeypatch, r)
        await sticky.set_sticky_nx(1, "", 10, "7", TTL)
        assert await r.keys("sticky:*") == []
        assert await sticky.get_sticky(1, "", 10, TTL) is None

    async def test_noop_on_empty_target(self, monkeypatch):
        r = _fr(); _patch(monkeypatch, r)
        await sticky.set_sticky_nx(1, "U", 10, "", TTL)
        assert await r.keys("sticky:*") == []

    async def test_company_scoped_key(self):
        assert sticky.sticky_key(1, "U", 10) == "sticky:1:U:10"
        assert sticky.sticky_key(2, "U", 10) != sticky.sticky_key(1, "U", 10)

    async def test_sliding_ttl_refreshed_on_read(self, monkeypatch):
        r = _fr(); _patch(monkeypatch, r)
        await sticky.set_sticky_nx(1, "U", 10, "7", TTL)
        await r.expire("sticky:1:U:10", 5)  # simulate near-expiry
        await sticky.get_sticky(1, "U", 10, TTL)  # read → refresh
        assert await r.ttl("sticky:1:U:10") > 5

    async def test_fail_open_on_error(self, monkeypatch):
        class _Boom:
            async def get(self, *a, **k):
                raise RuntimeError("redis down")

            async def set(self, *a, **k):
                raise RuntimeError("redis down")

            async def expire(self, *a, **k):
                raise RuntimeError("redis down")

        async def _gir():
            return _Boom()
        monkeypatch.setattr(sticky, "get_identity_redis", _gir)
        # None of these raise — sticky is enrichment, never fails a click.
        assert await sticky.get_sticky(1, "U", 10, TTL) is None
        await sticky.set_sticky_nx(1, "U", 10, "7", TTL)
        await sticky.repin(1, "U", 10, "7", TTL)
