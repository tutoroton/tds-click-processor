"""F-4 MEDIUM (audit 2026-05-25) — pull_from_central uses async httpx,
not blocking urllib.urlopen.

pull_from_central runs on the periodic-pull task inside the event loop.
A blocking `urllib.urlopen` stalled every other coroutine (shipper,
observability, /decide) for the request duration. The fix swaps to an
awaited httpx.AsyncClient. Source-pin so a refactor can't reintroduce the
blocking call.
"""

from __future__ import annotations

import inspect

import pytest


def test_pull_from_central_uses_async_httpx():
    from app.sync_client import pull_from_central

    source = inspect.getsource(pull_from_central)
    assert "httpx.AsyncClient" in source, (
        "pull_from_central must use httpx.AsyncClient (async, non-blocking)."
    )
    assert "await http_client.get" in source
    # The blocking urllib primitives must be gone.
    assert "urlopen(" not in source, (
        "Blocking urllib.urlopen re-entered pull_from_central — it stalls "
        "the event loop. Use the awaited httpx client."
    )


def test_sync_client_module_does_not_import_urllib():
    from app import sync_client

    src = inspect.getsource(sync_client)
    assert "from urllib.request import" not in src
    assert "import httpx" in src


@pytest.mark.asyncio
async def test_pull_catches_timeout_returns_none(monkeypatch):
    """A network timeout (httpx.TimeoutException ⊂ httpx.HTTPError) must be
    caught → return None, never crash the periodic-pull task."""
    import httpx
    from app import sync_client
    from app.config import settings

    monkeypatch.setattr(settings, "sync_url", "http://central:8101")

    class _Boom:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def get(self, *a, **k):
            raise httpx.TimeoutException("timed out")

    monkeypatch.setattr(httpx, "AsyncClient", lambda *a, **k: _Boom())
    result = await sync_client.pull_from_central(object())
    assert result is None


@pytest.mark.asyncio
async def test_pull_catches_http_500_returns_none(monkeypatch):
    """raise_for_status() on a 5xx raises HTTPStatusError ⊂ httpx.HTTPError
    → caught → None."""
    import httpx
    from app import sync_client
    from app.config import settings

    monkeypatch.setattr(settings, "sync_url", "http://central:8101")

    class _Resp:
        def raise_for_status(self):
            raise httpx.HTTPStatusError(
                "500", request=None, response=None,
            )

        def json(self):
            return {}

    class _Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def get(self, *a, **k):
            return _Resp()

    monkeypatch.setattr(httpx, "AsyncClient", lambda *a, **k: _Client())
    result = await sync_client.pull_from_central(object())
    assert result is None
