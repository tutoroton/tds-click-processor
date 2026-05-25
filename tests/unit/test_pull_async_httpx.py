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
