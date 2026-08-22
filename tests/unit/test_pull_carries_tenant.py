"""GTD-R855 — the config-snapshot PULL must state which tenant it is.

🔴 WHY THIS FILE EXISTS. The B4 per-tenant delivery gate makes admin-api
refuse an UNQUALIFIED snapshot request with 409, because the request
authenticates with the fleet-wide ``X-TDS-Key`` and that key cannot say
which company is asking. The node's own ``pull_from_central`` was sending
exactly such a request. Once ``TDS_SYNC_PER_TENANT_DELIVERY_ENABLED`` is on,
every periodic pull would 409 behind nothing louder than a
``logger.warning`` — the node would serve its LAST config forever, and a
FRESH node bootstrapping by pull would come up with no routing data at all.

It was dormant when found (``TDS_SYNC_URL`` defaults empty ⇒ pull disabled,
and no non-empty setter exists anywhere in the repo), which is why it is a
CUTOVER blocker rather than a live outage. This file is what stops it
becoming one.

**The parameter is a CLAIM, not authorisation** — see the last test. The
node asserts which tenant it is; nothing here authenticates that assertion.
That property is asserted deliberately so a future reader cannot mistake
this for an isolation boundary.
"""

from __future__ import annotations

import pytest


class _Resp:
    status_code = 200

    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


def _client_capturing(seen: list[str], payload=None):
    """An httpx.AsyncClient stand-in that records the URL it was given."""

    class _Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def get(self, url, headers=None, **kw):
            seen.append(url)
            return _Resp(payload if payload is not None else {"data": {}})

    return _Client


class _Redis:
    """Enough Redis for the version guard; the pull returns before apply."""

    async def get(self, _key):
        return None


@pytest.mark.asyncio
async def test_pull_carries_company_id_when_the_node_knows_its_tenant(
    monkeypatch,
):
    import httpx
    from app import sync_client
    from app.config import settings

    monkeypatch.setattr(settings, "sync_url", "http://central:8101")
    monkeypatch.setattr(settings, "company_id", 38)

    seen: list[str] = []
    monkeypatch.setattr(httpx, "AsyncClient", lambda *a, **k: _client_capturing(seen)())

    await sync_client.pull_from_central(_Redis())

    assert len(seen) == 1, "the pull must have been issued exactly once"
    assert seen[0] == (
        "http://central:8101/api/system/sync/snapshot?company_id=38"
    ), (
        "the pull must name its tenant — without it admin-api answers 409 "
        "once per-tenant delivery is on, and the node silently stops "
        f"updating. Got: {seen[0]}"
    )


@pytest.mark.asyncio
async def test_pull_is_unqualified_when_the_tenant_is_unset(monkeypatch):
    """The OTHER direction, so this file can fail in both.

    company_id unset (0) ⇒ no parameter. That is correct while per-tenant
    delivery is off, and it is exactly the request admin-api refuses once it
    is on — the refusal is the point, not an accident.
    """
    import httpx
    from app import sync_client
    from app.config import settings

    monkeypatch.setattr(settings, "sync_url", "http://central:8101")
    monkeypatch.setattr(settings, "company_id", 0)

    seen: list[str] = []
    monkeypatch.setattr(httpx, "AsyncClient", lambda *a, **k: _client_capturing(seen)())

    await sync_client.pull_from_central(_Redis())

    assert seen == ["http://central:8101/api/system/sync/snapshot"], (
        "an unset tenant must produce the bare URL — appending "
        "'?company_id=0' would ask central for a company that cannot exist"
    )


@pytest.mark.asyncio
async def test_the_tenant_is_a_claim_not_a_credential(monkeypatch):
    """🔴 The honesty pin.

    The node sends whatever ``company_id`` it was configured with, alongside
    the SHARED fleet key. Nothing in this path proves the node is entitled to
    that tenant — a holder of the key can name any company. That is not a
    regression (today the same key returns EVERY tenant's config in one
    response), and closing it needs a per-node credential, which is separate
    work. This test exists so nobody reads the parameter as a check.
    """
    import httpx
    from app import sync_client
    from app.config import settings

    monkeypatch.setattr(settings, "sync_url", "http://central:8101")
    monkeypatch.setattr(settings, "tds_secret_key", "the-one-fleet-key")
    # A tenant this node has no relationship with whatsoever.
    monkeypatch.setattr(settings, "company_id", 999493)

    seen: list[str] = []
    sent_headers: list[dict] = []

    class _Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def get(self, url, headers=None, **kw):
            seen.append(url)
            sent_headers.append(dict(headers or {}))
            return _Resp({"data": {}})

    monkeypatch.setattr(httpx, "AsyncClient", lambda *a, **k: _Client())

    await sync_client.pull_from_central(_Redis())

    assert seen[0].endswith("?company_id=999493"), (
        "the claim goes out verbatim — the node is not consulted about "
        "whether it may make it"
    )
    assert sent_headers[0]["X-TDS-Key"] == "the-one-fleet-key", (
        "and it travels with the SHARED key, which identifies 'a node', "
        "never 'this company's node' — the parameter is a claim, and this "
        "assertion is here so that stays visible"
    )
