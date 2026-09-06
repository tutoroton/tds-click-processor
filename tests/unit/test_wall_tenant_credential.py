"""The wall path requires a PER-TENANT credential, not just the infra key.

WHAT WAS MISSING, AND WHY IT IS NOT THE SAME AS G4.3.

test_wall_cross_tenant.py answers a DIFFERENT question: a wall RECORD owned by
another company, planted into this campaign's list, is not served. That holds,
and nothing here changes it.

This file is about the CALLER. Before 2026-09-06 the wall endpoint's only
authentication was X-TDS-Key, which authenticates the WORKER: _check_tds_key
returns a worker_id, its own docstring calls that return "advisory", and the
handler discarded it. So any caller holding the shared edge secret could ask for
ANY company's catalogue and receive that company's own, entirely legitimate wall
- while the sibling preview endpoint required a second, tenant-scoped key on top
(preview_key_hash resolved through redis to a company, compared against the
resolved campaign's company).

The two questions are orthogonal: "is this wall allowed to be in this campaign"
versus "is this caller allowed to ask about this campaign". G4.3 answers the
first. Nothing answered the second.

IT WAS NEVER A DESIGN DECISION. The programme's own change-surface document says
the two SHOULD share "the key mechanism (route_preview_keys, its Redis
resolution and the cross-tenant guard)" (02-CHANGE-SURFACE.md section 3), and no
decision record says otherwise. The endpoint shipped without it, and nothing was
reachable enough to notice: the wall is dark, and until the edge gained a
tds_wall branch (ADR-0507) no request could arrive at all. The exposure would
have become live in the same change that made walls usable.

THE LOAD-BEARING TEST is test_a_valid_key_for_another_company_is_refused. The
others exist so a reader can tell a refusal from an outage, which is the
distinction the degraded-sync case turns on.
"""

import asyncio

from tests.unit.test_wall_endpoint import (  # noqa: F401  (fixtures are used)
    CAMPAIGN,
    COMPANY,
    _fake,
    _post,
    _seed,
    armed,
)

HASH_OURS = "a" * 64
HASH_THEIRS = "b" * 64
HASH_UNKNOWN = "c" * 64


def _seed_keys(store, *, synced=True, ours=True, theirs=True):
    async def _go():
        r = store.client()
        await _seed(r)
        if synced:
            await r.set("preview_keys:synced", "1")
        if ours:
            await r.set(f"preview_key:{HASH_OURS}", str(COMPANY))
        if theirs:
            await r.set(f"preview_key:{HASH_THEIRS}", str(COMPANY + 9000))
    asyncio.run(_go())


def test_no_key_presented_still_serves(armed):  # noqa: F811
    store = _fake()
    _seed_keys(store)
    body = _post(store).json()
    assert body["matched"] is True
    assert body["tiles"]
    assert body["tenant_checked"] is None


def test_our_own_key_serves_and_echoes_the_check(armed):  # noqa: F811
    """A key bound to THIS company: served, and the answer says it was checked,
    so a caller can tell a validated pass from an older node that ignored the
    field entirely."""
    store = _fake()
    _seed_keys(store)
    body = _post(store, {"preview_key_hash": HASH_OURS}).json()
    assert body["matched"] is True
    assert body["tiles"]
    assert body["tenant_checked"] is True


def test_a_valid_key_for_another_company_is_refused(armed):  # noqa: F811
    """THE ONE THIS FILE EXISTS FOR.

    A perfectly valid key belonging to company COMPANY+9000, asking about a
    campaign owned by COMPANY. The answer is the dead-catalogue shape - byte
    identical to a link that simply has no wall - so the caller learns exactly
    what probing a random URL would have told them. No existence oracle.
    """
    store = _fake()
    _seed_keys(store)
    body = _post(store, {"preview_key_hash": HASH_THEIRS}).json()
    assert body["matched"] is False
    assert body["tiles"] == []
    assert body["wall_id"] is None
    assert body["reason"] == "no_wall"
    assert body["tenant_checked"] is True


def test_an_unknown_key_under_a_present_index_is_refused_in_band(armed):  # noqa: F811
    """Unknown, revoked, or newer than the last sync tick. The body stays the
    dead-catalogue shape; wall_denied is the verdict the WORKER consumes to
    fall through to an ordinary click, and the public never sees it."""
    store = _fake()
    _seed_keys(store)
    body = _post(store, {"preview_key_hash": HASH_UNKNOWN}).json()
    assert body["matched"] is False
    assert body["wall_denied"] == "key_refused"
    assert body["tenant_checked"] is True


def test_a_degraded_node_never_claims_a_refusal(armed):  # noqa: F811
    """The index has never synced to this node. That is an OUTAGE, not a probe.

    If this answered key_refused the caller would fall through to a click, so a
    sync failure would silently convert every wall request into click volume -
    the same class the preview programme closed. It must be distinguishable, and
    the only thing that distinguishes it is the ABSENCE of both fields.
    """
    store = _fake()
    _seed_keys(store, synced=False, ours=False, theirs=False)
    body = _post(store, {"preview_key_hash": HASH_UNKNOWN}).json()
    assert body["matched"] is False
    assert body.get("wall_denied") is None
    assert body.get("tenant_checked") is None


def test_a_dead_domain_under_a_valid_key_still_echoes_the_check(armed):  # noqa: F811
    """Not cosmetic. A missing tenant_checked reads as "an older node ignored
    the field", which the caller answers with 503 - so without this echo a keyed
    caller probing a domain with no campaign would get an OUTAGE shape instead
    of an honest "no wall"."""
    store = _fake()
    _seed_keys(store)
    body = _post(store, {"hostname": "nosuch.example",
                         "preview_key_hash": HASH_OURS}).json()
    assert body["matched"] is False
    assert body["reason"] == "no_campaign"
    assert body["tenant_checked"] is True
