"""Node-side tenant check on /preview — edge-preview P2.2 (anchor §8 R19, §11).

The check under test: a preview presenting `preview_key_hash` is answered the
DEAD-LINK shape — `matched:false` with the MEASURED dead-link reason
("blocked": the geo path fails closed; the parity tests below compare against
a LIVE probe, which is how the ruled literal "no_campaign" was refuted),
byte-identical to probing a URL that routes nothing (I-8) — when the hash
resolves to a DIFFERENT company than the routed attribution, or resolves to
nothing at all (which is exactly what a REVOKED key's hash looks like once the
sync builder drops it). An absent field leaves behaviour byte-identical to
today, which is what makes the rollout a non-event.

CALIBRATION IS STRUCTURAL, not monkeypatched, and it is the triangle the
lead asked to see first:

  * the SAME fixture with the field ABSENT answers `matched:true` — so the
    route genuinely matches, and the `false` in the cross-tenant test is MY
    check firing, not a broken fixture;
  * the SAME fixture with the RIGHT company answers `matched:true` — so the
    comparison reads the VALUE, not merely the key's presence;
  * delete either stage of the check in main.py and the cross-tenant /
    unknown-hash tests here go RED on `matched` (run before landing; the PR
    body carries the command and its output).

Helpers are imported from test_route_preview_endpoint (the sibling-import
precedent is test_route_code_seam.py). `_post` is NOT imported: the sibling's
deliberately patches only the pools a preview could reach BEFORE this change
(`router.get_redis` + both identity pools). The tenant check reads through
MAIN's own `get_redis` binding, so this file's `_post_keyed` patches that too
— reusing `_post` verbatim would have dialled a real Redis from the new code
path and failed for a reason that says nothing about the check.
"""

from __future__ import annotations

from unittest.mock import patch

from fastapi.testclient import TestClient

from app import identity, main, router, sticky, telemetry
from tests.unit.test_route_preview_endpoint import (
    COMPANY,
    HOST,
    OFFER,
    SECRET,
    _fake,
    _run,
    _seed_domain_route,
    _WriteRecorder,
    enabled,  # noqa: F401 — imported fixture, used by name in signatures
)

# 64 hex chars — shape-valid for PreviewRequest.preview_key_hash. The VALUE
# under the key decides the outcome; the hash itself is arbitrary in tests
# (production hashes a real token; the node never sees the token).
KEY_HASH = "ab" * 32
MARKER = "preview_keys:synced"


def _post_keyed(store, body: dict | None = None, log: list[str] | None = None):
    payload = {"hostname": HOST, "path": "/", "country": "US",
               "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2)"}
    payload.update(body or {})
    conn = store.client()
    ident = store.client()
    if log is not None:
        conn = _WriteRecorder(conn, log, "routing")
        ident = _WriteRecorder(ident, log, "identity")

    async def _get_redis():
        return conn

    async def _get_identity_redis():
        return ident

    with patch.object(router, "get_redis", _get_redis), \
            patch.object(main, "get_redis", _get_redis), \
            patch.object(identity, "get_identity_redis", _get_identity_redis), \
            patch.object(sticky, "get_identity_redis", _get_identity_redis):
        return TestClient(main.app).post(
            "/preview", json=payload, headers={"X-TDS-Key": SECRET})


def _seeded_store(*, key_company: str | None, marker: bool = True):
    """One fake server holding the standard matched route (company COMPANY)
    plus, optionally, the preview-key index entry and the sync marker."""
    store = _fake()

    async def _seed():
        r = store.client()
        await _seed_domain_route(r)
        if key_company is not None:
            await r.set(f"preview_key:{KEY_HASH}", key_company)
        if marker:
            await r.set(MARKER, "1")

    _run(_seed())
    return store


def _dead_link_answer():
    """The innocent twin: a link that routes NOTHING, measured from the
    running handler rather than hand-written. Deliberately probed against an
    EMPTY store: in the seeded fixture an unbound hostname is not dead — the
    geo fallback still finds the seeded campaign and answers `blocked`
    (measured; an artifact of the seed, not of dead links). An attacker's
    baseline for "this URL does not exist" is a URL that resolves nowhere,
    which is exactly what an empty store models."""
    return _post_keyed(_fake(), body={"hostname": "never-bound.example"})


# --------------------------------------------------------------------------- #
# The check itself                                                             #
# --------------------------------------------------------------------------- #
def test_cross_tenant_key_is_byte_identical_to_a_dead_link(enabled):
    """🔴 THE ONE THE RULING IS ABOUT. A key of company 2 asking about a
    company-1 campaign gets EXACTLY what a random dead URL gets — compared as
    whole response bodies against a live dead-link probe, not against a
    hand-written expectation, so a drift in either shape breaks this test."""
    store = _seeded_store(key_company=str(COMPANY + 1))

    refused = _post_keyed(store, body={"preview_key_hash": KEY_HASH})
    dead = _dead_link_answer()

    assert refused.status_code == 200 == dead.status_code
    assert refused.json() == dead.json()
    assert refused.json()["matched"] is False


def test_unknown_hash_refuses_with_the_dead_link_shape(enabled):
    """A hash the store does not hold IS the revoked-key shape: the builder
    drops a revoked key and the managed-keys sweep deletes it, so 'present
    but missing' must refuse or node-side revocation is vacuous."""
    store = _seeded_store(key_company=None)  # marker present, no key entry

    refused = _post_keyed(store, body={"preview_key_hash": KEY_HASH})

    assert refused.json() == _dead_link_answer().json()


def test_matching_company_key_previews_normally(enabled):
    """Calibration leg 2: the comparison reads the VALUE. The right company
    passes — so the refusals above are the comparison, not key-presence."""
    store = _seeded_store(key_company=str(COMPANY))

    resp = _post_keyed(store, body={"preview_key_hash": KEY_HASH})

    body = resp.json()
    assert body["matched"] is True
    assert body["offer_id"] == OFFER


def test_absent_field_is_todays_behaviour(enabled):
    """Calibration leg 1: the SAME fixture matches when no key is presented —
    proving the cross-tenant `false` above comes from the check, and pinning
    the rollout property (no caller sends the field today ⇒ byte-identical)."""
    store = _seeded_store(key_company=str(COMPANY + 1))

    assert _post_keyed(store).json()["matched"] is True


def test_malformed_hash_is_refused_before_it_reaches_the_lookup(enabled):
    """Schema, not handler: a non-sha256-shaped value never reaches the
    lookup. The status is 403, not 422, and that is THIS SERVICE'S OWN
    convention (D2/V17, `_validation_error_handler`): a validation error must
    not describe the request schema in front of the auth gate, so malformed
    and well-formed-but-unauthenticated are indistinguishable."""
    store = _seeded_store(key_company=None)

    resp = _post_keyed(store, body={"preview_key_hash": "not-a-hash"})

    assert resp.status_code == 403


def test_keyed_preview_still_performs_zero_writes(enabled):
    """I-1 extended to the new path: the tenant check adds a GET (and, on a
    miss, an EXISTS) — reads only. The recorder that caught the identity-pool
    blindness (hazard 9) watches both request-path pools here too."""
    store = _seeded_store(key_company=str(COMPANY))
    log: list[str] = []

    resp = _post_keyed(store, body={"preview_key_hash": KEY_HASH}, log=log)

    assert resp.json()["matched"] is True
    assert log == [], f"a keyed preview wrote: {log}"


# --------------------------------------------------------------------------- #
# The degraded-state discriminator (anchor §11)                                #
# --------------------------------------------------------------------------- #
def test_unsynced_node_fires_the_op_and_answers_the_identical_shape(enabled):
    """Marker ABSENT + miss ⇒ the throttled op fires (ours to see) and the
    wire answer is the SAME dead-link shape — both halves of the ruling in
    one test, the wire half measured by comparing bodies across the two
    branches rather than asserting a literal."""
    unsynced = _seeded_store(key_company=None, marker=False)
    synced = _seeded_store(key_company=None, marker=True)

    with patch.object(main, "capture_op_msg_throttled") as op:
        degraded = _post_keyed(unsynced, body={"preview_key_hash": KEY_HASH})
    assert op.call_count == 1
    assert op.call_args.args[0] == telemetry.OP_PREVIEW_KEYS_UNSYNCED

    with patch.object(main, "capture_op_msg_throttled") as op:
        genuine = _post_keyed(synced, body={"preview_key_hash": KEY_HASH})
    assert op.call_count == 0, "marker present — a plain miss must NOT alarm"

    assert degraded.json() == genuine.json(), (
        "the degraded and genuine branches must be indistinguishable on the wire"
    )
