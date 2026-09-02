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
    """The innocent twin for a KEY-HOLDING caller (R24 outcome b's I-8
    population): the SAME valid same-tenant hash probing a hostname that
    routes nothing. Measured from the running handler, never hand-written —
    under a present index the keyed no-route ladder carries
    `tenant_checked: true` exactly as outcome (b) does, so whole-body parity
    is the honest claim. (The unkeyed empty-store probe that stood here
    before R24 stopped being the right twin the moment answers grew the
    R25 echo: a prober WITH a key compares against keyed probes.)"""
    store = _seeded_store(key_company=str(COMPANY))
    return _post_keyed(store, body={"hostname": "never-bound.example",
                                    "preview_key_hash": KEY_HASH})


# --------------------------------------------------------------------------- #
# The check itself                                                             #
# --------------------------------------------------------------------------- #
def test_cross_tenant_key_is_byte_identical_to_a_dead_link(enabled):  # noqa: F811
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


def test_unknown_hash_with_present_index_carries_the_refusal_verdict(enabled):  # noqa: F811
    """R24 outcome (c) — the seam fix. A hash missing from a PRESENT index is
    the revoked/garbage-key shape, and the answer now carries the IN-BAND
    verdict the Worker's fall-to-click predicate reads (`preview_denied` —
    the ONLY field that sends it to the click path, which is what makes row 2
    implementable across the chain). The BODY stays the dead-link shape; the
    verdict rides beside it and the public never sees it — the worker
    collapses it into an ordinary click, which IS the non-oracular public
    answer."""
    store = _seeded_store(key_company=None)  # marker present, no key entry

    refused = _post_keyed(store, body={"preview_key_hash": KEY_HASH})

    body = refused.json()
    assert body["matched"] is False
    assert body["reason"] == "blocked"
    assert body["preview_denied"] == "key_refused"
    assert body["tenant_checked"] is True


def test_matching_company_key_previews_normally(enabled):  # noqa: F811
    """Calibration leg 2: the comparison reads the VALUE. The right company
    passes — so the refusals above are the comparison, not key-presence."""
    store = _seeded_store(key_company=str(COMPANY))

    resp = _post_keyed(store, body={"preview_key_hash": KEY_HASH})

    body = resp.json()
    assert body["matched"] is True
    assert body["offer_id"] == OFFER


def test_absent_field_is_todays_behaviour(enabled):  # noqa: F811
    """Calibration leg 1: the SAME fixture matches when no key is presented —
    proving the cross-tenant `false` above comes from the check, and pinning
    the rollout property (no caller sends the field today ⇒ byte-identical)."""
    store = _seeded_store(key_company=str(COMPANY + 1))

    assert _post_keyed(store).json()["matched"] is True


def test_malformed_hash_is_refused_before_it_reaches_the_lookup(enabled):  # noqa: F811
    """Schema, not handler: a non-sha256-shaped value never reaches the
    lookup. The status is 403, not 422, and that is THIS SERVICE'S OWN
    convention (D2/V17, `_validation_error_handler`): a validation error must
    not describe the request schema in front of the auth gate, so malformed
    and well-formed-but-unauthenticated are indistinguishable."""
    store = _seeded_store(key_company=None)

    resp = _post_keyed(store, body={"preview_key_hash": "not-a-hash"})

    assert resp.status_code == 403


def test_keyed_preview_still_performs_zero_writes(enabled):  # noqa: F811
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
def test_unsynced_node_answers_no_verdict_and_no_echo(enabled):  # noqa: F811
    """R24 outcome (d), and the trap the obvious fix walks into: a DEGRADED
    node must never claim `key_refused` — the Worker would turn a preview
    stream above click volume into a CLICK stream the moment sync breaks
    (R3 reopened through degradation) — and must never claim
    `tenant_checked` either, or a validated "no route" and a
    could-not-validate collapse into one answer. So (d) carries NEITHER
    field, the throttled op fires (ours to see), and (c) differs from (d) in
    exactly those two fields while the public-visible core (matched/reason)
    stays identical — the §11 wire-identity claim survives at the PUBLIC
    boundary (admin-api collapses both to bare matched:false) and is
    deliberately superseded at the authed hop, where the Worker NEEDS the
    difference: (d) reads as no-verdict → 503, never a click, never an
    answer."""
    unsynced = _seeded_store(key_company=None, marker=False)
    synced = _seeded_store(key_company=None, marker=True)

    with patch.object(main, "capture_op_msg_throttled") as op:
        degraded = _post_keyed(unsynced, body={"preview_key_hash": KEY_HASH})
    assert op.call_count == 1
    assert op.call_args.args[0] == telemetry.OP_PREVIEW_KEYS_UNSYNCED

    with patch.object(main, "capture_op_msg_throttled") as op:
        refused = _post_keyed(synced, body={"preview_key_hash": KEY_HASH})
    assert op.call_count == 0, "marker present — a plain miss must NOT alarm"

    d, c = degraded.json(), refused.json()
    assert d["preview_denied"] is None and d["tenant_checked"] is None
    assert c["preview_denied"] == "key_refused" and c["tenant_checked"] is True
    assert (d["matched"], d["reason"]) == (c["matched"], c["reason"]) == (False, "blocked")


def test_absent_hash_answers_carry_neither_new_field(enabled):  # noqa: F811
    """The rollout pin, R24 edition: a caller that presents NO hash gets
    null for both new fields — the pre-R24 shape plus two nulls, which is
    what keeps the admin-api path and every old caller byte-stable."""
    store = _seeded_store(key_company=None)

    body = _post_keyed(store).json()
    assert body["matched"] is True
    assert body["preview_denied"] is None
    assert body["tenant_checked"] is None


def test_matched_answer_under_a_key_carries_the_echo(enabled):  # noqa: F811
    """R24 outcome (a) + R25: a validated, matching preview says so — the
    field the Worker requires whenever it SENT a hash, which is what turns
    deploy order from operator memory into structure (a pre-R24 node that
    IGNORED the hash answers without it, and the caller maps that to
    no-verdict → 503, never to a validated preview)."""
    store = _seeded_store(key_company=str(COMPANY))

    body = _post_keyed(store, body={"preview_key_hash": KEY_HASH}).json()
    assert body["matched"] is True
    assert body["tenant_checked"] is True
    assert body["preview_denied"] is None
