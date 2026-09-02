"""`POST /preview` contract (GTD-R965 / plan route-preview M1.P2).

Programme: ``docs/development/route-preview-2026-08-31/00-ANCHOR.md``

THE LOAD-BEARING TEST IN THIS FILE is `test_preview_performs_zero_writes`.
Everything the feature is allowed to claim rests on it: a preview must not
create a visit, because a created visit would flip `seen_before` for the real
click that follows and corrupt `is_returning` / uniqueness (anchor §21.1).

That test is only worth something if its instrument can go red, so
`test_the_write_detector_can_actually_see_a_write` calibrates the detector
against a deliberate write before anything else trusts it — a detector that
reports "no writes" because it cannot see any is indistinguishable, from the
outside, from a genuinely clean run.
"""

from __future__ import annotations

import asyncio
import json
import re
from pathlib import Path
from unittest.mock import patch

import fakeredis.aioredis
import pytest
from fastapi.testclient import TestClient

from app import identity, main, redis_client as rc, router, sticky
from app.config import settings

HOST = "preview.test"
CAMPAIGN = "77"
FLOW = "900"
OFFER = 1
TARGET = 10
COMPANY = 1
SECRET = "preview-test-sync-secret-aaaaaaaaaaaaaaaa"
ROUTE_KEY = "preview-test-route-code-key-bbbbbbbbbbbb"


# --------------------------------------------------------------------------- #
# A Redis that remembers every mutating call — including inside a pipeline     #
# --------------------------------------------------------------------------- #
_WRITE_VERBS = frozenset({
    "set", "setex", "setnx", "getset", "mset", "msetnx", "append",
    "sadd", "srem", "spop", "smove",
    "hset", "hsetnx", "hdel", "hincrby",
    "expire", "pexpire", "expireat", "persist",
    "xadd", "xtrim", "xdel", "xack", "xgroup",
    "incr", "incrby", "decr", "decrby",
    "delete", "unlink", "rename", "flushdb", "flushall",
    "lpush", "rpush", "lpop", "rpop", "ltrim",
    "zadd", "zincrby", "zrem",
    "getdel", "copy", "restore",
})


class _WriteRecorder:
    """Wrap a Redis client and log every mutating verb it is asked to perform.

    A pipeline is wrapped too: `sticky` and `identity` both buffer writes in
    pipelines, so a recorder that only watched the top-level client would
    report a clean run while a pipeline wrote the whole keyspace. That gap is
    exactly the kind an instrument is expected to have and nobody checks, so
    the calibration test drives a write through BOTH paths.
    """

    def __init__(self, inner, log: list[str], label: str = "client"):
        self._inner = inner
        self._log = log
        self._label = label

    def __getattr__(self, name):
        attr = getattr(self._inner, name)
        if name == "pipeline":
            def _pipeline(*a, **kw):
                return _WriteRecorder(attr(*a, **kw), self._log, "pipeline")
            return _pipeline
        if name in _WRITE_VERBS and callable(attr):
            def _wrapped(*a, **kw):
                self._log.append(f"{self._label}.{name}")
                return attr(*a, **kw)
            return _wrapped
        return attr


class _Store:
    """One in-memory Redis server, handing out a FRESH CLIENT per event loop.

    fakeredis binds a CLIENT to the loop that first drives it, while the data
    lives on the SERVER. Seeding runs under `asyncio.run` and the request runs
    under TestClient's own loop, so a single shared client raises
    "bound to a different event loop" — measured, not anticipated. Two clients
    over one server is the fix, and it keeps the seeding readable.
    """

    def __init__(self):
        self._srv = fakeredis.FakeServer()

    def client(self):
        return fakeredis.aioredis.FakeRedis(
            server=self._srv, decode_responses=True)


def _fake():
    return _Store()


def _run(coro):
    """Drive one coroutine to completion from a sync test body."""
    return asyncio.run(coro)


async def _seed_domain_route(r) -> None:
    """One domain → campaign → company-scope flow → one pinned, active target.

    The simplest shape that produces a real MATCHED decision, so the preview
    under test is exercising the routing engine rather than an early return.
    """
    await r.set(
        f"domain:{HOST}:root",
        json.dumps({"campaign_id": CAMPAIGN, "binding_id": 1,
                    "binding_alias": "root"}),
    )
    await r.sadd("campaigns:active", CAMPAIGN)
    await r.hset(f"campaign:{CAMPAIGN}", mapping={
        "company_id": str(COMPANY), "priority": "0", "weight": "100"})
    await r.rpush(f"campaign:{CAMPAIGN}:flows", FLOW)
    await r.hset(f"flow:{FLOW}", mapping={
        "campaign_id": CAMPAIGN, "scope_type": "company",
        "scope_id": str(COMPANY), "seq_id": "1", "is_default": "0",
        "criteria": "[]", "action_type": "offer",
        "action_config": json.dumps({"offer_id": OFFER, "target_id": TARGET}),
    })
    await r.hset(f"offer_target:{TARGET}", mapping={
        "url": "https://advertiser.example/landing?c={click_id}",
        "is_default": "1", "availability": "active",
        "offer_id": str(OFFER), "criteria": "[]", "priority": "0"})


@pytest.fixture
def enabled(monkeypatch):
    monkeypatch.setattr(settings, "route_preview_enabled", True)
    monkeypatch.setattr(settings, "tds_secret_key", SECRET)
    monkeypatch.setattr(settings, "route_code_keys", f"1:{ROUTE_KEY}")
    monkeypatch.setattr(settings, "route_code_active_kid", "1")


def _post(store, body: dict | None = None, key: str | None = SECRET,
          log: list[str] | None = None):
    payload = {"hostname": HOST, "path": "/", "country": "US",
               "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2)"}
    payload.update(body or {})
    headers = {"X-TDS-Key": key} if key is not None else {}
    conn = store.client()
    ident = store.client()
    if log is not None:
        conn = _WriteRecorder(conn, log, "routing")
        ident = _WriteRecorder(ident, log, "identity")

    async def _get_redis():
        return conn

    async def _get_identity_redis():
        return ident

    # 🔴 BOTH REQUEST-PATH pools, not just the routing one. `identity.py` and
    # `sticky.py` write through `get_identity_redis()` — a SEPARATE client on a
    # dedicated noeviction instance. A recorder wrapped around the routing pool
    # alone reports a clean run while the whole returning-user keyspace is being
    # written, which is the precise blind spot this feature must not have.
    # Measured: with only `router.get_redis` patched, a handler deliberately
    # supplying a `visitor_id` still showed zero writes.
    #
    # ⚠️ "BOTH" is the count of pools a REQUEST can reach, not of pools that
    # exist. `app/redis_client.py` declares THREE factories — `get_redis`,
    # `get_identity_redis` and `get_shipper_redis`. The third is deliberately
    # NOT patched here and its absence is not a gap: its single caller is the
    # app lifespan (`main.py`, `run_shipper`'s blocking XREADGROUP wants its own
    # socket_timeout, TDSP-E20), so no request handler can reach it. Stated
    # because the failure this recorder exists to catch is exactly "an
    # instrument that wrapped one pool of N" — so the N has to be counted, and
    # a later fourth factory has to be judged against this sentence rather than
    # against the word "both". Count the FACTORIES (`get_*_redis`), never the
    # call sites.
    #
    # NOT a context manager: `with TestClient(app)` runs the app lifespan,
    # which dials the real Redis at startup. The rest of this service's
    # endpoint tests construct it the same way, for the same reason.
    with patch.object(router, "get_redis", _get_redis), \
            patch.object(identity, "get_identity_redis", _get_identity_redis), \
            patch.object(sticky, "get_identity_redis", _get_identity_redis):
        return TestClient(main.app).post(
            "/preview", json=payload, headers=headers)


# --------------------------------------------------------------------------- #
# The DARK default and the auth ladder                                         #
# --------------------------------------------------------------------------- #
def test_disabled_by_default_returns_404(monkeypatch):
    """The shipped default. 404, not 403 — a disabled feature must not confirm
    to anyone that it exists."""
    monkeypatch.setattr(settings, "route_preview_enabled", False)
    monkeypatch.setattr(settings, "tds_secret_key", SECRET)

    assert _post(_fake()).status_code == 404


def test_disabled_answers_404_even_with_a_valid_key(monkeypatch):
    """The flag is checked BEFORE auth, so a correct credential does not
    reveal the endpoint either."""
    monkeypatch.setattr(settings, "route_preview_enabled", False)
    monkeypatch.setattr(settings, "tds_secret_key", SECRET)

    assert _post(_fake(), key=SECRET).status_code == 404


@pytest.mark.parametrize("key", [None, "", "wrong-key"])
def test_enabled_but_unauthenticated_is_403(enabled, key):
    assert _post(_fake(), key=key).status_code == 403


# --------------------------------------------------------------------------- #
# 🔴 The invariant: a preview creates nothing                                  #
# --------------------------------------------------------------------------- #
def test_the_recorder_still_covers_every_pool_a_request_can_reach():
    """🔴 The ratchet behind the recorder's comment above.

    `test_preview_performs_zero_writes` is only as wide as the set of clients it
    wraps, and that set is currently spelled out in a COMMENT. A comment cannot
    notice a fourth `get_*_redis` factory being added — and the failure mode of
    this whole feature, recorded when it was nearly shipped, is precisely an
    instrument that wrapped one pool of N and reported purity while another was
    written.

    So the factory set is pinned. If this goes red, do NOT widen the list to
    match: decide first whether a REQUEST can reach the new pool. If it can, the
    recorder must wrap it (add it to `_post`); if it cannot — as with
    `get_shipper_redis`, whose only caller is the app lifespan — say so here,
    with the caller named, and then extend the set.
    """
    src = (Path(rc.__file__)).read_text(encoding="utf-8")
    factories = set(re.findall(r"^async def (get_\w*redis)\(", src, re.M))

    assert factories == {"get_redis", "get_identity_redis", "get_shipper_redis"}, (
        "the set of Redis client factories changed; see this test's docstring "
        f"before touching it. Found: {sorted(factories)}"
    )

    # And the exclusion is a FACT about the app, not an assumption: the shipper
    # factory has exactly ONE call site, and it is the lifespan.
    #
    # ⚠️ Counted as CALLS, not as occurrences of the string. The first version of
    # this assertion counted `main_src.count("get_shipper_redis")` and expected
    # 2 (an import plus a call) — it found 3, because the paragraph above that
    # call also names the function in prose. A grep-shaped check cannot tell a
    # mention from a use, which is the same class of error this whole test
    # exists to guard against, one level up.
    main_src = (Path(main.__file__)).read_text(encoding="utf-8")
    call_lines = [
        ln for ln in main_src.splitlines()
        if "get_shipper_redis(" in ln and not ln.lstrip().startswith("#")
    ]
    assert len(call_lines) == 1, (
        "get_shipper_redis now has more than the single lifespan call site — "
        f"check whether a request path can reach that pool. Found: {call_lines}"
    )
    assert "await get_shipper_redis()" in call_lines[0]


@pytest.mark.asyncio
async def test_the_write_detector_can_actually_see_a_write():
    """Calibration. Drive a write through BOTH the client and a pipeline and
    require the recorder to name each one — otherwise the zero-writes result
    below is a statement about the instrument, not about the code."""
    log: list[str] = []
    r = _WriteRecorder(_fake().client(), log)

    await r.set("canary", "1")
    pipe = r.pipeline()
    pipe.sadd("canary-set", "x")
    await pipe.execute()

    assert "client.set" in log
    assert "pipeline.sadd" in log


def test_preview_performs_zero_writes(enabled):
    """🔴 THE ONE THAT MATTERS.

    A preview must not create a visit. If it did, `seen_before` would flip for
    the real click that follows, and `is_unique` / `is_returning` would describe
    a visit the user never made.

    This holds without a preview-specific branch anywhere in the engine: with no
    identity signal `resolve_identity` returns `uid=""` on its documented
    "Case A … No writes" path, and every sticky verb is gated on `bool(uid)`.
    """
    log: list[str] = []
    store = _fake()
    _run(_seed_domain_route(store.client()))

    resp = _post(store, log=log)

    # Assert the engine actually REACHED a decision first. Without this the
    # test would still pass if the seed drifted and the route stopped
    # matching — a preview that routes nowhere writes nothing either, and
    # that green would be about an empty world, not about the invariant.
    assert resp.status_code == 200
    assert resp.json()["matched"] is True
    assert resp.json()["offer_target_id"] == TARGET

    assert log == [], f"the preview wrote to Redis: {log}"


def test_preview_leaves_the_keyspace_byte_identical(enabled):
    """The end-state twin of the test above: not merely 'no write calls' but
    'nothing changed'. Two instruments, one claim — a TTL-only write would
    escape this one, which is why the call recorder exists as well."""
    store = _fake()
    _run(_seed_domain_route(store.client()))
    before = _run(store.client().keys("*"))

    assert _post(store).status_code == 200

    after = _run(store.client().keys("*"))
    assert sorted(before) == sorted(after)


# --------------------------------------------------------------------------- #
# The answer itself                                                            #
# --------------------------------------------------------------------------- #
def test_a_matched_preview_returns_the_decision_and_a_code(enabled):
    store = _fake()
    _run(_seed_domain_route(store.client()))

    body = _post(store).json()

    assert body["matched"] is True
    assert body["offer_id"] == OFFER
    assert body["offer_target_id"] == TARGET
    assert body["route_code"]
    assert body["expires_at"] > 0


def test_the_returned_code_verifies_and_names_the_same_target(enabled):
    """End-to-end for the pair: what /preview mints, route_code.verify accepts,
    and it names the decision the engine actually made."""
    from app import route_code

    store = _fake()
    _run(_seed_domain_route(store.client()))

    decision = route_code.verify(_post(store).json()["route_code"])

    assert decision is not None
    assert decision.company_id == COMPANY
    assert decision.offer_id == OFFER
    assert decision.offer_target_id == TARGET


def test_a_matched_preview_still_answers_when_the_codec_is_inert(
    enabled, monkeypatch
):
    """The codec and the endpoint are independently gated: a node that can
    route but has no signing ring must still answer the question, with
    `route_code: null` rather than a 500."""
    monkeypatch.setattr(settings, "route_code_keys", "")
    monkeypatch.setattr(settings, "route_code_active_kid", "")

    store = _fake()
    _run(_seed_domain_route(store.client()))

    body = _post(store).json()

    assert body["matched"] is True
    assert body["offer_target_id"] == TARGET
    assert body["route_code"] is None
    assert body["expires_at"] is None


def test_no_campaign_is_a_normal_answer_not_an_error(enabled):
    """A visitor with no route under this link is an ordinary outcome."""
    body = _post(_fake(), {"hostname": "nothing-here.test"})

    assert body.status_code == 200
    assert body.json()["matched"] is False
    assert body.json()["route_code"] is None


# --------------------------------------------------------------------------- #
# What must never leave the node                                               #
# --------------------------------------------------------------------------- #
def test_the_response_carries_no_commercially_sensitive_field(enabled):
    """🔴 The engine that produced this answer had the advertiser's real
    tracking URL, the payout and the targeting ruleset in hand. The omission
    has to be deliberate and checked, not incidental (anchor §9.1).
    """
    store = _fake()
    _run(_seed_domain_route(store.client()))

    raw = _post(store).text
    body = _post(store).json()

    for forbidden in (
        "url_template", "payout", "criteria", "partner", "settings",
        "advertiser.example", "campaign_id", "flow_id", "seq_id",
    ):
        assert forbidden not in raw, f"{forbidden} leaked into the response"

    assert set(body) == {
        "matched", "offer_id", "offer_target_id", "route_code",
        "expires_at", "reason",
        # R24/R25 (the four-outcome contract) — a refusal verdict and a
        # validation echo for AUTHED callers. Neither is commercial data:
        # the verdict names OUR decision about the presented credential, the
        # echo names whether OUR check ran. Both are null on this unkeyed
        # request; their semantics are pinned in
        # test_route_preview_tenant_check.py.
        "preview_denied", "tenant_checked",
    }


def test_the_request_model_cannot_carry_an_identity(enabled):
    """Structural, not incidental: `PreviewRequest` must have no field through
    which a caller could hand us a visitor identity. If one were ever added,
    the preview would start resolving — and possibly minting — an identity,
    manufacturing a visit that never happened."""
    from app.models import PreviewRequest

    fields = set(PreviewRequest.model_fields)

    assert "visitor_id" not in fields
    assert "identity_token" not in fields
    assert "funnel_user_id" not in fields
    assert "is_returning" not in fields


def test_preview_query_params_coerce_exactly_like_a_click(enabled):
    """`PreviewRequest` reuses `ClickRequest`'s coercion through a private
    pydantic attribute. Pin the behaviour so an internals change fails here
    loudly instead of silently diverging the two shapes."""
    from app.models import ClickRequest, PreviewRequest

    raw = {"a": 1, "b": True, "c": "x"}

    assert (
        PreviewRequest(hostname=HOST, query_params=raw).query_params
        == ClickRequest(click_id="x1", query_params=raw).query_params
    )
