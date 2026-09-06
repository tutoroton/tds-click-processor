"""`POST /wall` — the offer wall's read side (G5.1 / G5.2 / G5.3 + the Gate).

Programme: ``docs/development/offerwall-2026-09-04/30-IMPLEMENTATION-ANCHOR.md``

THE THREE LOAD-BEARING PROPERTIES, and each has its own calibration because
none of them can be trusted from a green run alone:

  G5.1  flag OFF ⇒ 404, checked BEFORE auth. A disabled feature must not confirm
        it exists to anyone, authenticated or not.
  G5.2  the admission budget is counted in TILES, not requests (risk A19), and
        it is the WALL's own counter — sharing the preview one would let either
        product exhaust the budget that protects click serving from the other.
  G5.3  an over-limit wall answers 503 and NEVER falls through to a click. A
        wall flood converted into clicks is D149's defect through a new door.

Plus A34's zero-write property: a catalogue fetch must not stamp an identity.
The write recorder wraps BOTH request-reachable pools, for the reason
`test_route_preview_endpoint.py` spells out at length — a recorder on the
routing pool alone reported a clean run while `identity.py` wrote through its
own client.
"""
from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, patch

import fakeredis.aioredis
import pytest
from fastapi.testclient import TestClient

from app import identity, main, router, sticky
from app.config import settings

HOST = "wall.test"
CAMPAIGN = "88"
WALL_A = "910"
WALL_B = "911"
OFFER_1, OFFER_2 = 21, 22
TARGET_1, TARGET_2 = 31, 32
DEAD_TARGET = 39
COMPANY = 3
SECRET = "wall-test-sync-secret-aaaaaaaaaaaaaaaaaaaa"

_WRITE_VERBS = frozenset({
    "set", "setex", "setnx", "getset", "mset", "append",
    "sadd", "srem", "spop", "hset", "hsetnx", "hdel", "hincrby",
    "expire", "pexpire", "expireat", "persist",
    "xadd", "xtrim", "xdel", "xack", "xgroup",
    "incr", "incrby", "decr", "decrby",
    "delete", "unlink", "rename", "flushdb", "flushall",
    "lpush", "rpush", "lpop", "rpop", "ltrim",
    "zadd", "zincrby", "zrem", "getdel", "copy",
})


class _WriteRecorder:
    """Log every mutating verb, including inside a pipeline."""

    def __init__(self, inner, log, label):
        self._inner, self._log, self._label = inner, log, label

    def __getattr__(self, name):
        attr = getattr(self._inner, name)
        if name in _WRITE_VERBS and callable(attr):
            def _spy(*a, **kw):
                self._log.append(f"{self._label}:{name}")
                return attr(*a, **kw)
            return _spy
        return attr

    def pipeline(self, *a, **kw):
        return _WriteRecorder(self._inner.pipeline(*a, **kw),
                              self._log, f"{self._label}:pipe")


class _Store:
    """One fakeredis SERVER, a fresh CLIENT per event loop.

    fakeredis binds a client to the loop that first drives it while the data
    lives on the server; seeding runs under `asyncio.run` and the request under
    TestClient's own loop, so one shared client raises "bound to a different
    event loop".
    """

    def __init__(self):
        self._srv = fakeredis.FakeServer()

    def client(self):
        return fakeredis.aioredis.FakeRedis(
            server=self._srv, decode_responses=True)


def _fake():
    return _Store()


def _tiles(*pairs):
    return json.dumps({"tiles": [
        {"offer_id": o, "target_id": t} for o, t in pairs]})


async def _seed(r, *, walls=("A",), wall_a_tiles=None) -> None:
    """A domain → campaign → one or two company-scoped WALLS with pinned tiles.

    Deliberately NO routing flow: a wall must be reachable on a link that has
    none, and seeding one would let a passing test be explained by routing.
    """
    await r.set(
        f"domain:{HOST}:root",
        json.dumps({"campaign_id": CAMPAIGN, "binding_id": 1,
                    "binding_alias": "root"}),
    )
    await r.sadd("campaigns:active", CAMPAIGN)
    await r.hset(f"campaign:{CAMPAIGN}", mapping={
        "company_id": str(COMPANY), "priority": "0", "weight": "100"})

    # 🔴 A TRUSTED SOURCE, and it is load-bearing for the zero-write tests
    # rather than scenery. Measured directly against `resolve_and_stamp`:
    #   trusted=False, funnel_user_id="u-42" -> uid=''
    #   trusted=True,  funnel_user_id=None   -> uid=''
    #   trusted=True,  funnel_user_id="u-42" -> uid='458ff1...'
    # and the persist is gated `if commit and result.uid`. Without BOTH, a
    # `commit_identity=True` mutation writes nothing either, and "no writes"
    # stops being evidence about this endpoint's gate.
    await r.sadd(f"campaign:{CAMPAIGN}:sources", "55")
    await r.hset("source:55", mapping={
        "slug": "trusted-src", "source_trusted": "1", "param_mappings": "[]"})

    for oid, tid in ((OFFER_1, TARGET_1), (OFFER_2, TARGET_2)):
        await r.hset(f"offer_target:{tid}", mapping={
            "url": "https://advertiser.example/l?c={click_id}",
            "is_default": "1", "availability": "active",
            "offer_id": str(oid), "criteria": "[]", "priority": "0"})
        await r.hset(f"offer:{oid}", mapping={
            "name": f"Offer {oid}", "icon_url": f"https://cdn.example/{oid}.png",
            "company_id": str(COMPANY)})

    # A target that exists but cannot be served — the viability case.
    await r.hset(f"offer_target:{DEAD_TARGET}", mapping={
        "url": "", "is_default": "0", "availability": "closed",
        "offer_id": str(OFFER_1), "criteria": "[]", "priority": "0"})

    if "A" in walls:
        await r.rpush(f"campaign:{CAMPAIGN}:walls", WALL_A)
        await r.hset(f"flow:{WALL_A}", mapping={
            "campaign_id": CAMPAIGN, "scope_type": "company",
            "scope_id": str(COMPANY), "seq_id": "1", "is_default": "0",
            "criteria": "[]", "audience": "offerwall",
            "action_type": "offerwall",
            "action_config": wall_a_tiles or _tiles((OFFER_1, TARGET_1),
                                                    (OFFER_2, TARGET_2)),
        })
    if "B" in walls:
        await r.rpush(f"campaign:{CAMPAIGN}:walls", WALL_B)
        await r.hset(f"flow:{WALL_B}", mapping={
            "campaign_id": CAMPAIGN, "scope_type": "company",
            "scope_id": str(COMPANY), "seq_id": "2", "is_default": "0",
            "criteria": "[]", "audience": "offerwall",
            "action_type": "offerwall",
            "action_config": _tiles((OFFER_2, TARGET_2)),
        })


@pytest.fixture
def armed(monkeypatch):
    monkeypatch.setattr(settings, "offerwall_serve_enabled", True)
    monkeypatch.setattr(settings, "tds_secret_key", SECRET)
    # 🔴 THE RESOLVER IS ON, and that is what makes the zero-write test mean
    # something. With it off the identity block never runs, so "no writes" is
    # true for a reason that has nothing to do with this endpoint's gate —
    # measured: flipping `commit_identity` to True left the suite GREEN until
    # this line existed.
    monkeypatch.setattr(settings, "returning_resolver_enabled", True)


@pytest.fixture(autouse=True)
def _counter_reset():
    """The counter is MODULE state; a test that 503s must not poison the next.

    Autouse and both-ended on purpose — a leak would make the failure appear in
    whichever test happened to run afterwards, which is the hardest kind to read.
    """
    main._wall_tiles_inflight = 0
    yield
    main._wall_tiles_inflight = 0


def _post(store, body=None, key=SECRET, log=None, path="/wall"):
    payload = {"hostname": HOST, "path": "/", "country": "US",
               "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2)"}
    payload.update(body or {})
    headers = {"X-TDS-Key": key} if key is not None else {}
    conn, ident = store.client(), store.client()
    if log is not None:
        conn = _WriteRecorder(conn, log, "routing")
        ident = _WriteRecorder(ident, log, "identity")

    async def _get_redis():
        return conn

    async def _get_identity_redis():
        return ident

    # BOTH request-reachable pools, and patched in every module that binds the
    # name — `patch.object` rebinds inside ONE module, so patching `router` only
    # leaves a `main`-side call dialling a real Redis.
    with patch.object(router, "get_redis", _get_redis), \
            patch.object(main, "get_redis", _get_redis), \
            patch.object(identity, "get_identity_redis", _get_identity_redis), \
            patch.object(sticky, "get_identity_redis", _get_identity_redis):
        # NOT a context manager: `with TestClient(app)` runs the lifespan, which
        # dials the real Redis.
        return TestClient(main.app).post(path, json=payload, headers=headers)


# --------------------------------------------------------------------------- #
# G5.1 — the dark gate                                                         #
# --------------------------------------------------------------------------- #
class TestTheDarkGate:
    def test_disabled_by_default_returns_404(self, monkeypatch):
        monkeypatch.setattr(settings, "offerwall_serve_enabled", False)
        monkeypatch.setattr(settings, "tds_secret_key", SECRET)
        assert _post(_fake()).status_code == 404

    def test_disabled_returns_404_even_with_a_VALID_key(self, monkeypatch):
        """The flag is checked BEFORE auth, so a correct credential does not
        reveal the endpoint either. This is the ordering, not a duplicate of the
        test above: swap the two guards and only this one goes red."""
        monkeypatch.setattr(settings, "offerwall_serve_enabled", False)
        monkeypatch.setattr(settings, "tds_secret_key", SECRET)
        assert _post(_fake(), key=SECRET).status_code == 404

    def test_the_disabled_answer_is_INDISTINGUISHABLE_from_a_missing_route(
        self, monkeypatch
    ):
        """The Gate: flag OFF ⇒ byte-identical absence, no existence oracle.

        Compared against a path that genuinely does not exist, because "404" on
        its own is not the claim — the claim is that a prober cannot tell a
        disabled feature from an unimplemented one.
        """
        monkeypatch.setattr(settings, "offerwall_serve_enabled", False)
        monkeypatch.setattr(settings, "tds_secret_key", SECRET)
        store = _fake()
        off = _post(store)
        missing = _post(store, path="/definitely-not-a-route")
        assert off.status_code == missing.status_code == 404
        assert off.json() == missing.json(), (
            f"a disabled wall answers {off.json()!r} where a missing route "
            f"answers {missing.json()!r} — that difference IS the oracle"
        )

    def test_armed_without_a_key_is_refused(self, armed):
        r = _post(_fake(), key="wrong-key")
        assert r.status_code in (401, 403), r.status_code

    def test_armed_with_the_key_answers(self, armed):
        store = _fake()
        asyncio.run(_seed(store.client()))
        r = _post(store)
        assert r.status_code == 200, r.text


# --------------------------------------------------------------------------- #
# The wall it actually serves                                                  #
# --------------------------------------------------------------------------- #
class TestItServesTheWall:
    def test_a_matched_wall_returns_its_tiles(self, armed):
        store = _fake()
        asyncio.run(_seed(store.client()))
        body = _post(store).json()
        assert body["matched"] is True
        assert body["wall_id"] == int(WALL_A)
        assert [t["offer_id"] for t in body["tiles"]] == [OFFER_1, OFFER_2]
        assert body["tiles"][0]["offer_name"] == f"Offer {OFFER_1}"

    def test_a_link_with_no_wall_answers_matched_false(self, armed):
        """A normal answer, not an error. Most links have no wall."""
        store = _fake()
        asyncio.run(_seed(store.client(), walls=()))
        body = _post(store).json()
        assert body["matched"] is False
        assert body["reason"] == "no_wall"
        assert body["tiles"] == []

    def test_a_wall_whose_tiles_are_ALL_DEAD_is_rejected_and_the_next_serves(
        self, armed
    ):
        """Contract §2.1 — the viability loop.

        Wall A wins on `seq_id` and every one of its pinned targets is closed.
        Without the loop it would win and render an EMPTY catalogue; with it,
        wall B serves. The assertion is on WHICH wall came back, not merely that
        something did — "a wall was returned" is satisfied by the broken
        behaviour too.
        """
        store = _fake()
        asyncio.run(_seed(store.client(), walls=("A", "B"),
                          wall_a_tiles=_tiles((OFFER_1, DEAD_TARGET))))
        body = _post(store).json()
        assert body["matched"] is True
        assert body["wall_id"] == int(WALL_B), (
            "the dead wall was served; the viability loop did not run"
        )

    def test_a_PARTIALLY_dead_wall_is_KEPT_with_its_surviving_tiles(self, armed):
        """One eligible tile is enough — the contract says retained, not
        rejected. The mirror of the test above, and it is what stops the
        viability rule from being implemented as "any dead tile kills the wall"."""
        store = _fake()
        asyncio.run(_seed(store.client(), walls=("A", "B"),
                          wall_a_tiles=_tiles((OFFER_1, DEAD_TARGET),
                                              (OFFER_2, TARGET_2))))
        body = _post(store).json()
        assert body["wall_id"] == int(WALL_A)
        assert [t["offer_id"] for t in body["tiles"]] == [OFFER_2]

    def test_every_wall_dead_answers_matched_false_with_its_own_reason(
        self, armed
    ):
        store = _fake()
        asyncio.run(_seed(store.client(), walls=("A",),
                          wall_a_tiles=_tiles((OFFER_1, DEAD_TARGET))))
        body = _post(store).json()
        assert body["matched"] is False
        assert body["reason"] == "no_viable_wall", (
            "a wall existed and was unservable — that is a different fact from "
            "'no wall here', and an operator debugging an empty page needs the "
            "difference"
        )


# --------------------------------------------------------------------------- #
# G5.2 — the budget is TILES, and it is the WALL's own                         #
# --------------------------------------------------------------------------- #
class TestTheAdmissionBudget:
    def test_over_budget_answers_503(self, armed, monkeypatch):
        monkeypatch.setattr(settings, "offerwall_max_tiles_inflight", 24)
        monkeypatch.setattr(settings, "offerwall_admission_charge_tiles", 24)
        main._wall_tiles_inflight = 24  # one wall already in flight
        store = _fake()
        asyncio.run(_seed(store.client()))
        assert _post(store).status_code == 503

    def test_under_budget_still_answers(self, armed, monkeypatch):
        """The calibration for the test above: with the counter clear the same
        request succeeds, so the 503 is the BUDGET and not the fixture."""
        monkeypatch.setattr(settings, "offerwall_max_tiles_inflight", 24)
        monkeypatch.setattr(settings, "offerwall_admission_charge_tiles", 24)
        main._wall_tiles_inflight = 0
        store = _fake()
        asyncio.run(_seed(store.client()))
        assert _post(store).status_code == 200

    def test_the_budget_is_TILES_not_REQUESTS(self, armed, monkeypatch):
        """🔴 The distinction risk A19 exists for.

        The SAME number of in-flight requests (one) is admitted under a
        480-tile budget and shed under a 24-tile one carrying a 24-tile charge.
        A request-counting cap could not tell those two apart — which is the
        whole reason the wall does not share the preview counter.
        """
        store = _fake()
        asyncio.run(_seed(store.client()))

        # ⚠️ THE FIRST VERSION OF THIS TEST DID NOT DISCRIMINATE, and a mutation
        # setting `charge = 1` left it green: with cap 480 / inflight 24 both a
        # 24-charge and a 1-charge admit, and with cap 24 / inflight 24 both
        # shed. The numbers below are chosen so the two answers DIFFER — one
        # budget, one in-flight figure, and only the CHARGE moves the verdict.
        monkeypatch.setattr(settings, "offerwall_max_tiles_inflight", 30)
        main._wall_tiles_inflight = 10

        monkeypatch.setattr(settings, "offerwall_admission_charge_tiles", 24)
        assert _post(store).status_code == 503, (
            "10 in flight + a 24-tile charge exceeds 30 and must shed; a "
            "request-counting cap would have admitted this"
        )

        main._wall_tiles_inflight = 10
        monkeypatch.setattr(settings, "offerwall_admission_charge_tiles", 1)
        assert _post(store).status_code == 200, (
            "the same request under a 1-tile charge fits — so the verdict is "
            "the CHARGE, which is what makes this a work budget"
        )

    def test_the_counter_is_released_even_when_the_body_raises(
        self, armed, monkeypatch
    ):
        """A leaked charge would shed every later request on this worker — a
        bulkhead that fails CLOSED forever is worse than none."""
        monkeypatch.setattr(settings, "offerwall_max_tiles_inflight", 480)
        store = _fake()

        async def _boom(*a, **kw):
            raise RuntimeError("resolver exploded")

        with patch.object(main, "resolve_domain_campaign", _boom):
            with pytest.raises(Exception):
                _post(store)
        assert main._wall_tiles_inflight == 0, (
            f"charge leaked: counter is {main._wall_tiles_inflight}"
        )


# --------------------------------------------------------------------------- #
# G5.3 — 503 NEVER becomes a click                                             #
# --------------------------------------------------------------------------- #
class TestASheddedWallIsNeverAClick:
    def test_an_over_budget_request_touches_redis_not_at_all(
        self, armed, monkeypatch
    ):
        """The bulkhead sits BEFORE any Redis or routing work, so an over-limit
        request costs one counter read. Asserted on the RECORDER, which sees
        every mutating verb on both pools."""
        monkeypatch.setattr(settings, "offerwall_max_tiles_inflight", 24)
        monkeypatch.setattr(settings, "offerwall_admission_charge_tiles", 24)
        main._wall_tiles_inflight = 24
        store = _fake()
        asyncio.run(_seed(store.client()))
        log: list[str] = []
        assert _post(store, log=log).status_code == 503
        assert log == [], f"a shed request wrote to Redis: {log}"

    def test_shedding_walls_does_not_touch_the_PREVIEW_counter(
        self, armed, monkeypatch
    ):
        """Two products, two budgets. If the wall drew on `_preview_inflight`, a
        wall flood would shed previews — and a preview flood would shed walls."""
        monkeypatch.setattr(settings, "offerwall_max_tiles_inflight", 24)
        monkeypatch.setattr(settings, "offerwall_admission_charge_tiles", 24)
        main._wall_tiles_inflight = 24
        before = main._preview_inflight
        store = _fake()
        asyncio.run(_seed(store.client()))
        assert _post(store).status_code == 503
        assert main._preview_inflight == before

    def test_an_ORDINARY_CLICK_still_serves_while_walls_are_shed(
        self, armed, monkeypatch
    ):
        """The property the bulkhead exists for, stated as the product outcome
        rather than as a counter value: wall load must not cost a redirect."""
        monkeypatch.setattr(settings, "offerwall_max_tiles_inflight", 24)
        monkeypatch.setattr(settings, "offerwall_admission_charge_tiles", 24)
        main._wall_tiles_inflight = 24
        store = _fake()

        async def _seed_click(r):
            await _seed(r)
            await r.rpush(f"campaign:{CAMPAIGN}:flows", "999")
            await r.hset("flow:999", mapping={
                "campaign_id": CAMPAIGN, "scope_type": "company",
                "scope_id": str(COMPANY), "seq_id": "1", "is_default": "1",
                "criteria": "[]", "action_type": "offer",
                "action_config": json.dumps(
                    {"offer_id": OFFER_1, "target_id": TARGET_1}),
            })

        asyncio.run(_seed_click(store.client()))
        assert _post(store).status_code == 503

        conn = store.client()

        async def _get_redis():
            return conn

        # `/decide` authenticates against the per-WORKER key index, not the
        # sync secret, so it is stubbed the way this suite's other endpoint
        # tests stub it. The property under test is the BULKHEAD, not the click
        # path's auth — and using the wrong credential would have made this test
        # pass or fail for a reason that has nothing to do with walls.
        with patch.object(router, "get_redis", _get_redis), \
                patch.object(main, "get_redis", _get_redis), \
                patch("app.main._check_tds_key",
                      new=AsyncMock(return_value=1)):
            click = TestClient(main.app).post("/decide", json={
                "click_id": "01a0000000000000000000d2",
                "hostname": HOST, "path": "/", "country": "US",
                "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2)",
            }, headers={"X-TDS-Key": SECRET})
        assert click.status_code == 200, click.text


# --------------------------------------------------------------------------- #
# A34 — a catalogue fetch WRITES NOTHING                                       #
# --------------------------------------------------------------------------- #
class TestTheWallPathIsSideEffectFree:
    def test_a_served_wall_performs_zero_writes(self, armed):
        """A wall answer must not stamp an identity: a page RENDER that pinned a
        visitor is the class the preview programme closed (risk A34). The gate
        is `commit_identity=False`, not the absence of identity fields."""
        store = _fake()
        asyncio.run(_seed(store.client()))
        log: list[str] = []
        # A trusted source AND a funnel_user_id — the only combination that
        # mints a uid, and therefore the only one under which a commit could
        # write anything at all.
        r = _post(store, body={"query_params": {
            "source": "trusted-src", "funnel_user_id": "u-42"}}, log=log)
        assert r.status_code == 200 and r.json()["matched"] is True
        assert log == [], f"the wall path wrote to Redis: {log}"

    def test_the_wall_never_SCHEDULES_a_persist(self, armed):
        """🔴 THE DISCRIMINATING TEST FOR A34, and the write recorder is not it.

        `resolve_and_stamp` defers the persist with `asyncio.create_task`
        (`identity.py:864`), so the write may never run inside the request at
        all — a recorder watching Redis verbs can therefore report a clean run
        under BOTH `commit_identity=False` and `True`. Measured: flipping that
        argument left the whole suite green until this test existed.

        What IS deterministic is the CALL: `persist_identity(...)` is invoked to
        build the coroutine before `create_task` ever schedules it. Spying on
        the function catches the intent regardless of whether the loop gets
        round to it.

        A `funnel_user_id` is supplied because the resolver only mints a uid for
        a visitor it can name, and `if commit and result.uid` gates the persist —
        without one, `commit=True` would produce no schedule either and the test
        would pass for the wrong reason.
        """
        store = _fake()
        asyncio.run(_seed(store.client()))
        calls: list[tuple] = []

        async def _spy(*a, **kw):
            calls.append((a, kw))

        with patch.object(identity, "persist_identity", _spy):
            r = _post(store, body={"query_params": {
                "source": "trusted-src", "funnel_user_id": "u-42"}})

        assert r.status_code == 200
        assert calls == [], (
            f"the wall scheduled {len(calls)} identity persist(s) — a catalogue "
            "fetch must not let a page RENDER pin a visitor (A34)"
        )

    def test_that_persist_spy_can_actually_SEE_a_schedule(self, armed):
        """Calibration for the test above. A spy that never fires is
        indistinguishable from a path that never writes, so prove it fires when
        the same request runs with the commit ALLOWED.

        This drives `resolve_and_stamp` directly rather than through the
        endpoint: the point is that the spy is wired to the real symbol and sees
        a real schedule, not that some other endpoint writes.
        """
        store = _fake()
        calls: list[tuple] = []

        async def _spy(*a, **kw):
            calls.append((a, kw))

        ident_client = None

        async def _get_identity_redis():
            return ident_client

        async def _drive():
            nonlocal ident_client
            ident_client = store.client()
            # `resolve_and_stamp` takes no client — it dials the IDENTITY pool
            # itself, which is the second of the three factories and the one a
            # recorder on the routing pool would miss entirely.
            with patch.object(identity, "persist_identity", _spy), \
                    patch.object(identity, "get_identity_redis",
                                 _get_identity_redis):
                await identity.resolve_and_stamp(
                    company_id=COMPANY,
                    funnel_user_id="u-42",
                    visitor_id="",
                    campaign_id=CAMPAIGN,
                    # TRUE, measured: an untrusted source yields uid='' and the
                    # persist is gated on the uid, so a False here would make
                    # this calibration pass for the wrong reason — the exact
                    # failure it exists to rule out.
                    source_trusted=True,
                    commit=True,
                )

        asyncio.run(_drive())
        assert calls, (
            "the spy saw no schedule even with commit=True — it is not wired to "
            "the symbol the code calls, so the test above proves nothing"
        )

    def test_the_write_detector_can_actually_SEE_a_write(self):
        """Calibration. A detector reporting "no writes" because it cannot see
        any is indistinguishable, from outside, from a genuinely clean run."""
        store = _fake()
        log: list[str] = []
        rec = _WriteRecorder(store.client(), log, "routing")
        asyncio.run(rec.hset("probe:1", mapping={"a": "b"}))
        assert log == ["routing:hset"], log
