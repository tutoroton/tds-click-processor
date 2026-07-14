"""F4 (GTD-R173) — flow/offer resolution fail-open under load.

Pins the two-layer fix from FIX-DESIGN-F4.md / FIX-PLAN.md §1:

  Layer 1  — `BlockingConnectionPool` (sized + bounded-wait) replaces the default
             non-blocking `ConnectionPool(max_connections=20)` that RAISED
             `ConnectionError("Too many connections")` on exhaustion under a
             concurrency burst.
  Layer 2  — the flow-cascade reads (`_collect_candidate_ids`,
             `_load_flow_records`, `_load_target_availability`) + the legacy
             `select_offer` retry-once and, on persistent `redis.RedisError`,
             raise `cascade.FlowReadError` INSTEAD of the silent fail-open
             (`[]` / `{}` / `None`). Caught in `router._route_via_campaign` as a
             RECORDED `flow_read_failed` non-routed result (never dropped, never
             masqueraded as `no_flow_no_offer`).
  Layer 3  — throttled Sentry counter on the `no_flow_no_offer` rate.

The 5 ship-with tests (FIX-PLAN §1.4), each mapped to an invariant:
  (i)   INV-1 — pool exhaustion under N+1 concurrent reads no longer surfaces
                `[]` from the cascade (it raises FlowReadError).
  (ii)  INV-1 — a mocked Redis raise → `flow_read_failed` RECORDED (not dropped,
                not `no_flow`), caught in `_route_via_campaign`.
  (iii) INV-2 — a SUCCESSFUL empty read still → `no_flow_no_offer` (genuinely
                flowless campaign byte-identical).
  (iv)  INV-3/5 — `BlockingConnectionPool` wait is bounded by `timeout`
                (per-acquire), and `get_redis()` wires the env knobs.
  (v)   INV-4 — the reads catch the BASE `redis.RedisError` (covers TimeoutError,
                the new socket_timeout trigger), never `MaxConnectionsError`
                (absent from top-level redis 5.2.1), never bare `Exception`.
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import redis
from redis.asyncio.connection import BlockingConnectionPool

from app import cascade, router
from app.main import _decision_reason
from app.models import ClickRequest


# ---------------------------------------------------------------------------
# Fakes — a Redis surface for route(), with per-key POISONING so a single
# read can raise (pool-exhaustion / socket-timeout) while everything else
# succeeds. Mirrors tests/unit/test_router_cascade.py::FakeRedis.
# ---------------------------------------------------------------------------


class _FakeRedis:
    def __init__(
        self,
        *,
        strings: dict | None = None,
        hashes: dict | None = None,
        sets: dict | None = None,
        lists: dict | None = None,
        poison_keys: set[str] | None = None,
        poison_exc: BaseException | None = None,
    ):
        self.strings = strings or {}
        self.hashes = hashes or {}
        self.sets = sets or {}
        self.lists = lists or {}
        self.poison_keys = set(poison_keys or ())
        self.poison_exc = poison_exc or redis.ConnectionError("Too many connections")

    async def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    async def smembers(self, key):
        return set(self.sets.get(key, set()))

    async def sismember(self, key, member):
        return 1 if member in self.sets.get(key, set()) else 0

    async def get(self, key):
        return self.strings.get(key)

    async def hget(self, key, field):
        return self.hashes.get(key, {}).get(field)

    async def set(self, key, value, nx=False, ex=None):
        if nx and key in self.strings:
            return None
        self.strings[key] = value
        return True

    async def incr(self, key):
        cur = int(self.strings.get(key, 0))
        self.strings[key] = str(cur + 1)
        return cur + 1

    async def expire(self, key, seconds):
        return True

    async def xadd(self, *a, **k):
        return "1-0"

    def pipeline(self):
        return _FakePipe(self)


class _FakePipe:
    def __init__(self, parent: _FakeRedis):
        self.parent = parent
        self.ops: list[tuple] = []

    def hgetall(self, key):
        self.ops.append(("hgetall", key))

    def hget(self, key, field):
        self.ops.append(("hget", key, field))

    def smembers(self, key):
        self.ops.append(("smembers", key))

    def get(self, key):
        self.ops.append(("get", key))

    def lrange(self, key, _s, _e):
        self.ops.append(("lrange", key))

    def exists(self, key):
        self.ops.append(("exists", key))

    def sismember(self, key, member):
        self.ops.append(("sismember", key, member))

    def incr(self, key):
        self.ops.append(("incr", key))

    def expire(self, key, seconds):
        self.ops.append(("expire", key, seconds))

    async def execute(self):
        p = self.parent
        # Poison FIRST — a buffered read of a poisoned key raises the same
        # exception the exhausted pool / socket-timeout would, for THIS pipe only.
        for op in self.ops:
            if op[1] in p.poison_keys:
                raise p.poison_exc
        out = []
        for op in self.ops:
            kind, key = op[0], op[1]
            if kind == "hgetall":
                out.append(dict(p.hashes.get(key, {})))
            elif kind == "hget":
                out.append(p.hashes.get(key, {}).get(op[2]))
            elif kind == "smembers":
                out.append(set(p.sets.get(key, set())))
            elif kind == "get":
                out.append(p.strings.get(key))
            elif kind == "lrange":
                out.append(list(p.lists.get(key, [])))
            elif kind == "exists":
                ex = key in p.hashes or key in p.strings or key in p.sets
                out.append(1 if ex else 0)
            elif kind == "sismember":
                out.append(1 if op[2] in p.sets.get(key, set()) else 0)
            elif kind == "incr":
                cur = int(p.strings.get(key, 0))
                p.strings[key] = str(cur + 1)
                out.append(cur + 1)
            elif kind == "expire":
                out.append(True)
        return out


class _ExhaustedRedis:
    """Every pipeline execute() raises the exact exhaustion error the pre-F4
    default pool raised — used for the direct-cascade unit tests."""

    def __init__(self, exc: BaseException | None = None):
        self.execute_calls = 0
        self.exc = exc or redis.ConnectionError("Too many connections")

    def pipeline(self):
        return _ExhaustedPipe(self)


class _ExhaustedPipe:
    def __init__(self, parent: _ExhaustedRedis):
        self.parent = parent

    def _noop(self, *a, **k):
        return None

    lrange = hgetall = exists = hget = smembers = _noop

    async def execute(self):
        self.parent.execute_calls += 1
        raise self.parent.exc


_CID = "5"
_FID = "100"


def _match_snapshot(*, flows: list[str], poison_flows: bool = False) -> _FakeRedis:
    """A minimal keyspace where a US/mobile/iOS click matches campaign 5 (mirrors
    test_router_cascade::test_redirect_flow_wins). `flows` populates
    `campaign:5:flows`; `poison_flows` makes that candidate read RAISE."""
    return _FakeRedis(
        sets={
            "geo:US": {_CID},
            "device:mobile": {_CID},
            "os:ios": {_CID},
            "campaigns:active": {_CID},
        },
        hashes={
            f"campaign:{_CID}": {"company_id": "1", "priority": "0", "weight": "100"},
            f"flow:{_FID}": {
                "campaign_id": _CID,
                "scope_type": "company",
                "scope_id": "1",
                "seq_id": "1",
                "is_default": "0",
                "criteria": "[]",
                "action_type": "redirect",
                "action_config": json.dumps({"url": "https://lp.example/{click_id}"}),
            },
        },
        lists={f"campaign:{_CID}:flows": flows},
        poison_keys={f"campaign:{_CID}:flows"} if poison_flows else None,
    )


def _click() -> ClickRequest:
    return ClickRequest(
        click_id="test-click-1",
        country="US",
        user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_2)",
        query_params={},
    )


def _route_with(redis_obj: _FakeRedis):
    async def _async_redis():
        return redis_obj

    async def _runner():
        with patch.object(router, "get_redis", _async_redis):
            return await router.route(_click())

    return asyncio.run(_runner())


# ---------------------------------------------------------------------------
# (i) INV-1 — pool exhaustion no longer surfaces [] from the cascade.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_i_candidate_exhaustion_raises_not_empty():
    """A pool-exhaustion ConnectionError on the flow-candidate read RAISES
    FlowReadError after retry-once — it does NOT silently `return []` (the pre-F4
    offer-miss). Retry-once = exactly 2 execute attempts."""
    r = _ExhaustedRedis()
    with pytest.raises(cascade.FlowReadError) as ei:
        await cascade._collect_candidate_ids(
            r, campaign_id=_CID, company_id=1, buyer_id=None,
            team_id=None, department_id=None, custom_group_id=None,
            cap=200,
        )
    assert ei.value.stage == "candidate"
    assert r.execute_calls == 2  # first attempt + one retry, then raise


@pytest.mark.asyncio
async def test_i_concurrent_exhaustion_none_return_empty():
    """N+1 concurrent candidate reads against a saturated pool: EVERY one raises
    FlowReadError (honest); NONE silently returns [] (the pre-F4 fail-open that
    became `no_flow_no_offer` → offer-miss)."""
    async def _one():
        r = _ExhaustedRedis()
        with pytest.raises(cascade.FlowReadError):
            await cascade._collect_candidate_ids(
                r, campaign_id=_CID, company_id=1, buyer_id=None,
                team_id=None, department_id=None, custom_group_id=None,
                cap=200,
            )
        return True

    results = await asyncio.gather(*[_one() for _ in range(21)])  # N+1 vs old cap 20
    assert results == [True] * 21


@pytest.mark.asyncio
async def test_i_flow_load_and_availability_reads_also_raise():
    """The sibling reads (`_load_flow_records`, `_load_target_availability`) get
    the same treatment — a persistent read failure RAISES, never fails open."""
    r = _ExhaustedRedis()
    with pytest.raises(cascade.FlowReadError) as ei_load:
        await cascade._load_flow_records(r, [_FID])
    assert ei_load.value.stage == "flow_load"

    r2 = _ExhaustedRedis()
    flows = [{"_id": _FID, "action_type": "offer",
              "action_config": json.dumps({"offer_id": 1, "target_id": 77})}]
    with pytest.raises(cascade.FlowReadError) as ei_av:
        await cascade._load_target_availability(r2, flows)
    assert ei_av.value.stage == "availability"


# ---------------------------------------------------------------------------
# (ii) INV-1 — a raised read → RECORDED flow_read_failed, caught in
#      _route_via_campaign; not dropped, not no_flow.
# ---------------------------------------------------------------------------


def test_ii_raised_read_recorded_as_flow_read_failed_not_no_flow():
    """End-to-end `route()`: the flow-candidate read raises → the result is the
    G2 non-routed sentinel with routing_status=flow_read_failed, caught in
    `_route_via_campaign` (NOT propagated to route()'s catch-all → NOT dropped),
    and `_decision_reason` maps it to the DISTINCT `flow_read_failed` — never a
    silent `no_flow_no_offer`."""
    result = _route_with(_match_snapshot(flows=[_FID], poison_flows=True))

    assert result is not None                       # not dropped (route ≠ None)
    assert result.get("non_routed") is True         # → main.py RECORDS it
    assert result.get("routing_status") == "flow_read_failed"
    reason = _decision_reason(result, result["timing"], result["attribution"])
    assert reason == "flow_read_failed"
    assert reason != "no_flow_no_offer"


def test_ii_flow_read_failed_sentinel_is_recorded_via_decide():
    """The `flow_read_failed` sentinel flows through /decide's record → dedup →
    XADD path (RECORDED, not dropped) and is tagged decision_reason=
    flow_read_failed — the click is never lost from ClickHouse."""
    from fastapi.testclient import TestClient

    from app.main import app

    sentinel = {
        "url": None,
        "campaign_id": _CID,
        "offer_id": None,
        "binding_id": 0,
        "binding_alias": None,
        "timing": {"result": "flow_read_failed"},
        "non_routed": True,
        "routing_status": "flow_read_failed",
        "attribution": {"company_id": 1, "campaign_id": _CID},
        "fallback_url": None,
    }
    fake_redis = MagicMock()
    fake_redis.set = AsyncMock(return_value=True)
    fake_redis.xadd = AsyncMock(return_value="1-0")

    with patch("app.main._check_tds_key", new_callable=AsyncMock), \
         patch("app.main.get_redis", new=AsyncMock(return_value=fake_redis)), \
         patch("app.main.route", new=AsyncMock(return_value=sentinel)):
        resp = TestClient(app).post(
            "/decide",
            json={"click_id": "019e5be83c8179896a0859dd", "ip": "1.2.3.4",
                  "country": "DE", "user_agent": "geo-tds-test/1.0"},
            headers={"X-TDS-Key": "x"},
        )

    assert resp.status_code == 200
    fake_redis.xadd.assert_awaited_once()           # RECORDED, not dropped
    data = json.loads(fake_redis.xadd.await_args.args[1]["data"])
    assert data["decision_reason"] == "flow_read_failed"
    assert data["extra_params"]["routing_status"] == "flow_read_failed"


# ---------------------------------------------------------------------------
# (iii) INV-2 — a SUCCESSFUL empty read still → no_flow_no_offer.
# ---------------------------------------------------------------------------


def test_iii_successful_empty_read_still_no_flow_no_offer():
    """A campaign that matches but has a SUCCESSFUL empty flow list AND no legacy
    offer resolves to `no_flow_no_offer` — byte-identical to pre-F4. Only a
    RAISED read becomes flow_read_failed; an empty success must NOT."""
    result = _route_with(_match_snapshot(flows=[]))  # empty, successful — no poison

    assert result is not None
    assert result.get("non_routed") is True
    reason = _decision_reason(result, result["timing"], result["attribution"])
    assert reason == "no_flow_no_offer"
    assert reason != "flow_read_failed"


# ---------------------------------------------------------------------------
# (iv) INV-3/5 — BlockingConnectionPool wait bounded by `timeout` (per-acquire),
#      and get_redis() wires the env knobs.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_iv_get_redis_builds_sized_blocking_pool():
    """`get_redis()` builds a BlockingConnectionPool wired to the env knobs —
    bounded-wait, not the default raise-immediately pool."""
    import app.redis_client as rc
    from app.config import settings

    rc.pool = None  # force a fresh build
    try:
        client = await rc.get_redis()
        cp = client.connection_pool
        assert isinstance(cp, BlockingConnectionPool)
        assert cp.max_connections == settings.redis_max_connections
        assert cp.timeout == settings.redis_pool_timeout_seconds
    finally:
        rc.pool = None  # don't leak the fake into other tests


@pytest.mark.asyncio
async def test_iv_blocking_pool_wait_is_bounded_by_timeout():
    """A saturated BlockingConnectionPool WAITS (does not raise immediately, the
    pre-F4 bug) and the wait is BOUNDED by `timeout` — after which it raises a
    fast, honest ConnectionError (→ load-shed to a recorded fallback)."""
    class _FakeConn:
        def __init__(self, **kwargs):
            pass

        async def connect(self):
            pass

        async def disconnect(self, *a, **k):
            pass

        async def can_read_destructive(self):
            return False

        def __getattr__(self, _name):
            async def _noop(*a, **k):
                return None
            return _noop

    pool = BlockingConnectionPool(
        max_connections=1, timeout=0.1, connection_class=_FakeConn,
    )
    await pool.get_connection("PING")  # take the only slot
    t0 = time.monotonic()
    with pytest.raises(redis.ConnectionError):
        await pool.get_connection("PING")  # 2nd acquire blocks then raises
    waited = time.monotonic() - t0
    # It WAITED (>= most of the timeout — not an instant raise) and stayed
    # BOUNDED (well under the CF 2s abort). Generous bounds avoid CI flake.
    assert waited >= 0.05
    assert waited < 1.0


# ---------------------------------------------------------------------------
# INV-3 — deadlock-freedom precondition: the hot path holds <=1 connection at
# any instant. Structural pin — a BlockingConnectionPool can only deadlock under
# hold-and-wait, which requires concurrent multi-connection acquisition on the
# routing path. Assert none of the primitives that would create it exist there.
# ---------------------------------------------------------------------------


def test_inv3_hot_path_has_no_multi_connection_primitives():
    """No `asyncio.gather` / `TaskGroup` / `.watch(` / `.lock(` / explicit
    `get_connection(` on the routing hot path (router.py + cascade.py) → each
    request holds <=1 conn at any instant → no hold-and-wait → the sized
    BlockingConnectionPool cannot deadlock this code. Call-pattern scan (with
    `(`) so docstring PROSE mentioning 'gather' does not false-positive."""
    forbidden = (
        "asyncio.gather(", "TaskGroup(", ".watch(", ".lock(",
        "get_connection(", "connection_pool.get(", "connection_pool.acquire(",
    )
    for path in (router.__file__, cascade.__file__):
        src = Path(path).read_text()
        for pat in forbidden:
            assert pat not in src, f"{path}: hot-path deadlock risk — found {pat!r}"


# ---------------------------------------------------------------------------
# (v) INV-4 — catch the BASE redis.RedisError; no MaxConnectionsError; not
#     bare Exception.
# ---------------------------------------------------------------------------


def test_v_no_maxconnectionserror_symbol_and_base_catch_in_source():
    """Source pin: neither the cascade nor the router USES `MaxConnectionsError`
    as code — it is absent from the top-level redis 5.2.1 namespace, so an
    `import`/`except`/`redis.MaxConnectionsError` reference would break at
    runtime. Uses `tokenize` so a NAME token (real code) fails but explanatory
    comments/docstrings that NAME it (documenting the INV-4 rationale) pass. Both
    files catch the BASE `redis.RedisError`."""
    import io
    import tokenize

    for path in (cascade.__file__, router.__file__):
        src = Path(path).read_text()
        name_tokens = {
            tok.string
            for tok in tokenize.generate_tokens(io.StringIO(src).readline)
            if tok.type == tokenize.NAME
        }
        assert "MaxConnectionsError" not in name_tokens, (
            f"{path}: MaxConnectionsError referenced as code (not just a comment)"
        )
        assert "redis.MaxConnectionsError" not in src  # broken attribute access
        assert "redis.RedisError" in src               # base-class catch present


@pytest.mark.asyncio
async def test_v_timeouterror_raises_flowreaderror():
    """`redis.TimeoutError` (a RedisError SUBCLASS — the NEW socket_timeout=1.0s
    trigger) is caught by the base-class catch and becomes FlowReadError."""
    r = _ExhaustedRedis(exc=redis.TimeoutError("Timeout reading from socket"))
    with pytest.raises(cascade.FlowReadError):
        await cascade._collect_candidate_ids(
            r, campaign_id=_CID, company_id=1, buyer_id=None,
            team_id=None, department_id=None, custom_group_id=None,
            cap=200,
        )
    # retry-once still applied to a RedisError subclass.
    assert r.execute_calls == 2


@pytest.mark.asyncio
async def test_v_non_redis_error_propagates_not_swallowed():
    """A NON-Redis error (e.g. a genuine logic bug) is NOT caught as a read
    failure — it propagates, never converted to FlowReadError and never returned
    as `[]`. Pins 'catch the base RedisError, NOT bare Exception'."""
    r = _ExhaustedRedis(exc=ValueError("logic bug, not a redis fault"))
    with pytest.raises(ValueError):
        await cascade._collect_candidate_ids(
            r, campaign_id=_CID, company_id=1, buyer_id=None,
            team_id=None, department_id=None, custom_group_id=None,
            cap=200,
        )
    assert r.execute_calls == 1  # NOT retried (only RedisError is retried)


@pytest.mark.asyncio
async def test_v_select_offer_redis_raise_becomes_flowreaderror():
    """Layer 2b: `select_offer`'s legacy read raises FlowReadError on a
    persistent RedisError (retry-once), but a NON-Redis error preserves the
    pre-F4 fail-open-to-None (recorded no_flow_no_offer upstream, never a drop
    and never a false flow_read_failed)."""
    class _RaisingR:
        def __init__(self, exc):
            self.exc = exc
            self.calls = 0

        async def hgetall(self, key):
            self.calls += 1
            raise self.exc

        async def smembers(self, key):
            raise self.exc

    # RedisError → FlowReadError (retry-once = 2 hgetall calls).
    rr = _RaisingR(redis.ConnectionError("Too many connections"))
    with pytest.raises(cascade.FlowReadError) as ei:
        await router.select_offer(rr, _CID)
    assert ei.value.stage == "offer"
    assert rr.calls == 2

    # Non-Redis → preserved fail-open to None (no retry, no FlowReadError).
    rv = _RaisingR(ValueError("logic bug"))
    assert await router.select_offer(rv, _CID) is None
    assert rv.calls == 1
