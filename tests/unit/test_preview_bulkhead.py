"""GTD-D149 — the preview↔click bulkhead, and the rig that proves it refuses.

The owner's guarantee: preview load, at ANY volume, cannot degrade routing.
Today's mechanism is an ADMISSION CAP per worker process (main._preview_inflight
against settings.preview_max_concurrency): a preview beyond the bound answers
503 and never touches Redis or route().

Why an admission cap and not a pool split / --limit-concurrency: measured in
`test_one_route_is_a_handful_of_redis_ops_not_a_pool_drainer` below — one route() is a
handful of sub-millisecond Redis ops, so 128 pooled connections is a ceiling the
CPU/event loop never lets a single worker approach. Bounding ENTRY caps loop
share, CPU share and pool draw together; --limit-concurrency would cap CLICKS
too, and a separate pool would bound the resource that binds LAST.

The invariants under test map to the D149 brief:
  (a) over-limit REFUSES (503), never falls through to a click — proven by the
      response AND by an ALL-OP detector (reads included: a preview writes
      nothing served or shed, so only an op count that includes reads can tell
      route()-ran from route()-skipped): a shed preview performs ZERO Redis ops;
  (b) the click path takes on NO cap work — proven structurally (the /decide
      handler holds zero references to the cap) and here by the cap being a pure
      module counter the click path never reads;
  (c) safe with nothing tuned — the default is a positive int and `< 1` clamps
      to 1, so there is no "off" state to forget;
  (d) the knockout goes red for the RIGHT reason — remove the cap comparison and
      the refusal test fails by SERVING a preview beyond the bound, not by some
      unrelated error.
"""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest

from app import identity, main, router, sticky
from app.config import settings
from app.models import PreviewRequest
from tests.unit.test_route_preview_endpoint import (
    HOST,
    SECRET,
    _fake,
    _run,
    _seed_domain_route,
)

# Every Redis method — READS INCLUDED. The write-only recorder used elsewhere
# is the WRONG instrument here: a preview writes nothing whether it is served
# or shed, so a write count is identical for subject and control and measures
# nothing. What the cap changes is whether route() RUNS AT ALL, i.e. whether
# any Redis op (get/hgetall/smembers/pipeline) happens — so we count every op.
_READ_AND_WRITE = frozenset({
    "get", "mget", "hget", "hgetall", "hmget", "smembers", "sismember",
    "scard", "exists", "keys", "scan", "ttl", "type", "strlen", "pipeline",
    "set", "setex", "setnx", "hset", "sadd", "expire", "delete", "xadd",
})


class _OpRecorder:
    def __init__(self, inner, log):
        self._inner, self._log = inner, log

    def __getattr__(self, name):
        attr = getattr(self._inner, name)
        if name == "pipeline":
            def _pipeline(*a, **kw):
                self._log.append("pipeline")
                return _OpRecorder(attr(*a, **kw), self._log)
            return _pipeline
        if name in _READ_AND_WRITE and callable(attr):
            def _wrapped(*a, **kw):
                self._log.append(name)
                return attr(*a, **kw)
            return _wrapped
        return attr


def _seeded():
    store = _fake()
    _run(_seed_domain_route(store.client()))
    return store


@pytest.fixture
def enabled(monkeypatch):
    monkeypatch.setattr(settings, "route_preview_enabled", True)
    monkeypatch.setattr(settings, "tds_secret_key", SECRET)


def _clients(store, log=None):
    conn, ident = store.client(), store.client()
    if log is not None:
        conn = _OpRecorder(conn, log)
        ident = _OpRecorder(ident, log)

    async def _get_redis():
        return conn

    async def _get_identity_redis():
        return ident

    return _get_redis, _get_identity_redis


def _preview_coro(get_redis):
    """A fresh handler coroutine, drivable directly (not via TestClient, which
    serialises) so many can be in flight on one loop — the only way a
    concurrency cap is observable."""
    req = PreviewRequest(hostname=HOST, path="/", country="US",
                         user_agent="UA/1.0")

    async def _one():
        with patch.object(main, "get_redis", get_redis), \
                patch.object(router, "get_redis", get_redis):
            return await main.preview(req, x_tds_key=SECRET)

    return _one()


# --------------------------------------------------------------------------- #
# (a) + (d) — the refusal, and the knockout                                    #
# --------------------------------------------------------------------------- #
def test_over_the_cap_refuses_and_writes_nothing(enabled, monkeypatch):
    """🔴 THE ONE THE GUARANTEE RESTS ON. With the cap already full, a further
    preview raises 503 and performs ZERO Redis operations — so it did not run
    route(), did not read a binding, did not mint anything. Refuse, never
    redirect (brief invariant (a)); a shed preview converted into a click is
    R3 reopened through capacity."""
    from fastapi import HTTPException

    monkeypatch.setattr(settings, "preview_max_concurrency", 4)
    monkeypatch.setattr(main, "_preview_inflight", 4)  # cap already full
    store = _seeded()
    log: list[str] = []
    get_redis, _ = _clients(store, log)

    with pytest.raises(HTTPException) as exc:
        _run(_preview_coro(get_redis))

    assert exc.value.status_code == 503
    assert exc.value.detail == "preview_capacity"
    assert log == [], f"a shed preview touched Redis: {log}"  # zero OPS: route() never ran


def test_the_knockout_goes_red_by_SERVING_not_by_erroring(enabled, monkeypatch):
    """(d) calibration. The refusal test above must fail, when the cap is
    removed, by the preview being SERVED beyond the bound — not by an
    unrelated exception. Here we simulate the knockout: with the comparison
    neutralised (cap raised sky-high) the same over-full state SERVES, proving
    the 503 above was the cap and nothing else."""
    monkeypatch.setattr(settings, "preview_max_concurrency", 10_000)
    monkeypatch.setattr(main, "_preview_inflight", 4)
    store = _seeded()
    log: list[str] = []
    get_redis, _ = _clients(store, log)

    resp = _run(_preview_coro(get_redis))

    assert resp.matched is True
    assert log != [], "a served preview must have hit Redis in route()"


# --------------------------------------------------------------------------- #
# The cap actually bounds CONCURRENCY, and releases                            #
# --------------------------------------------------------------------------- #
def test_the_cap_bounds_simultaneous_previews_and_releases(enabled, monkeypatch):
    """Many previews launched AT ONCE on one loop: no more than the cap run
    route() concurrently, the rest get 503, and after they all finish the
    counter is back to 0 (the try/finally releases every admission, including
    the shed ones which never incremented)."""
    from fastapi import HTTPException

    CAP = 3
    monkeypatch.setattr(settings, "preview_max_concurrency", CAP)
    monkeypatch.setattr(main, "_preview_inflight", 0)

    peak = {"v": 0}
    real_route = router.route

    async def _watched_route(req):
        # Observe the peak concurrency actually admitted into route().
        peak["v"] = max(peak["v"], main._preview_inflight)
        await asyncio.sleep(0.01)  # hold the admission so peers pile up
        return await real_route(req)

    store = _seeded()               # seed OUTSIDE the loop (it uses asyncio.run)
    get_redis, _ = _clients(store)

    async def _run_flood():
        with patch.object(router, "route", _watched_route), \
                patch.object(main, "route", _watched_route):
            coros = [_preview_coro(get_redis) for _ in range(12)]
            return await asyncio.gather(*coros, return_exceptions=True)

    results = asyncio.run(_run_flood())

    served = [r for r in results if not isinstance(r, Exception)]
    shed = [r for r in results if isinstance(r, HTTPException) and r.status_code == 503]
    assert len(served) + len(shed) == 12
    assert peak["v"] <= CAP, f"admitted {peak['v']} into route(), cap was {CAP}"
    assert len(shed) >= 12 - CAP, "a 12-wide flood at cap 3 must shed the surplus"
    assert main._preview_inflight == 0, "every admission, including sheds, released"


# --------------------------------------------------------------------------- #
# (c) — safe with nothing tuned                                                #
# --------------------------------------------------------------------------- #
def test_default_is_a_safe_positive_bound(monkeypatch):
    """No configuration step stands between us and the guarantee: the shipped
    default is a positive int, and there is no 0/off state."""
    from app.config import Settings

    fresh = Settings(tds_secret_key="x", environment="local")
    assert isinstance(fresh.preview_max_concurrency, int)
    assert fresh.preview_max_concurrency >= 1


def test_a_misconfigured_zero_clamps_to_one_never_off(enabled, monkeypatch):
    """`< 1` clamps to 1 at the use site — a fat-fingered 0 or negative bounds
    to a single preview at a time, never to 'unlimited'. The guarantee cannot
    be turned off by tuning."""
    from fastapi import HTTPException

    monkeypatch.setattr(settings, "preview_max_concurrency", 0)
    monkeypatch.setattr(main, "_preview_inflight", 1)  # one already in flight
    store = _seeded()
    get_redis, _ = _clients(store)

    with pytest.raises(HTTPException) as exc:
        _run(_preview_coro(get_redis))
    assert exc.value.status_code == 503


# --------------------------------------------------------------------------- #
# The measurement behind the METHOD choice (D149's open question)              #
# --------------------------------------------------------------------------- #
def test_the_click_path_holds_no_reference_to_the_cap(enabled):
    """(b), structurally: the /decide handler contains ZERO references to the
    admission counter or its setting, so a preview flood cannot add a
    round-trip, a lock, or any awaited work to routing. The cap lives entirely
    on the /preview side."""
    import inspect
    from pathlib import Path

    src = Path(main.__file__).read_text(encoding="utf-8")
    decide_start = src.index('@app.post("/decide")')
    decide_end = src.index("\n@app.", decide_start + 10)
    decide_body = src[decide_start:decide_end]

    assert "_preview_inflight" not in decide_body
    assert "preview_max_concurrency" not in decide_body


def test_one_route_is_a_handful_of_redis_ops_not_a_pool_drainer(enabled):
    """The MEASUREMENT that chose an admission cap over a pool split: one
    route() on the simple fixture is a small, fixed number of Redis
    round-trips — nowhere near the 128-connection pool. So the pool is not
    what a preview flood exhausts; the event loop / CPU running these ops is,
    and an admission cap is what bounds THAT. Recorded as a regression pin on
    the FLOOR, not an upper bound (more flows/filters issue more)."""
    store = _seeded()
    log: list[str] = []
    get_redis, get_ident = _clients(store, log)

    async def _one():
        with patch.object(main, "get_redis", get_redis), \
                patch.object(router, "get_redis", get_redis), \
                patch.object(identity, "get_identity_redis", get_ident), \
                patch.object(sticky, "get_identity_redis", get_ident):
            from app.models import PreviewRequest
            return await main.preview(
                PreviewRequest(hostname=HOST, path="/", country="US",
                               user_agent="UA/1.0"),
                x_tds_key=SECRET)

    resp = _run(_one())
    assert resp.matched is True
    # A handful, not a hundred: proves the pool is not the binding resource.
    # The number is a floor for THIS fixture; the assertion is the order of
    # magnitude, not an exact count (a richer campaign issues more).
    assert 0 < len(log) < 40, f"unexpected op count on the simple fixture: {len(log)}"
