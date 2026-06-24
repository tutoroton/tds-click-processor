"""R70 — seed the decision RNG from `click_id` so every raced node decides
identically (recorded == served; D2-9 sticky cross-node divergence mooted).

`route()` builds ONE `random.Random(_seed_from(click_id))` and threads it through
the whole decision path (campaign winner, legacy split, the cascade split
re-roll). These tests exercise the guarantee end-to-end through `route()` over a
real `fakeredis` keyspace:

  - same click_id → identical served target (deterministic),
  - two independent nodes (fresh Redis) → identical pick (the cross-node invariant),
  - the weighted distribution is preserved over uniform click_ids (no bias),
  - single-leg / pinned (no-RNG) paths are unaffected.

NB: named `test_rng_determinism` to avoid colliding with `test_seed_env_gate`
(which is the unrelated `/admin/seed` config gate).
"""
from __future__ import annotations

import json

import fakeredis.aioredis
import pytest
from unittest.mock import patch

from app import router
from app.models import ClickRequest

pytestmark = pytest.mark.asyncio


def _click(click_id: str) -> ClickRequest:
    return ClickRequest(
        click_id=click_id,
        country="US",
        user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_2)",
        query_params={},
    )


async def _route(r, click_id: str) -> dict | None:
    async def _get_redis():
        return r

    with patch.object(router, "get_redis", _get_redis):
        return await router.route(_click(click_id))


def _served_tid(result: dict | None):
    """The offer_target_id the click actually served + recorded."""
    assert result is not None
    return result["attribution"]["offer_target_id"]


async def _seed_split(r, *, w1: int = 70, w2: int = 30) -> None:
    """A 2-leg split campaign (both legs active) under a company-scope flow."""
    cid, fid = "5", "100"
    await r.sadd("campaigns:active", cid)
    await r.hset(f"campaign:{cid}", mapping={
        "company_id": "1", "priority": "0", "weight": "100"})
    await r.rpush(f"campaign:{cid}:flows", fid)
    await r.hset(f"flow:{fid}", mapping={
        "campaign_id": cid, "scope_type": "company", "scope_id": "1",
        "seq_id": "1", "is_default": "0", "criteria": "[]",
        "action_type": "split",
        "action_config": json.dumps({"offers": [
            {"offer_id": 1, "target_id": 10, "weight": w1},
            {"offer_id": 1, "target_id": 11, "weight": w2},
        ]}),
    })
    for tid, host in ((10, "a"), (11, "b")):
        await r.hset(f"offer_target:{tid}", mapping={
            "url": f"https://{host}/{{click_id}}", "is_default": "0",
            "availability": "active", "offer_id": "1", "criteria": "[]",
            "priority": "0"})


async def _seed_pinned(r) -> None:
    """An offer-action flow with one pinned target — no RNG on its path."""
    cid, fid = "7", "200"
    await r.sadd("campaigns:active", cid)
    await r.hset(f"campaign:{cid}", mapping={
        "company_id": "1", "priority": "0", "weight": "100"})
    await r.rpush(f"campaign:{cid}:flows", fid)
    await r.hset(f"flow:{fid}", mapping={
        "campaign_id": cid, "scope_type": "company", "scope_id": "1",
        "seq_id": "1", "is_default": "0", "criteria": "[]",
        "action_type": "offer",
        "action_config": json.dumps({"offer_id": 1, "target_id": 10}),
    })
    await r.hset("offer_target:10", mapping={
        "url": "https://pinned/{click_id}", "is_default": "0",
        "availability": "active", "offer_id": "1", "criteria": "[]",
        "priority": "0"})


async def test_same_click_id_same_pick():
    """Same click_id + config → identical served target on every call."""
    r = fakeredis.aioredis.FakeRedis(decode_responses=True)
    await _seed_split(r)
    first = _served_tid(await _route(r, "click-same-xyz"))
    for _ in range(50):
        assert _served_tid(await _route(r, "click-same-xyz")) == first


async def test_two_node_simulation_identical():
    """Two independent nodes (fresh Redis copies), same config + same click_id →
    identical pick. Across many distinct clicks (so both legs are exercised) the
    two nodes never diverge — the recorded==served cross-node invariant."""
    node_a = fakeredis.aioredis.FakeRedis(decode_responses=True)
    node_b = fakeredis.aioredis.FakeRedis(decode_responses=True)
    await _seed_split(node_a)
    await _seed_split(node_b)
    served = set()
    for i in range(40):
        cid = f"click-node-{i}"
        a = _served_tid(await _route(node_a, cid))
        b = _served_tid(await _route(node_b, cid))
        assert a == b
        served.add(a)
    # sanity: the 70/30 split actually sent traffic to BOTH legs (else the
    # "identical" assertion would be trivially true for a constant pick).
    assert served == {10, 11}


async def test_distribution_preserved_over_seeds():
    """Many distinct (uniform) click_ids through a 70/30 split → the served
    frequency stays within ±5% of 70/30 (seeding does not bias the pick)."""
    r = fakeredis.aioredis.FakeRedis(decode_responses=True)
    await _seed_split(r, w1=70, w2=30)
    counts = {10: 0, 11: 0}
    n = 1500
    for i in range(n):
        counts[_served_tid(await _route(r, f"click-{i}-seed"))] += 1
    frac10 = counts[10] / n
    assert 0.65 <= frac10 <= 0.75, counts  # ±5% of 0.70


async def test_single_leg_split_deterministic():
    """A single-leg split → always that leg, regardless of seed."""
    r = fakeredis.aioredis.FakeRedis(decode_responses=True)
    cid, fid = "5", "100"
    await r.sadd("campaigns:active", cid)
    await r.hset(f"campaign:{cid}", mapping={"company_id": "1", "priority": "0"})
    await r.rpush(f"campaign:{cid}:flows", fid)
    await r.hset(f"flow:{fid}", mapping={
        "campaign_id": cid, "scope_type": "company", "scope_id": "1",
        "seq_id": "1", "is_default": "0", "criteria": "[]",
        "action_type": "split",
        "action_config": json.dumps({"offers": [
            {"offer_id": 1, "target_id": 10, "weight": 100}]}),
    })
    await r.hset("offer_target:10", mapping={
        "url": "https://only/{click_id}", "is_default": "0",
        "availability": "active", "offer_id": "1", "criteria": "[]",
        "priority": "0"})
    for i in range(20):
        assert _served_tid(await _route(r, f"any-{i}")) == 10


async def test_pinned_non_split_unaffected():
    """A pinned offer-action click has no RNG on its path → identical served
    target for any click_id (seeding cannot perturb a single pinned target)."""
    r = fakeredis.aioredis.FakeRedis(decode_responses=True)
    await _seed_pinned(r)
    served = {_served_tid(await _route(r, f"pinned-{i}")) for i in range(20)}
    assert served == {10}
