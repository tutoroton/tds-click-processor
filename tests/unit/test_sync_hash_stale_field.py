"""B3/V24 — a field we STOP sending must not live forever on the edge node.

`apply_snapshot` has three collection branches. A-2 hoisted `DELETE` above the
value-guard for `set` and `list`, and wrote the governing property in its own
words while doing so:

    # ... HSET/SADD never remove omitted/absent members ...

It names HSET. The hash branch was left with a bare HSET and this comment:

    # HSET is idempotent -- overwrites fields in-place, no delete needed

That is a true statement about the fields you SEND. It says nothing about the
fields you STOPPED sending, which is the whole of V24.

TWO defects, the second worse than the first:

  B3a  a field disappears from the payload -> its old value lives forever
  B3b  the hash goes ALL-THE-WAY empty    -> `if value:` issues NO command at
                                             all, so EVERY old field survives

B3b is exactly the case A-2 hoisted the delete for on set/list ("a collection
that transitions all-the-way to empty in PG clears its stale members"). The
same hoist never reached hash.

WHY THE OTHER SAFETY NET DOES NOT COVER THIS. Step 3 computes
`stale_keys = all_existing - new_keys` and deletes keys that vanish ENTIRELY
from the payload. That rescues the whole-key case and is pinned below as a
control. It cannot rescue a key that is still present with fewer fields --
which is precisely the shape of both defects.

This mechanism has already bitten once: `campaigns.py`'s `fallback_url` is
emitted unconditionally with an explicit comment that a conditional emit "left
the old URL stale on the node". That was fixed for ONE FIELD. This closes the
protocol instead of patching the next field.
"""

from __future__ import annotations

import fakeredis.aioredis
import pytest

from app.sync_client import apply_snapshot

pytestmark = pytest.mark.asyncio


async def _redis():
    return fakeredis.aioredis.FakeRedis(decode_responses=True)


def _snapshot(data, types, version=2):
    return {"data": data, "types": types, "sync_version": version}


async def _seed_via_sync(r, data, types):
    """Seed through the real code path, so `_MANAGED_KEY` is populated exactly
    as a previous successful apply would leave it. Seeding with a raw HSET
    would skip that and make the Step-3 control below vacuous."""
    await apply_snapshot(r, _snapshot(data, types, version=1))


# ---- B3a — a field that vanishes from the payload --------------------------

async def test_dropped_field_does_not_survive_on_the_node():
    """The field is gone from the new payload; the key remains. Its old value
    must not be readable afterwards."""
    r = await _redis()
    await _seed_via_sync(
        r,
        {"campaign:1": {"url": "https://new", "fallback_url": "https://OLD-STALE"}},
        {"campaign:1": "hash"},
    )
    assert await r.hget("campaign:1", "fallback_url") == "https://OLD-STALE"

    # The operator clears the fallback; the builder stops emitting the field.
    await apply_snapshot(
        r, _snapshot({"campaign:1": {"url": "https://new"}}, {"campaign:1": "hash"})
    )

    assert await r.hget("campaign:1", "fallback_url") is None, (
        "a field we stopped sending is still being served from the node"
    )
    assert await r.hget("campaign:1", "url") == "https://new"  # the sent field lands


# ---- B3b — the hash goes all-the-way empty ---------------------------------

async def test_emptied_hash_clears_every_field():
    """`if value:` skips the write entirely for an empty dict, so without the
    hoist NOTHING is issued and every old field survives. This is the case the
    set/list branches were explicitly hoisted for."""
    r = await _redis()
    await _seed_via_sync(
        r,
        {"campaign:2:source_overrides": {"7": "a", "9": "b"}},
        {"campaign:2:source_overrides": "hash"},
    )
    assert await r.hlen("campaign:2:source_overrides") == 2

    await apply_snapshot(
        r,
        _snapshot(
            # still PRESENT in the payload (so Step 3 will not rescue it), but empty
            {"campaign:2:source_overrides": {}, "campaign:9": {"url": "x"}},
            {"campaign:2:source_overrides": "hash", "campaign:9": "hash"},
        ),
    )

    assert await r.hlen("campaign:2:source_overrides") == 0, (
        "an emptied hash kept all of its stale fields"
    )


# ---- CONTROLS — each must pass BEFORE and AFTER the fix --------------------

async def test_control_present_fields_still_land():
    """The ordinary update path is unchanged."""
    r = await _redis()
    await _seed_via_sync(r, {"campaign:3": {"url": "old"}}, {"campaign:3": "hash"})
    await apply_snapshot(
        r, _snapshot({"campaign:3": {"url": "new", "geo": "US"}}, {"campaign:3": "hash"})
    )
    assert await r.hgetall("campaign:3") == {"url": "new", "geo": "US"}


async def test_control_set_branch_already_clears():
    """The asymmetry this item is about: `set` was ALREADY hoisted, so it
    clears. If this ever fails the instrument is wrong, not the subject."""
    r = await _redis()
    await _seed_via_sync(r, {"campaigns:active": ["1", "2"]}, {"campaigns:active": "set"})
    assert await r.scard("campaigns:active") == 2
    await apply_snapshot(
        r, _snapshot({"campaigns:active": [], "k": {"a": "b"}},
                     {"campaigns:active": "set", "k": "hash"})
    )
    assert await r.scard("campaigns:active") == 0


async def test_control_whole_key_removal_still_works():
    """Step 3 (`stale_keys = all_existing - new_keys`) is the OTHER net, and it
    must keep working — it is what makes the single-override case in live
    staging config harmless today."""
    r = await _redis()
    await _seed_via_sync(
        r, {"campaign:4": {"url": "u"}, "campaign:5": {"url": "v"}},
        {"campaign:4": "hash", "campaign:5": "hash"},
    )
    await apply_snapshot(
        r, _snapshot({"campaign:4": {"url": "u"}}, {"campaign:4": "hash"})
    )
    assert await r.exists("campaign:5") == 0
    assert await r.hget("campaign:4", "url") == "u"
