"""A-2 — apply_snapshot must CLEAR a collection that transitions to empty.

Background (RV-C3.md §A-2): `apply_snapshot` guarded the set/list write
behind `if value:`, so an emptied collection skipped both DELETE and the
re-populate. Because the key is added to `new_keys` UNCONDITIONALLY
(line ~`new_keys.add(key)`), it does NOT fall into the stale-delete
delta either — so the previous members persisted on the node. The only
always-emitted collection is `campaigns:active` (emptied only when every
campaign is archived), which is why this is latent / LOW, but the
mechanism is real and would mask a future builder bug that emits an
empty set.

Fix: hoist `delete(key)` ABOVE the `if value:` for the set + list
branches, so an empty value still clears stale members; a non-empty
value is byte-identical (delete + sadd/rpush). DELETE of an absent key
is a harmless no-op.

These pin the runtime behaviour with a mocked Redis pipeline (mirrors
`test_apply_snapshot_managed_keys.py`). The hash branch is intentionally
left to the A-1 fix and is NOT touched here.
"""

from __future__ import annotations

import inspect
from unittest.mock import AsyncMock, MagicMock

import pytest


def _make_redis_mock(initial_managed: set[str] | None = None) -> AsyncMock:
    """AsyncMock Redis mirroring the subset apply_snapshot calls.

    Records each pipeline so tests can assert what the WRITE pipeline
    (the first one created) queued. Same shape as the helper in
    `test_apply_snapshot_managed_keys.py`.
    """
    initial_managed = initial_managed or set()
    redis_mock = AsyncMock()
    redis_mock.smembers = AsyncMock(return_value=set(initial_managed))

    def _make_pipeline_mock() -> MagicMock:
        pipe = MagicMock()
        pipe.execute = AsyncMock(return_value=[])
        for op in ("hset", "sadd", "rpush", "set", "delete"):
            setattr(pipe, op, MagicMock(return_value=pipe))
        return pipe

    pipelines: list[MagicMock] = []

    def _pipeline_factory(*args, **kwargs):
        p = _make_pipeline_mock()
        pipelines.append(p)
        return p

    redis_mock.pipeline = MagicMock(side_effect=_pipeline_factory)
    redis_mock._pipelines = pipelines
    return redis_mock


def _snapshot(data: dict, types: dict) -> dict:
    return {
        "version": 1,
        "sync_version": 7,
        "timestamp": "2026-06-08T00:00:00Z",
        "key_count": len(data),
        "data": data,
        "types": types,
    }


def _write_pipe_delete_keys(redis) -> list[str]:
    """All keys DELETE-d on the WRITE pipeline (pipelines[0])."""
    write_pipe = redis._pipelines[0]
    return [c.args[0] for c in write_pipe.delete.call_args_list]


# ---------------------------------------------------------------------------
# Source-level pin — DELETE hoisted above `if value:` for set + list.
# ---------------------------------------------------------------------------


def test_set_and_list_delete_is_unconditional():
    from app import sync_client

    src = inspect.getsource(sync_client.apply_snapshot)

    # Set branch: `write_pipe.delete(key)` must be HOISTED above the
    # `if value:` guard (so an emptied set clears), while SADD stays
    # guarded by `if value:`. Bound the branch to its own elif block so
    # intervening comments don't matter.
    set_branch = src[
        src.index('elif key_type == "set"'):src.index('elif key_type == "list"')
    ]
    assert set_branch.index("write_pipe.delete(key)") < set_branch.index("if value:"), (
        "set branch DELETE must be hoisted ABOVE `if value:` (A-2) so an "
        "emptied set clears its stale members."
    )
    assert set_branch.index("if value:") < set_branch.index("write_pipe.sadd("), (
        "SADD must stay guarded by `if value:` (nothing to add when empty)."
    )

    # List branch: same hoisted shape (bounded to the final `else:`).
    list_start = src.index('elif key_type == "list"')
    list_branch = src[list_start:src.index("else:", list_start)]
    assert list_branch.index("write_pipe.delete(key)") < list_branch.index("if value:"), (
        "list branch DELETE must be hoisted ABOVE `if value:` (A-2)."
    )
    assert list_branch.index("if value:") < list_branch.index("write_pipe.rpush("), (
        "RPUSH must stay guarded by `if value:`."
    )


# ---------------------------------------------------------------------------
# Behavioural — empty set/list clears; non-empty is byte-identical.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_empty_set_is_cleared():
    """A set key that arrives empty (previously populated) must be DELETE-d
    on the write pipeline — and NOT re-SADD-ed (nothing to add)."""
    from app.sync_client import apply_snapshot

    # `campaigns:active` previously had members; now arrives empty. Pair it
    # with a real hash so the snapshot isn't an all-empty no-op.
    redis = _make_redis_mock(initial_managed={"campaigns:active", "campaign:1"})
    snapshot = _snapshot(
        {"campaigns:active": [], "campaign:1": {"name": "C1"}},
        {"campaigns:active": "set", "campaign:1": "hash"},
    )

    stats = await apply_snapshot(redis, snapshot)

    assert stats["status"] == "ok"
    write_pipe = redis._pipelines[0]
    assert "campaigns:active" in _write_pipe_delete_keys(redis), (
        "an emptied set must be DELETE-d on the write pipeline (A-2)"
    )
    # No SADD for the empty set (nothing to add).
    sadd_keys = [c.args[0] for c in write_pipe.sadd.call_args_list]
    assert "campaigns:active" not in sadd_keys


@pytest.mark.asyncio
async def test_empty_list_is_cleared():
    """Same A-2 hoist for a list-type key."""
    from app.sync_client import apply_snapshot

    redis = _make_redis_mock(initial_managed={"flow:1:order", "campaign:1"})
    snapshot = _snapshot(
        {"flow:1:order": [], "campaign:1": {"name": "C1"}},
        {"flow:1:order": "list", "campaign:1": "hash"},
    )

    await apply_snapshot(redis, snapshot)

    write_pipe = redis._pipelines[0]
    assert "flow:1:order" in _write_pipe_delete_keys(redis)
    rpush_keys = [c.args[0] for c in write_pipe.rpush.call_args_list]
    assert "flow:1:order" not in rpush_keys


@pytest.mark.asyncio
async def test_non_empty_set_still_delete_then_sadd():
    """Regression: a NON-empty set must remain delete + sadd, byte-for-byte
    the legacy behaviour (exact membership, no stale members)."""
    from app.sync_client import apply_snapshot

    redis = _make_redis_mock(initial_managed={"campaigns:active"})
    snapshot = _snapshot(
        {"campaigns:active": ["1", "2", "3"]},
        {"campaigns:active": "set"},
    )

    await apply_snapshot(redis, snapshot)

    write_pipe = redis._pipelines[0]
    assert "campaigns:active" in _write_pipe_delete_keys(redis)
    sadd_keys = [c.args[0] for c in write_pipe.sadd.call_args_list]
    assert "campaigns:active" in sadd_keys, (
        "a non-empty set must still be re-populated via SADD"
    )
    # Members passed through intact.
    sadd_call = next(
        c for c in write_pipe.sadd.call_args_list if c.args[0] == "campaigns:active"
    )
    assert set(sadd_call.args[1:]) == {"1", "2", "3"}
