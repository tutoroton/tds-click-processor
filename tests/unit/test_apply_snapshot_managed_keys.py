"""Tests for the click-processor side of T1.3 / G-21 — managed-keys
SET replaces SCAN for stale-key discovery in `apply_snapshot`.

Two independent layers of defense:

  1. Source-level pin — `apply_snapshot` MUST NOT call `redis.scan`
     anywhere in its body. This is the regression fence: if a future
     refactor reintroduces SCAN (e.g., as a "safety sweep"), the test
     fails immediately, before the change ships.

  2. Functional contract — driven by `unittest.mock.AsyncMock`
     standing in for the asyncio Redis client:

     a. fresh node (empty `_MANAGED_KEY`) writes new keys, deletes
        nothing, and rebuilds `_MANAGED_KEY` with the snapshot's keys
     b. upgraded node (populated `_MANAGED_KEY`) deletes the
        delta `previous \\ new` and only that delta — no SCAN call,
        no over-delete, no leak
     c. snapshot.data == previous_managed → zero deletes (no-op
        catches bugs where a stale-detection regression silently
        wipes everything)
     d. SMEMBERS round-trip happens exactly once per apply (cost
        sanity — not 17 calls like the legacy SCAN loop)

Reference: rule `sync-protocol` "Adding New Synced Entities" + the
module-docstring "Stale-key discovery (T1.3 / G-21)" block in
`services/click-processor/app/sync_client.py`.
"""

from __future__ import annotations

import inspect
from unittest.mock import AsyncMock, MagicMock, call

import pytest


# ---------------------------------------------------------------------------
# Source-level regression fence — SCAN must not return to apply_snapshot.
# ---------------------------------------------------------------------------


class TestNoScanInApplySnapshot:
    """Pin the absence of `redis.scan` calls in the function body.

    SCAN at scale (10k+ keys per prefix bucket × 17 buckets) was the
    dominant cost of `apply_snapshot` pre-T1.3. The replacement uses
    a single `SMEMBERS(_MANAGED_KEY)` round-trip. A future refactor
    that reintroduces SCAN — even as a "safety net" — must fail this
    test loud so the perf regression cannot land silently.
    """

    def _source(self) -> str:
        from app.sync_client import apply_snapshot

        return inspect.getsource(apply_snapshot)

    def test_apply_snapshot_does_not_call_redis_scan(self):
        source = self._source()
        # The dotted call form `redis.scan(` is the only shape we use
        # client-side. Match it strictly so unrelated occurrences of
        # the substring "scan" (e.g., the comment "scan the keyspace")
        # do not falsely trigger.
        assert "redis.scan(" not in source, (
            "redis.scan(...) re-entered apply_snapshot — T1.3 / G-21 "
            "regression. Stale-key discovery must use SMEMBERS on the "
            "`_MANAGED_KEY` set, not SCAN. See module docstring "
            "'Stale-key discovery (T1.3 / G-21)' in sync_client.py."
        )

    def test_apply_snapshot_calls_smembers_managed_key(self):
        source = self._source()
        # Pin the canonical call shape — both the SET name constant
        # and the method, so a refactor that hand-rolls a string
        # literal "sync:managed_keys" still surfaces.
        assert "redis.smembers(_MANAGED_KEY)" in source, (
            "Expected `redis.smembers(_MANAGED_KEY)` in apply_snapshot "
            "as the stale-key-discovery primitive (T1.3 / G-21)."
        )

    def test_managed_key_rebuild_is_transactional(self):
        """F-4 HIGH-004 — the _MANAGED_KEY DELETE+SADD rebuild must be a
        MULTI/EXEC so a concurrent reader never sees an empty set."""
        source = self._source()
        assert "track_pipe = redis.pipeline(transaction=True)" in source, (
            "The managed-keys tracking pipeline must be transactional "
            "(HIGH-004) so DELETE + SADD commit atomically."
        )

    def test_managed_key_not_folded_into_write_pipe(self):
        """The literal 'fold _MANAGED_KEY into the write MULTI' was
        REJECTED — committing the new managed set before the Step-3
        stale deletes regresses orphan cleanup. Pin that the write
        pipeline (`write_pipe`) NEVER touches _MANAGED_KEY; the only
        _MANAGED_KEY mutations are on the `track_pipe` (post-delete)."""
        source = self._source()
        # Robust against comment/step renumbering: assert directly that
        # the write pipeline never references _MANAGED_KEY.
        assert "write_pipe.sadd(_MANAGED_KEY" not in source
        assert "write_pipe.delete(_MANAGED_KEY" not in source
        assert "write_pipe.set(_MANAGED_KEY" not in source
        # And _MANAGED_KEY IS rebuilt on the dedicated tracking pipe.
        assert "track_pipe.sadd(_MANAGED_KEY" in source

    def test_routing_prefixes_are_documentation_only(self):
        """`_ROUTING_PREFIXES` must remain in the module (operator
        debug breadcrumb for `redis-cli --scan --pattern <prefix>*`)
        but must NOT be iterated inside apply_snapshot."""
        source = self._source()
        assert "_ROUTING_PREFIXES" not in source, (
            "_ROUTING_PREFIXES leaked back into apply_snapshot — the "
            "list is documentation-only post-T1.3 (see module "
            "docstring). Iterating it implies a SCAN regression."
        )


# ---------------------------------------------------------------------------
# Functional contract — drive apply_snapshot with a mocked Redis client.
# ---------------------------------------------------------------------------


def _make_redis_mock(initial_managed: set[str] | None = None) -> AsyncMock:
    """Build an AsyncMock that mimics the subset of asyncio Redis
    methods `apply_snapshot` actually calls.

    The mock tracks pipeline operations through a small recorder so
    tests can assert what was queued in the delete-stale-keys pipeline
    (the most regression-prone branch). HSET/SADD/RPUSH/SET ops on
    the write pipeline are accepted as no-ops; we don't reverify the
    write path here — `test_admin_sync_gzip.py` and the existing
    contract pin it.
    """
    initial_managed = initial_managed or set()

    redis_mock = AsyncMock()

    # SMEMBERS returns the previous managed-keys set. asyncio-redis
    # returns a set-of-bytes/str; downstream code wraps in `set(...)`,
    # so both shapes are accepted. We return a plain set here.
    redis_mock.smembers = AsyncMock(return_value=set(initial_managed))

    # The pipeline is itself a mock — we record each method invocation
    # for inspection. Pipeline.execute() is awaited; everything else
    # is a sync chainable call (asyncio Redis pipeline semantics).
    def _make_pipeline_mock() -> MagicMock:
        pipe = MagicMock()
        pipe.execute = AsyncMock(return_value=[])
        # Sync passthrough chainable methods (return self for chaining
        # parity with the real client — apply_snapshot doesn't use
        # the chain return value, but real code does).
        for op in ("hset", "sadd", "rpush", "set", "delete"):
            setattr(pipe, op, MagicMock(return_value=pipe))
        return pipe

    # Each call to redis.pipeline() must return a fresh mock — code
    # creates THREE pipelines (write / delete-stale / track) and the
    # delete pipeline only exists when there's something to delete.
    pipelines: list[MagicMock] = []

    def _pipeline_factory(*args, **kwargs):
        # H2 fix (2026-05-11) added `transaction=True` to the write
        # pipeline construction in `apply_snapshot`. Accept arbitrary
        # args/kwargs so the test mock stays compatible with either
        # call style. The real redis-py accepts the kwarg and
        # returns a pipeline that wraps ops in MULTI/EXEC.
        p = _make_pipeline_mock()
        pipelines.append(p)
        return p

    redis_mock.pipeline = MagicMock(side_effect=_pipeline_factory)
    redis_mock._pipelines = pipelines  # backdoor for assertions
    return redis_mock


def _snapshot(data: dict, types: dict | None = None) -> dict:
    """Build a minimal snapshot envelope the way admin-api emits it."""
    return {
        "version": 1,
        "sync_version": 7,
        "timestamp": "2026-05-09T10:00:00Z",
        "key_count": len(data),
        "data": data,
        "types": types or {k: "hash" for k in data},
    }


@pytest.mark.asyncio
async def test_fresh_node_no_managed_keys_no_deletes():
    """A node with empty `_MANAGED_KEY` (brand-new install) must
    write the snapshot's keys without deleting anything — Redis can't
    have stale routing data when it never had any data at all."""
    from app.sync_client import apply_snapshot

    redis = _make_redis_mock(initial_managed=set())
    snapshot = _snapshot(
        {"campaign:1": {"name": "C1"}, "offer:1": {"url": "https://x"}},
    )

    stats = await apply_snapshot(redis, snapshot)

    assert stats["status"] == "ok"
    assert stats["keys_written"] == 2
    assert stats["stale_removed"] == 0

    # SMEMBERS called exactly once — replaces 17-prefix SCAN loop.
    redis.smembers.assert_awaited_once()
    # Two pipelines: write + tracking. NO delete-stale pipeline
    # because there are no stale keys to delete on a fresh node.
    assert len(redis._pipelines) == 2, (
        f"Expected 2 pipelines (write + tracking) on fresh node; "
        f"got {len(redis._pipelines)}. Extra pipeline implies a "
        f"phantom stale-key DELETE."
    )

    # Tracking pipeline must rewrite _MANAGED_KEY exactly to the new set.
    track_pipe = redis._pipelines[1]
    track_pipe.delete.assert_called_once_with("sync:managed_keys")
    track_pipe.sadd.assert_called_once()
    sadd_args = track_pipe.sadd.call_args
    assert sadd_args.args[0] == "sync:managed_keys"
    assert set(sadd_args.args[1:]) == {"campaign:1", "offer:1"}


@pytest.mark.asyncio
async def test_upgraded_node_deletes_only_the_delta():
    """A node carrying a populated `_MANAGED_KEY` from the previous
    apply must delete keys that are no longer in the snapshot — and
    ONLY those. This is the precision contract the legacy SCAN
    iterated 17 prefix buckets to deliver."""
    from app.sync_client import apply_snapshot

    previous = {"campaign:1", "campaign:2", "offer:1", "offer:99"}
    redis = _make_redis_mock(initial_managed=previous)
    snapshot = _snapshot(
        {
            "campaign:1": {"name": "C1"},
            "campaign:2": {"name": "C2"},
            "offer:1": {"url": "https://x"},
            # offer:99 dropped — must be deleted as stale.
            # offer:5 added — must be written but never deleted.
            "offer:5": {"url": "https://y"},
        },
    )

    stats = await apply_snapshot(redis, snapshot)

    assert stats["keys_written"] == 4
    assert stats["stale_removed"] == 1, (
        f"Expected exactly one stale key (offer:99); got "
        f"stale_removed={stats['stale_removed']}."
    )

    # Three pipelines: write + delete-stale + tracking.
    assert len(redis._pipelines) == 3
    delete_pipe = redis._pipelines[1]
    deleted_keys = [c.args[0] for c in delete_pipe.delete.call_args_list]
    assert deleted_keys == ["offer:99"], (
        f"Stale-delete pipeline should target only offer:99; got "
        f"{deleted_keys}."
    )

    # Tracking pipeline overwrites _MANAGED_KEY with the new set —
    # no leakage of offer:99 into the next apply's previous-set.
    track_pipe = redis._pipelines[2]
    sadd_args = track_pipe.sadd.call_args
    assert set(sadd_args.args[1:]) == {
        "campaign:1", "campaign:2", "offer:1", "offer:5",
    }


@pytest.mark.asyncio
async def test_crash_recovery_self_heals_stale_keys():
    """HIGH-004 invariant: the track-AFTER-delete ordering is what makes
    a crash mid-sync self-healing. Simulate the recovery: a prior apply
    crashed after the writes but before the stale-delete, so this node's
    `_MANAGED_KEY` still carries the OLD set. The NEXT apply must still
    discover and delete the now-stale keys via `all_existing` (= the old
    managed set). This is the entire correctness argument for NOT folding
    `_MANAGED_KEY` into the write MULTI."""
    from app.sync_client import apply_snapshot

    # Old managed set from before the (crashed) prior apply.
    old = {"campaign:1", "offer:1", "offer:OLD"}
    redis = _make_redis_mock(initial_managed=old)
    # New snapshot drops offer:OLD.
    snapshot = _snapshot(
        {"campaign:1": {"name": "C1"}, "offer:1": {"url": "https://x"}},
    )

    stats = await apply_snapshot(redis, snapshot)

    assert stats["stale_removed"] == 1
    delete_pipe = redis._pipelines[1]
    deleted = [c.args[0] for c in delete_pipe.delete.call_args_list]
    assert deleted == ["offer:OLD"], (
        "A crash that left _MANAGED_KEY = old set MUST still let the next "
        "apply clean the stale key — the self-healing property."
    )


@pytest.mark.asyncio
async def test_no_op_when_managed_keys_match_snapshot():
    """Steady-state: the snapshot's keys equal the previous managed
    set. Zero deletes, but the tracking pipeline still re-runs (cheap
    overwrite — and forward-compatible with sync_version bumps)."""
    from app.sync_client import apply_snapshot

    keys = {"campaign:1", "offer:1"}
    redis = _make_redis_mock(initial_managed=keys)
    snapshot = _snapshot(
        {"campaign:1": {"name": "C1"}, "offer:1": {"url": "https://x"}},
    )

    stats = await apply_snapshot(redis, snapshot)

    assert stats["stale_removed"] == 0
    # No delete pipeline — nothing to do.
    assert len(redis._pipelines) == 2


@pytest.mark.asyncio
async def test_smembers_called_exactly_once_per_apply():
    """Cost sanity: the function must do ONE round-trip for stale-key
    discovery. Pre-T1.3, this was at minimum 17 SCAN cursor iterations
    — and many more under load. A future refactor that loops SMEMBERS
    per prefix is a regression."""
    from app.sync_client import apply_snapshot

    redis = _make_redis_mock(initial_managed={"campaign:1"})
    snapshot = _snapshot({"campaign:1": {"name": "C1"}})

    await apply_snapshot(redis, snapshot)

    assert redis.smembers.await_count == 1
    redis.smembers.assert_awaited_with("sync:managed_keys")


@pytest.mark.asyncio
async def test_empty_snapshot_is_idempotent_noop():
    """An empty snapshot (admin-api gracefully returning no-op
    payload, e.g., during builder-failure recovery) must NOT touch
    Redis — neither write nor delete. Pre-T1.3 the SCAN block ran
    unconditionally before the empty-data short-circuit; today the
    short-circuit is the first non-bookkeeping line."""
    from app.sync_client import apply_snapshot

    redis = _make_redis_mock(initial_managed={"campaign:1"})
    snapshot = _snapshot({})  # empty

    stats = await apply_snapshot(redis, snapshot)

    assert stats == {"status": "empty", "keys_written": 0}
    redis.smembers.assert_not_awaited()
    redis.pipeline.assert_not_called()


@pytest.mark.asyncio
async def test_managed_key_itself_is_never_in_stale_delta():
    """Defense-in-depth: even if a future bug populates
    `_MANAGED_KEY` with itself (e.g., self-reference), the snapshot
    NEVER contains the bookkeeping key, so it would land in the
    stale set and DELETE itself — collapsing the next apply's
    discovery. This test verifies the snapshot.data convention
    holds (no self-reference) AND the delete pipeline is empty
    when there's no real stale to remove."""
    from app.sync_client import apply_snapshot

    # Snapshot intentionally omits _MANAGED_KEY (the canonical
    # admin-api convention — the bookkeeping key never appears in
    # `all_kv`). The previous managed set ALSO omits it (admin-api
    # never adds the SET name to the SET itself).
    redis = _make_redis_mock(initial_managed={"campaign:1"})
    snapshot = _snapshot({"campaign:1": {"name": "C1"}})

    await apply_snapshot(redis, snapshot)

    # Tracking pipeline DELETEs `_MANAGED_KEY` to clear it before
    # repopulating — that's the final-step rewrite, not a stale
    # delete. SADD must follow with all current keys.
    track_pipe = redis._pipelines[-1]
    track_pipe.delete.assert_called_once_with("sync:managed_keys")
    sadd_args = track_pipe.sadd.call_args
    # `sync:managed_keys` must NEVER be a member of `sync:managed_keys`.
    assert "sync:managed_keys" not in sadd_args.args[1:]
