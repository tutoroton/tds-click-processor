"""Tests for H2 fix — sync_client write pipeline uses MULTI/EXEC.

Background: `apply_snapshot` rebuilds set-type Redis keys via
`pipe.delete(key); pipe.sadd(key, *values)`. Without
`transaction=True` on the pipeline, this pair is pipelined (single
round-trip) but NOT atomic at the server. A concurrent `/decide`
reader observing the set between DELETE and SADD applying server-side
sees an empty set and falls through to no-match fallback.

Under single-process uvicorn (--workers 2 default in Dockerfile),
the race window is microseconds and rarely observable. It becomes
load-bearing if the deployment scales workers horizontally or a
future heavy `/decide` path widens the read-vs-sync race.

H2 fix: `redis.pipeline(transaction=True)` makes the entire write
batch a Redis MULTI/EXEC block. Readers see either the pre-sync set
or the post-sync set, never an empty intermediate state. The
`delete + sadd` pair for set rebuilds is now atomic.

Source-pin: pipeline factory MUST receive `transaction=True`. A
refactor that drops it re-opens H2.
"""

from __future__ import annotations

import inspect

import pytest


class TestApplySnapshotUsesTransactionPipeline:
    """The write pipeline MUST be transactional. If a refactor drops
    `transaction=True`, the race window re-opens silently — no other
    test would catch it because the existing unit tests use a mock
    pipeline that accepts any kwargs without enforcement."""

    def test_apply_snapshot_source_pins_transaction_true(self):
        """Read the apply_snapshot source and assert the write
        pipeline is constructed with transaction=True. This is the
        single canonical source pin for H2.
        """
        from app import sync_client

        src = inspect.getsource(sync_client.apply_snapshot)

        # The write pipeline is the one that batches HSET/SADD/RPUSH
        # operations. If a future refactor moves the pipeline out
        # into a helper, this test should be updated to chase the
        # helper — but it should NEVER stop asserting transaction=True
        # somewhere in the write path.
        assert "write_pipe = redis.pipeline(transaction=True)" in src, (
            "apply_snapshot MUST construct its write pipeline with "
            "transaction=True so DELETE + SADD on set-type keys is "
            "atomic at the server (H2 fix). Without this, concurrent "
            "/decide readers see empty sets during sync and fall "
            "through to no-match fallback."
        )

    def test_apply_snapshot_executes_pipeline(self):
        """Companion sanity check — the source MUST also `await
        write_pipe.execute()`. Without execute(), nothing flushes."""
        from app import sync_client

        src = inspect.getsource(sync_client.apply_snapshot)
        assert "await write_pipe.execute()" in src, (
            "apply_snapshot must await write_pipe.execute() — without "
            "it the pipeline never flushes to Redis."
        )
