"""LOSSFIX-4 (2026-08-15) — the edge deadletter park's replay procedure.

The park's whole justification is that a click the shipper gave up on is
RECOVERABLE rather than retired into an evicting ring. That claim is worth
exactly as much as the tool that brings it back, so the tool's safety
properties are pinned here.

Two are load-bearing, and they are different from the collector tool's:

* **The file is CLAIMED (atomic rename) before it is read.** The collector's
  park is a Redis stream where XDEL is per-entry, so per-entry ordering was
  enough. A FILE is rewritten whole — so a live worker appending mid-replay
  would have its append deleted by the rewrite. `TestTheClaimProtectsALiveWriter`
  is the test that fails if the claim is removed.
* **Re-add before removal, per entry** — a record leaves the park only after
  its own XADD landed. `TestTheRemovalIsGatedOnTheReinjection` is the pair.

Every refusal here has a positive control beside it: a gate that says no to
everything is not a gate, it is a wedge.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import fakeredis.aioredis
import pytest

from app import disk_queue


def _load_tool():
    path = (Path(__file__).resolve().parents[2] / "scripts"
            / "replay_deadletter_park.py")
    spec = importlib.util.spec_from_file_location("replay_deadletter_park", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


tool = _load_tool()


@pytest.fixture(autouse=True)
def _park_root(tmp_path, monkeypatch):
    """Point BOTH the tool and the service module at a scratch root — they
    read `settings.disk_queue_root` from two different imported `settings`
    objects only if the import graph diverges, so pin both explicitly."""
    monkeypatch.setattr(disk_queue.settings, "disk_queue_root", str(tmp_path))
    monkeypatch.setattr(tool.settings, "disk_queue_root", str(tmp_path))
    return tmp_path


def _args(**kw):
    base = dict(reason=None, limit=0, apply=False, out=None,
                i_have_fixed_the_payload=False)
    base.update(kw)
    return SimpleNamespace(**base)


def _redis():
    """Plain helper, not an async fixture — matches the idiom in
    test_identity_mint.py and sidesteps pytest-asyncio strict mode."""
    return fakeredis.aioredis.FakeRedis(decode_responses=True)


def _rec(click_id: str, reason: str = "queue_failure") -> dict:
    return {
        "click_id": click_id,
        "data": json.dumps({"click_id": click_id, "geo": "UA"}),
        "attempt_count": "5",
        "last_rejection_reason": reason,
        "deadlettered_at": "1755000000.0",
        "node_id": "edge-test",
    }


def _write_park(root: Path, name: str, records: list[dict]) -> Path:
    d = root / disk_queue._PARK_DIRNAME
    d.mkdir(parents=True, exist_ok=True)
    p = d / name
    p.write_text("".join(json.dumps(r) + "\n" for r in records))
    return p


def _remaining(root: Path) -> list[dict]:
    out: list[dict] = []
    for f in sorted((root / disk_queue._PARK_DIRNAME).glob("park-*.ndjson")):
        out += [json.loads(line) for line in f.read_text().splitlines() if line]
    return out


class TestTheClaimProtectsALiveWriter:
    """🔴 The property unique to a FILE-backed park. Remove `_claim` from
    `cmd_replay` and this class is what goes red."""

    @pytest.mark.asyncio
    async def test_a_park_written_mid_replay_survives_the_rewrite(self, _park_root):
        """A worker deadlettering a click WHILE the replay runs must not have
        it deleted by the replay's rewrite of the remainder.

        🔴 The park file MUST be named for the REAL live worker prefix. An
        earlier version of this test used an invented `park-100-1.ndjson`
        while the simulated worker appended to `_park_path()` — two DIFFERENT
        files, so the race it claimed to test could not occur, and it passed
        against claim-less code. A test whose setup does not create the
        condition it names is not a test (ANCHOR §185).
        """
        live = f"park-{disk_queue._worker_prefix()}.ndjson"
        _write_park(_park_root, live, [_rec("old")])
        r = _redis()

        real_xadd = r.xadd
        appended: list[str] = []

        async def _xadd_then_a_worker_parks(*a, **kw):
            res = await real_xadd(*a, **kw)
            # The live worker's own park path — exactly what
            # `disk_queue._append_parked_sync` does, mid-flight.
            if not appended:
                assert await disk_queue.enqueue_parked_click(_rec("arrived-during"))
                appended.append("x")
            return res

        r.xadd = _xadd_then_a_worker_parks
        await tool.cmd_replay(r, _args(reason="queue_failure", apply=True))

        ids = {rec["click_id"] for rec in _remaining(_park_root)}
        assert "arrived-during" in ids, (
            "the replay's rewrite deleted a click a live worker parked while "
            "it was running — the tool became the loss it exists to undo"
        )
        assert "old" not in ids  # control: the replayed one DID leave

    @pytest.mark.asyncio
    async def test_a_claimed_leftover_is_re_adopted_not_stranded(self, _park_root):
        """A previous run that died after claiming must not strand the park.
        A claimed snapshot is still counted by the `park_lines` health signal
        (it keeps the `.ndjson` extension on purpose), but nothing DRAINS it
        — if this tool did not pick it up, nothing ever would."""
        _write_park(_park_root, "park-100-1-claimed-1755000000.ndjson",
                    [_rec("stranded")])
        r = _redis()
        await tool.cmd_replay(r, _args(reason="queue_failure", apply=True))

        assert await r.xlen(tool.STREAM_KEY) == 1
        assert _remaining(_park_root) == []

    @pytest.mark.asyncio
    async def test_claiming_is_idempotent_and_does_not_grow_the_name(self):
        """Re-claiming on every attempt would grow the filename without bound
        across repeated failed runs."""
        p = Path("/tmp/park-1-2-claimed-99.ndjson")
        assert tool._claim(p) == p


class TestTheRemovalIsGatedOnTheReinjection:
    """A record leaves the park only after ITS OWN XADD landed. The pair
    below is the measurement: without the control, 'nothing was removed'
    could equally mean the tool is simply broken."""

    @pytest.mark.asyncio
    async def test_a_failed_xadd_leaves_the_record_parked(self, _park_root):
        _write_park(_park_root, "park-100-1.ndjson", [_rec("c1")])
        r = AsyncMock()
        # A healthy backpressure gate, so the ONLY thing under test here is
        # the XADD failure — not an incidental refusal earlier in the path.
        r.xlen = AsyncMock(return_value=0)
        r.info = AsyncMock(return_value={"used_memory": 1, "maxmemory": 100})
        r.xadd = AsyncMock(side_effect=RuntimeError("edge redis down"))

        rc = await tool.cmd_replay(r, _args(reason="queue_failure", apply=True))

        assert rc == 1, "a failed replay must exit non-zero"
        assert [rec["click_id"] for rec in _remaining(_park_root)] == ["c1"]

    @pytest.mark.asyncio
    async def test_the_control_a_successful_xadd_does_remove_it(self, _park_root):
        _write_park(_park_root, "park-100-1.ndjson", [_rec("c1")])
        r = _redis()

        rc = await tool.cmd_replay(r, _args(reason="queue_failure", apply=True))

        assert rc == 0
        assert _remaining(_park_root) == []
        entries = await r.xrange(tool.STREAM_KEY)
        assert json.loads(entries[0][1]["data"])["click_id"] == "c1"

    @pytest.mark.asyncio
    async def test_the_reinjected_shape_is_what_the_shipper_reads(self, _park_root):
        """`_retry_click` XADDs `{"data": <click json>}` — anything else is
        re-injected into a stream nothing can parse."""
        _write_park(_park_root, "park-100-1.ndjson", [_rec("c1")])
        r = _redis()
        await tool.cmd_replay(r, _args(reason="queue_failure", apply=True))

        _, fields = (await r.xrange(tool.STREAM_KEY))[0]
        assert set(fields) == {"data"}

    @pytest.mark.asyncio
    async def test_only_the_named_reason_moves(self, _park_root):
        _write_park(_park_root, "park-100-1.ndjson", [
            _rec("transient", "queue_failure"),
            _rec("other", "counter_error:queue_failure"),
        ])
        r = _redis()
        await tool.cmd_replay(r, _args(reason="queue_failure", apply=True))

        assert [rec["click_id"] for rec in _remaining(_park_root)] == ["other"]
        assert await r.xlen(tool.STREAM_KEY) == 1

    @pytest.mark.asyncio
    async def test_a_record_with_no_payload_is_kept_not_dropped(self, _park_root):
        """There is nothing to replay, so there is also nothing that would
        justify deleting it. Keep it and count it as skipped."""
        broken = _rec("c1")
        del broken["data"]
        _write_park(_park_root, "park-100-1.ndjson", [broken])
        r = _redis()
        await tool.cmd_replay(r, _args(reason="queue_failure", apply=True))

        assert [rec["click_id"] for rec in _remaining(_park_root)] == ["c1"]
        assert await r.xlen(tool.STREAM_KEY) == 0


class TestRefusals:
    @pytest.mark.asyncio
    async def test_a_data_verdict_reason_refuses(self, _park_root):
        _write_park(_park_root, "park-100-1.ndjson",
                    [_rec("c1", "validation_failed")])
        with pytest.raises(SystemExit):
            await tool.cmd_replay(_redis(),
                                  _args(reason="validation_failed", apply=True))

    @pytest.mark.asyncio
    async def test_a_data_verdict_nested_in_a_transient_wrapper_also_refuses(self):
        """The shipper composes `counter_error:{reason}` — the collector's
        verdict can be the INNER half. Matching only the prefix would replay
        a payload the collector will reject identically, forever."""
        assert tool._is_data_verdict("counter_error:validation_failed")
        assert tool._is_data_verdict("requeue_error:missing_click_id")

    @pytest.mark.asyncio
    async def test_the_control_a_transient_reason_does_not_refuse(self, _park_root):
        """Without this, the refusal above could be a tool that refuses
        everything."""
        _write_park(_park_root, "park-100-1.ndjson", [_rec("c1")])
        assert not tool._is_data_verdict("counter_error:queue_failure")
        rc = await tool.cmd_replay(_redis(),
                                   _args(reason="queue_failure", apply=True))
        assert rc == 0

    @pytest.mark.asyncio
    async def test_the_override_is_available_and_explicit(self, _park_root):
        _write_park(_park_root, "park-100-1.ndjson",
                    [_rec("c1", "validation_failed")])
        rc = await tool.cmd_replay(
            _redis(), _args(reason="validation_failed", apply=True,
                            i_have_fixed_the_payload=True))
        assert rc == 0

    @pytest.mark.asyncio
    async def test_no_reason_refuses(self):
        with pytest.raises(SystemExit):
            await tool.cmd_replay(_redis(), _args(apply=True))

    @pytest.mark.asyncio
    async def test_a_full_stream_refuses(self, monkeypatch):
        monkeypatch.setattr(tool.settings, "stream_clicks_maxlen", 3)
        r = AsyncMock()
        r.xlen = AsyncMock(return_value=3)
        with pytest.raises(SystemExit, match="M1 reject threshold"):
            await tool._assert_safe_to_write(r)

    @pytest.mark.asyncio
    async def test_a_saturated_routing_redis_refuses(self, monkeypatch):
        monkeypatch.setattr(tool.settings, "stream_clicks_maxlen", 1000)
        monkeypatch.setattr(tool.settings, "watermark_shed_pct", 85.0)
        r = AsyncMock()
        r.xlen = AsyncMock(return_value=1)
        r.info = AsyncMock(return_value={"used_memory": 90, "maxmemory": 100})
        with pytest.raises(SystemExit, match="shed watermark"):
            await tool._assert_safe_to_write(r)

    @pytest.mark.asyncio
    async def test_the_control_a_healthy_edge_passes_the_gate(self, monkeypatch):
        """Both refusals above are only measurements because this passes."""
        monkeypatch.setattr(tool.settings, "stream_clicks_maxlen", 1000)
        r = AsyncMock()
        r.xlen = AsyncMock(return_value=1)
        r.info = AsyncMock(return_value={"used_memory": 10, "maxmemory": 100})
        await tool._assert_safe_to_write(r)  # must not raise

    @pytest.mark.asyncio
    async def test_an_unreadable_memory_probe_degrades_LOUDLY(
            self, monkeypatch, capsys):
        """A silently-skipped safety check reads exactly like a passed one."""
        monkeypatch.setattr(tool.settings, "stream_clicks_maxlen", 1000)
        r = AsyncMock()
        r.xlen = AsyncMock(return_value=1)
        r.info = AsyncMock(side_effect=Exception("INFO disabled"))
        await tool._assert_safe_to_write(r)
        assert "could not read Redis memory" in capsys.readouterr().err

    @pytest.mark.asyncio
    async def test_a_dry_run_writes_nothing_and_deletes_nothing(self, _park_root):
        _write_park(_park_root, "park-100-1.ndjson", [_rec("c1")])
        r = _redis()
        await tool.cmd_replay(r, _args(reason="queue_failure", apply=False))

        assert await r.xlen(tool.STREAM_KEY) == 0
        assert [rec["click_id"] for rec in _remaining(_park_root)] == ["c1"]


class TestScanScope:
    @pytest.mark.asyncio
    async def test_every_workers_park_is_seen_not_just_this_process(
            self, _park_root, capsys):
        """A recovery tool scoped to the CURRENT process's park file would
        skip exactly the parks of dead workers — the ones nobody else is
        coming back for. Node-wide, like the `park_lines` health signal."""
        _write_park(_park_root, "park-100-1.ndjson", [_rec("mine")])
        _write_park(_park_root, "park-200-2.ndjson", [_rec("a-dead-worker")])
        _write_park(_park_root, "park-300-3-claimed-99.ndjson", [_rec("stranded")])

        await tool.cmd_list(_redis(), _args())
        out = capsys.readouterr().out
        assert "3 parked click(s) in 3 file(s)" in out
        assert "CLAIMED" in out

    @pytest.mark.asyncio
    async def test_a_torn_line_is_skipped_not_replayed_half(
            self, _park_root, capsys):
        d = _park_root / disk_queue._PARK_DIRNAME
        d.mkdir(parents=True, exist_ok=True)
        (d / "park-100-1.ndjson").write_text(
            json.dumps(_rec("whole")) + "\n" + '{"click_id":"tor')

        r = _redis()
        await tool.cmd_replay(r, _args(reason="queue_failure", apply=True))

        assert await r.xlen(tool.STREAM_KEY) == 1
        assert "not valid JSON" in capsys.readouterr().err

    @pytest.mark.asyncio
    async def test_an_absent_park_directory_is_not_an_error(self, _park_root):
        assert tool._park_files() == []
        rc = await tool.cmd_replay(_redis(),
                                   _args(reason="queue_failure", apply=True))
        assert rc == 0

    @pytest.mark.asyncio
    async def test_the_limit_caps_the_replay_and_keeps_the_rest(self, _park_root):
        _write_park(_park_root, "park-100-1.ndjson",
                    [_rec(f"c{i}") for i in range(5)])
        r = _redis()
        await tool.cmd_replay(r, _args(reason="queue_failure", limit=2, apply=True))

        assert await r.xlen(tool.STREAM_KEY) == 2
        assert len(_remaining(_park_root)) == 3
