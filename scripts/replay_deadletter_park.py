#!/usr/bin/env python3
"""Operator tool for the DURABLE edge deadletter park (LOSSFIX-4, 2026-08-15).

`shipper._deadletter_click` now parks a click it is giving up on onto this
node's disk BEFORE anything else, and that park is the sole authority on
whether the click may be ACKed off `stream:clicks`. But "parked, not lost" is
only true if the click can come BACK — without a replay path, "parked" is a
politer word for lost, and LOSSFIX-4's whole durability claim would rest on a
procedure nobody wrote. This is that procedure, executable.

⚠️ **PER-NODE.** The park lives on the edge node's own disk
(`{TDS_DISK_QUEUE_ROOT}/deadletter-park/`), so this runs ON THAT NODE with the
service's own environment. Unlike the collector's `replay_poison_park.py`
there is no central copy to work from — each node is replayed separately.

    # what is in there, and why
    python3 scripts/replay_deadletter_park.py list

    # take a copy out before touching anything
    python3 scripts/replay_deadletter_park.py export --out /root/park.jsonl

    # put them back on stream:clicks (dry-run unless --apply)
    python3 scripts/replay_deadletter_park.py replay --reason queue_failure
    python3 scripts/replay_deadletter_park.py replay --reason queue_failure --apply

Safety properties, each deliberate:

* **CLAIM the whole file by atomic rename, before reading a byte of it.** A
  live worker appends to the park by PATH (`disk_queue._append_parked_sync`
  opens `_park_path()` fresh on every call), so after the rename its next
  park creates a NEW file and this tool owns a frozen snapshot. Without the
  claim, rewriting the remainder of a file a worker is still appending to
  would silently delete whatever it appended while we were replaying — this
  tool would itself become the loss it exists to undo.
* **Re-add BEFORE removal, per entry.** A click leaves the park file only
  after ITS OWN XADD to `stream:clicks` succeeded. Every crash window points
  at a DUPLICATE (the collector's `click:central_seen` dedup collapses it),
  never at a loss.
* **Dry-run by default.** `--apply` is the only thing that mutates.
* **Data-verdict reasons are NOT replayable by default.** `validation_failed`
  and `missing_click_id` are the COLLECTOR's judgement on the payload itself
  (`collector/app/models.py`): replaying gets an identical rejection and
  re-parks the click — a loop that only grows the park. Export those, fix
  them, and POST to `/api/clicks/batch`.
* **Refuses to run under backpressure.** Sharper here than at the collector:
  `stream:clicks` lives on the edge's ROUTING Redis (256 MB), the same
  instance `/decide` reads on every request. Replaying into a saturated one
  does not slow a recovery down — it takes routing down. Thresholds are read
  from the service's own settings, never re-invented here.
* **Never trims, never MAXLENs.** Re-injection matches `_retry_click`
  exactly: `XADD stream:clicks {"data": <click json>}`, no `maxlen`.
* **Says out loud that it does NOT know whether a click is already stored**
  (V12/NV-021, 2026-08-25). Every other replay tool in this system can ask
  ClickHouse; this one cannot, and must not learn how: rule `architecture`
  makes it an invariant that the click-processor never reads PG or CH, and
  breaching an architecture invariant to improve a script is the wrong
  trade. So instead of guessing, it states the gap and names the handoff.

  🔴 **The handoff, when you need the answer:** export here, carry the file
  to the central host, and ask the collector's tool, which has the client --

      # on this node
      python3 scripts/replay_deadletter_park.py export --out /root/park.jsonl
      # on the central host, against the same file
      python3 scripts/replay_poison_park.py check --in /root/park.jsonl

  Replaying without doing that is not dangerous -- every reader plane
  deduplicates by design -- but it may restore nothing while reporting
  success, which is the defect this note exists to stop being invisible.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import redis.asyncio as aioredis  # noqa: E402

from app.config import settings  # noqa: E402
from app.disk_queue import _PARK_DIRNAME, _fsync_dir_sync  # noqa: E402

STREAM_KEY = "stream:clicks"  # lockstep with shipper.STREAM_KEY

# Reasons that are the COLLECTOR's verdict on the PAYLOAD, not a fault on our
# side. Sources: collector/app/models.py (`missing_click_id`,
# `validation_failed`). A reason is composed by the shipper as
# `counter_error:{reason}` / `requeue_error:{reason}`, so the data verdict can
# be the INNER half — match anywhere in the string, not just at the start.
DATA_VERDICT_TOKENS = ("validation_failed", "missing_click_id")

# `-claimed-{ts}` is how a claimed snapshot is named. It deliberately keeps the
# `.ndjson` extension so the service's own byte-cap scan
# (`disk_queue._scan_queue_stats_sync`, an rglob) keeps counting its bytes even
# while this tool owns it — a claimed park must never become invisible to the
# gate that bounds disk use.
_CLAIM_INFIX = "-claimed-"


def _is_data_verdict(reason: str) -> bool:
    return any(tok in reason for tok in DATA_VERDICT_TOKENS)


def _park_dir() -> Path:
    return Path(settings.disk_queue_root) / _PARK_DIRNAME


def _park_files() -> list[Path]:
    """Every park file on this node — this worker's, every SIBLING worker's,
    every DEAD worker's, and any snapshot a previous run of this tool claimed
    and then crashed before finishing.

    The same scope as the service's own `park_lines` health signal
    (`disk_queue._scan_queue_stats_sync`) — deliberately node-wide, because a
    per-worker count would hide exactly the parks nobody is coming back for.
    """
    d = _park_dir()
    if not d.exists():
        return []
    return sorted(d.glob("park-*.ndjson"))


def _read_records(path: Path) -> list[dict]:
    """Parse one park file. A torn last line (a crash mid-append, before the
    fsync returned) is dropped with a warning — that click's park never
    returned True, so the shipper never ACKed it and it is still in the PEL.
    Dropping it here is not loss; silently REPLAYING half a record would be."""
    out: list[dict] = []
    try:
        raw = path.read_text()
    except OSError as exc:
        print(f"WARNING: cannot read {path.name}: {exc}", file=sys.stderr)
        return out
    for i, line in enumerate(raw.splitlines(), 1):
        if not line.strip():
            continue
        try:
            out.append(json.loads(line))
        except (json.JSONDecodeError, ValueError):
            print(f"WARNING: {path.name}:{i} is not valid JSON — skipped "
                  f"(a torn tail is expected; the click it represents was "
                  f"never ACKed and is still in the PEL)", file=sys.stderr)
    return out


def _claim(path: Path) -> Path:
    """Atomically take ownership of a park file. Returns the claimed path.

    An already-claimed file (a previous run died mid-replay) is returned
    as-is: it is already ours, nobody else writes it, and re-claiming would
    grow the name on every attempt.
    """
    if _CLAIM_INFIX in path.name:
        return path
    claimed = path.with_name(
        f"{path.stem}{_CLAIM_INFIX}{int(time.time())}{path.suffix}")
    os.rename(path, claimed)
    _fsync_dir_sync(claimed.parent)
    return claimed


def _rewrite_remainder(path: Path, records: list[dict]) -> None:
    """Replace a CLAIMED file's content with exactly `records` (write tmp →
    fsync → atomic rename → fsync dir). Empty ⇒ the file is removed.

    Safe only because the file is claimed: no live worker holds this path, so
    there is no append to lose between the read and the rename.
    """
    if not records:
        try:
            os.unlink(path)
            _fsync_dir_sync(path.parent)
        except OSError as exc:
            print(f"WARNING: could not remove drained park file {path.name}: "
                  f"{exc}", file=sys.stderr)
        return
    tmp = path.with_name(path.name + ".tmp")
    payload = "".join(json.dumps(rec) + "\n" for rec in records).encode()
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.write(fd, payload)
        os.fsync(fd)
    finally:
        os.close(fd)
    os.rename(tmp, path)
    _fsync_dir_sync(path.parent)


async def _assert_safe_to_write(r) -> None:
    """The edge equivalent of the collector tool's gate, and a sharper one:
    `stream:clicks` shares the routing Redis with `/decide`."""
    stream_len = await r.xlen(STREAM_KEY)
    if stream_len >= settings.stream_clicks_maxlen:
        raise SystemExit(
            f"REFUSING: stream:clicks has {stream_len} entries, at/over the "
            f"M1 reject threshold {settings.stream_clicks_maxlen}. Live clicks "
            f"are ALREADY being diverted to the disk fallback; replaying now "
            f"makes it worse. Let the shipper drain first."
        )
    try:
        info = await r.info("memory")
    except Exception as exc:  # noqa: BLE001
        # Mirrors `app/watermark.py`, which does NOT shed on a probe failure.
        # Say so out loud rather than let a silently-skipped safety check read
        # as a passed one.
        print(f"WARNING: could not read Redis memory ({exc}) — proceeding on "
              f"the entry-count check alone.", file=sys.stderr)
        return
    used, limit = info.get("used_memory", 0), info.get("maxmemory", 0)
    if limit:
        pct = 100.0 * used / limit
        if pct >= settings.watermark_shed_pct:
            raise SystemExit(
                f"REFUSING: edge routing Redis at {pct:.1f}% of maxmemory, "
                f"at/over the shed watermark {settings.watermark_shed_pct}%. "
                f"This is the SAME instance /decide reads — replaying into it "
                f"now risks the routing path, not just this recovery."
            )


# V12/NV-021: this tool cannot see ClickHouse (and must not -- see the
# module docstring). Both `list` and `replay` print this so that "restored
# it" and "re-sent something already stored" are never silently the same
# outcome.
_NO_DOWNSTREAM_CHECK = (
    "  NOTE: this tool does NOT know whether these clicks are already stored "
    "downstream —\n"
    "  an edge node has no ClickHouse client by design. To find out: "
    "`export --out FILE` here,\n"
    "  then on the central host "
    "`python3 scripts/replay_poison_park.py check --in FILE`."
)


async def cmd_list(r, args) -> int:
    reasons: Counter = Counter()
    total = 0
    per_file: list[tuple[str, int]] = []
    claimed_files = 0
    for path in _park_files():
        recs = _read_records(path)
        per_file.append((path.name, len(recs)))
        if _CLAIM_INFIX in path.name:
            claimed_files += 1
        total += len(recs)
        for rec in recs:
            reasons[rec.get("last_rejection_reason", "?")] += 1

    print(f"{_park_dir()}: {total} parked click(s) in {len(per_file)} file(s)")
    for name, n in per_file:
        tag = "  [CLAIMED — a previous replay run did not finish]" \
            if _CLAIM_INFIX in name else ""
        print(f"  {n:>7}  {name}{tag}")
    if total:
        print("\nby reason:")
        for reason, n in reasons.most_common():
            flag = "  [NOT blindly replayable — collector's verdict on the " \
                   "payload]" if _is_data_verdict(reason) else ""
            print(f"  {n:>7}  {reason}{flag}")
    if claimed_files:
        print(f"\nNOTE: {claimed_files} claimed snapshot(s) above are safe to "
              f"replay — this tool re-adopts them automatically. They are NOT "
              f"counted by /health's per-worker park depth.")
    if total:
        print()
        print(_NO_DOWNSTREAM_CHECK)
    return 0


async def cmd_export(r, args) -> int:
    out = open(args.out, "w") if args.out else sys.stdout
    n = 0
    try:
        for path in _park_files():
            for rec in _read_records(path):
                if args.reason and rec.get("last_rejection_reason") != args.reason:
                    continue
                out.write(json.dumps({"park_file": path.name, **rec}) + "\n")
                n += 1
    finally:
        if args.out:
            out.close()
    print(f"exported {n} entry(ies)"
          + (f" to {args.out}" if args.out else ""), file=sys.stderr)
    return 0


async def cmd_replay(r, args) -> int:
    if not args.reason:
        raise SystemExit(
            "REFUSING: --reason is required. Replaying the whole park blindly "
            "mixes classes that need different handling — run `list` first."
        )
    if _is_data_verdict(args.reason) and not args.i_have_fixed_the_payload:
        raise SystemExit(
            f"REFUSING: '{args.reason}' is the COLLECTOR's verdict on the "
            f"payload, not a fault on our side — replaying gets the identical "
            f"rejection and re-parks the click, a loop that only grows the "
            f"park. Export them, repair them, and POST to /api/clicks/batch. "
            f"Pass --i-have-fixed-the-payload to override."
        )
    if args.apply:
        await _assert_safe_to_write(r)

    replayed = skipped = failed = 0
    for path in _park_files():
        records = _read_records(path)
        if not any(rec.get("last_rejection_reason") == args.reason
                   for rec in records):
            continue

        if not args.apply:
            replayed += sum(1 for rec in records
                            if rec.get("last_rejection_reason") == args.reason
                            and rec.get("data"))
            skipped += sum(1 for rec in records
                           if rec.get("last_rejection_reason") == args.reason
                           and not rec.get("data"))
            continue

        # Freeze the file before reading it for real — see `_claim`.
        claimed = _claim(path)
        records = _read_records(claimed)
        remainder: list[dict] = []
        for rec in records:
            if rec.get("last_rejection_reason") != args.reason:
                remainder.append(rec)
                continue
            if args.limit and replayed >= args.limit:
                remainder.append(rec)
                continue
            data = rec.get("data")
            if not data:
                skipped += 1
                remainder.append(rec)  # keep it: we cannot replay what is not there
                continue
            try:
                # 1. put the copy on stream:clicks FIRST ...
                await r.xadd(STREAM_KEY, {"data": data})
            except Exception as exc:  # noqa: BLE001
                failed += 1
                remainder.append(rec)
                print(f"  XADD failed for click_id={rec.get('click_id')}: "
                      f"{exc} — LEFT in the park", file=sys.stderr)
                continue
            # 2. ... and only then let it leave the park.
            replayed += 1
        _rewrite_remainder(claimed, remainder)

    verb = "would replay" if not args.apply else "replayed"
    print(f"{verb} {replayed}, skipped {skipped} (no payload), failed {failed}")
    if replayed:
        print(_NO_DOWNSTREAM_CHECK)
    if not args.apply:
        print("DRY RUN — nothing was written or deleted. Re-run with --apply.")
    return 1 if failed else 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("list", help="count parked clicks by reason and by file")

    pe = sub.add_parser("export", help="dump parked clicks as JSONL")
    pe.add_argument("--out", help="file to write (default: stdout)")
    pe.add_argument("--reason", help="only this reason")

    pr = sub.add_parser("replay", help="re-inject parked clicks (dry-run by default)")
    pr.add_argument("--reason", required=False, help="REQUIRED — see `list`")
    pr.add_argument("--limit", type=int, default=0, help="cap the number replayed")
    pr.add_argument("--apply", action="store_true", help="actually write + remove")
    pr.add_argument("--i-have-fixed-the-payload", action="store_true",
                    help="override the data-verdict refusal")

    args = p.parse_args()

    async def run() -> int:
        r = aioredis.from_url(settings.redis_url, decode_responses=True)
        try:
            return await {"list": cmd_list, "export": cmd_export,
                          "replay": cmd_replay}[args.cmd](r, args)
        finally:
            await r.aclose()

    return asyncio.run(run())


if __name__ == "__main__":
    sys.exit(main())
