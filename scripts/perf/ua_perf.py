#!/usr/bin/env python3
"""UA-parser performance + regression harness (F.41).

Single committed runner that formalizes the three ad-hoc harnesses used
during the 2026-05-30 investigation (golden value snapshot, cold-start
reproduction, warm-novel latency) so the before/after experiment is
reproducible by anyone, not a one-off.

Why this exists
---------------
The click-processor parses every routed click's User-Agent with
``device_detector`` (pure-Python Matomo port). A **cold** first parse
performs the library's full lazy YAML/regex load and can take hundreds of
milliseconds — enough to cross the CF Worker's 2000ms race deadline
(``services/worker/src/index.js`` ``AbortSignal.timeout(2000)``), so the
first click after every node start/deploy falls back to the worker
``fallback_url`` instead of the offer (finding F1, Sentry
``GEO-TDS-WORKERS-2``). The fix (warm-up at lifespan + larger LRU + env
worker count) does NOT change any parsed value — this harness proves that
(``golden`` + ``compare`` ⇒ byte-identical output) while also
demonstrating the cold→warm first-request latency collapse
(``coldstart``).

Design notes
------------
* Imports the REAL ``app.ua_parser`` so the golden snapshot is exactly
  what the service emits (no reimplementation drift). Run with the
  click-processor dir on ``sys.path`` (this file lives under
  ``services/click-processor/scripts/perf/`` and inserts the service root
  automatically).
* ``coldstart`` spawns a FRESH interpreter per sample so each "cold"
  measurement is genuinely cold (a warm in-process LRU/lazy-load would
  pollute the number). ``--mode nowarm`` measures the status-quo first
  request (cold lazy-load); ``--mode warm`` calls ``warmup()`` first
  (the optimized startup) then times a NOVEL request — i.e. what a real
  first click sees once the node is warmed.
* Pure stdlib (argparse/subprocess/statistics/json). No new deps.

Usage
-----
    python scripts/perf/ua_perf.py golden   --corpus UAS.txt --out results/golden_before.tsv
    python scripts/perf/ua_perf.py compare  results/golden_before.tsv results/golden_after.tsv
    python scripts/perf/ua_perf.py coldstart --runs 7
    python scripts/perf/ua_perf.py warmbench --corpus UAS.txt --n 2000

Results default under ``scripts/perf/results/`` (gitignored). The script
itself is committed and reusable.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
import time

# --- make `app.*` importable regardless of CWD -----------------------------
# This file: services/click-processor/scripts/perf/ua_perf.py
# Service root (has the `app/` package): ../../
_SERVICE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _SERVICE_ROOT not in sys.path:
    sys.path.insert(0, _SERVICE_ROOT)

_RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")

# Ordered output columns — exactly the keys app.ua_parser.parse_ua returns.
# Locked so golden TSVs are stable + diffable across runs.
_FIELDS = [
    "device_type", "device_type_raw", "os", "os_version",
    "browser", "browser_version", "device_brand", "device_model", "is_bot",
]

# Novel UAs used purely for cold-start timing (NOT in the warm-up set, so
# `--mode warm` measures a real cache-miss parse against a warmed library,
# not a trivial LRU hit). Distinct strings per mode to avoid any overlap.
_COLD_PROBE_UA = (
    "Mozilla/5.0 (Linux; Android 15; Pixel 9 Pro XL Build/UQ1A) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.6778.39 Mobile Safari/537.36 UAPERF-COLD"
)
_WARM_PROBE_UA = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/18.1 Mobile/15E148 Safari/604.1 UAPERF-WARM"
)


# ---------------------------------------------------------------------------
# golden — snapshot parse_ua over a corpus
# ---------------------------------------------------------------------------
def cmd_golden(args: argparse.Namespace) -> int:
    from app.ua_parser import parse_ua

    uas = _load_corpus(args.corpus)
    out_path = args.out or os.path.join(_RESULTS_DIR, "golden.tsv")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    n = 0
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("ua\t" + "\t".join(_FIELDS) + "\n")
        for ua in uas:
            r = parse_ua(ua)
            # Tabs/newlines in a UA would corrupt the TSV; sanitize the key
            # column only (values from parse_ua never contain tabs).
            safe_ua = ua.replace("\t", "\\t").replace("\n", "\\n").replace("\r", "")
            row = [str(r[k]) for k in _FIELDS]
            f.write(safe_ua + "\t" + "\t".join(row) + "\n")
            n += 1
    print(f"golden: wrote {n} rows → {out_path}")
    return 0


# ---------------------------------------------------------------------------
# compare — diff two golden TSVs (output-value identity, G3/S1)
# ---------------------------------------------------------------------------
def cmd_compare(args: argparse.Namespace) -> int:
    a = _read_tsv(args.before)
    b = _read_tsv(args.after)

    only_a = a.keys() - b.keys()
    only_b = b.keys() - a.keys()
    common = a.keys() & b.keys()
    mismatches = [(ua, a[ua], b[ua]) for ua in common if a[ua] != b[ua]]

    print(f"compare: {len(common)} shared UAs | only-before={len(only_a)} "
          f"only-after={len(only_b)} | value-mismatches={len(mismatches)}")
    for ua, va, vb in mismatches[:20]:
        print(f"  DIFF {ua[:80]!r}\n    before={va}\n    after ={vb}")
    ok = not mismatches and not only_a and not only_b
    print("RESULT: " + ("IDENTICAL ✓ (G3/S1 PASS)" if ok else "DIFFERENT ✗"))
    return 0 if ok else 1


# ---------------------------------------------------------------------------
# coldstart — fresh-process first-request latency, nowarm vs warm
# ---------------------------------------------------------------------------
def cmd_coldstart(args: argparse.Namespace) -> int:
    results: dict[str, list[float]] = {"nowarm": [], "warm": []}
    for mode in ("nowarm", "warm"):
        for _ in range(args.runs):
            ms = _spawn_cold_child(mode)
            if ms is not None:
                results[mode].append(ms)

    print("\n== COLD-START first-request latency (fresh process each sample) ==")
    print(f"   runs per mode: {args.runs}   (device_detector lazy-load IS the cost)\n")
    summary = {}
    for mode, label in (("nowarm", "NOWARM (status-quo: cold lazy-load on 1st request)"),
                        ("warm", "WARM   (optimized: warmup() at startup, then 1st request)")):
        v = results[mode]
        if not v:
            print(f"   {label}: no samples")
            continue
        s = {"min": min(v), "median": statistics.median(v), "max": max(v), "n": len(v)}
        summary[mode] = s
        print(f"   {label}")
        print(f"       min={s['min']:8.1f}ms  median={s['median']:8.1f}ms  max={s['max']:8.1f}ms")

    if "nowarm" in summary and "warm" in summary:
        nm, wm = summary["nowarm"]["median"], summary["warm"]["median"]
        print(f"\n   Δ median: {nm:.1f}ms → {wm:.1f}ms  ({nm / wm:.0f}× faster first request)")
        print(f"   2000ms worker deadline: NOWARM {'CROSSES' if summary['nowarm']['max'] > 2000 else 'risks'} "
              f"on cold worst-case; WARM max={summary['warm']['max']:.1f}ms ≪ 2000ms")
    if args.out:
        os.makedirs(os.path.dirname(args.out), exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)
        print(f"\n   wrote {args.out}")
    return 0


def _spawn_cold_child(mode: str) -> float | None:
    """Run one cold sample in a brand-new interpreter, return its ms."""
    proc = subprocess.run(
        [sys.executable, os.path.abspath(__file__), "_coldchild", "--mode", mode],
        capture_output=True, text=True,
    )
    out = (proc.stdout or "").strip().splitlines()
    for line in reversed(out):
        if line.startswith("COLDMS="):
            try:
                return float(line.split("=", 1)[1])
            except ValueError:
                return None
    sys.stderr.write(proc.stderr or "")
    return None


def cmd_coldchild(args: argparse.Namespace) -> int:
    """INTERNAL: executed in a fresh process by `coldstart`. Prints COLDMS=<ms>."""
    if args.mode == "warm":
        # Optimized startup: pre-load every lazy module, THEN time a novel
        # (cache-miss) request — exactly what a node's first real click sees.
        from app.ua_parser import parse_ua
        try:
            from app.ua_parser import warmup
        except ImportError:
            sys.stderr.write("warmup() not present (pre-C1 code) — warm mode N/A\n")
            return 2
        warmup()
        t0 = time.perf_counter()
        parse_ua(_WARM_PROBE_UA)
        ms = (time.perf_counter() - t0) * 1000.0
    else:
        # Status quo: the very first parse pays the full cold lazy-load.
        from app.ua_parser import parse_ua
        t0 = time.perf_counter()
        parse_ua(_COLD_PROBE_UA)
        ms = (time.perf_counter() - t0) * 1000.0
    print(f"COLDMS={ms:.3f}")
    return 0


# ---------------------------------------------------------------------------
# warmbench — warm novel-parse latency distribution (G7 no-regression)
# ---------------------------------------------------------------------------
def cmd_warmbench(args: argparse.Namespace) -> int:
    from app.ua_parser import parse_ua

    uas = _load_corpus(args.corpus)[: args.n]
    # Warm the library first so we measure steady-state novel-parse cost,
    # not the one-off cold load. Each corpus UA is unique ⇒ cache-miss path.
    # Prefer the real warmup() (post-C1) — it pre-loads ALL lazy modules, so
    # the measured distribution reflects the actual warmed service. Pre-C1
    # (no warmup) fall back to a single probe (one path only ⇒ tail noise).
    try:
        from app.ua_parser import warmup
        warmup()
    except ImportError:
        parse_ua(_WARM_PROBE_UA)
    samples = []
    for ua in uas:
        t0 = time.perf_counter()
        parse_ua(ua)
        samples.append((time.perf_counter() - t0) * 1000.0)

    # Cache-hit cost: re-parse the same UA (LRU short-circuit).
    hot = uas[0] if uas else _WARM_PROBE_UA
    parse_ua(hot)
    t0 = time.perf_counter()
    parse_ua(hot)
    hit_ms = (time.perf_counter() - t0) * 1000.0

    s = sorted(samples)
    p = lambda q: s[min(len(s) - 1, int(q * len(s)))]  # noqa: E731
    out = {
        "n": len(samples),
        "p50": p(0.50), "p95": p(0.95), "p99": p(0.99),
        "mean": statistics.mean(samples) if samples else 0.0,
        "throughput_ua_s": (1000.0 / statistics.mean(samples)) if samples else 0.0,
        "cache_hit_ms": hit_ms,
    }
    print("\n== WARM novel-parse latency (steady state) ==")
    print(f"   n={out['n']}  p50={out['p50']:.3f}ms  p95={out['p95']:.3f}ms  "
          f"p99={out['p99']:.3f}ms  ~{out['throughput_ua_s']:.0f} UA/s/thread")
    print(f"   cache-hit (LRU short-circuit): {out['cache_hit_ms']:.4f}ms")
    if args.out:
        os.makedirs(os.path.dirname(args.out), exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(out, f, indent=2)
        print(f"   wrote {args.out}")
    return 0


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def _load_corpus(path: str | None) -> list[str]:
    if not path:
        sys.exit("error: --corpus PATH is required for this command")
    with open(path, encoding="utf-8") as f:
        return [ln for ln in f.read().splitlines() if ln.strip()]


def _read_tsv(path: str) -> dict[str, tuple]:
    """Read a golden TSV → {ua: (field values tuple)}; skips header."""
    rows: dict[str, tuple] = {}
    with open(path, encoding="utf-8") as f:
        next(f, None)  # header
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 1 + len(_FIELDS):
                continue
            rows[parts[0]] = tuple(parts[1:1 + len(_FIELDS)])
    return rows


def main() -> int:
    ap = argparse.ArgumentParser(description="UA-parser perf + regression harness (F.41)")
    sub = ap.add_subparsers(dest="cmd", required=True)

    g = sub.add_parser("golden", help="snapshot parse_ua over a corpus → TSV")
    g.add_argument("--corpus", required=True)
    g.add_argument("--out")
    g.set_defaults(func=cmd_golden)

    c = sub.add_parser("compare", help="diff two golden TSVs (output-value identity)")
    c.add_argument("before")
    c.add_argument("after")
    c.set_defaults(func=cmd_compare)

    cs = sub.add_parser("coldstart", help="fresh-process first-request latency: nowarm vs warm")
    cs.add_argument("--runs", type=int, default=5)
    cs.add_argument("--out")
    cs.set_defaults(func=cmd_coldstart)

    wb = sub.add_parser("warmbench", help="warm novel-parse latency distribution")
    wb.add_argument("--corpus", required=True)
    wb.add_argument("--n", type=int, default=2000)
    wb.add_argument("--out")
    wb.set_defaults(func=cmd_warmbench)

    cc = sub.add_parser("_coldchild", help=argparse.SUPPRESS)
    cc.add_argument("--mode", choices=["nowarm", "warm"], required=True)
    cc.set_defaults(func=cmd_coldchild)

    args = ap.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
