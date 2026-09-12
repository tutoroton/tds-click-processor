"""J5 · WHICH REQUEST PATHS CAN PRODUCE A COUNTED CLICK — pinned, not described.

§2's J5 box asks for an inventory of every click-producing path and an explicit
confirmation that preview-only produces zero counted clicks. Counting zero
preview clicks in a run would satisfy the letter of that and prove very little:
**zero is also what a broken probe prints.** The stronger claim available here is
structural — the node cannot emit a click from `/wall` or `/preview` because
neither function contains a writer.

Measured 2026-09-12 by walking the AST of `app/main.py`: every writer into the
click stream is inside `decide()`.

    line 1474  xadd                  stream:clicks   in decide()   <- SMOKE-TEST
    line 1965  xadd                  stream:clicks   in decide()   <- real click
    line 2097  enqueue_click_to_disk click_record    in decide()   <- disk fallback

A docstring saying so is not a mechanism; this test is. Add an `xadd` to the wall
body and it goes red naming the function, which is exactly the regression the
inventory exists to catch — a second writer appearing on a path the box believes
is silent.

WHY AN AST WALK AND NOT A RUNTIME ASSERTION. A runtime check can only observe the
paths a test happens to drive, so it answers "this request wrote nothing" — the
weaker claim again. The AST answers "no such code exists on that path", which is
the claim J5 is actually making. It is also why this file imports `ast` and not
`app`: it is a statement about the SOURCE.

🔴 IT DELIBERATELY DOES NOT IMPORT `scripts/`. This service has a PUBLIC mirror
(`git subtree push --prefix=services/click-processor`), so a test here that
reached into the harness would break that mirror for everyone.
"""

from __future__ import annotations

import ast
from pathlib import Path

MAIN = Path(__file__).resolve().parents[2] / "app" / "main.py"

# The names that put a record on its way to the sink. `xadd` covers the Redis
# stream (primary); `enqueue_click_to_disk` covers the durability fallback that
# LOSSFIX P1b added — a fallback is still an emission, and leaving it out would
# make this guard blind to exactly the path that exists for when things break.
WRITER_NAMES = {"xadd", "enqueue_click_to_disk", "enqueue_click"}

# The ONE request handler allowed to produce a click.
EXPECTED_EMITTER = "decide"


def _writer_sites() -> list[tuple[int, str, str]]:
    """-> [(lineno, writer_name, enclosing_function)] for every writer call."""
    tree = ast.parse(MAIN.read_text())
    spans = [
        (n.lineno, n.end_lineno, n.name)
        for n in ast.walk(tree)
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
    ]

    def enclosing(line: int) -> str:
        cands = [s for s in spans if s[0] <= line <= s[1]]
        # innermost wins, so a nested helper is reported as itself rather than
        # as whatever module-level function happens to contain it
        return min(cands, key=lambda s: s[1] - s[0])[2] if cands else "<module>"

    out = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            name = getattr(node.func, "attr", None) or getattr(node.func, "id", None)
            if name in WRITER_NAMES:
                out.append((node.lineno, name, enclosing(node.lineno)))
    return sorted(out)


def test_every_click_writer_in_main_lives_inside_decide():
    sites = _writer_sites()
    assert sites, (
        "no click writer found at all — this guard has become structurally "
        "unable to fail, which is worse than the regression it watches for. "
        "Either the emission moved out of main.py (then move this guard with "
        "it) or WRITER_NAMES has gone stale."
    )
    strays = [s for s in sites if s[2] != EXPECTED_EMITTER]
    assert not strays, (
        "a click writer appeared outside %s(): %s.\n"
        "J5's inventory records that /wall and /preview cannot produce a counted "
        "click BECAUSE no writer exists on those paths. If this is a deliberate "
        "new emission, the inventory in 74-PRODUCTION-READINESS-PROOF-PLAN.md "
        "must be updated in the SAME change — otherwise the reconciliation will "
        "treat its rows as unexplained extras."
        % (EXPECTED_EMITTER, strays)
    )


def _calls_and_writers() -> tuple[dict[str, set[str]], set[str]]:
    """-> (function -> names it calls, functions containing a writer call)."""
    tree = ast.parse(MAIN.read_text())
    calls: dict[str, set[str]] = {}
    writers: set[str] = set()
    for fn in ast.walk(tree):
        if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        named: set[str] = set()
        for node in ast.walk(fn):
            if isinstance(node, ast.Call):
                name = getattr(node.func, "attr", None) or getattr(
                    node.func, "id", None)
                if name:
                    named.add(name)
                    if name in WRITER_NAMES:
                        writers.add(fn.name)
        calls[fn.name] = named
    return calls, writers


# The request paths J5's inventory records as producing NO counted click.
SILENT_ENTRYPOINTS = ("wall", "preview")


def test_no_writer_is_REACHABLE_from_the_wall_or_preview_entrypoints():
    """Reachability, not residence — and the difference is the whole test.

    🔴 THIS ASSERTION WAS WEAKER THAN ITS OWN NAME UNTIL THE CALIBRATOR SAID SO.
    It first checked only whether a writer sat *inside* a function named in a
    hand-written list of handlers. Injecting a writer into a HELPER beside
    `_wall_body` left it green — and a helper is how such a thing would really
    arrive. Nobody inlines an `xadd` in a request handler.

    So it now walks the call graph: from `wall`/`preview`, transitively through
    every locally-defined function they call, and fails if any of them writes.
    That is the property the box actually asserts. It also stops the list of
    "silent" names from being something a reader must remember to update —
    a new helper is followed automatically.

    Kept separate from the decide-only test because the two fail differently: a
    writer added to a THIRD handler trips that one and not this one, while a
    writer reached only through a deep helper chain trips this one even if
    somebody widened EXPECTED_EMITTER.
    """
    calls, writers = _calls_and_writers()

    for entry in SILENT_ENTRYPOINTS:
        assert entry in calls, (
            "entrypoint %r no longer exists in main.py — this guard is now "
            "watching nothing. Repoint it at whatever replaced it." % entry)

    seen: set[str] = set()
    frontier = list(SILENT_ENTRYPOINTS)
    while frontier:
        fn = frontier.pop()
        if fn in seen:
            continue
        seen.add(fn)
        frontier.extend(n for n in calls.get(fn, ()) if n in calls)

    offenders = sorted(seen & writers)
    assert not offenders, (
        "a click writer is REACHABLE from %s: %s.\n"
        "J5's inventory records these paths as producing zero counted clicks. "
        "Either this emission is a defect, or the inventory is now wrong — and "
        "the reconciliation will read its rows as unexplained extras until one "
        "of the two is fixed." % (list(SILENT_ENTRYPOINTS), offenders)
    )


def test_the_smoke_test_bypass_is_counted_in_the_inventory():
    """There are TWO stream writers in `decide`, and the second is easy to miss.

    `main.py:1474` emits a SMOKE-TEST record that bypasses routing entirely. It
    is a real row in `stream:clicks`, so a reconciliation that does not know it
    exists would report it as an unexplained EXTRA — or, worse, let it mask a
    missing one. It is distinguishable only by its `smoke-test-<hex>` click_id
    prefix, and it is emitted by admin-api's `EdgeNodeService._run_smoke_test`,
    which is why the local two-lane stack (no admin-api) never sees one.

    This asserts the COUNT so that a third stream writer cannot be added to
    `decide` without somebody revisiting the inventory.
    """
    stream_writers = [s for s in _writer_sites() if s[1] == "xadd"]
    assert len(stream_writers) == 2, (
        "expected exactly 2 stream writers in %s() — the real click and the "
        "smoke-test bypass — found %d: %s. A new one must be added to J5's "
        "inventory, with a statement of how the reconciliation tells it apart."
        % (EXPECTED_EMITTER, len(stream_writers), stream_writers)
    )
