"""ADR-0560 — a code-served click KEEPS the flow that won the cascade.

Finding F19. A click whose target came from a signed route code is filed under the
flow that won the cascade, and the offer it serves may be one that flow cannot
serve. Measured on staging over 30 days: 655 of 762 (86.0%) code-served clicks
name an offer their winning flow CAN serve; 107 (14.0%, an UPPER BOUND — a flow
reconfigured inside the window counts as disagreeing with no code involved) do
not. On the money side the same shape: 96 conversion rows sit on route-code
clicks, all filed under flow 796, and 64 of the 96 carry offers that flow cannot
serve.

**The ruling accepted that**, and this file is why it cannot be quietly undone:

    The click keeps `flow_id` — the flow that won the cascade — and gains a
    separate `target_selection_path='route_code'` bucket. The flow genuinely
    won; a code re-pointed the TARGET inside that outcome. Moving the click to
    another flow is not even well-defined: a code names `t : offer_target_id`,
    not a flow. The bucket is a PROVENANCE bucket, not an error bucket — 86% of
    code-served clicks name a target their flow could have picked anyway.

🔴 WHY A PIN EXISTS FOR SOMETHING THE CODE ALREADY DOES RIGHT. The invariant
holds today by CONSTRUCTION — `_route_code_target` simply never assigns a flow.
That is exactly what makes it fragile: nothing announces itself when it changes.
And the wrong move is one line, sitting right there, and it looks like a FIX:
`decoded.origin_flow_id` is in scope, so someone confronted with "14% of clicks
are filed under a flow that cannot serve them" would naturally reach for
`result["flow_id"] = decoded.origin_flow_id`. The data would seem to justify it.
The ruling says it is wrong, and this file is what says so at the moment it is
attempted rather than months later in a revenue report.

Measured 2026-09-17: `grep ADR-0560` across `services/` returned NOTHING, against
a positive control on ADR-0515 which returns three files. The decision was
accepted and entirely unpinned.

SCOPE, stated so nobody over-reads it: this pins the ROUTING plane's write. It
does not prove the read plane presents the bucket, and it says nothing about
whether 14% is the right number — only that the click stays where the cascade
put it.
"""

from __future__ import annotations

import ast
import pathlib

import pytest

_ROUTER = pathlib.Path(__file__).resolve().parents[2] / "app" / "router.py"
_FUNC = "_route_code_target"


def _function_node():
    tree = ast.parse(_ROUTER.read_text())
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == _FUNC:
            return node
    raise AssertionError(
        f"{_FUNC} not found in {_ROUTER.name}. It was renamed or moved — find it "
        f"and repoint this pin; do NOT delete the pin."
    )


def _result_assignments(fn) -> dict:
    """Every `result[<str literal>] = <value>` inside the function, by literal."""
    out = {}
    for node in ast.walk(fn):
        if not isinstance(node, ast.Assign):
            continue
        for tgt in node.targets:
            if (
                isinstance(tgt, ast.Subscript)
                and isinstance(tgt.value, ast.Name)
                and tgt.value.id == "result"
                and isinstance(tgt.slice, ast.Constant)
                and isinstance(tgt.slice.value, str)
            ):
                out[tgt.slice.value] = node.value
    return out


def test_the_pin_can_see_what_it_claims_to_read():
    """🔴 THE CONTROL, and it comes first.

    A parse that found nothing would make every assertion below pass over an
    empty set — a gate structurally unable to go red. This asserts the function
    was found AND that the one assignment we already know about is visible.
    """
    assigned = _result_assignments(_function_node())
    assert assigned, f"{_FUNC} makes no `result[...]` assignment — the pin is blind"
    assert "target_selection_path" in assigned, (
        "the known stamp is not visible to this pin, so its silence means nothing"
    )


def test_the_honoured_click_is_stamped_route_code():
    """The provenance half of the ruling: the bucket exists and is named."""
    node = _result_assignments(_function_node())["target_selection_path"]
    assert isinstance(node, ast.Constant) and node.value == "route_code", (
        "the provenance bucket is no longer the literal 'route_code'; every "
        "analytics query and the ADR-0560 ruling name that exact string"
    )


def test_the_code_path_does_NOT_reassign_the_flow():
    """🔴 THE RULING. The cascade winner keeps the click.

    `flow_id` reaches this function as a PARAMETER — the flow the cascade already
    chose. A code names a target, never a flow, so nothing here may overwrite it.
    """
    assert "flow_id" not in _result_assignments(_function_node()), (
        "the route-code path now writes `result['flow_id']`. ADR-0560 rules that "
        "a code-served click KEEPS the flow that won the cascade: a code names "
        "`t : offer_target_id`, not a flow, so moving the click is not even "
        "well-defined. If the ruling has genuinely changed, supersede ADR-0560 "
        "first and change this pin in the same commit."
    )


def test_the_ORIGIN_flow_never_leaks_into_the_click_fact():
    """🔴 The tempting one-liner, named so it fails the moment it is written.

    `decoded.origin_flow_id` is in scope here for two legitimate uses: the wall
    membership lookup, and `trace['origin_wall_id']` (G8.5 — a separate ORIGIN
    dimension that deliberately does NOT live in `target_selection_path`).
    Feeding it into the click fact instead would silently re-file revenue.
    """
    for key, value in _result_assignments(_function_node()).items():
        names = {n.attr for n in ast.walk(value) if isinstance(n, ast.Attribute)}
        assert "origin_flow_id" not in names, (
            f"`result[{key!r}]` is now derived from `origin_flow_id`. That is the "
            f"exact move ADR-0560 forbids — the origin wall is a SECOND dimension "
            f"on one click fact (G8.2), carried in the routing trace, never folded "
            f"into the attribution."
        )


@pytest.mark.parametrize("forbidden", ["campaign_id", "company_id"])
def test_the_code_path_does_not_reassign_the_other_identity_fields_either(forbidden):
    """A v2 code BINDS the campaign (`route_code.py:111`), so it cannot re-point one.

    Widened beyond the ruling's letter on purpose: the same class of mistake with
    `campaign_id` would cross a tenant-visible boundary rather than merely
    misfile a flow.
    """
    assert forbidden not in _result_assignments(_function_node()), (
        f"the route-code path now writes `result[{forbidden!r}]`; a signed code "
        f"names a target inside an ALREADY-decided campaign, it does not choose one"
    )
