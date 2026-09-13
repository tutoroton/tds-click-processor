"""G4.3 — a wall tile's answer cannot be poisoned by a click parameter.

🔴 THE BOX SAYS "tile URL construction", AND THE FIRST FINDING IS THAT THERE IS
NO TILE URL. Read out of the code 2026-09-13:

  * `WallTile` (models.py) publishes exactly five fields — `offer_id`,
    `offer_target_id`, `offer_name`, `offer_icon_url`, `route_code` — and its
    docstring puts `url_template`, `payout_value`, `criteria`, the partner and
    the offer's `settings` on an explicit FORBIDDEN list;
  * the worker filters again, per tile, through `WALL_TILE_PUBLIC_FIELDS`;
  * a tile's routing claim is a SIGNED code: `_mint_tile_codes` calls
    `route_code.sign(company_id, campaign_id, offer_id, offer_target_id,
    ttl_seconds, origin_flow_id)` — six bound values, none of them from the
    request;
  * a tile's contents come from the wall's `action_config` and the target
    record, both SYNCED operator config, never from `req.query_params`;
  * when the code is later honoured, the destination URL is built by the
    ORDINARY path (`macros.build_url`), whose template was validated at write
    time by admin-api's `validate_url_template` — the Vector A2 defence
    (`action_executor.py`). The wall adds no URL-construction surface of its
    own; it pins a target the ordinary path then serves.

So the honest claim is structural: there is nothing URL-shaped in the answer to
poison, and no published tile field derives from the click.

🔴 WHAT THIS FILE IS FOR is the part that was NOT mechanised. Two files in two
LANGUAGES have to agree on the tile allowlist, and that agreement existed only
as prose in both of them. A comment saying "keep in sync" is not a mechanism;
`git-workflow`'s own rule is that if two places must agree, the agreement has to
be checkable. A field added to `WallTile` and forgotten in the worker is a
silent widening of a boundary the programme calls a security boundary.

⚠️ NOT COVERED HERE, AND NAMED SO IT IS NOT MISTAKEN FOR COVERED:
`PREVIEW_PUBLIC_FIELDS` and `PreviewResponse` are the SAME boundary one surface
over, with the same forbidden list, and they have no pin of their own — measured
2026-09-13, `PREVIEW_PUBLIC_FIELDS` appears only in `index.js` and in no test.
That belongs to the route-preview programme, not to G4.3, so it is recorded
rather than fixed in passing.

🔴 AND A NEAR-MISS FROM WRITING THIS FILE, because it is the better lesson.
Calibrating it meant adding `url_template` to `WallTile` — and the pattern used
matched the identical line in `PreviewResponse` too. Reverting the wall one left
the PREVIEW model carrying a `url_template` field: the calibration of a security
test had silently created, on the neighbouring visitor-facing surface, the exact
defect the test exists to prevent. `git diff` caught it; nothing else would
have. Bound the walk, and diff before believing a revert.
"""

from __future__ import annotations

import ast
import inspect
import re
from pathlib import Path

from app import main as cp_main
from app.models import WallTile

_SERVICES = Path(__file__).resolve().parents[3]
_WORKER = _SERVICES / "worker" / "src" / "index.js"

#: The five, named here so ADDING one is a deliberate edit to a security test
#: rather than a field that appears on a visitor-facing surface by omission.
EXPECTED = {"offer_id", "offer_target_id", "offer_name", "offer_icon_url",
            "route_code"}

#: Named in `WallTile`'s own docstring as the things that must never reach a
#: visitor. `url_template` is the one this box is about.
FORBIDDEN = {"url_template", "payout_value", "criteria", "partner", "settings",
             "url", "payout", "target_url"}


def _worker_tile_allowlist() -> set[str]:
    """Read `WALL_TILE_PUBLIC_FIELDS` out of the worker bundle."""
    src = _WORKER.read_text()
    m = re.search(r"const WALL_TILE_PUBLIC_FIELDS\s*=\s*\[(.*?)\]", src, re.S)
    assert m, "WALL_TILE_PUBLIC_FIELDS is no longer declared in the worker"
    return set(re.findall(r"'([^']+)'", m.group(1)))


class TestThereIsNoUrlInTheAnswer:
    def test_the_tile_model_carries_exactly_the_five_public_fields(self):
        assert set(WallTile.model_fields) == EXPECTED

    def test_no_forbidden_field_is_on_the_tile(self):
        """`url_template` above all: the advertiser's real tracking link never
        reaches the visitor, so there is no constructed URL to tamper with."""
        leaked = FORBIDDEN & set(WallTile.model_fields)
        assert not leaked, f"a wall tile would publish {sorted(leaked)}"


class TestTheTwoLanguagesAgree:
    """The agreement that was prose in both files until this test existed.

    🔴 THREE ASSERTIONS, NOT ONE, AND THE FIRST DRAFT HAD THE WRONG TWO. It
    carried a single test named `..._matches_the_model` which actually compared
    the worker to the hard-coded `EXPECTED` — a name claiming a cross-file
    agreement over a mechanism that checked a constant. Calibration exposed it:
    widening BOTH sides together was predicted to keep that assertion green, and
    it went red instead, because it was never comparing the two sides at all.

    A name is what an author wrote about the thing. So the constant comparison
    keeps its own honest name, the real model-to-worker agreement is its own
    test, and the forbidden-set check is a third — each able to fail alone.
    """

    def test_the_worker_allowlist_matches_the_EXPECTED_set(self):
        """A third anchor: both sides are pinned to a list that lives in this
        security test, so widening them together still lands here."""
        assert _worker_tile_allowlist() == EXPECTED

    def test_the_MODEL_and_the_WORKER_agree_with_each_other(self):
        """The genuine cross-file, cross-language agreement. A field added to
        `WallTile` and forgotten in the worker (or the reverse) is a boundary
        that drifted, and neither file can see the other."""
        assert set(WallTile.model_fields) == _worker_tile_allowlist()

    def test_the_worker_allowlist_publishes_nothing_forbidden(self):
        """Independent of both comparisons above: this one holds even if every
        anchor is widened in step, because it names the fields themselves."""
        leaked = FORBIDDEN & _worker_tile_allowlist()
        assert not leaked, f"the worker would publish {sorted(leaked)}"


class TestTheSignedClaimBindsIdsOnly:
    def test_the_minter_takes_no_request_and_no_params(self):
        """A function that cannot see the query string cannot bind it into a
        code. This is the cheapest possible proof and the most durable one."""
        params = set(inspect.signature(cp_main._mint_tile_codes).parameters)
        assert params == {"tiles", "company_id", "campaign_id", "wall_id"}
        for forbidden in ("req", "request", "query_params", "params", "query"):
            assert forbidden not in params

    def test_the_signature_binds_the_six_values_and_no_others(self):
        """Read from the AST of the call, not from the docstring above it."""
        src = inspect.getsource(cp_main._mint_tile_codes)
        tree = ast.parse(src.lstrip())
        calls = [n for n in ast.walk(tree)
                 if isinstance(n, ast.Call)
                 and isinstance(n.func, ast.Attribute)
                 and n.func.attr == "sign"]
        assert len(calls) == 1, "expected exactly one route_code.sign call"
        bound = {kw.arg for kw in calls[0].keywords}
        assert bound == {"company_id", "campaign_id", "offer_id",
                         "offer_target_id", "ttl_seconds", "origin_flow_id"}

    def test_the_tenant_is_a_PRECONDITION_of_minting(self):
        """An unbindable code is the defect; minting nothing is the feature not
        applying. The guard must refuse without a company, not sign anyway."""
        src = inspect.getsource(cp_main._mint_tile_codes)
        assert "if company_id is None or not wall_id:" in src
        i_guard = src.index("if company_id is None or not wall_id:")
        i_sign = src.index("route_code.sign(")
        assert i_guard < i_sign, "the tenant guard must run BEFORE any signing"
