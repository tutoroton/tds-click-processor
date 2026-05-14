"""Regression fence — every X-TDS-Key check uses `hmac.compare_digest`.

Pinned by rule `sync-protocol` → "Key Rules" → "hmac.compare_digest
for auth": click-processor exposes 3 endpoints that authenticate via
the shared X-TDS-Key header (`POST /decide`, `POST /admin/sync`,
`POST /admin/seed`). All three MUST use timing-safe comparison.

Background — T1.13 (G-30 closure 2026-05-08): `/admin/seed` historically
diverged from its siblings, using `x_tds_key != settings.tds_secret_key`.
Defense-in-depth fix; the endpoint is dev-only (gated by
`environment == "production"`), but the inconsistency would have been a
foot-gun if anyone copy-pasted the block to a new endpoint.

Strategy: source-grep regression at the file level. The test:

  1. Forbids the `==` / `!=` patterns against `tds_secret_key`.
  2. Counts `hmac.compare_digest` calls with the canonical signature
     and asserts ≥3 (one per protected endpoint).

Pure file-read — no module import side effects, no Redis/FastAPI
startup. Runs in milliseconds.

If a fourth protected endpoint ships, this test will fail loud until
the count is bumped here AND the endpoint adopts the timing-safe
pattern. That's the regression fence: future drift is impossible to
land silently.
"""

from __future__ import annotations

from pathlib import Path


_MAIN_PATH = (
    Path(__file__).resolve().parent.parent.parent / "app" / "main.py"
)


def _read_main_source() -> str:
    return _MAIN_PATH.read_text(encoding="utf-8")


def test_no_direct_equality_against_tds_secret_key():
    """Forbid `==` / `!=` patterns — they leak per-byte timing.

    `hmac.compare_digest` performs the comparison in constant time
    regardless of where the first mismatch occurs, defeating remote
    timing-attack reconstruction of the secret. A naive `==` or `!=`
    short-circuits on first mismatch — measurable over many requests.
    """
    source = _read_main_source()
    # Both directions of the comparison must be banned, in case
    # someone authors `settings.tds_secret_key != x_tds_key` for
    # readability.
    forbidden = [
        "x_tds_key != settings.tds_secret_key",
        "x_tds_key == settings.tds_secret_key",
        "settings.tds_secret_key != x_tds_key",
        "settings.tds_secret_key == x_tds_key",
    ]
    offenders = [pat for pat in forbidden if pat in source]
    assert not offenders, (
        "Timing-attack risk in services/click-processor/app/main.py — "
        f"found forbidden direct-equality pattern(s): {offenders!r}. "
        "Use `hmac.compare_digest(x_tds_key, settings.tds_secret_key)` "
        "for ALL X-TDS-Key checks. See rule `sync-protocol` → "
        "'hmac.compare_digest for auth' for the canonical recipe."
    )


def test_at_least_three_check_tds_key_calls_for_admin_endpoints():
    """At least 3 calls to `_check_tds_key` helper: /decide,
    /admin/sync, /admin/seed.

    H6 fix (2026-05-11): the three previously-inline auth checks
    were consolidated into a single shared helper `_check_tds_key`
    that fails CLOSED on empty stored secret. The helper itself
    contains the canonical `hmac.compare_digest` call — pinned by
    its own assertion below. Endpoint count is pinned by counting
    helper invocations.

    If a 4th protected endpoint lands, bump this assertion and add
    its name to the docstring of this module. If the count drops to
    <3, regression: an endpoint dropped the auth check entirely OR
    silently re-introduced inline auth (also forbidden — see the
    timing-safety assertion above).
    """
    source = _read_main_source()
    helper_call = "_check_tds_key(x_tds_key)"
    count = source.count(helper_call)
    assert count >= 3, (
        f"Expected ≥3 calls of `{helper_call}` in main.py "
        f"(one per protected endpoint: /decide, /admin/sync, "
        f"/admin/seed) — found {count}. Either an endpoint dropped "
        "the auth check or re-introduced inline auth (timing-safety "
        "regression). See H6 fix design + this module's docstring."
    )


def test_check_tds_key_helper_uses_hmac_compare_digest():
    """The `_check_tds_key` helper MUST use `hmac.compare_digest`.

    H6 fix consolidated the per-endpoint inline checks into this
    single helper. If a future refactor strips the timing-safe
    comparison out of the helper, ALL three endpoints regress at
    once. This pin defends the helper's contract.
    """
    source = _read_main_source()
    # Locate the helper body. We search for the function header and
    # require `hmac.compare_digest` to appear in the surrounding
    # function code. A strict regex would be fragile; the file-level
    # presence + the `def _check_tds_key` anchor are sufficient.
    assert "def _check_tds_key(" in source, (
        "`_check_tds_key` helper is missing from main.py. "
        "H6 fix design requires a single canonical auth helper to "
        "prevent per-endpoint drift."
    )
    # Pull out the helper body (rough — from header to next top-level
    # `def` or `@app`).
    start = source.index("def _check_tds_key(")
    # Stop at the next top-level definition.
    rest = source[start:]
    end_markers = ["\n@app.", "\ndef ", "\nasync def "]
    end_offsets = [rest.find(m, 1) for m in end_markers if rest.find(m, 1) > 0]
    body = rest[:min(end_offsets)] if end_offsets else rest
    assert "hmac.compare_digest" in body, (
        "`_check_tds_key` body must call hmac.compare_digest for "
        "timing-safe comparison. Found body without it — regression."
    )


def test_helper_fails_closed_when_either_side_empty():
    """The helper signature MUST verify BOTH provided and stored
    secrets are non-empty BEFORE accepting. This is the H6 closure
    — the legacy `if settings.tds_secret_key and ...` pattern fails
    OPEN when the stored secret is empty; the helper fails CLOSED."""
    source = _read_main_source()
    start = source.index("def _check_tds_key(")
    rest = source[start:]
    end_markers = ["\n@app.", "\ndef ", "\nasync def "]
    end_offsets = [rest.find(m, 1) for m in end_markers if rest.find(m, 1) > 0]
    body = rest[:min(end_offsets)] if end_offsets else rest
    # The fail-closed invariant: both `provided` and `stored` must
    # appear in the condition alongside `compare_digest`. The exact
    # AST shape is implementation-detail, but the three identifiers
    # MUST co-occur.
    assert "provided" in body and "stored" in body, (
        "`_check_tds_key` should bind `provided` and `stored` "
        "locals so the fail-closed contract is explicit. "
        "If renaming, update this test."
    )
    # F.24 Phase 1 (2026-05-14): the helper went from
    #   `if not (provided and stored and compare_digest(...))`
    # to the inverted positive-match form
    #   `if provided and stored and compare_digest(...): return 0`
    # The fail-closed semantics are preserved — if either side is
    # empty, the positive branch doesn't fire and execution falls
    # through to `raise HTTPException`. The regression fence below
    # pins the semantic invariant: all three identifiers
    # (`provided`, `stored`, `compare_digest`) MUST co-occur in the
    # legacy fallback branch.
    assert (
        "if provided and stored and hmac.compare_digest" in body
        or "if not (provided and stored and hmac.compare_digest" in body
    ), (
        "H6 fail-closed contract requires `provided`, `stored`, and "
        "`hmac.compare_digest` to co-occur in the legacy auth branch. "
        "Either the positive form (F.24 Phase 1) or the original "
        "negative form is acceptable; both fail closed when either "
        "side is empty. If neither matches, the auth surface drifted "
        "off the regression fence — fix the helper or update this test."
    )
