"""Regression fence — X-TDS-Key auth is timing-safe + single-path.

Click-processor exposes 3 endpoints that authenticate via the shared
X-TDS-Key header (`POST /decide`, `POST /admin/sync`,
`POST /admin/seed`), all through the single shared helper
`_check_tds_key`.

**F.25 (2026-05-16):** `_check_tds_key` no longer does ANY string
comparison of `settings.tds_secret_key` — the legacy global-secret
fallback was removed (every Worker carries a per-Worker secret,
verified deterministically). X-TDS-Key auth is now a one-way sha256
digest → Redis `worker_secret_hash:{hex}` lookup, which is strictly
MORE timing-safe than a string compare (a one-way digest lookup
leaks nothing about the secret). The `hmac.compare_digest` timing
discipline (rule `sync-protocol` "hmac.compare_digest for auth")
still governs the X-TDS-Body-Sig verifier + `sync/router` channel
auth — distinct paths, out of scope for this helper.

Background — T1.13 (G-30 closure 2026-05-08): `/admin/seed`
historically diverged using `x_tds_key != settings.tds_secret_key`.
The `==`/`!=` ban below is the permanent fence so a naive
direct-equality compare against the secret can never land silently.

Strategy: source-grep regression at the file level. The tests:

  1. Forbid the `==` / `!=` patterns against `tds_secret_key`.
  2. Pin ≥3 `_check_tds_key` call sites (one per protected endpoint).
  3. Pin the helper does NOT compare the global secret (single-path).
  4. Pin the helper fails closed (no legacy sentinel `return 0`).

Pure file-read — no import side effects, no Redis/FastAPI startup.

If a fourth protected endpoint ships, test #2 fails loud until the
count is bumped AND the endpoint adopts `_check_tds_key`.
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


def _strip_first_docstring(fn_src: str) -> str:
    """Drop the function's leading docstring so source-pins inspect
    CODE only — a docstring legitimately *describes* removed patterns
    (e.g. explains why `settings.tds_secret_key` is no longer
    consulted), which must not trip a `not in` code assertion.
    """
    parts = fn_src.split('"""')
    if len(parts) >= 3:
        # parts[0] = signature line, parts[1] = docstring body,
        # parts[2:] = the actual code after the closing triple-quote.
        return parts[0] + '"""'.join(parts[2:])
    return fn_src


def _check_tds_key_body(source: str) -> str:
    """Slice the `_check_tds_key` helper body (header → next top-level
    def / @app), docstring removed."""
    assert "async def _check_tds_key(" in source, (
        "`_check_tds_key` helper is missing from main.py. The single "
        "canonical auth helper must exist to prevent per-endpoint "
        "drift."
    )
    start = source.index("async def _check_tds_key(")
    rest = source[start:]
    end_markers = ["\n@app.", "\ndef ", "\nasync def "]
    end_offsets = [rest.find(m, 1) for m in end_markers if rest.find(m, 1) > 0]
    sliced = rest[: min(end_offsets)] if end_offsets else rest
    return _strip_first_docstring(sliced)


def test_check_tds_key_helper_does_not_compare_the_global_secret():
    """F.25 (2026-05-16): `_check_tds_key` no longer does ANY string
    comparison of `settings.tds_secret_key`. The legacy global-secret
    fallback was removed; auth is a one-way sha256 digest → Redis
    `worker_secret_hash:{hex}` lookup. That is strictly more
    timing-safe than the prior `hmac.compare_digest` branch (a
    one-way digest lookup leaks nothing about the secret).

    Pin: the helper body MUST NOT reference `settings.tds_secret_key`
    at all (no fallback compare can be reintroduced silently), and
    MUST still perform the `worker_secret_hash:` lookup. A future
    refactor that re-adds a global-secret compare here regresses the
    F.25 single-path contract AND re-opens the dual-window.
    """
    body = _check_tds_key_body(_read_main_source())
    assert "settings.tds_secret_key" not in body, (
        "`_check_tds_key` must NOT consult `settings.tds_secret_key` "
        "post-F.25 — the legacy global-secret fallback was removed. "
        "Re-introducing it re-opens the dual-window AND adds a "
        "string-compare timing surface. The global secret remains the "
        "SYNC-channel credential only (see rule `outbound-http-safety`)."
    )
    assert "worker_secret_hash:" in body, (
        "`_check_tds_key` must perform the per-Worker "
        "`worker_secret_hash:{hex}` Redis lookup — the sole auth "
        "path post-F.25."
    )


def test_helper_fails_closed_single_path():
    """Post-F.25 fail-closed contract: an empty `x_tds_key` is
    rejected at step 1 (`raise HTTPException`); a per-Worker miss /
    corrupted index / Redis error all fall through to a final
    `raise HTTPException(status_code=403`. `settings.tds_secret_key`
    is never consulted, so an empty/misconfigured global secret can
    no longer auto-authenticate any Worker here — strictly more
    fail-closed than the pre-F.25 H6 invert."""
    body = _check_tds_key_body(_read_main_source())
    assert body.count("raise HTTPException(status_code=403") >= 2, (
        "Helper must raise 403 on empty header (step 1) AND on the "
        "terminal per-Worker miss / Redis-error path — fail closed."
    )
    assert "provided" in body, (
        "`_check_tds_key` should still bind a `provided` local for "
        "the empty-header guard. If renaming, update this test."
    )
    # Negative pin: the removed legacy branch + sentinel must NOT
    # silently return — no `return 0` (the old legacy sentinel).
    assert "return 0" not in body, (
        "Found `return 0` — the F.24 legacy sentinel. F.25 removed "
        "the global-secret fallback; a per-Worker hit returns the "
        "real worker_id, everything else raises 403. A `return 0` "
        "means the legacy branch was reintroduced."
    )
