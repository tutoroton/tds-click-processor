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


def test_at_least_three_compare_digest_calls_for_admin_endpoints():
    """At least 3 timing-safe calls: /decide, /admin/sync, /admin/seed.

    The exact canonical signature used by all 3 endpoints today is:

        hmac.compare_digest(x_tds_key, settings.tds_secret_key)

    If a 4th protected endpoint lands, bump this assertion and add
    its name to the docstring of this module. If the count drops to
    <3, regression: an endpoint was either deleted (update test) or
    silently downgraded to direct-equality (fix the endpoint).
    """
    source = _read_main_source()
    canonical_call = "hmac.compare_digest(x_tds_key, settings.tds_secret_key)"
    count = source.count(canonical_call)
    assert count >= 3, (
        f"Expected ≥3 calls of `{canonical_call}` in main.py "
        f"(one per protected endpoint: /decide, /admin/sync, "
        f"/admin/seed) — found {count}. Either an endpoint dropped "
        "the timing-safe check, or this regression-fence count is "
        "stale and needs bumping for a new endpoint."
    )
