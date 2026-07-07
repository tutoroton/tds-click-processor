"""B4 doc-rot guard (LOSSFIX P3, 2026-07-07 — companion to the C5 config
discipline invariant).

``click_dedup_ttl_seconds`` shrunk 86400s (24h) -> 600s (10min). This test
fails loudly if a FUTURE edit reintroduces a hardcoded assumption that it
is still 86400 anywhere in this service — a comment, docstring, or test
literal that nobody updated when the default changed. A genuinely new
"86400" hit that is unrelated to click-dedup (e.g. some other setting
that also happens to default to 24h) is not itself a bug — but it must
fall within the allowlist below with a stated reason, not silently slip
through, so a real drift can never hide behind an unreviewed exception.
"""

from __future__ import annotations

import re
from pathlib import Path

# Markers that make a "86400" hit KNOWN-OK when found in a window around
# it (not necessarily the exact same physical line — a multi-line comment
# or an implicitly-concatenated string literal can split the historical
# marker from the number). Each documents WHY it's not a live assumption.
_UNRELATED_SETTING_MARKERS = (
    # A DIFFERENT setting (shipper retry-counter TTL) that coincidentally
    # also defaults to 24h — nothing to do with click dedup.
    "shipper_retry_ttl_seconds",
    'expire.assert_called_once_with("click:retry:x", 86400)',
)
# The deliberate historical marker used everywhere this phase intentionally
# references the OLD default (config.py comments, the one-shot deprecation
# log in main.py, and this file's own docstrings) — never a live assumption.
_HISTORICAL_MARKER = "LOSSFIX P3"

_WINDOW_CHARS = 200


def _service_root() -> Path:
    # services/click-processor/tests/unit/this_file.py -> services/click-processor
    return Path(__file__).resolve().parent.parent.parent


def _find_unallowlisted_hits(text: str, rel_path: str) -> list[str]:
    hits = []
    for m in re.finditer("86400", text):
        start, end = m.start(), m.end()
        window = text[max(0, start - _WINDOW_CHARS): end + _WINDOW_CHARS]
        if _HISTORICAL_MARKER in window:
            continue
        if any(marker in window for marker in _UNRELATED_SETTING_MARKERS):
            continue
        lineno = text.count("\n", 0, start) + 1
        line = text.splitlines()[lineno - 1].strip()
        hits.append(f"{rel_path}:{lineno}: {line}")
    return hits


_EXCLUDED_DIR_NAMES = frozenset({
    "__pycache__", ".venv", ".pytest_cache", ".ruff_cache", ".git",
})


def test_no_hardcoded_86400_dedup_assumption():
    root = _service_root()
    hits: list[str] = []
    for path in sorted(root.rglob("*.py")):
        if _EXCLUDED_DIR_NAMES & set(path.parts):
            continue
        if path.name == Path(__file__).name:
            continue  # this guard's own prose talks ABOUT "86400" freely
        text = path.read_text()
        if "86400" not in text:
            continue
        hits.extend(_find_unallowlisted_hits(text, str(path.relative_to(root))))

    assert hits == [], (
        "Found '86400' outside the allowlist — click_dedup_ttl_seconds' "
        "default is 600s as of LOSSFIX P3 (2026-07-07, was 86400s/24h). "
        "Update the stale reference (or tag it with 'LOSSFIX P3' if it's "
        "a deliberate historical mention, or add its marker to "
        "_UNRELATED_SETTING_MARKERS if it's genuinely a different "
        "setting):\n" + "\n".join(hits)
    )
