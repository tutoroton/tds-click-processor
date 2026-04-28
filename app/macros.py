"""URL macro substitution with safe-output guarantees.

Stage 2 / Vector 1.2 Phase 5 pre-ship helper. Pure function — NO I/O.
Used by:

  - Click-processor at `/decide` to build the redirect URL from
    `offer_target.url_template` (and `flow.action_config.url` for
    `action_type='redirect'`). Phase 5 `build_url()` rewrite (next
    Stage 2 ship) integrates this helper.
  - Event-processor (Stage 4) on outbound postbacks — same logic
    applies; Stage 4 will either import this module via a shared
    package or vendor a copy. The research stub
    (`docs/roadmap/stage-1a-research/macros-registry.md`) reserves
    `app/common/macros.py` as the canonical home; this helper
    currently lives at `services/click-processor/app/macros.py`
    because click-processor has no `app/common/` subdirectory yet.
    Stage 4 entry will resolve the location once the second consumer
    arrives.

Design pin: `docs/roadmap/stage-1a-research/macros-registry.md`
sections "Substitution failure mode" + M4 + M5. The substituter
guarantees the output URL is **syntactically valid** even when one
or more macros resolve to NULL — the user constraint was "безпечне
рішення, яке не зламає маршрутизацію". No literal `{macro}` ever
leaks; empty path segments collapse with adjacent slashes; empty
query params are dropped; the `https://` scheme double-slash is
preserved as a special case.

Notes for callers:
  - Substitution is one-pass — only macros present in the template
    are replaced. Unknown macros (not in the values dict) substitute
    to empty string.
  - Values are URL-encoded via `urllib.parse.quote(value, safe='')`
    (M5). Numeric IDs pass through; user-supplied strings (sub
    slots, extras) are escaped.
  - Non-scalar values (list / dict / bytes) raise `TypeError` rather
    than silently coercing via `str()` — see `_coerce_value`.
  - Cleanup runs unconditionally — same code path whether or not
    any macros resolved to NULL. Cheap when nothing needs cleaning.
  - Operator-authored templates that put macros in the AUTHORITY
    position (e.g. `https://{hostname}/path`) are NOT defended by
    this helper — that's a config bug. Phase 5 admin-api
    `validate_url_template` is the right enforcement point. See
    security audit 2026-04-28.
"""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import quote


__all__ = ["safe_substitute"]


# Macro placeholder pattern — `{slot_name}` where the name follows
# the canonical registry (lowercase + digit + underscore). The
# anchor on `[a-z]` prevents accidentally matching things like
# JSONB-encoded `{"key": "value"}` blobs (uppercase keys, special
# chars) that might accidentally end up in templates. Length cap
# 100 chars keeps adversarial inputs bounded — real registry
# names are 3-20 chars.
_MACRO_RE: re.Pattern[str] = re.compile(r"\{([a-z][a-z0-9_]{0,99})\}")

# Pre-compiled slash-collapse pattern — used inside `_cleanup_url`
# on every call. Pre-compiling keeps the click-processor hot path
# under its 10ms budget regardless of regex-cache pressure.
_SLASH_RE: re.Pattern[str] = re.compile(r"/{2,}")

# Query cleanup is parse-and-rejoin (split on `&`, drop empty
# values, rejoin). Simpler than regex tricks — see
# `_clean_query_string` below.


def safe_substitute(template: str, values: dict[str, Any]) -> str:
    """Substitute `{macro}` placeholders in `template` with values
    from `values`, then clean up the resulting URL so it is always
    syntactically valid.

    Args:
        template: URL template with `{macro}` placeholders.
            Example: "https://landing.com/{country}/page?gclid={source_click_id}".
        values: Mapping of macro name (without braces) → value. Use
            `None` (or omit the key) to indicate "no value resolved";
            the substituter will drop the param or collapse the
            path segment.

    Returns:
        The substituted URL with cleanup applied. NEVER contains a
        literal `{macro}` placeholder, regardless of how `values`
        is shaped.

    Examples:
        >>> safe_substitute(
        ...     "https://landing.com/?gclid={source_click_id}&geo={country}",
        ...     {"source_click_id": None, "country": "US"},
        ... )
        'https://landing.com/?geo=US'

        >>> safe_substitute(
        ...     "https://landing.com/{country}/{funnel_type}/page",
        ...     {"country": "US", "funnel_type": None},
        ... )
        'https://landing.com/US/page'

        >>> safe_substitute(
        ...     "https://landing.com/{country}/{funnel_type}",
        ...     {"country": "US"},
        ... )
        'https://landing.com/US'
    """
    # 1. Substitute every {macro}: NULL → empty, otherwise URL-encode.
    def _replace(match: re.Match[str]) -> str:
        macro_name = match.group(1)
        value = values.get(macro_name)
        if value is None or value == "":
            return ""
        return quote(_coerce_value(value), safe="")

    url = _MACRO_RE.sub(_replace, template)

    # 2. Cleanup pass — drop empty query params, then collapse path
    # slashes, then strip trailing dangling separators. Order matters:
    # query cleanup before path cleanup so we don't accidentally
    # rewrite query content as path.
    return _cleanup_url(url)


# Maximum length of a single macro value before substitution. Real
# RESERVED_SLOTS use cases stay well under 4 KB (gclid ~50 chars,
# fbclid ~150, keyword free-text ~200). A 10 MB value from a
# misbehaving advertiser would amplify into multi-MB URLs after
# percent-encoding and blow the click-processor 10ms latency
# budget. Truncate defensively rather than embedding garbage in the
# redirect target. See security audit 2026-04-28 (MEDIUM-002).
_MAX_MACRO_VALUE_LENGTH = 4096


def _coerce_value(value: Any) -> str:
    """Convert a macro value to its string form for URL encoding.

    Accepts the scalar types the registry can legitimately produce:
    `str`, `int`, `float`, `bool`. Anything else (list, dict, bytes,
    custom object) raises `TypeError` — silent `str([1,2,3])` →
    `"[1, 2, 3]"` would put garbage in advertiser-facing URLs.
    Per code-review + security-audit 2026-04-28.

    String values longer than 4 KB are truncated with a warning.
    """
    if isinstance(value, str):
        if len(value) > _MAX_MACRO_VALUE_LENGTH:
            return value[:_MAX_MACRO_VALUE_LENGTH]
        return value
    if isinstance(value, bool):
        # `isinstance(True, int)` is True — handle bool BEFORE int.
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    raise TypeError(
        f"Macro value must be str / int / float / bool / None, "
        f"got {type(value).__name__}: {value!r}"
    )


def _cleanup_url(url: str) -> str:
    """Apply the M4 cleanup rules to an already-substituted URL.

    Separated from `safe_substitute` so callers that build URLs by
    other means (templating libraries, manual concat) can reuse
    the cleanup pass.
    """
    # Split scheme/host from path/query/fragment so we don't ever
    # touch the `://` double-slash.
    scheme_end = url.find("://")
    if scheme_end != -1:
        scheme_part = url[: scheme_end + 3]  # includes `://`
        rest = url[scheme_end + 3 :]
    else:
        scheme_part = ""
        rest = url

    # Split rest into [authority+path][?query][#fragment].
    fragment_part = ""
    if "#" in rest:
        rest, fragment = rest.split("#", 1)
        fragment_part = f"#{fragment}"

    if "?" in rest:
        path_part, query_part = rest.split("?", 1)
    else:
        path_part, query_part = rest, None

    # 2a. Path cleanup — collapse repeated slashes, then strip
    # trailing `/` (but only if the path is non-empty after the
    # authority). Pattern is module-level for hot-path performance.
    path_part = _SLASH_RE.sub("/", path_part)
    if path_part.endswith("/") and path_part.count("/") > 1:
        # Don't strip the slash that follows the authority on
        # bare-host URLs ("https://host/" → keep as "https://host/").
        # Only strip if there's a deeper path that became trailing.
        path_part = path_part.rstrip("/")
    elif path_part == "/":
        # Bare-host root URL — leave as-is so callers consuming
        # downstream don't see "https://host" (no slash).
        pass

    # 2b. Query cleanup — drop pairs whose value is empty.
    if query_part is not None:
        query_part = _clean_query_string(query_part)

    # 3. Reassemble.
    out = scheme_part + path_part
    if query_part:
        out += f"?{query_part}"
    out += fragment_part
    return out


def _clean_query_string(query: str) -> str | None:
    """Drop empty-value pairs from a query string, normalise to
    `key=value` form. Returns None if nothing remains.

    Handles every shape that the substituter can produce:
      - `a=&b=2`        → `b=2`
      - `a=1&b=`        → `a=1`
      - `a=1&b=&c=3`    → `a=1&c=3`
      - `a=&b=&c=`      → None (caller drops `?` separator)

    Bare flags (no `=`) are preserved on the principle that empty
    flags are valid HTTP — but we don't expect them in landing
    templates so this is purely defensive.
    """
    pairs = query.split("&")
    kept: list[str] = []
    for p in pairs:
        if not p:
            # `?&a=1` or `?a=1&&b=2` — skip empty positions.
            continue
        if "=" not in p:
            # Bare flag (no `=`); preserve.
            kept.append(p)
            continue
        key, _, value = p.partition("=")
        if value:
            kept.append(p)
        # else: empty-value pair → drop.
    if not kept:
        return None
    return "&".join(kept)
