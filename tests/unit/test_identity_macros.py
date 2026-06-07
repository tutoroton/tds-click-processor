"""FIX-LD-F1 (2026-06-07) — returning-user URL macros resolve in templates.

Audit-2 MED LD-F1 (docs/development/audit-findings/2/raw-LIVE-D-v2.md): the
macros `{uid}`, `{is_returning}`, `{is_roaming}`, `{is_unique}` silently
collapsed in redirect / fallback URL templates because `build_url` never put
them in its macro-values dict (they are NOT in `CANONICAL_SLOTS`, so the slot
resolver never carried them either). Operators authoring offer_target / flow
templates that USE these macros got a literal-stripped URL.

Fix: `build_url` gained an `identity` layer (system-fixed, like `{click_id}`)
fed from the per-click attribution via `router._identity_macros`. These tests
pin the resolution contract through the single substitution chokepoint
(`build_url`) AND through the `build_url_fn` injection seam that the
action-executor delivery paths use (the partial-bound form `router` threads).

Boolean rendering: `macros._coerce_value` renders `bool` → `'true'` / `'false'`
(lowercase) — the SAME casing the cascade returning-criterion palette emits for
`is_returning` / `is_roaming` (see `router._try_flow_cascade` click_attrs), so a
template flag and a routing criterion agree.
"""

from __future__ import annotations

import functools
import inspect

from app.action_executor import _execute_redirect
from app.models import ClickRequest
from app.router import _identity_macros, build_url


def _req(**overrides) -> ClickRequest:
    """Sensible-defaults ClickRequest — only override what a test cares about."""
    defaults = dict(
        click_id="click-abc",
        country="US",
        city="New York",
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        accept_language="uk-UA,uk;q=0.9",
        query_params={"source": "fb"},
    )
    defaults.update(overrides)
    return ClickRequest(**defaults)


# A realistic resolved attribution for a RETURNING visitor (seen before, same
# campaign), with a canonical hex uid. is_unique=False (not their first click).
_RETURNING = {
    "uid": "abc123def456",
    "is_unique": False,
    "is_returning": True,
    "is_roaming": False,
}

# A first-ever visitor under a live resolver: a uid was minted, is_unique=True,
# but they are neither returning nor roaming yet.
_FIRST_VISIT = {
    "uid": "f00dcafe9001",
    "is_unique": True,
    "is_returning": False,
    "is_roaming": False,
}


# ---------------------------------------------------------------------------
# Resolution against a real attribution
# ---------------------------------------------------------------------------


class TestIdentityMacrosResolve:
    """The four returning-user macros substitute the resolved values, with NO
    literal `{macro}` left behind."""

    def test_all_four_macros_in_one_redirect_template(self):
        url = build_url(
            "https://x.example/?uid={uid}&ret={is_returning}"
            "&roam={is_roaming}&uniq={is_unique}",
            _req(),
            "1", "101",
            identity=_identity_macros(_RETURNING),
        )
        for needed in (
            "uid=abc123def456",  # hex uid passes through encoder unchanged
            "ret=true",          # is_returning True  → 'true'
            "roam=false",        # is_roaming  False  → 'false'
            "uniq=false",        # is_unique   False  → 'false'
        ):
            assert needed in url, f"Missing {needed!r} in {url!r}"
        # The whole point of LD-F1: zero leftover placeholders.
        for leftover in ("{uid}", "{is_returning}", "{is_roaming}", "{is_unique}"):
            assert leftover not in url

    def test_first_visit_flags_render(self):
        url = build_url(
            "https://x.example/?uid={uid}&uniq={is_unique}&ret={is_returning}",
            _req(),
            "1", "101",
            identity=_identity_macros(_FIRST_VISIT),
        )
        assert "uid=f00dcafe9001" in url
        assert "uniq=true" in url   # is_unique True → 'true'
        assert "ret=false" in url   # is_returning False → 'false'

    def test_macros_in_path_position(self):
        """Identity macros work in a path segment too (not only query)."""
        url = build_url(
            "https://x.example/{uid}/landing",
            _req(),
            "1", "101",
            identity=_identity_macros(_RETURNING),
        )
        assert url == "https://x.example/abc123def456/landing"


# ---------------------------------------------------------------------------
# DARK / anonymous — sensible defaults, never a crash, never a leftover macro
# ---------------------------------------------------------------------------


class TestIdentityMacrosDark:
    """Resolver OFF / anonymous / fail-open → no identity in attribution. The
    invariant: `{uid}` collapses to empty, the three flags render the sensible
    `false` default, the URL builds without error, and no literal `{macro}`
    survives."""

    def test_no_identity_arg_collapses_uid_renders_false_flags(self):
        # identity omitted entirely (callers without identity context).
        url = build_url(
            "https://x.example/?uid={uid}&ret={is_returning}"
            "&roam={is_roaming}&uniq={is_unique}&keep=1",
            _req(),
            "1", "101",
        )
        # uid empty → its query pair dropped by safe_substitute cleanup.
        assert "uid=" not in url
        # Flags default to 'false' (sensible default — NOT collapsed).
        assert "ret=false" in url
        assert "roam=false" in url
        assert "uniq=false" in url
        # The unrelated param is untouched, and nothing leaked.
        assert "keep=1" in url
        for leftover in ("{uid}", "{is_returning}", "{is_roaming}", "{is_unique}"):
            assert leftover not in url

    def test_empty_attribution_via_helper(self):
        """A DARK click's attribution carries NO uid/flag keys — the helper
        projects empty uid + all-False flags."""
        macros = _identity_macros({})
        assert macros == {
            "uid": None,
            "is_unique": False,
            "is_returning": False,
            "is_roaming": False,
        }
        url = build_url(
            "https://x.example/?uid={uid}&ret={is_returning}",
            _req(),
            "1", "101",
            identity=macros,
        )
        assert "uid=" not in url
        assert "ret=false" in url

    def test_empty_string_uid_collapses(self):
        """Resolver ran but produced an empty uid ('' — anon, no signal) →
        `{uid}` collapses, flags still render."""
        url = build_url(
            "https://x.example/?uid={uid}&uniq={is_unique}&keep=1",
            _req(),
            "1", "101",
            identity=_identity_macros(
                {"uid": "", "is_unique": False, "is_returning": False,
                 "is_roaming": False}
            ),
        )
        assert "uid=" not in url
        assert "uniq=false" in url
        assert "keep=1" in url

    def test_dark_template_with_only_uid_is_not_an_error(self):
        """A template whose only macro is {uid} on a dark campaign → empty,
        not an exception, not a leftover."""
        url = build_url(
            "https://x.example/path?uid={uid}",
            _req(),
            "1", "101",
        )
        # Empty query param dropped → bare path URL, no '{uid}'.
        assert url == "https://x.example/path"


# ---------------------------------------------------------------------------
# Encoding + non-regression
# ---------------------------------------------------------------------------


class TestIdentityMacrosEncodingAndRegression:
    def test_uid_url_encoded_via_same_mechanism(self):
        """uid is hex (safe), but a defensive check: any non-alnum char in a
        uid would be percent-encoded by the shared `quote(..., safe='')` path,
        exactly like every other macro value — no special-casing."""
        url = build_url(
            "https://x.example/?uid={uid}",
            _req(),
            "1", "101",
            # Hypothetical non-hex uid to prove the encoder runs on it.
            identity={"uid": "a/b c", "is_unique": False,
                      "is_returning": False, "is_roaming": False},
        )
        # '/' → %2F, ' ' → %20 — encoded, never raw in the query.
        assert "uid=a%2Fb%20c" in url

    def test_pre_existing_macros_unbroken_alongside_identity(self):
        """Identity layer is additive — the technical / worker / slot macros
        still resolve when an identity is also supplied."""
        url = build_url(
            "https://x.example/?cid={click_id}&camp={campaign_id}"
            "&country={country}&src={source}&uid={uid}&ret={is_returning}",
            _req(),
            "1", "101",
            identity=_identity_macros(_RETURNING),
        )
        for needed in (
            "cid=click-abc",
            "camp=1",
            "country=US",
            "src=fb",
            "uid=abc123def456",
            "ret=true",
        ):
            assert needed in url, f"Missing {needed!r} in {url!r}"


# ---------------------------------------------------------------------------
# Threading seam — the action-executor delivery path
# ---------------------------------------------------------------------------


class TestIdentityMacrosThroughBuildUrlFn:
    """The router threads identity onto `build_url` via a `functools.partial`
    used as `build_url_fn` for the action-executor (and the sticky pin). Prove a
    redirect action's URL resolves the identity macros through that seam — the
    same shape `_resolve_action_with_sticky` constructs."""

    def test_redirect_action_resolves_identity_via_partial(self):
        build_url_fn = functools.partial(
            build_url, identity=_identity_macros(_RETURNING)
        )
        result = _execute_redirect(
            {"url": "https://land.example/?uid={uid}&ret={is_returning}"},
            _req(),
            "1",
            build_url_fn,
            None,   # source_mappings
            None,   # campaign_mappings
            "7",    # flow_id
        )
        assert result is not None
        url = result["url"]
        assert "uid=abc123def456" in url
        assert "ret=true" in url
        assert "{uid}" not in url and "{is_returning}" not in url

    def test_redirect_action_dark_via_partial(self):
        """build_url_fn bound with identity=None (DARK) → uid empty, flag
        'false', flow_id still resolves — proving the partial default path."""
        build_url_fn = functools.partial(build_url, identity=None)
        result = _execute_redirect(
            {"url": "https://land.example/?uid={uid}&ret={is_returning}"
                    "&fid={flow_id}&keep=1"},
            _req(),
            "1",
            build_url_fn,
            None,
            None,
            "9",
        )
        url = result["url"]
        assert "uid=" not in url
        assert "ret=false" in url
        assert "fid=9" in url
        assert "keep=1" in url


# ---------------------------------------------------------------------------
# Signature pin
# ---------------------------------------------------------------------------


class TestSignaturePin:
    def test_build_url_accepts_identity_kwarg(self):
        params = inspect.signature(build_url).parameters
        assert "identity" in params, "build_url MUST accept `identity` kwarg (FIX-LD-F1)."
        assert params["identity"].default is None
