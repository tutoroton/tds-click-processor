"""Tests for the click-processor startup central_url guard.

F.29 Sprint 1.1 (2026-05-23). Closes the catastrophic silent-disable
path surfaced by audit 2026-05-16: AU+CA edge nodes had
``TDS_CENTRAL_URL=""`` → shipper silently returned at startup
(services/click-processor/app/shipper.py:34-36 pre-F.29) → 50-day
click-persistence blackout. The shipper accepted clicks into local
``stream:clicks`` (4637 + 271 stockpiled) but never delivered to central.
Central PG ``clicks`` table grew by ONE row in that 50-day window.

The guard mirrors ``_enforce_secret_presence`` (config.py line ~168) —
same shape, same env-tolerance carve-out, same loud-on-fail discipline.
Coverage:

  * Local environments tolerate empty central_url (dev workflow).
  * Non-local environments + require=True REJECT empty central_url.
  * Non-local environments + require=False ACCEPT empty central_url
    (operator escape hatch).
  * Non-local environments ACCEPT a configured central_url.
  * Unknown environment is treated as non-local (fail-closed default).

Reference: F.29 plan §3 G6, plan §7.1 (require=True default decision).
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from app.config import Settings


# Minimum-viable settings for non-local construction: secrets long
# enough to satisfy ``_enforce_secret_presence`` so the OTHER validator
# doesn't pre-empt the central_url guard under test here. Length 32
# matches ``api-security.md`` HS256 floor (same as admin-api guard).
_VALID_SECRET = "x" * 32


# ---------------------------------------------------------------------------
# Local environments tolerate empty / unset central_url (dev mode)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("env", ["local", "development"])
def test_local_env_accepts_empty_central_url(env):
    """Local + development envs are intentionally lenient — the
    ``central_url: str = ""`` default still works in dev so an
    engineer can ``make dev`` without provisioning a central
    collector. Mirrors the same carve-out as ``_enforce_secret_presence``.
    """
    s = Settings(environment=env, central_url="", require_central_url=True)
    assert s.central_url == ""
    assert s.environment == env
    # Pin the default flag value too — drift to False would silently
    # downgrade production safety (the audit-2026-05-16 case must
    # remain fail-closed by default per F.29 plan §7.1).
    assert s.require_central_url is True


@pytest.mark.parametrize("env", ["local", "development"])
def test_local_env_accepts_empty_central_url_with_flag_false(env):
    """Local env + flag=False — both opt-outs combined still pass.
    Sanity-pin so a future flag-default change doesn't break local
    dev for operators who set TDS_REQUIRE_CENTRAL_URL=false in their
    .env.local for some reason."""
    s = Settings(environment=env, central_url="", require_central_url=False)
    assert s.central_url == ""
    assert s.require_central_url is False


# ---------------------------------------------------------------------------
# Non-local environments + require=True enforce the guard
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("env", ["staging", "production"])
def test_non_local_env_rejects_empty_central_url(env):
    """The CRITICAL audit case — without this rejection, the shipper
    silently disabled on AU+CA for 50 days. Pin the rejection at the
    env level with the default flag (require=True).
    """
    with pytest.raises(ValidationError) as exc:
        Settings(
            environment=env,
            tds_secret_key=_VALID_SECRET,
            central_url="",
            require_central_url=True,
        )

    msg = str(exc.value)
    assert "TDS_CENTRAL_URL" in msg
    assert env in msg
    # The error message MUST mention the historical incident so an
    # operator reading the boot log knows WHY the gate exists, not
    # just THAT it failed. Audit date is a unique anchor for the
    # post-incident review.
    assert "2026-05-16" in msg
    # And it must hand the operator the exact rollback flag so they
    # don't waste minutes hunting through source during incident
    # recovery.
    assert "TDS_REQUIRE_CENTRAL_URL" in msg


@pytest.mark.parametrize("env", ["staging", "production"])
def test_non_local_env_accepts_configured_central_url(env):
    """Sanity-pin the happy path so a future tightening of the
    validator (e.g., scheme restriction) doesn't accidentally break
    compliant deployments. Sydney + Toronto staging nodes ship through
    this path today (F.27 staging validation 2026-05-22)."""
    s = Settings(
        environment=env,
        tds_secret_key=_VALID_SECRET,
        central_url="https://167.99.246.6:8200",
        require_central_url=True,
    )
    assert s.central_url == "https://167.99.246.6:8200"
    assert s.environment == env


# ---------------------------------------------------------------------------
# Operator escape hatch (require=False) tolerates empty central_url
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("env", ["staging", "production"])
def test_non_local_env_with_flag_false_accepts_empty(env):
    """F.29 plan §7.1 emergency rollback path. Operator flips
    TDS_REQUIRE_CENTRAL_URL=false ONLY when the boot-time refusal
    blocks a known-good node during incident recovery. Settings
    construction must NOT raise — the runtime warning lives in
    ``shipper.assert_shipper_ready`` instead."""
    s = Settings(
        environment=env,
        tds_secret_key=_VALID_SECRET,
        central_url="",
        require_central_url=False,
    )
    assert s.central_url == ""
    assert s.require_central_url is False


# ---------------------------------------------------------------------------
# Defense in depth — unknown environment treated as non-local
# ---------------------------------------------------------------------------


def test_unknown_env_treated_as_non_local_for_central_url():
    """Defense in depth: an unknown environment value (typo,
    rolling-deploy mid-rename) MUST be treated as non-local — fail-closed.
    Otherwise ``TDS_ENVIRONMENT=stagung`` (typo) would silently bypass
    BOTH the secret guard AND the central_url guard, surfacing as a
    50-day click-persistence blackout instead of a boot-time error.
    """
    with pytest.raises(ValidationError):
        Settings(
            environment="some-unknown-env",
            tds_secret_key=_VALID_SECRET,
            central_url="",
            require_central_url=True,
        )


# ---------------------------------------------------------------------------
# require_central_url default value — pinned per F.29 plan §7.1
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# F.29 Sprint 2.7b (2026-05-23) — HTTPS enforcement validator
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("env", ["staging", "production"])
def test_non_local_env_rejects_http_url(env):
    """The Sprint 2.7b security finding (Agent 2 HIGH S2-002): plain
    HTTP exposes the click pipeline to MITM downgrade attacks that
    trigger the Sprint 2.5 shim → silent click loss. Validator must
    refuse boot when central_url uses http:// in non-local env."""
    with pytest.raises(ValidationError) as exc:
        Settings(
            environment=env,
            tds_secret_key=_VALID_SECRET,
            central_url="http://central:8200",
            require_central_url=True,
        )
    msg = str(exc.value)
    assert "HTTPS" in msg
    # Error message must mention the security risk so operators
    # understand WHY HTTPS is required, not just THAT it's required.
    assert "MITM" in msg or "silent" in msg.lower()
    # And it must hand the operator the rollback flag for transitional
    # TLS deployment scenarios.
    assert "TDS_REQUIRE_CENTRAL_URL_HTTPS" in msg


@pytest.mark.parametrize("env", ["staging", "production"])
def test_non_local_env_accepts_https_url(env):
    """Happy path post-Sprint-2.7b: https:// in non-local env →
    Settings constructs cleanly."""
    s = Settings(
        environment=env,
        tds_secret_key=_VALID_SECRET,
        central_url="https://central:8200",
        require_central_url=True,
    )
    assert s.central_url == "https://central:8200"
    assert s.require_central_url_https is True


@pytest.mark.parametrize("env", ["staging", "production"])
def test_non_local_env_with_https_escape_hatch(env):
    """Operator escape hatch — TDS_REQUIRE_CENTRAL_URL_HTTPS=false
    permits http:// for transitional TLS-rollout deployments. Settings
    must NOT raise; the runtime risk is documented in the validator
    error message but accepted by the operator."""
    s = Settings(
        environment=env,
        tds_secret_key=_VALID_SECRET,
        central_url="http://central:8200",
        require_central_url=True,
        require_central_url_https=False,
    )
    assert s.central_url == "http://central:8200"
    assert s.require_central_url_https is False


@pytest.mark.parametrize("env", ["local", "development"])
def test_local_env_accepts_http_url(env):
    """Local + development envs are exempt from HTTPS enforcement —
    localhost dev workflows use plain http:// against a local
    collector. Mirrors the existing tolerance for empty
    tds_secret_key / central_url in local env."""
    s = Settings(
        environment=env,
        tds_secret_key="x",  # short OK in local
        central_url="http://localhost:8200",
        require_central_url=True,
    )
    assert s.central_url == "http://localhost:8200"


def test_require_central_url_https_default_is_true():
    """Pin the default — flipping to False silently broadens MITM
    attack surface. Operator must explicitly opt out."""
    s = Settings(environment="local")
    assert s.require_central_url_https is True


def test_require_central_url_default_is_true():
    """F.29 plan §7.1 decision: TDS_REQUIRE_CENTRAL_URL defaults to True
    for immediate enforcement. All known production nodes already have
    proper TDS_CENTRAL_URL per F.27 staging validation. Any future
    misconfigured node refusing to boot is THE DESIRED behavior.

    If this default drifts to False the audit-2026-05-16 silent-disable
    blast radius re-opens silently. Pin the default explicitly.
    """
    # Use local env to avoid triggering the validator's non-local path
    # — we only want to read the default for the new field here.
    s = Settings(environment="local")
    assert s.require_central_url is True
