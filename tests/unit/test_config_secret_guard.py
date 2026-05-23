"""Tests for the click-processor startup secret guard.

Closes Agent 2 HIGH-1 audit finding (2026-05-09): without this
guard, a click-processor node deployed with `TDS_SECRET_KEY=""`
in a non-local environment would silently no-op BOTH the
X-TDS-Key auth check AND the T2.4 X-TDS-Body-Sig verifier —
leaving `/admin/sync` open to unauthenticated +
un-integrity-checked snapshot pushes.

The guard mirrors `services/admin-api/app/config.py`'s
`_enforce_secret_presence` model_validator. Coverage:

  * Local environments tolerate empty secret (dev workflow).
  * Non-local environments REJECT empty secret.
  * Non-local environments REJECT short secret (<32 chars).
  * Non-local environments ACCEPT compliant secret.

Reference: rule `api-security` "JWT lifecycle" + "Secret startup
guard"; rule `sync-protocol` "hmac.compare_digest for auth".
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from app.config import Settings


# ---------------------------------------------------------------------------
# Local environments tolerate empty / weak secret (dev mode)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("env", ["local", "development"])
def test_local_env_accepts_empty_secret(env):
    """Local + development envs are intentionally lenient — the
    `tds_secret_key: str = ""` default still works in dev so an
    engineer can `make dev` without provisioning a secret."""
    s = Settings(environment=env, tds_secret_key="")
    assert s.tds_secret_key == ""
    assert s.environment == env


@pytest.mark.parametrize("env", ["local", "development"])
def test_local_env_accepts_short_secret(env):
    """Even a 1-char secret passes in local — operators may be
    smoke-testing the auth path without a production secret on
    hand. Production gate below catches the real risk."""
    s = Settings(environment=env, tds_secret_key="x")
    assert s.tds_secret_key == "x"


# ---------------------------------------------------------------------------
# Non-local environments enforce the guard
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("env", ["staging", "production"])
def test_non_local_env_rejects_empty_secret(env):
    """The CRITICAL audit case — without this rejection, both
    auth + body-sig collapse to no-ops and the /admin/sync
    endpoint is fully open. Pin the rejection at the env level."""
    with pytest.raises(ValidationError) as exc:
        Settings(environment=env, tds_secret_key="")

    msg = str(exc.value)
    assert "TDS_SECRET_KEY" in msg
    assert env in msg
    # The error message MUST mention the actual security
    # consequence so an operator reading the boot log knows
    # WHY the gate exists, not just THAT it failed.
    assert "MITM" in msg or "auth" in msg.lower()


@pytest.mark.parametrize("env", ["staging", "production"])
def test_non_local_env_rejects_short_secret(env):
    """31 chars — one shy of the 32-char floor. Mirrors admin-api's
    same-length guard so a single rotation procedure produces a
    secret that satisfies BOTH services."""
    short = "x" * 31
    with pytest.raises(ValidationError) as exc:
        Settings(environment=env, tds_secret_key=short)

    msg = str(exc.value)
    assert "32" in msg
    # Don't echo the secret value in the error message — that
    # would leak via logs / Sentry capture. The length is the
    # only thing the message should reveal.
    assert short not in msg


@pytest.mark.parametrize("env", ["staging", "production"])
def test_non_local_env_accepts_compliant_secret(env):
    """32+ chars + non-local env + valid central_url → boot succeeds.
    Sanity-pin the happy path so a future tightening of the validator
    (e.g., char-class restriction) doesn't accidentally break compliant
    secrets.

    F.29 Sprint 1.1 cascade (2026-05-23): the new
    ``_enforce_central_url_presence`` validator (config.py, sibling of
    ``_enforce_secret_presence``) ALSO refuses non-local boots with
    empty ``central_url`` when ``require_central_url`` is True (default).
    The original pre-F.29 form of this test relied on the implicit
    ``central_url: str = ""`` default passing in non-local env — that's
    now a misconfig. Explicitly provide a valid central_url so this
    test pins ONLY the secret-guard happy path, leaving the central_url
    guard to ``test_config_central_url_guard.py``.
    """
    secret = "x" * 32
    s = Settings(
        environment=env,
        tds_secret_key=secret,
        # F.29 Sprint 2.7b — HTTPS now required by default in non-local
        # env. Use https:// here so the test pins ONLY the secret-guard
        # happy path, not the URL scheme validator (covered separately
        # in test_config_central_url_guard.py).
        central_url="https://central:8200",
    )
    assert s.tds_secret_key == secret
    assert s.environment == env


def test_unknown_env_treated_as_non_local():
    """Defense in depth: an unknown environment value (typo,
    rolling-deploy mid-rename) MUST be treated as non-local —
    fail-closed. Otherwise `TDS_ENVIRONMENT=stagung` (typo)
    would silently bypass the guard."""
    with pytest.raises(ValidationError):
        Settings(environment="some-unknown-env", tds_secret_key="")
