"""Pin the .env contract produced by deploy/render-env.sh (F.32 Track 1).

render-env.sh is the SINGLE generator of a node's .env, shared by BOTH
provisioning paths (admin cloud-init + tds-deploy) — this is where the .env
key contract now lives (moved out of the admin-api cloud-init heredoc, which
had drifted). Runs the actual bash script in a tmp dir and asserts the output,
so a change to the key set / double-write / https-guard / central_api_key
default is caught here.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "deploy" / "render-env.sh"

BASE = {
    "TDS_NODE_ID": "au-syd1",
    "TDS_NODE_REGION": "au",
    "TDS_SECRET_KEY": "shared-secret-xyz",
    "TDS_CENTRAL_URL": "https://api-collector.example.com",
    "TDS_CENTRAL_API_KEY": "collector-shared-key",
    "TDS_ENVIRONMENT": "production",
    "CADDY_DOMAIN": "api-au.example.com",
}


def _run(tmp_path, overrides=None, drop=()):
    env = {**os.environ, **BASE, **(overrides or {})}
    for k in drop:
        env.pop(k, None)
    return subprocess.run(
        ["bash", str(SCRIPT), str(tmp_path)],
        env=env, capture_output=True, text=True,
    )


def _env_dict(tmp_path) -> dict[str, str]:
    lines = (tmp_path / ".env").read_text().splitlines()
    return dict(
        line.split("=", 1) for line in lines if "=" in line and not line.startswith("#")
    )


def test_writes_full_canonical_key_set(tmp_path):
    r = _run(tmp_path)
    assert r.returncode == 0, r.stderr
    env = _env_dict(tmp_path)
    for key in (
        "TDS_NODE_ID", "TDS_NODE_REGION", "TDS_ENVIRONMENT", "TDS_PORT",
        "CADDY_DOMAIN", "TDS_SECRET_KEY", "TDS_TDS_SECRET_KEY",
        "TDS_CENTRAL_URL", "TDS_CENTRAL_API_KEY", "TDS_SYNC_URL",
        "TDS_SMOKE_PROBE_SECRET",
        "TDS_SENTRY_DSN", "TDS_DIAG_TRACES_BOOST", "TDS_DIAG_OBS_STREAM",
        "TDS_DIAG_VERBOSE_LOGS", "TDS_CODE_VERSION",
        # GTD-R75 / ADR-0055 — capacity auto-config
        "WEB_CONCURRENCY", "TDS_REDIS_MAX_CONNECTIONS",
    ):
        assert key in env, f"missing key {key}"


def test_secret_double_write(tmp_path):
    """TDS_TDS_SECRET_KEY mirrors TDS_SECRET_KEY (pydantic env_prefix=TDS_)."""
    _run(tmp_path)
    env = _env_dict(tmp_path)
    assert env["TDS_SECRET_KEY"] == "shared-secret-xyz"
    assert env["TDS_TDS_SECRET_KEY"] == "shared-secret-xyz"


def test_central_api_key_required_not_secret(tmp_path):
    """Q5 (F.32 Track 2 CORRECTED): the collector key is a SHARED deployment
    secret (collector_api_key), NOT the node secret_key. It is REQUIRED — the
    old `:-$TDS_SECRET_KEY` default produced the wrong value and the collector
    rejected the shipper with 403. Dropping it must fail loud and write no .env."""
    r = _run(tmp_path, drop=("TDS_CENTRAL_API_KEY",))
    assert r.returncode != 0
    assert "TDS_CENTRAL_API_KEY" in r.stderr
    assert not (tmp_path / ".env").exists()


def test_central_api_key_uses_passed_value_not_secret(tmp_path):
    """The rendered key is the passed collector key — never the node secret."""
    _run(tmp_path, {"TDS_CENTRAL_API_KEY": "distinct-collector-key"})
    env = _env_dict(tmp_path)
    assert env["TDS_CENTRAL_API_KEY"] == "distinct-collector-key"
    assert env["TDS_CENTRAL_API_KEY"] != env["TDS_SECRET_KEY"]


def test_sync_url_empty_by_default(tmp_path):
    """TDS_SYNC_URL (admin-api config-pull host) defaults empty ⇒ pull disabled."""
    _run(tmp_path)
    assert _env_dict(tmp_path)["TDS_SYNC_URL"] == ""


def test_sync_url_passthrough(tmp_path):
    _run(tmp_path, {"TDS_SYNC_URL": "https://api-tds.example.com"})
    assert _env_dict(tmp_path)["TDS_SYNC_URL"] == "https://api-tds.example.com"


def test_https_guard_rejects_http_in_production(tmp_path):
    """F.29 boot-guard parity: http central_url in production fails LOUD and
    writes no .env (so a misconfigured node never even builds)."""
    r = _run(tmp_path, {"TDS_CENTRAL_URL": "http://api-collector.example.com"})
    assert r.returncode != 0
    assert "https" in r.stderr.lower()
    assert not (tmp_path / ".env").exists()


def test_http_allowed_in_local_env(tmp_path):
    r = _run(tmp_path, {"TDS_ENVIRONMENT": "local", "TDS_CENTRAL_URL": "http://localhost:8200"})
    assert r.returncode == 0, r.stderr
    assert _env_dict(tmp_path)["TDS_CENTRAL_URL"] == "http://localhost:8200"


def test_missing_required_field_fails(tmp_path):
    r = _run(tmp_path, drop=("TDS_SECRET_KEY",))
    assert r.returncode != 0
    assert "TDS_SECRET_KEY" in r.stderr


def test_explicit_code_version_stamped(tmp_path):
    _run(tmp_path, {"TDS_CODE_VERSION": "abc1234"})
    assert _env_dict(tmp_path)["TDS_CODE_VERSION"] == "abc1234"


def test_capacity_config_empty_by_default(tmp_path):
    """GTD-R75 / ADR-0055 — unset ⇒ empty, NOT a hardcoded number. An
    existing node re-provisioned with no resolved capacity config must stay
    byte-unchanged: click-processor's own Dockerfile (`${WEB_CONCURRENCY:-2}`)
    / config.py (`redis_max_connections = 128`) defaults govern unopposed."""
    _run(tmp_path)
    env = _env_dict(tmp_path)
    assert env["WEB_CONCURRENCY"] == ""
    assert env["TDS_REDIS_MAX_CONNECTIONS"] == ""


def test_capacity_config_passthrough(tmp_path):
    """When provisioning resolved a config (cores parsed), both values pass
    straight through into .env unmodified."""
    _run(tmp_path, {"WEB_CONCURRENCY": "8", "TDS_REDIS_MAX_CONNECTIONS": "256"})
    env = _env_dict(tmp_path)
    assert env["WEB_CONCURRENCY"] == "8"
    assert env["TDS_REDIS_MAX_CONNECTIONS"] == "256"
