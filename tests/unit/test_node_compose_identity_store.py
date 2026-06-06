"""Pin the edge-node compose invariants for the returning-user identity store.

Guards two bugs found in the 2026-06-06 edge-identity research
(DECISION-edge-identity-architecture.md, R2/R8):

  * B1 — the ROUTING redis must stay evictable (disposable cache), and the
    IDENTITY store must be a SEPARATE instance with `noeviction`. Putting
    identity on the evicting routing cache silently turns returning users
    back into "new" and drops sticky pins.
  * B2 — `TDS_IDENTITY_REDIS_URL` must never be present without a container
    behind it. Here it is a compose-literal pointing at the `identity-redis`
    service, so the URL and the service ship together in one tracked file.

These are structural assertions over docker-compose.node.yml — no runtime.
"""

from __future__ import annotations

from pathlib import Path

import yaml

COMPOSE = Path(__file__).resolve().parents[2] / "docker-compose.node.yml"


def _compose() -> dict:
    return yaml.safe_load(COMPOSE.read_text())


def test_identity_redis_service_exists_and_is_noeviction():
    svc = _compose()["services"]
    assert "identity-redis" in svc, "dedicated identity store service missing (B2)"
    cmd = svc["identity-redis"]["command"]
    assert "--maxmemory-policy noeviction" in cmd, (
        "identity store MUST be noeviction — an eviction silently degrades a "
        "returning user to new + drops sticky pins (B1/D30)"
    )


def test_routing_redis_stays_evictable():
    # The routing cache is disposable (rebuildable from PG via sync); eviction
    # is the correct degradation. It must NOT host identity.
    cmd = _compose()["services"]["redis"]["command"]
    assert "--maxmemory-policy volatile-lru" in cmd, (
        "routing cache should remain evictable (disposable-cache invariant)"
    )


def test_identity_url_is_compose_literal_pointing_at_identity_redis():
    cp = _compose()["services"]["click-processor"]
    env = cp.get("environment", [])
    # environment is a list of "KEY=VALUE" strings in this compose.
    kv = dict(e.split("=", 1) for e in env if "=" in e)
    url = kv.get("TDS_IDENTITY_REDIS_URL")
    assert url, "TDS_IDENTITY_REDIS_URL must be a compose-literal (B2)"
    assert "identity-redis" in url, (
        "identity URL must target the dedicated noeviction service, not the "
        "routing redis (B1)"
    )
    # And it must NOT point at the routing cache host.
    assert url != kv.get("TDS_REDIS_URL"), "identity must not reuse the routing cache"


def test_click_processor_depends_on_identity_redis_healthy():
    dep = _compose()["services"]["click-processor"].get("depends_on", {})
    assert "identity-redis" in dep, "click-processor should wait for the identity store"


def test_identity_volume_declared():
    vols = _compose().get("volumes", {})
    assert "tds-identity-data" in vols, "identity store needs its own named volume"
