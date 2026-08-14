"""The `/stats` loopback carve-out is safe only because of the node's TOPOLOGY.

`GET /stats` on an edge node is authenticated in non-local environments, with
one exception: a caller whose peer address is loopback is let through
unauthenticated, so the deploy's `health.sh` probe needs no credential
(`app/main.py`, `_LOOPBACK_HOSTS`). The endpoint returns node identity, region,
active-campaign count and Redis memory.

That carve-out rests on an assumption the code states but cannot enforce, in its
own words:

    INVARIANT: this carve-out is sound ONLY while click-processor :8100 is
    reached DIRECTLY (no L7 reverse-proxy on the same host). A front proxy
    would make every caller's peer == loopback → unauthenticated /stats.
    Re-evaluate before fronting a node with a proxy.

**A node IS fronted by a proxy** — `docker-compose.node.yml` publishes 80/443
and `${TDS_PORT:-8100}` on a Caddy container, and click-processor publishes
nothing. Measured 2026-08-15, that is still SAFE, and the reason is precisely
the topology: Caddy reaches click-processor over the compose BRIDGE network, so
`request.client.host` is Caddy's bridge address (172.x), never `127.0.0.1`.
The carve-out therefore does not fire for proxied traffic, and every external
caller must present the node's `X-TDS-Key`.

Three separate things keep that true, and nothing pinned any of them:

1. **click-processor publishes no host port.** Publishing `8100:8100` would put
   the app itself on the node's public IP, bypassing Caddy — and with it the
   whole vhost layer.
2. **No service runs `network_mode: host`.** That is the exact scenario the
   invariant warns about: with a shared network namespace Caddy would reach the
   app over loopback, and then EVERY external caller's peer reads as
   `127.0.0.1` and `/stats` is open to the internet.
3. **uvicorn does not trust forwarded headers.** `request.client.host` is the
   kernel-set TCP peer, which cannot be spoofed — unless uvicorn is told to
   rewrite it from `X-Forwarded-For`. The Dockerfile launches uvicorn with no
   `--proxy-headers`/`--forwarded-allow-ips`, and `FORWARDED_ALLOW_IPS` appears
   nowhere in the repo; uvicorn's default trusts only `127.0.0.1`, which Caddy
   is not. A `FORWARDED_ALLOW_IPS=*` (a common docker idiom) would hand the
   peer address to a header.

Each is a one-line edit away from being false, each looks like an ordinary
performance or convenience tweak, and none of them would fail anything.
"""

from __future__ import annotations

import pathlib

import pytest
import yaml

_SERVICE = pathlib.Path(__file__).resolve().parents[2]
_NODE_COMPOSE = _SERVICE / "docker-compose.node.yml"
_DOCKERFILE = _SERVICE / "Dockerfile"
_MAIN = _SERVICE / "app/main.py"


def _node_services() -> dict:
    return yaml.safe_load(_NODE_COMPOSE.read_text()).get("services") or {}


# --------------------------------------------------------------------------
# Positive controls — this file guards a carve-out; if the carve-out is gone,
# say so loudly instead of passing over an empty world.
# --------------------------------------------------------------------------

def test_the_node_compose_and_the_carveout_both_still_exist():
    assert _NODE_COMPOSE.is_file(), f"{_NODE_COMPOSE} is gone — did the node layout move?"
    services = _node_services()
    assert len(services) >= 3, f"only {sorted(services)} in the node compose"
    assert "click-processor" in services, sorted(services)

    main = _MAIN.read_text()
    assert "_LOOPBACK_HOSTS" in main, (
        "the /stats loopback carve-out is gone from app/main.py. If it was "
        "removed deliberately, this whole file can go too — but do that on "
        "purpose, not by leaving a guard that now protects nothing."
    )


# --------------------------------------------------------------------------
# The three topology facts the carve-out depends on
# --------------------------------------------------------------------------

def test_click_processor_publishes_no_host_port_on_a_node():
    ports = (_node_services().get("click-processor") or {}).get("ports")
    assert not ports, (
        f"click-processor publishes {ports} on the edge node. That puts the app "
        "on the node's public IP directly, bypassing Caddy and every vhost-level "
        "guard with it. Only Caddy should publish; the app is reached over the "
        "compose network."
    )


@pytest.mark.parametrize("service", sorted(_node_services()))
def test_no_node_service_shares_the_host_network_namespace(service: str):
    mode = (_node_services().get(service) or {}).get("network_mode")
    assert mode != "host", (
        f"{service} runs with network_mode: host. This is the exact scenario "
        "app/main.py's carve-out warns about: sharing the host namespace makes "
        "Caddy reach click-processor over LOOPBACK, so every external caller's "
        "peer address reads as 127.0.0.1 and /stats becomes unauthenticated on "
        "the public internet — node id, region, campaign count and Redis memory."
    )


def test_uvicorn_does_not_take_its_peer_address_from_a_header():
    """`request.client.host` must stay the kernel-set TCP peer.

    A socket peer cannot be spoofed by a client; an `X-Forwarded-For` can. Every
    knob that swaps one for the other is checked here, in the two places that
    could set it — the launch command and the node compose.
    """
    dockerfile = _DOCKERFILE.read_text()
    compose = _NODE_COMPOSE.read_text()

    for needle, where in (
        ("--proxy-headers", "the Dockerfile CMD"),
        ("--forwarded-allow-ips", "the Dockerfile CMD"),
        ("FORWARDED_ALLOW_IPS", "the Dockerfile"),
    ):
        assert needle not in dockerfile, (
            f"{needle} appears in {where}. uvicorn would then rewrite "
            "request.client.host from X-Forwarded-For, and the /stats loopback "
            "carve-out would be decided by a header the caller controls rather "
            "than by the socket."
        )

    assert "FORWARDED_ALLOW_IPS" not in compose, (
        "FORWARDED_ALLOW_IPS is set in the node compose. With '*' — the common "
        "docker idiom — uvicorn trusts X-Forwarded-For from anyone, and the "
        "/stats carve-out stops being about the socket peer at all."
    )
