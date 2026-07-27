"""Tests for the click shipper's DEDICATED Redis connection pool.

TDSP-E20 (2026-07-27). Root-cause fix for ~592K duplicate Sentry
``xreadgroup`` ``TimeoutError`` events (98% of the organisation's error
volume): the shipper's blocking ``XREADGROUP ... BLOCK=BATCH_TIMEOUT_MS``
(2000ms) shared the ROUTING pool's ``socket_timeout`` (1.0s, sized for the
``/decide`` hot path). redis-py's client-side ``socket_timeout`` fires
before Redis's own BLOCK deadline and tears the connection down — so every
IDLE poll (not just a real Redis stall) threw ``TimeoutError``, captured
TWICE (a ``logger.error`` + an explicit Sentry capture).

Fix: ``get_shipper_redis()`` (app/redis_client.py) — a third pool, same
pattern as the existing ``identity_pool``/``get_identity_redis()`` — whose
``socket_timeout`` carries margin OVER the shipper's own BLOCK value.

Coverage (class-level invariants, not magic numbers — the specific
seconds are tunable; what must ALWAYS hold is
``redis_shipper_socket_timeout_seconds > BLOCK``):

  * ``get_shipper_redis()`` builds an ISOLATED ``BlockingConnectionPool``
    wired to the ``redis_shipper_*`` settings — distinct object from the
    routing pool, distinct timeout value.
  * ``_assert_shipper_pool_timeout_margin`` (the boot-time guard) accepts
    any margin > BLOCK and rejects any value <= BLOCK, regardless of the
    concrete numbers involved.
  * ``assert_shipper_ready()`` runs the margin guard unconditionally
    (before the central_url branch) — a misconfigured pool fails boot
    even when the shipper would otherwise be disabled.
  * Source pin: ``main.py`` wires the shipper task to
    ``get_shipper_redis()``, not the shared routing pool.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from redis.asyncio.connection import BlockingConnectionPool

import app.main as main_module
import app.redis_client as rc
from app import shipper
from app.config import settings
from app.shipper import (
    BATCH_TIMEOUT_MS,
    _assert_shipper_pool_timeout_margin,
    assert_shipper_ready,
)


@pytest.fixture(autouse=True)
def _reset_pools():
    """Isolate the module-level pool singletons across tests."""
    rc.pool = None
    rc.shipper_pool = None
    yield
    rc.pool = None
    rc.shipper_pool = None


# ---------------------------------------------------------------------------
# get_shipper_redis() — isolated pool, wired to the shipper-specific knobs
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_shipper_redis_builds_isolated_blocking_pool():
    """A dedicated BlockingConnectionPool, wired to `redis_shipper_*` —
    not the routing pool's settings."""
    client = await rc.get_shipper_redis()
    cp = client.connection_pool
    assert isinstance(cp, BlockingConnectionPool)
    assert cp.max_connections == settings.redis_shipper_max_connections
    assert cp.timeout == settings.redis_shipper_pool_timeout_seconds
    assert (
        cp.connection_kwargs["socket_timeout"]
        == settings.redis_shipper_socket_timeout_seconds
    )
    assert (
        cp.connection_kwargs["socket_connect_timeout"]
        == settings.redis_shipper_socket_connect_timeout_seconds
    )


@pytest.mark.asyncio
async def test_shipper_pool_is_distinct_from_routing_pool():
    """The whole point of the fix: two SEPARATE pools, so a shipper-sized
    socket_timeout never leaks onto the `/decide` hot path and vice versa."""
    shipper_client = await rc.get_shipper_redis()
    routing_client = await rc.get_redis()
    assert shipper_client is not routing_client
    assert shipper_client.connection_pool is not routing_client.connection_pool


@pytest.mark.asyncio
async def test_shipper_pool_is_a_singleton_across_calls():
    first = await rc.get_shipper_redis()
    second = await rc.get_shipper_redis()
    assert first is second


@pytest.mark.asyncio
async def test_close_shipper_redis_resets_singleton():
    await rc.get_shipper_redis()
    assert rc.shipper_pool is not None
    await rc.close_shipper_redis()
    assert rc.shipper_pool is None


# ---------------------------------------------------------------------------
# The CLASS invariant: socket_timeout must have margin over BLOCK,
# regardless of the concrete numbers on either side.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "socket_timeout_seconds",
    [
        # Exactly the pre-fix bug: a shared/short timeout <= BLOCK.
        BATCH_TIMEOUT_MS / 1000 - 1.0,
        BATCH_TIMEOUT_MS / 1000,  # equal — still races the server's timer
        0.001,
    ],
)
def test_margin_guard_rejects_any_timeout_at_or_below_block(
    monkeypatch, socket_timeout_seconds,
):
    monkeypatch.setattr(
        shipper.settings,
        "redis_shipper_socket_timeout_seconds",
        socket_timeout_seconds,
    )
    with pytest.raises(RuntimeError, match="Shipper Redis pool misconfigured"):
        _assert_shipper_pool_timeout_margin()


@pytest.mark.parametrize(
    "socket_timeout_seconds",
    [
        BATCH_TIMEOUT_MS / 1000 + 0.001,  # smallest possible margin
        3.0,
        5.0,  # the shipped default
        30.0,
    ],
)
def test_margin_guard_accepts_any_timeout_above_block(
    monkeypatch, socket_timeout_seconds,
):
    monkeypatch.setattr(
        shipper.settings,
        "redis_shipper_socket_timeout_seconds",
        socket_timeout_seconds,
    )
    _assert_shipper_pool_timeout_margin()  # must not raise


def test_default_config_satisfies_the_margin_invariant():
    """The shipped default must itself satisfy the class invariant —
    otherwise every real boot would hit the guard."""
    assert settings.redis_shipper_socket_timeout_seconds > (
        BATCH_TIMEOUT_MS / 1000
    )


def test_margin_guard_runs_even_when_shipper_would_be_disabled(monkeypatch):
    """A misconfigured pool fails boot BEFORE the central_url branch —
    it must not be possible to silently ship a broken pool config just
    because the shipper is disabled in this environment."""
    monkeypatch.setattr(shipper.settings, "central_url", "")
    monkeypatch.setattr(shipper.settings, "environment", "local")
    monkeypatch.setattr(
        shipper.settings, "redis_shipper_socket_timeout_seconds", 0.1,
    )
    with pytest.raises(RuntimeError, match="Shipper Redis pool misconfigured"):
        assert_shipper_ready()


def test_assert_shipper_ready_happy_path_still_passes_the_guard(monkeypatch):
    """Sanity: a correctly-configured pool + configured central_url still
    reaches the existing happy-path return (no regression to F.29 Sprint
    1.2's fail-closed behaviour)."""
    monkeypatch.setattr(shipper.settings, "central_url", "https://central.example")
    assert_shipper_ready() is None


# ---------------------------------------------------------------------------
# Source pin — main.py must wire the shipper task off the dedicated pool.
# ---------------------------------------------------------------------------


def test_main_wires_shipper_task_to_dedicated_pool():
    """`run_shipper(...)` in main.py must NOT be called with the shared
    routing client — regression guard against silently reverting to the
    pre-fix shared-pool wiring."""
    src = Path(main_module.__file__).read_text()
    assert "get_shipper_redis" in src
    assert "run_shipper(shipper_redis)" in src
    assert "run_shipper(r)" not in src


def test_main_closes_shipper_pool_on_shutdown():
    src = Path(main_module.__file__).read_text()
    assert "close_shipper_redis()" in src
