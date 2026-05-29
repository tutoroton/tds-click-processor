"""Unit tests for click-processor `app/diag.py`.

Mirror of services/admin-api/tests/unit/test_diag.py — both modules
share the same contract. Trimmed to behaviours specific to the
click-processor twin (`service` field on emitted obs entries) and a
sanity check that the gating logic behaves identically.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.fixture(autouse=True)
def _reset_queue():
    from app import diag
    diag._obs_queue = None
    yield
    diag._obs_queue = None


def _set_toggles(*, traces=False, obs=False, verbose=False, queue_max=10_000,
                 drain_ms=100, stream_maxlen=10_000, ttl=3600):
    from app import diag
    return patch.multiple(
        diag.settings,
        diag_traces_boost=traces,
        diag_obs_stream=obs,
        diag_verbose_logs=verbose,
        diag_obs_queue_max=queue_max,
        diag_obs_drain_interval_ms=drain_ms,
        diag_obs_stream_maxlen=stream_maxlen,
        diag_obs_stream_ttl_seconds=ttl,
    )


def test_emit_obs_no_op_without_test_id():
    from app import diag
    with _set_toggles(obs=True):
        diag.emit_obs("click.decide_in", {"foo": "bar"})
    assert diag._obs_queue is None


def test_set_test_id_rejects_invalid_values():
    """Audit closure 2026-05-10 — UUID-shape gate prevents Redis key
    poisoning + Sentry tag pollution from attacker-crafted X-Test-Id
    headers reaching click-processor (auth-gated by X-TDS-Key but
    defense-in-depth)."""
    from app import diag
    for bad in ("abc", "a" * 65, "valid; rm -rf /", "with\nnewline"):
        diag.set_test_id(bad)
        assert diag.get_test_id() == "", f"should reject {bad!r}"
    valid = "7f3a2c61-e08e-4f62-91a7-d4f9c0b8a2e1"
    diag.set_test_id(valid)
    assert diag.get_test_id() == valid


def test_emit_obs_no_op_when_toggle_off():
    from app import diag
    diag.set_test_id("abc12345")
    with _set_toggles(obs=False):
        diag.emit_obs("click.decide_in", {"foo": "bar"})
    assert diag._obs_queue is None


def test_emit_obs_queues_when_both_conditions_true():
    from app import diag
    diag.set_test_id("abc12345")
    with _set_toggles(obs=True):
        diag.emit_obs("click.decide_in", {"click_id": "019e..."})
    assert diag._obs_queue is not None
    assert diag._obs_queue.qsize() == 1


def test_traces_sampler_boosts_for_tagged_request():
    from app import diag
    with _set_toggles(traces=True):
        ctx = {"asgi_scope": {"headers": [(b"x-test-id", b"abcdef12")]}}
        assert diag.traces_sampler(ctx) == 1.0


def test_traces_sampler_baseline_otherwise():
    from app import diag
    with _set_toggles(traces=False):
        ctx = {"asgi_scope": {"headers": [(b"x-test-id", b"abcdef12")]}}
        assert diag.traces_sampler(ctx) == 0.1


def test_drain_batch_emits_click_processor_service_field():
    """The click-processor twin tags entries with `service=click-processor`
    and `node_id=<settings.node_id>` — distinct from admin-api's
    `service=admin-api`/`node_id=central`. Trace CLI relies on this
    to route timeline rendering."""
    from app import diag

    diag.set_test_id("abc12345")
    with _set_toggles(obs=True):
        diag.emit_obs("click.decide_in", {})

    pipe = MagicMock()
    pipe.execute = AsyncMock(return_value=[])
    redis = MagicMock()
    redis.pipeline = MagicMock(return_value=pipe)

    asyncio.run(diag._drain_batch(redis, diag._obs_queue, maxlen=5000, ttl=3600))

    # Inspect what XADD was called with
    assert pipe.xadd.call_count == 1
    args, kwargs = pipe.xadd.call_args
    # First positional arg = key, second = entry dict
    key = args[0]
    entry = args[1]
    assert key.startswith("obs:test:")
    assert entry["service"] == "click-processor"
    # node_id mirrors settings.node_id; default in tests is "local"
    assert "node_id" in entry


# --------------------------------------------------------------------------- #
# F.40 PII hardening (C14 / CRIT-001) — before_send strips body + truncates IP #
# --------------------------------------------------------------------------- #


class TestF40PiiScrub:
    """Once a node attaches to a tenant Sentry account, events ship to a
    THIRD-PARTY org. The `before_send` hook MUST strip the request body
    (click payloads carry visitor IP / geo / sub-ids) and truncate any IP
    to its network prefix. This is the CRIT-001 gate — Phase 3 is blocked
    on it being verified green."""

    def test_truncate_ipv4_to_24(self):
        from app.diag import _truncate_ip
        assert _truncate_ip("203.0.113.77") == "203.0.113.0"

    def test_truncate_ipv6_to_48(self):
        from app.diag import _truncate_ip
        assert _truncate_ip("2001:db8:abcd:1234::1") == "2001:db8:abcd::"

    def test_truncate_malformed_returns_empty(self):
        from app.diag import _truncate_ip
        assert _truncate_ip("not-an-ip") == ""
        assert _truncate_ip("") == ""

    def test_request_body_stripped(self):
        from app.diag import before_send
        event = {"request": {"data": {"ip": "203.0.113.7", "sub_id": "aff42"}}}
        out = before_send(event, {})
        assert out["request"]["data"] == "[stripped]"

    def test_env_remote_addr_truncated(self):
        from app.diag import before_send
        event = {"request": {"env": {"REMOTE_ADDR": "203.0.113.77"}}}
        out = before_send(event, {})
        assert out["request"]["env"]["REMOTE_ADDR"] == "203.0.113.0"

    def test_user_ip_truncated(self):
        from app.diag import before_send
        event = {"user": {"ip_address": "2001:db8:abcd:1234::9"}}
        out = before_send(event, {})
        assert out["user"]["ip_address"] == "2001:db8:abcd::"

    def test_user_ip_unparseable_dropped_not_blanked(self):
        # A blank ip_address re-triggers Sentry {{auto}} fill — so an
        # unparseable value must be DROPPED, not set to "".
        from app.diag import before_send
        event = {"user": {"ip_address": "garbage", "id": "u1"}}
        out = before_send(event, {})
        assert "ip_address" not in out["user"]
        assert out["user"]["id"] == "u1"

    def test_scrub_never_crashes_on_malformed_event(self):
        # Total-failure-safe: a weird event shape must not raise (the SDK
        # send path must never blow up on scrubbing).
        from app.diag import before_send
        weird = {"request": {"data": object()}, "user": "not-a-dict"}
        out = before_send(weird, {})
        assert out is weird  # returned, not dropped
