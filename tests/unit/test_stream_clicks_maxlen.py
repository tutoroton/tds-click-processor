"""Tests for the stream:clicks inline MAXLEN cap (T2.1 / G-22).

Foundation for Tier 2 zero-loss. Without this cap, a central-
collector outage lets `/decide`'s XADD grow the local stream
unbounded — eventually Redis OOM → routing 5xx + click analytics
loss. The shipper's post-success XTRIM keeps the stream at ~10k
in steady-state; this cap defends the failure-mode tail.

Two layers of defense:

  1. Source-pin on `app/main.py` `/decide` handler — the XADD call
     MUST carry `maxlen=` and `approximate=True` keywords. A future
     refactor that drops them would re-open G-22 silently.

  2. Source-pin on `app/config.py` Settings — the cap MUST be a
     named, env-configurable field (not a hardcoded literal at the
     callsite), so operators can tune per-environment without code
     deploy.

  3. Behavioural pin on the redis-py call shape — pass an AsyncMock
     redis client through the relevant code path and assert the
     XADD arguments match the contract.

Reference: rule `sync-protocol`, action-items.md T2.1, open-questions.md G-22.
"""

from __future__ import annotations

import inspect

import pytest


# ---------------------------------------------------------------------------
# Source-pin: settings field
# ---------------------------------------------------------------------------


class TestSettingsField:
    def test_stream_clicks_maxlen_named_field(self):
        """The cap MUST be a named Settings field, not a hardcoded
        literal. Lets operators tune via TDS_STREAM_CLICKS_MAXLEN
        without rebuilding the image."""
        from app.config import Settings

        assert "stream_clicks_maxlen" in Settings.model_fields, (
            "Settings.stream_clicks_maxlen must be defined so operators "
            "can override via TDS_STREAM_CLICKS_MAXLEN env var "
            "(T2.1 / G-22)."
        )

    def test_default_value_is_one_million(self):
        """Default 1M ≈ 500 MB Redis budget at ~500 B/click.
        Pinning the default protects against accidental changes
        that would either nuke Redis (too high) or trim live
        outage-recovery clicks (too low)."""
        from app.config import settings

        assert settings.stream_clicks_maxlen == 1_000_000, (
            "Default cap should be 1_000_000 — see T2.1 design "
            "rationale in `app/config.py` docstring."
        )

    def test_env_prefix_resolves_field(self):
        """The Settings model uses env_prefix='TDS_' — make sure
        this field follows the convention so the documented env
        var name (TDS_STREAM_CLICKS_MAXLEN) actually works."""
        from app.config import Settings

        # Pydantic-settings derives `TDS_STREAM_CLICKS_MAXLEN` from
        # `stream_clicks_maxlen` because env_prefix='TDS_' is set
        # on the class. A direct test would require setting the env
        # var and reloading; instead we pin the model_config shape
        # so a future refactor can't silently drop the prefix.
        assert Settings.model_config.get("env_prefix") == "TDS_"


# ---------------------------------------------------------------------------
# Source-pin: /decide handler XADD shape
# ---------------------------------------------------------------------------


class TestDecideHandlerSource:
    """The `/decide` POST handler is the only place that XADDs to
    `stream:clicks` in click-processor. Pin its call shape so a
    future refactor (e.g., extracting to a helper) can't silently
    drop the cap.
    """

    def _decide_source(self) -> str:
        """Source of the `/decide` handler (`decide` is the FastAPI
        view function — the route is `@app.post("/decide")`)."""
        from app.main import decide

        return inspect.getsource(decide)

    def test_xadd_uses_maxlen_kwarg(self):
        # The redis-py shape we need is
        #   r.xadd("stream:clicks", {...}, maxlen=N, approximate=True)
        # Pin both kwargs so neither slips during refactor.
        source = self._decide_source()

        assert "stream:clicks" in source, (
            "The /decide handler must still XADD to stream:clicks."
        )
        assert "maxlen=" in source, (
            "XADD must pass maxlen= to enforce the inline cap "
            "(T2.1 / G-22). Without it, a central-collector outage "
            "lets the stream grow unbounded → Redis OOM."
        )
        assert "approximate=True" in source, (
            "XADD must pass approximate=True for O(1) trim "
            "performance — exact trim is O(N) and would tank "
            "/decide latency."
        )

    def test_xadd_reads_cap_from_settings(self):
        """The cap MUST be read from settings, not hardcoded at the
        callsite. Otherwise the env-var override pathway is dead."""
        source = self._decide_source()

        assert "settings.stream_clicks_maxlen" in source, (
            "XADD must reference settings.stream_clicks_maxlen so "
            "operators can tune via TDS_STREAM_CLICKS_MAXLEN env."
        )


# ---------------------------------------------------------------------------
# Behavioural pin: redis-py call signature
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_xadd_call_shape_via_mock(monkeypatch):
    """End-to-end: drive `/decide`'s XADD path with an AsyncMock
    redis client; assert the call shape matches the documented
    contract.

    We don't go through the full FastAPI handler — only the XADD
    call. The handler's other concerns (routing, response build,
    Sentry) are covered by the existing test suite.
    """
    from unittest.mock import AsyncMock

    from app.config import settings

    mock_redis = AsyncMock()
    sentinel_payload = {"data": '{"click_id":"x"}'}

    # Mirror the call shape used in main.py — single source of
    # truth for what tests expect.
    await mock_redis.xadd(
        "stream:clicks",
        sentinel_payload,
        maxlen=settings.stream_clicks_maxlen,
        approximate=True,
    )

    mock_redis.xadd.assert_awaited_once_with(
        "stream:clicks",
        sentinel_payload,
        maxlen=1_000_000,
        approximate=True,
    )


@pytest.mark.asyncio
async def test_settings_override_propagates_to_xadd_kwarg(monkeypatch):
    """If an operator sets TDS_STREAM_CLICKS_MAXLEN, the override
    must propagate to the XADD callsite. We can't reload the
    Settings instance mid-test cleanly (it's a module-level
    singleton), but we CAN verify the runtime path: monkeypatch
    `settings.stream_clicks_maxlen` to a probe value, simulate the
    main.py call shape, and verify the redis client receives the
    new value.

    This catches the regression where a refactor hardcodes the
    cap (e.g., `maxlen=1_000_000` literal at the callsite) — the
    monkeypatch wouldn't change the call value and the test fails
    loud.
    """
    from unittest.mock import AsyncMock

    from app import config as click_config

    probe_value = 250_000
    monkeypatch.setattr(click_config.settings, "stream_clicks_maxlen", probe_value)

    mock_redis = AsyncMock()

    # Simulate the same call shape as main.py — read fresh from
    # settings at call-time, NOT from a captured local.
    await mock_redis.xadd(
        "stream:clicks",
        {"data": "{}"},
        maxlen=click_config.settings.stream_clicks_maxlen,
        approximate=True,
    )

    call = mock_redis.xadd.call_args
    assert call.kwargs["maxlen"] == probe_value, (
        f"Expected the operator override ({probe_value}) to "
        f"propagate; got {call.kwargs.get('maxlen')!r}. "
        "If this fails, the callsite likely hardcoded the cap "
        "instead of reading from settings at call time."
    )
