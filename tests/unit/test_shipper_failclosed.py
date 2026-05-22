"""Tests for the click-processor shipper runtime fail-closed guard.

F.29 Sprint 1.2 (2026-05-23). Defense-in-depth complement to
``test_config_central_url_guard.py``. The Pydantic validator
``_enforce_central_url_presence`` catches the misconfig at
config-construction time; this test pins the SECOND line of defence
inside ``shipper.assert_shipper_ready`` which fires if the validator was
bypassed (env mutation post-boot, or a test using
``Settings.model_construct`` to skip validation).

Coverage:

  * ``assert_shipper_ready``:
      - happy path: configured central_url → returns silently.
      - local env + empty url → INFO log + returns (no raise).
      - non-local + empty url + flag=False → WARN log + Sentry
        capture_message + returns (escape hatch).
      - non-local + empty url + flag=True → CRITICAL log + Sentry
        capture_message(level="fatal") + raises
        ShipperDisabledError.

  * ``run_shipper``:
      - re-runs ``assert_shipper_ready`` (defense in depth) — pinned
        via a mock that raises on second call.
      - empty central_url in local env: coroutine returns without
        touching Redis (no xgroup_create attempt).

Reference: F.29 plan §3 G1-CRIT closure, plan §4 Sprint 1.2 row,
audit 2026-05-16 incident anchor.
"""

from __future__ import annotations

import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app import shipper
from app.shipper import (
    ShipperDisabledError,
    assert_shipper_ready,
    run_shipper,
)


@pytest.fixture(autouse=True)
def _reset_shipper_settings(monkeypatch):
    """Per-test isolation. Each test sets the central_url +
    environment + require flag it cares about; the autouse fixture
    starts from a known baseline so tests don't leak state."""
    monkeypatch.setattr(shipper.settings, "central_url", "")
    monkeypatch.setattr(shipper.settings, "central_api_key", "")
    monkeypatch.setattr(shipper.settings, "environment", "local")
    monkeypatch.setattr(shipper.settings, "require_central_url", True)


# ---------------------------------------------------------------------------
# assert_shipper_ready — happy path
# ---------------------------------------------------------------------------


def test_assert_ready_returns_silently_with_central_url(monkeypatch):
    """Configured central_url + any env → no raise, no Sentry call.
    The shipper proceeds to its main loop without any boot-time noise.
    """
    monkeypatch.setattr(shipper.settings, "central_url", "http://central:8200")
    monkeypatch.setattr(shipper.settings, "environment", "staging")

    with patch("app.shipper.sentry_sdk") as mock_sentry:
        assert_shipper_ready()  # Must not raise
        mock_sentry.capture_message.assert_not_called()


# ---------------------------------------------------------------------------
# assert_shipper_ready — local env carve-out
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("env", ["local", "development"])
def test_assert_ready_local_env_empty_url_logs_info(monkeypatch, caplog, env):
    """Local + development envs are intentionally lenient. Empty
    central_url → INFO log mentioning standalone mode + return.
    No Sentry call (operator intent, not an alert)."""
    monkeypatch.setattr(shipper.settings, "environment", env)
    # central_url already "" from fixture

    with patch("app.shipper.sentry_sdk") as mock_sentry, caplog.at_level(
        logging.INFO, logger="tds.shipper"
    ):
        assert_shipper_ready()
        mock_sentry.capture_message.assert_not_called()

    assert any(
        "standalone mode" in rec.message for rec in caplog.records
    ), "Expected INFO log mentioning standalone mode"


# ---------------------------------------------------------------------------
# assert_shipper_ready — operator escape hatch (flag=False)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("env", ["staging", "production"])
def test_assert_ready_escape_hatch_warns_but_passes(
    monkeypatch, caplog, env
):
    """Non-local env + empty url + ``require_central_url=False``:
    legacy silent-disable is preserved BUT visibility is added (WARN
    log + Sentry capture_message at warning level). Operator chose
    this and accepts the consequences; we make sure they SEE the
    deferral in their logs / Sentry feed."""
    monkeypatch.setattr(shipper.settings, "environment", env)
    monkeypatch.setattr(shipper.settings, "require_central_url", False)
    # central_url already "" from fixture

    with patch("app.shipper.sentry_sdk") as mock_sentry, caplog.at_level(
        logging.WARNING, logger="tds.shipper"
    ):
        assert_shipper_ready()  # Must not raise
        mock_sentry.capture_message.assert_called_once()
        # Verify the Sentry call carries warning level (not fatal,
        # not error) — this is operator-intent, not an incident.
        _, kwargs = mock_sentry.capture_message.call_args
        assert kwargs.get("level") == "warning"

    assert any(
        "operator escape hatch" in rec.message.lower()
        or "TDS_REQUIRE_CENTRAL_URL=false" in rec.message
        for rec in caplog.records
    ), "Expected WARN log mentioning the escape-hatch flag"


# ---------------------------------------------------------------------------
# assert_shipper_ready — FATAL fail-closed branch (the audit case)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("env", ["staging", "production"])
def test_assert_ready_raises_when_url_empty_in_non_local_with_flag_true(
    monkeypatch, caplog, env
):
    """The CRITICAL F.29 case — closes the audit-2026-05-16
    silent-disable blast radius. Non-local env + empty url + flag=True
    → CRITICAL log + Sentry FATAL + ShipperDisabledError raised.
    """
    monkeypatch.setattr(shipper.settings, "environment", env)
    monkeypatch.setattr(shipper.settings, "require_central_url", True)
    # central_url already "" from fixture

    with patch("app.shipper.sentry_sdk") as mock_sentry, caplog.at_level(
        logging.CRITICAL, logger="tds.shipper"
    ):
        with pytest.raises(ShipperDisabledError) as exc:
            assert_shipper_ready()

        # Sentry MUST be notified at fatal level — alert rules in
        # Sprint 4.1 will key off this exact level value to page the
        # operator out-of-hours.
        mock_sentry.capture_message.assert_called_once()
        _, kwargs = mock_sentry.capture_message.call_args
        assert kwargs.get("level") == "fatal"

    msg = str(exc.value)
    assert "TDS_CENTRAL_URL" in msg
    # The error must hand the operator the rollback flag explicitly
    # so they don't waste minutes hunting source mid-incident.
    assert "TDS_REQUIRE_CENTRAL_URL" in msg
    # Anchor to the historical incident date so post-incident review
    # has a single grep target.
    assert "2026-05-16" in msg
    # The CRITICAL log must echo the misconfig — at least one CRITICAL
    # record mentioning the env.
    assert any(
        env in rec.message for rec in caplog.records
        if rec.levelno >= logging.CRITICAL
    ), f"Expected CRITICAL log mentioning env={env}"


# ---------------------------------------------------------------------------
# run_shipper — wires assert_shipper_ready and exits cleanly
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_shipper_returns_immediately_in_local_env(monkeypatch):
    """In local env with empty central_url, run_shipper must return
    without touching Redis. Pre-F.29 the function returned after a
    single ``logger.info`` — F.29 preserves that local-env behaviour
    while replacing the silent-disable footgun for non-local."""
    monkeypatch.setattr(shipper.settings, "environment", "local")
    # central_url already "" from fixture

    redis_mock = MagicMock()
    redis_mock.xgroup_create = AsyncMock()
    redis_mock.xreadgroup = AsyncMock()

    # Coroutine must complete (return) within a short window. If it
    # tries to enter the main loop it will block on xreadgroup.
    await asyncio.wait_for(run_shipper(redis_mock), timeout=1.0)

    redis_mock.xgroup_create.assert_not_called()
    redis_mock.xreadgroup.assert_not_called()


@pytest.mark.asyncio
async def test_run_shipper_raises_on_non_local_empty_url(monkeypatch):
    """Defense in depth — run_shipper re-asserts before entering the
    loop. A coroutine invocation that bypassed lifespan validation
    (e.g., a unit test that calls ``asyncio.create_task(run_shipper)``
    directly with mutated settings) still raises ShipperDisabledError.
    """
    monkeypatch.setattr(shipper.settings, "environment", "staging")
    monkeypatch.setattr(shipper.settings, "require_central_url", True)
    # central_url already "" from fixture

    redis_mock = MagicMock()

    with patch("app.shipper.sentry_sdk"):  # Suppress Sentry side effect
        with pytest.raises(ShipperDisabledError):
            await run_shipper(redis_mock)


# ---------------------------------------------------------------------------
# F.29 Sprint 1.6 — try/finally guarantee that mark_stopped() always
# fires on run_shipper exit. Caught by Agent 3 validation 2026-05-23.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_shipper_marks_stopped_on_cancellation(monkeypatch):
    """G5 critical fix verification. Pre-Sprint-1.6 there was NO
    finally block around the main loop — task cancellation left
    ``shipper_metrics.running=True`` forever. /health then reported
    the shipper as alive while the task was actually dead — exactly
    the audit-2026-05-16 silent-disable pathology that G5 was built
    to surface.

    Post-Sprint-1.6: the loop is wrapped in try/finally so cancellation
    (graceful lifespan shutdown OR test-driven cancel) propagates
    CancelledError through finally → mark_stopped() fires →
    ``shipper_metrics.running`` flips to False BEFORE the task
    coroutine fully unwinds.
    """
    import asyncio

    from app import shipper_metrics as shipper_metrics_module
    shipper_metrics_module._reset_for_tests()

    # Configure a valid central_url so the loop actually enters; in
    # local env so we don't trip the fail-closed guard.
    monkeypatch.setattr(shipper.settings, "environment", "local")
    monkeypatch.setattr(shipper.settings, "central_url", "http://test:8200")
    monkeypatch.setattr(shipper.settings, "central_api_key", "key")

    # Redis mock: xgroup_create succeeds, xreadgroup blocks forever
    # so the loop is "running" until we cancel it.
    redis_mock = MagicMock()
    redis_mock.xgroup_create = AsyncMock()

    blocked = asyncio.Event()
    async def _block_forever(*args, **kwargs):
        blocked.set()
        await asyncio.sleep(3600)  # blocks until cancelled
    redis_mock.xreadgroup = AsyncMock(side_effect=_block_forever)

    task = asyncio.create_task(run_shipper(redis_mock))

    # Wait until the shipper has flipped running=True and is parked
    # inside xreadgroup. Short timeout — if we don't reach this state
    # quickly the test is genuinely broken.
    await asyncio.wait_for(blocked.wait(), timeout=1.0)
    assert shipper_metrics_module.metrics.running is True, (
        "Pre-cancel state: shipper should report running=True after "
        "mark_running() fires in the loop entry."
    )

    # Cancel and await — finally block must run.
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # The critical G5 invariant: after cancellation, /health must
    # reflect the shipper as STOPPED. Pre-Sprint-1.6 this was True
    # — the bug Agent 3 caught.
    assert shipper_metrics_module.metrics.running is False, (
        "G5 promise broken: after task cancellation, shipper_metrics"
        ".running is still True. /health would lie about shipper "
        "liveness — exactly the audit-2026-05-16 pathology."
    )


@pytest.mark.asyncio
async def test_run_shipper_marks_stopped_on_unexpected_crash(monkeypatch):
    """Sister test to cancellation. An unexpected exception that
    escapes the outer catch-all (rare but possible — e.g., asyncio
    primitive errors that don't go through except blocks) must still
    trigger mark_stopped() via the finally clause.

    We inject a BaseException subclass (which the catch-all
    ``except Exception`` does NOT catch) so the loop's catch-all is
    bypassed and we exercise the FINALLY path specifically.
    """
    import asyncio

    from app import shipper_metrics as shipper_metrics_module
    shipper_metrics_module._reset_for_tests()

    monkeypatch.setattr(shipper.settings, "environment", "local")
    monkeypatch.setattr(shipper.settings, "central_url", "http://test:8200")

    class _CrashBaseException(BaseException):
        """Subclass of BaseException, not Exception — bypasses the
        loop's `except Exception` catch-all."""

    redis_mock = MagicMock()
    redis_mock.xgroup_create = AsyncMock()
    redis_mock.xreadgroup = AsyncMock(
        side_effect=_CrashBaseException("simulated crash"),
    )

    with pytest.raises(_CrashBaseException):
        await run_shipper(redis_mock)

    assert shipper_metrics_module.metrics.running is False, (
        "G5 invariant broken: unexpected crash (BaseException) left "
        "running=True. The finally clause should fire regardless of "
        "exception class."
    )
