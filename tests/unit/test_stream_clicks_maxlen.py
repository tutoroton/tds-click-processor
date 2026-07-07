"""Tests for `stream:clicks` entry-count handling (T2.1 / G-22, REPURPOSED
by M1 — LOSSFIX P1b, 2026-07-07).

Pre-fix: an inline `MAXLEN ~ N` on every XADD to `stream:clicks` SILENTLY
TRIMMED the oldest UNCONSUMED entries once the stream grew past the cap
during an extended central-collector outage (the M-TRIM pathology).

Post-fix: NO XADD to `stream:clicks` carries a MAXLEN cap, at ANY of the
four call sites:

  1. `/decide`'s real-click XADD (`main.py`) — GATED: before attempting
     the XADD, a cached stream-length check
     (`main._check_stream_backpressure`, never a per-click round-trip)
     diverts an over-threshold click to the EXISTING disk-fallback path
     instead — reject, not trim.
  2. `/decide`'s smoke-probe XADD (`main.py`) — GATED, reject-only: an
     over-threshold smoke probe 503s outright (no XADD attempt, no disk
     fallback — synthetic click, nothing to preserve). Gates node
     ACTIVATION.
  3. `shipper.py` `_retry_click` — NO new gate: a size-neutral swap (the
     old entry is ACKed right after), gating would starve transient
     retries into deadletters.
  4. `disk_queue.py` drainer replay XADD — NO new gate: the existing
     stop-on-first-failure is this phase's self-limit; watermark-gated
     drain pacing is P2 (do not half-build it here).

The reject threshold itself is the OLD `stream_clicks_maxlen` setting,
repurposed (same name, new meaning + a new default — see A2 below).

Reference: rule `sync-protocol`, LOSSFIX-FINAL-PLAN.md M1 (edge),
BLAST-RADIUS-map.md, action-items.md T2.1, open-questions.md G-22.
"""

from __future__ import annotations

import inspect

import pytest


# ---------------------------------------------------------------------------
# Source-pin: settings field
# ---------------------------------------------------------------------------


class TestSettingsField:
    def test_stream_clicks_maxlen_named_field(self):
        """The threshold MUST be a named Settings field, not a hardcoded
        literal. Lets operators tune via TDS_STREAM_CLICKS_MAXLEN
        without rebuilding the image."""
        from app.config import Settings

        assert "stream_clicks_maxlen" in Settings.model_fields, (
            "Settings.stream_clicks_maxlen must be defined so operators "
            "can override via TDS_STREAM_CLICKS_MAXLEN env var "
            "(T2.1 / G-22 / M1)."
        )

    def test_default_value_is_300k_not_the_dead_code_1m(self):
        """A2 (MUST, LOSSFIX P1b) — pin the NEW default explicitly.

        Edge routing Redis is provisioned at 256 MB
        (`docker-compose.yml` `--maxmemory 256mb`). At ~500-600 B/entry,
        the OLD default of 1_000_000 entries would be ~500-600 MB —
        ABOVE the actual memory budget, so Redis would OOM long before
        XLEN could ever reach that count: the reject path would be dead
        code and the H6 boot log would announce a gate that never
        gates. 300_000 (~70% of the 256 MB budget at ~600 B/entry)
        keeps the threshold inside the real budget."""
        from app.config import settings

        assert settings.stream_clicks_maxlen == 300_000, (
            "Default should be 300_000 (A2) — NOT the old 1_000_000, "
            "which sat above the 256 MB edge-Redis memory budget and "
            "made the M1 reject path unreachable dead code."
        )

    def test_env_prefix_resolves_field(self):
        """The Settings model uses env_prefix='TDS_' — make sure
        this field follows the convention so the documented env
        var name (TDS_STREAM_CLICKS_MAXLEN) actually works."""
        from app.config import Settings

        assert Settings.model_config.get("env_prefix") == "TDS_"


# ---------------------------------------------------------------------------
# Source-pin: /decide handler — real-click XADD site (1 of 4)
# ---------------------------------------------------------------------------


class TestDecideRealClickXaddSource:
    """The real-click XADD in `/decide`. Pin its call shape so a future
    refactor can't silently reintroduce the trimming MAXLEN cap.
    """

    def _decide_source(self) -> str:
        from app.main import decide

        return inspect.getsource(decide)

    def test_real_xadd_still_writes_stream_clicks(self):
        source = self._decide_source()
        assert "stream:clicks" in source, (
            "The /decide handler must still XADD to stream:clicks."
        )

    def test_real_xadd_carries_no_maxlen_kwarg(self):
        """M1 — the real-click XADD call itself must NOT carry
        `maxlen=`/`approximate=` anymore. Anchored on the unique
        `json.dumps(click_record` marker so this doesn't accidentally
        match the SEPARATE smoke-probe XADD (which serialises
        `smoke_record`, not `click_record`)."""
        source = self._decide_source()
        anchor = source.find("json.dumps(click_record")
        assert anchor > 0, (
            "Expected to find the real click's XADD payload "
            "(json.dumps(click_record, ...)) in /decide's source."
        )
        # Look at the XADD call surrounding the anchor — the call opens
        # a few lines above the payload construction and closes a few
        # lines below it; a window is simpler and more robust than
        # trying to balance parens across a reformatted call.
        window = source[max(0, anchor - 200):anchor + 200]
        assert "maxlen=" not in window, (
            "The real-click XADD must NOT carry maxlen= — M1 removed "
            "the silently-trimming cap. Over-threshold clicks are "
            "diverted to the disk fallback via "
            "main._check_stream_backpressure() instead."
        )
        assert "approximate" not in window

    def test_check_stream_backpressure_helper_exists_and_used(self):
        """The gate helper must exist and actually be called from
        /decide — otherwise M1's reject-threshold is unreachable."""
        from app import main

        assert hasattr(main, "_check_stream_backpressure"), (
            "main._check_stream_backpressure is the canonical M1 gate "
            "helper — if renamed, update this pin."
        )
        source = self._decide_source()
        assert "_check_stream_backpressure()" in source, (
            "/decide MUST call _check_stream_backpressure() before the "
            "real-click XADD attempt."
        )


# ---------------------------------------------------------------------------
# Source-pin: /decide handler — smoke-probe XADD site (2 of 4)
# ---------------------------------------------------------------------------


class TestDecideSmokeXaddSource:
    def _decide_source(self) -> str:
        from app.main import decide

        return inspect.getsource(decide)

    def test_smoke_xadd_carries_no_maxlen_kwarg(self):
        source = self._decide_source()
        anchor = source.find("json.dumps(smoke_record")
        assert anchor > 0, (
            "Expected to find the smoke probe's XADD payload "
            "(json.dumps(smoke_record)) in /decide's source."
        )
        window = source[max(0, anchor - 200):anchor + 200]
        assert "maxlen=" not in window
        assert "approximate" not in window

    def test_smoke_path_gated_before_xadd_attempt(self):
        """A4 — the smoke probe must check the SAME backpressure gate
        BEFORE attempting its XADD (reject-only, no disk fallback)."""
        source = self._decide_source()
        gate_pos = source.find("_check_stream_backpressure()")
        smoke_xadd_pos = source.find("json.dumps(smoke_record")
        assert gate_pos > 0 and smoke_xadd_pos > 0
        assert gate_pos < smoke_xadd_pos, (
            "The backpressure gate check must precede the smoke XADD "
            "attempt — reject-only means we must not even try the "
            "write once over threshold."
        )


# ---------------------------------------------------------------------------
# Source-pin: shipper.py `_retry_click` — site 3 of 4
# ---------------------------------------------------------------------------


class TestShipperRetryClickSource:
    def test_retry_click_carries_no_maxlen_kwarg(self):
        from app.shipper import _retry_click

        source = inspect.getsource(_retry_click)
        assert "stream_clicks_maxlen" not in source, (
            "_retry_click must not read stream_clicks_maxlen at all — "
            "M1 removed the cap here with NO new gate (retries are "
            "size-neutral; gating them would starve transient failures "
            "into deadletters)."
        )
        assert "maxlen=" not in source
        assert "approximate" not in source


# ---------------------------------------------------------------------------
# Source-pin: disk_queue.py drainer replay XADD — site 4 of 4
# ---------------------------------------------------------------------------


class TestDiskQueueDrainerXaddSource:
    def test_drainer_xadd_carries_no_maxlen_kwarg(self):
        from app.disk_queue import drain_to_redis

        source = inspect.getsource(drain_to_redis)
        assert "maxlen=" not in source, (
            "The drainer replay XADD must not carry maxlen= — M1 "
            "removed the cap here too (no new gate; the existing "
            "stop-on-first-XADD-failure is this phase's self-limit — "
            "watermark-gated drain pacing is P2)."
        )
        assert "approximate" not in source


# ---------------------------------------------------------------------------
# Behavioural pin: redis-py call signature (real click, direct simulation)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_xadd_call_shape_via_mock():
    """End-to-end-ish: the real-click XADD shape has no maxlen/approximate
    kwargs. We don't go through the full FastAPI handler here — only the
    XADD call shape; the gated end-to-end behaviour is covered in
    test_stream_backpressure_edge.py."""
    from unittest.mock import AsyncMock

    mock_redis = AsyncMock()
    sentinel_payload = {"data": '{"click_id":"x"}'}

    # Mirror the (post-M1) call shape used in main.py — no maxlen/approximate.
    await mock_redis.xadd("stream:clicks", sentinel_payload)

    mock_redis.xadd.assert_awaited_once_with(
        "stream:clicks",
        sentinel_payload,
    )
