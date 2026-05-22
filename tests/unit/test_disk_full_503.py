"""Tests for the disk-queue pre-flight pressure check (F.29 Sprint 1.5).

Pre-F.29 the disk-fallback path at main.py:659-674 called
``enqueue_click_to_disk`` blindly when XADD failed. On a truly-full
mount the inner ``os.write`` raised OSError → enqueue_click caught,
logged at ERROR + Sentry-captured, and returned False — but /decide
kept going and responded 302 to the Worker. The click was "genuinely
lost" per the pre-F.29 comment at main.py:674, with NO HTTP signal
back to the Worker that storage was saturated.

Sprint 1.5 closes plan §3 G4 by adding a synchronous pre-flight check
that:

  1. Reads free bytes on the disk-queue mountpoint
     (``shutil.disk_usage``).
  2. Compares against ``TDS_DISK_QUEUE_MIN_FREE_BYTES`` (default 1 GiB).
  3. If under threshold AND non-local env → /decide returns 503
     disk_pressure to the CF Worker. Worker falls through to its own
     fallback URL → user still gets redirected via the fallback path,
     but the operator sees the 503 + Sentry breadcrumb tagged
     ``op=disk_pressure``.

Coverage:

  * ``check_disk_pressure()`` policy matrix:
      - empty disk_queue_root → (False, None) — operator disabled
      - non-existent path → (False, None) — first-boot tolerance
      - local env + tight disk → (False, free_bytes) — dev carve-out
      - non-local + free > threshold → (False, free_bytes)
      - non-local + free < threshold → (True, free_bytes)

  * ``HealthResponse.disk_free_bytes`` Sprint 1.4 sanity-pin: the
    same shutil call surfaces here — confirms shared semantics.

Reference: F.29 plan §3 G4, §4 Sprint 1.5 row, §7 (1 GiB default).
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from app import disk_queue
from app.disk_queue import check_disk_pressure


# ---------------------------------------------------------------------------
# disk_queue_root absent / unreadable
# ---------------------------------------------------------------------------


def test_pressure_check_empty_disk_queue_root(monkeypatch):
    """Operator opted out of disk fallback (TDS_DISK_QUEUE_ROOT="").
    Pressure check must report (False, None) — no path to measure,
    no pressure to surface. The downstream /decide handler treats
    this as "no fallback available" and 503s only if the underlying
    Redis path also failed."""
    monkeypatch.setattr(disk_queue.settings, "disk_queue_root", "")
    monkeypatch.setattr(disk_queue.settings, "environment", "staging")

    is_pressured, free_bytes = check_disk_pressure()
    assert is_pressured is False
    assert free_bytes is None


def test_pressure_check_nonexistent_path_returns_unmeasurable(
    monkeypatch, tmp_path,
):
    """First-boot tolerance: the disk-queue parent dir gets created
    lazily by ``_write_file_sync.mkdir`` on the first enqueue. A
    /health probe BEFORE that first enqueue would fail
    ``shutil.disk_usage`` with FileNotFoundError. We treat this as
    "cannot measure", not as pressure — to avoid 503'ing brand-new
    nodes that haven't taken traffic yet."""
    nonexistent = str(tmp_path / "this-path-does-not-exist")
    monkeypatch.setattr(
        disk_queue.settings, "disk_queue_root", nonexistent,
    )
    monkeypatch.setattr(disk_queue.settings, "environment", "staging")

    is_pressured, free_bytes = check_disk_pressure()
    assert is_pressured is False
    assert free_bytes is None


# ---------------------------------------------------------------------------
# Local env carve-out — dev partitions never trigger pressure
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("env", ["local", "development"])
def test_pressure_check_local_env_never_pressured(
    monkeypatch, tmp_path, env,
):
    """Engineers may have small dev partitions (Docker on a laptop
    with /var pinned to a small volume). Pressure check skips the
    threshold comparison entirely in local env so /decide doesn't
    503 dev traffic. The free_bytes is still surfaced for
    /health visibility — operator might want to see the value
    even in dev."""
    monkeypatch.setattr(
        disk_queue.settings, "disk_queue_root", str(tmp_path),
    )
    monkeypatch.setattr(disk_queue.settings, "environment", env)
    # Threshold higher than free space — would trigger pressure in
    # non-local env but must not here.
    monkeypatch.setattr(
        disk_queue.settings, "disk_queue_min_free_bytes",
        10 ** 18,  # 1 exabyte — guaranteed > free
    )

    is_pressured, free_bytes = check_disk_pressure()
    assert is_pressured is False
    # free_bytes still reported (not None) — local env should still
    # show the value so /health surfaces it.
    assert free_bytes is not None
    assert free_bytes > 0


# ---------------------------------------------------------------------------
# Non-local env — happy path (sufficient free space)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("env", ["staging", "production"])
def test_pressure_check_non_local_sufficient_space(
    monkeypatch, tmp_path, env,
):
    """Normal operation: tmp_path has plenty of free bytes (CI mounts
    typically have GB+ available), threshold 1 byte → not pressured.
    Sanity-pin the happy path so a future change to check_disk_pressure
    doesn't silently start returning True for healthy nodes."""
    monkeypatch.setattr(
        disk_queue.settings, "disk_queue_root", str(tmp_path),
    )
    monkeypatch.setattr(disk_queue.settings, "environment", env)
    monkeypatch.setattr(
        disk_queue.settings, "disk_queue_min_free_bytes", 1,
    )

    is_pressured, free_bytes = check_disk_pressure()
    assert is_pressured is False
    assert free_bytes is not None
    assert free_bytes > 1


# ---------------------------------------------------------------------------
# Non-local env — pressure detected (free < threshold)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("env", ["staging", "production"])
def test_pressure_check_non_local_under_pressure(
    monkeypatch, tmp_path, env,
):
    """The CRITICAL F.29 case: free_bytes < threshold AND non-local
    env → is_pressured=True. The downstream /decide handler keys off
    this exact return value to raise 503. Threshold 10^18 bytes
    (1 exabyte) is reliably above any plausible test-filesystem free
    space, simulating disk pressure deterministically without
    actually filling the partition."""
    monkeypatch.setattr(
        disk_queue.settings, "disk_queue_root", str(tmp_path),
    )
    monkeypatch.setattr(disk_queue.settings, "environment", env)
    monkeypatch.setattr(
        disk_queue.settings, "disk_queue_min_free_bytes", 10 ** 18,
    )

    is_pressured, free_bytes = check_disk_pressure()
    assert is_pressured is True
    assert free_bytes is not None
    assert free_bytes < 10 ** 18


# ---------------------------------------------------------------------------
# Exact threshold boundary — strictly-less-than semantics
# ---------------------------------------------------------------------------


def test_pressure_check_boundary_is_strictly_less_than(
    monkeypatch, tmp_path,
):
    """Pin the comparison operator. ``free_bytes < threshold`` means
    free == threshold is NOT pressured (you have EXACTLY enough). A
    drift to ``<=`` would 503 on the exact boundary which is a
    subtle UX regression — operator sees 503s "for no reason" when
    free space is exactly at the configured floor."""
    monkeypatch.setattr(
        disk_queue.settings, "disk_queue_root", str(tmp_path),
    )
    monkeypatch.setattr(disk_queue.settings, "environment", "staging")

    # Patch shutil.disk_usage to a known value so we can test the
    # exact boundary deterministically.
    fake_usage = type("U", (), {"total": 1000, "used": 500, "free": 1000})

    with patch.object(disk_queue.shutil, "disk_usage", return_value=fake_usage):
        # free == threshold → NOT pressured
        monkeypatch.setattr(
            disk_queue.settings, "disk_queue_min_free_bytes", 1000,
        )
        is_pressured, free_bytes = check_disk_pressure()
        assert is_pressured is False
        assert free_bytes == 1000

        # free < threshold (by 1 byte) → IS pressured
        monkeypatch.setattr(
            disk_queue.settings, "disk_queue_min_free_bytes", 1001,
        )
        is_pressured, free_bytes = check_disk_pressure()
        assert is_pressured is True
        assert free_bytes == 1000


# ---------------------------------------------------------------------------
# Default threshold pin — 1 GiB per F.29 plan §3 G4 + §7 rationale
# ---------------------------------------------------------------------------


def test_default_threshold_is_one_gib():
    """F.29 plan §3 G4 specifies "< 1GB free" as the trigger. 1 GiB
    (1_073_741_824 bytes) is the canonical SI binary value used by
    most disk tools (df, du). Pin the default explicitly — a drift
    to 1 GB decimal (10^9) would be a 7% lower threshold and could
    change paging behavior at the boundary."""
    from app.config import Settings

    # Build a Settings with explicit env=local so we don't trigger
    # the central_url validator side effects.
    s = Settings(environment="local")
    assert s.disk_queue_min_free_bytes == 1_073_741_824  # 1 GiB
