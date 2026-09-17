"""N1 — the per-principal issuance budget.

Every test here names the property it pins, because the finding this closes was
phrased three times and measured wrong each time, and a test whose name says
only "test_charge" would not have caught any of them.

🔴 The load-bearing one is `test_the_budget_is_CODES_not_REQUESTS`: a request
quota looks identical to a capability quota on every single-code call, and
diverges only on the wall, which is the path that actually mints in bulk.
"""
from __future__ import annotations

import pytest

from app import mint_quota
from app.config import settings


class _FakeRedis:
    """Counts like Redis does, and records what it was asked."""

    def __init__(self):
        self.store: dict[str, int] = {}
        self.expires: dict[str, int] = {}
        self.calls: list[tuple] = []

    async def incrby(self, key, amount):
        self.calls.append(("incrby", key, amount))
        self.store[key] = self.store.get(key, 0) + amount
        return self.store[key]

    async def expire(self, key, seconds):
        self.calls.append(("expire", key, seconds))
        self.expires[key] = seconds
        return True


class _ExplodingRedis:
    """Stands in for a Redis that cannot answer."""

    async def incrby(self, key, amount):
        raise RuntimeError("redis is down")

    async def expire(self, key, seconds):  # pragma: no cover - never reached
        raise RuntimeError("redis is down")


class _ForbiddenRedis:
    """Any use at all is a failure — used to prove the OFF path is inert."""

    async def incrby(self, key, amount):  # pragma: no cover
        raise AssertionError("a DISABLED quota must not touch Redis")

    async def expire(self, key, seconds):  # pragma: no cover
        raise AssertionError("a DISABLED quota must not touch Redis")


@pytest.fixture
def fake_redis(monkeypatch):
    r = _FakeRedis()

    async def _get():
        return r

    monkeypatch.setattr(mint_quota, "get_redis", _get)
    return r


@pytest.fixture
def armed(monkeypatch):
    """Arm both budgets at a small cap so the tests are legible."""
    monkeypatch.setattr(settings, "mint_quota_preview_codes_per_window", 5)
    monkeypatch.setattr(settings, "mint_quota_wall_codes_per_window", 5)
    monkeypatch.setattr(settings, "mint_quota_window_seconds", 60)


class TestOffIsByteIdentical:
    """A cap of 0 means the node behaves as if this module did not exist."""

    @pytest.mark.asyncio
    async def test_a_disabled_preview_budget_touches_no_redis(self, monkeypatch):
        monkeypatch.setattr(settings, "mint_quota_preview_codes_per_window", 0)

        async def _get():
            return _ForbiddenRedis()

        monkeypatch.setattr(mint_quota, "get_redis", _get)
        await mint_quota.charge_preview("a" * 64)  # must not raise

    @pytest.mark.asyncio
    async def test_a_disabled_wall_budget_touches_no_redis(self, monkeypatch):
        monkeypatch.setattr(settings, "mint_quota_wall_codes_per_window", 0)

        async def _get():
            return _ForbiddenRedis()

        monkeypatch.setattr(mint_quota, "get_redis", _get)
        await mint_quota.charge_wall(347, 24)  # must not raise


class TestTheBudgetIsSpentAndThenRefuses:
    @pytest.mark.asyncio
    async def test_under_the_cap_is_allowed(self, fake_redis, armed):
        for _ in range(5):
            await mint_quota.charge_preview("a" * 64)

    @pytest.mark.asyncio
    async def test_the_charge_AT_the_cap_is_still_allowed(self, fake_redis, armed):
        await mint_quota.charge_preview("a" * 64, units=5)

    @pytest.mark.asyncio
    async def test_one_over_the_cap_refuses(self, fake_redis, armed):
        await mint_quota.charge_preview("a" * 64, units=5)
        with pytest.raises(mint_quota.MintQuotaExceeded):
            await mint_quota.charge_preview("a" * 64)

    @pytest.mark.asyncio
    async def test_the_refusal_CARRIES_the_numbers(self, fake_redis, armed):
        with pytest.raises(mint_quota.MintQuotaExceeded) as caught:
            await mint_quota.charge_preview("a" * 64, units=99)
        exc = caught.value
        assert exc.cap == 5 and exc.window_seconds == 60 and exc.units == 99, (
            "a refusal that cannot say what it refused tells an operator nothing"
        )


class TestTheUnitIsCODES:
    """🔴 The property the whole design rests on."""

    @pytest.mark.asyncio
    async def test_the_budget_is_CODES_not_REQUESTS(self, fake_redis, armed):
        # ONE request, TEN codes, against a cap of five. A request-counting
        # quota would allow this — it is one request. A capability quota must
        # refuse it, and that difference IS the finding.
        with pytest.raises(mint_quota.MintQuotaExceeded):
            await mint_quota.charge_wall(347, 10)

    @pytest.mark.asyncio
    async def test_the_charge_reaches_redis_as_the_FULL_amount(
        self, fake_redis, armed,
    ):
        try:
            await mint_quota.charge_wall(347, 10)
        except mint_quota.MintQuotaExceeded:
            pass
        assert ("incrby", *fake_redis.calls[0][1:]) == fake_redis.calls[0]
        assert fake_redis.calls[0][2] == 10, (
            "the tile count must be charged as one INCRBY of N, not N of 1 — "
            "otherwise a batch can straddle a window boundary mid-charge"
        )


class TestTheKEYSPACESAreSeparate:
    @pytest.mark.asyncio
    async def test_two_credentials_do_not_share_a_budget(self, fake_redis, armed):
        await mint_quota.charge_preview("a" * 64, units=5)
        await mint_quota.charge_preview("b" * 64, units=5)  # must not raise

    @pytest.mark.asyncio
    async def test_two_campaigns_do_not_share_a_budget(self, fake_redis, armed):
        await mint_quota.charge_wall(347, 5)
        await mint_quota.charge_wall(397, 5)  # must not raise

    @pytest.mark.asyncio
    async def test_preview_and_wall_do_not_share_a_budget(self, fake_redis, armed):
        # The same principal STRING on both paths must not collide. Separate by
        # construction, for the reason D149 separated its own two budgets.
        await mint_quota.charge_preview("347", units=5)
        await mint_quota.charge_wall(347, 5)  # must not raise
        keys = {c[1] for c in fake_redis.calls if c[0] == "incrby"}
        assert len(keys) == 2, f"the two scopes collided: {keys}"


class TestFailurePosture:
    @pytest.mark.asyncio
    async def test_a_dead_redis_FAILS_CLOSED_for_the_mint(self, monkeypatch, armed):
        async def _get():
            return _ExplodingRedis()

        monkeypatch.setattr(mint_quota, "get_redis", _get)
        with pytest.raises(mint_quota.MintQuotaExceeded):
            await mint_quota.charge_preview("a" * 64)

    @pytest.mark.asyncio
    async def test_the_expire_is_issued_on_the_FIRST_charge_only(
        self, fake_redis, armed,
    ):
        await mint_quota.charge_preview("a" * 64)
        await mint_quota.charge_preview("a" * 64)
        expires = [c for c in fake_redis.calls if c[0] == "expire"]
        assert len(expires) == 1, (
            "one EXPIRE per window, on the first charge — and it must be "
            "issued, because a key that never expires refuses its principal "
            "FOREVER (the latent defect in admin-api's limiter)"
        )
        assert expires[0][2] == 60


class TestAPrincipalWeCannotNameIsNotCharged:
    @pytest.mark.asyncio
    async def test_no_credential_is_not_a_charge(self, monkeypatch, armed):
        async def _get():
            return _ForbiddenRedis()

        monkeypatch.setattr(mint_quota, "get_redis", _get)
        await mint_quota.charge_preview(None)
        await mint_quota.charge_preview("")

    @pytest.mark.asyncio
    async def test_no_campaign_is_not_a_charge(self, monkeypatch, armed):
        async def _get():
            return _ForbiddenRedis()

        monkeypatch.setattr(mint_quota, "get_redis", _get)
        await mint_quota.charge_wall(None, 5)
        await mint_quota.charge_wall(0, 5)

    @pytest.mark.asyncio
    async def test_zero_units_is_not_a_charge(self, monkeypatch, armed):
        async def _get():
            return _ForbiddenRedis()

        monkeypatch.setattr(mint_quota, "get_redis", _get)
        await mint_quota.charge_wall(347, 0)


class TestTheWindowIsFixedAndStated:
    def test_the_window_start_is_stable_within_a_window(self):
        # 1020 and 1079.9 are inside ONE 60s window; my first attempt used
        # 1000 and 1059.9, which straddle a boundary (1000 -> 960, 1059.9 ->
        # 1020). The test was wrong, not the code, and it is written down
        # because picking numbers that look adjacent is exactly how a window
        # test ends up asserting the opposite of what it claims.
        assert mint_quota._window_start(1020.0, 60) == mint_quota._window_start(
            1079.9, 60,
        )

    def test_the_window_start_MOVES_across_a_boundary(self):
        a = mint_quota._window_start(1079.9, 60)
        b = mint_quota._window_start(1080.1, 60)
        assert b > a, (
            "if the window never moved, a spent budget would never recover and "
            "the bound would be a permanent ban rather than a rate"
        )
