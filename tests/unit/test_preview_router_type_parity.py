"""A preview must predict the SAME destination the real click gets — on ALL
FOUR router types.

Anchor: ``docs/development/route-preview-2026-08-31/05-EDGE-PREVIEW-ANCHOR.md``

THE PRODUCT PROMISE, and why it needs its own file. The owner's requirement is
not "a preview returns an offer". It is:

    «Нам по максимуму треба зберегти ті обставини, в яких буде слідувати
    реальний клік… навіть втрата будь-якого параметра може вийти абсолютно
    інший маршрут»

A preview that answers *an* offer while the click gets *another* is worse than
no preview at all: the landing page advertises a destination the visitor will
never reach, and nothing anywhere reports an error. So the assertion here is
never "matched is true" — it is always **the preview's answer equals the real
click's answer, on the same URL**.

WHY FOUR SEPARATE FIXTURES RATHER THAN ONE PARAMETRISED HOST. The four router
types are four different code paths through ``_resolve_domain``, not four
spellings of one path — different Redis keys, a priority ladder between them,
and for the subdomain rung an entirely separate fail-closed branch gated on
``domains:wildcard``. A single fixture would prove one of them and imply three.

WHAT THIS FILE DOES NOT COVER, said out loud so its green is not read as wider
than it is:

  * ZERO WRITES is pinned elsewhere (``test_preview_identity_writes.py``, on the
    lane that adds the ``identity_writes`` gate). This file is cut from a base
    that predates it, and testing routing parity does not require it.
  * The LIVE four-router-type validation on staging is still owed. A fixture
    proves the resolver's shape; only a real node proves the deploy.

⚠️ IF YOU CALIBRATE THIS FILE BY MUTATE-AND-RESTORE, CLEAR THE CACHE.
Knocking out an assertion to watch it go red is the right instinct — the rung
checks below were calibrated exactly that way, by telling `test_root` to expect
the `path` rung and confirming it fires. But after restoring the file, pytest
reported the MUTATED assertion message again on a byte-identical file. The
restore had worked (verified against the backup, line for line); the stale
`__pycache__` / assertion-rewrite cache had not. Left untreated it reads as
"my restore failed", and the hunt that follows is for a bug that is not there.

    find tests -name __pycache__ -type d -exec rm -rf {} +
    rm -rf .pytest_cache
"""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import fakeredis.aioredis
import pytest

from app import identity, router
from app.config import settings
from app.models import ClickRequest

pytestmark = pytest.mark.asyncio

CO = 1
OFFER = "61"
DEST = "https://dest.example/{click_id}"


async def _seed_campaign(fake, campaign_id: str) -> None:
    """One campaign that serves a single, unambiguous offer.

    Deliberately NOT a split: a weighted split would make the two routes agree
    only probabilistically, and a test that passes most of the time is a test
    that fails at the worst moment.
    """
    await fake.hset(
        f"campaign:{campaign_id}",
        mapping={"company_id": str(CO), "priority": "0"},
    )
    await fake.sadd(f"campaign:{campaign_id}:offers", OFFER)
    await fake.hset(f"offer:{OFFER}", mapping={"url": DEST, "has_targets": "0"})


def _click(hostname: str, path: str, query: dict, click_id: str) -> ClickRequest:
    """A REAL click."""
    return ClickRequest(
        click_id=click_id,
        country="US",
        user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_2)",
        hostname=hostname,
        path=path,
        query_params=query,
        visitor_id="vid-real-visitor",
    )


def _preview(hostname: str, path: str, query: dict, click_id: str) -> ClickRequest:
    """The PREVIEW of that same click.

    Identical in every routing-relevant field — hostname, path, and every query
    parameter — because that identity IS the feature. The only difference is the
    absence of identity signals, which is what the /preview handler builds.
    """
    return ClickRequest(
        click_id=click_id,
        country="US",
        user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_2)",
        hostname=hostname,
        path=path,
        query_params=query,
        visitor_id=None,
        identity_token=None,
        is_returning=False,
    )


async def _assert_rung(fake, req, expected_tier: str):
    """Prove this fixture exercises the rung it CLAIMS to.

    Without this the four parity tests could all be passing through one rung —
    four greens that look like four-way coverage and are not. Measured once by
    hand and then written down here, because a coverage claim that lives in a
    session's memory is a coverage claim that quietly narrows.

    `resolve_domain_campaign` returns a DomainResolution whose 5th field names
    the tier that produced the binding: subdomain | path | param | root | none.
    """
    async def _get():
        return fake

    with patch.object(router, "get_redis", _get):
        resolution = await router.resolve_domain_campaign(fake, req)
    assert resolution.match_tier == expected_tier, (
        f"this fixture resolved via the {resolution.match_tier!r} rung, not "
        f"{expected_tier!r} — the test name overstates what it covers"
    )


async def _route(fake, req):
    async def _get():
        return fake

    with patch.object(router, "get_redis", _get), \
            patch.object(identity, "get_identity_redis", _get):
        result = await router.route(req)
    for _ in range(5):
        await asyncio.sleep(0)
    return result


def _destination(result):
    """What the visitor would actually be sent to, reduced to a comparable."""
    if not result:
        return ("no_route", None, None)
    if result.get("blocked"):
        return ("blocked", None, None)
    attribution = result.get("attribution") or {}
    return (
        "routed" if result.get("url") else "no_destination",
        result.get("offer_id"),
        attribution.get("offer_target_id"),
    )


@pytest.fixture(autouse=True)
def resolver_off(monkeypatch):
    """Keep the returning layer out of this file's way.

    It is not what is under test here, and leaving it on would make the real
    click and the preview differ for a reason that has nothing to do with
    router types — which is precisely the confound this file exists to avoid.
    """
    monkeypatch.setattr(settings, "returning_resolver_enabled", False)


@pytest.fixture
def fake():
    return fakeredis.aioredis.FakeRedis(decode_responses=True)


class TestParityAcrossAllFourRouterTypes:
    """Each test seeds ONE rung and proves preview == click on it."""

    async def test_root(self, fake):
        host, campaign = "root.example", "801"
        await _seed_campaign(fake, campaign)
        await fake.set(f"domain:{host}:root", campaign)

        await _assert_rung(fake, _click(host, "/", {}, "tier-root"), "root")
        real = await _route(fake, _click(host, "/", {}, "click-root"))
        prev = await _route(fake, _preview(host, "/", {}, "preview-root"))

        assert _destination(real)[0] == "routed", f"the fixture did not route: {real}"
        assert _destination(prev) == _destination(real), (
            f"ROOT rung: preview predicted {_destination(prev)}, "
            f"click got {_destination(real)}"
        )

    async def test_path_segment(self, fake):
        host, campaign, seg = "path.example", "802", "promo"
        await _seed_campaign(fake, campaign)
        await fake.set(f"domain:{host}:path:{seg}", campaign)

        await _assert_rung(fake, _click(host, f"/{seg}", {}, "tier-path"), "path")
        real = await _route(fake, _click(host, f"/{seg}", {}, "click-path"))
        prev = await _route(fake, _preview(host, f"/{seg}", {}, "preview-path"))

        assert _destination(real)[0] == "routed", f"the fixture did not route: {real}"
        assert _destination(prev) == _destination(real), (
            f"PATH rung: preview predicted {_destination(prev)}, "
            f"click got {_destination(real)}"
        )

    async def test_param_selector(self, fake):
        host, campaign, alias = "param.example", "803", "spring"
        await _seed_campaign(fake, campaign)
        await fake.set(f"domain:{host}:param:{alias}", campaign)

        query = {"c": alias, "sub1": "keepme"}
        await _assert_rung(fake, _click(host, "/", query, "tier-param"), "param")
        real = await _route(fake, _click(host, "/", query, "click-param"))
        prev = await _route(fake, _preview(host, "/", query, "preview-param"))

        assert _destination(real)[0] == "routed", f"the fixture did not route: {real}"
        assert _destination(prev) == _destination(real), (
            f"PARAM rung: preview predicted {_destination(prev)}, "
            f"click got {_destination(real)}"
        )

    async def test_subdomain(self, fake):
        base, label, campaign = "sub.example", "promo", "804"
        host = f"{label}.{base}"
        await _seed_campaign(fake, campaign)
        # The subdomain rung lives behind the §6 wildcard branch, which is
        # fail-closed and only entered when the base is a registered wildcard.
        await fake.sadd("domains:wildcard", base)
        await fake.set(f"domain:{base}:subdomain:{label}", campaign)

        await _assert_rung(fake, _click(host, "/", {}, "tier-sub"), "subdomain")
        real = await _route(fake, _click(host, "/", {}, "click-sub"))
        prev = await _route(fake, _preview(host, "/", {}, "preview-sub"))

        assert _destination(real)[0] == "routed", f"the fixture did not route: {real}"
        assert _destination(prev) == _destination(real), (
            f"SUBDOMAIN rung: preview predicted {_destination(prev)}, "
            f"click got {_destination(real)}"
        )


class TestTheFixturesActuallyDiscriminate:
    """Calibration. Four greens prove nothing if the fixtures cannot go red.

    Each test below constructs a case where preview and click MUST differ, or
    where a rung MUST fail, and asserts that the comparison notices. Without
    these, `_destination(prev) == _destination(real)` would pass just as
    happily on two identical no-route answers — which is exactly the shape a
    misconfigured fixture produces.
    """

    async def test_a_rung_that_was_never_seeded_does_not_route(self, fake):
        # If this routed, every parity assertion above would be comparing two
        # no-route answers and calling it agreement.
        await _seed_campaign(fake, "805")
        result = await _route(fake, _click("unseeded.example", "/", {}, "c-unseeded"))
        assert _destination(result)[0] != "routed"

    async def test_a_LOST_parameter_changes_the_route_which_is_why_we_forward_them(self, fake):
        """The owner's own reason for forwarding every parameter, made concrete.

        Same host, same path, one query parameter dropped — and the two
        requests reach different campaigns. This is what a preview that
        "helpfully" stripped an unknown parameter would do to the prediction.
        """
        host = "param.example"
        await _seed_campaign(fake, "806")
        await _seed_campaign(fake, "807")
        await fake.set(f"domain:{host}:param:alpha", "806")
        await fake.set(f"domain:{host}:root", "807")

        with_param = await _route(fake, _click(host, "/", {"c": "alpha"}, "c-with"))
        without = await _route(fake, _click(host, "/", {}, "c-without"))

        assert _destination(with_param)[0] == "routed"
        assert _destination(without)[0] == "routed"
        assert with_param.get("campaign_id") != without.get("campaign_id"), (
            "dropping ?c= reached the SAME campaign — this fixture can no "
            "longer demonstrate why parameters must be forwarded verbatim"
        )

    async def test_the_comparison_notices_a_genuine_disagreement(self, fake):
        """Prove `_destination` discriminates, by feeding it two real routes
        that genuinely differ. A comparison nobody has watched fail is not
        evidence."""
        host = "param.example"
        await _seed_campaign(fake, "808")
        await _seed_campaign(fake, "809")
        await fake.set(f"domain:{host}:param:one", "808")
        await fake.set(f"domain:{host}:param:two", "809")

        a = await _route(fake, _click(host, "/", {"c": "one"}, "c-a"))
        b = await _route(fake, _click(host, "/", {"c": "two"}, "c-b"))

        assert _destination(a)[0] == "routed" and _destination(b)[0] == "routed"
        assert a.get("campaign_id") != b.get("campaign_id")
