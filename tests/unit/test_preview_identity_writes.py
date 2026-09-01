"""EPV-DEFECT-1 — a route PREVIEW must not mint or stamp persistent identity.

Decision: ``.roadmap/decisions/ADR-0468-route-preview-identity-neutralisation-
gates-the-resolver-block-not-the-resolve-call-line.md``
Programme: ``docs/development/route-preview-2026-08-31/05-EDGE-PREVIEW-ANCHOR.md`` §8

WHAT WENT WRONG, so these tests are readable as a record and not just as asserts.

``/preview`` blanked ``visitor_id`` and ``identity_token`` and its docstring called
that a complete zero-writes proof. There are THREE identity signals. The third is
``funnel_user_id``, and it arrives inside ``query_params`` — the one field a preview
MUST forward verbatim, because a lost parameter can change the route. Worse, it needs
no operator configuration whatsoever: the canonical-binding rule (``resolution.py:28``)
makes every ``CANONICAL_SLOTS`` name a primary input key, so a bare
``?funnel_user_id=`` binds on any campaign of any trusted source.

TWO WRITE PATHS, which is why the fix gates the resolver BLOCK and not the
``resolve_and_stamp`` call. A domain-matched click resolves with
``commit_identity=False``, stashes the signals, and ``_commit_deferred_identity``
writes them the moment the campaign serves. A fix at the call line alone would ship
green and leave the defect live — so the fixture below deliberately uses a campaign
that SERVES on the domain rung, exercising the deferred path rather than the inline one.

THE DETECTOR, and why it is shaped this way. Identity gets its OWN fakeredis,
separate from routing, and it starts EMPTY. Then "did anything write?" is not a
guess about which key names matter — **any key at all in that store is a write**.
This is deliberate: ``click-processor/CLAUDE.md`` hazard 9 records a detector wired
to the ROUTING pool alone going 15/15 green while identity writes flowed. A detector
pointed at the wrong pool is not a weak detector, it is a blind one.

``TestTheDetectorItself`` is not decoration. A green we have never seen fail proves
nothing, so it drives a real click through the same rig and proves the detector
records writes — and then reproduces the defect by asking for exactly the behaviour
the fix removed, proving the guard can still go red.
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

CAMP = "77"
OFFER = "31"
HOST = "preview.test"       # 2-label → non-wildcard host
PARAM_C = "epv"             # domain binding selector (?c=epv)
SOURCE_ID = "9"
SOURCE_SLUG = "trusted-src"
CO = 1
FUID = "funnel-user-abc123"


async def _seed(routing) -> None:
    """A domain-matched campaign that SERVES, reached through a TRUSTED source.

    Both halves are load-bearing. It must SERVE (not fall through) so the
    deferred-commit path runs, and the source must be trusted or the funnel
    signal is dropped before it ever reaches the resolver — the test would then
    pass for the wrong reason.
    """
    await routing.set(f"domain:{HOST}:param:{PARAM_C}", CAMP)
    await routing.hset(
        f"campaign:{CAMP}",
        mapping={"company_id": str(CO), "priority": "0", "returning_resolver": "1"},
    )
    await routing.sadd(f"campaign:{CAMP}:offers", OFFER)
    await routing.hset(
        f"offer:{OFFER}",
        mapping={"url": "https://dest.example/{click_id}", "has_targets": "0"},
    )
    # The trusted source. `source_trusted` is what admin-api's sync emits from
    # `sources.funnel_user_id_trusted`; without it `_source_trusted` is False
    # and the L2 tier stays dark.
    await routing.sadd(f"campaign:{CAMP}:sources", SOURCE_ID)
    await routing.hset(
        f"source:{SOURCE_ID}",
        mapping={"slug": SOURCE_SLUG, "source_trusted": "1"},
    )


def _request(click_id: str, *, identity_writes: bool) -> ClickRequest:
    """The SAME request either way — only the write permission differs.

    Keeping every other field identical is the point: it makes the flag the
    single variable, so a difference in outcome cannot be attributed to
    anything else.
    """
    return ClickRequest(
        click_id=click_id,
        country="US",
        user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_2)",
        hostname=HOST,
        visitor_id=None,
        identity_token=None,
        is_returning=False,
        identity_writes=identity_writes,
        query_params={
            "c": PARAM_C,
            "source": SOURCE_SLUG,
            # No alias, no param_mappings entry, no operator action — the
            # canonical slot name binds on its own. That is the defect's reach.
            "funnel_user_id": FUID,
        },
    )


async def _route(routing, ident, req: ClickRequest):
    """Drive the REAL route() with the two pools kept SEPARATE.

    The separation is the detector. It also mirrors production, where the
    routing cache and the identity store are different Redis instances.
    """
    async def _get_routing():
        return routing

    async def _get_ident():
        return ident

    with patch.object(router, "get_redis", _get_routing), \
            patch.object(identity, "get_identity_redis", _get_ident):
        result = await router.route(req)
    # `commit_resolution` schedules `persist_identity` with create_task; let it
    # land before asserting, or a real write would be invisible to the assert
    # and this whole file would be a green that proves nothing.
    for _ in range(10):
        await asyncio.sleep(0)
    return result


@pytest.fixture
def resolver_on(monkeypatch):
    """The resolver ON is the condition under which the defect exists at all.

    Measured on staging node 55 (`/health` → `returning_resolver_active: true`),
    and default-true in `config.py` plus both deploy scripts — so this fixture
    is the production posture, not a contrived one.
    """
    monkeypatch.setattr(settings, "returning_resolver_enabled", True)
    monkeypatch.setattr(settings, "returning_routing_enabled", False)


@pytest.fixture
def routing():
    return fakeredis.aioredis.FakeRedis(decode_responses=True)


@pytest.fixture
def ident():
    """Starts EMPTY and is never seeded. Any key here is a write."""
    return fakeredis.aioredis.FakeRedis(decode_responses=True)


class TestPreviewWritesNothing:
    """T1 — the property the owner is owed."""

    async def test_preview_with_a_trusted_funnel_id_writes_nothing(
        self, resolver_on, routing, ident,
    ):
        await _seed(routing)

        result = await _route(routing, ident, _request("preview-t1", identity_writes=False))

        # The preview must still have WORKED — a zero-writes claim about a
        # preview that silently failed to route is worthless.
        assert result is not None, "preview did not route; the fixture is wrong, not the fix"
        assert result.get("url"), f"preview reached no destination: {result}"

        leaked = await ident.keys("*")
        assert leaked == [], (
            "a preview wrote to the identity store — EPV-DEFECT-1 is live. "
            f"keys: {sorted(leaked)}"
        )

    async def test_preview_leaves_uid_empty(self, resolver_on, routing, ident):
        """The mechanism, not just the symptom.

        Every sticky and fresh-track write is gated on `bool(uid)`. If uid is
        empty they are unreachable by control flow, which is a stronger
        statement than "no keys appeared this time".
        """
        await _seed(routing)

        result = await _route(routing, ident, _request("preview-t1b", identity_writes=False))

        attribution = (result or {}).get("attribution") or {}
        assert attribution.get("uid", "") == "", (
            f"preview resolved a usable uid: {attribution.get('uid')!r} — "
            "the sticky verbs are reachable"
        )


class TestPreviewDoesNotAgeTheRealClick:
    """T2 — the symptom the owner would actually have noticed."""

    async def test_real_click_after_a_preview_is_still_unique(
        self, resolver_on, routing, ident,
    ):
        await _seed(routing)

        await _route(routing, ident, _request("preview-t2", identity_writes=False))
        real = await _route(routing, ident, _request("click-t2", identity_writes=True))

        attribution = (real or {}).get("attribution") or {}
        assert attribution.get("is_unique") is True, (
            "the real click was aged by the preview that preceded it — "
            f"attribution: {attribution}"
        )
        assert attribution.get("seen_before") is False, (
            "the real click resolved as RETURNING because of a preview — "
            f"attribution: {attribution}"
        )


class TestTheDetectorItself:
    """T3 — calibration. Without this, everything above is an unfalsified green.

    Two directions, because a detector can fail either way: it can miss a real
    write (blind), or it can be structurally unable to report one (vacuous).
    """

    async def test_a_real_click_DOES_write(self, resolver_on, routing, ident):
        """Proves the rig can SEE a write. If this ever goes green-with-no-keys,
        every other assertion in this file is meaningless."""
        await _seed(routing)

        result = await _route(routing, ident, _request("click-t3a", identity_writes=True))

        assert result is not None and result.get("url"), "the real click did not route"
        wrote = await ident.keys("*")
        assert wrote, (
            "a REAL click wrote nothing to the identity store — the detector is "
            "blind, or the fixture never reaches the resolver. Either way the "
            "zero-writes tests above prove nothing."
        )

    async def test_removing_the_neutralisation_reproduces_the_defect(
        self, resolver_on, routing, ident,
    ):
        """Proves the guard can still go RED — by asking for the pre-fix behaviour.

        This is EPV-DEFECT-1 reproduced, not a simulation of it: the request is
        byte-identical to the preview in T1 except that it is permitted to write.
        The keys that appear here are exactly the ones that were appearing in
        production before the gate landed.
        """
        await _seed(routing)

        # Identical to T1's request in every field but the flag.
        await _route(routing, ident, _request("preview-t3b", identity_writes=True))

        wrote = await ident.keys("*")
        assert wrote, (
            "with identity_writes=True a preview-shaped request wrote NOTHING. "
            "That means this test can no longer detect the defect it was written "
            "for — the funnel signal is being dropped somewhere else, and T1's "
            "green is uninformative. Do not 'fix' this by deleting it."
        )
