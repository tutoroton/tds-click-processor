"""F2 (2026-09-16) — a domain binding matches case-INSENSITIVELY, because the
value it is matched against was force-lowercased when it was saved.

admin-api stores every `binding_value` through `(raw or "").strip().lower()`, so
a Redis binding key can never carry an uppercase character. Until this fix the
resolver lowercased the HOSTNAME but fed the first path segment and the `?c=`
selector into the key verbatim — so `/Promo` built `domain:{host}:path:Promo`,
matched nothing, and fell through to geo targeting. The visitor was not shown a
404; they were shown ANOTHER campaign's offer.

Normalising the matcher rather than restricting what may be saved is this repo's
established answer to this shape —
`ADR-0105-language-filter-single-english-strip-region-at-match` ruled the same
way for the language filter.

The rig is `fakeredis`, not a hand-rolled stand-in, deliberately: the claim under
test is about the exact KEY STRING the resolver builds, and a fake I wrote myself
could mirror my own assumption about it. Here the key must match byte-for-byte
what was seeded or the lookup misses, exactly as in production.
"""

from __future__ import annotations

import json

import fakeredis.aioredis
import pytest

from app.models import ClickRequest
from app.resolution import BINDING_SELECTOR_KEY
from app.router import resolve_domain_campaign

pytestmark = pytest.mark.asyncio

# 2-label host — `len(parts) >= 3` is false, so the subdomain tier is not in play
# and the path/param rungs are what answer.
HOST = "d2case.test"

CAMP_PATH = "77"
CAMP_PARAM = "88"
CAMP_OTHER = "99"


def _binding(campaign_id: str, binding_id: int, alias: str) -> str:
    return json.dumps(
        {"campaign_id": campaign_id, "binding_id": binding_id, "binding_alias": alias}
    )


async def _seeded_lowercase_bindings():
    """The only shape admin-api can actually produce: lowercase stored values."""
    fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
    await fake.set(f"domain:{HOST}:path:promo", _binding(CAMP_PATH, 901, "promo-path"))
    await fake.set(f"domain:{HOST}:param:promo", _binding(CAMP_PARAM, 902, "promo-param"))
    await fake.set(f"domain:{HOST}:path:promo2", _binding(CAMP_OTHER, 903, "promo2-path"))
    return fake


def _req(**kw) -> ClickRequest:
    return ClickRequest(click_id="d2-case-test", hostname=HOST, **kw)


class TestAnUppercaseArrivalReachesItsBinding:
    async def test_uppercase_path_segment_matches_its_lowercase_binding(self):
        fake = await _seeded_lowercase_bindings()
        res = await resolve_domain_campaign(fake, _req(path="/Promo"))
        assert res.campaign_id == CAMP_PATH
        assert res.match_tier == "path"
        assert res.binding_id == 901

    async def test_mixed_case_path_segment_matches(self):
        fake = await _seeded_lowercase_bindings()
        res = await resolve_domain_campaign(fake, _req(path="/PrOmO/deeper"))
        assert res.campaign_id == CAMP_PATH
        assert res.match_tier == "path"

    async def test_uppercase_param_selector_matches_its_lowercase_binding(self):
        fake = await _seeded_lowercase_bindings()
        res = await resolve_domain_campaign(
            fake, _req(query_params={BINDING_SELECTOR_KEY: "PROMO"}))
        assert res.campaign_id == CAMP_PARAM
        assert res.match_tier == "param"
        assert res.binding_id == 902


    async def test_folding_does_not_merge_two_distinct_values(self):
        """`promo` and `promo2` differ by more than case and must stay distinct —
        a fold that truncated or over-matched would collapse them.

        NOT a both-sides control: it drives uppercase arrivals, so it can only
        pass once the fold exists. It belongs here, with the behaviour the fix
        introduces, rather than among the guards that must not move.
        """
        fake = await _seeded_lowercase_bindings()
        assert (await resolve_domain_campaign(fake, _req(path="/Promo"))).campaign_id == CAMP_PATH
        assert (await resolve_domain_campaign(fake, _req(path="/Promo2"))).campaign_id == CAMP_OTHER


class TestControlsTheFixMustNotMove:
    async def test_CONTROL_an_exact_lowercase_arrival_still_matches(self):
        """An arrival that already matched by exact case still matches.

        ⚠️ Narrowed 2026-09-16 after an adversarial review: this docstring used
        to claim it covered the plan's whole regression clause, 'any currently
        matching binding stops matching'. It does not — it exercises one
        arrival shape. What the fold CAN change for already-matching traffic is
        WHICH TIER answers, and that is pinned separately below.
        """
        fake = await _seeded_lowercase_bindings()
        res = await resolve_domain_campaign(fake, _req(path="/promo"))
        assert res.campaign_id == CAMP_PATH
        assert res.match_tier == "path"

    async def test_CONTROL_a_genuinely_unknown_segment_still_misses(self):
        """Folding must not make everything match. No root binding is seeded, so
        an unknown selector resolves to nothing at all."""
        fake = await _seeded_lowercase_bindings()
        res = await resolve_domain_campaign(fake, _req(path="/Nowhere"))
        assert res.campaign_id is None
        assert not res.blocked

    async def test_CONTROL_a_selector_present_still_blocks_the_root_rung(self):
        """`_root_rung_allowed` asks only whether a selector is PRESENT, and the
        fold must not empty one. With an unmatched `/Promo` the root binding must
        still NOT answer (the A1a rule), which it would if folding had produced
        an empty segment."""
        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        await fake.set(f"domain:{HOST}:root", _binding("35", 212, "root"))
        res = await resolve_domain_campaign(fake, _req(path="/Promo"))
        assert res.campaign_id is None, "the root catch-all must not answer a selector miss"
        assert res.binding_id != 212

    async def test_CONTROL_a_bare_root_arrival_still_reaches_the_root_rung(self):
        """The other side of the same control: with NO selector the root rung is
        still reached, so the fold did not break root resolution."""
        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        await fake.set(f"domain:{HOST}:root", _binding("35", 212, "root"))
        res = await resolve_domain_campaign(fake, _req())
        assert res.campaign_id == "35"
        assert res.match_tier == "root"


class TestTierPrecedenceMovesAndThatIsTHEPOINT:
    """🔴 The consequence an adversarial review found that my own control had
    over-claimed away.

    The resolver answers the FIRST populated tier in the order
    subdomain > path > param > root. Before the fold, `/Promo?c=other` built
    `path:Promo`, missed, and fell through to the `?c=` tier. After it, the path
    tier matches and answers FIRST — so the click changes campaign.

    That is the tier order working as designed once the path tier can match at
    all; it is not a new precedence rule. But it IS a behaviour change on
    traffic that was already matching something, and shipping it unnamed would
    have been the omission, not the change.

    Measured on staging over 30 days before shipping, 386 446 events: arrivals
    carrying BOTH an uppercase first segment and a `?c=` selector — ZERO;
    answered by the param tier — ZERO; by subdomain — ZERO; by root — 2. The
    positive controls are non-trivial (241 413 arrivals carry a first segment,
    2 987 carry a selector), so those zeros mean none, not a broken parse.
    """

    async def test_an_uppercase_path_now_OUTRANKS_a_matching_param_selector(self):
        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        await fake.set(f"domain:{HOST}:path:promo", _binding(CAMP_PATH, 901, "promo-path"))
        await fake.set(f"domain:{HOST}:param:other", _binding(CAMP_PARAM, 902, "other-param"))

        res = await resolve_domain_campaign(
            fake, _req(path="/Promo", query_params={BINDING_SELECTOR_KEY: "other"}))

        assert res.match_tier == "path", "the path tier answers once it can match"
        assert res.campaign_id == CAMP_PATH

    async def test_CONTROL_without_a_matching_path_the_selector_still_answers(self):
        """The other side: where no path binding matches, the `?c=` tier answers
        exactly as before. So the change above is the path tier becoming
        reachable, not the param tier being broken."""
        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        await fake.set(f"domain:{HOST}:param:other", _binding(CAMP_PARAM, 902, "other-param"))

        res = await resolve_domain_campaign(
            fake, _req(path="/Nowhere", query_params={BINDING_SELECTOR_KEY: "other"}))

        assert res.match_tier == "param"
        assert res.campaign_id == CAMP_PARAM


class TestTheRecordedValueIsUntouched:
    async def test_the_request_is_not_mutated_by_the_fold(self):
        """🔴 The fold is a MATCH-time normalisation only. The click record is
        built from the request, so if resolution rewrote it the stored path and
        selector would silently lose the visitor's actual casing. Assert the
        request still carries exactly what arrived."""
        fake = await _seeded_lowercase_bindings()
        req = _req(path="/Promo/Deeper", query_params={BINDING_SELECTOR_KEY: "PROMO",
                                                       "utm_source": "FB"})
        await resolve_domain_campaign(fake, req)
        assert req.path == "/Promo/Deeper"
        assert req.query_params[BINDING_SELECTOR_KEY] == "PROMO"
        assert req.query_params["utm_source"] == "FB"


class TestTheHazardIsNamedNotHidden:
    async def test_two_bindings_differing_ONLY_by_case_now_resolve_to_the_LOWERCASE_one(self):
        """🔴 This is the risk register's worst outcome, written down rather than
        left implicit: if two bindings on one host differed only by case, folding
        makes the uppercase one unreachable and its traffic goes to the other
        campaign.

        It is safe to ship because that population is EMPTY, measured on staging
        immediately before the change: of 202 bindings, ZERO carry any uppercase
        (path 0/48, param 0/134) and ZERO pairs differ only by case. admin-api
        force-lowercases at save, so such a pair cannot be created through the
        API at all — this state is reachable only by writing Redis directly.

        The census must be RE-RUN before this lands if time has passed.
        """
        fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
        await fake.set(f"domain:{HOST}:path:promo", _binding(CAMP_PATH, 901, "lower"))
        await fake.set(f"domain:{HOST}:path:Promo", _binding(CAMP_OTHER, 904, "UPPER"))

        res = await resolve_domain_campaign(fake, _req(path="/Promo"))

        assert res.campaign_id == CAMP_PATH, (
            "the fold sends the arrival to the lowercase binding — the documented "
            "consequence, not an accident"
        )
        assert res.binding_id == 901
