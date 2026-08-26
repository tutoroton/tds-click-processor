"""A1a / V18 — the click must record WHAT selector was asked for and WHETHER we
honoured it.

Before this, `resolve_domain_campaign` had no direct unit test at all, which is
the structural reason the gap survived: the resolver returned only the WINNER
(`binding_id`/`binding_alias`), so a visitor who asked for an unknown selector
and fell through to the root binding produced a click byte-identical to a visit
to the bare domain.

Reproduced on deployed staging 2026-08-25 over the WHOLE stored row (not a
chosen column list), so the two halves below are measurements, not a design
opinion:

    GET https://geotdsclicks.com/?c=rfixprobe-unknown-param-9x7q
      -> click 01a0398ade07bce2966e6b56, 3312 bytes, path="/", binding_id=212
      -> the selector appears NOWHERE in the row              (destroyed)

    GET https://geotdsclicks.com/__rfix_probe_connectivity_check_do_not_route__
      -> click 01a039809cf205edd2432675, path carries the segment, binding_id=212
      -> the VALUE survives; the fact that it was IGNORED does not  (unrecorded)

Nothing here changes routing. Every assertion is about what is RECORDED.
"""
import pytest

from app.main import _build_extra_params
from app.models import ClickRequest
from app.resolution import BINDING_SELECTOR_KEY
from app.router import resolve_domain_campaign


class _FakePipeline:
    """Records the calls made on it and replays the answers in order.

    `sismember` always answers False (no disabled base, no wildcard base) — the
    branches under test are the ordinary non-wildcard ones. `get` answers from
    the binding map, so the ORDER `resolve_domain_campaign` builds its keys in is
    what decides the result, exactly as in production.
    """

    def __init__(self, bindings: dict[str, str]):
        self._bindings = bindings
        self._calls: list[tuple[str, str]] = []

    def sismember(self, _key, _member):
        self._calls.append(("sismember", _member))

    def get(self, key):
        self._calls.append(("get", key))

    async def execute(self):
        return [
            False if kind == "sismember" else self._bindings.get(arg)
            for kind, arg in self._calls
        ]


class _FakeRedis:
    def __init__(self, bindings: dict[str, str]):
        self._bindings = bindings

    def pipeline(self):
        return _FakePipeline(self._bindings)

    async def sismember(self, _key, _member):
        return False


def _req(**kw) -> ClickRequest:
    return ClickRequest(click_id="a1a-test", **kw)


HOST = "geotdsclicks.com"
ROOT_BINDING = '{"campaign_id":"35","binding_id":212,"binding_alias":"root"}'
PARAM_BINDING = '{"campaign_id":"77","binding_id":901,"binding_alias":"t2-seg"}'
PATH_BINDING = '{"campaign_id":"88","binding_id":902,"binding_alias":"lc-fresh"}'


class TestWhichTierAnswered:
    @pytest.mark.asyncio
    async def test_a_param_selector_that_matches_is_reported_as_param(self):
        r = _FakeRedis({f"domain:{HOST}:param:t2-seg": PARAM_BINDING,
                        f"domain:{HOST}:root": ROOT_BINDING})
        res = await resolve_domain_campaign(
            r, _req(hostname=HOST, query_params={BINDING_SELECTOR_KEY: "t2-seg"}))
        assert res.campaign_id == "77"
        assert res.binding_id == 901
        assert res.match_tier == "param"

    @pytest.mark.asyncio
    async def test_a_path_selector_that_matches_is_reported_as_path(self):
        r = _FakeRedis({f"domain:{HOST}:path:lc-fresh": PATH_BINDING,
                        f"domain:{HOST}:root": ROOT_BINDING})
        res = await resolve_domain_campaign(
            r, _req(hostname=HOST, path="/lc-fresh/deep"))
        assert res.binding_id == 902
        assert res.match_tier == "path"

    @pytest.mark.asyncio
    async def test_an_unknown_param_selector_NO_LONGER_falls_to_root(self):
        """THE 14 129 CASE — and A1b SUPERSEDES what A1a pinned here.

        A1a asserted `binding_id == 212` with the comment "routing is UNCHANGED",
        because A1a's whole job was to RECORD the dismissal without altering it.
        A1b is the decision that followed, and it changes exactly this: a request
        that NAMED a selector we do not have no longer inherits someone else's
        campaign. Same population, one deliberate step later.
        """
        r = _FakeRedis({f"domain:{HOST}:root": ROOT_BINDING})
        res = await resolve_domain_campaign(
            r, _req(hostname=HOST,
                    query_params={BINDING_SELECTOR_KEY: "rfixprobe-unknown-9x7q"}))
        assert res.binding_id != 212, "the root catch-all must no longer answer"
        assert res.campaign_id is None
        assert not res.blocked, (
            "this is a geo fall-through (_NO_DOMAIN_MATCH), NOT a hard block — "
            "refusing outright would be a bigger change than the finding asked for"
        )

    @pytest.mark.asyncio
    async def test_an_unknown_path_selector_NO_LONGER_falls_to_root(self):
        """The scanner-probe case, which is 90.9% of binding 212's traffic."""
        r = _FakeRedis({f"domain:{HOST}:root": ROOT_BINDING})
        res = await resolve_domain_campaign(
            r, _req(hostname=HOST, path="/wp-admin/install.php"))
        assert res.binding_id != 212
        assert res.campaign_id is None

    @pytest.mark.asyncio
    async def test_no_selector_at_all_also_reports_root(self):
        """The CONTROL that makes the tier honest: an ordinary visit to the bare
        domain is ALSO `root`. So `match_tier` alone is not the discriminator —
        the pair (tier == 'root' AND something was asked for) is. A read that
        forgets this counts every honest visitor as a miss."""
        r = _FakeRedis({f"domain:{HOST}:root": ROOT_BINDING})
        res = await resolve_domain_campaign(r, _req(hostname=HOST, path="/"))
        assert res.match_tier == "root"

    @pytest.mark.asyncio
    async def test_no_binding_at_all_is_none_not_root(self):
        """`none` (no domain binding — the click falls through to geo) must never
        be conflated with `root` (a binding answered, just not the asked-for
        one). They mean opposite things about whether the domain is configured."""
        res = await resolve_domain_campaign(_FakeRedis({}), _req(hostname=HOST))
        assert res.campaign_id is None
        assert res.match_tier == "none"


class TestTheReservedKeyIsHandledOneWayOnEveryPath:
    """F-PARAM-2 says `c` is globally-reserved routing control, never advertiser
    data, and `resolve_slots` drops it — on the RESOLVED path. The no-match /
    pre-campaign path rebuilds extras from the RAW query params and so let it
    back in: the guard covered one path of two.

    Measured on deployed staging 2026-08-25: of 252 568 clicks in 30 days, one
    carried `{"c":"zz-a5-1787358817","routing_status":"blocked"}` — the reserved
    key stored as though an advertiser had sent it. Rare only because a no-match
    click carrying `?c=` is rare; the mechanism was unconditional.
    """

    def test_the_raw_no_match_path_no_longer_leaks_it(self):
        extras = _build_extra_params(
            None, {BINDING_SELECTOR_KEY: "zz-a5-1787358817", "utm_source": "fb"})
        assert BINDING_SELECTOR_KEY not in extras, (
            "the reserved routing key is masquerading as a custom param"
        )
        assert extras["utm_source"] == "fb", "…and real advertiser params survive"

    def test_the_resolved_path_still_does_not_leak_it(self):
        """Control: this half was already correct, and must stay correct."""
        extras = _build_extra_params({"extras": {"utm_source": "fb"}}, {})
        assert BINDING_SELECTOR_KEY not in extras
        assert extras["utm_source"] == "fb"

    def test_a_selector_shaped_advertiser_key_is_NOT_dropped(self):
        """The exclusion must be the EXACT reserved key, not a prefix match — a
        `?campaign=` or `?category=` is ordinary advertiser data."""
        extras = _build_extra_params(None, {"campaign": "x", "category": "y"})
        assert extras == {"campaign": "x", "category": "y"}


class TestA1bUnmatchedSelectorRefusal:
    """A1b / V18 — the DECISION that followed A1a's measurement.

    A1a made the dismissal visible; this class pins what we decided to do about
    it. Every assertion below is about ROUTING, not recording.

    WHY THE ROOT RUNG WAS THE PROBLEM: it was appended UNCONDITIONALLY as the
    last rung, so a root binding became a universal catch-all. Measured on
    staging over 30 days, binding types read from `campaign_domains` rather than
    inferred (the first cut of this measurement did NOT separate them and was
    wrong — a path-bound binding legitimately has a non-bare path):

        212 geotdsclicks.com  root  15 624 clicks, 14 195 (90.9%) non-bare
        314 geotds.xyz        root   6 787 clicks,  5 498 (81.0%) non-bare
        334 geotdsclicks.com  path      305 clicks — MATCHES its own rung
        216/227/231           param     323 clicks — MATCH their own rung

    and 212's path census is scanner traffic top to bottom: /wp-admin/install.php
    368 · /robots.txt 137 · /favicon.ico 100 · /wp-login.php 80 · /.env 69 ·
    /.git/config 52 · /xmlrpc.php 41. No landing-page-shaped path appears at all.
    """

    @pytest.mark.asyncio
    async def test_CONTROL_a_bare_request_still_reaches_root(self):
        """The one that stops this being 'refuse everything'. A bare visit is
        what a root binding is FOR, and it must keep working."""
        r = _FakeRedis({f"domain:{HOST}:root": ROOT_BINDING})
        res = await resolve_domain_campaign(r, _req(hostname=HOST, path="/"))
        assert res.binding_id == 212
        assert res.campaign_id == "35"
        assert res.match_tier == "root"

    @pytest.mark.asyncio
    async def test_CONTROL_a_matching_path_selector_still_routes(self):
        """Binding 334's real population — it matches its own rung and must be
        untouched by a change aimed at UNMATCHED selectors."""
        r = _FakeRedis({f"domain:{HOST}:path:audit-split": PATH_BINDING,
                        f"domain:{HOST}:root": ROOT_BINDING})
        res = await resolve_domain_campaign(
            r, _req(hostname=HOST, path="/audit-split"))
        assert res.binding_id == 902
        assert res.match_tier == "path"

    @pytest.mark.asyncio
    async def test_CONTROL_a_matching_param_selector_still_routes(self):
        r = _FakeRedis({f"domain:{HOST}:param:t2-seg": PARAM_BINDING,
                        f"domain:{HOST}:root": ROOT_BINDING})
        res = await resolve_domain_campaign(
            r, _req(hostname=HOST, path="/",
                    query_params={BINDING_SELECTOR_KEY: "t2-seg"}))
        assert res.binding_id == 901
        assert res.match_tier == "param"

    @pytest.mark.asyncio
    async def test_the_KILL_SWITCH_restores_the_old_fallthrough(self, monkeypatch):
        """A guard I cannot turn off is a guard I cannot roll back.

        This is also the calibration in the OTHER direction: it proves the two
        refusal tests above fail for the reason claimed (the new rule), not
        because the fixture stopped resolving anything at all.
        """
        from app.config import settings as live_settings

        monkeypatch.setattr(
            live_settings, "root_fallthrough_on_unmatched_selector", True)
        r = _FakeRedis({f"domain:{HOST}:root": ROOT_BINDING})
        res = await resolve_domain_campaign(
            r, _req(hostname=HOST, path="/wp-admin/install.php"))
        assert res.binding_id == 212, "kill-switch must restore the old behaviour"
        assert res.match_tier == "root"

    def test_the_helper_is_the_one_deciding_and_not_a_stray_import(self):
        """Structural, and deliberately NOT `hasattr` — presence of a name proves
        nothing. This asserts the helper answers the four cases directly."""
        from app.router import _root_rung_allowed

        assert _root_rung_allowed("", "") is True, "bare request keeps root"
        assert _root_rung_allowed("wp-admin", "") is False
        assert _root_rung_allowed("", "unknown-seg") is False
        assert _root_rung_allowed("wp-admin", "unknown-seg") is False
