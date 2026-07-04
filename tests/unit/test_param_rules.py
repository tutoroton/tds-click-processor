"""GTD-R166 W2 — campaign parameter rules: evaluation, threading, provenance.

The rules engine (`app/param_rules.py`) evaluates ONCE per click in
`router._build_campaign_attribution`, post-chain + fill-only, and its `fills`
outcome is threaded to `build_url` at every call site so the 302 URL and the DB
row never diverge. This suite pins:

  * evaluation matrix — fill-only vs occupied; every op; and/or logic; rule
    order + earlier-fills visibility (in conditions AND macro values); macro
    values incl. empty-expansion; fail-open per-rule + whole-config;
  * provenance content + advertiser spoof-strip;
  * buyer-fill ordering (fills a slot BEFORE it would be read by enrichment);
  * threading through build_url / _resolve_fallback_template / the
    action-executor partial — DB-vs-URL convergence (pattern: test_identity_macros).
"""

from __future__ import annotations

import functools
import inspect
import json

import pytest

from app import param_rules
from app.action_executor import _execute_redirect
from app.main import _build_extra_params
from app.models import ClickRequest
from app.param_rules import apply_param_rules, parse_param_rules
from app.router import (
    _resolve_action_with_sticky,
    _resolve_fallback_template,
    build_macro_values,
    build_url,
)


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #

_DESKTOP_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def _req(**ov) -> ClickRequest:
    defaults = dict(
        click_id="click-abc",
        country="US",
        city="",
        user_agent=_DESKTOP_UA,
        hostname="land.example",
        referer="https://ref.example/path",
        accept_language="en-US,en;q=0.9",
        query_params={},
    )
    defaults.update(ov)
    return ClickRequest(**defaults)


def _rule(rid="r1", *, conditions=None, assignments=None, enabled=True, logic="and"):
    return {
        "id": rid,
        "enabled": enabled,
        "conditions_logic": logic,
        "conditions": conditions if conditions is not None else [],
        "assignments": assignments if assignments is not None else [],
    }


def _cond(dim, op, value=None):
    c = {"dim": dim, "op": op}
    if value is not None:
        c["value"] = value
    return c


def _asg(slot, value):
    return {"slot": slot, "value": value}


def _apply(rules, slots, *, req=None, campaign_id="1"):
    """Run apply_param_rules with a realistic macro-values dict (built exactly
    the way router._build_campaign_attribution builds it)."""
    req = req or _req()
    macro_values = build_macro_values(req=req, slots=dict(slots), campaign_id=campaign_id)
    return apply_param_rules(
        rules_raw=rules, req=req, slots=slots, macro_values=macro_values,
    )


# --------------------------------------------------------------------------- #
# config parse / fail-open whole payload
# --------------------------------------------------------------------------- #


class TestParseConfig:
    def test_none_empty_list(self):
        assert parse_param_rules(None) == []
        assert parse_param_rules("") == []
        assert parse_param_rules("[]") == []
        assert parse_param_rules([]) == []

    def test_malformed_json_string_fails_open(self):
        assert parse_param_rules('[{"id": ') == []

    def test_non_list_json_ignored(self):
        assert parse_param_rules('{"id": "x"}') == []
        assert parse_param_rules("123") == []

    def test_oversized_ignored(self):
        assert parse_param_rules("[" + "0" * (70 * 1024) + "]") == []

    def test_pre_parsed_list_passthrough(self):
        r = [_rule()]
        assert parse_param_rules(r) is r


class TestNoRulesZeroChange:
    @pytest.mark.parametrize("raw", [None, "", "[]", []])
    def test_no_rules_no_fills_slots_untouched(self, raw):
        slots = {"sub5": "V5"}
        out = _apply(raw, slots)
        assert out == {"fills": {}, "applied": []}
        assert slots == {"sub5": "V5"}

    def test_malformed_config_fails_open_no_crash(self):
        slots = {}
        out = _apply('[{"id":', slots)
        assert out == {"fills": {}, "applied": []}
        assert slots == {}


# --------------------------------------------------------------------------- #
# fill-only semantics — the core contract rung
# --------------------------------------------------------------------------- #


class TestFillOnly:
    def test_fills_empty_absent_slot(self):
        slots: dict = {}
        out = _apply([_rule(assignments=[_asg("sub8", "X")])], slots)
        assert slots["sub8"] == "X"
        assert out["fills"] == {"sub8": "X"}
        assert out["applied"] == [{"id": "r1", "slots": ["sub8"]}]

    def test_does_not_overwrite_url_delivered_slot(self):
        slots = {"sub8": "URLVAL"}
        out = _apply([_rule(assignments=[_asg("sub8", "X")])], slots)
        assert slots["sub8"] == "URLVAL"  # URL wins — never pierced
        assert out["fills"] == {}
        assert out["applied"] == []

    def test_fills_explicit_none_slot(self):
        slots = {"sub8": None}  # explicitly-mapped, resolved to NULL
        _apply([_rule(assignments=[_asg("sub8", "X")])], slots)
        assert slots["sub8"] == "X"

    def test_source_slot_assignment_forbidden(self):
        slots: dict = {}
        out = _apply([_rule(assignments=[_asg("source", "hax")])], slots)
        assert "source" not in slots
        assert out["fills"] == {}

    def test_non_canonical_slot_assignment_skipped(self):
        slots: dict = {}
        out = _apply([_rule(assignments=[_asg("not_a_slot", "x")])], slots)
        assert out["fills"] == {}

    def test_disabled_rule_never_fills(self):
        slots: dict = {}
        _apply([_rule(enabled=False, assignments=[_asg("sub8", "X")])], slots)
        assert "sub8" not in slots


# --------------------------------------------------------------------------- #
# condition operators
# --------------------------------------------------------------------------- #


class TestConditionOps:
    def _fires(self, cond, *, slots=None, req=None):
        slots = dict(slots or {})
        out = _apply(
            [_rule(conditions=[cond], assignments=[_asg("sub8", "HIT")])],
            slots, req=req,
        )
        return out["fills"].get("sub8") == "HIT"

    def test_eq(self):
        assert self._fires(_cond("os", "eq", "windows"))
        assert not self._fires(_cond("os", "eq", "linux"))

    def test_eq_case_insensitive(self):
        assert self._fires(_cond("geo", "eq", "us"))   # click "US" vs "us"
        assert self._fires(_cond("browser", "eq", "chrome"))  # click "Chrome"

    def test_ne(self):
        assert self._fires(_cond("os", "ne", "linux"))
        assert not self._fires(_cond("os", "ne", "windows"))

    def test_in(self):
        assert self._fires(_cond("geo", "in", ["CA", "US", "GB"]))
        assert not self._fires(_cond("geo", "in", ["CA", "GB"]))

    def test_in_non_list_value_fails_closed(self):
        assert not self._fires(_cond("geo", "in", "US"))  # string, not list

    def test_contains(self):
        assert self._fires(_cond("referrer", "contains", "ref.example"))
        assert not self._fires(_cond("referrer", "contains", "other.com"))

    def test_empty(self):
        # param:sub5 absent → empty → matches
        assert self._fires(_cond("param:sub5", "empty"))
        # occupied → not empty → no match
        assert not self._fires(_cond("param:sub5", "empty"), slots={"sub5": "x"})

    def test_not_empty(self):
        assert self._fires(_cond("param:sub5", "not_empty"), slots={"sub5": "x"})
        assert not self._fires(_cond("param:sub5", "not_empty"))

    def test_unknown_op_fails_closed(self):
        assert not self._fires(_cond("os", "regex", "win.*"))

    def test_unknown_dim_fails_closed(self):
        assert not self._fires(_cond("nonsense", "eq", "x"))


class TestContextDims:
    def _fires(self, cond, *, slots=None, req=None):
        slots = dict(slots or {})
        out = _apply(
            [_rule(conditions=[cond], assignments=[_asg("sub8", "HIT")])],
            slots, req=req,
        )
        return out["fills"].get("sub8") == "HIT"

    def test_geo(self):
        assert self._fires(_cond("geo", "eq", "US"))
        assert self._fires(_cond("geo", "eq", "DE"), req=_req(country="DE"))

    def test_os_device_browser(self):
        assert self._fires(_cond("os", "eq", "windows"))
        assert self._fires(_cond("device", "eq", "desktop"))
        assert self._fires(_cond("browser", "contains", "chrome"))

    def test_domain(self):
        assert self._fires(_cond("domain", "eq", "land.example"))
        # hostname is lowercased → mixed-case host still matches
        assert self._fires(_cond("domain", "eq", "land.example"),
                           req=_req(hostname="Land.EXAMPLE"))

    def test_referrer(self):
        assert self._fires(_cond("referrer", "contains", "ref.example"))

    def test_source_token_dim(self):
        assert self._fires(_cond("source", "eq", "fb"), slots={"source": "fb"})
        assert not self._fires(_cond("source", "eq", "fb"), slots={"source": "tt"})

    def test_param_slot_dim(self):
        assert self._fires(_cond("param:sub5", "eq", "V5"), slots={"sub5": "V5"})


# --------------------------------------------------------------------------- #
# and / or logic + empty conditions
# --------------------------------------------------------------------------- #


class TestLogic:
    def _fires(self, conditions, logic, *, slots=None):
        slots = dict(slots or {})
        out = _apply(
            [_rule(conditions=conditions, logic=logic,
                   assignments=[_asg("sub8", "HIT")])],
            slots,
        )
        return out["fills"].get("sub8") == "HIT"

    def test_and_all_true(self):
        assert self._fires(
            [_cond("os", "eq", "windows"), _cond("geo", "eq", "US")], "and")

    def test_and_one_false(self):
        assert not self._fires(
            [_cond("os", "eq", "windows"), _cond("geo", "eq", "DE")], "and")

    def test_or_one_true(self):
        assert self._fires(
            [_cond("os", "eq", "linux"), _cond("geo", "eq", "US")], "or")

    def test_or_all_false(self):
        assert not self._fires(
            [_cond("os", "eq", "linux"), _cond("geo", "eq", "DE")], "or")

    def test_empty_conditions_match_all(self):
        assert self._fires([], "and")
        assert self._fires([], "or")


# --------------------------------------------------------------------------- #
# rule order + earlier-fills visibility
# --------------------------------------------------------------------------- #


class TestOrderAndEarlierFills:
    def test_later_rule_condition_sees_earlier_fill(self):
        slots: dict = {}
        rules = [
            _rule("r1", assignments=[_asg("sub5", "A")]),
            _rule("r2", conditions=[_cond("param:sub5", "eq", "A")],
                  assignments=[_asg("sub6", "B")]),
        ]
        out = _apply(rules, slots)
        assert slots["sub5"] == "A" and slots["sub6"] == "B"
        assert out["applied"] == [
            {"id": "r1", "slots": ["sub5"]},
            {"id": "r2", "slots": ["sub6"]},
        ]

    def test_later_rule_macro_value_sees_earlier_fill(self):
        slots: dict = {}
        rules = [
            _rule("r1", assignments=[_asg("sub5", "A")]),
            _rule("r2", assignments=[_asg("sub6", "{sub5}")]),
        ]
        _apply(rules, slots)
        assert slots["sub6"] == "A"  # {sub5} expanded from rule-1's fill

    def test_earlier_rule_wins_same_slot(self):
        slots: dict = {}
        rules = [
            _rule("r1", assignments=[_asg("sub8", "FIRST")]),
            _rule("r2", assignments=[_asg("sub8", "SECOND")]),
        ]
        out = _apply(rules, slots)
        assert slots["sub8"] == "FIRST"
        assert out["applied"] == [{"id": "r1", "slots": ["sub8"]}]


# --------------------------------------------------------------------------- #
# macro values (literal / macro / empty-expansion)
# --------------------------------------------------------------------------- #


class TestMacroValues:
    def test_literal_value(self):
        slots: dict = {}
        _apply([_rule(assignments=[_asg("sub8", "literal-42")])], slots)
        assert slots["sub8"] == "literal-42"

    def test_macro_value_expands(self):
        slots: dict = {}
        _apply([_rule(assignments=[_asg("sub8", "{country}")])], slots,
               req=_req(country="US"))
        assert slots["sub8"] == "US"

    def test_macro_expanding_to_empty_no_fill(self):
        slots: dict = {}
        # {city} is empty on this request → expands "" → NO fill, slot falls through
        out = _apply([_rule(assignments=[_asg("sub8", "{city}")])], slots,
                     req=_req(city=""))
        assert "sub8" not in slots
        assert out["fills"] == {}

    def test_mixed_literal_and_macro(self):
        slots: dict = {}
        _apply([_rule(assignments=[_asg("sub8", "geo_{country}")])], slots,
               req=_req(country="US"))
        assert slots["sub8"] == "geo_US"

    def test_click_id_macro(self):
        slots: dict = {}
        _apply([_rule(assignments=[_asg("sub8", "{click_id}")])], slots,
               req=_req(click_id="ck-99"))
        assert slots["sub8"] == "ck-99"

    def test_post_routing_macro_expands_empty_at_attribution_time(self):
        # {offer_id}/{flow_id} are NOT click-time-legal — they resolve empty when
        # rules run (offer/flow not chosen yet) → no fill.
        slots: dict = {}
        out = _apply([_rule(assignments=[_asg("sub8", "{offer_id}")])], slots)
        assert out["fills"] == {}


# --------------------------------------------------------------------------- #
# fail-open per rule
# --------------------------------------------------------------------------- #


class TestFailOpenPerRule:
    def test_one_rule_raises_others_still_apply(self, monkeypatch):
        orig = param_rules._apply_assignments

        def boom(rule, slots, mv, fills):
            if rule.get("id") == "boom":
                raise ValueError("synthetic")
            return orig(rule, slots, mv, fills)

        monkeypatch.setattr(param_rules, "_apply_assignments", boom)
        slots: dict = {}
        rules = [
            _rule("boom", assignments=[_asg("sub8", "X")]),
            _rule("good", assignments=[_asg("sub9", "Y")]),
        ]
        out = apply_param_rules(
            rules_raw=rules, req=_req(), slots=slots,
            macro_values=build_macro_values(req=_req(), slots={}, campaign_id="1"),
        )
        assert "sub8" not in slots        # boom rule skipped
        assert slots["sub9"] == "Y"       # good rule still applied
        assert out["applied"] == [{"id": "good", "slots": ["sub9"]}]

    def test_non_dict_rule_skipped(self):
        slots: dict = {}
        out = _apply(["not-a-rule", _rule(assignments=[_asg("sub8", "X")])], slots)
        assert slots["sub8"] == "X"
        assert len(out["applied"]) == 1


# --------------------------------------------------------------------------- #
# buyer fill ordering
# --------------------------------------------------------------------------- #


class TestBuyerFillOrdering:
    def test_rule_fills_buyer_id_when_empty(self):
        # buyer_id empty post-chain → rule fills it. Because apply runs BEFORE
        # _resolve_buyer_chain in _build_campaign_attribution, this filled value
        # is what enrichment reads (slots['buyer_id']).
        slots: dict = {}
        out = _apply([_rule(assignments=[_asg("buyer_id", "42")])], slots)
        assert slots["buyer_id"] == "42"
        assert out["fills"] == {"buyer_id": "42"}

    def test_rule_does_not_override_url_buyer_id(self):
        slots = {"buyer_id": "777"}  # buyer arrived on the URL
        _apply([_rule(assignments=[_asg("buyer_id", "42")])], slots)
        assert slots["buyer_id"] == "777"

    def test_apply_precedes_buyer_chain_in_source(self):
        """Structural guard: the rules-apply call is BEFORE _resolve_buyer_chain
        in _build_campaign_attribution, so a filled buyer_id reaches enrichment."""
        from app import router
        src = inspect.getsource(router._build_campaign_attribution)
        assert src.index("apply_param_rules(") < src.index("_resolve_buyer_chain(")


# --------------------------------------------------------------------------- #
# provenance content + advertiser spoof-strip
# --------------------------------------------------------------------------- #


class TestProvenanceAndSpoof:
    def test_applied_provenance_shape(self):
        slots: dict = {}
        out = _apply(
            [_rule("rule-uuid-1", assignments=[
                _asg("sub8", "X"), _asg("buyer_id", "42")])],
            slots,
        )
        assert out["applied"] == [
            {"id": "rule-uuid-1", "slots": ["sub8", "buyer_id"]}]
        # serializes to the compact provenance JSON the record builder writes
        assert (json.dumps(out["applied"], separators=(",", ":"))
                == '[{"id":"rule-uuid-1","slots":["sub8","buyer_id"]}]')

    def test_matched_rule_that_fills_nothing_not_in_applied(self):
        slots = {"sub8": "OCCUPIED"}
        out = _apply(
            [_rule("r1", assignments=[_asg("sub8", "X")])], slots)
        assert out["applied"] == []  # matched but fill-only skipped → no provenance

    def test_spoof_advertiser_param_rules_stripped_matched_path(self):
        # matched click: extras carries a forged _param_rules from the advertiser
        attribution = {"extras": {"_param_rules": "[FORGED]", "keep": "1"}}
        extras = _build_extra_params(attribution, {})
        assert "_param_rules" not in extras
        assert extras["keep"] == "1"

    def test_spoof_stripped_on_no_match_path(self):
        # no-match click: extras built from raw query params
        extras = _build_extra_params(None, {"_param_rules": "[FORGED]", "a": "b"})
        assert "_param_rules" not in extras
        assert extras["a"] == "b"


# --------------------------------------------------------------------------- #
# threading — DB-vs-URL convergence through every build_url call-site shape
# --------------------------------------------------------------------------- #


class TestThreadingSignaturePins:
    def test_build_url_accepts_param_fills(self):
        p = inspect.signature(build_url).parameters
        assert "param_fills" in p and p["param_fills"].default is None

    def test_fallback_template_accepts_param_fills(self):
        p = inspect.signature(_resolve_fallback_template).parameters
        assert "param_fills" in p

    def test_action_sticky_accepts_param_fills(self):
        p = inspect.signature(_resolve_action_with_sticky).parameters
        assert "param_fills" in p


class TestThreadingBuildUrl:
    def test_fill_overlaid_into_redirect_url(self):
        url = build_url(
            "https://x.example/?s8={sub8}&keep=1", _req(), "1", "101",
            param_fills={"sub8": "FILLED"},
        )
        assert "s8=FILLED" in url

    def test_no_param_fills_is_byte_identical(self):
        tmpl = "https://x.example/?s8={sub8}&kw={keyword}&keep=1"
        req = _req(query_params={"keyword": "K"})
        assert build_url(tmpl, req, "1", "101") == build_url(
            tmpl, req, "1", "101", param_fills=None)
        assert build_url(tmpl, req, "1", "101", param_fills={}) == build_url(
            tmpl, req, "1", "101")

    def test_fill_does_not_override_url_present_slot(self):
        # build_url re-resolves; a URL-delivered sub8 must win — but a fill only
        # ever carries slots that were EMPTY post-chain, so we assert the fill
        # simply isn't produced for an occupied slot at the engine level, and
        # here that build_url's own resolution keeps the URL value if no fill.
        url = build_url(
            "https://x.example/?s8={sub8}", _req(query_params={"sub8": "URL"}),
            "1", "101", param_fills={},
        )
        assert "s8=URL" in url

    def test_db_vs_url_convergence(self):
        """The SAME fills the engine wrote to `slots` (DB row) resolve in the URL
        via build_url — no divergence."""
        slots: dict = {}
        out = _apply([_rule(assignments=[_asg("sub8", "CONV")])], slots)
        # DB side: the resolved slots carry the fill
        assert slots["sub8"] == "CONV"
        # URL side: build_url with the same fills renders the same value
        url = build_url("https://x.example/?s8={sub8}", _req(), "1", "101",
                        param_fills=out["fills"])
        assert "s8=CONV" in url


class TestThreadingFallbackTemplate:
    def test_fallback_url_carries_fill(self):
        url = _resolve_fallback_template(
            "https://fb.example/?s8={sub8}", _req(), "1", None, None,
            None, {"sub8": "FBFILL"},
        )
        assert "s8=FBFILL" in url

    def test_none_template_stays_none(self):
        assert _resolve_fallback_template(
            None, _req(), "1", None, None, None, {"sub8": "X"}) is None


class TestThreadingActionExecutorPartial:
    def test_redirect_action_resolves_fill_via_partial(self):
        # mirrors the router's `functools.partial(build_url, ..., param_fills=...)`
        # bound into the action-executor delivery paths.
        build_url_fn = functools.partial(
            build_url, identity=None, param_fills={"sub8": "AEFILL"})
        result = _execute_redirect(
            {"url": "https://land.example/?s8={sub8}&fid={flow_id}"},
            _req(), "1", build_url_fn, None, None, "9",
        )
        assert result is not None
        assert "s8=AEFILL" in result["url"]
        assert "fid=9" in result["url"]

    def test_partial_without_fill_is_dark_safe(self):
        build_url_fn = functools.partial(build_url, identity=None, param_fills={})
        result = _execute_redirect(
            {"url": "https://land.example/?s8={sub8}&keep=1"},
            _req(), "1", build_url_fn, None, None, "9",
        )
        assert "s8=" not in result["url"]   # empty {sub8} collapses
        assert "keep=1" in result["url"]
