"""Executable contract for the click-parameter resolution value-chain.

This is the anti-degradation regression suite for `resolve_slots`
(`app/resolution.py`). Each row of `CONTRACT` pins ONE rung of the
proven value-chain so that a future change which breaks a rung fails
CI with a row name that says exactly which rule regressed.

The value-chain (the contract — DESIGN LOCKED 2026-06-02, live-proven
on staging 2026-06-02..03):

    value = URL( canonical > eff_source.alias > campaign.alias )
            > eff_source.hardcode
            > campaign.hardcode
            > NULL

  - `eff_source` = the EFFECTIVE source layer the resolver receives in
    `source_mappings`. The per-link-override merge
    (`override if non-null else source-global`; `null` ⇒ inherit,
    `[]` ⇒ explicit-empty/shadow) happens UPSTREAM in
    `app/main.py` reading Redis `campaign:{id}:source_overrides`, so at
    the resolver boundary an explicit-empty override arrives simply as
    `source_mappings=[]`. The rows below exercise the effective layer.
  - SOURCE-WINS: `eff_source.alias` precedes `campaign.alias`, and
    `eff_source.hardcode` precedes `campaign.hardcode`.
  - Canonical self-binding (F.X): every reserved slot AND `sub1..20`
    auto-binds its own name as a URL key without an authored mapping.
  - C-1 (2026-06-02): `sub1..8` are FREE — never legacy-routed to
    source/creative/buyer; they land in their own columns.
  - Finding #4 (2026-06-03): a slot aliased by BOTH eff_source AND the
    campaign consults BOTH alias keys — the campaign alias resolves
    (not empty) when it is the only key present, and neither alias key
    bleeds into `extras`.

Ground truth: `docs/development/staging-param-test-2026-06-02.md`
(every scenario below was proven with a live click → CH column +
302 macro). `docs/development/param-source-campaign-overrides-2026-06-02.md`
is the SOURCE-WINS contract.

`resolve_slots` return shape (pinned by these rows):
  - `slots`: a slot appears with its resolved value; a slot that was
    EXPLICITLY enumerated by either layer but resolved to nothing
    appears as `None`; a canonical slot that merely auto-iterated and
    matched nothing is OMITTED.
  - `extras`: query keys not consumed by any slot AND not a canonical
    slot name, value coerced to `str`.
  - buyer_id is returned as the raw STRING; UInt32 coercion is a
    downstream collector concern (`app/clickhouse.py:_uint`).
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from app.resolution import resolve_slots


@dataclass(frozen=True)
class Case:
    name: str
    url: dict
    expected_slots: dict
    expected_extras: dict
    source: list | None = None
    campaign: list | None = None
    note: str = ""


def _m(slot, alias=None, default_value=None):
    """Build one param-mapping entry (slot + optional alias/default)."""
    e: dict = {"slot": slot}
    if alias is not None:
        e["alias"] = alias
    if default_value is not None:
        e["default_value"] = default_value
    return e


CONTRACT: list[Case] = [
    # ---- canonical self-binding + free subs (C-1) ----
    Case(
        name="canonical_self_binding_reserved",
        url={"source": "S1"},
        expected_slots={"source": "S1"},
        expected_extras={},
        note="reserved slot auto-binds its own name, no mapping needed",
    ),
    Case(
        name="canonical_self_binding_sub",
        url={"sub5": "V5"},
        expected_slots={"sub5": "V5"},
        expected_extras={},
        note="sub slot auto-binds its own name",
    ),
    Case(
        name="canonical_self_binding_funnel_user_id",
        url={"funnel_user_id": "FU-123"},
        expected_slots={"funnel_user_id": "FU-123"},
        expected_extras={},
        note="P1: funnel_user_id reserved slot auto-binds (L2 identity anchor, dark)",
    ),
    Case(
        name="c1_free_subs_not_legacy_routed",
        url={"sub1": "FREE1", "sub3": "FREE3", "sub8": "FREE8"},
        expected_slots={"sub1": "FREE1", "sub3": "FREE3", "sub8": "FREE8"},
        expected_extras={},
        note="C-1: sub1..8 land in their OWN columns, NOT source/creative/buyer",
    ),
    # ---- alias resolves / hardcode fills ----
    Case(
        name="alias_resolves_via_source",
        url={"fbp": "PX"},
        source=[_m("pixel_id", alias="fbp")],
        expected_slots={"pixel_id": "PX"},
        expected_extras={},
        note="URL value reaches the slot through the source-defined alias",
    ),
    Case(
        name="hardcode_fills_from_source_default",
        url={},
        source=[_m("funnel_id", default_value="SRCFID")],
        expected_slots={"funnel_id": "SRCFID"},
        expected_extras={},
    ),
    Case(
        name="hardcode_fills_from_campaign_default",
        url={},
        campaign=[_m("landing_id", default_value="CMPLID")],
        expected_slots={"landing_id": "CMPLID"},
        expected_extras={},
    ),
    # ---- value-chain precedence, each rung isolated (slot=keyword) ----
    Case(
        name="vc1_url_canonical_beats_source_alias",
        url={"keyword": "CANON", "ekw": "SRC"},
        source=[_m("keyword", alias="ekw")],
        expected_slots={"keyword": "CANON"},
        expected_extras={},
        note="canonical URL key trumps the source alias on collision",
    ),
    Case(
        name="vc2_source_alias_beats_campaign_alias",
        url={"ekw": "SRC", "ckw": "CMP"},
        source=[_m("keyword", alias="ekw")],
        campaign=[_m("keyword", alias="ckw")],
        expected_slots={"keyword": "SRC"},
        expected_extras={},
        note="SOURCE-WINS at the alias rung; campaign alias does not bleed",
    ),
    Case(
        name="vc3_campaign_alias_beats_source_hardcode",
        url={"ckw": "CMPALIAS"},
        source=[_m("keyword", alias="ekw", default_value="SRCHC")],
        campaign=[_m("keyword", alias="ckw")],
        expected_slots={"keyword": "CMPALIAS"},
        expected_extras={},
        note="URL layer (even campaign.alias) beats the eff_source hardcode",
    ),
    Case(
        name="vc4_source_hardcode_beats_campaign_hardcode",
        url={},
        source=[_m("keyword", default_value="SRCHC")],
        campaign=[_m("keyword", default_value="CMPHC")],
        expected_slots={"keyword": "SRCHC"},
        expected_extras={},
        note="SOURCE-WINS at the hardcode rung",
    ),
    Case(
        name="vc5_campaign_hardcode_when_no_source",
        url={},
        source=[],
        campaign=[_m("keyword", default_value="CMPHC")],
        expected_slots={"keyword": "CMPHC"},
        expected_extras={},
    ),
    Case(
        name="vc6_null_when_nothing_resolves",
        url={},
        source=[_m("keyword", alias="ekw")],
        expected_slots={"keyword": None},
        expected_extras={},
        note="explicitly mapped slot with no value resolves to None",
    ),
    # ---- per-link override = effective source layer ----
    Case(
        name="override_empty_shadows_source_global",
        url={"ckw": "CMP"},
        source=[],
        campaign=[_m("keyword", alias="ckw")],
        expected_slots={"keyword": "CMP"},
        expected_extras={},
        note=(
            "explicit-empty override arrives as source_mappings=[]: the "
            "effective source contributes nothing, campaign still resolves "
            "(override-replace/null-inherit merge is upstream in main.py)"
        ),
    ),
    # ---- canonical > alias collision (reserved slot) ----
    Case(
        name="collision_canonical_beats_alias_reserved",
        url={"pixel_id": "CANONPX", "fbp": "ALIASPX"},
        source=[_m("pixel_id", alias="fbp")],
        expected_slots={"pixel_id": "CANONPX"},
        expected_extras={},
        note="canonical reserved key wins; the alias loser does not bleed",
    ),
    # ---- Finding #4: slot aliased by BOTH layers ----
    Case(
        name="finding4_dual_alias_source_wins_no_bleed",
        url={"ekw": "SRCALIAS", "ckw": "CMPALIAS"},
        source=[_m("keyword", alias="ekw")],
        campaign=[_m("keyword", alias="ckw")],
        expected_slots={"keyword": "SRCALIAS"},
        expected_extras={},
        note="both alias keys consulted; source wins; campaign alias not in extras",
    ),
    Case(
        name="finding4_dual_alias_campaign_only_resolves",
        url={"ckw": "CMPONLY"},
        source=[_m("keyword", alias="ekw")],
        campaign=[_m("keyword", alias="ckw")],
        expected_slots={"keyword": "CMPONLY"},
        expected_extras={},
        note="campaign-alias-only click resolves via campaign.alias (not NULL)",
    ),
    # ---- buyer_id: resolver returns the STRING ----
    Case(
        name="buyer_id_resolver_returns_string",
        url={"ebid": "778899"},
        source=[_m("buyer_id", alias="ebid")],
        expected_slots={"buyer_id": "778899"},
        expected_extras={},
        note="resolve_slots stores the raw string; UInt32 coercion is downstream",
    ),
    # ---- extras semantics ----
    Case(
        name="extras_unknown_key_lands_in_extras",
        url={"fbclid": "fb_xxx"},
        expected_slots={},
        expected_extras={"fbclid": "fb_xxx"},
    ),
    Case(
        name="extras_registered_alias_consumed_not_in_extras",
        url={"fbp": "PX", "utm_source": "fb"},
        source=[_m("pixel_id", alias="fbp")],
        expected_slots={"pixel_id": "PX"},
        expected_extras={"utm_source": "fb"},
        note="the consumed alias key never duplicates into extras",
    ),
    Case(
        name="extras_canonical_name_dropped_even_unmapped",
        url={"keyword": "K", "random": "r"},
        expected_slots={"keyword": "K"},
        expected_extras={"random": "r"},
        note="a canonical slot name is never an extras leak",
    ),
]


@pytest.mark.parametrize("case", CONTRACT, ids=[c.name for c in CONTRACT])
def test_param_resolution_contract(case: Case):
    slots, extras = resolve_slots(
        query_params=case.url,
        source_mappings=case.source,
        campaign_mappings=case.campaign,
    )
    assert slots == case.expected_slots, (
        f"[{case.name}] slots mismatch — value-chain rung regressed. "
        f"{case.note}"
    )
    assert extras == case.expected_extras, (
        f"[{case.name}] extras mismatch — consumption/bleed rule regressed. "
        f"{case.note}"
    )


def test_duplicate_url_key_last_wins_is_an_upstream_parse_concern():
    """Boundary documentation, NOT a resolve_slots feature.

    Duplicate URL keys (`?sub5=A&sub5=B` → `sub5=B`, last-wins) are
    collapsed at the EDGE query-parse layer (CF Worker → click-processor
    payload) BEFORE `resolve_slots` ever runs — the resolver receives an
    already-flat dict. This was proven live (DUP1/DUP2/DUP3 in
    `staging-param-test-2026-06-02.md`). Here we pin only the boundary:
    given the post-dedup flat dict, the resolver uses that single value.
    If a future refactor pushes dedup responsibility down into
    `resolve_slots`, replace this with a real multi-value test.
    """
    slots, _ = resolve_slots(
        query_params={"sub5": "DUPB"},  # edge already kept the last value
        source_mappings=None,
        campaign_mappings=None,
    )
    assert slots == {"sub5": "DUPB"}


# ===========================================================================
# NEW RUNG (GTD-R166 W2) — post-chain, fill-only campaign parameter rules.
#
# Rules run AFTER the resolve_slots value-chain (in
# `router._build_campaign_attribution`) and fill a slot ONLY if it is still
# empty. The URL / source-hardcode / campaign-hardcode rungs above are
# byte-UNCHANGED — every existing CONTRACT row runs with NO rules, proving
# resolve_slots itself is untouched. These rows pin the ONE new rung:
#
#     value = URL(...) > eff_source.hardcode > campaign.hardcode
#             > FILL-RULE (only if still empty)          <-- NEW, lowest
#             > NULL
# ===========================================================================

from app.models import ClickRequest  # noqa: E402
from app.param_rules import apply_param_rules  # noqa: E402
from app.router import build_macro_values  # noqa: E402

# A match-ALL rule (empty conditions) that assigns keyword — isolates the
# fill-vs-occupied precedence from condition matching (covered in
# test_param_rules.py).
_KEYWORD_FILL_RULE = [{
    "id": "rung-rule",
    "enabled": True,
    "conditions_logic": "and",
    "conditions": [],
    "assignments": [{"slot": "keyword", "value": "RULEFILL"}],
}]


def _resolve_then_rules(url, *, source=None, campaign=None, rules=_KEYWORD_FILL_RULE):
    """The production order: resolve the full chain, THEN apply post-chain
    rules over the resolved slots (fill-only)."""
    req = ClickRequest(click_id="c", country="US", user_agent="", query_params=url)
    slots, _extras = resolve_slots(
        query_params=url, source_mappings=source, campaign_mappings=campaign)
    mv = build_macro_values(req=req, slots=dict(slots), campaign_id="1")
    out = apply_param_rules(rules_raw=rules, req=req, slots=slots, macro_values=mv)
    return slots, out


def test_rung_fill_applies_only_when_empty_post_chain():
    """Slot empty after URL+hardcode chain → the fill-rule supplies it."""
    slots, out = _resolve_then_rules({})
    assert slots["keyword"] == "RULEFILL"
    assert out["fills"] == {"keyword": "RULEFILL"}


def test_rung_url_value_beats_fill():
    """URL delivered the slot → 'URL wins' invariant intact; rule is a no-op."""
    slots, out = _resolve_then_rules({"keyword": "FROMURL"})
    assert slots["keyword"] == "FROMURL"
    assert out["fills"] == {}


def test_rung_source_hardcode_beats_fill():
    """eff_source hardcode filled the slot → above the fill-rule; rule no-op."""
    slots, out = _resolve_then_rules(
        {}, source=[_m("keyword", default_value="SRCHC")])
    assert slots["keyword"] == "SRCHC"
    assert out["fills"] == {}


def test_rung_campaign_hardcode_beats_fill():
    """campaign hardcode filled the slot → above the fill-rule; rule no-op."""
    slots, out = _resolve_then_rules(
        {}, campaign=[_m("keyword", default_value="CMPHC")])
    assert slots["keyword"] == "CMPHC"
    assert out["fills"] == {}


def test_rung_fill_above_null_only():
    """The fill-rule sits ABOVE NULL: an explicitly-mapped slot that resolved to
    None (vc6 rung) is still empty → the fill applies."""
    slots, out = _resolve_then_rules(
        {}, source=[_m("keyword", alias="ekw")])  # mapped, no value → None
    assert slots["keyword"] == "RULEFILL"
    assert out["fills"] == {"keyword": "RULEFILL"}


def test_rung_no_rules_leaves_chain_byte_identical():
    """With NO rules the resolved slots equal the pure-chain result — the rung is
    purely additive (the whole CONTRACT list above already proves this for
    resolve_slots; here we prove apply_param_rules is a no-op when empty)."""
    for raw in (None, [], "[]"):
        slots, out = _resolve_then_rules(
            {"keyword": "K", "fbclid": "x"}, rules=raw)
        assert slots == {"keyword": "K"}
        assert out == {"fills": {}, "applied": []}
