"""Tests for `app.resolution` — Vector 2.8 click parameter resolution.

Coverage targets:
  - `parse_param_mappings` defensive parsing of every shape that
    Redis sync can produce (JSON string, parsed list, malformed,
    None, dict).
  - `resolve_slots` correctness across all 4 priority levels of the
    PARAMETER-SYSTEM.md resolution chain.
  - Campaign-overrides-source-per-slot semantics for the alias map.
  - Source hardcoded fallback when campaign overrides alias but
    leaves `default_value` empty (subtle but spec-mandated case).
  - `extras` JSONB-equivalent capture of unmapped query params.
  - Edge cases: malformed entries, non-string slots, None mappings.

Pinned by `docs/design/PARAMETER-SYSTEM.md` §"Resolution chain"
and `docs/roadmap/stage-1-vector-1-2-implementation-plan.md` Phase 5.
"""

import pytest

from app.resolution import parse_param_mappings, resolve_slots


# ============================================================
# parse_param_mappings — defensive parsing
# ============================================================


class TestParseParamMappings:
    def test_none(self):
        assert parse_param_mappings(None) == []

    def test_empty_string(self):
        assert parse_param_mappings("") == []

    def test_empty_list(self):
        assert parse_param_mappings([]) == []

    def test_valid_list(self):
        data = [{"slot": "sub1", "alias": "creative", "default_value": None}]
        assert parse_param_mappings(data) == data

    def test_valid_json_string(self):
        raw = '[{"slot":"sub1","alias":"creative","default_value":null,"label":null}]'
        result = parse_param_mappings(raw)
        assert result == [
            {"slot": "sub1", "alias": "creative", "default_value": None, "label": None}
        ]

    def test_malformed_json_returns_empty(self):
        # Adversarial input — must NOT raise.
        result = parse_param_mappings("not-json{[")
        assert result == []

    def test_partial_json_returns_empty(self):
        result = parse_param_mappings('[{"slot":"sub1"')
        assert result == []

    def test_json_dict_returns_empty(self):
        # Pre-Stage-1 schema may have stored as dict; defensive.
        result = parse_param_mappings('{"sub1": "creative"}')
        assert result == []

    def test_unexpected_type_returns_empty(self):
        # int, bytes, etc. — defensive type guard.
        assert parse_param_mappings(123) == []
        assert parse_param_mappings(b"bytes") == []

    def test_json_with_extra_fields(self):
        # Tolerate unknown keys (forward-compat).
        raw = '[{"slot":"sub1","alias":"creative","default_value":"hc","label":"Creative ID","extra_field":"future"}]'
        result = parse_param_mappings(raw)
        assert len(result) == 1
        assert result[0]["slot"] == "sub1"


# ============================================================
# resolve_slots — Priority Level 1 (request URL)
# ============================================================


class TestPriority1RequestURL:
    def test_simple_alias_match(self):
        slots, _ = resolve_slots(
            query_params={"creative": "vid_42"},
            source_mappings=[{"slot": "sub1", "alias": "creative"}],
            campaign_mappings=None,
        )
        assert slots == {"sub1": "vid_42"}

    def test_slot_as_implicit_alias(self):
        # When alias is None/empty, the slot name itself is the URL key.
        slots, _ = resolve_slots(
            query_params={"sub1": "raw_value"},
            source_mappings=[{"slot": "sub1", "alias": None}],
            campaign_mappings=None,
        )
        assert slots == {"sub1": "raw_value"}

    def test_request_wins_over_hardcoded(self):
        # Request URL beats every hardcoded layer.
        slots, _ = resolve_slots(
            query_params={"gclid": "live_click"},
            source_mappings=[{"slot": "source_click_id", "alias": "gclid", "default_value": "src_default"}],
            campaign_mappings=[{"slot": "source_click_id", "alias": "gclid", "default_value": "campaign_default"}],
        )
        assert slots == {"source_click_id": "live_click"}

    def test_campaign_alias_wins_for_lookup(self):
        # Campaign aliases `gclid` → `source_click_id`. Source aliased same slot
        # via `gbraid`. The campaign-defined alias is what we look up.
        slots, _ = resolve_slots(
            query_params={"gclid": "g123", "gbraid": "gb456"},
            source_mappings=[{"slot": "source_click_id", "alias": "gbraid"}],
            campaign_mappings=[{"slot": "source_click_id", "alias": "gclid"}],
        )
        # Campaign alias wins → use `gclid` value.
        assert slots == {"source_click_id": "g123"}


# ============================================================
# resolve_slots — Priority Level 2 (campaign hardcoded)
# ============================================================


class TestPriority2CampaignHardcoded:
    def test_campaign_hardcoded_when_request_absent(self):
        slots, _ = resolve_slots(
            query_params={},
            source_mappings=None,
            campaign_mappings=[{"slot": "funnel_type", "default_value": "tripwire"}],
        )
        assert slots == {"funnel_type": "tripwire"}

    def test_campaign_hardcoded_beats_source_hardcoded(self):
        slots, _ = resolve_slots(
            query_params={},
            source_mappings=[{"slot": "pixel_id", "default_value": "src_pixel"}],
            campaign_mappings=[{"slot": "pixel_id", "default_value": "campaign_pixel"}],
        )
        assert slots == {"pixel_id": "campaign_pixel"}

    def test_empty_query_value_falls_through_to_campaign_hardcoded(self):
        # Advertiser sent ?source_click_id= (empty). Treat as no value.
        slots, _ = resolve_slots(
            query_params={"clk": ""},
            source_mappings=None,
            campaign_mappings=[{"slot": "source_click_id", "alias": "clk", "default_value": "campaign_fallback"}],
        )
        assert slots == {"source_click_id": "campaign_fallback"}


# ============================================================
# resolve_slots — Priority Level 3 (source hardcoded)
# ============================================================


class TestPriority3SourceHardcoded:
    def test_source_hardcoded_when_neither_above_filled(self):
        slots, _ = resolve_slots(
            query_params={},
            source_mappings=[{"slot": "pixel_id", "default_value": "fb_pixel_42"}],
            campaign_mappings=None,
        )
        assert slots == {"pixel_id": "fb_pixel_42"}

    def test_source_hardcoded_falls_through_when_campaign_alias_only(self):
        # Subtle case from PARAMETER-SYSTEM.md spec:
        # Campaign overrides slot's ALIAS but leaves `default_value`
        # null. Source still has its hardcoded value. Per spec line
        # 105-107 — source's hardcoded must apply as final fallback.
        slots, _ = resolve_slots(
            query_params={},  # no request value
            source_mappings=[{"slot": "pixel_id", "alias": "px", "default_value": "src_fallback"}],
            campaign_mappings=[{"slot": "pixel_id", "alias": "campaign_px", "default_value": None}],
        )
        # Campaign defined slot but no hardcoded; falls through to source's.
        assert slots == {"pixel_id": "src_fallback"}


# ============================================================
# resolve_slots — Priority Level 4 (NULL)
# ============================================================


class TestPriority4Null:
    def test_no_value_anywhere(self):
        slots, _ = resolve_slots(
            query_params={},
            source_mappings=[{"slot": "buyer_id"}],  # mapping but no default
            campaign_mappings=None,
        )
        assert slots == {"buyer_id": None}

    def test_request_alias_mismatch(self):
        # Query has key but it doesn't match the alias.
        slots, _ = resolve_slots(
            query_params={"unmapped_key": "v"},
            source_mappings=[{"slot": "buyer_id", "alias": "uid"}],
            campaign_mappings=None,
        )
        assert slots == {"buyer_id": None}

    def test_unmapped_slot_not_in_result(self):
        # Slot that no layer defines doesn't appear in slots.
        slots, _ = resolve_slots(
            query_params={"keyword": "trader"},
            source_mappings=[{"slot": "sub1"}],
            campaign_mappings=None,
        )
        # `keyword` slot was never mapped — not present in slots.
        assert "keyword" not in slots
        # `sub1` is mapped but no value → NULL.
        assert slots == {"sub1": None}


# ============================================================
# Merged map semantics — campaign overrides source per slot
# ============================================================


class TestMergedMap:
    def test_disjoint_slots_both_resolved(self):
        # Source defines sub1, campaign defines sub2.
        slots, _ = resolve_slots(
            query_params={"creative": "c1", "kw": "k1"},
            source_mappings=[{"slot": "sub1", "alias": "creative"}],
            campaign_mappings=[{"slot": "sub2", "alias": "kw"}],
        )
        assert slots == {"sub1": "c1", "sub2": "k1"}

    def test_campaign_alias_replaces_source_alias_for_lookup(self):
        # Source maps sub1 → "src_key"; campaign maps sub1 → "cmp_key".
        # Only "cmp_key" should be the lookup key.
        slots, _ = resolve_slots(
            query_params={"src_key": "from_src", "cmp_key": "from_cmp"},
            source_mappings=[{"slot": "sub1", "alias": "src_key", "default_value": "x"}],
            campaign_mappings=[{"slot": "sub1", "alias": "cmp_key"}],
        )
        # Campaign alias wins → lookup `cmp_key`.
        assert slots == {"sub1": "from_cmp"}

    def test_only_source_layer(self):
        slots, _ = resolve_slots(
            query_params={"creative": "v"},
            source_mappings=[{"slot": "sub1", "alias": "creative"}],
            campaign_mappings=None,
        )
        assert slots == {"sub1": "v"}

    def test_only_campaign_layer(self):
        slots, _ = resolve_slots(
            query_params={"funnel": "tripwire"},
            source_mappings=None,
            campaign_mappings=[{"slot": "funnel_type", "alias": "funnel"}],
        )
        assert slots == {"funnel_type": "tripwire"}

    def test_both_layers_none(self):
        slots, extras = resolve_slots(
            query_params={"random": "key"},
            source_mappings=None,
            campaign_mappings=None,
        )
        assert slots == {}
        assert extras == {"random": "key"}


# ============================================================
# Extras — unmapped keys
# ============================================================


class TestExtras:
    def test_unmapped_keys_in_extras(self):
        _, extras = resolve_slots(
            query_params={"creative": "v", "fbclid": "fb_xxx", "utm_source": "fb"},
            source_mappings=[{"slot": "sub1", "alias": "creative"}],
            campaign_mappings=None,
        )
        assert extras == {"fbclid": "fb_xxx", "utm_source": "fb"}

    def test_consumed_key_not_in_extras_even_with_empty_value(self):
        # Key used by mapping is NOT extras even when value is empty.
        slots, extras = resolve_slots(
            query_params={"clk": "", "fbclid": "fb_xxx"},
            source_mappings=[{"slot": "source_click_id", "alias": "clk", "default_value": "fallback"}],
            campaign_mappings=None,
        )
        assert slots == {"source_click_id": "fallback"}
        assert "clk" not in extras
        assert extras == {"fbclid": "fb_xxx"}

    def test_extras_excludes_none_values(self):
        # None query values shouldn't appear in extras.
        _, extras = resolve_slots(
            query_params={"a": "1", "b": None},
            source_mappings=None,
            campaign_mappings=None,
        )
        assert extras == {"a": "1"}

    def test_numeric_query_values_coerced_to_string(self):
        _, extras = resolve_slots(
            query_params={"age": 42, "active": True},
            source_mappings=None,
            campaign_mappings=None,
        )
        assert extras == {"age": "42", "active": "True"}


# ============================================================
# Edge cases — malformed entries, defensive guards
# ============================================================


class TestEdgeCases:
    def test_entry_without_slot_skipped(self):
        slots, _ = resolve_slots(
            query_params={"creative": "v"},
            source_mappings=[
                {"alias": "creative"},  # missing slot — skipped
                {"slot": "sub1", "alias": "creative"},
            ],
            campaign_mappings=None,
        )
        assert slots == {"sub1": "v"}

    def test_entry_with_empty_slot_skipped(self):
        slots, _ = resolve_slots(
            query_params={"creative": "v"},
            source_mappings=[
                {"slot": "", "alias": "creative"},  # empty slot — skipped
                {"slot": "sub1", "alias": "creative"},
            ],
            campaign_mappings=None,
        )
        assert slots == {"sub1": "v"}

    def test_entry_with_non_string_slot_skipped(self):
        slots, _ = resolve_slots(
            query_params={"creative": "v"},
            source_mappings=[
                {"slot": 42},  # non-string — skipped
                {"slot": "sub1", "alias": "creative"},
            ],
            campaign_mappings=None,
        )
        assert slots == {"sub1": "v"}

    def test_non_dict_entry_skipped(self):
        # E.g., null entry that snuck in via legacy data.
        slots, _ = resolve_slots(
            query_params={"creative": "v"},
            source_mappings=[None, {"slot": "sub1", "alias": "creative"}],
            campaign_mappings=None,
        )
        assert slots == {"sub1": "v"}

    def test_duplicate_slot_in_layer_keeps_last(self):
        # Admin-api validator prevents this, but defensive.
        slots, _ = resolve_slots(
            query_params={},
            source_mappings=[
                {"slot": "sub1", "default_value": "first"},
                {"slot": "sub1", "default_value": "second"},
            ],
            campaign_mappings=None,
        )
        # Last wins (dict insert overwrites).
        assert slots == {"sub1": "second"}

    def test_empty_default_value_treated_as_missing(self):
        # `""` default → no value → falls through to next priority.
        slots, _ = resolve_slots(
            query_params={},
            source_mappings=[{"slot": "sub1", "default_value": ""}],
            campaign_mappings=None,
        )
        assert slots == {"sub1": None}

    def test_whitespace_default_value_treated_as_missing(self):
        slots, _ = resolve_slots(
            query_params={},
            source_mappings=[{"slot": "sub1", "default_value": "   "}],
            campaign_mappings=None,
        )
        assert slots == {"sub1": None}

    def test_numeric_default_value_coerced(self):
        slots, _ = resolve_slots(
            query_params={},
            source_mappings=[{"slot": "buyer_id", "default_value": 42}],
            campaign_mappings=None,
        )
        assert slots == {"buyer_id": "42"}

    def test_alias_with_whitespace_trimmed(self):
        slots, _ = resolve_slots(
            query_params={"creative": "v"},
            source_mappings=[{"slot": "sub1", "alias": "  creative  "}],
            campaign_mappings=None,
        )
        assert slots == {"sub1": "v"}

    def test_empty_query_params(self):
        slots, extras = resolve_slots(
            query_params={},
            source_mappings=[{"slot": "sub1", "alias": "creative"}],
            campaign_mappings=None,
        )
        assert slots == {"sub1": None}
        assert extras == {}

    def test_json_string_mappings_through_parse_helper(self):
        # End-to-end: parse Redis JSON string → resolve.
        raw_src = '[{"slot":"sub1","alias":"creative","default_value":null}]'
        raw_cmp = '[{"slot":"sub1","alias":"creative","default_value":"hc"}]'
        slots, _ = resolve_slots(
            query_params={},
            source_mappings=parse_param_mappings(raw_src),
            campaign_mappings=parse_param_mappings(raw_cmp),
        )
        assert slots == {"sub1": "hc"}


# ============================================================
# Spec scenario — combined real-world example
# ============================================================


class TestSpecScenario:
    def test_facebook_source_with_campaign_pixel_override(self):
        """End-to-end: Facebook Source + Campaign that overrides pixel_id.

        Source maps:
          - sub1 ← creative
          - source_click_id ← fbclid
          - pixel_id default = "fb_default_pixel"
        Campaign overrides pixel_id with hardcoded "promo_pixel_42".
        Click arrives with ?creative=ad42&fbclid=fb_xxx&utm=remix.

        Expected:
          - sub1 = "ad42" (request)
          - source_click_id = "fb_xxx" (request)
          - pixel_id = "promo_pixel_42" (campaign hardcoded)
          - extras = {"utm": "remix"}
        """
        source_mappings = [
            {"slot": "sub1", "alias": "creative"},
            {"slot": "source_click_id", "alias": "fbclid"},
            {"slot": "pixel_id", "default_value": "fb_default_pixel"},
        ]
        campaign_mappings = [
            {"slot": "pixel_id", "default_value": "promo_pixel_42"},
        ]
        query_params = {
            "creative": "ad42",
            "fbclid": "fb_xxx",
            "utm": "remix",
        }
        slots, extras = resolve_slots(
            query_params=query_params,
            source_mappings=source_mappings,
            campaign_mappings=campaign_mappings,
        )
        assert slots == {
            "sub1": "ad42",
            "source_click_id": "fb_xxx",
            "pixel_id": "promo_pixel_42",
        }
        assert extras == {"utm": "remix"}

    def test_no_source_match_campaign_only_fallback(self):
        # Click arrives without ?source — only campaign mappings apply.
        slots, extras = resolve_slots(
            query_params={"funnel": "ladder"},
            source_mappings=None,  # no source matched
            campaign_mappings=[
                {"slot": "funnel_type", "alias": "funnel"},
                {"slot": "buyer_id", "default_value": "system_default"},
            ],
        )
        assert slots == {
            "funnel_type": "ladder",
            "buyer_id": "system_default",
        }
        assert extras == {}

    def test_source_routing_key_lands_in_extras_when_unmapped(self):
        """Pin behavior: `?source=<slug>` is the routing key consumed
        by `_resolve_source_for_click`, but `resolve_slots` has no
        knowledge of routing semantics. When neither the source nor
        the campaign maps the canonical slot `source`, the routing
        key flows into extras (Stage 3 will store it as JSONB).

        This is intentional — extras = "everything not slot-bound".
        Stage 3 storage spec MAY add a routing-key exclusion list,
        but per Vector 2.8 the contract is "extras carries it".
        """
        slots, extras = resolve_slots(
            query_params={"source": "fb", "creative": "v"},
            source_mappings=[{"slot": "sub1", "alias": "creative"}],
            campaign_mappings=None,
        )
        assert slots == {"sub1": "v"}
        # `source` was used by the router for slug lookup but the
        # mapping never bound it to a slot, so it lands in extras.
        assert extras == {"source": "fb"}

    def test_source_slot_explicitly_mapped_consumed(self):
        """Inverse case: when the canonical `source` slot IS mapped
        (admin defines it in source.param_mappings), the `?source`
        query key is consumed and does NOT appear in extras.
        """
        slots, extras = resolve_slots(
            query_params={"source": "fb", "creative": "v"},
            source_mappings=[
                {"slot": "source"},  # canonical slot, alias defaults to slot name
                {"slot": "sub1", "alias": "creative"},
            ],
            campaign_mappings=None,
        )
        assert slots == {"source": "fb", "sub1": "v"}
        assert extras == {}


# ============================================================
# Defensive caps + hardening (security-audit 2026-04-28)
# ============================================================


class TestDefensiveCaps:
    def test_oversized_json_string_rejected(self):
        # 256 KB cap — anything larger is dropped with a warning.
        big = "[" + ",".join(['{"slot":"sub1"}'] * 50000) + "]"
        assert len(big) > 256 * 1024
        result = parse_param_mappings(big)
        assert result == []

    def test_under_cap_json_string_accepted(self):
        # 200 KB worth of valid mappings still parses.
        entries = ['{"slot":"sub1","alias":"a","default_value":null,"label":"x"}'] * 3000
        raw = "[" + ",".join(entries) + "]"
        assert len(raw) > 100 * 1024
        assert len(raw) < 256 * 1024
        result = parse_param_mappings(raw)
        assert len(result) == 3000

    def test_bool_default_value_lowercased(self):
        """`_entry_default` mirrors `macros._coerce_value` for bools."""
        slots, _ = resolve_slots(
            query_params={},
            source_mappings=[{"slot": "sub1", "default_value": True}],
            campaign_mappings=None,
        )
        # Lowercase, matching macros.py behavior (not Python's "True").
        assert slots == {"sub1": "true"}

    def test_unknown_default_value_type_ignored(self):
        slots, _ = resolve_slots(
            query_params={},
            source_mappings=[{"slot": "sub1", "default_value": [1, 2, 3]}],
            campaign_mappings=None,
        )
        # Non-scalar defaults dropped rather than serialised as garbage.
        assert slots == {"sub1": None}
