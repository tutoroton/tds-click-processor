"""Tests for `app.resolution` — Vector 2.8 click parameter resolution.

Coverage targets:
  - `parse_param_mappings` defensive parsing of every shape that
    Redis sync can produce (JSON string, parsed list, malformed,
    None, dict).
  - `resolve_slots` correctness across all 4 priority levels of the
    SOURCE-WINS resolution chain.
  - SOURCE-overrides-campaign-per-slot semantics for the alias map
    (SOURCE specializes the campaign on conflict, 2026-06-02).
  - Campaign hardcoded fallback when the source defines the slot's
    alias but leaves `default_value` empty (subtle but spec-mandated
    case — campaign is now the FALLBACK layer).
  - `extras` JSONB-equivalent capture of unmapped query params.
  - Edge cases: malformed entries, non-string slots, None mappings.

Pinned by the SOURCE-WINS contract
`docs/development/param-source-campaign-overrides-2026-06-02.md`
(DESIGN LOCKED 2026-06-02) — `source_mappings` is the EFFECTIVE source
layer (per-link override or source global). Priority:
`URL > effective_source > campaign > NULL`.
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

    def test_source_alias_wins_for_lookup(self):
        # SOURCE-WINS (2026-06-02): source aliases `gbraid` →
        # `source_click_id`; campaign aliases the same slot via `gclid`.
        # The SOURCE-defined alias is what we look up (source specializes
        # the campaign).
        slots, _ = resolve_slots(
            query_params={"gclid": "g123", "gbraid": "gb456"},
            source_mappings=[{"slot": "source_click_id", "alias": "gbraid"}],
            campaign_mappings=[{"slot": "source_click_id", "alias": "gclid"}],
        )
        # Source alias wins → use `gbraid` value.
        assert slots == {"source_click_id": "gb456"}


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

    def test_source_hardcoded_beats_campaign_hardcoded(self):
        # SOURCE-WINS (2026-06-02): source hardcoded default beats the
        # campaign hardcoded default — the source specializes the campaign.
        slots, _ = resolve_slots(
            query_params={},
            source_mappings=[{"slot": "pixel_id", "default_value": "src_pixel"}],
            campaign_mappings=[{"slot": "pixel_id", "default_value": "campaign_pixel"}],
        )
        assert slots == {"pixel_id": "src_pixel"}

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

    def test_campaign_hardcoded_falls_through_when_source_alias_only(self):
        # Subtle SOURCE-WINS case (2026-06-02): the SOURCE defines the
        # slot's ALIAS but leaves `default_value` null. The CAMPAIGN still
        # has its hardcoded value. Since the source layer is checked first
        # but contributes no default, the campaign hardcoded applies as the
        # final fallback (campaign is now the FALLBACK layer).
        slots, _ = resolve_slots(
            query_params={},  # no request value
            source_mappings=[{"slot": "pixel_id", "alias": "px", "default_value": None}],
            campaign_mappings=[{"slot": "pixel_id", "alias": "campaign_px", "default_value": "cmp_fallback"}],
        )
        # Source defined slot but no hardcoded; falls through to campaign's.
        assert slots == {"pixel_id": "cmp_fallback"}


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

    def test_canonical_slot_autobinds_without_mapping(self):
        """F.X (2026-05-14) — canonical-binding rule.

        Pre-F.X: a canonical slot like ``keyword`` only appeared in
        the result when an entry on source/campaign explicitly
        enumerated it. The same-named GET key was a stranger that
        landed in ``extras``.

        Post-F.X: every name in ``CANONICAL_SLOTS`` (39 names) is a
        primary input key for its slot regardless of whether an
        entry exists. A same-named GET key auto-binds.

        Explicitly mapped slots that didn't resolve still emit
        ``None`` so consumers (``build_url``) see the slot is present
        but empty — same contract as pre-F.X.
        """
        slots, extras = resolve_slots(
            query_params={"keyword": "trader"},
            source_mappings=[{"slot": "sub1"}],
            campaign_mappings=None,
        )
        # `keyword` is canonical → auto-binds from same-named key.
        # `sub1` is explicitly mapped but has no resolvable value → NULL.
        assert slots == {"keyword": "trader", "sub1": None}
        # `keyword` is the canonical input — never an extras leak.
        assert extras == {}


# ============================================================
# Merged map semantics — source overrides campaign per slot (SOURCE-WINS)
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

    def test_source_alias_replaces_campaign_alias_for_lookup(self):
        # SOURCE-WINS (2026-06-02): source maps sub1 → "src_key";
        # campaign maps sub1 → "cmp_key". The SOURCE alias is the lookup
        # key (source specializes the campaign).
        slots, _ = resolve_slots(
            query_params={"src_key": "from_src", "cmp_key": "from_cmp"},
            source_mappings=[{"slot": "sub1", "alias": "src_key", "default_value": "x"}],
            campaign_mappings=[{"slot": "sub1", "alias": "cmp_key"}],
        )
        # Source alias wins → lookup `src_key`.
        assert slots == {"sub1": "from_src"}

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
# Finding #4 — dual-aliased slot (eff_source AND campaign)
# ============================================================


class TestFinding4DualAliasFallback:
    """Finding #4 (2026-06-03): when a slot is aliased by BOTH the
    effective-source layer AND the campaign with *different* alias
    keys, the campaign alias must (a) act as a real URL fallback after
    the source alias — `URL(canonical > eff_source.alias >
    campaign.alias)` — and (b) be consumed so the losing alias key
    never bleeds into `extras`. Pre-fix `get_keys` only ever held the
    source alias, so a campaign-alias-only click resolved NULL and the
    campaign-alias key leaked into extras (measured: `ckw=CMPALIAS`).
    """

    def test_dual_alias_source_wins_and_no_bleed(self):
        # Both alias keys present: source alias `ekw` and campaign
        # alias `ckw` both map `keyword`. Source value wins (SOURCE-
        # WINS) AND the losing campaign-alias key must NOT leak into
        # extras — this is the bleed we measured live.
        slots, extras = resolve_slots(
            query_params={"ekw": "SRCALIAS", "ckw": "CMPALIAS"},
            source_mappings=[{"slot": "keyword", "alias": "ekw"}],
            campaign_mappings=[{"slot": "keyword", "alias": "ckw"}],
        )
        assert slots["keyword"] == "SRCALIAS"
        assert "ckw" not in extras
        assert "ekw" not in extras
        assert extras == {}

    def test_dual_alias_campaign_alias_only_resolves_via_campaign(self):
        # Latent value bug: with ONLY the campaign-alias key present
        # (no canonical, no source-alias key), the slot must resolve
        # from the campaign alias — not empty. Pre-fix `get_keys` held
        # only the source alias, so this resolved NULL.
        slots, extras = resolve_slots(
            query_params={"ckw": "CMPALIAS"},
            source_mappings=[{"slot": "keyword", "alias": "ekw"}],
            campaign_mappings=[{"slot": "keyword", "alias": "ckw"}],
        )
        assert slots["keyword"] == "CMPALIAS"
        assert "ckw" not in extras
        assert extras == {}

    def test_dual_alias_canonical_beats_both(self):
        # Canonical slot name still trumps BOTH aliases on collision,
        # and neither alias key bleeds.
        slots, extras = resolve_slots(
            query_params={
                "keyword": "CANON", "ekw": "SRCALIAS", "ckw": "CMPALIAS",
            },
            source_mappings=[{"slot": "keyword", "alias": "ekw"}],
            campaign_mappings=[{"slot": "keyword", "alias": "ckw"}],
        )
        assert slots["keyword"] == "CANON"
        assert extras == {}

    def test_regression_single_layer_alias_and_unknown_extras(self):
        # Regression: the dual-alias change must not disturb the
        # single-layer path — source-only alias resolves, an unknown
        # param still lands in extras.
        slots, extras = resolve_slots(
            query_params={"creative": "v42", "mk": "nonce"},
            source_mappings=[{"slot": "sub1", "alias": "creative"}],
            campaign_mappings=None,
        )
        assert slots["sub1"] == "v42"
        assert extras == {"mk": "nonce"}


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
    def test_facebook_source_pixel_wins_over_campaign(self):
        """End-to-end SOURCE-WINS (2026-06-02): Facebook Source + Campaign
        that ALSO declares a pixel_id default.

        Source maps:
          - sub1 ← creative
          - source_click_id ← fbclid
          - pixel_id default = "fb_default_pixel"
        Campaign also hardcodes pixel_id = "promo_pixel_42".
        Click arrives with ?creative=ad42&fbclid=fb_xxx&utm=remix.

        Expected (SOURCE specializes the campaign on the pixel_id slot):
          - sub1 = "ad42" (request)
          - source_click_id = "fb_xxx" (request)
          - pixel_id = "fb_default_pixel" (SOURCE hardcoded wins)
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
            "pixel_id": "fb_default_pixel",
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

    def test_source_routing_key_autobinds_canonical_slot(self):
        """F.X (2026-05-14) — canonical-binding rule.

        Pre-F.X: ``?source=<slug>`` was the routing key consumed by
        ``_resolve_source_for_click`` for slug lookup, but
        ``resolve_slots`` had no knowledge of routing semantics. When
        neither layer mapped the canonical slot ``source``, the
        routing key flowed into extras.

        Post-F.X: ``source`` is canonical, so the GET key
        auto-binds — both as the routing slug AND as the value of
        ``{source}`` URL macro. The two roles intentionally overlap.
        Routing identification by slug happens via
        ``_resolve_source_for_click`` (a separate hardcoded read of
        ``query_params['source']``), and ``resolve_slots`` separately
        populates the same value into ``slots['source']``. Operators
        wanting the macro to differ from the source slug must alias
        the slot (e.g., ``{slot:'source', alias:'src_value'}``);
        canonical-first then still emits ``slots['source']`` from
        ``?source=`` because canonical wins on collision.
        """
        slots, extras = resolve_slots(
            query_params={"source": "fb", "creative": "v"},
            source_mappings=[{"slot": "sub1", "alias": "creative"}],
            campaign_mappings=None,
        )
        # `source` canonical-auto-binds; `sub1` resolves through the
        # explicit alias entry.
        assert slots == {"source": "fb", "sub1": "v"}
        # No extras — both keys are claimed by slots.
        assert extras == {}

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
# SOURCE-WINS contract (2026-06-02) — source specializes campaign
# ============================================================
#
# Pins the inverted tiebreak from
# `docs/development/param-source-campaign-overrides-2026-06-02.md`.
# Priority: URL > effective_source > campaign > NULL. The source layer
# passed to `resolve_slots` is the EFFECTIVE source (per-link override OR
# source global) — the router picks which one; from `resolve_slots`'s POV
# it is simply "whatever source mappings were handed in".


class TestSourceWins:
    def test_source_alias_wins_over_campaign_alias(self):
        # (a) Both layers alias the SAME slot to DIFFERENT URL keys, both
        # present in the URL. Source alias wins the lookup.
        slots, _ = resolve_slots(
            query_params={"s_key": "from_src", "c_key": "from_cmp"},
            source_mappings=[{"slot": "keyword", "alias": "s_key"}],
            campaign_mappings=[{"slot": "keyword", "alias": "c_key"}],
        )
        assert slots == {"keyword": "from_src"}

    def test_source_default_wins_over_campaign_default(self):
        # (b) Both layers hardcode the SAME slot, no URL value. Source
        # hardcoded default wins.
        slots, _ = resolve_slots(
            query_params={},
            source_mappings=[{"slot": "funnel_type", "default_value": "src_funnel"}],
            campaign_mappings=[{"slot": "funnel_type", "default_value": "cmp_funnel"}],
        )
        assert slots == {"funnel_type": "src_funnel"}

    def test_url_still_beats_source_default(self):
        # URL ALWAYS wins — even over the source default (full chain order).
        slots, _ = resolve_slots(
            query_params={"funnel_type": "url_funnel"},
            source_mappings=[{"slot": "funnel_type", "default_value": "src_funnel"}],
            campaign_mappings=[{"slot": "funnel_type", "default_value": "cmp_funnel"}],
        )
        assert slots == {"funnel_type": "url_funnel"}

    def test_campaign_fills_slot_source_did_not_specialize(self):
        # Non-conflict: source specializes slot X, campaign owns slot Y.
        # Both resolve — campaign is the fallback layer, not suppressed.
        slots, _ = resolve_slots(
            query_params={},
            source_mappings=[{"slot": "pixel_id", "default_value": "src_pixel"}],
            campaign_mappings=[{"slot": "funnel_type", "default_value": "cmp_funnel"}],
        )
        assert slots == {"pixel_id": "src_pixel", "funnel_type": "cmp_funnel"}

    def test_effective_source_override_replaces_global(self):
        # (c) The EFFECTIVE source layer is the per-link override when set.
        # `resolve_slots` receives whichever the router chose — so passing
        # the override list (NOT the source global) yields override-driven
        # resolution. Here the override hardcodes pixel_id = "override_px";
        # were the GLOBAL ["fb_default_pixel"] passed instead, the result
        # would differ — proving the override list fully REPLACES the
        # global (it is not merged).
        per_link_override = [{"slot": "pixel_id", "default_value": "override_px"}]
        source_global = [{"slot": "pixel_id", "default_value": "fb_default_pixel"}]
        campaign_mappings = [{"slot": "pixel_id", "default_value": "cmp_px"}]

        slots_override, _ = resolve_slots(
            query_params={},
            source_mappings=per_link_override,
            campaign_mappings=campaign_mappings,
        )
        assert slots_override == {"pixel_id": "override_px"}

        # Contrast: the global would have produced the global's value —
        # confirming the two source layers are mutually exclusive (replace,
        # not merge).
        slots_global, _ = resolve_slots(
            query_params={},
            source_mappings=source_global,
            campaign_mappings=campaign_mappings,
        )
        assert slots_global == {"pixel_id": "fb_default_pixel"}


# ============================================================
# F.X canonical-binding rule — contract-pinning tests
# ============================================================
#
# These tests pin the canonical-binding contract introduced by
# F.X (locked 2026-05-14). Plan doc:
# `docs/roadmap/stage-1a-research/canonical-slot-binding-fix.md`.
# Each test exercises a specific cell of the decision matrix in
# § 3 of that document.


class TestCanonicalBindingFX:
    def test_canonical_source_binds_without_mapping(self):
        """Plan § 3 — canonical-only iteration.

        Empty mappings on both layers. ``?source=fb`` MUST land in
        ``slots['source']`` because ``source`` is a canonical slot
        name. Other 38 canonical slots auto-iterate but resolve to
        NULL and aren't explicitly mapped → omitted from result.
        """
        slots, extras = resolve_slots(
            query_params={"source": "fb"},
            source_mappings=None,
            campaign_mappings=None,
        )
        assert slots == {"source": "fb"}
        assert extras == {}

    def test_canonical_sub1_binds_without_mapping(self):
        """Plan § 3 — SUB_SLOTS coverage.

        ``sub1`` is part of ``SUB_SLOTS`` (sub1..sub20). The
        canonical-binding rule applies to it identically to
        ``RESERVED_SLOTS`` members. ``?sub1=fb`` MUST bind without
        any explicit mapping entry.
        """
        slots, extras = resolve_slots(
            query_params={"sub1": "fb"},
            source_mappings=None,
            campaign_mappings=None,
        )
        assert slots == {"sub1": "fb"}
        assert extras == {}

    def test_canonical_wins_over_alias_on_collision(self):
        """Plan § 3 — decision matrix row "canonical wins on collision".

        When both ``?source=`` AND ``?s=`` arrive AND the source
        defines ``{slot:'source', alias:'s'}``, canonical wins:
        ``slots['source']`` resolves to the canonical key's value,
        not the alias key's. Both keys are marked examined so
        neither leaks to extras.
        """
        slots, extras = resolve_slots(
            query_params={"source": "fb", "s": "other"},
            source_mappings=[{"slot": "source", "alias": "s"}],
            campaign_mappings=None,
        )
        assert slots == {"source": "fb"}
        # `s` is the alias — also stripped from extras.
        assert extras == {}

    def test_alias_works_when_canonical_absent(self):
        """Plan § 3 — alias as additional input key.

        When the canonical-named GET key is absent, the alias is
        the secondary input. ``?s=fb`` resolves ``slots['source']``
        when an entry maps ``source`` to alias ``s``.
        """
        slots, extras = resolve_slots(
            query_params={"s": "fb"},
            source_mappings=[{"slot": "source", "alias": "s"}],
            campaign_mappings=None,
        )
        assert slots == {"source": "fb"}
        assert extras == {}

    def test_canonical_name_never_in_extras(self):
        """Plan § 3 / § 5 design rule — canonical guard on extras.

        Even when the canonical slot auto-iterated and resolved
        successfully, the canonical-name GET key is stripped from
        ``extras`` (defence-in-depth — the key is semantically the
        slot's input regardless of whether the resolution loop
        added the key to ``examined_keys`` or not).
        """
        _, extras = resolve_slots(
            query_params={"source": "fb"},
            source_mappings=None,
            campaign_mappings=None,
        )
        assert "source" not in extras

    def test_canonical_resolves_via_campaign_default(self):
        """Plan § 3 — priority chain still applies under canonical-binding.

        When the canonical GET key is absent and no alias entry
        exists, the campaign's hardcoded ``default_value`` fills
        the slot. The auto-iteration of canonical slots doesn't
        bypass the priority chain — it only adds the slot to the
        iteration set.
        """
        slots, extras = resolve_slots(
            query_params={},
            source_mappings=None,
            campaign_mappings=[{"slot": "source", "default_value": "organic"}],
        )
        assert slots == {"source": "organic"}
        assert extras == {}

    def test_alias_collision_with_other_canonical_fills_only_canonical_slot(self):
        """Plan § 3 — alias pointing at ANOTHER canonical name.

        An admin who maps ``{slot:'source', alias:'host'}`` is doing
        something unusual: the alias name is itself a canonical
        slot (``host``). The canonical-first rule resolves this
        deterministically:

          - ``?host=X`` populates the canonical ``host`` slot first
            (auto-iteration of ``host`` consumes the GET key).
          - The ``source`` slot's alias lookup then sees ``host``
            already examined; the value-resolution loop ALSO
            reads ``query_params['host']`` (the get_keys list for
            the ``source`` slot is ``[source, host]`` since alias
            is set). So ``slots['source']`` ALSO resolves to ``X``.

        Both slots get the same value — the admin's choice. The
        admin-api alias-collision validator (Phase 6 of the plan,
        optional) would warn at save time so the operator
        understands the consequence. This test pins the runtime
        behaviour either way.
        """
        slots, extras = resolve_slots(
            query_params={"host": "example.com"},
            source_mappings=[{"slot": "source", "alias": "host"}],
            campaign_mappings=None,
        )
        # Both canonical `host` (auto-bind) and `source`
        # (alias-lookup) read the same GET key.
        assert slots == {"host": "example.com", "source": "example.com"}
        # `host` is canonical — never in extras regardless.
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
