"""Canonical slot registry for the click-processor.

Mirror of `services/admin-api/app/common/parameters.py` —
`RESERVED_SLOTS` (19) + `SUB_SLOTS` (`sub1`..`sub20`). Combined,
the 39 canonical click-row slot names that participate in the
canonical-binding rule documented in
`docs/roadmap/stage-1a-research/canonical-slot-binding-fix.md`
(F.X plan locked 2026-05-14).

**Why a copy and not an import:** click-processor is the public
mirror that runs on every edge node (`tutoroton/tds-click-processor`,
deployed via `git subtree push`). It MUST NOT depend on admin-api
code — the two services are deployed independently, run on different
servers, and the click-processor sees admin-api only through the
Redis sync snapshot. The canonical registry is small + stable
(Round 2 locked 2026-04-27) and changes are rare, so the cost of
keeping a manual copy is far lower than the alternative of bundling
admin-api code at the edge.

**Drift detection (canonical contract):** the admin-api copy at
`services/admin-api/app/common/parameters.py` is the source of
truth. Any change to that file MUST land in the SAME COMMIT as a
mirroring change here, per the `context-consistency` rule. The
`agent-context-discipline` rule's "Adding Fields" cascade is the
mechanical checklist; the F.X plan doc § 4 row "Where canonical
list lives in click-processor" is the design pin.
"""

from __future__ import annotations


# ============================================================
# Configurable slot names — mirror of admin-api source-of-truth
# ============================================================
#
# IF YOU CHANGE THIS: also update
# `services/admin-api/app/common/parameters.py` (RESERVED_SLOTS,
# SUB_SLOTS) in the SAME commit. Drift between the two breaks
# canonical-binding semantics silently — clicks would auto-bind on
# one side and not the other depending on which name was added or
# removed. The two lists MUST match byte-for-byte.

# 19 reserved slot names. Each has a dedicated click-row column on
# the admin-api side (Stage 3 storage target). Source-platform
# attribution + click identifiers + funnel/journey + org/buyer
# attribution + mobile/app. Locked at 19 in Round 2 (2026-04-27).
RESERVED_SLOTS: frozenset[str] = frozenset({
    # Group A — Source identity
    "source",          # which traffic source the click came from
    "host",            # incoming hostname (multi-tenant landing domain)
    "placement",       # where on the source the ad ran
    # Group B — Source-platform attribution
    "ad_campaign_id",  # advertiser-side campaign id
    "adset_id",        # ad-set / ad-group id on the platform
    "ad_id",           # individual ad id
    "creative_id",     # creative / variant id
    "keyword",         # search / paid keyword
    # Group C — Click identifiers
    "source_click_id", # cross-platform click id (gclid, fbclid, ttclid…)
    "pixel_id",        # tracking-pixel id used by the advertiser
    # Group D — Funnel / journey
    "funnel_id",       # opaque funnel identifier
    "funnel_type",     # funnel category
    "funnel_click_id", # click id local to the funnel
    "funnel_user_id",  # funnel's own stable user identifier (L2 returning-user identity anchor — P1 2026-06-05, dark until P2)
    "subscribe_id",    # subscription / opt-in id
    "landing_id",      # landing page id
    # Group E — Org / buyer attribution
    "buyer_id",        # internal media-buyer attribution
    "user_id",         # advertiser-side user identifier
    "external_id",     # generic external reference
    # Group F — Mobile / app
    "app_id",          # mobile app identifier
})

# 20 configurable sub-slots — flexible per-Source containers.
# Aliased to incoming GET keys via `sources.param_mappings[].alias`.
SUB_SLOTS: frozenset[str] = frozenset(f"sub{i}" for i in range(1, 21))

# The full canonical click-row vocabulary (39 names). Every name
# in this set is treated as a *primary* GET key for its slot by
# `resolve_slots` — see F.X plan § 3 for the binding rule and
# § 4 for the design decision matrix.
CANONICAL_SLOTS: frozenset[str] = RESERVED_SLOTS | SUB_SLOTS


__all__ = [
    "RESERVED_SLOTS",
    "SUB_SLOTS",
    "CANONICAL_SLOTS",
]
