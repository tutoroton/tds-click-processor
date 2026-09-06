"""G4.3 / I-2 — a foreign tenant's wall is not servable at the ENDPOINT.

WHY HERE AND NOT IN THE LOADER. `load_wall_candidates`' own docstring defers to
this box: *"a foreign tenant's wall is unreachable because its ids are never
asked for, not because this function rejects them. G4.3 tests that at the
endpoint, where a foreign tenant can actually be asked for."* The scope lists
ARE company-keyed (`walls:scope:{company}:…`), so that half is safe by
construction. `campaign:{id}:walls` is not keyed and is fetched unconditionally.

WHAT WAS MEASURED, AND IT IS NOT WHAT I EXPECTED TO WRITE
---------------------------------------------------------
Isolation HOLDS. A wall owned by another company, planted into this campaign's
list, is never served — including when it is the only wall present.

But `load_wall_candidates` does not reject it, and neither does `select_wall`.
The mechanism is `cascade._pick_winner`, which buckets candidates by
`(scope_type, scope_id) == click_levels[scope_type]`. A wall claiming another
company's `scope_id` matches no bucket and cannot be picked. **Tenancy on this
path is a side effect of scope matching, not a tenant check** — the wall
record's own `company_id` field is never read, which the fourth test pins by
serving a wall whose scope matches and whose `company_id` is foreign.

Mutation-calibrated 4/4 against that account (`_pick_winner` losing its
`scope_id` term goes RED). One correction worth carrying: dropping the
`scope_type` term was predicted RED and came back GREEN, because the surviving
`scope_id` comparison alone excludes the foreign wall. So **`scope_id` is the
term that carries tenancy here and `scope_type` is not** — a fact reading the
code did not settle, and the prediction was corrected rather than forced with a
contrived fixture.

WHY THIS LANE DOES NOT ADD A TENANT FILTER
-------------------------------------------
It is tempting, and `multi-tenant-isolation` does say a read path must hold on
its own rather than inherit a writer's invariant. Two measurements stopped it:

  1. `app/cascade.py` has the IDENTICAL shape — `campaign:{id}:flows` is fetched
     unconditionally and loaded records are never compared against the click's
     company. So this is the established property of the whole routing plane,
     not something the wall introduced. Fixing it only for walls would leave the
     artery unchanged and the two paths inconsistent.
  2. The publisher DOES write `company_id` into every `flow:{id}`
     (`sync/builders/flows.py:301`), so a fail-closed reader filter would be
     effective rather than a no-op — this is a real, buildable change, which is
     precisely why it deserves its own lane and its own blast-radius analysis on
     a <10 ms hot path, not a passenger seat in an offerwall PR.

Recorded as a finding rather than fixed in passing (`excellence-mandate`
§ Stewardship). What these tests deliver is the PROPERTY, stated at the
endpoint, so the day someone changes scope matching they are told that tenant
isolation was riding on it.
"""

import asyncio
import json

from tests.unit.test_wall_endpoint import (  # noqa: F401  (fixtures are used)
    CAMPAIGN,
    COMPANY,
    OFFER_1,
    TARGET_1,
    WALL_A,
    _fake,
    _post,
    _seed,
    armed,
)

FOREIGN_COMPANY = COMPANY + 1
# NOT "911" — that is `test_wall_endpoint.WALL_B`. The default seed does not
# create it, so a clash would not fail today; it would fail the day someone
# adds `walls=("A","B")` here, as a confusing wrong answer rather than an error.
FOREIGN_WALL = "920"


def _plant_foreign_wall(store) -> None:
    """Put a wall owned by ANOTHER company into this campaign's wall list.

    Deliberately a shape the publisher would never produce — that is the point.
    A test that only fed correctly-published data would re-prove the publisher
    and say nothing about whether the reader is independently safe.
    """
    r = store.client()

    async def _go():
        await r.hset(f"flow:{FOREIGN_WALL}", mapping={
            "campaign_id": CAMPAIGN,
            "scope_type": "company",
            "scope_id": str(FOREIGN_COMPANY),
            "seq_id": "1",
            "is_default": "0",
            "criteria": "[]",
            "audience": "offerwall",
            "action_type": "offerwall",
            "company_id": str(FOREIGN_COMPANY),
            "action_config": json.dumps(
                {"tiles": [{"offer_id": OFFER_1, "target_id": TARGET_1}]}),
        })
        # PREPENDED so it is the newest — `load_wall_candidates` admits
        # newest-first, and selection would reach it before the legitimate one.
        # Planting it where it could not win anyway would be a test that cannot
        # fail.
        await r.rpush(f"campaign:{CAMPAIGN}:walls", FOREIGN_WALL)

    asyncio.run(_go())


class TestAForeignWallIsNotServed:
    def test_a_wall_owned_by_another_company_never_reaches_the_answer(self, armed):  # noqa: F811
        store = _fake()
        asyncio.run(_seed(store.client()))
        _plant_foreign_wall(store)

        body = _post(store).json()

        # The legitimate wall may serve; the foreign one may not — asserted on
        # the ID rather than on `matched`, because "a wall was served" and "the
        # RIGHT wall was served" are different claims and only the second is
        # about tenancy.
        assert body.get("wall_id") != int(FOREIGN_WALL), (
            "a wall belonging to company %d was served to company %d"
            % (FOREIGN_COMPANY, COMPANY)
        )

    def test_the_legitimate_wall_still_serves_the_calibration(self, armed):  # noqa: F811
        # Without this, the assertion above is satisfied by an endpoint that
        # serves NO wall at all, which would be a different defect wearing the
        # shape of a passing isolation test.
        store = _fake()
        asyncio.run(_seed(store.client()))
        body = _post(store).json()
        assert body["matched"] is True
        assert body["wall_id"] == int(WALL_A)

    def test_a_foreign_wall_ALONE_serves_nothing(self, armed):  # noqa: F811
        # The sharpest form: no legitimate wall to hide behind. If the reader
        # has no tenant check, this answers `matched: true` with the foreign
        # wall's tiles — the leak, undisguised.
        store = _fake()
        asyncio.run(_seed(store.client(), walls=()))
        _plant_foreign_wall(store)

        body = _post(store).json()
        assert body["matched"] is False, (
            "a foreign tenant's wall was served on a campaign with no wall of "
            "its own — the campaign-bound path has no independent tenant check"
        )


# ---------------------------------------------------------------------------
# WHICH MECHANISM ACTUALLY REJECTS IT - a green whose cause you cannot name is
# not evidence.
#
# The three tests above pass, and my reading of `load_wall_candidates` could not
# say why: `company_id` is used ONLY to build the scope-list keys. Reading the
# selector answers it - `_pick_winner` buckets candidates by
# `(scope_type, scope_id) == click_levels[scope_type]`, and the click's company
# level is its own. A wall claiming `scope_id=<foreign company>` therefore
# matches no bucket and can never be picked.
#
# 🔴 SO TENANCY ON THIS PATH IS A SIDE EFFECT OF SCOPE MATCHING, not a tenant
# check. The wall record's own `company_id` field is never read. This test
# states that as a measured property rather than leaving it an inference,
# because the next person to touch scope matching needs to know that tenant
# isolation rides on it.
# ---------------------------------------------------------------------------

MASKED_WALL = "921"


def _plant_scope_matching_foreign_wall(store) -> None:
    r = store.client()

    async def _go():
        await r.hset(f"flow:{MASKED_WALL}", mapping={
            "campaign_id": CAMPAIGN,
            # SCOPE says the click's own company...
            "scope_type": "company",
            "scope_id": str(COMPANY),
            # ...seq_id 0 beats the legitimate wall's 1, so if this is admitted
            # at all it WINS. A planted row that could not win either way would
            # make a pass unreadable.
            "seq_id": "0",
            "is_default": "0",
            "criteria": "[]",
            "audience": "offerwall",
            "action_type": "offerwall",
            # ...while the record's OWN company field says otherwise.
            "company_id": str(FOREIGN_COMPANY),
            "action_config": json.dumps(
                {"tiles": [{"offer_id": OFFER_1, "target_id": TARGET_1}]}),
        })
        await r.rpush(f"campaign:{CAMPAIGN}:walls", MASKED_WALL)

    asyncio.run(_go())


class TestWhatTheProtectionActuallyIS:
    def test_scope_is_the_mechanism_and_the_company_field_is_not_read(self, armed):  # noqa: F811
        """PINS THE MECHANISM. It does not endorse it.

        A wall whose scope matches the click is served even though its own
        `company_id` names another tenant - so the isolation proven above comes
        from scope matching, not from a tenant check on the read side.

        Whether that is ACCEPTABLE is a separate question, answered in the
        module docstring and in the PR: the scope lists are company-keyed, so
        the only way such a row reaches the pool is `campaign:{id}:walls`, which
        only the publisher writes. The exposure is bounded by the publisher's
        correctness - exactly the dependency rule `multi-tenant-isolation` says
        a read path must not have.
        """
        store = _fake()
        asyncio.run(_seed(store.client()))
        _plant_scope_matching_foreign_wall(store)

        body = _post(store).json()

        assert body["matched"] is True
        assert body["wall_id"] == int(MASKED_WALL), (
            "EXPECTED the scope-matching wall to win on seq_id. If this fails, "
            "something DOES read the record's company_id and the module "
            "docstring's account of the mechanism is wrong - re-derive it "
            "rather than adjusting this assertion."
        )
