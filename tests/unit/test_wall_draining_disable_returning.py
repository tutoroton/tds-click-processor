"""U3 / defect №3 — `draining` × `disable_returning_flows` on a WALL campaign.

THE RULE, from the owner, quoted rather than paraphrased (П4, adjudicated in
``63-THE-LINE-18-DECIDED.md`` §3): «можемо включити дрейнінг, і тоді для
повторних там буде доступно… і це має впливати».

THE INPUT WHERE CODE AND RULE VISIBLY DISAGREED: a wall campaign with
``disable_returning_flows=ON``, a REPEAT visitor, and a tile naming a
``draining`` target. The rule sends them to the offer they chose. The code
refused the tile and served the first offer instead. Two destinations, both
visible to the visitor — which is what made this a defect and not a fork in the
road, and why it was reclassified out of the eighteen open questions.

WHY THE FLAG WAS THE WRONG INPUT. ``disable_returning_flows`` switches off the
returning-audience PARTITION — whether returning FLOWS run. It never said
anything about which availability classes a repeat visitor may be served. One
flag, two effects, and only one of them named where an operator would look.

SCOPE, from the decision verbatim: «на ВІТРИННИХ кампаніях… Звичайні кампанії не
чіпаємо» (invariant 1). The ordinary route is unchanged and still pinned by
``test_c2_availability_delivery.py``; the PARTITION is unchanged on both.

🔴 THE STRUCTURAL HALF IS NOT DECORATION — IT IS WHERE THE FIRST REPAIR FAILED.
The rule was written out TWICE: in ``_allowed_availability`` and as the
cascade's ``returning_visitor=`` argument, while ``_allowed_availability``'s own
docstring claimed the two shared it "BY CONSTRUCTION". They shared an INPUT
PATTERN, which is not the same thing and cannot be checked. Fixing only the
first copy produced a wall whose every tile was ``draining`` serving NOBODY
while a wall with one draining tile served it — same visitor, same flag, two
answers. Measured, not reasoned: that is exactly what the first version of this
repair did, and what the T3 matrix caught. Hence the source-level pin below.
"""

from __future__ import annotations

from unittest.mock import patch

from app import router
from app.config import settings

from tests.unit.test_wall_draining_matrix import TILE_1, TILE_2, _run, _served

_WALL = {"flow_family": "offerwall"}
_ORDINARY = {"flow_family": "standard"}
_NO_FAMILY: dict = {}          # every campaign that predates the field


def _applies(campaign: dict, *, seen_before: bool, disable_returning: bool,
             routing: bool = True, delivery: bool = True) -> bool:
    """`_draining_class_applies` with every input set explicitly.

    `disable_returning_flows` rides on the campaign hash, so it is merged in
    here rather than in the module-level constants — the point of these tests is
    that two campaigns differing ONLY in that field can behave the same.
    """
    c = dict(campaign)
    c["disable_returning_flows"] = "true" if disable_returning else "false"
    # The per-company opt-in, which `_returning_live` reads FREE off the same
    # hash. Set explicitly: without it the predicate is False for every input
    # and every assertion here would pass or fail for the wrong reason.
    c["returning_routing"] = "true"
    with patch.object(settings, "returning_routing_enabled", routing), \
            patch.object(settings, "wall_delivery_enabled", delivery):
        return router._draining_class_applies(c, seen_before)


class TestTheWallCampaignFollowsFreshness:
    def test_a_repeat_visitor_keeps_the_class_with_the_flag_ON(self):
        assert _applies(_WALL, seen_before=True, disable_returning=True) is True

    def test_and_with_the_flag_OFF_too_the_flag_is_simply_not_an_input(self):
        assert _applies(_WALL, seen_before=True, disable_returning=False) is True

    def test_a_FRESH_visitor_does_NOT_get_the_class(self):
        # The discriminator. Without it every assertion above is satisfied by a
        # predicate that returns True unconditionally.
        assert _applies(_WALL, seen_before=False, disable_returning=True) is False

    def test_a_DEAD_resolver_removes_the_class_even_for_a_repeat_visitor(self):
        # The second thing that can still say no — the class is derived from a
        # live returning layer, not asserted.
        assert _applies(_WALL, seen_before=True, disable_returning=True,
                        routing=False) is False


class TestTheORDINARYRouteIsUntouched:
    """Invariant 1 of the decision, and the reason this is a narrow repair."""

    def test_the_flag_still_removes_the_class_on_an_ordinary_campaign(self):
        assert _applies(_ORDINARY, seen_before=True, disable_returning=True) is False

    def test_and_without_the_flag_an_ordinary_repeat_visitor_keeps_it(self):
        # The control: the row above is the FLAG acting, not the fixture
        # refusing everything.
        assert _applies(_ORDINARY, seen_before=True, disable_returning=False) is True

    def test_a_campaign_with_NO_family_field_is_treated_as_ordinary(self):
        # Every campaign that predates migration 327 arrives this way. If this
        # ever flipped, the repair would have escaped its scope into the whole
        # estate without a single test changing colour.
        assert _applies(_NO_FAMILY, seen_before=True, disable_returning=True) is False


class TestTheDarkFlagStillGovernsIt:
    def test_with_wall_delivery_OFF_a_wall_campaign_behaves_as_ordinary(self):
        # `wall_delivery_enabled` is the one switch that returns the node to
        # its pre-feature behaviour, and it has to govern this repair too —
        # otherwise "turn the wall delivery off" would stop meaning what it says.
        assert _applies(_WALL, seen_before=True, disable_returning=True,
                        delivery=False) is False


class TestOneDefinitionTwoCallers:
    """The structural pin. Its subject is the SOURCE, deliberately.

    A behavioural test cannot see a second copy that currently agrees with the
    first — it only sees them after they have drifted, which is the day the
    defect ships. The two call sites are named here so that adding a third
    inline copy is a visible edit rather than a silent one.
    """

    @staticmethod
    def _src() -> str:
        from pathlib import Path
        return Path(router.__file__).read_text(encoding="utf-8")

    def test_both_consumers_call_the_shared_predicate(self):
        src = self._src()
        assert "returning_visitor = _draining_class_applies(" in src, (
            "`_allowed_availability` no longer uses the shared predicate"
        )
        assert "returning_visitor=_draining_class_applies(" in src, (
            "the cascade's availability argument no longer uses the shared "
            "predicate — this is the copy whose omission made an all-draining "
            "wall serve nobody while a one-draining wall served fine"
        )

    def test_the_old_inline_expression_survives_in_NEITHER_place(self):
        src = self._src()
        assert "returning_visitor=seen_before if audience_routing else False" not in src, (
            "an inline copy of the old rule is back. Note the cascade's OWN "
            "`seen_before=` argument keeps its original gate ON PURPOSE — the "
            "partition is not part of this repair — so check WHICH argument "
            "before relaxing this assertion"
        )

    def test_the_partition_argument_is_still_gated_the_OLD_way(self):
        # The other half of the same claim: this repair must NOT have widened
        # into the audience partition. If this fails, returning FLOWS have been
        # switched back on under a flag that says they are off.
        src = self._src()
        assert "seen_before=seen_before if audience_routing else False" in src or \
               "seen_before=_seen_before_for_partition" in src, (
            "the cascade's partition argument changed. The decision limits this "
            "repair to the availability class: «Звичайні кампанії не чіпаємо», "
            "and the partition is not the class"
        )


class TestEndToEndTheHeadlineCase:
    """The exact input the adjudication quotes, through the real frame."""

    def test_a_repeat_visitor_clicking_a_draining_tile_reaches_THAT_offer(self):
        from tests.unit.test_wall_tile_membership import _sign_wall

        result = _run(disable_returning=True, avails={TILE_1: "draining"},
                      code=_sign_wall(target_id=TILE_1))
        assert result is not None
        assert _served(result) == str(TILE_1)

    def test_a_FRESH_visitor_on_the_identical_fixture_is_walked_past_it(self):
        from tests.unit.test_wall_tile_membership import _sign_wall

        result = _run(disable_returning=True, avails={TILE_1: "draining"},
                      seen_before=False, code=_sign_wall(target_id=TILE_1))
        assert result is not None
        assert _served(result) == str(TILE_2)

    def test_an_ALL_draining_wall_serves_the_repeat_visitor_too(self):
        # 🔴 THE CASE THAT CAUGHT THE HALF-REPAIR. With only
        # `_allowed_availability` fixed, this returned nothing at all: the
        # cascade's own copy of the rule floored the whole wall before the
        # delivery path was ever consulted. One visitor, one flag, two answers.
        result = _run(disable_returning=True,
                      avails={TILE_1: "draining", TILE_2: "draining"})
        assert result is not None, (
            "the wall served nobody — the cascade pre-floor is using a "
            "different availability rule from the delivery path again"
        )
        assert _served(result) == str(TILE_1)
