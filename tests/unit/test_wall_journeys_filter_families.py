"""J4 — the wall's filter families, and the guard that existed only as a comment.

🔴 WHAT WAS ACTUALLY MISSING, measured before a line was written (2026-09-12).
Replacing the wall endpoint's `build_click_attrs(...)` call with a PARTIAL dict —
`{"geo": click_req.country.upper()}` — left the **entire click-processor unit
suite at 2289 passed, 0 failed**. Risk **A33** is written out in full in
`build_click_attrs`'s own docstring and in a comment directly above the wall's
call site, and **nothing enforced it**.

WHY THAT IS THE DANGEROUS DIRECTION, in the code's own words: the matcher reads
a missing dim as `""`, and `cascade._OPS_SATISFIED_BY_MISSING_VALUE == {"empty"}`
— so a criterion `<dim> empty` **MATCHES when the dim is absent** and does not
match when it carries a value. *"A partial dict does not merely hide candidates,
it ADMITS ones the full dict rejects."* A wall shown to a visitor who was
filtered away from it is cross-audience exposure, not a cosmetic miss.

WHAT THE BOX ASKED FOR, AND WHAT THE CODE SAYS — the sixth criterion in this
plan to overstate the gap, and the only one where the wording inverts a
SECURITY property:

  * *"Real wall **GET** and POST shapes"* — **there is no GET wall shape, by
    design.** The Worker's dispatcher is
    `if (request.method === 'POST') return wantsWall ? 'wall' : 'preview'; return 'none';`
    and its comment says why: *"a landing page may leave the parameter in an href
    it hands a REAL visitor. That visitor must get their destination, not JSON."*
    A GET carrying the wall parameter is an **ordinary click**. Building a "GET
    wall shape" would build the defect. Already pinned, correctly, by
    `services/worker/test/edge-wall-mode.test.js`
    (*"GET with the parameter is an ordinary click, never JSON"*), 18 cases
    covering the dispatcher, flag independence and ambiguity.
  * *"an explicit rejection/no-side-effect contract for unsupported shapes"* —
    already built: the dark gate's 404-equals-a-missing-route oracle, the key
    refusal, and A34's calibrated zero-write property, all in
    `test_wall_endpoint.py`.
  * *"each supported filter family"* — **this is the part that was missing**, and
    it is what this file builds.

THE FOUR FAMILIES, read from `cascade.KNOWN_EVALUATED_DIMS` rather than assumed:
base dims · returning-history dims · structural dims (buyer/team/department/
custom_group) · `param:<slot>` identifier dims. They are evaluated by ONE shared
function, which the wall reuses — so the risk was never "does family X work", it
was **"does the wall hand that function the whole dict"**. `TestTheWallUsesThe
SharedAttributeBuilder` asks exactly that, once, for every family at once.
"""

from __future__ import annotations

import asyncio
import json

import pytest
from unittest.mock import patch

from app import main
from app.cascade import _EVALUATED_BASE_DIMS

from tests.unit.test_wall_endpoint import (  # noqa: F401  (`armed` is a fixture)
    WALL_A,
    _fake,
    _post,
    _seed,
    armed,
)

#: The click every test below sends: US, an iPhone UA, one identifier slot.
#: Chosen so that several FAMILIES carry a real value at once — a criterion
#: asserting `empty` on any of them must therefore fail.
_CLICK = {
    "country": "US",
    "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2)",
    "query_params": {"sub1": "cohort-a"},
}


def _seeded(criteria):
    """The shared wall fixture, with THIS wall's criteria replaced.

    Reuses `test_wall_endpoint._seed` verbatim rather than growing a second
    wall fixture: a near-copy is how two files start describing two systems.
    Only the one field under test is overwritten.
    """
    store = _fake()

    async def _go():
        r = store.client()
        await _seed(r)
        await r.hset("flow:%s" % WALL_A,
                     mapping={"criteria": json.dumps(criteria)})

    asyncio.run(_go())
    return store


def _matched(criteria, body=None) -> bool:
    payload = dict(_CLICK)
    payload.update(body or {})
    r = _post(_seeded(criteria), body=payload)
    assert r.status_code == 200, r.text
    return bool(r.json().get("matched"))


@pytest.mark.usefixtures("armed")
class TestTheFixtureCanSayBothThings:
    """Calibration first. Without it every refusal below is equally explained
    by a fixture whose wall is never eligible for anything."""

    def test_a_wall_with_NO_criteria_is_matched(self):
        assert _matched([]) is True

    def test_a_wall_whose_criterion_MATCHES_this_click_is_matched(self):
        assert _matched([{"type": "geo", "op": "in",
                          "values": ["US"]}]) is True

    def test_a_wall_whose_criterion_does_NOT_match_is_refused(self):
        assert _matched([{"type": "geo", "op": "in",
                          "values": ["DE"]}]) is False


@pytest.mark.usefixtures("armed")
class TestTheBaseFamilyDiscriminatesOnTheWallPath:
    """One representative dim per shape: a geo string, a parsed-UA value, and a
    dim the click genuinely cannot carry."""

    def test_device_type_is_read_from_the_user_agent(self):
        assert _matched([{"type": "device_type", "op": "in",
                          "values": ["mobile"]}]) is True

    def test_and_the_WRONG_device_type_refuses(self):
        assert _matched([{"type": "device_type", "op": "in",
                          "values": ["desktop"]}]) is False

    def test_os_too_so_it_is_not_one_dim_carrying_the_whole_family(self):
        assert _matched([{"type": "os", "op": "in",
                          "values": ["ios"]}]) is True

    def test_not_in_fails_CLOSED_on_a_dim_the_click_cannot_carry(self):
        # `city` has no source in this request. Every operator except `empty`
        # must then fail closed — including `not_in`, which reads as "should
        # match anything" to a human and does not.
        assert _matched([{"type": "city", "op": "not_in",
                          "values": ["Paris"]}]) is False


@pytest.mark.usefixtures("armed")
class TestTheIdentifierFamilyReachesTheWall:
    """`param:<slot>` — a whole family that lives behind a prefix, and so is the
    easiest one for a narrowed dict to drop without anybody noticing."""

    def test_a_param_slot_the_click_carries_matches(self):
        assert _matched([{"type": "param:sub1", "op": "in",
                          "values": ["cohort-a"]}]) is True

    def test_the_WRONG_value_on_the_same_slot_refuses(self):
        assert _matched([{"type": "param:sub1", "op": "in",
                          "values": ["cohort-b"]}]) is False

    def test_a_slot_the_click_does_NOT_carry_fails_closed_on_in(self):
        assert _matched([{"type": "param:sub2", "op": "in",
                          "values": ["anything"]}]) is False


@pytest.mark.usefixtures("armed")
class TestTheEmptyOperatorIsTheAsymmetricOne:
    """🔴 A33's failure direction, made behavioural.

    `empty` is the ONE operator a MISSING dim satisfies. So a narrowed
    attribute dict does not merely hide walls — it ADMITS walls the full dict
    rejects. These two rows are the pair that inverts under a partial dict, and
    they are the reason this file exists.
    """

    def test_EMPTY_on_a_dim_the_click_DOES_carry_is_refused(self):
        # The iPhone UA resolves `device_type` to a real value, so "this
        # dimension has no value" is false and the wall must not be shown.
        # Under a partial dict `device_type` is absent, `empty` is satisfied,
        # and this visitor sees a wall they were filtered away from.
        assert _matched([{"type": "device_type",
                          "op": "empty", "values": []}]) is False

    def test_CONTROL_empty_on_a_dim_the_click_genuinely_LACKS_is_matched(self):
        # Without this, the refusal above is equally explained by an `empty`
        # operator that never matches anything.
        assert _matched([{"type": "city",
                          "op": "empty", "values": []}]) is True

    def test_the_same_asymmetry_on_the_IDENTIFIER_family(self):
        carried = _matched([{"type": "param:sub1",
                             "op": "empty", "values": []}])
        absent = _matched([{"type": "param:sub2",
                            "op": "empty", "values": []}])
        assert carried is False, (
            "`param:sub1 empty` matched a click that carries sub1 — the "
            "identifier family is missing from the wall's attribute dict (A33)"
        )
        assert absent is True


@pytest.mark.usefixtures("armed")
class TestTheWallUsesTheSharedAttributeBuilder:
    """ONE test for every family at once — including the ones above do not name.

    The behavioural rows can only cover dims a unit request can realistically
    carry. This one asks the structural question directly: whatever the wall
    hands the shared evaluator, does it contain the whole evaluated base set?
    A narrowing of ANY family — including the returning-history and structural
    dims, which a bare POST cannot populate — goes red here.
    """

    @staticmethod
    def _captured_attrs() -> dict:
        seen: dict = {}
        real = main.load_wall_candidates

        async def _spy(*a, **kw):
            seen["click_attrs"] = kw.get("click_attrs")
            return await real(*a, **kw)

        with patch.object(main, "load_wall_candidates", _spy):
            r = _post(_seeded([]), body=_CLICK)
            assert r.status_code == 200, r.text
        assert "click_attrs" in seen, "the loader was never reached"
        return seen["click_attrs"]

    def test_every_evaluated_BASE_dim_is_present(self):
        attrs = self._captured_attrs()
        missing = sorted(_EVALUATED_BASE_DIMS - set(attrs))
        assert not missing, (
            "the wall filters on a NARROWED attribute dict — missing %s. "
            "A33: a missing dim reads as '' and the `empty` operator is "
            "SATISFIED by that, so this ADMITS walls the full dict rejects."
            % missing
        )

    def test_the_identifier_family_is_present_as_a_prefixed_block(self):
        attrs = self._captured_attrs()
        assert any(k.startswith("param:") for k in attrs), (
            "no `param:` dims reached the wall — the identifier filter family "
            "is silently unevaluable, and every `param:<slot> empty` criterion "
            "now matches every visitor"
        )

    def test_the_set_is_WIDER_than_the_base_dims_alone(self):
        # The anti-degenerate guard: a dict containing exactly the base dims
        # and nothing else would satisfy the first row while having dropped
        # two whole families. Stated separately so that cannot pass quietly.
        attrs = self._captured_attrs()
        assert set(attrs) - _EVALUATED_BASE_DIMS, (
            "the wall's attribute dict is EXACTLY the base dims — the "
            "identifier, structural and returning families are all absent"
        )
