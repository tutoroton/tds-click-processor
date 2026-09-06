"""The PRE-REFACTOR ORACLE for routing's cascade boundary.

WHAT THIS IS FOR. `router._try_flow_cascade` builds a ~40-dimension `click_attrs`
dict inline and hands it to `cascade.resolve_flow`. The offerwall read endpoint
(G5) must evaluate wall criteria against the SAME dict — a narrower one does not
merely hide candidates, it ADMITS the ones whose criterion is `<dim> empty`
(risk A33; `cascade._OPS_SATISFIED_BY_MISSING_VALUE`). So that construction has to
become a shared function, and the extraction touches routing's hot path.

🔴 THIS FILE EXISTS TO BE CAPTURED BEFORE THAT EXTRACTION, AND ONLY EVER COMPARED
AFTER IT. The fixture `tests/fixtures/cascade_boundary_oracle.json` was generated
by running these exact scenarios against the UNEXTRACTED code and committed in its
own commit, ahead of the refactor — so the expectation cannot have been produced
by the thing it is supposed to check. A golden regenerated from the new
implementation would agree with it by construction and prove nothing.

WHY IT ASSERTS TYPES AND NOT JUST VALUES. Three dims are set-valued — `prev_offer`,
`prev_offer_target`, `prev_sub` are `frozenset`s matched by an intersection branch
(`cascade.py`), while every other dim is a `str` compared by equality. A cleanup
that "normalises everything to a string" would keep every VALUE looking right and
silently break those three. `str(None)` is `"None"`, and `None in frozenset()` is
False exactly like `"" in frozenset()` — so a route-level assertion cannot see the
difference. The snapshot therefore records `[type_name, value]` for every entry.
This is the same reasoning as `test_none_slot_cast_produces_string_not_none_in_click_attrs`,
applied to the whole dict instead of one key.

WHAT IT DOES NOT PROVE. It pins the cascade boundary's INPUTS. It does not prove the
surrounding routing function still computes `returning_live` / `campaign_mode` /
`effective_mode` for its own later use — those are consumed AFTER the cascade call
(`router.py`, sticky-pin block) and a boundary snapshot cannot see them. The
extraction must leave them in the caller, and `test_router_cascade.py`'s sticky and
returning-mode tests are what cover that half.

DETERMINISM. `_extra_click_dims` derives `time_of_day` / `day_of_week` from
`req.arrival_ts` and nothing else — no clock is read — so a scenario that pins an
explicit `arrival_ts` is stable, and one that omits it is stably empty. There is no
randomness in any scenario: none of them reaches a rotating split.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import patch

from app import cascade as cascade_mod
from app import identity as identity_mod
from app.config import settings
from app.identity import IdentityResult
from app.models import ClickRequest

from .test_router_cascade import FakeRedis, _route_with

_FIXTURE = (
    Path(__file__).resolve().parent.parent
    / "fixtures" / "cascade_boundary_oracle.json"
)

#: The cascade kwargs the snapshot records besides `click_attrs`. These are the
#: other outputs of the same inline block, and the extraction must not disturb
#: them either — a builder that quietly recomputed the returning gate would keep
#: `click_attrs` identical and change which POOL the cascade searches.
_ALSO_CAPTURED = ("audience_routing", "seen_before", "returning_visitor")

_CAMPAIGN = "10"


def _typed(value: Any) -> list[Any]:
    """`[type_name, canonical_value]` — types are half of the contract.

    Sets are sorted so the snapshot is stable; the TYPE NAME is what records that
    they were sets in the first place, which sorting would otherwise erase.
    """
    name = type(value).__name__
    if isinstance(value, (frozenset, set)):
        return [name, sorted(str(v) for v in value)]
    return [name, value]


def _snapshot(captured: dict[str, Any]) -> dict[str, Any]:
    attrs = captured.get("click_attrs")
    if attrs is None:
        return {"click_attrs": None, "other": {}}
    return {
        "click_attrs": {k: _typed(v) for k, v in sorted(attrs.items())},
        "other": {k: _typed(captured[k]) for k in _ALSO_CAPTURED if k in captured},
    }


def _base_redis(campaign_extra: dict[str, str] | None = None,
                flows: list[str] | None = None,
                hashes_extra: dict[str, dict[str, str]] | None = None,
                sources: set[str] | None = None) -> FakeRedis:
    camp = {"company_id": "1", "priority": "0"}
    camp.update(campaign_extra or {})
    hashes: dict[str, dict[str, str]] = {f"campaign:{_CAMPAIGN}": camp}
    hashes.update(hashes_extra or {})
    redis = FakeRedis(
        sets={
            "geo:US": {_CAMPAIGN},
            "device:mobile": {_CAMPAIGN},
            "os:ios": {_CAMPAIGN},
            "campaigns:active": {_CAMPAIGN},
        },
        hashes=hashes,
        lists={f"campaign:{_CAMPAIGN}:flows": flows or []},
    )
    if sources:
        redis.sets[f"campaign:{_CAMPAIGN}:sources"] = sources
    return redis


def _capture(redis: FakeRedis, req: ClickRequest, *,
             returning: bool = False) -> dict[str, Any]:
    """Run one scenario through `route()` and record the cascade's arguments.

    `resolve_flow` is replaced by a recorder that returns None (forcing a
    non-route). We are pinning what routing HANDS the cascade, not what the
    cascade decides — a scenario that reached a winner would add the picker's
    behaviour to a snapshot that is meant to isolate the boundary.
    """
    captured: dict[str, Any] = {}

    async def _recorder(r, **kw):  # noqa: ANN001
        captured["click_attrs"] = kw.get("click_attrs")
        for name in _ALSO_CAPTURED:
            if name in kw:
                captured[name] = kw[name]
        return None

    async def _seen_before_visitor(**kw):
        return IdentityResult(uid="U", seen_before=True, is_returning=True)

    stack = [patch.object(cascade_mod, "resolve_flow", _recorder)]
    if returning:
        stack += [
            patch.object(settings, "returning_resolver_enabled", True),
            patch.object(settings, "returning_routing_enabled", True),
            patch.object(identity_mod, "resolve_and_stamp", _seen_before_visitor),
        ]
    for p in stack:
        p.start()
    try:
        _route_with(redis, req)
    finally:
        for p in reversed(stack):
            p.stop()
    return captured


# --------------------------------------------------------------------------
# The scenarios. Each one exists to move a DIFFERENT part of the dict; a set
# that varied only the geo would pin one branch seven times.
# --------------------------------------------------------------------------

def _s_minimal() -> dict[str, Any]:
    """Baseline: no source, no params, no arrival_ts, asn 0."""
    return _capture(_base_redis(), ClickRequest(
        click_id="oracle-1", country="US",
        user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_2)",
    ))


def _s_casing_and_time() -> dict[str, Any]:
    """The casing contract plus the two time dims, pinned at a fixed instant.

    `geo` upper, `city` lower, `region` and `browser` verbatim — four different
    rules in one dict, which is exactly where a "tidy up the casing" edit lands.
    """
    return _capture(_base_redis(), ClickRequest(
        click_id="oracle-2", country="us", city="New York", region="New York",
        user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"),
        accept_language="en-GB,en;q=0.9",
        asn=13335,
        arrival_ts="2026-09-06T14:37:00Z",
    ))


def _s_identifier_resolved() -> dict[str, Any]:
    """A mapped identifier slot that RESOLVES — `param:creative_id` non-empty."""
    return _capture(
        _base_redis(
            hashes_extra={"source:99": {
                "slug": "fb",
                "param_mappings": json.dumps([{"slot": "creative_id",
                                               "alias": "cr"}]),
            }},
            sources={"99"},
        ),
        ClickRequest(
            click_id="oracle-3", country="US",
            user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_2)",
            query_params={"source": "fb", "cr": "AdVariant_A"},
        ),
    )


def _s_identifier_mapped_but_unresolved() -> dict[str, Any]:
    """Mapped and NOT supplied — the `.get(slot) or ""` case.

    `resolve_slots` returns `None` here, not an absent key, so a naive
    `.get(slot, "")` would leave a `None` in the dict. That is the exact bug
    Unknown 6 fixed, and this scenario is what keeps the snapshot able to see it.
    """
    return _capture(
        _base_redis(
            hashes_extra={"source:99": {
                "slug": "fb",
                "param_mappings": json.dumps([{"slot": "creative_id",
                                               "alias": "cr"}]),
            }},
            sources={"99"},
        ),
        ClickRequest(
            click_id="oracle-4", country="US",
            user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_2)",
            query_params={"source": "fb"},
        ),
    )


def _s_structural_buyer() -> dict[str, Any]:
    """A resolved buyer — the four structural dims carry STRINGS, never ints.

    `str(_v) if _v is not None else ""` is load-bearing: a Python int is never
    `==` a str of the same digits, so dropping the cast reproduces the CF-3
    fail-open class inside the fix meant to prevent it.
    """
    return _capture(
        _base_redis(
            hashes_extra={
                "source:99": {
                    "slug": "fb",
                    "param_mappings": json.dumps([{"slot": "buyer_id",
                                                   "alias": "b"}]),
                },
                "buyer:7": {"company_id": "1", "team_id": "3",
                            "department_id": "4", "custom_group_id": "5"},
            },
            sources={"99"},
        ),
        ClickRequest(
            click_id="oracle-5", country="US",
            user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_2)",
            query_params={"source": "fb", "b": "7"},
        ),
    )


def _s_returning_off() -> dict[str, Any]:
    """Partition OFF ⇒ the five returning dims are ABSENT, not present-and-false.

    Their absence is the contract: a first-pool flow never carries these dims, so
    injecting them unconditionally would change which criteria can be authored
    against, not merely what they evaluate to.
    """
    return _capture(_base_redis(), ClickRequest(
        click_id="oracle-6", country="US",
        user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_2)",
    ))


def _s_returning_on_seen_before() -> dict[str, Any]:
    """Partition ON + a seen-before visitor ⇒ the returning palette is present.

    This is the one scenario that puts `frozenset`s in the dict, so it is the one
    that makes the type half of the snapshot bite.
    """
    return _capture(
        _base_redis(campaign_extra={"returning_resolver": "1",
                                    "returning_routing": "1"}),
        ClickRequest(
            click_id="oracle-7", country="US",
            user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_2)",
        ),
        returning=True,
    )


SCENARIOS = {
    "minimal": _s_minimal,
    "casing_and_time": _s_casing_and_time,
    "identifier_resolved": _s_identifier_resolved,
    "identifier_mapped_but_unresolved": _s_identifier_mapped_but_unresolved,
    "structural_buyer": _s_structural_buyer,
    "returning_off": _s_returning_off,
    "returning_on_seen_before": _s_returning_on_seen_before,
}


def capture_all() -> dict[str, Any]:
    """Every scenario's snapshot. Shared with the generator so the two cannot
    drift — the generator supplies the REVISION, never the scenarios."""
    return {name: _snapshot(fn()) for name, fn in SCENARIOS.items()}


class TestTheCascadeBoundaryIsUnchanged:
    def test_fixture_exists_and_covers_every_scenario(self):
        """A missing or partial fixture must FAIL, never regenerate itself.

        A test that writes its own expectation when it cannot find one is green
        on its first run against any implementation whatsoever.
        """
        assert _FIXTURE.exists(), (
            f"{_FIXTURE} is missing. It is captured ONCE, against the "
            "pre-extraction code, by scripts/dev/capture_cascade_oracle.py — "
            "never regenerated from the current implementation."
        )
        golden = json.loads(_FIXTURE.read_text())
        assert set(golden) == set(SCENARIOS), (
            "the fixture and the scenario list disagree; a scenario added "
            "without re-capturing on the ORIGINAL revision proves nothing"
        )

    def test_every_scenario_matches_the_pre_refactor_capture(self):
        golden = json.loads(_FIXTURE.read_text())
        actual = capture_all()
        for name in sorted(SCENARIOS):
            assert actual[name] == golden[name], (
                f"scenario {name!r}: what routing hands cascade.resolve_flow "
                "changed. Compare key set, VALUES and TYPES — a str where a "
                "frozenset belongs reads as an equal value and is not one."
            )

    def test_the_snapshot_is_not_vacuous(self):
        """Guard against the whole oracle passing because it captured nothing.

        Every assertion above compares two dicts; two empty dicts are equal. If
        `resolve_flow` stopped being reached — a fixture drifting until routing
        bails before the cascade — the comparison would stay green while
        measuring nothing at all.
        """
        actual = capture_all()
        for name, snap in actual.items():
            assert snap["click_attrs"], (
                f"scenario {name!r} captured NO click_attrs — routing never "
                "reached the cascade, so this scenario proves nothing"
            )
        assert len(actual["minimal"]["click_attrs"]) >= 20, (
            "the base dict should carry the 10 base dims + 4 structural + the "
            "identifier palette; a much smaller one means the capture is "
            "happening somewhere other than the real boundary"
        )

    def test_the_returning_palette_appears_only_under_the_partition(self):
        """The two returning scenarios must DIFFER, or neither proves anything.

        This is the calibration for the pair: if the gate stopped working, both
        would carry the palette (or neither would) and both would still match
        their own golden entries — because the golden would have been captured
        from the same broken code. Comparing them to EACH OTHER is what catches
        a gate that has silently become unconditional.
        """
        actual = capture_all()
        off = set(actual["returning_off"]["click_attrs"])
        on = set(actual["returning_on_seen_before"]["click_attrs"])
        palette = {"is_returning", "is_roaming", "prev_offer",
                   "prev_offer_target", "prev_sub"}
        assert not (palette & off), (
            f"partition OFF carried returning dims: {sorted(palette & off)}"
        )
        assert palette <= on, (
            "partition ON did not carry the full returning palette: missing "
            f"{sorted(palette - on)}"
        )

    def test_set_valued_dims_are_sets_and_scalar_dims_are_strings(self):
        """The type contract, asserted where it can actually be seen.

        `prev_*` are frozensets matched by intersection; everything else is a
        string matched by equality. A normalisation pass that flattened the
        first group would leave every VALUE plausible.
        """
        snap = capture_all()["returning_on_seen_before"]["click_attrs"]
        for dim in ("prev_offer", "prev_offer_target", "prev_sub"):
            assert snap[dim][0] == "frozenset", (
                f"{dim} is {snap[dim][0]}, not a frozenset — the intersection "
                "branch in the matcher expects a set"
            )
        for dim, (type_name, _value) in snap.items():
            if dim.startswith("prev_"):
                continue
            assert type_name == "str", (
                f"{dim} is {type_name}; every non-history dim is compared as a "
                "string, and a non-str silently never matches"
            )
