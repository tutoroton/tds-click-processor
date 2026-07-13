"""Execution-time availability floor for UN-PINNED offer actions.

Closes a gap flagged explicitly in `FINDINGS-G1-MATRIX.md` §2 (landing
table, last row) / §6.4: a flow whose action is `offer` WITHOUT a pinned
`target_id` dodges the cascade's PRE-selection availability floor entirely
(`cascade._referenced_target_ids` returns `[]` for it — see
`cascade.py:460-484`'s own docstring). The ONLY gate for such a flow is the
EXECUTION-time floor inside `action_executor._offer_default_template`
(`_avail_ok(...)` + the `avail_blocked` short-circuit that suppresses the
bare-url fallback, `action_executor.py:596-607`).

`test_action_executor.py::TestOfferDefaultTargetGap` covers the NO-default /
NO-bare-url shapes but never a CLOSED/draining `is_default` target
specifically — the exact scenario this file closes.

Kept in its own file rather than appended to `test_action_executor.py`
(already 885 lines, over the 600-line test-file cap per
`code-organization.md`) so this campaign doesn't grow an oversized file
further — flagged in FINDINGS-G2-RESULTS.md.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.action_executor import UNAVAILABLE_RESULT, execute_action
from app.models import ClickRequest


def _click() -> ClickRequest:
    return ClickRequest(
        click_id="abc-123", country="US",
        user_agent="Mozilla/5.0 (iPhone)", query_params={},
    )


def _flow(action_type: str, action_config: dict) -> dict:
    return {"_id": "1", "action_type": action_type, "action_config": json.dumps(action_config)}


def _stub_build_url():
    def fn(template, req, campaign_id, offer_id, *,
           source_mappings, campaign_mappings, target_id=None, flow_id=None):
        return f"FINAL[{template}]"
    return fn


def _redis_with_hashes(hashes: dict, sets: dict | None = None) -> MagicMock:
    sets = sets or {}

    class FakePipeline:
        def __init__(self):
            self._ops: list[str] = []

        def hgetall(self, key):
            self._ops.append(key)

        async def execute(self):
            return [dict(hashes.get(k, {})) for k in self._ops]

    redis = MagicMock()
    redis.pipeline = lambda: FakePipeline()
    redis.hgetall = AsyncMock(side_effect=lambda key: dict(hashes.get(key, {})))
    redis.smembers = AsyncMock(side_effect=lambda key: set(sets.get(key, set())))
    return redis


class TestUnpinnedOfferExecutionTimeFloor:
    """No pinned `target_id` in the flow's `action_config` → the cascade
    pre-selection floor is a documented no-op for this flow. The ONLY gate
    is here, at execution time."""

    @pytest.mark.asyncio
    async def test_closed_default_target_does_not_leak_new_visitor(self):
        flow = _flow("offer", {"offer_id": 5})  # NO target_id — un-pinned
        r = _redis_with_hashes(
            {
                "offer:5": {"has_targets": "1"},  # no bare url either
                "offer_target:7": {
                    "url": "https://closed-default", "is_default": "1",
                    "availability": "closed",
                },
            },
            sets={"offer:5:targets": {"7"}},
        )
        result = await execute_action(
            r, flow, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=_stub_build_url(),
        )
        # MUST be the terminal-fallback sentinel — NOT the closed URL, and
        # NOT a bare None (which would legacy-reserve past the drain/close).
        assert result is UNAVAILABLE_RESULT

    @pytest.mark.asyncio
    async def test_draining_default_target_excluded_for_new_visitor(self):
        flow = _flow("offer", {"offer_id": 5})
        r = _redis_with_hashes(
            {
                "offer:5": {"has_targets": "1"},
                "offer_target:7": {
                    "url": "https://draining-default", "is_default": "1",
                    "availability": "draining",
                },
            },
            sets={"offer:5:targets": {"7"}},
        )
        result = await execute_action(
            r, flow, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=_stub_build_url(),
            allowed_avail=frozenset({"active"}),  # NEW visitor
        )
        assert result is UNAVAILABLE_RESULT

    @pytest.mark.asyncio
    async def test_draining_default_target_served_for_returning_visitor(self):
        """Same config, RETURNING visitor's allowed set includes 'draining'
        → the default target IS served (draining keeps returning traffic —
        FINDINGS-G1 §2 landing table)."""
        flow = _flow("offer", {"offer_id": 5})
        r = _redis_with_hashes(
            {
                "offer:5": {"has_targets": "1"},
                "offer_target:7": {
                    "url": "https://draining-default", "is_default": "1",
                    "availability": "draining",
                },
            },
            sets={"offer:5:targets": {"7"}},
        )
        result = await execute_action(
            r, flow, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=_stub_build_url(),
            allowed_avail=frozenset({"active", "draining"}),  # RETURNING visitor
        )
        assert result is not None
        assert result["target_id"] == "7"

    @pytest.mark.asyncio
    async def test_closed_default_does_not_fall_back_to_bare_url(self):
        """The critical leak-prevention assertion: even when the offer ALSO
        carries a bare `url` (legacy fallback field), a CLOSED `is_default`
        target must NOT silently degrade to serving the bare url — that
        would defeat the operator's drain/close intent
        (`action_executor.py` `_offer_default_template` comment,
        lines ~603-607: "do NOT silently fall to the bare offer.url")."""
        flow = _flow("offer", {"offer_id": 5})
        r = _redis_with_hashes(
            {
                "offer:5": {"has_targets": "1", "url": "https://bare-leak/x"},
                "offer_target:7": {
                    "url": "https://closed-default", "is_default": "1",
                    "availability": "closed",
                },
            },
            sets={"offer:5:targets": {"7"}},
        )
        result = await execute_action(
            r, flow, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=_stub_build_url(),
        )
        assert result is UNAVAILABLE_RESULT
