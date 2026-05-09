"""Unit tests for `app/action_executor.py` — Stage 2 / Vectors 2.4 + 2.5.

Mock Redis via `AsyncMock`, inject `build_url_fn` to avoid coupling
to router internals. Each branch of `execute_action` (redirect / offer
/ split / block) gets happy-path + edge-case coverage.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.action_executor import (
    BLOCK_RESULT,
    _is_positive_int,
    _parse_action_config,
    execute_action,
)
from app.models import ClickRequest


def _click() -> ClickRequest:
    return ClickRequest(
        click_id="abc-123",
        country="US",
        user_agent="Mozilla/5.0 (iPhone)",
        query_params={},
    )


def _flow(action_type: str, action_config: dict) -> dict:
    return {
        "_id": "1",
        "action_type": action_type,
        "action_config": json.dumps(action_config),
    }


def _stub_build_url():
    """Return a stub `build_url_fn` that captures inputs + emits a marker URL.

    Signature MUST mirror `router.build_url` — including the T2.5
    additions `target_id` + `flow_id` (kwargs). When the production
    signature gains a new param, this stub must be updated too;
    otherwise action_executor's call shape breaks here loudly.
    """
    calls: list[tuple] = []

    def fn(template, req, campaign_id, offer_id, *,
           source_mappings, campaign_mappings,
           target_id=None, flow_id=None):
        calls.append((template, campaign_id, offer_id, target_id, flow_id))
        return f"FINAL[{template}|cid={campaign_id}|oid={offer_id}]"

    fn.calls = calls  # type: ignore[attr-defined]
    return fn


def _redis_with_hashes(hashes: dict[str, dict], sets: dict[str, set] | None = None) -> MagicMock:
    sets = sets or {}

    class FakePipeline:
        def __init__(self):
            self._ops: list[tuple[str, str]] = []

        def hgetall(self, key):
            self._ops.append(("hgetall", key))

        async def execute(self):
            return [dict(hashes.get(key, {})) for _, key in self._ops]

    redis = MagicMock()
    redis.pipeline = lambda: FakePipeline()
    redis.hgetall = AsyncMock(side_effect=lambda key: dict(hashes.get(key, {})))
    redis.smembers = AsyncMock(side_effect=lambda key: set(sets.get(key, set())))
    return redis


# ============================================================
# Redirect
# ============================================================


class TestRedirect:
    @pytest.mark.asyncio
    async def test_redirect_substitutes_url(self):
        flow = _flow("redirect", {"url": "https://lp.example.com/{click_id}"})
        build_url = _stub_build_url()
        r = _redis_with_hashes({})

        result = await execute_action(
            r, flow, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=build_url,
        )
        assert result is not None
        assert result["url"].startswith("FINAL[https://lp.example.com")
        assert result["offer_id"] is None
        assert result["target_id"] is None
        assert build_url.calls[0][0] == "https://lp.example.com/{click_id}"

    @pytest.mark.asyncio
    async def test_redirect_missing_url_returns_none(self):
        flow = _flow("redirect", {})
        result = await execute_action(
            _redis_with_hashes({}), flow, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=_stub_build_url(),
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_redirect_with_non_string_url_returns_none(self):
        flow = _flow("redirect", {"url": 12345})
        result = await execute_action(
            _redis_with_hashes({}), flow, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=_stub_build_url(),
        )
        assert result is None


# ============================================================
# Offer
# ============================================================


class TestOffer:
    @pytest.mark.asyncio
    async def test_offer_pinned_target(self):
        flow = _flow("offer", {"offer_id": 5, "target_id": 7})
        r = _redis_with_hashes({
            "offer_target:7": {"url": "https://t.example/path", "is_default": "0"},
        })
        build_url = _stub_build_url()
        result = await execute_action(
            r, flow, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=build_url,
        )
        assert result is not None
        assert result["offer_id"] == "5"
        assert result["target_id"] == "7"
        assert build_url.calls[0][0] == "https://t.example/path"

    @pytest.mark.asyncio
    async def test_offer_target_missing_falls_back_to_default(self):
        flow = _flow("offer", {"offer_id": 5, "target_id": 999})  # 999 absent
        r = _redis_with_hashes(
            {
                "offer:5": {"has_targets": "1", "url": "https://offer.bare/x"},
                "offer_target:7": {"url": "https://t-default", "is_default": "1"},
            },
            sets={"offer:5:targets": {"7"}},
        )
        build_url = _stub_build_url()
        result = await execute_action(
            r, flow, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=build_url,
        )
        assert result is not None
        assert result["offer_id"] == "5"
        assert result["target_id"] == "7"  # default target picked
        assert build_url.calls[0][0] == "https://t-default"

    @pytest.mark.asyncio
    async def test_offer_no_target_no_default_uses_offer_url(self):
        flow = _flow("offer", {"offer_id": 5})  # No target_id at all
        r = _redis_with_hashes({
            "offer:5": {"has_targets": "0", "url": "https://offer.fallback/p"},
        })
        build_url = _stub_build_url()
        result = await execute_action(
            r, flow, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=build_url,
        )
        assert result is not None
        assert result["offer_id"] == "5"
        assert result["target_id"] is None
        assert build_url.calls[0][0] == "https://offer.fallback/p"

    @pytest.mark.asyncio
    async def test_offer_missing_offer_id_returns_none(self):
        flow = _flow("offer", {})
        result = await execute_action(
            _redis_with_hashes({}), flow, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=_stub_build_url(),
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_offer_string_offer_id_rejected(self):
        """Numeric strings drifting through sync — refuse defensively."""
        flow = _flow("offer", {"offer_id": "5"})
        result = await execute_action(
            _redis_with_hashes({}), flow, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=_stub_build_url(),
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_offer_completely_missing_in_redis(self):
        flow = _flow("offer", {"offer_id": 999})
        r = _redis_with_hashes({})
        result = await execute_action(
            r, flow, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=_stub_build_url(),
        )
        assert result is None


# ============================================================
# Split
# ============================================================


class TestSplit:
    @pytest.mark.asyncio
    async def test_split_picks_one_offer_by_weight(self):
        flow = _flow("split", {
            "offers": [
                {"offer_id": 5, "target_id": 7, "weight": 100},
                # weight=0 should be skipped, but list must contain ≥1 valid
            ],
        })
        r = _redis_with_hashes({
            "offer_target:7": {"url": "https://t-7", "is_default": "0"},
        })
        result = await execute_action(
            r, flow, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=_stub_build_url(),
        )
        assert result is not None
        assert result["offer_id"] == "5"
        assert result["target_id"] == "7"

    @pytest.mark.asyncio
    async def test_split_zero_weights_returns_none(self):
        flow = _flow("split", {
            "offers": [
                {"offer_id": 5, "target_id": 7, "weight": 0},
                {"offer_id": 6, "target_id": 8, "weight": 0},
            ],
        })
        result = await execute_action(
            _redis_with_hashes({}), flow, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=_stub_build_url(),
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_split_empty_offers_returns_none(self):
        flow = _flow("split", {"offers": []})
        result = await execute_action(
            _redis_with_hashes({}), flow, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=_stub_build_url(),
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_split_skips_invalid_entries(self):
        flow = _flow("split", {
            "offers": [
                "not-a-dict",
                {"offer_id": -1, "weight": 100},  # bad offer_id
                {"offer_id": 5, "target_id": 7, "weight": -5},  # bad weight
                {"offer_id": 8, "target_id": 9, "weight": 50},  # only valid one
            ],
        })
        r = _redis_with_hashes({
            "offer_target:9": {"url": "https://only-valid", "is_default": "0"},
        })
        result = await execute_action(
            r, flow, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=_stub_build_url(),
        )
        assert result is not None
        assert result["offer_id"] == "8"

    @pytest.mark.asyncio
    async def test_split_no_offers_key_returns_none(self):
        flow = _flow("split", {})
        result = await execute_action(
            _redis_with_hashes({}), flow, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=_stub_build_url(),
        )
        assert result is None


# ============================================================
# Block
# ============================================================


class TestBlock:
    @pytest.mark.asyncio
    async def test_block_returns_sentinel(self):
        flow = _flow("block", {})
        result = await execute_action(
            _redis_with_hashes({}), flow, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=_stub_build_url(),
        )
        assert result == BLOCK_RESULT
        assert result["action"] == "block"
        assert result["url"] is None

    @pytest.mark.asyncio
    async def test_block_with_alert_subconfig(self):
        """Alert sub-config doesn't change execution — Stage 6 handles it."""
        flow = _flow("block", {
            "code": 404,
            "alert": {"severity": "high", "message": "lost!", "_origin": "auto-cascade"},
        })
        result = await execute_action(
            _redis_with_hashes({}), flow, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=_stub_build_url(),
        )
        assert result == BLOCK_RESULT


# ============================================================
# Unknown / corrupt actions
# ============================================================


class TestEdge:
    @pytest.mark.asyncio
    async def test_unknown_action_type_returns_none(self):
        flow = _flow("teleport", {})
        result = await execute_action(
            _redis_with_hashes({}), flow, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=_stub_build_url(),
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_corrupt_action_config_treated_as_empty(self):
        flow = {"_id": "1", "action_type": "redirect", "action_config": "{not json"}
        result = await execute_action(
            _redis_with_hashes({}), flow, _click(), "1",
            source_mappings=None, campaign_mappings=None,
            build_url_fn=_stub_build_url(),
        )
        # Empty config → no `url` → returns None.
        assert result is None


# ============================================================
# Pure helpers
# ============================================================


class TestHelpers:
    def test_parse_action_config_dict_passthrough(self):
        assert _parse_action_config({"x": 1}) == {"x": 1}

    def test_parse_action_config_string_json(self):
        assert _parse_action_config('{"x": 1}') == {"x": 1}

    def test_parse_action_config_bad_json(self):
        assert _parse_action_config("{bad") == {}

    def test_parse_action_config_none(self):
        assert _parse_action_config(None) == {}

    def test_parse_action_config_list_returns_empty(self):
        """JSON could parse to a list — we want a dict, refuse."""
        assert _parse_action_config('[1,2,3]') == {}

    def test_is_positive_int_strict(self):
        assert _is_positive_int(1) is True
        assert _is_positive_int(0) is False
        assert _is_positive_int(-1) is False
        assert _is_positive_int("1") is False  # string rejected
        assert _is_positive_int(1.0) is False  # float rejected
        assert _is_positive_int(None) is False
        # Bool explicitly rejected even though `isinstance(True, int)`.
        # JSON-parsed `{"offer_id": true}` must NEVER be treated as id=1.
        assert _is_positive_int(True) is False
        assert _is_positive_int(False) is False
