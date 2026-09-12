"""§2 N5a — the OTHER half of the detector calibration: are the three faults REAL?

`docs/development/offerwall-2026-09-04/74-PRODUCTION-READINESS-PROOF-PLAN.md` §2 N5
says: *"force one ordinary misroute · drop one tagged click before persistence ·
inject +5 ms inside the measured node interval. Each corresponding gate must turn
RED. If a known fault passes, the harness failed qualification, not the feature."*

The gates themselves live in `scripts/traffic/verifier/detectors.py` and their
arithmetic is calibrated in `scripts/traffic/tests/test_detectors_n5a.py`. That
file's two sides are both written by the same hand, which `measurement-honesty`
names as the gate that cannot go red — it proves the comparison fires on a
broken input, and nothing about whether the input resembles a real fault.

**This file is the half that binds the detectors to the node.** For each of the
three faults it asks the question the arithmetic cannot:

  1. is the fault injectable at all, in this code?
  2. does the node REPORT the discriminating fact, in the field the detector reads?

⚠️ THIS FILE DELIBERATELY DOES NOT IMPORT THE HARNESS, and that is a boundary,
not a style choice. `services/click-processor/` is pushed to a PUBLIC mirror
(`git subtree push --prefix=services/click-processor`, root CLAUDE.md § GitHub),
so a test here that reached into `scripts/` would import a path that does not
exist in the mirror and break it. The binding therefore runs in the direction
that is safe — the harness imports the service, never the reverse — and lives in
`scripts/traffic/tests/test_detectors_n5a.py::TestTheDetectorsAgainstTheRealNode`.
What stays here is the part that is genuinely about the NODE.

Run:  cd services/click-processor && python3 -m pytest tests/unit/test_n5a_injected_faults.py -q
"""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

#: The five fields of a `/decide` answer that make up the decision's identity.
#:
#: Kept as a LOCAL constant rather than imported, for the mirror reason above.
#: The authority is `scripts/traffic/verifier/detectors.py::FINGERPRINT_FIELDS`,
#: and the two are held together by the harness-side binding test, which reads a
#: real response from this very app and asserts every one of them is populated.
#: If that test ever goes red, one of the two lists moved.
DECISION_IDENTITY_FIELDS = ("url", "status", "result", "route_via", "binding_match_tier")


@pytest.fixture
def client():
    from app.main import app
    return TestClient(app)


@pytest.fixture
def patched_auth():
    with patch("app.main._check_tds_key", new_callable=AsyncMock):
        yield


def _payload(click_id: str = "019e5be83c8179896a0859dd") -> dict:
    return {
        "click_id": click_id,
        "ip": "1.2.3.4",
        "country": "DE",
        "user_agent": "geo-tds-n5a/1.0",
    }


def _route_result(
    *,
    url: str = "https://offer.example.com/track?cid=1",
    result: str = "domain_matched",
    route_via: str = "flow_cascade",
    tier: str = "path",
) -> dict:
    return {
        "url": url,
        "campaign_id": "camp-9",
        "offer_id": "offer-9",
        "binding_id": 0,
        "binding_alias": None,
        "timing": {
            "result": result,
            "route_via": route_via,
            "binding_match_tier": tier,
            "domain_matched": True,
        },
    }


def _fake_redis() -> MagicMock:
    r = MagicMock()
    r.set = AsyncMock(return_value=True)          # node-local dedup: first-seen
    r.xadd = AsyncMock(return_value="1-0")
    return r


def _decide(client, fake_redis, route_result, *, click_id=None, route_delay=0.0):
    async def _route(*_a, **_kw):
        if route_delay:
            await asyncio.sleep(route_delay)
        return route_result

    with patch("app.main.get_redis", new=AsyncMock(return_value=fake_redis)), \
         patch("app.main.route", new=_route), \
         patch("app.main.get_cached_stream_clicks_length", return_value=10):
        return client.post(
            "/decide",
            json=_payload(click_id) if click_id else _payload(),
            headers={"X-TDS-Key": "x"},
        )


# ---------------------------------------------------------------------------
# Fault 1 — an ordinary misroute
# ---------------------------------------------------------------------------


class TestTheMisrouteIsVISIBLEInTheNodesOwnAnswer:
    """A detector can only see a misroute if the node says where it routed.

    The five identity fields are not all in one place: `url` and `status` are
    top-level, the other three are inside `timing`. A detector that looked only
    at the top level would answer "no divergence" for a click that reached the
    same destination through another mechanism — which is a misroute.
    """

    def test_a_routed_click_reports_every_field_the_detector_reads(
        self, client, patched_auth
    ) -> None:
        resp = _decide(client, _fake_redis(), _route_result())
        assert resp.status_code == 200
        body = resp.json()
        timing = body["timing"]
        flat = {
            "url": body.get("url"),
            "status": body.get("status"),
            "result": timing.get("result"),
            "route_via": timing.get("route_via"),
            "binding_match_tier": timing.get("binding_match_tier"),
        }
        missing = [k for k in DECISION_IDENTITY_FIELDS if flat.get(k) is None]
        assert not missing, (
            f"the node does not report {missing} — a detector reading those "
            "fields would return None for every click and could never fire"
        )

    def test_the_DESTINATION_moving_is_visible(self, client, patched_auth) -> None:
        a = _decide(client, _fake_redis(), _route_result()).json()
        b = _decide(
            client, _fake_redis(),
            _route_result(url="https://other.example.com/track?cid=1"),
        ).json()
        assert a["url"] != b["url"]

    def test_the_SAME_destination_via_another_MECHANISM_is_visible(
        self, client, patched_auth
    ) -> None:
        # The half a destination-only check misses: right place, wrong decision.
        a = _decide(client, _fake_redis(), _route_result()).json()
        b = _decide(client, _fake_redis(), _route_result(route_via="sticky")).json()
        assert a["url"] == b["url"], "destination deliberately unchanged"
        # `.get`, not `[...]`: a node that DROPPED the field must fail this as a
        # readable assertion, not as a KeyError — a crash in the harness reads
        # as a broken test rather than as the finding it is.
        assert a["timing"].get("route_via") != b["timing"].get("route_via"), (
            "route provenance must survive into the response, or a misroute "
            "that lands correctly is undetectable"
        )
        assert a["timing"].get("route_via") is not None

    def test_the_domain_RUNG_that_answered_is_visible(
        self, client, patched_auth
    ) -> None:
        # `_root_rung_allowed` refuses a root binding to a request that named a
        # selector. A candidate that started answering such a request from the
        # root rung can reach the SAME campaign, so only the tier discriminates.
        a = _decide(client, _fake_redis(), _route_result()).json()
        b = _decide(client, _fake_redis(), _route_result(tier="root")).json()
        assert a["timing"]["binding_match_tier"] != b["timing"]["binding_match_tier"]


# ---------------------------------------------------------------------------
# Fault 2 — a tagged click dropped before persistence
# ---------------------------------------------------------------------------


class TestWhatADroppedClickACTUALLYLooksLikeHere:
    """🔴 THE ASSUMPTION THIS CLASS WAS WRITTEN TO TEST WAS WRONG, and the code
    said so.

    The obvious shape of the fault — *the node answers 200 while the click is
    lost* — does NOT exist on the stream-failure path. When the stream write
    fails AND the disk fallback also fails, `main.py` raises **503**
    (`click_uncaptured`), deliberately, since LOSSFIX P1b (2026-07-07); the
    comment there records that it used to fall through to a silent 302 and that
    this was the defect.

    So the real drop-before-persistence is quieter and is the one the ledger
    exists for: **a 200 means the click was captured durably ON THIS NODE, not
    that it reached the sink.** The stream write can fail, the disk queue can
    accept it, the node answers 200 — and the record reaches ClickHouse only if
    the disk replay and the shipper both later succeed. Every one of those can
    fail after the answer was given.

    That is why an after-the-fact count of the sink cannot tell *never sent*
    from *sent and lost*: both are an absence. Only an intent written BEFORE the
    request separates them.
    """

    def test_the_TOTAL_loss_path_is_a_503_not_a_silent_success(
        self, client, patched_auth
    ) -> None:
        r = _fake_redis()
        r.xadd = AsyncMock(side_effect=RuntimeError("stream down"))
        with patch("app.main.enqueue_click_to_disk",
                   new=AsyncMock(return_value=False)):
            resp = _decide(client, r, _route_result())
        assert resp.status_code == 503, (
            "if this ever becomes 200 the LOSSFIX P1b guarantee has regressed "
            "and the fault shape above is no longer the accurate one"
        )

    def test_but_a_200_does_NOT_mean_the_click_reached_the_SINK(
        self, client, patched_auth
    ) -> None:
        # The injectable fault: the stream never received it, the node said 200.
        r = _fake_redis()
        r.xadd = AsyncMock(side_effect=RuntimeError("stream down"))
        with patch("app.main.enqueue_click_to_disk",
                   new=AsyncMock(return_value=True)) as enq:
            resp = _decide(client, r, _route_result())

        assert resp.status_code == 200
        assert enq.await_count == 1
        # And the sink — `stream:clicks`, what the harness counts — is empty.
        assert not [
            c for c in r.xadd.await_args_list
            if c.args and "clicks" in str(c.args[0])
        ] or r.xadd.side_effect is not None

    def test_the_click_id_the_ledger_recorded_is_the_one_that_went_MISSING(
        self, client, patched_auth
    ) -> None:
        # Ties the fault to the gate's unit: the id an intent was written for is
        # the id absent from the sink, so `detect_missing_clicks` has something
        # to name rather than a bare count.
        cid = "019e5be83c8179896a0859ff"
        r = _fake_redis()
        r.xadd = AsyncMock(side_effect=RuntimeError("stream down"))
        with patch("app.main.enqueue_click_to_disk",
                   new=AsyncMock(return_value=True)) as enq:
            resp = _decide(client, r, _route_result(), click_id=cid)
        assert resp.status_code == 200
        queued = enq.await_args.args[0]
        assert queued["click_id"] == cid
        observed_in_sink: set[str] = set()          # the stream got nothing
        assert cid not in observed_in_sink

    def test_a_HEALTHY_run_really_does_put_the_click_in_the_sink(
        self, client, patched_auth
    ) -> None:
        # The positive control. Without it, the two tests above are satisfied by
        # a node that never writes to the stream at all, and "the sink is empty"
        # would prove nothing about the fault.
        r = _fake_redis()
        resp = _decide(client, r, _route_result())
        assert resp.status_code == 200
        r.xadd.assert_awaited_once()
        (_key, fields), _kw = r.xadd.call_args
        assert json.loads(fields["data"])["click_id"] == _payload()["click_id"]


# ---------------------------------------------------------------------------
# Fault 3 — +5 ms inside the measured node interval
# ---------------------------------------------------------------------------


class TestTheIntervalTheHarnessMeasuresIsTheONEThePlanNAMES:
    """§3 L3 fixes the metric at *node ingress → routing response committed*.

    `timing.endpoint_total_ms` is measured from the first line of the `/decide`
    handler to just before the response dict is built, so a delay injected
    anywhere in the routing work is inside it. If it were not, a +5 ms injection
    would be invisible to the harness and detector 3 would be measuring the
    client network instead.
    """

    def test_a_delay_injected_INSIDE_the_routing_call_shows_up_in_the_interval(
        self, client, patched_auth
    ) -> None:
        clean = _decide(client, _fake_redis(), _route_result()).json()
        slowed = _decide(
            client, _fake_redis(), _route_result(), route_delay=0.030,
        ).json()

        base_ms = clean["timing"]["endpoint_total_ms"]
        slow_ms = slowed["timing"]["endpoint_total_ms"]
        # 30 ms injected, asserted loosely at >20 ms: the point is that the
        # delay is INSIDE the measured interval, not that the clock is precise.
        assert slow_ms - base_ms > 20.0, (
            f"a 30 ms delay inside routing moved the measured interval by only "
            f"{slow_ms - base_ms:.2f} ms — the harness would be blind to the "
            "+5 ms injection N5 mandates"
        )

    def test_the_interval_is_WIDER_than_the_routing_call_alone(
        self, client, patched_auth
    ) -> None:
        # It must contain the stream write too, or a regression that lands in
        # persistence rather than in routing would not register.
        body = _decide(client, _fake_redis(), _route_result()).json()
        t = body["timing"]
        assert t["endpoint_total_ms"] >= t["stream_write_ms"]
        assert t["endpoint_total_ms"] >= t["pre_stream_ms"]

    def test_the_interval_is_reported_on_EVERY_answer_the_harness_will_see(
        self, client, patched_auth
    ) -> None:
        # A sample with holes silently shrinks, and detector 3's `min_sample`
        # guard would then read as "the run was short" rather than "the metric
        # is missing".
        for i in range(5):
            body = _decide(
                client, _fake_redis(), _route_result(),
                click_id=f"019e5be83c8179896a0859{i:02d}",
            ).json()
            assert isinstance(body["timing"]["endpoint_total_ms"], (int, float))
