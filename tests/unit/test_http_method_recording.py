"""D1 / V11 — the HTTP verb that minted a click must reach the click record.

Until 2026-08-26 the CF Worker never read `request.method`: every verb — GET,
POST, HEAD, OPTIONS, PUT, DELETE — minted an identical click and the method was
recorded NOWHERE, so "what share of our clicks are not GET" had no answer, not
even a bad one. These tests pin the RECORDING half only. Nothing here asserts
that any verb is refused, dropped or routed differently, because nothing is:
deciding to stop minting for HEAD/OPTIONS is a separate change that must not be
taken while the evidence is invisible.
"""
import pytest
from pydantic import ValidationError

from app.main import _phase3_attribution_fields
from app.models import ClickRequest

_RDT = "2026-08-26T00:00:00.000Z"


def _fields(**kw):
    return _phase3_attribution_fields(
        {"attribution": {}}, ClickRequest(click_id="c1", **kw), {}, _RDT,
    )


class TestTheVerbReachesTheRecord:
    @pytest.mark.parametrize(
        "verb", ["GET", "POST", "HEAD", "OPTIONS", "PUT", "DELETE"]
    )
    def test_every_verb_is_recorded_as_itself(self, verb):
        """The whole point: the six verbs that were indistinguishable are now
        distinguishable. Parametrised rather than asserted once, because a
        single GET case would pass just as happily if the field were
        hardcoded."""
        assert _fields(http_method=verb)["http_method"] == verb

    def test_a_lowercase_verb_is_normalised(self):
        """A column split across "get"/"GET" cannot be counted — which is the
        only reason this column exists."""
        assert _fields(http_method="post")["http_method"] == "POST"

    def test_an_edge_that_does_not_send_it_lands_empty(self):
        """DARK / default-safe. Expected on EVERY row until the Worker carrying
        D1 is deployed — so a reader that treats "" as "no method" would be
        wrong about every historical click."""
        assert _fields()["http_method"] == ""

    def test_the_control_a_sibling_field_still_arrives(self):
        """CONTROL. Were `_phase3_attribution_fields` to stop reading request
        fields altogether, every assertion above would still pass by landing
        "". This is the observation that could only hold if the reader works."""
        f = _fields(http_protocol="HTTP/2", http_method="HEAD")
        assert f["http_protocol"] == "HTTP/2", "the helper stopped reading req"
        assert f["http_method"] == "HEAD"


class TestDeployOrderCannotBreakRouting:
    """The Worker and the nodes deploy through DIFFERENT mechanisms (the
    admin-panel Deploy CTA vs the node deploy), so they are never simultaneous.
    Both orders must be safe, and this is the property that makes them safe."""

    def test_an_unknown_field_from_a_newer_edge_is_ignored_not_rejected(self):
        """WORKER-FIRST. A worker that sends a field the node has never heard
        of must not 422 — a rejected /decide is a failed route, i.e. real
        traffic lost. Pydantic v2's default is extra='ignore', and this pins
        it: if anyone adds model_config extra='forbid' to ClickRequest, that is
        a routing outage and it should fail HERE, not on staging."""
        req = ClickRequest(click_id="c1", some_future_edge_field="whatever")
        assert not hasattr(req, "some_future_edge_field")
        assert req.click_id == "c1"

    def test_node_first_is_safe_because_the_field_defaults(self):
        """NODE-FIRST. A node that knows the field, fed by an edge that does
        not send it, records "" and routes normally."""
        assert ClickRequest(click_id="c1").http_method == ""

    def test_the_verb_is_bounded(self):
        """It is CLIENT-chosen. An unbounded field here is the path from "we
        record the method" to "we record whatever token a scanner sent"."""
        with pytest.raises(ValidationError):
            ClickRequest(click_id="c1", http_method="X" * 21)
        # …and the boundary itself is accepted, so the cap is a cap and not an
        # off-by-one that rejects legitimate input.
        assert (
            ClickRequest(click_id="c1", http_method="X" * 20).http_method
            == "X" * 20
        )
