"""Every routing input the router reads must be able to REACH a preview.

Anchor: ``docs/development/route-preview-2026-08-31/10-NIGHT-GATE-ANCHOR-2026-09-03.md``

WHY THIS FILE EXISTS, AND WHY THE EXISTING PARITY TEST DID NOT CATCH IT.

``test_preview_router_type_parity.py`` already asserts "the preview's answer
equals the real click's answer". It does so across the FOUR ROUTER TYPES — the
four rungs of domain resolution. That is a real question and it is not this
one. It varies the HOSTNAME and holds everything else fixed, so a routing
dimension that never reaches the preview at all is invisible to it: both paths
lose the dimension equally, both agree, and the test is green.

The defect that motivated this file (measured live on staging 2026-09-04, 0/12
vs 12/12 with a healthy 12/12-vs-12/12 control) was exactly that shape:

    router._extra_click_dims()   isp_asn = str(req.asn), ALWAYS
    models.ClickRequest          asn: int = 0
    models.PreviewRequest        declared no `asn` field at all
    worker _servePreview         sent no `asn`

So a flow filtering on ``isp_asn`` matched the click and never the preview. The
landing page advertised one offer; the visitor was sent to another; nothing
anywhere logged an error. That is the precise failure the whole feature exists
to prevent.

WHAT THIS FILE PINS, AND WHY IT IS A RATCHET RATHER THAN A SPOT-CHECK.

Pinning ``asn`` alone would close one hole and leave the CLASS open: the next
routing dim added to ``router.py`` would walk past in the same way. So the guard
is derived rather than enumerated — it reads every ``req.<field>`` access in
``router.py`` and requires each one to be either declared on ``PreviewRequest``
or listed in ``NOT_A_ROUTING_INPUT`` below with a reason. A new dim therefore
fails this test until somebody makes a decision about it.

The exemption list is deliberately tiny and every entry is load-bearing:
neutralising the identity fields is the ADR-0468 invariant, not an oversight, so
the guard must ALLOW their absence while still refusing a silent new one.
"""

from __future__ import annotations

import ast
import pathlib

from app import router
from app.models import ClickRequest, PreviewRequest

_ROUTER_PY = pathlib.Path(router.__file__)

# Fields `router.py` reads off the request that are NOT routing inputs a caller
# could or should supply. Each entry needs a reason, because an entry added
# without one is how this guard would quietly stop guarding.
NOT_A_ROUTING_INPUT = {
    # Plumbing: the preview handler mints its own throwaway id
    # (`preview<hex>`), deliberately NOT reused as the later click's id.
    "click_id",
    # The three identity signals. Their absence is the ADR-0468 invariant —
    # a preview routes as a first-time visitor BY CONSTRUCTION, and the
    # structural absence from PreviewRequest is what makes a caller inventing
    # an identity unrepresentable rather than merely forbidden.
    "visitor_id",
    "identity_token",
    # Set by the handler to False; it is the zero-writes gate itself, not an
    # input anyone supplies.
    "identity_writes",
}


def _request_fields_read_by_router() -> set[str]:
    """Every `req.<attr>` in router.py, read from the source, not recalled."""
    tree = ast.parse(_ROUTER_PY.read_text())
    return {
        node.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "req"
    }


def test_every_routing_input_is_reachable_from_a_preview():
    """The ratchet. A new `req.X` in router.py must be declared on
    PreviewRequest or explicitly exempted — it may not simply go missing."""
    read = _request_fields_read_by_router()
    # Sanity: if the scan finds nothing, it is broken and its green means
    # nothing. A probe that cannot fail is not a probe.
    assert len(read) >= 10, (
        f"the AST scan found only {len(read)} `req.*` reads in router.py — it "
        "is almost certainly broken, so its result proves nothing"
    )

    missing = sorted(
        f for f in read
        if f not in PreviewRequest.model_fields and f not in NOT_A_ROUTING_INPUT
    )
    assert not missing, (
        f"router.py reads {missing} off the request, but PreviewRequest does "
        "not declare them and they are not exempted. A preview would evaluate "
        "these as their ClickRequest defaults while a real click evaluates the "
        "visitor's actual values, so the preview can predict a different route "
        "than the click takes. Either add the field to PreviewRequest (and "
        "forward it from the Worker's _servePreview payload AND the handler's "
        "ClickRequest construction), or add it to NOT_A_ROUTING_INPUT with a "
        "reason."
    )


def test_the_exemptions_are_still_real():
    """Guard the guard: an exemption for a field nobody reads any more is dead
    weight that makes the list look more considered than it is."""
    read = _request_fields_read_by_router()
    stale = sorted(f for f in NOT_A_ROUTING_INPUT if f not in read)
    assert not stale, (
        f"NOT_A_ROUTING_INPUT exempts {stale}, which router.py no longer reads. "
        "Drop the entry."
    )


def test_isp_asn_follows_the_asn_a_preview_supplies():
    """The specific regression, stated behaviourally rather than structurally.

    `_extra_click_dims` is the exact function that turns `req.asn` into the
    `isp_asn` criterion dim, so this asserts the dim a filtered flow matches on
    — not merely that a field exists somewhere.
    """
    preview = PreviewRequest(hostname="x.example", asn=39603)
    # The bridge the handler performs: PreviewRequest -> ClickRequest.
    click_req = ClickRequest(
        click_id="preview-test", hostname=preview.hostname, asn=preview.asn,
    )
    assert router._extra_click_dims(click_req)["isp_asn"] == "39603"


def test_a_preview_with_no_asn_still_reports_the_matchable_sentinel():
    """The no-data case must stay "0" rather than "", because `_extra_click_dims`
    documents "0" as MATCHABLE: an operator's `not_in ['0']` correctly excludes
    a no-ASN request, and mapping it to "" would re-open the CF-3 fail-open."""
    click_req = ClickRequest(click_id="preview-test", hostname="x.example")
    assert router._extra_click_dims(click_req)["isp_asn"] == "0"
