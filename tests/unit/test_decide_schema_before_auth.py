"""D2 / V17 — `/decide` must not describe its schema to an unauthenticated caller.

MEASURED ON DEPLOYED NODE 55 (2026-08-26), before the fix:

    no key   + {}                    -> 422 {"loc":["body","click_id"],
                                             "msg":"Field required"}
    no key   + {"click_id":"..."}    -> 403 {"detail":"Invalid TDS key"}
    bad key  + {"click_id":12345}    -> 422 "Input should be a valid string",
                                        echoing `12345` back

The 403 arm is what makes this a defect rather than an observation: the endpoint
IS gated, and the body validator answers in FRONT of the gate, because `/decide`
declares `req: ClickRequest` as a body model while reading `X-TDS-Key` inside the
function. So an unauthenticated caller enumerates field names, types and
required-ness one malformed request at a time.

THE PROPERTY PINNED HERE: before authentication, a malformed body and a
well-formed one are INDISTINGUISHABLE — same status, same bytes. After
authentication the full 422 diagnostic is preserved, because that is what makes a
real integration bug findable for the CF Worker.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient


VALID_BODY = {"click_id": "01a0000000000000000000d2"}
INVALID_BODIES = [
    {},                              # missing the required field
    {"click_id": 12345},             # wrong type, and it used to echo the input
    {"click_id": None},              # null
]


@pytest.fixture
def client():
    from app.main import app
    return TestClient(app, raise_server_exceptions=False)


def _unauthenticated():
    """_check_tds_key's real failure mode: it RAISES 403, never returns falsy."""
    return patch(
        "app.main._check_tds_key",
        new=AsyncMock(side_effect=HTTPException(status_code=403,
                                                detail="Invalid TDS key")),
    )


def _authenticated():
    return patch("app.main._check_tds_key", new=AsyncMock(return_value=1))


@pytest.mark.parametrize("body", INVALID_BODIES)
def test_unauthenticated_malformed_body_does_not_disclose_the_schema(client, body):
    """THE DEFECT. Pre-fix this was 422 naming `click_id`; now it is a bare 403."""
    with _unauthenticated():
        r = client.post("/decide", json=body)
    assert r.status_code == 403, f"expected 403, got {r.status_code}: {r.text}"
    text = r.text.lower()
    for leak in ("click_id", '"loc"', "field required", "body", "12345"):
        assert leak not in text, f"response leaks {leak!r}: {r.text}"


def test_unauthenticated_malformed_is_byte_identical_to_wellformed(client):
    """The strongest form of the property: an attacker cannot even tell WHICH of
    the two refusals it got, so malformed-vs-wellformed carries no signal."""
    with _unauthenticated():
        bad = client.post("/decide", json={})
        good = client.post("/decide", json=VALID_BODY)
    assert bad.status_code == good.status_code == 403
    assert bad.content == good.content, (
        f"refusals differ, so the shape of the body is still observable:\n"
        f"  malformed: {bad.content!r}\n  well-formed: {good.content!r}"
    )


def test_control_authenticated_malformed_body_KEEPS_the_diagnostic(client):
    """CONTROL, and the one that stops the fix from being 'return 403 always'.

    A caller that HAS authenticated must still get the precise validation error —
    that is the diagnostic the CF Worker needs. If this test also went 403, the
    fix would have destroyed the endpoint's usefulness instead of closing a leak.
    """
    with _authenticated():
        r = client.post("/decide", json={})
    assert r.status_code == 422, f"expected 422 for an authed caller, got {r.status_code}"
    payload = json.loads(r.text)
    assert "click_id" in json.dumps(payload), (
        "the authenticated 422 lost its field detail: " + r.text
    )


def test_control_the_gate_itself_still_refuses_a_wellformed_body(client):
    """CONTROL: the endpoint is genuinely gated. Without this, a 403 above could
    just mean '/decide refuses everything' and would prove nothing about auth."""
    with _unauthenticated():
        r = client.post("/decide", json=VALID_BODY)
    assert r.status_code == 403
    assert "Invalid TDS key" in r.text


def test_control_the_handler_is_actually_registered():
    """Structural: a handler that is not wired cannot fail, and every assertion
    above would then be describing FastAPI's default behaviour instead of ours."""
    from fastapi.exceptions import RequestValidationError

    from app.main import app

    handler = app.exception_handlers.get(RequestValidationError)
    assert handler is not None, "no RequestValidationError handler registered at all"
    # 🔴 Asserting mere PRESENCE is a TAUTOLOGY: FastAPI installs its OWN default
    # RequestValidationError handler in __init__, so `in app.exception_handlers` is
    # true with this fix reverted — measured, when the calibration run showed this
    # test passing against the unfixed tree. Identify the handler, or the assertion
    # is about the framework rather than about us.
    assert handler.__name__ == "_validation_error_handler", (
        f"the registered handler is {handler.__name__!r}, i.e. FastAPI's default — "
        "this fix is not wired, and every assertion above would be describing the "
        "framework's behaviour instead of ours"
    )
