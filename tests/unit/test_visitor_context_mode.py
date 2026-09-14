"""ADR-0542 — the node's OWN arm of the visitor-source contract.

Anchor:  ``docs/development/server-supplied-visitor-2026-09-14/00-ANCHOR.md`` (C4)
Matrix:  ``docs/development/server-supplied-visitor-2026-09-14/30-USE-CASE-MATRIX.md``

Every test cites a matrix row (``M-N*``). The worker is the PRIMARY gate — a node
422 never reaches the caller, because the worker retries any non-OK answer
against a second node and then reports a generic failure. This file is therefore
about DEFENCE IN DEPTH, and that framing decides what is worth testing: not
"does the node produce a nice error", but "can a node be talked into answering
from values it was never given".

🔴 THE LOAD-BEARING TEST IS ``test_the_worker_and_both_models_agree_on_the_key_set``.
The models' own docstring claims the field list "MIRRORS ``VISITOR_REQUIRED_KEYS``
in ``services/worker/src/index.js``, and the mirror is pinned by a test rather
than by anyone remembering". That test is this one. Two lists that must agree and
have no mechanism forcing them to are a defect waiting for its first divergence:
a field added to the worker but not here would be accepted by the worker and then
treated as never-sent by the node.
"""

import re
from pathlib import Path

import pytest
from pydantic import ValidationError

from app.models import (
    VISITOR_CONTEXT_REQUIRED_FIELDS,
    PreviewRequest,
    PreviewResponse,
    WallRequest,
    WallResponse,
)

# Both request models are exercised by every test, because they are two separate
# declarations of one contract: a fix applied to one is invisible in the other,
# and they share a criterion vocabulary, so a disagreement between them would be
# a silent disagreement about the same stored criterion (risk A33).
REQUEST_MODELS = [PreviewRequest, WallRequest]
RESPONSE_MODELS = [PreviewResponse, WallResponse]

# A complete visitor, as the worker sends it under `payload`.
COMPLETE = {
    "country": "BR",
    "region": "SP",
    "city": "Sao Paulo",
    "user_agent": "Mozilla/5.0 (iPhone)",
    "accept_language": "pt-BR",
    "referer": "https://ref.test/p",
    "asn": 28573,
    "continent": "SA",
    "timezone": "America/Sao_Paulo",
    "is_bot": False,
    "is_proxy": False,
}


def _build(model, **overrides):
    """A minimally-valid request for either model, plus whatever the test sets."""
    return model(hostname="x.geotdsclicks.com", path="/go", **overrides)


# ---------------------------------------------------------------------------
# M-N7 — the ratchet. The one test that cannot be replaced by care.
# ---------------------------------------------------------------------------


def test_the_worker_and_both_models_agree_on_the_key_set():
    """M-N7 — the worker's list and the node's frozenset are ONE contract.

    Read the worker source rather than restating its list here: a copy in this
    file would be a third thing to keep in sync, which is the defect, not the
    fix.

    WHAT A DIVERGENCE WOULD DO, in each direction:

    * a key in the WORKER but not here — the worker demands it, the node treats
      it as never-sent, and `payload` mode silently loses a routing input;
    * a key here but not in the worker — the worker forwards a complete-looking
      payload that the node then REFUSES, which the worker launders into a
      generic 503 the caller cannot act on.

    Neither is visible in any other test, because each side is internally
    consistent.
    """
    worker_src = (
        Path(__file__).resolve().parents[3] / "worker" / "src" / "index.js"
    ).read_text()

    block = re.search(
        r"export const VISITOR_REQUIRED_KEYS = \[(.*?)\];", worker_src, re.S
    )
    assert block, "VISITOR_REQUIRED_KEYS not found in the worker source"
    worker_keys = set(re.findall(r"'([a-z_]+)'", block.group(1)))

    # Calibration: if the regex silently matched nothing, the comparison below
    # would be `set() == set()` for an empty frozenset and could never fail.
    assert len(worker_keys) == 11, f"parsed {len(worker_keys)} keys, expected 11"

    assert worker_keys == set(VISITOR_CONTEXT_REQUIRED_FIELDS)


def test_the_fixture_matches_the_contract():
    """A fixture that drifted would turn every `payload` test into a M-N4 test."""
    assert set(COMPLETE) == set(VISITOR_CONTEXT_REQUIRED_FIELDS)


# ---------------------------------------------------------------------------
# M-N1 / M-N2 — the controls. Today's callers must be untouched.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("model", REQUEST_MODELS)
def test_absent_mode_is_accepted_with_nothing_supplied(model):
    """M-N1 — THE CONTROL. Every caller alive today sends exactly this.

    If this ever fails, the change has broken the front end, and no amount of
    correctness in the payload path would make that acceptable.
    """
    req = _build(model)
    assert req.visitor_context_mode is None


@pytest.mark.parametrize("model", REQUEST_MODELS)
def test_edge_mode_is_accepted_and_requires_nothing(model):
    """M-N2 — an explicit `edge` assertion carries no completeness duty."""
    req = _build(model, visitor_context_mode="edge")
    assert req.visitor_context_mode == "edge"


@pytest.mark.parametrize("model", REQUEST_MODELS)
def test_edge_mode_does_not_demand_fields_even_when_some_are_present(model):
    """M-N2 — a partially-populated `edge` request is ordinary, not suspicious.

    The worker refuses `edge` + a visitor OBJECT, but flat edge-read fields are
    exactly what every request has always carried; demanding completeness here
    would 422 the front end.
    """
    req = _build(model, visitor_context_mode="edge", country="DE")
    assert req.country == "DE"


# ---------------------------------------------------------------------------
# M-N3 / M-N4 / M-N5 — the assertion, and both directions of getting it wrong
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("model", REQUEST_MODELS)
def test_payload_mode_with_every_field_is_accepted(model):
    """M-N3 — the happy path, and the values survive."""
    req = _build(model, visitor_context_mode="payload", **COMPLETE)
    assert req.visitor_context_mode == "payload"
    assert req.country == "BR"
    assert req.asn == 28573


@pytest.mark.parametrize("model", REQUEST_MODELS)
@pytest.mark.parametrize("omitted", sorted(VISITOR_CONTEXT_REQUIRED_FIELDS))
def test_payload_mode_refuses_when_any_single_field_is_missing(model, omitted):
    """M-N4 — every one of the eleven, one at a time, on both models.

    Parametrised over the contract rather than over a hand-picked field or two:
    a test that only ever omits `city` cannot see a validator that special-cases
    `city`. 22 cases, and each names the field it dropped.
    """
    supplied = {k: v for k, v in COMPLETE.items() if k != omitted}
    with pytest.raises(ValidationError) as exc:
        _build(model, visitor_context_mode="payload", **supplied)
    assert omitted in str(exc.value), "the error must NAME the missing field"


@pytest.mark.parametrize("model", REQUEST_MODELS)
def test_payload_mode_names_every_missing_field_not_just_the_first(model):
    """M-N4 — a half-built object gets one answer, not eleven round-trips."""
    supplied = {k: v for k, v in COMPLETE.items() if k not in {"city", "asn"}}
    with pytest.raises(ValidationError) as exc:
        _build(model, visitor_context_mode="payload", **supplied)
    message = str(exc.value)
    assert "asn" in message and "city" in message


@pytest.mark.parametrize("model", REQUEST_MODELS)
def test_payload_mode_accepts_fields_that_are_present_but_empty(model):
    """M-N5 — REQUIRED KEYS, NULLABLE VALUES, and this is the calibration.

    🔴 WITHOUT THIS TEST THE SUITE ABOVE IS SATISFIED BY A VALIDATOR THAT
    REFUSES EVERYTHING. Cloudflare does not always resolve a field for a REAL
    click either, so refusing an empty value would refuse a visitor the click
    path happily serves — and the two paths must agree, because
    preview-vs-click fidelity is the property this feature exists to preserve.

    It is also the only test that can tell `model_fields_set` from a
    truthiness check: every value below is falsy, and all of them were SENT.
    """
    empties = {
        "country": "", "region": "", "city": "",
        "user_agent": "", "accept_language": "", "referer": "",
        "asn": 0, "continent": "", "timezone": "",
        "is_bot": False, "is_proxy": False,
    }
    req = _build(model, visitor_context_mode="payload", **empties)
    assert req.visitor_context_mode == "payload"
    assert req.country == ""
    assert req.asn == 0


@pytest.mark.parametrize("model", REQUEST_MODELS)
def test_model_fields_set_is_what_distinguishes_sent_from_unsent(model):
    """M-N5 — the MECHANISM, asserted directly rather than inferred.

    Every field carries a default, so a value-level check cannot separate
    "sent empty" from "never sent". This pins the one property that can, so a
    future refactor toward `if not self.country` fails here with a reason
    rather than in production with a wrong route.
    """
    sent_empty = _build(model, country="")
    never_sent = _build(model)
    assert "country" in sent_empty.model_fields_set
    assert "country" not in never_sent.model_fields_set
    assert sent_empty.country == never_sent.country == ""


# ---------------------------------------------------------------------------
# M-N6 — an unknown mode is refused, never defaulted
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("model", REQUEST_MODELS)
@pytest.mark.parametrize("bad", ["sideways", "Payload", "PAYLOAD", "edge ", "", "payload;edge"])
def test_an_unknown_mode_is_refused(model, bad):
    """M-N6 — a caller that misspells the mode must not receive the other one.

    Case matters deliberately: accepting `Payload` would mean a caller could get
    edge behaviour from a string they believe asserts payload — the exact
    confusion ADR-0542 exists to make impossible.
    """
    with pytest.raises(ValidationError):
        _build(model, visitor_context_mode=bad, **COMPLETE)


# ---------------------------------------------------------------------------
# M-S — the echo the worker checks
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("model", RESPONSE_MODELS)
def test_both_response_models_can_carry_the_echo(model):
    """M-S3 — the field exists on BOTH responses, defaulting to absent.

    `PREVIEW_PUBLIC_FIELDS` and `WALL_PUBLIC_FIELDS` are separate allowlists by
    ADR-0507 and must never be merged, so the field has to be declared twice —
    and a model that could not carry it would make every payload request 503 on
    the worker's echo check.
    """
    assert "visitor_context_applied" in model.model_fields
    assert model.model_fields["visitor_context_applied"].default is None


def test_the_echo_defaults_to_absent_not_false():
    """M-S1 — absence is the negative answer; `False` would be a third state.

    The worker asserts `!== 'payload'`. If the default were `False`, an
    unupgraded node would still be refused, but a reader of the response could
    not tell "did not apply" from "does not know the field" — and the whole
    point of the echo is that skew is legible from outside.
    """
    assert PreviewResponse(matched=False).visitor_context_applied is None
    assert WallResponse(matched=False).visitor_context_applied is None
