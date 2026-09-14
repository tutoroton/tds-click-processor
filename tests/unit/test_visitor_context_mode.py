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

    # 🔴 COMMENTS ARE STRIPPED FIRST, and the partner-critic constructed the
    # exact input that makes this necessary: leave
    #     // 'referer' dropped 2026-09-20: …
    # inside the brackets after removing 'referer' from the list, and the naive
    # findall reports eleven keys INCLUDING referer. Both assertions below then
    # pass while the worker requires ten and the node eleven — the very
    # divergence this test exists to catch, hidden by the test itself.
    body = re.sub(r"/\*.*?\*/", "", block.group(1), flags=re.S)
    body = re.sub(r"//[^\n]*", "", body)
    worker_keys = set(re.findall(r"'([A-Za-z0-9_]+)'", body))

    # Calibration, and the denominator is DERIVED rather than hand-typed. A
    # literal `== 11` is maintained by the same person, on the same day, as the
    # change that legitimately moves the count — so it stops being independent
    # exactly when it is needed. The floor is independent: the contract must be
    # non-trivial, and a regex that matched nothing yields 0 and fails here
    # rather than comparing two empty sets.
    assert len(worker_keys) >= 8, f"parsed only {len(worker_keys)} keys — parser broken?"
    assert len(worker_keys) == len(VISITOR_CONTEXT_REQUIRED_FIELDS), (
        f"worker declares {len(worker_keys)} keys, node declares "
        f"{len(VISITOR_CONTEXT_REQUIRED_FIELDS)}"
    )

    assert worker_keys == set(VISITOR_CONTEXT_REQUIRED_FIELDS)


def test_the_key_set_parser_is_not_fooled_by_a_comment():
    """The parser's own calibration — the input the partner-critic constructed.

    A commented-out key inside the array literal must NOT be counted. Without
    this, the ratchet above reports agreement while the two sides disagree, and
    it is the only guard either side has.

    Also pinned: a double-quoted or capitalised key is invisible to a
    single-quoted-lowercase regex. That is why the character class is widened
    above and why the floor assertion exists — an unparseable list must fail
    loudly rather than compare two empty sets.
    """
    fixture = (
        "export const VISITOR_REQUIRED_KEYS = [\n"
        "\t'country', 'region',\n"
        "\t// 'referer' dropped 2026-09-20: nobody forwards it correctly\n"
        "\t'city', /* 'timezone' pending */ 'asn',\n"
        "];"
    )
    block = re.search(
        r"export const VISITOR_REQUIRED_KEYS = \[(.*?)\];", fixture, re.S
    )
    body = re.sub(r"/\*.*?\*/", "", block.group(1), flags=re.S)
    body = re.sub(r"//[^\n]*", "", body)
    keys = set(re.findall(r"'([A-Za-z0-9_]+)'", body))

    assert keys == {"country", "region", "city", "asn"}
    assert "referer" not in keys, "a commented-out key was counted as declared"
    assert "timezone" not in keys, "a block-commented key was counted as declared"


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


# ---------------------------------------------------------------------------
# M-S1 (node half) — the HANDLER WIRING, which had no test at all
# ---------------------------------------------------------------------------
#
# 🔴 FOUND BY THE PARTNER-CRITIC, and it is the finding with the worst live
# consequence in the whole review. Every test above is MODEL-level. The echo is
# stamped by `_stamp_visitor_context` at each handler's single exit, and that
# wiring was asserted by nothing: the critic measured that deleting the call
# from `wall()` or from `preview()`, or stamping unconditionally, or stamping a
# wrong literal, all left the suite at 2429 green.
#
# What that costs in production is total: the WORKER refuses any payload answer
# that lacks the echo, so an unstamped node yields `context_not_applied` on
# every request and the whole feature is dark — invisible to CI, and only
# discoverable by a live probe.
#
# These tests drive the real handler and monkeypatch only the BODY function, so
# they need no Redis and no database. What they exercise is exactly the seam
# that was uncovered: does the value the handler RETURNS carry the echo.


@pytest.mark.parametrize(
    "handler_name,body_name,response_factory,request_factory",
    [
        (
            "preview",
            "_preview_body",
            lambda: PreviewResponse(matched=False, reason="blocked", tenant_checked=True),
            lambda **kw: PreviewRequest(hostname="x", path="/go", **kw),
        ),
        (
            "wall",
            "_wall_body",
            lambda: WallResponse(matched=False, reason="no_wall", tenant_checked=True),
            lambda **kw: WallRequest(hostname="x", path="/go", **kw),
        ),
    ],
)
def test_the_handler_stamps_the_echo_on_a_payload_request(
    monkeypatch, handler_name, body_name, response_factory, request_factory
):
    """M-S1 — a payload request comes back from the HANDLER carrying the echo."""
    import asyncio

    from app import main
    from app.config import settings

    # The dark gate fires FIRST in both handlers, by design — a disabled
    # feature must not confirm it exists. Arm both so the request reaches the
    # exit this test is about.
    monkeypatch.setattr(settings, "route_preview_enabled", True)
    monkeypatch.setattr(settings, "offerwall_serve_enabled", True)

    # The edge-secret check is the second gate and is NOT what this test is
    # about — it authenticates the WORKER, not the tenant, and has its own
    # tests. Stub it so the request reaches the handler's exit.
    async def _ok_key(_x_tds_key):
        return 1

    monkeypatch.setattr(main, "_check_tds_key", _ok_key)

    async def _body(req, *args, **kwargs):
        return response_factory()

    monkeypatch.setattr(main, body_name, _body)

    req = request_factory(visitor_context_mode="payload", **COMPLETE)
    resp = asyncio.run(getattr(main, handler_name)(req))

    assert resp.visitor_context_applied == "payload", (
        "the handler returned an answer with no echo — the worker would refuse "
        "it and the feature would be dark on every node"
    )


@pytest.mark.parametrize(
    "handler_name,body_name,response_factory,request_factory",
    [
        (
            "preview",
            "_preview_body",
            lambda: PreviewResponse(matched=False, reason="blocked", tenant_checked=True),
            lambda **kw: PreviewRequest(hostname="x", path="/go", **kw),
        ),
        (
            "wall",
            "_wall_body",
            lambda: WallResponse(matched=False, reason="no_wall", tenant_checked=True),
            lambda **kw: WallRequest(hostname="x", path="/go", **kw),
        ),
    ],
)
def test_the_handler_does_not_stamp_an_edge_request(
    monkeypatch, handler_name, body_name, response_factory, request_factory
):
    """M-S2 — THE CONTROL, and it is what makes the test above mean something.

    An unconditional stamp would satisfy the payload test and be a lie: it would
    tell every caller their context was applied, including the front end, which
    supplied none. The critic measured that mutation as green too.
    """
    import asyncio

    from app import main
    from app.config import settings

    # The dark gate fires FIRST in both handlers, by design — a disabled
    # feature must not confirm it exists. Arm both so the request reaches the
    # exit this test is about.
    monkeypatch.setattr(settings, "route_preview_enabled", True)
    monkeypatch.setattr(settings, "offerwall_serve_enabled", True)

    # The edge-secret check is the second gate and is NOT what this test is
    # about — it authenticates the WORKER, not the tenant, and has its own
    # tests. Stub it so the request reaches the handler's exit.
    async def _ok_key(_x_tds_key):
        return 1

    monkeypatch.setattr(main, "_check_tds_key", _ok_key)

    async def _body(req, *args, **kwargs):
        return response_factory()

    monkeypatch.setattr(main, body_name, _body)

    for kwargs in ({}, {"visitor_context_mode": "edge"}):
        resp = asyncio.run(getattr(main, handler_name)(request_factory(**kwargs)))
        assert resp.visitor_context_applied is None, (
            f"an {kwargs or 'absent-mode'} request was told its context was applied"
        )


def test_the_stamped_value_is_exactly_the_literal_the_worker_checks():
    """M-S1 — the worker compares `!== 'payload'`, so a near-miss is a 503.

    Stamping "applied", or "PAYLOAD", or True would pass any is-it-set check and
    fail every live request. Pinning the literal is the cheapest guard against a
    refactor that "tidies" the value.
    """
    from app.main import _stamp_visitor_context

    resp = PreviewResponse(matched=False)
    req = PreviewRequest(hostname="x", visitor_context_mode="payload", **COMPLETE)
    assert _stamp_visitor_context(resp, req).visitor_context_applied == "payload"


def test_stamping_tolerates_a_none_response():
    """A handler arm may legitimately return None; stamping must not raise.

    Not hypothetical: the preview path returns `None` for row 3 (no credential),
    and an exception here would convert that into a 500 on the one path that is
    supposed to be an ordinary click.
    """
    from app.main import _stamp_visitor_context

    req = PreviewRequest(hostname="x", visitor_context_mode="payload", **COMPLETE)
    assert _stamp_visitor_context(None, req) is None


def test_the_echo_defaults_to_absent_not_false():
    """M-S1 — absence is the negative answer; `False` would be a third state.

    The worker asserts `!== 'payload'`. If the default were `False`, an
    unupgraded node would still be refused, but a reader of the response could
    not tell "did not apply" from "does not know the field" — and the whole
    point of the echo is that skew is legible from outside.
    """
    assert PreviewResponse(matched=False).visitor_context_applied is None
    assert WallResponse(matched=False).visitor_context_applied is None
