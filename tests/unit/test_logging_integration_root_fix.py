"""[TDSP][E19] GTD-R466 root-fix pin (2026-07-27).

Sentry's default ``LoggingIntegration`` (``event_level=ERROR``) turns every
``logger.error()``/``logger.exception()`` call into its own Sentry event —
in addition to any explicit ``capture_exception``/``capture_op_exc`` call
the same handler ALSO makes. PR #617 fixed one instance of this
(``_handle_shipper_loop_error``); this pins the root fix that closes the
whole class: ``sentry_sdk.init(..., integrations=[..., LoggingIntegration
(event_level=None)])`` in ``app/main.py``.

Two things are pinned:

1. THE MECHANISM (class, not instance) — with ``event_level=None`` active,
   a bare ``logger.error()``/``.exception()`` produces NO Sentry event,
   while an explicit ``capture_exception``/``capture_message`` call still
   does. This is the SDK's own documented contract; pinning it here means
   a `sentry-sdk` upgrade that silently changes this behaviour breaks a
   test immediately, not months later as unexplained event-volume drift.
2. SOURCE-LEVEL PIN — ``app/main.py``'s actual ``sentry_sdk.init()`` call
   passes ``LoggingIntegration(event_level=None)``. Catches a regression
   where someone edits the integrations list and drops it, silently
   reopening the double-capture defect for every ``logger.error`` in the
   service (69+ sites across 5 services were audited to have exactly this
   defect — see docs/development/e19-sentry-audit-2026-07-27/).
"""

from __future__ import annotations

import inspect
import logging

import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration
from sentry_sdk.transport import Transport

from app import main as app_main


class _CapturingTransport(Transport):
    """Test-only transport — records every envelope sent, never network I/O."""

    def __init__(self) -> None:
        super().__init__()
        self.envelopes: list = []

    def capture_envelope(self, envelope) -> None:
        self.envelopes.append(envelope)


def test_logging_integration_event_level_none_suppresses_bare_logger_calls():
    """THE CLASS, not the instance: with event_level=None, logger.error()/
    .exception() produce zero Sentry events; an explicit capture call still
    produces one. Proves the mechanism this whole fix relies on."""
    transport = _CapturingTransport()
    client = sentry_sdk.Client(
        dsn="https://public@sentry.example.invalid/1",
        transport=transport,
        integrations=[LoggingIntegration(event_level=None)],
        default_integrations=False,
    )
    logger = logging.getLogger("tds.test.logging_integration_root_fix")

    with sentry_sdk.isolation_scope() as scope:
        scope.set_client(client)

        logger.error("this must NOT become a Sentry event")
        logger.exception("this must NOT become a Sentry event either", exc_info=False)
        assert transport.envelopes == [], (
            "logger.error()/.exception() produced a Sentry event despite "
            "event_level=None — the root fix's core mechanism is broken."
        )

        try:
            raise RuntimeError("boom")
        except RuntimeError as exc:
            sentry_sdk.capture_exception(exc)
        assert len(transport.envelopes) == 1, (
            "an explicit sentry_sdk.capture_exception() did not reach the "
            "transport — event_level=None must not suppress explicit captures."
        )


def test_logging_integration_event_level_none_is_wired_into_init():
    """Source-level pin. A regression that drops ``LoggingIntegration
    (event_level=None)`` from the real ``sentry_sdk.init()`` call would pass
    the behavioural test above trivially (it builds its own isolated client)
    but silently reopen the double-capture defect in production. Assert the
    actual init call still wires it."""
    src = inspect.getsource(app_main)
    assert "LoggingIntegration(event_level=None)" in src, (
        "app/main.py's sentry_sdk.init() no longer passes "
        "LoggingIntegration(event_level=None) — every logger.error()/"
        ".exception() in this service will double-report again "
        "(GTD-R466 root-fix regression)."
    )
