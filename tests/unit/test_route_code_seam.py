"""The SEAM: a code minted by `/preview` is honoured by the click that follows.

Programme SoT: ``docs/development/route-preview-2026-08-31/00-ANCHOR.md``

🔴 WHY THIS FILE EXISTS, stated bluntly because the gap it closes was invisible.

Both halves of this feature were already well covered, and neither coverage said
anything about the other:

  * ``test_route_preview_endpoint.py`` drives the REAL routing engine and checks
    that ``/preview`` mints a code which ``route_code.verify`` accepts;
  * ``test_route_code_honoured.py`` drives the REAL resolver against 25
    scenarios — forged, expired, cross-tenant, paused target, sticky pin — and
    every one of its codes is signed BY THE TEST (``route_code.sign``).

So the two ends were proven against the CODEC, never against each other — and
what that costs is MEASURED, not asserted. Three mutations, each planted and
each suite run against it:

===========================================  ======  =====  ======
mutation                                      seam    mint   honour
===========================================  ======  =====  ======
A  ``/preview`` mints for the WRONG TARGET     3 F    1 F    25 PASS
B  ``/preview`` mints for the WRONG TENANT     4 F    1 F    25 PASS
C  the honour hook serves the WRONG ID         3 F   15 PASS   4 F
===========================================  ======  =====  ======

Read the PASS cells: **each half is blind to the other half's drift.** The
honour suite signs its own codes, so nothing the mint side does can reach it;
the mint suite never runs a click, so nothing the honour side does can reach it.
Only the seam reddens on all three — and the failure it prevents is the one that
matters commercially: the landing page advertises one offer and the click
delivers another.

(The mint suite is not entirely helpless — its one ``verify`` test binds a
minted code to the decision, which is why A and B show 1 F there rather than
0. Stated because the first draft of this docstring claimed neither half said
anything about the other, and the measurement corrected it.)

This file runs the whole thing once — seed a route, POST ``/preview``, take the
``route_code`` STRING OUT OF THE RESPONSE BODY, and hand that exact string to
the honour hook the click path uses. Nothing is re-signed, nothing is
reconstructed; the only thing that crosses the seam is what a landing page would
actually put on its link.

The routing fixture is IMPORTED from the preview test rather than copied, so a
change to the seeded shape moves both sides together — a second copy would let
the seam quietly start testing a world neither half lives in.
"""

from __future__ import annotations

import asyncio

import pytest

from app import router
from app.config import settings
from app.models import ClickRequest

from tests.unit.test_route_preview_endpoint import (
    CAMPAIGN,
    COMPANY,
    FLOW,
    HOST,
    OFFER,
    ROUTE_KEY,
    SECRET,
    TARGET,
    _fake,
    _post,
    _run,
    _seed_domain_route,
)


@pytest.fixture
def enabled(monkeypatch):
    """Both flags on and one key ring, shared by mint and honour.

    That sharing is not incidental — it is the deployment invariant §23.2 names:
    the node that MINTS a code is rarely the node that HONOURS it, so a fleet
    running two different rings would verify nothing and every click would fall
    through to ordinary routing, silently. Here one ring stands in for a
    correctly distributed fleet.
    """
    monkeypatch.setattr(settings, "route_preview_enabled", True)
    monkeypatch.setattr(settings, "tds_secret_key", SECRET)
    monkeypatch.setattr(settings, "route_code_keys", f"1:{ROUTE_KEY}")
    monkeypatch.setattr(settings, "route_code_active_kid", "1")


def _mint_via_preview(store) -> dict:
    """Ask `/preview` the way a landing page does, and return its parsed body."""
    _run(_seed_domain_route(store.client()))
    body = _post(store).json()
    assert body["matched"] is True, body
    assert body["route_code"], body
    return body


def _honour(store, code, *, company_id=COMPANY, campaign_id=CAMPAIGN):
    """Feed the code to the hook the click path calls, on the SAME store."""

    # SYNC on purpose — `action_executor.pinned_target_result` is a plain `def`
    # and calls this WITHOUT await (the real one is a `functools.partial` over
    # `router.build_url`). An `async def` stub here type-checks fine, silently
    # puts a coroutine object into `result["url"]`, and the only visible trace is
    # a RuntimeWarning nobody reads. Reproduce the caller's shape, not a
    # plausible one.
    def _build_url(url_template, _req, _campaign_id, offer_id, **_kw):
        return f"{url_template}#offer={offer_id}"

    async def _runner():
        return await router._route_code_target(
            store.client(),
            ClickRequest(
                click_id="seam-click",
                hostname=HOST,
                path="/",
                country="US",
                user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_2)",
                query_params={router.ROUTE_CODE_PARAM: code},
            ),
            campaign_id,
            company_id=company_id,
            flow_id=FLOW,
            allowed_avail=frozenset({"active"}),
            build_url_fn=_build_url,
            source_mappings={},
            campaign_mappings={},
        )

    return asyncio.run(_runner())


# --------------------------------------------------------------------------- #
# S1 — the whole point of the feature, in one run                              #
# --------------------------------------------------------------------------- #
def test_a_code_minted_by_preview_serves_the_offer_preview_promised(enabled):
    """The landing page's promise is kept.

    Not "a code verifies" and not "a signed code is honoured" — those are the
    two half-truths this file exists to join. What is asserted here is that the
    target named in the PREVIEW RESPONSE is the target the CLICK is served.
    """
    store = _fake()
    body = _mint_via_preview(store)

    result = _honour(store, body["route_code"])

    assert result is not None, "the click did not honour a code preview just minted"
    assert str(result["target_id"]) == str(body["offer_target_id"])
    assert str(result["offer_id"]) == str(body["offer_id"])
    assert result["target_selection_path"] == "route_code"
    # And the URL is a real string built from the CODED target's template — not
    # a coroutine, and not the template of whatever ordinary routing would have
    # picked. This is what the visitor is actually sent to.
    assert isinstance(result["url"], str)
    assert result["url"].startswith("https://advertiser.example/landing")


def test_the_target_served_is_the_one_the_routing_engine_actually_chose(enabled):
    """And that target is not a constant the fixture happens to name — it is
    what the engine decided, which is why the preview ran the real `route()`."""
    store = _fake()
    body = _mint_via_preview(store)

    assert body["offer_target_id"] == TARGET
    assert body["offer_id"] == OFFER

    result = _honour(store, body["route_code"])
    assert str(result["target_id"]) == str(TARGET)


# --------------------------------------------------------------------------- #
# The seam's negative directions — each one a way the halves could disagree    #
# --------------------------------------------------------------------------- #
def test_the_same_code_is_refused_for_a_different_tenant(enabled):
    """S7 across the seam. The honour side binds the code's tenant to the
    company resolved for THIS click, so a real minted code carried to another
    company's campaign is ignored — not an error, just ordinary routing."""
    store = _fake()
    body = _mint_via_preview(store)

    assert _honour(store, body["route_code"], company_id=COMPANY + 1) is None


def test_a_minted_code_is_refused_once_its_target_is_paused(enabled):
    """S2 across the seam, and the property that makes carrying a decision in
    the client safe at all: the code buys the target a HEARING, never a
    guarantee. Availability is re-read at click time from the same keyspace."""
    store = _fake()
    body = _mint_via_preview(store)

    _run(store.client().hset(f"offer_target:{TARGET}", mapping={"availability": "paused"}))

    assert _honour(store, body["route_code"]) is None


def test_a_minted_code_is_refused_once_its_target_vanishes(enabled):
    """S4 across the seam — a target dropped from Redis between the preview and
    the click. Fail-open: the visitor is routed, not errored."""
    store = _fake()
    body = _mint_via_preview(store)

    _run(store.client().delete(f"offer_target:{TARGET}"))

    assert _honour(store, body["route_code"]) is None


def test_the_flag_off_ignores_a_code_this_very_run_minted(enabled, monkeypatch):
    """S9 across the seam. A node whose flag is off must behave exactly as it
    did before the feature existed, even holding a code it could verify — which
    is what makes a staged, per-node rollout safe."""
    store = _fake()
    body = _mint_via_preview(store)

    monkeypatch.setattr(settings, "route_preview_enabled", False)
    assert _honour(store, body["route_code"]) is None


def test_honouring_a_freshly_minted_code_writes_nothing(enabled):
    """§21.2 across the seam: the code never mutates returning-user state.

    Asserted on the store the preview itself used, so this covers BOTH legs of
    the round trip — the mint and the honour — against one keyspace, rather
    than each half against its own stub.
    """
    store = _fake()
    body = _mint_via_preview(store)

    keys_after_mint = _run(store.client().keys("*"))
    result = _honour(store, body["route_code"])
    keys_after_honour = _run(store.client().keys("*"))

    # 🔴 The discriminator. `_route_code_target` is FAIL-OPEN by construction:
    # any fault inside it returns None and the click routes normally. So a
    # keyspace that did not change is equally consistent with "the code was
    # honoured and wrote nothing" and with "the hook blew up and never ran" —
    # the second would make this test vacuous while looking green. Assert the
    # honour actually HAPPENED before believing what it did not do.
    assert result is not None and result["target_selection_path"] == "route_code"

    assert sorted(keys_after_honour) == sorted(keys_after_mint), (
        "honouring a route code changed the routing keyspace"
    )
