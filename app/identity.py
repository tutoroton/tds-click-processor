"""Returning-user identity resolver (P2, 2026-06-05). DARK by default.

Derives a canonical company-scoped ``uid`` from a click's identity signals and
the *redefined* ``is_unique`` / ``is_returning`` flags. The router calls this
ONLY when the gate-#1 master toggle (``settings.returning_resolver_enabled``)
AND a per-company flag are both set; otherwise the click is byte-identical to
pre-P2 behaviour.

Design   : docs/development/returning-users-identity-design-2026-06-04.md §1,§4,§5
Plan     : docs/development/returning-users-implementation-plan-2026-06-04.md §P2
Audit    : returning-users-regression-safety-2026-06-04.md (gates V1/#2/#6/#7/#8)

Signals, highest precedence first:
  1. ``funnel_user_id``  (L2) — the funnel's own user id. Used as identity ONLY
     when the matched source is ``trusted`` (anti-poisoning, G6). DARK in P2:
     no source carries the flag yet (admin plumbing is P4), so this tier is
     effectively dormant and identity rides the cookie vid.
  2. ``visitor_id``      (L1) — the ``_tds_vid`` cookie. Same-domain only.
  3. ``fp``              (L3) — fingerprint, future; the signal-tier list makes
     it a one-line addition at the lowest precedence (R2 §5).

Flag semantics (v2 R re-key — is_returning keyed on CAMPAIGN, add is_roaming):
  * ``is_unique``    = this click MINTED the uid (genuinely first appearance).
    Under the NX mint only the winner of a concurrent race reports True (G7).
  * ``is_returning`` = uid seen before AND ``campaign_id`` is in the uid's
    campaigns-seen set (return to the SAME campaign — segment B).
  * ``is_roaming``   = uid seen before but ``campaign_id`` is NOT in the set
    (a different campaign within the same company — segment C). Mutually
    exclusive with ``is_returning``.
  * New user ⇒ unique·True, returning/roaming False (segment A). Same-campaign
    return ⇒ returning·True (B). Different-campaign return ⇒ roaming·True (C).

Provenance (v2 R, log-not-merge):
  * ``signal_tier``      = which signal won precedence (``fuid`` | ``vid`` |
    ``none``) — observability of WHICH identity anchor resolved the uid.
  * ``identity_conflict`` = two present signals resolved to DIFFERENT existing
    uids. We do NOT live-merge — flag it, adopt the highest-precedence uid.

Hot path: ≤2 pipelined Redis round-trips on the critical path
(signal-map reads → mint-NX OR funnels SISMEMBER). The profile/attach/TTL
writes are deferred (fire-and-forget, error-swallowing — gate #8). Company-scoped
keys (gate #7, hard multi-tenant
boundary). uid is written to every click → the Redis map is rebuildable from
ClickHouse (architecture: Redis is a disposable cache).
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import re
import secrets
import time
from dataclasses import dataclass, field

from app.config import _LOCAL_ENVIRONMENTS, settings
from app.history import _offers_key, _subs_key, _targets_key
from app.redis_client import get_identity_redis

logger = logging.getLogger(__name__)

# Signal tiers in precedence order (high → low). Adding L3 later = append
# ("fp", ...) here + an extractor branch in `_present_signals` — no change to
# the resolution algorithm (R2 §5 pluggability).
_TIER_FUID = "fuid"   # funnel_user_id (L2)
_TIER_VID = "vid"     # cookie visitor id (L1)
_TIER_NONE = "none"   # no usable identity signal on this click (case A)

# DOC-1 — the `signal_tier` PROVENANCE label uses the canonical reserved-slot
# token (`funnel_user_id`), not the internal key-tier abbreviation. CRITICAL:
# the Redis KEY tier above stays "fuid"/"vid" — changing it would re-key every
# existing `id:{co}:fuid:*` identity map, orphaning returning users (they'd
# degrade to new). So we decouple: keys keep the abbreviation, the emitted
# label canonicalizes. Only `fuid`→`funnel_user_id` per DOC-1; `vid`/`none`
# unchanged (byte-identical for those).
_SIGNAL_TIER_LABEL = {_TIER_FUID: "funnel_user_id"}


def _tier_label(tier: str) -> str:
    """Map an internal key-tier to its canonical signal_tier provenance label."""
    return _SIGNAL_TIER_LABEL.get(tier, tier)


# SEC-M2 — a uid minted by this resolver is ``secrets.token_hex(16)`` = exactly
# 32 lowercase hex chars. A value READ BACK from a signal map (``id:{co}:{tier}:
# {val}``) is attacker-influenceable (the signal value is advertiser-supplied)
# and could be corrupt/poisoned. Before a read-back uid is trusted as identity
# — and especially before it is concatenated into a sticky/history Redis KEY
# (``sticky:{co}:{uid}:{camp}`` / ``id:{co}:uid:{uid}:offers`` …) — it MUST
# match this shape, else we FAIL OPEN AS NEW (ignore the hit; no key built from
# untrusted bytes). A freshly minted uid always matches → zero effect on the
# happy path (byte-identical).
_UID_RE = re.compile(r"^[0-9a-f]{32}$")


def _valid_uid(uid) -> bool:
    return isinstance(uid, str) and _UID_RE.match(uid) is not None


# Bound the campaigns-seen set so a pathological caller can't grow one uid's
# profile without limit (cardinality guard). A real uid touches a handful of
# campaigns (fewer than funnels), so 64 is generous headroom.
_MAX_CAMPAIGNS_PER_UID = 64


@dataclass(frozen=True)
class IdentityResult:
    """Resolver output stamped onto the click's attribution.

    `uid` is "" when the click carried NO usable identity signal (a brand-new,
    cookie-less visitor) — there is no persistent identity to track, so the
    click is treated as unique·new (segment A) and nothing is written.

    `is_roaming` (v2 R) — uid seen before but on a DIFFERENT campaign
    (segment C); mutually exclusive with `is_returning`. `signal_tier` /
    `identity_conflict` are provenance (which signal won; whether two signals
    disagreed — log-not-merge).

    `prev_offers` / `prev_targets` / `prev_subs` are the uid's previous-visit
    history sets (P3-written), read in RT#2 ONLY for a returning visitor when
    `with_history` is requested (routing enabled). Empty for new users / when
    routing is OFF. The cascade reads these to match the P4 `prev_*` criteria.
    """

    uid: str
    is_unique: bool
    is_returning: bool
    is_roaming: bool = False
    signal_tier: str = _TIER_NONE
    identity_conflict: bool = False
    prev_offers: frozenset = field(default_factory=frozenset)
    prev_targets: frozenset = field(default_factory=frozenset)
    prev_subs: frozenset = field(default_factory=frozenset)


def _hash(value: str) -> str:
    """Stable short hash — bounds key length + minimises PII-at-rest for the
    advertiser-supplied funnel_user_id (a raw email/UUID never becomes a key)."""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:32]


def _present_signals(
    funnel_user_id: str | None,
    visitor_id: str | None,
    source_trusted: bool,
) -> list[tuple[str, str]]:
    """The signals this click carries, in precedence order → ``[(tier, key_value)]``.

    ``funnel_user_id`` is included ONLY when the source is trusted (G6). In P2
    no source is trusted yet, so this returns at most the vid tier.
    """
    out: list[tuple[str, str]] = []
    if source_trusted and funnel_user_id:
        out.append((_TIER_FUID, _hash(funnel_user_id)))
    if visitor_id:
        out.append((_TIER_VID, visitor_id))
    return out


def _campaign_bucket(campaign_id) -> str:
    """The campaigns-seen-set member for this click — the campaign the click
    matched. Absent campaign → the "" bucket (defensive only; campaign_id is
    always present at the router call site)."""
    return str(campaign_id) if campaign_id else ""


def _sig_key(company_id: int, tier: str, value: str) -> str:
    return f"id:{company_id}:{tier}:{value}"


def _campaigns_key(company_id: int, uid: str) -> str:
    return f"id:{company_id}:uid:{uid}:campaigns"


def _profile_key(company_id: int, uid: str) -> str:
    return f"id:{company_id}:uid:{uid}"


async def resolve_identity(
    r,
    *,
    company_id: int,
    funnel_user_id: str | None,
    visitor_id: str | None,
    campaign_id,
    source_trusted: bool,
    ttl: int,
    with_history: bool = False,
) -> IdentityResult:
    """Critical-path resolution: signals → canonical uid + redefined flags.

    Concurrency-safe (gate G7): a NEW user mints via ``SET NX``; the winner
    reports ``is_unique=True``, a racing first-click reads back the winner's
    uid and reports ``is_unique=False`` — two simultaneous first clicks
    converge on ONE uid. Reuses the same atomic primitive as the click-dedup
    gate (``main.py`` ``SET … NX EX``).

    v2 R: ``is_returning`` is now keyed on CAMPAIGN (was funnel); a seen-before
    uid on a DIFFERENT campaign reports ``is_roaming=True`` instead. Also
    captures ``signal_tier`` (winning signal) + ``identity_conflict`` (two
    signals → two existing uids; log-not-merge, adopt highest precedence).

    Round-trips: RT#1 = pipelined GET of each present signal map; RT#2 =
    mint-NX (new user) OR campaigns SISMEMBER (seen-before). ≤2 either way.
    """
    signals = _present_signals(funnel_user_id, visitor_id, source_trusted)

    # Case A (R2 §1.3): no usable signal — a cookie-less, untrusted-funnel
    # visitor. No persistent identity; unique·new (segment A). No writes.
    if not signals:
        return IdentityResult(
            uid="", is_unique=True, is_returning=False, is_roaming=False,
            signal_tier=_tier_label(_TIER_NONE), identity_conflict=False,
        )

    # RT#1 — read every present signal's map in one pipeline.
    pipe = r.pipeline()
    for tier, value in signals:
        pipe.get(_sig_key(company_id, tier, value))
    hits = await pipe.execute()

    # Highest-precedence non-null hit wins (signals are precedence-ordered);
    # record WHICH tier won. identity_conflict (v2 R, log-not-merge): two
    # present signals resolve to DIFFERENT existing uids — we flag it but do
    # NOT live-merge, adopting the highest-precedence uid.
    # SEC-M2 — only a SHAPE-VALID read-back uid is trusted. A corrupt/poisoned
    # signal-map value is ignored (treated as a miss for that tier) so it is
    # never adopted as identity nor concatenated into a sticky/history key.
    resolved_uid: str | None = None
    winner_tier = signals[0][0]
    for (tier, _value), hit in zip(signals, hits):
        if hit and _valid_uid(hit):
            resolved_uid = hit
            winner_tier = tier
            break
    # Conflict is computed over VALID hits only — a malformed value is not a
    # competing identity.
    identity_conflict = len({h for h in hits if h and _valid_uid(h)}) > 1

    if resolved_uid is None:
        # NEW USER — mint on the highest-precedence present signal, in-band NX.
        new_uid = secrets.token_hex(16)
        top_tier, top_value = signals[0]
        top_key = _sig_key(company_id, top_tier, top_value)
        won = await r.set(top_key, new_uid, nx=True, ex=ttl)  # RT#2
        if won:
            return IdentityResult(
                uid=new_uid, is_unique=True, is_returning=False,
                is_roaming=False, signal_tier=_tier_label(top_tier),
                identity_conflict=identity_conflict,
            )
        # Race lost — adopt the winner's uid; NOT unique (G7). The uid was
        # minted microseconds ago → no campaigns seen yet → fresh (B/C False).
        adopted = await r.get(top_key)
        # SEC-M2 — the read-back winner must be shape-valid before we adopt it
        # as identity. A malformed value (corruption / poison at the key) →
        # FAIL OPEN AS NEW (uid="" → segment A, no persistent identity, no key
        # built from untrusted bytes). A normal race always reads our own
        # token_hex winner → valid → unchanged.
        if not _valid_uid(adopted):
            return IdentityResult(
                uid="", is_unique=True, is_returning=False, is_roaming=False,
                signal_tier=_tier_label(_TIER_NONE),
                identity_conflict=identity_conflict,
            )
        return IdentityResult(
            uid=adopted, is_unique=False, is_returning=False,
            is_roaming=False, signal_tier=_tier_label(top_tier),
            identity_conflict=identity_conflict,
        )

    # SEEN BEFORE — is_returning iff THIS campaign is in the uid's
    # campaigns-seen set (segment B); else is_roaming (segment C, different
    # campaign same company). Mutually exclusive. When routing is enabled
    # (`with_history`), the previous-visit history sets ride the SAME pipeline
    # so prev_* costs ZERO extra round-trips (RT#2 = campaigns SISMEMBER
    # (+ history SMEMBERS) in one round).
    bucket = _campaign_bucket(campaign_id)
    pipe = r.pipeline()
    pipe.sismember(_campaigns_key(company_id, resolved_uid), bucket)
    if with_history:
        pipe.smembers(_offers_key(company_id, resolved_uid))
        pipe.smembers(_targets_key(company_id, resolved_uid))
        pipe.smembers(_subs_key(company_id, resolved_uid))
    rt2 = await pipe.execute()  # RT#2

    is_returning = bool(rt2[0])
    is_roaming = not is_returning  # seen-before AND not this campaign
    if with_history:
        return IdentityResult(
            uid=resolved_uid, is_unique=False, is_returning=is_returning,
            is_roaming=is_roaming, signal_tier=_tier_label(winner_tier),
            identity_conflict=identity_conflict,
            prev_offers=frozenset(rt2[1] or ()),
            prev_targets=frozenset(rt2[2] or ()),
            prev_subs=frozenset(rt2[3] or ()),
        )
    return IdentityResult(
        uid=resolved_uid, is_unique=False, is_returning=is_returning,
        is_roaming=is_roaming, signal_tier=_tier_label(winner_tier),
        identity_conflict=identity_conflict,
    )


async def persist_identity(
    r,
    *,
    company_id: int,
    uid: str,
    funnel_user_id: str | None,
    visitor_id: str | None,
    campaign_id,
    source_trusted: bool,
    ttl: int,
) -> None:
    """Deferred, non-blocking writes (gate #8). Off the critical path:
    attach any present signal maps to the uid (alias-stitching, SET NX so a
    conflict never overwrites), record the click's CAMPAIGN, stamp first_seen,
    and slide every key's TTL forward. Any failure is swallowed — identity
    drift is acceptable; a failed write must NEVER fail the click.
    """
    if not uid:
        return  # case A — nothing persistent to write
    try:
        signals = _present_signals(funnel_user_id, visitor_id, source_trusted)
        bucket = _campaign_bucket(campaign_id)
        ckey = _campaigns_key(company_id, uid)
        pkey = _profile_key(company_id, uid)

        pipe = r.pipeline()
        # Attach/refresh each signal → uid. SET NX preserves the canonical
        # mapping on a conflict (log-not-merge, R2 §1.3 case E) while still
        # sliding the TTL via the trailing EXPIRE.
        for tier, value in signals:
            skey = _sig_key(company_id, tier, value)
            pipe.set(skey, uid, nx=True)
            pipe.expire(skey, ttl)
        # Campaigns-seen set (membership = "uid has hit this campaign before").
        pipe.sadd(ckey, bucket)
        pipe.expire(ckey, ttl)
        # Profile — first_seen stamped once; sliding TTL.
        pipe.hsetnx(pkey, "first_seen", str(int(time.time())))
        pipe.expire(pkey, ttl)
        await pipe.execute()

        # Cardinality guard — trim runaway campaign sets (rare; outside the
        # pipeline so SCARD's result is available).
        if await r.scard(ckey) > _MAX_CAMPAIGNS_PER_UID:
            logger.warning(
                "identity: campaigns set for company=%s uid=%s exceeds cap %d",
                company_id, uid, _MAX_CAMPAIGNS_PER_UID,
            )
    except Exception as e:  # pragma: no cover — best-effort, never fail a click
        logger.warning("identity persist failed (swallowed): %s", e)


async def resolve_and_stamp(
    *,
    company_id: int,
    funnel_user_id: str | None,
    visitor_id: str | None,
    campaign_id,
    source_trusted: bool,
    with_history: bool = False,
) -> IdentityResult:
    """Router entrypoint: resolve on the identity Redis, then SCHEDULE the
    deferred writes (fire-and-forget). Returns the result to stamp onto the
    click attribution. The CALLER wraps this in fail-open (gate V1).

    `campaign_id` (v2 R) keys the seen-before set — the router passes the
    matched campaign so is_returning/is_roaming are campaign-relative.
    `with_history` (P5) — the caller passes the two-layer routing gate (env
    AND per-company); only then are the prev_* history sets read (in RT#2).
    """
    r = await get_identity_redis()
    ttl = settings.returning_uid_ttl_seconds
    result = await resolve_identity(
        r,
        company_id=company_id,
        funnel_user_id=funnel_user_id,
        visitor_id=visitor_id,
        campaign_id=campaign_id,
        source_trusted=source_trusted,
        ttl=ttl,
        with_history=with_history,
    )
    # Deferred writes off the critical path. create_task so the response is not
    # blocked; the task body is fully error-swallowing.
    if result.uid:
        asyncio.create_task(
            persist_identity(
                r,
                company_id=company_id,
                uid=result.uid,
                funnel_user_id=funnel_user_id,
                visitor_id=visitor_id,
                campaign_id=campaign_id,
                source_trusted=source_trusted,
                ttl=ttl,
            )
        )
    return result


async def assert_identity_namespace_safe() -> None:
    """Gate #2 (R4 + v2 P0.3) — enforce the no-eviction requirement at boot.

    The identity store MUST NOT evict: an evicted ``id:*`` key silently
    degrades a returning visitor back to "new" AND drops their sticky pins.
    Code cannot set a Redis maxmemory policy, so we VERIFY it at boot and —
    in a non-local environment — REFUSE TO START when the identity Redis is
    absent or misconfigured, turning a silent data-quality bug into a loud
    deploy failure.

    Fires ONLY when the resolver is enabled (``returning_resolver_enabled``).
    With the resolver OFF (the dark default) this is a zero-cost no-op, so
    every existing node boots byte-identically — the gate activates only when
    an operator opts a node into identity resolution.

    Policy by environment (resolver ON):

    * **non-local** — a dedicated ``TDS_IDENTITY_REDIS_URL`` is REQUIRED.
      Empty ⇒ refuse (we will NOT silently reuse the evictable routing
      Redis). Set ⇒ the instance must be reachable AND report
      ``maxmemory-policy noeviction``; an unreachable instance or a
      confirmed eviction policy ⇒ refuse. If the policy cannot be READ
      (managed Redis may restrict ``CONFIG GET``) we log CRITICAL and allow
      boot — the operator owns that residual risk.
    * **local** — warn only, never refuse. Reusing the routing Redis is
      acceptable for dev; we probe + log the policy so the requirement stays
      visible.

    Raises ``RuntimeError`` to abort startup in the non-local refuse cases.
    """
    if not settings.returning_resolver_enabled:
        return

    is_local = settings.environment in _LOCAL_ENVIRONMENTS

    if not settings.identity_redis_url:
        if not is_local:
            raise RuntimeError(
                "TDS_IDENTITY_REDIS_URL must be set when the returning-user "
                "resolver is ENABLED in a non-local environment "
                f"(TDS_ENVIRONMENT={settings.environment!r}). The identity "
                "keyspace requires a dedicated `maxmemory-policy noeviction` "
                "Redis — an evicted identity key silently degrades a returning "
                "visitor to 'new' and loses sticky pins. Refusing to silently "
                "reuse the evictable routing Redis; point TDS_IDENTITY_REDIS_URL "
                "at a dedicated no-eviction instance."
            )
        # Local dev — reuse the routing Redis, probe + warn only.
        await _probe_identity_policy(required=False)
        return

    # A dedicated identity URL is configured — verify reachability + policy.
    await _probe_identity_policy(required=not is_local)


async def _probe_identity_policy(*, required: bool) -> None:
    """Probe the identity Redis: reachable + ``maxmemory-policy``.

    ``required=True`` (non-local) ⇒ raise ``RuntimeError`` on an unreachable
    instance or a CONFIRMED eviction policy. ``required=False`` (local) ⇒ log
    only. An UNREADABLE policy (CONFIG GET restricted) is CRITICAL-logged and
    tolerated in both modes — we cannot prove misconfiguration, so we surface
    it loudly rather than block boot.
    """
    try:
        r = await get_identity_redis()
        await r.ping()
    except Exception as e:
        msg = (
            "identity: resolver ENABLED but the identity Redis is unreachable "
            f"({e!r})."
        )
        if required:
            raise RuntimeError(
                msg + " Refusing to start in a non-local environment."
            ) from e
        logger.warning("%s Reusing routing Redis in local dev.", msg)
        return

    try:
        cfg = await r.config_get("maxmemory-policy")
        policy = (cfg or {}).get("maxmemory-policy", "")
    except Exception as e:  # CONFIG GET may be restricted on managed Redis
        logger.critical(
            "identity: resolver ENABLED but maxmemory-policy could NOT be "
            "verified (%r). Ensure the identity Redis is `noeviction` — an "
            "evicted identity key silently degrades returning visitors.", e,
        )
        return

    if policy and policy != "noeviction":
        msg = (
            "identity: resolver ENABLED but the identity Redis uses "
            f"maxmemory-policy={policy!r} (eviction). An evicted identity key "
            "silently degrades a returning visitor to 'new' and loses sticky "
            "pins. Set `maxmemory-policy noeviction`."
        )
        if required:
            raise RuntimeError(
                msg + " Refusing to start in a non-local environment."
            )
        logger.critical(msg)
        return

    logger.info(
        "identity: resolver ENABLED on %s Redis (maxmemory-policy=%r) — OK.",
        "dedicated" if settings.identity_redis_url else "shared routing",
        policy or "unknown",
    )
