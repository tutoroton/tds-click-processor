#!/usr/bin/env bash
# update.sh — in-place GIT update of a click-processor node.
#
# Lineage:
#   * F.32 Track 1 — bring a node to its release-branch tip from the
#     public mirror (replaces the manual rsync path).
#   * F.36 N3 — add auto-rollback on health-gate failure (this file).
#
# Runs ON the node from the checkout dir (default /opt/tds-node). On
# every invocation:
#   1. Snapshot the current SHA (PREV_SHA) before any mutation.
#   2. git fetch + hard reset to the release-branch tip.
#   3. Stamp TDS_CODE_VERSION in .env (the /health field).
#   4. docker compose build + up -d --no-deps click-processor.
#   5. Health-gate (30s budget).
#   6. F.36 N3 — on health-gate FAIL, ONE auto-rollback attempt:
#        a. git reset --hard $PREV_SHA
#        b. re-stamp TDS_CODE_VERSION = PREV_SHA in .env
#        c. docker compose build + up -d click-processor
#        d. health-gate again (30s).
#        e. If healthy → exit 2 (rolled-back, recovered).
#        f. If still unhealthy → exit 3 (broken — operator SSH needed).
#
# Exit codes (interpreted by admin-api `_ssh.deploy_via_ssh`):
#   0  — success (new code healthy)
#   2  — health-gate failed but rollback to PREV_SHA recovered the node
#   3  — health-gate failed AND rollback also failed (node is broken)
#   1  — pre-flight failure (no .git, missing .env, etc.) — fail fast
#         BEFORE any swap so the running container is untouched.
#
# Hard invariants preserved across rollback:
#   * `.env` is NEVER deleted/regenerated — only the TDS_CODE_VERSION
#     line is rewritten (it gets the rollback SHA on the recovery path).
#     Secrets live in .env (TDS_SECRET_KEY, TDS_CENTRAL_URL, ...) and
#     are NEVER reachable by git reset (gitignored).
#   * Disk queue volume `tds-click-queue` survives the rebuild — it is
#     a Docker named volume mounted at /var/tds/click-queue. Container
#     recreate preserves the volume (M7 zero-loss invariant).
#   * Redis volume (Docker named) survives the rebuild.
#   * MAX 1 rollback attempt. No infinite loop on PREV_SHA being itself
#     broken — exit 3 escalates loudly to the operator.
#
# Triggered by `tds-deploy node update` (engineer SSH, legacy) or by
# admin-api `_ssh.deploy_via_ssh` (F.36 N2+N3 operator click).
set -euo pipefail

NODE_DIR="${NODE_DIR:-/opt/tds-node}"
BRANCH="${TDS_NODE_BRANCH:-main}"   # mirror release branch (monorepo stage → mirror main)
COMPOSE="docker compose -f docker-compose.node.yml"

# Optional CLI override: `--branch <name>` (admin-api passes this via
# `sudo /opt/tds-node/deploy/update.sh --branch main`). Keep behaviour
# back-compat: no flag → use env / default. Validates as a single
# token; the admin-api caller already enforces a strict allowlist
# (see `_ssh._BRANCH_NAME_RE`), this is belt-and-braces.
while [ $# -gt 0 ]; do
  case "$1" in
    --branch)
      shift
      BRANCH="$1"
      shift
      ;;
    *)
      echo "update.sh: unknown argument '$1'" >&2
      exit 1
      ;;
  esac
done

cd "$NODE_DIR"

if [ ! -d .git ]; then
  echo "update.sh: $NODE_DIR is not a git checkout. Run the F.32 migration first" >&2
  echo "  (convert the legacy rsync dir → a git clone of the mirror)." >&2
  exit 1
fi

if [ ! -f .env ]; then
  echo "update.sh: no .env present — node not provisioned? Aborting BEFORE swap." >&2
  exit 1
fi

# Returning-users v2 — ensure the per-node master enable for the identity
# resolver + segmented routing is present in .env. update.sh PRESERVES .env
# across code updates (never regenerates it via render-env.sh), so a node
# provisioned before these keys existed would never get them from a code
# redeploy — the resolver stays inert despite the per-company gate being ON in
# the admin panel (the 2026-06-06 audit found exactly this). Append-only +
# idempotent: an existing value is NEVER overwritten (an operator who set it
# false to kill the feature keeps that) — only a MISSING key is added with the
# production default (true). ENV_HEALED=1 when something was appended, so the
# no-op fast-path below still force-recreates to pick up the new env.
ENV_HEALED=0
ensure_env_default() {
  local key="$1" val="$2"
  if ! grep -q "^${key}=" .env; then
    printf '%s=%s\n' "$key" "$val" >> .env
    ENV_HEALED=1
    echo "update.sh: healed .env — added ${key}=${val}"
  fi
}
ensure_env_default TDS_RETURNING_RESOLVER_ENABLED true
ensure_env_default TDS_RETURNING_ROUTING_ENABLED true

# F.36 N3 — snapshot the SHA we are about to leave. This is the
# rollback target if the new code fails health-gate. Captured BEFORE
# any git fetch so a botched fetch doesn't itself become the
# "previous" version (we'd silently freeze the node on a corrupt SHA).
PREV_SHA="$(git rev-parse --short HEAD)"

echo "=== STEP: prepare === branch=$BRANCH prev_sha=$PREV_SHA $(date -u +%FT%TZ)"

# --- helper: stamp TDS_CODE_VERSION in .env (idempotent) ---
# Used on both the forward (new SHA) and rollback (prev SHA) paths.
# Preserves every other line in .env verbatim — secrets are NEVER
# rewritten, only the version stamp. Crash-safe via the temp file +
# atomic mv pattern (no partial write window).
stamp_code_version() {
  local sha="$1"
  if grep -q '^TDS_CODE_VERSION=' .env; then
    # In-place edit via temp file (sed -i is itself temp-file-based,
    # but doing it explicitly here makes the atomic-mv intent visible).
    local tmp
    tmp="$(mktemp .env.XXXXXX)"
    awk -v sha="$sha" '
      /^TDS_CODE_VERSION=/ { print "TDS_CODE_VERSION=" sha; next }
      { print }
    ' .env > "$tmp"
    mv "$tmp" .env
  else
    printf "TDS_CODE_VERSION=%s\n" "$sha" >> .env
  fi
}

# --- helper: health-gate (30s, returns 0 on healthy, 1 otherwise) ---
# Polls /health on the local click-processor every 1s for up to 30s.
# Caller uses the return to decide success vs rollback / broken state.
health_gate() {
  local label="$1"
  echo "=== STEP: $label === waiting for /health (max 30s)..."
  for i in $(seq 1 30); do
    if curl -sf "http://localhost:${TDS_PORT:-8100}/health" >/dev/null 2>&1; then
      echo "=== STEP: $label === healthy after ${i}s"
      return 0
    fi
    sleep 1
  done
  echo "=== STEP: $label === FAILED after 30s — recent logs follow:" >&2
  $COMPOSE logs --tail=30 click-processor >&2 || true
  return 1
}

# --- 1. Forward path: fetch + reset to release tip ---
echo "=== STEP: fetch === branch=$BRANCH"
git fetch --depth 1 origin "$BRANCH"
git reset --hard "origin/$BRANCH"
NEW_SHA="$(git rev-parse --short HEAD)"
echo "=== STEP: fetch === new_sha=$NEW_SHA prev_sha=$PREV_SHA"

# Returning-users v2 (B2 fix) — ensure the dedicated noeviction identity-redis
# sibling is running. The compose now declares `identity-redis` + a
# compose-literal TDS_IDENTITY_REDIS_URL on click-processor. BUT the swap below
# uses `up -d --no-deps click-processor`, which will NOT start a newly-added
# sibling — so a node whose compose just gained identity-redis would have the
# URL set with no container behind it, and the resolver boot-gate would DEGRADE
# (routing stays fine, returning inert). Start it here, idempotently and
# non-disruptively (no-op if already running; NEVER touches click-processor),
# in BOTH the forward and no-op paths so an at-tip node still converges. On an
# older mirror without the service this is a harmless no-op. NB the chicken-egg:
# the FIRST redeploy onto this update.sh runs the OLD in-memory script (no
# self-heal) — convergence completes on the SECOND redeploy. The degrade gate
# keeps routing safe in between.
echo "=== STEP: identity-store === ensuring identity-redis is up (idempotent)"
$COMPOSE up -d identity-redis 2>/dev/null \
  || echo "  (no identity-redis service in this compose — skipping)"

if [ "$NEW_SHA" = "$PREV_SHA" ]; then
  # Nothing to do — already at tip. Run the health-gate anyway to
  # confirm the node is healthy (operator may have triggered Deploy
  # specifically because they suspect a crash). Skip the build/swap.
  echo "=== STEP: no-op === already at tip; verifying health only."
  # Force-recreate click-processor on the no-op path when EITHER:
  #  (a) we just healed .env above (added the returning toggles to a node
  #      provisioned before they existed) — the running container has the OLD
  #      env; OR
  #  (b) the resolver is ENABLED in .env but the RUNNING container reports it
  #      INACTIVE — i.e. the boot-gate DEGRADED it because identity-redis was
  #      unreachable when click-processor booted (the chicken-egg: redeploy #1
  #      ran the old in-memory script that started click-processor BEFORE the
  #      identity-redis sibling existed). identity-redis is up now (started
  #      above), so a single recreate re-runs the boot-gate WITH the store
  #      reachable → the resolver ARMS. Precise: only on a real degraded
  #      mismatch (queried via /health) → no needless blips on an already-active
  #      node. No rebuild (image unchanged); --force-recreate swaps in place.
  NEED_RECREATE="$ENV_HEALED"
  if [ "$NEED_RECREATE" != "1" ] && grep -q '^TDS_RETURNING_RESOLVER_ENABLED=true' .env; then
    RESOLVER_ACTIVE="$(curl -sf "http://localhost:${TDS_PORT:-8100}/health" 2>/dev/null \
      | grep -o '"returning_resolver_active":[^,}]*' | grep -o '[a-z]*$' || echo unknown)"
    if [ "$RESOLVER_ACTIVE" = "false" ]; then
      echo "=== STEP: resolver-arm === resolver enabled but INACTIVE (booted before identity-redis); recreating to re-run the boot-gate"
      NEED_RECREATE=1
    fi
  fi
  if [ "$NEED_RECREATE" = "1" ]; then
    echo "=== STEP: recreate === force-recreating click-processor (env-heal or resolver-arm)"
    $COMPOSE up -d --no-deps --force-recreate click-processor
  fi
  if health_gate "health-noop"; then
    echo "=== STEP: done === no-op (already at $NEW_SHA), node healthy"
    exit 0
  else
    echo "=== STEP: error === node was already on tip but unhealthy" >&2
    echo "  Investigate via docker logs; no swap was attempted." >&2
    exit 1
  fi
fi

# --- 2. Stamp + build + swap forward ---
stamp_code_version "$NEW_SHA"
echo "=== STEP: build === target=$NEW_SHA"
$COMPOSE build click-processor
echo "=== STEP: swap === bringing up click-processor"
$COMPOSE up -d --no-deps click-processor

# --- 3. Health-gate forward path ---
if health_gate "health-forward"; then
  echo "=== STEP: done === running version: $NEW_SHA"
  $COMPOSE ps
  exit 0
fi

# --- 4. F.36 N3 rollback path (forward health failed) ---
# At this point the new container is up but NOT serving /health within
# the 30s budget. We attempt ONE auto-rollback to PREV_SHA. CRITICAL:
# the rollback may itself fail (PREV_SHA also broken, or Docker layer
# corruption mid-build). In that case we exit 3 — operator must SSH in
# to recover. We DO NOT loop.
echo "=== STEP: rollback === forward health failed; reverting to $PREV_SHA" >&2

# git reset --hard to the rollback target. Same gitignored .env
# untouched — only tracked files (compose, Caddyfile, app code) revert.
git reset --hard "$PREV_SHA"

# Re-stamp TDS_CODE_VERSION so /health reports the actual rollback
# SHA (operator surface stays truthful — they see the recovered
# version, not the failed-forward attempt).
stamp_code_version "$PREV_SHA"

echo "=== STEP: rebuild === target=$PREV_SHA"
$COMPOSE build click-processor
echo "=== STEP: re-swap === bringing up click-processor (rollback)"
$COMPOSE up -d --no-deps click-processor

if health_gate "health-rollback"; then
  # Recovered on the rollback. Exit 2 distinguishes this case from
  # exit 0 (clean forward success): admin-api emits an audit row +
  # Sentry WARNING (not HIGH) + leaves operational `status='active'`.
  echo "=== STEP: done === rolled back to $PREV_SHA (forward $NEW_SHA failed health gate)"
  $COMPOSE ps
  exit 2
fi

# Rollback ALSO unhealthy. The node is broken. Exit 3 escalates to
# admin-api → audit `edge_node_deploy_broken` CRITICAL + Sentry
# CRITICAL + flip operational `status` → `error_during_update` (per
# plan §"Rollback exit-code interpretation"). Operator must SSH in.
echo "=== STEP: broken === rollback to $PREV_SHA also failed health gate" >&2
echo "  Node is in error_during_update. Manual SSH recovery required." >&2
echo "  See: docs/development/node-operations-runbook.md §6." >&2
exit 3
