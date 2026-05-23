#!/usr/bin/env bash
# update.sh — in-place GIT update of a click-processor node (F.32 Track 1).
#
# Runs ON the node from the checkout dir (default /opt/tds-node). Brings the
# node to the tip of its release branch from the public mirror, byte-identical
# to a freshly-provisioned node — this is the "clear mechanism to roll out code
# to EXISTING nodes" half of F.32. PRESERVES .env (per-node secrets are not
# re-derivable here). Triggered by `tds-deploy node update` (SSH) today; an
# admin-panel trigger is deferred (needs control-plane auth, see plan D13).
set -euo pipefail

NODE_DIR="${NODE_DIR:-/opt/tds-node}"
BRANCH="${TDS_NODE_BRANCH:-main}"   # mirror release branch (monorepo stage → mirror main)
COMPOSE="docker compose -f docker-compose.node.yml"

cd "$NODE_DIR"

if [ ! -d .git ]; then
  echo "update.sh: $NODE_DIR is not a git checkout. Run the F.32 migration first" >&2
  echo "  (convert the legacy rsync dir → a git clone of the mirror)." >&2
  exit 1
fi

echo "=== TDS node update ($(date -u +%FT%TZ)) branch=$BRANCH ==="

# 1. Fetch + hard-reset code/compose/Caddyfile to the release tip. .env is
#    untracked (gitignored) → reset --hard never touches it → secrets preserved.
git fetch --depth 1 origin "$BRANCH"
git reset --hard "origin/$BRANCH"
NEW_SHA="$(git rev-parse --short HEAD)"

# 2. Stamp the running code version into .env so /health (and the admin node
#    list) shows it → drift is one-glance visible.
if [ ! -f .env ]; then
  echo "update.sh: no .env present — node not provisioned? Aborting before swap." >&2
  exit 1
fi
if grep -q '^TDS_CODE_VERSION=' .env; then
  sed -i "s/^TDS_CODE_VERSION=.*/TDS_CODE_VERSION=${NEW_SHA}/" .env
else
  echo "TDS_CODE_VERSION=${NEW_SHA}" >> .env
fi

# 3. Build-first-then-swap. NOT zero-downtime: a ~5-15s window while the new
#    click-processor container comes up (Caddy 502s briefly). Building BEFORE
#    the swap guarantees a broken build never takes the running node down.
echo "Building new image..."
$COMPOSE build click-processor
echo "Swapping click-processor (brief restart window)..."
$COMPOSE up -d --no-deps click-processor

# 4. Health-gate — abort loudly if the new container is unhealthy.
echo "Waiting for health..."
for i in $(seq 1 30); do
  if curl -sf "http://localhost:${TDS_PORT:-8100}/health" >/dev/null 2>&1; then
    echo "Healthy. Running version: ${NEW_SHA}"
    $COMPOSE ps
    exit 0
  fi
  sleep 1
done
echo "ERROR: health check failed after 30s — recent logs:" >&2
$COMPOSE logs --tail=30 click-processor >&2
exit 1
