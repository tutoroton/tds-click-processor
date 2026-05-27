#!/usr/bin/env bash
# F.36 N3 — integration test for update.sh auto-rollback.
#
# Validates the new exit-code contract WITHOUT touching any real
# infrastructure: builds a self-contained git checkout + a stub
# `docker compose` + a stub `curl` so the test can drive update.sh
# end-to-end on a tmp dir and assert:
#
#   * Healthy forward path: exit 0.
#   * Forward unhealthy + rollback healthy: exit 2, git HEAD back at
#     PREV_SHA, .env's TDS_CODE_VERSION stamped with PREV_SHA.
#   * Forward unhealthy + rollback also unhealthy: exit 3.
#
# Gated on TDS_RUN_NODE_INTEGRATION=1 per the project's
# `writing-integration-test` skill — NOT in the default CI suite
# (creates a tmp dir, shells out to git, exercises bash control flow).
# Run locally:
#
#   TDS_RUN_NODE_INTEGRATION=1 bash services/click-processor/tests/test_update_sh_rollback.sh
#
# Reference: docs/development/PLAN-F36-node-fleet-lifecycle.md §N3.8.

set -euo pipefail

if [ "${TDS_RUN_NODE_INTEGRATION:-0}" != "1" ]; then
  echo "SKIP (set TDS_RUN_NODE_INTEGRATION=1 to enable)"
  exit 0
fi

# ─── tmpdir + checkout layout ────────────────────────────────────────
ROOT="$(mktemp -d -t tds-update-sh-XXXXXX)"
trap 'rm -rf "$ROOT"' EXIT

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
UPDATE_SH="$SCRIPT_DIR/../deploy/update.sh"
if [ ! -f "$UPDATE_SH" ]; then
  echo "FAIL: update.sh not found at $UPDATE_SH" >&2
  exit 1
fi

# Each scenario gets its own tmp checkout. We mock:
#   * docker (compose): records the build/up arguments in $ROOT/docker.log
#   * curl: returns 200 ONLY when /var/tmp/tds-healthy-$NEW_SHA exists.
# So a scenario controls health by touching the right marker file.

setup_scenario() {
  local name="$1"
  local node_dir="$ROOT/$name/node"
  local origin_dir="$ROOT/$name/origin.git"
  mkdir -p "$ROOT/$name"

  # Step 1: build an "upstream" bare repo with TWO commits (PREV, NEW)
  # via an intermediate working tree. The node will later `git fetch
  # origin main` from this bare to receive NEW.
  local seed_dir="$ROOT/$name/seed"
  mkdir -p "$seed_dir/deploy"
  cd "$seed_dir"
  cp "$UPDATE_SH" deploy/update.sh
  chmod +x deploy/update.sh
  echo "services: {}" > docker-compose.node.yml
  git init -q -b main .
  git config user.email "test@local"
  git config user.name  "test"
  git config core.hooksPath /dev/null
  echo ".env" > .gitignore
  echo "v=prev" > app.txt
  git add .gitignore app.txt docker-compose.node.yml deploy/update.sh
  git -c gc.auto=0 commit -q -m "prev"
  PREV_SHA="$(git rev-parse --short HEAD)"
  echo "v=new" > app.txt
  git -c gc.auto=0 commit -q -am "new"
  NEW_SHA="$(git rev-parse --short HEAD)"
  git init -q --bare "$origin_dir"
  git push -q "$origin_dir" main >/dev/null 2>&1

  # Step 2: clone the bare into the node dir, then hard-reset to PREV
  # so update.sh's `git fetch origin main` will pull NEW.
  cd "$ROOT/$name"
  git clone -q "$origin_dir" node >/dev/null
  cd "$node_dir"
  git config core.hooksPath /dev/null
  git -c gc.auto=0 reset --hard "$PREV_SHA" >/dev/null
  mkdir -p bin

  # Step 3: per-node .env (gitignored, secrets preserved across reset).
  echo "TDS_PORT=8100" > .env
  echo "TDS_SECRET_KEY=preserved-across-rollback" >> .env

  # Step 4: docker stub (records args, succeeds).
  cat > bin/docker <<'DOCKER_STUB'
#!/usr/bin/env bash
echo "docker $*" >> "$DOCKER_LOG"
exit 0
DOCKER_STUB
  chmod +x bin/docker
  # Step 5: curl stub returns 200 iff /tmp marker exists for the SHA
  # currently stamped in .env's TDS_CODE_VERSION line.
  cat > bin/curl <<'CURL_STUB'
#!/usr/bin/env bash
sha="$(awk -F= '/^TDS_CODE_VERSION=/ {print $2}' "$ENV_FILE" 2>/dev/null)"
if [ -f "/tmp/tds-healthy-${sha}" ]; then
  echo "ok"
  exit 0
fi
exit 22  # curl: HTTP error
CURL_STUB
  chmod +x bin/curl

  echo "$PREV_SHA" > /tmp/tds-prev-sha
  echo "$NEW_SHA" > /tmp/tds-new-sha
  export DOCKER_LOG="$node_dir/docker.log"
  export ENV_FILE="$node_dir/.env"
  # Stub PATH so update.sh's `docker compose ...` + `curl` use stubs.
  export PATH="$node_dir/bin:$PATH"
  cd "$node_dir"
}

cleanup_scenario() {
  rm -f /tmp/tds-healthy-* /tmp/tds-prev-sha /tmp/tds-new-sha
}

# ─── Scenario 1: forward healthy → exit 0 ────────────────────────────
echo "─── scenario 1: forward path healthy ───"
setup_scenario "scenario-1-forward-healthy"
NEW_SHA="$(cat /tmp/tds-new-sha)"
# Mark NEW_SHA healthy so the forward health-gate passes.
touch "/tmp/tds-healthy-${NEW_SHA}"
set +e
NODE_DIR="$PWD" bash deploy/update.sh --branch main >scenario.out 2>&1
rc=$?
set -e
if [ "$rc" -ne 0 ]; then
  echo "FAIL: scenario 1 expected exit 0, got $rc"
  cat scenario.out
  exit 1
fi
grep -q "TDS_CODE_VERSION=${NEW_SHA}" .env || {
  echo "FAIL: scenario 1 .env not stamped with NEW_SHA"; exit 1;
}
grep -q "TDS_SECRET_KEY=preserved-across-rollback" .env || {
  echo "FAIL: scenario 1 .env secret was clobbered"; exit 1;
}
echo "OK: scenario 1 — forward healthy (exit 0)"
cleanup_scenario

# ─── Scenario 2: forward unhealthy + rollback healthy → exit 2 ──────
echo "─── scenario 2: rollback recovers ───"
setup_scenario "scenario-2-rollback-recovers"
PREV_SHA="$(cat /tmp/tds-prev-sha)"
NEW_SHA="$(cat /tmp/tds-new-sha)"
# Mark PREV_SHA healthy, NEW_SHA NOT — forward fails, rollback recovers.
touch "/tmp/tds-healthy-${PREV_SHA}"
set +e
NODE_DIR="$PWD" bash deploy/update.sh --branch main >scenario.out 2>&1
rc=$?
set -e
if [ "$rc" -ne 2 ]; then
  echo "FAIL: scenario 2 expected exit 2, got $rc"
  cat scenario.out
  exit 1
fi
HEAD_NOW="$(git rev-parse --short HEAD)"
if [ "$HEAD_NOW" != "$PREV_SHA" ]; then
  echo "FAIL: scenario 2 git HEAD is $HEAD_NOW, expected $PREV_SHA"
  exit 1
fi
grep -q "TDS_CODE_VERSION=${PREV_SHA}" .env || {
  echo "FAIL: scenario 2 .env not re-stamped with PREV_SHA"
  cat .env
  exit 1
}
grep -q "TDS_SECRET_KEY=preserved-across-rollback" .env || {
  echo "FAIL: scenario 2 .env secret was clobbered during rollback"
  exit 1
}
echo "OK: scenario 2 — rollback recovers (exit 2)"
cleanup_scenario

# ─── Scenario 3: forward unhealthy + rollback also unhealthy → exit 3 ─
echo "─── scenario 3: rollback also broken ───"
setup_scenario "scenario-3-rollback-broken"
# No healthy markers anywhere — both forward AND rollback fail health.
set +e
NODE_DIR="$PWD" bash deploy/update.sh --branch main >scenario.out 2>&1
rc=$?
set -e
if [ "$rc" -ne 3 ]; then
  echo "FAIL: scenario 3 expected exit 3, got $rc"
  cat scenario.out
  exit 1
fi
echo "OK: scenario 3 — rollback also broken (exit 3)"
cleanup_scenario

# ─── Scenario 4: already at tip, healthy → exit 0 (no-op path) ──────
echo "─── scenario 4: already at tip, healthy ───"
setup_scenario "scenario-4-noop"
PREV_SHA="$(cat /tmp/tds-prev-sha)"
NEW_SHA="$(cat /tmp/tds-new-sha)"
# Fast-forward to NEW so the script's `if NEW==PREV` no-op branch fires.
# Pre-stamp .env with the SHA — in production a previous deploy would
# have stamped it; the no-op path skips re-stamping (no swap).
git -c gc.auto=0 reset --hard "$NEW_SHA" >/dev/null
echo "TDS_CODE_VERSION=${NEW_SHA}" >> .env
touch "/tmp/tds-healthy-${NEW_SHA}"
set +e
NODE_DIR="$PWD" bash deploy/update.sh --branch main >scenario.out 2>&1
rc=$?
set -e
if [ "$rc" -ne 0 ]; then
  echo "FAIL: scenario 4 expected exit 0 (no-op), got $rc"
  cat scenario.out
  exit 1
fi
grep -q "STEP: no-op" scenario.out || {
  echo "FAIL: scenario 4 expected 'STEP: no-op' marker"; exit 1;
}
echo "OK: scenario 4 — no-op (exit 0)"
cleanup_scenario

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  ALL SCENARIOS PASS — update.sh rollback contract verified"
echo "═══════════════════════════════════════════════════════════════"
