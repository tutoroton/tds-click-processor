#!/usr/bin/env bash
# Health check for a TDS routing node.
set -euo pipefail

PORT="${TDS_PORT:-8100}"
NODE_DIR="${NODE_DIR:-/opt/tds-node}"

echo "=== TDS Node Health ==="

# Container status
echo "--- Containers ---"
docker compose -f "$NODE_DIR/docker-compose.node.yml" ps 2>/dev/null || echo "Containers not running"

echo ""

# API health
echo "--- Health Endpoint ---"
health=$(curl -s "http://localhost:$PORT/health" 2>/dev/null)
if [ -n "$health" ]; then
    echo "$health" | python3 -m json.tool 2>/dev/null || echo "$health"
else
    echo "UNREACHABLE at localhost:$PORT"
    exit 1
fi

echo ""

# Stats
echo "--- Stats ---"
# C3/V14 (2026-08-25) — this probe must AUTHENTICATE, and it must be able to
# fail.
#
# `/stats` is key-gated for every non-loopback caller, with a loopback
# carve-out added so exactly this probe could read it without a key. That
# carve-out is dead on a deployed node: host `:8100` is published by the CADDY
# CONTAINER (`docker-compose.node.yml`), click-processor is only `expose`d, so
# every request — including this one from the host — reaches the app from
# Caddy's docker IP. `request.client.host` is never a loopback address, and
# uvicorn runs without `--proxy-headers`, so it is the socket peer and cannot
# be spoofed. Measured on node 55: `/stats` -> 403 {"detail":"Invalid TDS key"}
# while `/health` -> 200, so the node was fine and only the gate refused.
#
# 🔴 Do NOT "fix" this with `--proxy-headers` + a trusted X-Forwarded-For.
# That would let ANY external caller send `X-Forwarded-For: 127.0.0.1` and read
# node identity, config and memory size with no key — turning a broken probe
# into the unauthenticated hole the carve-out exists to prevent.
#
# The second half of the defect was that it could not fail: the gate answers
# `{"detail":"Invalid TDS key"}`, which is VALID JSON, so `json.tool` exited 0
# and the `|| echo "Stats unavailable"` fallback never ran. The operator saw a
# pretty-printed auth error where statistics belong, with no signal. Status is
# now checked explicitly rather than inferred from parseability.
stats_key="$(sed -n 's/^TDS_SECRET_KEY=//p' "$NODE_DIR/.env" 2>/dev/null | head -1)"
stats_body="$(mktemp)"
stats_code="$(curl -s -o "$stats_body" -w '%{http_code}' \
    -H "X-TDS-Key: $stats_key" \
    "http://localhost:$PORT/stats" 2>/dev/null || echo "000")"
if [ "$stats_code" = "200" ]; then
    python3 -m json.tool < "$stats_body" 2>/dev/null || cat "$stats_body"
else
    echo "Stats unavailable (HTTP $stats_code): $(head -c 200 "$stats_body")"
fi
rm -f "$stats_body"

echo ""

# Redis
echo "--- Redis ---"
docker exec tds-redis redis-cli INFO memory 2>/dev/null | grep -E "used_memory_human|maxmemory_human" || echo "Redis unreachable"
