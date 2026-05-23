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
curl -s "http://localhost:$PORT/stats" 2>/dev/null | python3 -m json.tool 2>/dev/null || echo "Stats unavailable"

echo ""

# Redis
echo "--- Redis ---"
docker exec tds-redis redis-cli INFO memory 2>/dev/null | grep -E "used_memory_human|maxmemory_human" || echo "Redis unreachable"
