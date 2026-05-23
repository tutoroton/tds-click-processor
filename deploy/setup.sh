#!/usr/bin/env bash
# First-time node setup. Run on the target server.
# Installs Docker, creates directories, prepares for deployment.
set -euo pipefail

echo "=== TDS Node Setup ==="

# Install Docker if not present
if ! command -v docker &>/dev/null; then
    echo "Installing Docker..."
    curl -fsSL https://get.docker.com | sh
    systemctl enable docker
    systemctl start docker
    echo "Docker installed"
else
    echo "Docker already installed: $(docker --version)"
fi

# Ensure docker compose plugin
if ! docker compose version &>/dev/null; then
    echo "Installing docker compose plugin..."
    apt-get update -qq && apt-get install -y -qq docker-compose-plugin
fi

# Create app directory
mkdir -p /opt/tds-node
echo "Node setup complete. Ready for deployment."
