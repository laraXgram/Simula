#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "[1/2] Running scraper + generator..."
docker compose --profile release run --rm scraper

echo "[2/2] Building and starting release services..."
docker compose --profile release up -d --build api-server client

echo "Release stack is up."
echo "Client: http://127.0.0.1:8888"
echo "API: http://127.0.0.1:8081"
