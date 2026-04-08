#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

BACKUP_FILE="${1:-latest}"

echo "Restoring Simula data from backup: $BACKUP_FILE"
docker compose --profile ops run --rm -e BACKUP_FILE="$BACKUP_FILE" restore
