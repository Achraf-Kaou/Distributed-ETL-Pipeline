#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"
require_cmd docker
compose down -v --remove-orphans || true
rm -f "$ROOT_DIR/docker/sqlite/data/etl.db"
rm -rf "$ROOT_DIR/output/final" "$ROOT_DIR/output/quarantine" "$ROOT_DIR/output/audit"
echo "Database volumes, SQLite file, and generated outputs were removed."
