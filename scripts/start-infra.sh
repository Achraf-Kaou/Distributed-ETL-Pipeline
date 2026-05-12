#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"
require_cmd docker
compose up -d postgres mysql sqlite-init pgadmin
"$ROOT_DIR/scripts/wait-for-services.sh"
echo "Infrastructure is ready. pgAdmin: http://localhost:${ETL_PGADMIN_PORT:-5050}"
