#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"
PORT="${ETL_API_MOCK_PORT:-8081}"
cd "$ROOT_DIR/data/raw/enterprise/api"
echo "Serving API mock responses from $PWD on http://localhost:$PORT"
python3 -m http.server "$PORT"
