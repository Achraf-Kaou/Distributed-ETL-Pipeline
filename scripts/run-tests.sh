#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"
require_cmd sbt
cd "$ROOT_DIR"
sbt test
