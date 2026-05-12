#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"
require_cmd sbt
CONFIG_FILE="${1:-application.conf}"
cd "$ROOT_DIR"
sbt "runMain Main $CONFIG_FILE"
