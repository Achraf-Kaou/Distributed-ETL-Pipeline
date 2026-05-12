#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"

missing=0
for cmd in java python3 docker; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "❌ Missing required command: $cmd"
    missing=1
  else
    echo "✅ Found $cmd"
  fi
done

if command -v sbt >/dev/null 2>&1; then
  echo "✅ Found sbt"
else
  echo "⚠️ sbt is not installed or not on PATH"
  missing=1
fi

if [[ ! -f "$ROOT_DIR/application.conf" ]]; then
  echo "❌ application.conf not found"
  missing=1
else
  echo "✅ application.conf found"
fi

if [[ ! -f "$COMPOSE_FILE" ]]; then
  echo "❌ docker-compose.yml not found"
  missing=1
else
  echo "✅ docker/docker-compose.yml found"
fi

if [[ $missing -ne 0 ]]; then
  exit 1
fi
