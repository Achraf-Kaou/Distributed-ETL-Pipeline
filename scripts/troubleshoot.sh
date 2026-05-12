#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"

echo "== Docker services =="
if command -v docker >/dev/null 2>&1; then
  compose ps || true
fi

echo
printf '== Output directories ==
'
find "$ROOT_DIR/output" -maxdepth 3 -type d 2>/dev/null | sort || echo "No output generated yet"

echo
printf '== Latest audit entries ==
'
if [[ -f "$ROOT_DIR/output/audit/audit.log" ]]; then
  tail -n 20 "$ROOT_DIR/output/audit/audit.log"
else
  echo "No audit log found"
fi
