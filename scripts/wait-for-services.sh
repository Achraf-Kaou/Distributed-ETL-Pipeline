#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"
require_cmd docker
load_env

wait_for_health() {
  local container="$1"
  local retries="${2:-60}"
  for ((i=1; i<=retries; i++)); do
    local status
    status="$(docker inspect --format='{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container" 2>/dev/null || true)"
    if [[ "$status" == "healthy" || "$status" == "running" || "$status" == "exited" ]]; then
      echo "✅ $container is $status"
      return 0
    fi
    sleep 2
  done
  echo "❌ Timed out waiting for $container"
  return 1
}

wait_for_health "${ETL_POSTGRES_CONTAINER:-etl-postgres}"
wait_for_health "${ETL_MYSQL_CONTAINER:-etl-mysql}"
wait_for_health "${ETL_SQLITE_CONTAINER:-etl-sqlite-init}"
wait_for_health "${ETL_PGADMIN_CONTAINER:-etl-pgadmin}"
