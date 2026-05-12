#!/usr/bin/env sh
set -eu
apk add --no-cache sqlite
mkdir -p /sqlite/data
sqlite3 /sqlite/data/etl.db < /init.sql
echo "SQLite seed database refreshed at /sqlite/data/etl.db"