#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

if [ ! -f pokieticker.db ] && [ -f pokieticker.db.gz ]; then
  echo "[start] unpacking pokieticker.db.gz -> pokieticker.db"
  gzip -dc pokieticker.db.gz > pokieticker.db
fi

exec uvicorn backend.api.main:app --host 0.0.0.0 --port "${PORT:-8000}"
