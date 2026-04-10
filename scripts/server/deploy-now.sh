#!/usr/bin/env bash
set -euo pipefail

DEFAULT_APP_DIR="/var/www/chronolab"
LEGACY_APP_DIR="/var/www/chrono-lab"
if [ -n "${APP_DIR:-}" ]; then
  APP_DIR="${APP_DIR}"
elif [ -d "$DEFAULT_APP_DIR" ]; then
  APP_DIR="$DEFAULT_APP_DIR"
elif [ -d "$LEGACY_APP_DIR" ]; then
  APP_DIR="$LEGACY_APP_DIR"
else
  APP_DIR="$DEFAULT_APP_DIR"
fi
BRANCH="${BRANCH:-main}"
PM2_APP="${PM2_APP:-chronolab}"
ENV_FILE="${ENV_FILE:-$APP_DIR/.env}"

cd "$APP_DIR"

echo "[1/6] fetch latest ($BRANCH)"
git fetch origin "$BRANCH"

echo "[2/6] checkout $BRANCH"
git checkout -q "$BRANCH"

echo "[3/6] pull latest"
git pull --ff-only origin "$BRANCH"

echo "[4/6] install production dependencies"
npm install --omit=dev

echo "[5/6] restart app"
if pm2 describe "$PM2_APP" >/dev/null 2>&1; then
  pm2 restart "$PM2_APP" --update-env
else
  if [ -f "$ENV_FILE" ]; then
    pm2 start "$APP_DIR/server.js" --name "$PM2_APP" --node-args="--env-file=$ENV_FILE"
  else
    pm2 start "$APP_DIR/server.js" --name "$PM2_APP"
  fi
fi
pm2 save

echo "[6/6] healthcheck"
"$APP_DIR/scripts/server/healthcheck.sh" "${1:-chronolab.co.kr}"

echo "[ok] deploy completed"
