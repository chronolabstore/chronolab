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
PM2_APP="${PM2_APP:-chronolab}"
ENV_FILE="${ENV_FILE:-$APP_DIR/.env}"

echo "[1/3] reload pm2 app: $PM2_APP"
if pm2 describe "$PM2_APP" >/dev/null 2>&1; then
  pm2 reload "$PM2_APP" --update-env || pm2 restart "$PM2_APP" --update-env
else
  if [ -f "$ENV_FILE" ]; then
    pm2 start "$APP_DIR/server.js" --name "$PM2_APP" --node-args="--env-file=$ENV_FILE"
  else
    pm2 start "$APP_DIR/server.js" --name "$PM2_APP"
  fi
fi
pm2 save

echo "[2/3] reload nginx"
nginx -t
systemctl reload nginx

echo "[3/3] quick checks"
"$APP_DIR/scripts/server/healthcheck.sh" "${1:-chronolab.co.kr}"

echo "[ok] restart completed"
