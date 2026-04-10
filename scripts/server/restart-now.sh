#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/var/www/chronolab}"
PM2_APP="${PM2_APP:-chronolab}"
ENV_FILE="${ENV_FILE:-$APP_DIR/.env}"

echo "[1/3] restart pm2 app: $PM2_APP"
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

echo "[2/3] reload nginx"
nginx -t
systemctl reload nginx

echo "[3/3] quick checks"
"$APP_DIR/scripts/server/healthcheck.sh" "${1:-chronolab.co.kr}"

echo "[ok] restart completed"
