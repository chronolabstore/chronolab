#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/var/www/chrono-lab}"
PM2_APP="${PM2_APP:-chrono-lab}"

echo "[1/3] restart pm2 app: $PM2_APP"
pm2 restart "$PM2_APP" --update-env
pm2 save

echo "[2/3] reload nginx"
nginx -t
systemctl reload nginx

echo "[3/3] quick checks"
"$APP_DIR/scripts/server/healthcheck.sh" "${1:-chronolab.co.kr}"

echo "[ok] restart completed"
