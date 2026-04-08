#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/var/www/chrono-lab}"
BRANCH="${BRANCH:-main}"
PM2_APP="${PM2_APP:-chrono-lab}"

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
pm2 restart "$PM2_APP" --update-env
pm2 save

echo "[6/6] healthcheck"
"$APP_DIR/scripts/server/healthcheck.sh" "${1:-chronolab.co.kr}"

echo "[ok] deploy completed"
