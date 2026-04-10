#!/usr/bin/env bash
set -euo pipefail

exec 9>/var/lock/chronolab-autodeploy.lock
flock -n 9 || exit 0

APP_DIR="${APP_DIR:-/var/www/chronolab}"
BRANCH="${BRANCH:-main}"
PM2_APP="${PM2_APP:-chronolab}"
ENV_FILE="${ENV_FILE:-$APP_DIR/.env}"
cd "$APP_DIR"

if ! git fetch -q origin "$BRANCH"; then
  echo "$(date '+%F %T') [error] fetch failed for $BRANCH" >&2
  exit 1
fi

LOCAL_SHA="$(git rev-parse HEAD)"
REMOTE_SHA="$(git rev-parse "origin/$BRANCH")"

if [ "$LOCAL_SHA" = "$REMOTE_SHA" ]; then
  exit 0
fi

git checkout -q "$BRANCH"
git pull --ff-only -q origin "$BRANCH"
npm install --omit=dev --no-audit --no-fund

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

echo "$(date '+%F %T') deployed ${LOCAL_SHA:0:7} -> ${REMOTE_SHA:0:7}"
