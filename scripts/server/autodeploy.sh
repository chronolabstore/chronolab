#!/usr/bin/env bash
set -euo pipefail

exec 9>/var/lock/chronolab-autodeploy.lock
flock -n 9 || exit 0

APP_DIR="${APP_DIR:-/var/www/chrono-lab}"
cd "$APP_DIR"

git fetch origin main
LOCAL_SHA="$(git rev-parse HEAD)"
REMOTE_SHA="$(git rev-parse origin/main)"

if [ "$LOCAL_SHA" = "$REMOTE_SHA" ]; then
  exit 0
fi

git checkout -q main
git pull --ff-only origin main
npm install --omit=dev
pm2 restart chrono-lab --update-env
pm2 save

echo "$(date '+%F %T') deployed $REMOTE_SHA"
