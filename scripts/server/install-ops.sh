#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/var/www/chrono-lab}"
DOMAIN="${1:-chronolab.co.kr}"

cd "$APP_DIR"

echo "[1/5] install autodeploy cron"
"$APP_DIR/scripts/server/install-cron-autodeploy.sh"

echo "[2/5] install backup cron"
"$APP_DIR/scripts/server/install-cron-backup.sh"

echo "[3/5] create initial backup now"
/usr/local/bin/chronolab-backup-nightly.sh

echo "[4/5] run status check"
"$APP_DIR/scripts/server/show-status.sh" "$DOMAIN"

echo "[5/5] cron list"
crontab -l

echo "[ok] ops setup completed"
