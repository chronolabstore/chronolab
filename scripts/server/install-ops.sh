#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/var/www/chrono-lab}"
DOMAIN="${1:-chronolab.co.kr}"
WITH_SECURITY="${2:-}"

cd "$APP_DIR"

echo "[1/8] install autodeploy cron"
"$APP_DIR/scripts/server/install-cron-autodeploy.sh"

echo "[2/8] install backup cron"
"$APP_DIR/scripts/server/install-cron-backup.sh"

echo "[3/8] install logrotate"
"$APP_DIR/scripts/server/install-logrotate.sh"

if [ "$WITH_SECURITY" = "--with-security" ]; then
  echo "[4/8] harden ssh (port 22 + key-only)"
  "$APP_DIR/scripts/server/install-stable-ssh.sh"

  echo "[5/8] install fail2ban"
  "$APP_DIR/scripts/server/install-fail2ban.sh"
else
  echo "[4/8] skip security hardening (pass --with-security to enable)"
  echo "[5/8] skip fail2ban setup (pass --with-security to enable)"
fi

echo "[6/8] create initial backup now"
/usr/local/bin/chronolab-backup-nightly.sh

echo "[7/8] run status check"
"$APP_DIR/scripts/server/show-status.sh" "$DOMAIN"

echo "[8/8] cron list"
crontab -l

echo "[ok] ops setup completed"
