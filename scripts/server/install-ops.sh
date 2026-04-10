#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/var/www/chronolab}"
DOMAIN="${1:-chronolab.co.kr}"
WITH_SECURITY="${2:-}"

cd "$APP_DIR"

echo "[1/9] install autodeploy cron"
"$APP_DIR/scripts/server/install-cron-autodeploy.sh"

echo "[2/9] install backup cron"
"$APP_DIR/scripts/server/install-cron-backup.sh"

echo "[3/9] install logrotate"
"$APP_DIR/scripts/server/install-logrotate.sh"

if [ "$WITH_SECURITY" = "--with-security" ]; then
  echo "[4/9] harden ssh (port 22 + key-only)"
  "$APP_DIR/scripts/server/install-stable-ssh.sh"

  echo "[5/9] install fail2ban"
  "$APP_DIR/scripts/server/install-fail2ban.sh"
else
  echo "[4/9] skip security hardening (pass --with-security to enable)"
  echo "[5/9] skip fail2ban setup (pass --with-security to enable)"
fi

echo "[6/9] configure persistent DB/upload paths"
"$APP_DIR/scripts/server/configure-persistent-storage.sh"

echo "[7/9] create initial backup now"
/usr/local/bin/chronolab-backup-nightly.sh

echo "[8/9] run status check"
"$APP_DIR/scripts/server/show-status.sh" "$DOMAIN"

echo "[9/9] cron list"
crontab -l

echo "[ok] ops setup completed"
