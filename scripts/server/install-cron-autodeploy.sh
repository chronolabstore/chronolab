#!/usr/bin/env bash
set -euo pipefail

SCRIPT_PATH="/usr/local/bin/chronolab-autodeploy.sh"
LOG_PATH="/var/log/chronolab-autodeploy.log"

cat > "$SCRIPT_PATH" <<'SH'
#!/usr/bin/env bash
set -euo pipefail

DEFAULT_APP_DIR="/var/www/chronolab"
LEGACY_APP_DIR="/var/www/chrono-lab"
if [ -n "${APP_DIR:-}" ]; then
  APP_DIR="${APP_DIR}"
elif [ -d "$DEFAULT_APP_DIR/.git" ]; then
  APP_DIR="$DEFAULT_APP_DIR"
elif [ -d "$LEGACY_APP_DIR/.git" ]; then
  APP_DIR="$LEGACY_APP_DIR"
else
  APP_DIR="$DEFAULT_APP_DIR"
fi

exec "$APP_DIR/scripts/server/autodeploy.sh"
SH
chmod 755 "$SCRIPT_PATH"
touch "$LOG_PATH"

LINE="* * * * * $SCRIPT_PATH >> $LOG_PATH 2>&1"
( crontab -l 2>/dev/null | grep -v "chronolab-autodeploy.sh"; echo "$LINE" ) | crontab -

echo "[ok] installed: $SCRIPT_PATH"
echo "[ok] cron installed"
crontab -l | grep chronolab-autodeploy.sh || true
