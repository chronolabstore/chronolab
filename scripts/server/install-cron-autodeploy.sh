#!/usr/bin/env bash
set -euo pipefail

SCRIPT_PATH="/usr/local/bin/chronolab-autodeploy.sh"
LOG_PATH="/var/log/chronolab-autodeploy.log"

install -m 755 ./scripts/server/autodeploy.sh "$SCRIPT_PATH"
touch "$LOG_PATH"

LINE="* * * * * $SCRIPT_PATH >> $LOG_PATH 2>&1"
( crontab -l 2>/dev/null | grep -v "chronolab-autodeploy.sh"; echo "$LINE" ) | crontab -

echo "[ok] installed: $SCRIPT_PATH"
echo "[ok] cron installed"
crontab -l | grep chronolab-autodeploy.sh || true
