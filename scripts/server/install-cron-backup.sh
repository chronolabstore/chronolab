#!/usr/bin/env bash
set -euo pipefail

SCRIPT_PATH="/usr/local/bin/chronolab-backup-nightly.sh"
LOG_PATH="/var/log/chronolab-backup.log"

install -m 755 ./scripts/server/backup-nightly.sh "$SCRIPT_PATH"
touch "$LOG_PATH"

# Daily 03:40 KST
LINE="40 3 * * * $SCRIPT_PATH >> $LOG_PATH 2>&1"
( crontab -l 2>/dev/null | grep -v "chronolab-backup-nightly.sh"; echo "$LINE" ) | crontab -

echo "[ok] installed: $SCRIPT_PATH"
echo "[ok] cron installed"
crontab -l | grep chronolab-backup-nightly.sh || true
