#!/usr/bin/env bash
set -euo pipefail

CONF_PATH="/etc/logrotate.d/chronolab"

cat > "$CONF_PATH" <<'CONF'
/var/log/chronolab-autodeploy.log /var/log/chronolab-backup.log {
  daily
  rotate 14
  missingok
  notifempty
  compress
  delaycompress
  copytruncate
}
CONF

chmod 644 "$CONF_PATH"
logrotate -d "$CONF_PATH" >/dev/null 2>&1 || true

echo "[ok] logrotate installed: $CONF_PATH"
