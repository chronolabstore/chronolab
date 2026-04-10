#!/usr/bin/env bash
set -euo pipefail

CONF_FILE="/etc/ssh/sshd_config.d/99-chronolab.conf"
BACKUP_DIR="/root/backup/sshd"
STAMP="$(date +%Y%m%d-%H%M%S)"

mkdir -p "$BACKUP_DIR"

if [ ! -s /root/.ssh/authorized_keys ]; then
  echo "[warn] /root/.ssh/authorized_keys is missing or empty."
  echo "[warn] skip SSH hardening to avoid accidental lockout."
  exit 0
fi

if [ -f "$CONF_FILE" ]; then
  cp -a "$CONF_FILE" "$BACKUP_DIR/99-chronolab.conf.$STAMP.bak"
fi

cat > "$CONF_FILE" <<'EOF'
Port 22
PubkeyAuthentication yes
PasswordAuthentication no
PermitRootLogin prohibit-password
KbdInteractiveAuthentication no
ChallengeResponseAuthentication no
UsePAM yes
EOF

chmod 644 "$CONF_FILE"

sshd -t
systemctl restart ssh

echo "[ok] ssh hardened (port 22 + key-only)."
echo "[ok] config: $CONF_FILE"
