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
Protocol 2
PubkeyAuthentication yes
PasswordAuthentication no
PermitRootLogin prohibit-password
PermitEmptyPasswords no
KbdInteractiveAuthentication no
ChallengeResponseAuthentication no
UseDNS no
X11Forwarding no
AllowAgentForwarding no
AllowTcpForwarding no
PermitTunnel no
MaxAuthTries 3
LoginGraceTime 30
MaxSessions 4
ClientAliveInterval 300
ClientAliveCountMax 2
UsePAM yes
EOF

chmod 644 "$CONF_FILE"

sshd -t
if systemctl list-unit-files | grep -q '^sshd\.service'; then
  systemctl restart sshd
else
  systemctl restart ssh
fi

echo "[ok] ssh hardened (port 22 + key-only)."
echo "[ok] config: $CONF_FILE"
