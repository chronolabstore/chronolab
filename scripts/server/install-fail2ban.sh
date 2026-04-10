#!/usr/bin/env bash
set -euo pipefail

JAIL_FILE="/etc/fail2ban/jail.d/chronolab-sshd.local"

export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y fail2ban

cat > "$JAIL_FILE" <<'EOF'
[sshd]
enabled = true
port = 22
logpath = %(sshd_log)s
backend = systemd
maxretry = 5
findtime = 10m
bantime = 1h
EOF

chmod 644 "$JAIL_FILE"
systemctl enable --now fail2ban
systemctl restart fail2ban

echo "[ok] fail2ban enabled."
fail2ban-client status sshd || true
