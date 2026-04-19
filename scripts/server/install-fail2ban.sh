#!/usr/bin/env bash
set -euo pipefail

JAIL_FILE="/etc/fail2ban/jail.d/chronolab-sshd.local"
ADMIN_FILTER_FILE="/etc/fail2ban/filter.d/chronolab-admin.conf"
ADMIN_JAIL_FILE="/etc/fail2ban/jail.d/chronolab-admin.local"
RECIDIVE_JAIL_FILE="/etc/fail2ban/jail.d/chronolab-recidive.local"
NGINX_ACCESS_LOG="/var/log/nginx/access.log"

export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y fail2ban

mkdir -p "$(dirname "$NGINX_ACCESS_LOG")"
touch "$NGINX_ACCESS_LOG"

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

cat > "$ADMIN_FILTER_FILE" <<'EOF'
[Definition]
failregex = ^<HOST> - .* "(GET|POST|HEAD) /admin(?:[/?].*)? HTTP/.*" (403|429) .*
ignoreregex =
EOF

cat > "$ADMIN_JAIL_FILE" <<EOF
[chronolab-admin]
enabled = true
port = http,https
filter = chronolab-admin
logpath = $NGINX_ACCESS_LOG
backend = auto
maxretry = 8
findtime = 10m
bantime = 24h
EOF

cat > "$RECIDIVE_JAIL_FILE" <<'EOF'
[recidive]
enabled = true
logpath = /var/log/fail2ban.log
banaction = iptables-multiport
bantime = 7d
findtime = 1d
maxretry = 5
EOF

chmod 644 "$ADMIN_FILTER_FILE" "$ADMIN_JAIL_FILE" "$RECIDIVE_JAIL_FILE"
systemctl enable --now fail2ban
systemctl restart fail2ban

echo "[ok] fail2ban enabled."
fail2ban-client status sshd || true
fail2ban-client status chronolab-admin || true
fail2ban-client status recidive || true
