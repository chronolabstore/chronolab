#!/usr/bin/env bash
set -euo pipefail

PM2_APP="${PM2_APP:-chrono-lab}"
DOMAIN="${1:-chronolab.co.kr}"

echo "== PM2 =="
pm2 describe "$PM2_APP" | sed -n '1,40p'

echo "== NGINX =="
systemctl is-active nginx
nginx -t

echo "== SSH =="
ss -lntp | grep sshd || true

echo "== FAIL2BAN =="
systemctl is-active fail2ban || true
fail2ban-client status sshd || true

echo "== LOCAL APP =="
curl -sS -o /dev/null -w "http://127.0.0.1:3100/main -> %{http_code}\n" http://127.0.0.1:3100/main

echo "== DOMAIN =="
curl -sS -o /dev/null -w "https://${DOMAIN} -> %{http_code}\n" "https://${DOMAIN}"
curl -sS -o /dev/null -w "https://${DOMAIN}/main -> %{http_code}\n" "https://${DOMAIN}/main"

echo "== PM2 TAIL (last 20) =="
pm2 logs "$PM2_APP" --lines 20 --nostream
