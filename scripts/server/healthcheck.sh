#!/usr/bin/env bash
set -euo pipefail

DOMAIN="${1:-chronolab.co.kr}"

echo "== PM2 =="
pm2 status | sed -n '1,20p'

echo "== NGINX =="
nginx -t
systemctl is-active nginx

echo "== APP =="
curl -I "http://127.0.0.1:3100/main" | head -n 5

echo "== DOMAIN =="
curl -I "https://${DOMAIN}" | head -n 8
curl -I "https://${DOMAIN}/main" | head -n 8
