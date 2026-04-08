# Chrono Lab Deployment Guide (Cafe24)

## 1) 배포 구조
- GitHub: `Heptalabs/chrono-lab`
- App Server: Cafe24 OpenClaw VPS (Ubuntu)
- Process: PM2 (`chrono-lab`)
- Reverse Proxy: Nginx
- Domain: `chronolab.co.kr`, `www.chronolab.co.kr`
- SSL: Certbot (Let's Encrypt)

## 2) 서버 1회 초기 세팅
```bash
apt update
apt install -y git curl nginx ufw ca-certificates gnupg certbot python3-certbot-nginx
curl -fsSL https://deb.nodesource.com/setup_22.x | bash -
apt install -y nodejs
npm install -g pm2

ufw allow 22/tcp
ufw allow 80/tcp
ufw allow 443/tcp
ufw --force enable
```

## 3) 코드 배포
```bash
mkdir -p /var/www
cd /var/www
git clone https://github.com/Heptalabs/chrono-lab.git || true
cd /var/www/chrono-lab
git fetch origin
git checkout main
git pull origin main
npm install --omit=dev
```

`.env` 생성:
```bash
cat > /var/www/chrono-lab/.env <<'ENV'
NODE_ENV=production
PORT=3100
SESSION_SECRET=CHANGE_ME_LONG_RANDOM_SECRET
DB_PATH=./data/chronolab.db
SMTP_HOST=
SMTP_PORT=
SMTP_SECURE=false
SMTP_USER=
SMTP_PASS=
SMTP_FROM=
ENV

sed -i "s/CHANGE_ME_LONG_RANDOM_SECRET/$(openssl rand -hex 32)/" /var/www/chrono-lab/.env
mkdir -p /var/www/chrono-lab/data /var/www/chrono-lab/uploads
```

PM2 실행:
```bash
cd /var/www/chrono-lab
pm2 start server.js --name chrono-lab
pm2 save
pm2 startup
```

## 4) Nginx + 도메인 + HTTPS
Nginx 설정 파일:
```nginx
server {
    listen 80;
    server_name chronolab.co.kr www.chronolab.co.kr;
    return 301 https://chronolab.co.kr$request_uri;
}

server {
    listen 443 ssl http2;
    server_name www.chronolab.co.kr;
    return 301 https://chronolab.co.kr$request_uri;

    ssl_certificate /etc/letsencrypt/live/chronolab.co.kr/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/chronolab.co.kr/privkey.pem;
}

server {
    listen 443 ssl http2;
    server_name chronolab.co.kr;

    ssl_certificate /etc/letsencrypt/live/chronolab.co.kr/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/chronolab.co.kr/privkey.pem;

    client_max_body_size 30M;

    location = / {
        return 302 /main;
    }

    location / {
        proxy_pass http://127.0.0.1:3100;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

적용:
```bash
ln -sf /etc/nginx/sites-available/chronolab.conf /etc/nginx/sites-enabled/chronolab.conf
rm -f /etc/nginx/sites-enabled/default
nginx -t
systemctl reload nginx
certbot --nginx -d chronolab.co.kr -d www.chronolab.co.kr --agree-tos -m you@example.com --redirect
certbot renew --dry-run
```

## 5) 자동배포(푸시 후 최대 1분 반영)
```bash
cd /var/www/chrono-lab
./scripts/server/install-cron-autodeploy.sh
```

## 6) 일일 백업(03:40)
```bash
cd /var/www/chrono-lab
./scripts/server/install-cron-backup.sh
```

백업 수동 실행:
```bash
/usr/local/bin/chronolab-backup-nightly.sh
```

## 7) 운영 점검
```bash
cd /var/www/chrono-lab
./scripts/server/healthcheck.sh chronolab.co.kr
pm2 logs chrono-lab --lines 100
```

## 8) 원클릭 운영 스크립트 (권장)
최초 1회:
```bash
cd /var/www/chrono-lab
chmod +x ./scripts/server/*.sh
./scripts/server/install-ops.sh chronolab.co.kr
```

수동 즉시 배포:
```bash
cd /var/www/chrono-lab
./scripts/server/deploy-now.sh chronolab.co.kr
```

서비스 재시작 + 점검:
```bash
cd /var/www/chrono-lab
./scripts/server/restart-now.sh chronolab.co.kr
```

현재 상태 확인:
```bash
cd /var/www/chrono-lab
./scripts/server/show-status.sh chronolab.co.kr
```

정상 판정 기준:
- `pm2` 상태가 `online`
- `systemctl is-active nginx` 결과가 `active`
- `http://127.0.0.1:3100/main -> 200`
- `https://chronolab.co.kr/main -> 200`

비정상 예시:
- `pm2`가 `errored/stopped`
- Nginx 상태가 `inactive/failed`
- HTTP 코드가 `5xx` 또는 `000`

## 9) Render 종료 체크리스트
1. Render Dashboard -> 해당 서비스 선택
2. `Settings` -> `Delete Service`
3. (선택) Blueprint도 사용 안 하면 삭제
4. DNS에 Render를 가리키는 레코드가 남아있지 않은지 최종 확인
5. 최종 접속 확인: `https://chronolab.co.kr/main`

## 10) 운영 명령 (자주 사용)
```bash
cd /var/www/chrono-lab
git pull origin main
npm install --omit=dev
pm2 restart chrono-lab --update-env
pm2 save
```
