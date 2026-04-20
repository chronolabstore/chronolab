# Chrono Lab Deployment Guide (Cafe24)

## 1) 배포 구조
- GitHub: `chronolabstore/chronolab`
- App Server: Cafe24 OpenClaw VPS (Ubuntu)
- Process: PM2 (`chronolab`)
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
git clone https://github.com/chronolabstore/chronolab.git || true
cd /var/www/chronolab
git fetch origin
git checkout main
git pull origin main
npm install --omit=dev
```

`.env` 생성:
```bash
cat > /var/www/chronolab/.env <<'ENV'
NODE_ENV=production
PORT=3100
SESSION_SECRET=CHANGE_ME_LONG_RANDOM_SECRET
DB_PATH=/var/lib/chronolab/chronolab.db
UPLOAD_DIR=/var/lib/chronolab/uploads
ENABLE_BOOTSTRAP_SEED=0
ENABLE_STARTUP_DATA_MAINTENANCE=0
SMTP_HOST=
SMTP_PORT=
SMTP_SECURE=false
SMTP_USER=
SMTP_PASS=
SMTP_FROM=
ADMIN_OTP_ISSUER=Chrono LAB
ADMIN_OTP_ENFORCED=1
ADMIN_ENTRY_PATH=/admin-your-random-key
ADMIN_WAF_ENABLED=1
ADMIN_WAF_BOT_BLOCK_ENABLED=1
ADMIN_WAF_GEO_BLOCK_ENABLED=1
ADMIN_WAF_ALLOWED_COUNTRY_CODES=KR
ADMIN_WAF_ASN_BLOCK_ENABLED=0
ADMIN_WAF_BLOCKED_ASNS=
ADMIN_WAF_IP_ALLOWLIST=
ADMIN_WAF_IP_ALLOWLIST_ENFORCED=0
SECURITY_ALERT_NOTIFY_ENABLED=1
SECURITY_ALERT_NOTIFY_WEBHOOK_URL=
SECURITY_ALERT_NOTIFY_EMAIL_TO=
SECURITY_ALERT_NOTIFY_TELEGRAM_ENABLED=0
SECURITY_ALERT_NOTIFY_TELEGRAM_BOT_TOKEN=
SECURITY_ALERT_NOTIFY_TELEGRAM_CHAT_IDS=
SECURITY_ALERT_NOTIFY_TELEGRAM_THREAD_ID=
SECURITY_ALERT_NOTIFY_TELEGRAM_SILENT=0
SECURITY_ALERT_NOTIFY_THROTTLE_MS=60000
SECURITY_ALERT_NOTIFY_NOISY_THROTTLE_MS=600000
SECURITY_ALERT_NOTIFY_INCLUDE_RAW_CODE=0
ENV

sed -i "s/CHANGE_ME_LONG_RANDOM_SECRET/$(openssl rand -hex 32)/" /var/www/chronolab/.env
mkdir -p /var/lib/chronolab/uploads
```

PM2 실행:
```bash
cd /var/www/chronolab
pm2 start server.js --name chronolab --node-args="--env-file=/var/www/chronolab/.env"
pm2 save
pm2 startup
```

기존 데이터/업로드를 영구 경로로 자동 이전 + 환경값 고정:
```bash
cd /var/www/chronolab
./scripts/server/configure-persistent-storage.sh
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
cd /var/www/chronolab
./scripts/server/install-cron-autodeploy.sh
```

## 6) 일일 백업(03:40)
```bash
cd /var/www/chronolab
./scripts/server/install-cron-backup.sh
```

백업 수동 실행:
```bash
/usr/local/bin/chronolab-backup-nightly.sh
```

## 7) 운영 점검
```bash
cd /var/www/chronolab
./scripts/server/healthcheck.sh chronolab.co.kr
pm2 logs chronolab --lines 100
```

## 8) 원클릭 운영 스크립트 (권장)
최초 1회:
```bash
cd /var/www/chronolab
chmod +x ./scripts/server/*.sh
./scripts/server/install-ops.sh chronolab.co.kr --with-security
```
`install-ops.sh`는 자동배포 cron + 백업 cron + 로그 로테이션(`logrotate`) + SSH 고정(22/key-only) + fail2ban까지 함께 설정합니다.
또한 DB/업로드 경로를 `/var/lib/chronolab`으로 고정하고 PM2를 `.env` 기준으로 재기동합니다.

보안 설정을 건너뛰고 운영 스크립트만 적용하려면:
```bash
./scripts/server/install-ops.sh chronolab.co.kr
```

수동 즉시 배포:
```bash
cd /var/www/chronolab
./scripts/server/deploy-now.sh chronolab.co.kr
```

서비스 재시작 + 점검:
```bash
cd /var/www/chronolab
./scripts/server/restart-now.sh chronolab.co.kr
```

현재 상태 확인:
```bash
cd /var/www/chronolab
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

## 8-1) Cafe24 방화벽 고정 권장값
- `INBOUND (모든 IP)`:
  - `80/tcp ALLOW`
  - `443/tcp ALLOW`
- `INBOUND (특정 IP)`:
  - 사용하지 않음 (이동 작업이 많을 때는 비권장)
- SSH:
  - 서버는 `22`만 사용
  - `59398`은 사용하지 않음

이동 환경(카페/테더링 등)에서 작업 시에는 Cafe24 방화벽에서 SSH를 특정 IP로 묶지 말고, 서버 내부는 `key-only + fail2ban`으로 방어하는 방식을 권장합니다.

## 8-2) 어드민 접근 보안 권장값
- 관리자 경로 랜덤화 권장:
  - `ADMIN_ENTRY_PATH=/admin-your-random-key` 처럼 추측 어려운 경로를 설정
  - 설정 후 기존 `/admin` 직접 접근은 `404`로 차단됨
- 관리자 OTP 강제 권장:
  - `ADMIN_OTP_ENFORCED=1` (OTP 미설정 관리자 로그인 차단)
- 관리자 IP 고정(강제) 권장:
  - `ADMIN_WAF_IP_ALLOWLIST=<VPN_고정_IP>` (쉼표로 다중 IP 가능)
  - `ADMIN_WAF_IP_ALLOWLIST_ENFORCED=1` 설정 시, 허용 목록 외 관리자 접근은 즉시 차단
- 운영 중 잠금 방지:
  - 먼저 `ADMIN_WAF_IP_ALLOWLIST`에 현재 운영자 IP를 넣고,
  - 접속 확인 후 `ADMIN_WAF_IP_ALLOWLIST_ENFORCED=1` 활성화 권장
- `/admin` 경로(내부 canonical)는 서버에서 Bot 시그니처/Geo/ASN 정책을 함께 검사합니다.
- 기본 권장:
  - `ADMIN_WAF_ENABLED=1`
  - `ADMIN_WAF_BOT_BLOCK_ENABLED=1`
  - `ADMIN_WAF_GEO_BLOCK_ENABLED=1`
  - `ADMIN_WAF_ALLOWED_COUNTRY_CODES=KR`
- 운영자가 해외에서 접속해야 하면, 임시로 `ADMIN_WAF_ALLOWED_COUNTRY_CODES`에 국가코드를 추가하거나 `ADMIN_WAF_IP_ALLOWLIST`에 본인 공인IP를 등록합니다.

## 8-3) 실시간 보안 알림 권장값
- 보안 이벤트(`로그인 실패/권한 차단/WAF 차단`)는 DB에 기록됩니다.
- 실시간 수신이 필요하면 아래 중 하나 이상 설정:
  - `SECURITY_ALERT_NOTIFY_WEBHOOK_URL` (Slack/Discord/사내 Webhook)
  - `SECURITY_ALERT_NOTIFY_EMAIL_TO` (쉼표 구분 이메일)
  - `SECURITY_ALERT_NOTIFY_TELEGRAM_ENABLED=1` + Telegram Bot 설정
- 알림 폭주 완화/표시 포맷:
  - `SECURITY_ALERT_NOTIFY_THROTTLE_MS` (기본 60초)
  - `SECURITY_ALERT_NOTIFY_NOISY_THROTTLE_MS` (기본 10분, 봇/숨김경로 차단 이벤트)
  - `SECURITY_ALERT_NOTIFY_INCLUDE_RAW_CODE=0` (기본: 내부 영문 코드 비노출)

Telegram 설정(권장):
1. 텔레그램 `@BotFather`에서 봇 생성 후 Bot Token 발급
2. 알림 받을 개인/그룹/채널에 봇 추가
3. 봇에게 대상 채팅에서 메시지 1개 이상 전송
4. 아래 호출로 `chat.id` 확인:
```bash
curl -s "https://api.telegram.org/bot<YOUR_BOT_TOKEN>/getUpdates"
```
5. `.env` 반영:
```bash
SECURITY_ALERT_NOTIFY_TELEGRAM_ENABLED=1
SECURITY_ALERT_NOTIFY_TELEGRAM_BOT_TOKEN=<YOUR_BOT_TOKEN>
SECURITY_ALERT_NOTIFY_TELEGRAM_CHAT_IDS=<CHAT_ID>
```
- 여러 곳으로 받으려면 `SECURITY_ALERT_NOTIFY_TELEGRAM_CHAT_IDS`에 쉼표로 다중 입력
- 포럼 토픽(스레드)으로 받을 때만 `SECURITY_ALERT_NOTIFY_TELEGRAM_THREAD_ID` 추가

## 9) Render 종료 체크리스트
1. Render Dashboard -> 해당 서비스 선택
2. `Settings` -> `Delete Service`
3. (선택) Blueprint도 사용 안 하면 삭제
4. DNS에 Render를 가리키는 레코드가 남아있지 않은지 최종 확인
5. 최종 접속 확인: `https://chronolab.co.kr/main`

## 10) 운영 명령 (자주 사용)
```bash
cd /var/www/chronolab
git pull origin main
npm install --omit=dev
pm2 restart chronolab --update-env
pm2 save
```
