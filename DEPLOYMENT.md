# Chrono Lab Deployment Guide

## 1) HeptaLabs GitHub에 먼저 올리기

### 1-1. GitHub에서 새 리포 생성
- 예시 이름: `chrono-lab`
- Visibility: `Public`
- README/.gitignore는 생성하지 않음(로컬 파일 사용)

### 1-2. 로컬에서 최초 푸시
```bash
cd chrono-lab
git init
git add .
git commit -m "init: chrono lab shopping mall + admin"
git branch -M main
git remote add origin git@github.com:Heptalabs/chrono-lab.git
git push -u origin main
```

## 2) 임시 도메인으로 먼저 확인 (권장: Render)

### 2-1. Render 배포
- Render에서 `New +` -> `Blueprint` 선택
- GitHub 리포 `Heptalabs/chrono-lab` 연결
- `render.yaml` 자동 인식 후 배포
- 임시 URL 예시: `https://chrono-lab.onrender.com`

### 2-2. 확인 경로
- `/main`
- `/notice`
- `/shop`
- `/qc`
- `/admin/login`

## 3) 나중에 다른 GitHub 계정으로 이전

### 방법 A (권장): GitHub Repository Transfer
- 기존 리포 Settings -> Transfer ownership
- 장점: 이슈/PR/히스토리/URL 리디렉션 유지

### 방법 B: 미러 푸시
```bash
cd chrono-lab
./scripts/mirror-to-new-account.sh git@github.com:NEW_OWNER/chrono-lab.git
```

## 4) 도메인 최종 연결
- 임시 도메인 검수 완료 후, 배포 플랫폼에서 Custom Domain 추가
- DNS (CNAME/A) 연결 후 SSL 자동 발급 확인

## 5) 운영 전 필수
- 어드민 초기 비밀번호 변경
- `SESSION_SECRET` 강한 랜덤값 사용
- `NODE_ENV=production` 유지
- `SHOW_DEFAULT_ADMIN_HINT=0` 유지 (초기 계정 힌트 숨김)
- 실제 계좌 정보/사업자 정보 반영
