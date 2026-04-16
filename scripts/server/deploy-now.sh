#!/usr/bin/env bash
set -euo pipefail

DEFAULT_APP_DIR="/var/www/chronolab"
LEGACY_APP_DIR="/var/www/chrono-lab"
if [ -n "${APP_DIR:-}" ]; then
  APP_DIR="${APP_DIR}"
elif [ -d "$DEFAULT_APP_DIR/.git" ]; then
  APP_DIR="$DEFAULT_APP_DIR"
elif [ -d "$LEGACY_APP_DIR/.git" ]; then
  APP_DIR="$LEGACY_APP_DIR"
else
  APP_DIR="$LEGACY_APP_DIR"
fi
BRANCH="${BRANCH:-main}"
PM2_APP="${PM2_APP:-}"
ENV_FILE="${ENV_FILE:-$APP_DIR/.env}"
EXPECTED_REPO_SLUG="${EXPECTED_REPO_SLUG:-chronolabstore/chronolab}"
EXPECTED_ORIGIN_URL="${EXPECTED_ORIGIN_URL:-git@github.com:${EXPECTED_REPO_SLUG}.git}"

resolve_pm2_app_name() {
  if [ -n "$PM2_APP" ]; then
    echo "$PM2_APP"
    return
  fi
  for candidate in chronolab chrono-lab; do
    if pm2 describe "$candidate" >/dev/null 2>&1; then
      echo "$candidate"
      return
    fi
  done
  echo "chronolab"
}

is_expected_origin_url() {
  local current_url="${1:-}"
  local normalized_current normalized_slug
  normalized_current="$(printf '%s' "$current_url" | tr '[:upper:]' '[:lower:]')"
  normalized_slug="$(printf '%s' "$EXPECTED_REPO_SLUG" | tr '[:upper:]' '[:lower:]')"
  [[ "$normalized_current" == *"$normalized_slug"* ]]
}

ensure_origin_remote() {
  local current_url
  current_url="$(git remote get-url origin 2>/dev/null || true)"
  if [ -n "$current_url" ] && is_expected_origin_url "$current_url"; then
    return
  fi

  if git remote get-url origin >/dev/null 2>&1; then
    git remote set-url origin "$EXPECTED_ORIGIN_URL"
  else
    git remote add origin "$EXPECTED_ORIGIN_URL"
  fi
}

sync_branch_to_origin() {
  if ! git show-ref --verify --quiet "refs/heads/$BRANCH"; then
    git checkout -q -B "$BRANCH" "origin/$BRANCH"
    return 0
  fi

  git checkout -q "$BRANCH"

  local local_sha remote_sha
  local_sha="$(git rev-parse "$BRANCH")"
  remote_sha="$(git rev-parse "origin/$BRANCH")"
  if [ "$local_sha" = "$remote_sha" ]; then
    return 1
  fi

  if git merge-base --is-ancestor "$BRANCH" "origin/$BRANCH"; then
    git reset --hard -q "origin/$BRANCH"
    return 0
  fi

  local backup_branch
  backup_branch="backup/${BRANCH}-$(date '+%Y%m%d-%H%M%S')"
  git branch "$backup_branch" "$BRANCH" >/dev/null 2>&1 || true
  git reset --hard -q "origin/$BRANCH"
  return 0
}

PM2_APP="$(resolve_pm2_app_name)"

cd "$APP_DIR"

echo "[1/6] fetch latest ($BRANCH)"
ensure_origin_remote
git fetch origin "$BRANCH"

echo "[2/6] checkout $BRANCH"
if sync_branch_to_origin; then
  echo "[2/6] synced local branch to origin/$BRANCH"
else
  echo "[2/6] already up to date with origin/$BRANCH"
fi

echo "[3/6] pull latest"
echo "[3/6] current commit $(git rev-parse --short HEAD)"

echo "[4/6] install production dependencies"
npm install --omit=dev

echo "[5/6] reload app"
if pm2 describe "$PM2_APP" >/dev/null 2>&1; then
  pm2 reload "$PM2_APP" --update-env || pm2 restart "$PM2_APP" --update-env
else
  if [ -f "$ENV_FILE" ]; then
    pm2 start "$APP_DIR/server.js" --name "$PM2_APP" --node-args="--env-file=$ENV_FILE"
  else
    pm2 start "$APP_DIR/server.js" --name "$PM2_APP"
  fi
fi
pm2 save

echo "[6/6] healthcheck"
"$APP_DIR/scripts/server/healthcheck.sh" "${1:-chronolab.co.kr}"

echo "[ok] deploy completed"
