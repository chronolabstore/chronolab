#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/var/www/chronolab}"
DATA_ROOT="${DATA_ROOT:-/var/lib/chronolab}"
ENV_FILE="${ENV_FILE:-$APP_DIR/.env}"
PM2_APP="${PM2_APP:-chronolab}"

if [ ! -d "$APP_DIR" ]; then
  echo "[error] APP_DIR not found: $APP_DIR" >&2
  exit 1
fi

if ! command -v pm2 >/dev/null 2>&1; then
  echo "[error] pm2 command not found. Install pm2 first." >&2
  exit 1
fi

resolve_path() {
  local raw="$1"
  if [ -z "$raw" ]; then
    return 1
  fi
  if [[ "$raw" = /* ]]; then
    printf '%s\n' "$raw"
    return 0
  fi
  printf '%s/%s\n' "$APP_DIR" "$raw"
}

read_env_value() {
  local key="$1"
  if [ ! -f "$ENV_FILE" ]; then
    return 0
  fi
  grep -E "^${key}=" "$ENV_FILE" | tail -n 1 | cut -d '=' -f 2- || true
}

upsert_env() {
  local key="$1"
  local value="$2"
  if grep -qE "^${key}=" "$ENV_FILE"; then
    sed -i "s|^${key}=.*|${key}=${value}|" "$ENV_FILE"
    return 0
  fi
  printf '%s=%s\n' "$key" "$value" >> "$ENV_FILE"
}

mkdir -p "$DATA_ROOT" "$DATA_ROOT/uploads"
touch "$ENV_FILE"

current_db_raw="$(read_env_value DB_PATH)"
current_upload_raw="$(read_env_value UPLOAD_DIR)"
current_db_path="$(resolve_path "${current_db_raw:-data/chronolab.db}")"
current_upload_dir="$(resolve_path "${current_upload_raw:-uploads}")"
target_db_path="$DATA_ROOT/chronolab.db"
target_upload_dir="$DATA_ROOT/uploads"

if [ "$current_db_path" != "$target_db_path" ] && [ -f "$current_db_path" ] && [ ! -f "$target_db_path" ]; then
  cp -a "$current_db_path" "$target_db_path"
  echo "[migrate] copied DB: $current_db_path -> $target_db_path"
fi

if [ "$current_upload_dir" != "$target_upload_dir" ] && [ -d "$current_upload_dir" ]; then
  if [ -z "$(find "$target_upload_dir" -mindepth 1 -maxdepth 1 2>/dev/null)" ]; then
    if command -v rsync >/dev/null 2>&1; then
      rsync -a "$current_upload_dir"/ "$target_upload_dir"/
    else
      cp -a "$current_upload_dir"/. "$target_upload_dir"/
    fi
    echo "[migrate] copied uploads: $current_upload_dir -> $target_upload_dir"
  fi
fi

upsert_env NODE_ENV production
upsert_env DB_PATH "$target_db_path"
upsert_env UPLOAD_DIR "$target_upload_dir"
upsert_env ENABLE_BOOTSTRAP_SEED 0

if [ -z "$(read_env_value SESSION_SECRET)" ]; then
  upsert_env SESSION_SECRET "$(openssl rand -hex 32)"
fi

cd "$APP_DIR"
if pm2 describe "$PM2_APP" >/dev/null 2>&1; then
  pm2 delete "$PM2_APP"
fi
pm2 start "$APP_DIR/server.js" --name "$PM2_APP" --node-args="--env-file=$ENV_FILE"
pm2 save

echo "[ok] persistence configured"
echo "DB_PATH=$target_db_path"
echo "UPLOAD_DIR=$target_upload_dir"
