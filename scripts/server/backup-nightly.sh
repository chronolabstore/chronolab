#!/usr/bin/env bash
set -euo pipefail

DEFAULT_APP_DIR="/var/www/chronolab"
LEGACY_APP_DIR="/var/www/chrono-lab"
if [ -n "${APP_DIR:-}" ]; then
  APP_DIR="${APP_DIR}"
elif [ -d "$DEFAULT_APP_DIR" ]; then
  APP_DIR="$DEFAULT_APP_DIR"
elif [ -d "$LEGACY_APP_DIR" ]; then
  APP_DIR="$LEGACY_APP_DIR"
else
  APP_DIR="$DEFAULT_APP_DIR"
fi
BACKUP_ROOT="${BACKUP_ROOT:-/var/backups/chronolab}"
KEEP_DAYS="${KEEP_DAYS:-14}"
ENV_FILE="${ENV_FILE:-$APP_DIR/.env}"

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

STAMP="$(date +%Y%m%d-%H%M%S)"
DEST_DIR="$BACKUP_ROOT/$STAMP"
mkdir -p "$DEST_DIR"

cd "$APP_DIR"

if [ -f "$ENV_FILE" ]; then
  set -a
  # shellcheck disable=SC1090
  . "$ENV_FILE"
  set +a
  cp "$ENV_FILE" "$DEST_DIR/.env"
fi

DB_SOURCE="$(resolve_path "${DB_PATH:-data/chronolab.db}")"
UPLOAD_SOURCE="$(resolve_path "${UPLOAD_DIR:-uploads}")"

[ -f "$DB_SOURCE" ] && cp "$DB_SOURCE" "$DEST_DIR/chronolab.db"
[ -d "$UPLOAD_SOURCE" ] && tar -czf "$DEST_DIR/uploads.tar.gz" -C "$UPLOAD_SOURCE" .

cat > "$DEST_DIR/README.txt" <<TXT
Chrono Lab backup
- created_at: $(date '+%F %T %Z')
- app_dir: $APP_DIR
- db_source: $DB_SOURCE
- upload_source: $UPLOAD_SOURCE
- contains: .env, SQLite DB, uploads archive
TXT

find "$BACKUP_ROOT" -mindepth 1 -maxdepth 1 -type d -mtime +"$KEEP_DAYS" -exec rm -rf {} +

echo "[ok] backup created at: $DEST_DIR"
