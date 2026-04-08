#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/var/www/chrono-lab}"
BACKUP_ROOT="${BACKUP_ROOT:-/var/backups/chronolab}"
KEEP_DAYS="${KEEP_DAYS:-14}"

STAMP="$(date +%Y%m%d-%H%M%S)"
DEST_DIR="$BACKUP_ROOT/$STAMP"
mkdir -p "$DEST_DIR"

cd "$APP_DIR"

[ -f .env ] && cp .env "$DEST_DIR/.env"
[ -f data/chronolab.db ] && cp data/chronolab.db "$DEST_DIR/chronolab.db"
[ -d uploads ] && tar -czf "$DEST_DIR/uploads.tar.gz" uploads

cat > "$DEST_DIR/README.txt" <<TXT
Chrono Lab backup
- created_at: $(date '+%F %T %Z')
- app_dir: $APP_DIR
- contains: .env, SQLite DB, uploads archive
TXT

find "$BACKUP_ROOT" -mindepth 1 -maxdepth 1 -type d -mtime +"$KEEP_DAYS" -exec rm -rf {} +

echo "[ok] backup created at: $DEST_DIR"
