#!/usr/bin/env bash
set -euo pipefail

TARGET_REMOTE="${2:-origin}"
REPO_SSH="${1:-git@github.com:chronolabstore/chronolab.git}"

if [[ ! -d .git ]]; then
  echo "This script must be run at the git repository root."
  exit 1
fi

if git remote get-url "$TARGET_REMOTE" >/dev/null 2>&1; then
  git remote set-url "$TARGET_REMOTE" "$REPO_SSH"
else
  git remote add "$TARGET_REMOTE" "$REPO_SSH"
fi

git branch -M main
git push -u "$TARGET_REMOTE" main

echo "Pushed to remote '$TARGET_REMOTE': $REPO_SSH"
