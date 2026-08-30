#!/usr/bin/env bash
set -euo pipefail

: "${DATABASE_URL:?DATABASE_URL is required}"
target="${PONS_BROKEN_TARGET_VERSION:?PONS_BROKEN_TARGET_VERSION is required}"
timeout_seconds="${PONS_ROLLBACK_TIMEOUT_SECONDS:-180}"
deadline=$(( $(date +%s) + timeout_seconds ))
while (( $(date +%s) < deadline )); do
  state="$(psql "$DATABASE_URL" -X -At --set ON_ERROR_STOP=1 -c "SELECT state FROM update_jobs WHERE target_version='${target}' ORDER BY started_at DESC LIMIT 1")"
  case "$state" in
    ROLLED_BACK) echo 'PASS: broken release rolled back'; exit 0 ;;
    ROLLBACK_FAILED|FAILED) echo "FAIL: updater ended in $state" >&2; exit 1 ;;
  esac
  sleep 2
done
echo 'FAIL: rollback did not complete before timeout' >&2
exit 1
