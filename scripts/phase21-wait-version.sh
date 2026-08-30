#!/usr/bin/env bash
set -euo pipefail

version="${PONS_EXPECTED_VERSION:?PONS_EXPECTED_VERSION is required}"
build="${PONS_EXPECTED_BUILD:?PONS_EXPECTED_BUILD is required}"
timeout_seconds="${PONS_VERSION_TIMEOUT_SECONDS:-120}"
deadline=$(( $(date +%s) + timeout_seconds ))
while (( $(date +%s) < deadline )); do
  identity="$(curl --silent --max-time 3 http://127.0.0.1:3000/api/v1/system/version || true)"
  if jq -e --arg version "$version" --arg build "$build" '.app_version == $version and .frontend_build_id == $build' <<<"$identity" >/dev/null 2>&1 \
     && curl --fail --silent --output /dev/null http://127.0.0.1:3000/readyz; then
    echo "$identity"
    echo 'PASS: expected version is ready'
    exit 0
  fi
  sleep 2
done
echo "FAIL: expected version did not become ready" >&2
exit 1
