#!/usr/bin/env bash
set -euo pipefail

base_url="${PONS_BASE_URL:?PONS_BASE_URL is required}"
origin="${PONS_ALLOWED_ORIGIN:?PONS_ALLOWED_ORIGIN is required}"
cookie_file="${PONS_ADMIN_COOKIE_FILE:-/root/pons-radar-admin.cookies}"
csrf="$(awk '$6 == "pons_csrf" {print $7}' "$cookie_file")"
[[ -n "$csrf" ]]

check="$(curl --fail --silent --show-error --cookie "$cookie_file" \
  -X POST -H "origin: $origin" -H "x-csrf-token: $csrf" -H 'content-type: application/json' \
  --data '{}' "$base_url/api/v1/admin/updates/check")"
jq -e '.available.manifest.app_version != null and .available.signature == "VALID" and .available.install_allowed == true' <<<"$check" >/dev/null
jq '{available_version:.available.manifest.app_version,signature:.available.signature,schema_compatible:.available.schema_compatible,rollback_compatible:.available.rollback_compatible,install_allowed:.available.install_allowed}' <<<"$check"

curl --fail --silent --show-error --cookie "$cookie_file" \
  -X POST -H "origin: $origin" -H "x-csrf-token: $csrf" -H 'content-type: application/json' \
  --data '{"confirm":true}' "$base_url/api/v1/admin/updates/install"
