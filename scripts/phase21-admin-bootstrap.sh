#!/usr/bin/env bash
set -euo pipefail

# Staging-only bootstrap. Credentials remain in root-owned files on the host.
base_url="${PONS_BASE_URL:?PONS_BASE_URL is required}"
origin="${PONS_ALLOWED_ORIGIN:?PONS_ALLOWED_ORIGIN is required}"
environment_file="${PONS_ENVIRONMENT_FILE:-/etc/pons-radar/environment}"
credential_file="${PONS_ADMIN_CREDENTIAL_FILE:-/root/pons-radar-admin-initial}"
cookie_file="${PONS_ADMIN_COOKIE_FILE:-/root/pons-radar-admin.cookies}"

set -a
# shellcheck disable=SC1090
source "$environment_file"
# shellcheck disable=SC1090
source "$credential_file"
set +a

setup_required="$(curl --fail --silent --show-error "$base_url/api/v1/auth/setup-status" | jq -er '.setup_required')"
if [[ "$setup_required" == "true" ]]; then
  payload="$(jq -nc --arg username "$username" --arg password "$password" '{username:$username,password:$password}')"
  curl --fail --silent --show-error --output /dev/null \
    -H "origin: $origin" -H "x-setup-token: $ADMIN_SETUP_TOKEN" -H 'content-type: application/json' \
    --data "$payload" "$base_url/api/v1/auth/setup"
fi

payload="$(jq -nc --arg username "$username" --arg password "$password" '{username:$username,password:$password}')"
curl --fail --silent --show-error --output /dev/null --cookie-jar "$cookie_file" \
  -H "origin: $origin" -H 'content-type: application/json' \
  --data "$payload" "$base_url/api/v1/auth/login"
chmod 0600 "$cookie_file"
grep -q $'\tpons_session\t' "$cookie_file"
grep -q $'\tpons_csrf\t' "$cookie_file"
echo 'PASS: first-run admin is initialized and an authenticated staging session was created'
