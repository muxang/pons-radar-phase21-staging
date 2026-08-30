#!/usr/bin/env bash
set -euo pipefail

base_url="${PONS_BASE_URL:?PONS_BASE_URL is required}"
origin="${PONS_ALLOWED_ORIGIN:?PONS_ALLOWED_ORIGIN is required}"
start_block="${PONS_DEPLOYMENT_START_BLOCK:?PONS_DEPLOYMENT_START_BLOCK is required}"
factory="${PONS_FACTORY_ADDRESS:-0x7eD598BcEf8bd9Edd8C97A195C6d13f40801EC7e}"
cookie_file="${PONS_ADMIN_COOKIE_FILE:-/root/pons-radar-admin.cookies}"
csrf="$(awk '$6 == "pons_csrf" {print $7}' "$cookie_file")"
[[ -n "$csrf" ]]

request() {
  curl --fail --silent --show-error --cookie "$cookie_file" -H "origin: $origin" "$@"
}

deployments="$(request "$base_url/api/v1/admin/deployments")"
id="$(jq -er --arg address "${factory,,}" '.[] | select((.address | ascii_downcase) == $address) | .id' <<<"$deployments" | head -1 || true)"
if [[ -z "$id" ]]; then
  payload="$(jq -nc --arg address "$factory" --argjson start_block "$start_block" '{chain_id:4663,address:$address,start_block:$start_block,enabled:false,expected_event_topics:[],source:"Phase 21 operator-verified on-chain staging evidence"}')"
  created="$(request -H "x-csrf-token: $csrf" -H 'content-type: application/json' --data "$payload" "$base_url/api/v1/admin/deployments")"
  id="$(jq -er '.id' <<<"$created")"
else
  current_start="$(jq -er --arg id "$id" '.[] | select(.id == $id) | .start_block' <<<"$deployments")"
  if [[ "$current_start" != "$start_block" ]]; then
    payload="$(jq -nc --argjson start_block "$start_block" '{enabled:false,start_block:$start_block}')"
    request -X PATCH -H "x-csrf-token: $csrf" -H 'content-type: application/json' --data "$payload" "$base_url/api/v1/admin/deployments/$id" >/dev/null
  fi
fi

verified="$(request -X POST -H "x-csrf-token: $csrf" -H 'content-type: application/json' --data '{}' "$base_url/api/v1/admin/deployments/$id/verify")"
jq -e '.health == "VERIFIED" and .trust_basis == "OPERATOR_APPROVED"' <<<"$verified" >/dev/null
enabled="$(request -X PATCH -H "x-csrf-token: $csrf" -H 'content-type: application/json' --data '{"enabled":true}' "$base_url/api/v1/admin/deployments/$id")"
jq -e '.enabled == true and .health == "VERIFIED"' <<<"$enabled" >/dev/null
jq '{id,address,start_block,enabled,health,trust_basis,last_verified_at,verification_evidence}' <<<"$enabled"
