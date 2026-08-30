#!/usr/bin/env bash
set -euo pipefail

# Operator-only staging validation. CI must never call this script.
: "${RH_RPC_HTTP_URL:?RH_RPC_HTTP_URL must be supplied through the environment}"
: "${RH_RPC_WS_URL:?RH_RPC_WS_URL must be supplied through the environment}"
: "${DATABASE_URL:?DATABASE_URL must be supplied through the environment}"

evidence_dir="${PHASE21_EVIDENCE_DIR:-./phase21-evidence/$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "$evidence_dir"
chmod 0700 "$evidence_dir"

rpc() {
  curl --fail --silent --show-error --max-time 15 \
    -H 'content-type: application/json' --data "$1" "$RH_RPC_HTTP_URL"
}

chain_json="$(rpc '{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}')"
chain_id="$(jq -er '.result' <<<"$chain_json")"
if [[ "$chain_id" != "0x1237" ]]; then
  echo "FAIL: expected Robinhood chain 4663/0x1237; received $chain_id" >&2
  exit 1
fi

head_json="$(rpc '{"jsonrpc":"2.0","id":2,"method":"eth_blockNumber","params":[]}')"
head_hex="$(jq -er '.result' <<<"$head_json")"
head_decimal="$((16#${head_hex#0x}))"
jq -n --arg checked_at "$(date -u +%FT%TZ)" --arg chain_id "$chain_id" \
  --argjson head "$head_decimal" \
  '{status:"PASS",provider_type:"operator-supplied Robinhood HTTP RPC",checked_at:$checked_at,chain_id:$chain_id,head:$head}' \
  >"$evidence_dir/http-rpc.json"

if ! command -v websocat >/dev/null 2>&1; then
  jq -n --arg checked_at "$(date -u +%FT%TZ)" \
    '{status:"BLOCKED",checked_at:$checked_at,reason:"websocat is not installed"}' \
    >"$evidence_dir/ws-rpc.json"
else
  ws_reply="$(printf '%s\n' '{"jsonrpc":"2.0","id":3,"method":"eth_chainId","params":[]}' | timeout 20s websocat -1 "$RH_RPC_WS_URL")"
  ws_chain="$(jq -er 'select(.id==3).result' <<<"$ws_reply")"
  [[ "$ws_chain" == "0x1237" ]] || { echo "FAIL: WSS wrong chain $ws_chain" >&2; exit 1; }
  jq -n --arg checked_at "$(date -u +%FT%TZ)" --arg chain_id "$ws_chain" \
    '{status:"PASS",provider_type:"operator-supplied Robinhood WSS RPC",checked_at:$checked_at,chain_id:$chain_id}' \
    >"$evidence_dir/ws-rpc.json"
fi

curl --fail --silent --show-error --max-time 10 http://127.0.0.1:3000/healthz \
  >"$evidence_dir/healthz.json"
curl --fail --silent --show-error --max-time 10 http://127.0.0.1:3000/readyz \
  >"$evidence_dir/readyz.json"
psql "$DATABASE_URL" -X --set ON_ERROR_STOP=1 --file scripts/phase21-validation-evidence.sql \
  >"$evidence_dir/database-evidence.txt"

printf 'PASS: evidence written to %s\n' "$evidence_dir"
