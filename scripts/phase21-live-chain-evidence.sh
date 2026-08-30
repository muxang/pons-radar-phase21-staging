#!/usr/bin/env bash
set -euo pipefail

: "${RH_RPC_HTTP_URL:?RH_RPC_HTTP_URL is required}"
factory="${PONS_FACTORY_ADDRESS:-0x7eD598BcEf8bd9Edd8C97A195C6d13f40801EC7e}"
launch_topic="0x8d4aad4953d0ca700d468f3753aa14432d1b35b43ec6409f051fb6aa43a89607"
evidence_block="${PONS_EVIDENCE_BLOCK:-0x2d9eb94}"

rpc() {
  curl --fail --silent --show-error --max-time 20 -H 'content-type: application/json' --data "$1" "$RH_RPC_HTTP_URL"
}

chain="$(rpc '{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}')"
head="$(rpc '{"jsonrpc":"2.0","id":2,"method":"eth_blockNumber","params":[]}')"
code="$(rpc "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"eth_getCode\",\"params\":[\"$factory\",\"latest\"]}")"
logs="$(rpc "{\"jsonrpc\":\"2.0\",\"id\":4,\"method\":\"eth_getLogs\",\"params\":[{\"fromBlock\":\"$evidence_block\",\"toBlock\":\"$evidence_block\",\"address\":\"$factory\",\"topics\":[\"$launch_topic\"]}]}")"
recent_logs='{"result":[]}'
if [[ -n "${PONS_RECENT_FROM_BLOCK:-}" ]]; then
  recent_logs="$(rpc "{\"jsonrpc\":\"2.0\",\"id\":5,\"method\":\"eth_getLogs\",\"params\":[{\"fromBlock\":\"$PONS_RECENT_FROM_BLOCK\",\"toBlock\":\"latest\",\"address\":\"$factory\",\"topics\":[\"$launch_topic\"]}]}")"
fi

jq -n --arg checked_at "$(date -u +%FT%TZ)" \
  --arg chain_id "$(jq -er '.result' <<<"$chain")" \
  --arg head "$(jq -er '.result' <<<"$head")" \
  --arg evidence_block "$evidence_block" \
  --argjson code_bytes "$(( ($(jq -er '.result | length' <<<"$code") - 2) / 2 ))" \
  --argjson launch_count "$(jq -er '.result | length' <<<"$logs")" \
  --argjson transactions "$(jq -c '[.result[].transactionHash]' <<<"$logs")" \
  --argjson recent_launch_count "$(jq -c '.result | length' <<<"$recent_logs")" \
  --argjson recent_launches "$(jq -c '[.result[-5:][] | {blockNumber,transactionHash}]' <<<"$recent_logs")" \
  '{checked_at:$checked_at,chain_id:$chain_id,head:$head,factory_code_bytes:$code_bytes,evidence_block:$evidence_block,token_launched_count:$launch_count,transaction_hashes:$transactions,recent_launch_count:$recent_launch_count,recent_launches:$recent_launches}'
