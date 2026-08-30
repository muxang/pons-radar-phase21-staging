#!/usr/bin/env bash
set -euo pipefail

: "${DATABASE_URL:?DATABASE_URL is required}"
service="${PONS_SERVICE_NAME:-pons-radar.service}"
snapshot() {
  psql "$DATABASE_URL" -X -At -F, --set ON_ERROR_STOP=1 -c \
    "SELECT count(*),(SELECT count(*) FROM token_trades),(SELECT count(*) FROM smart_trades),COALESCE((SELECT max(seq) FROM event_outbox),0),COALESCE((SELECT max(last_processed_block) FROM chain_cursors),0) FROM raw_chain_logs"
}

before="$(snapshot)"
sudo systemctl restart "$service"
for _ in {1..30}; do
  if curl --fail --silent --output /dev/null http://127.0.0.1:3000/readyz; then break; fi
  sleep 1
done
curl --fail --silent --output /dev/null http://127.0.0.1:3000/readyz
sleep 5
after="$(snapshot)"
duplicates="$(psql "$DATABASE_URL" -X -At --set ON_ERROR_STOP=1 -c "SELECT (SELECT count(*)-count(DISTINCT(chain_id,tx_hash,log_index,event_type)) FROM token_trades)+(SELECT count(*)-count(DISTINCT token_trade_id) FROM smart_trades)")"
jq -n --arg checked_at "$(date -u +%FT%TZ)" --arg before "$before" --arg after "$after" --argjson duplicates "$duplicates" \
  '{status:(if $duplicates == 0 then "PASS" else "FAIL" end),checked_at:$checked_at,before:$before,after:$after,duplicate_semantic_rows:$duplicates}'
[[ "$duplicates" == "0" ]]
