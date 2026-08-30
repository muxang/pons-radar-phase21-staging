#!/usr/bin/env bash
set -euo pipefail

: "${DATABASE_URL:?DATABASE_URL must be supplied through the environment}"
duration_seconds="${PHASE21_SOAK_SECONDS:-86400}"
interval_seconds="${PHASE21_SOAK_INTERVAL_SECONDS:-60}"
output="${PHASE21_SOAK_OUTPUT:-./phase21-soak-$(date -u +%Y%m%dT%H%M%SZ).csv}"

printf '%s\n' 'observed_at,health,ready,main_pid,memory_bytes,cpu_nsec,db_connections,max_cursor_block,outbox_seq,metadata_backlog,confirmation_backlog,position_backlog,market_backlog,signal_backlog,ai_backlog' >"$output"
deadline=$(( $(date +%s) + duration_seconds ))
while (( $(date +%s) < deadline )); do
  observed_at="$(date -u +%FT%TZ)"
  health="$(curl --silent --output /dev/null --write-out '%{http_code}' --max-time 5 http://127.0.0.1:3000/healthz || true)"
  ready="$(curl --silent --output /dev/null --write-out '%{http_code}' --max-time 5 http://127.0.0.1:3000/readyz || true)"
  main_pid="$(systemctl show pons-radar.service --property MainPID --value)"
  memory="$(systemctl show pons-radar.service --property MemoryCurrent --value)"
  cpu="$(systemctl show pons-radar.service --property CPUUsageNSec --value)"
  metrics="$(psql "$DATABASE_URL" -X -At --set ON_ERROR_STOP=1 -F, -c "SELECT (SELECT count(*) FROM pg_stat_activity WHERE datname=current_database()),COALESCE((SELECT max(last_processed_block) FROM chain_cursors),0),COALESCE((SELECT max(seq) FROM event_outbox),0),(SELECT count(*) FROM token_metadata_jobs WHERE status<>'SUCCEEDED'),(SELECT count(*) FROM trade_confirmation_jobs WHERE status NOT IN('CONFIRMED','REJECTED')),(SELECT count(*) FROM position_rebuild_jobs WHERE status<>'COMPLETED'),(SELECT count(*) FROM market_rebuild_jobs WHERE status<>'COMPLETED'),(SELECT count(*) FROM signal_rebuild_jobs WHERE status<>'COMPLETED'),(SELECT count(*) FROM ai_research_jobs WHERE status NOT IN('SUCCEEDED','FAILED'))")"
  printf '%s,%s,%s,%s,%s,%s,%s\n' "$observed_at" "$health" "$ready" "$main_pid" "$memory" "$cpu" "$metrics" >>"$output"
  sleep "$interval_seconds"
done
printf 'Soak samples written to %s\n' "$output"
