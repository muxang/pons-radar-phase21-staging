\pset pager off
\timing on

SELECT now() AT TIME ZONE 'UTC' AS evidence_generated_utc,
       max(version) AS latest_migration
FROM _sqlx_migrations WHERE success;

SELECT id,chain_id,'0x'||encode(address,'hex') AS address,start_block,end_block,
       enabled,health AS verification_state,trust_basis,last_verified_at,verification_error
FROM protocol_deployments
WHERE protocol='PONS' AND generation='V2'
ORDER BY created_at;

SELECT stream,last_processed_block::text,last_processed_hash,updated_at
FROM chain_cursors ORDER BY stream;

SELECT count(*) AS raw_log_count,max(block_number)::text AS latest_raw_block,
       max(observed_at) AS latest_observed_at
FROM raw_chain_logs;
SELECT event_type,parser_version,schema_version,count(*) AS event_count,max(created_at) AS latest
FROM normalized_events GROUP BY event_type,parser_version,schema_version ORDER BY event_type;

SELECT t.id,'0x'||encode(t.address,'hex') AS token,'0x'||encode(t.curve_address,'hex') AS curve,
       t.launch_block::text,t.launch_time,t.lifecycle,m.status AS metadata_status,m.attempts,m.last_error
FROM tokens t LEFT JOIN token_metadata_jobs m ON m.token_id=t.id
ORDER BY t.launch_time DESC LIMIT 20;

SELECT side,count(*) AS trade_count,max(block_time) AS latest_trade,
       count(*) FILTER(WHERE status='ORPHANED') AS orphaned
FROM token_trades GROUP BY side ORDER BY side;
SELECT s.confirmation_level,t.status AS chain_status,s.classification_source,count(*) AS smart_trade_count
FROM smart_trades s JOIN token_trades t ON t.id=s.token_trade_id
GROUP BY s.confirmation_level,t.status,s.classification_source
ORDER BY confirmation_level,chain_status,classification_source;

SELECT 'token_trades' AS entity,count(*)-count(DISTINCT(chain_id,tx_hash,log_index,event_type)) AS duplicate_semantic_rows
FROM token_trades
UNION ALL
SELECT 'smart_trades',count(*)-count(DISTINCT token_trade_id) FROM smart_trades;

SELECT 'metadata' AS worker,status,count(*) AS jobs,max(last_error) AS last_error FROM token_metadata_jobs GROUP BY status
UNION ALL SELECT 'confirmation',status,count(*),max(last_error) FROM trade_confirmation_jobs GROUP BY status
UNION ALL SELECT 'position',status,count(*),max(last_error) FROM position_rebuild_jobs GROUP BY status
UNION ALL SELECT 'market',status,count(*),max(last_error) FROM market_rebuild_jobs GROUP BY status
UNION ALL SELECT 'signal',status,count(*),max(last_error) FROM signal_rebuild_jobs GROUP BY status
UNION ALL SELECT 'ai',status,count(*),max(last_error) FROM ai_research_jobs GROUP BY status
ORDER BY worker,status;

SELECT max(seq) AS outbox_high_watermark,count(*) AS durable_events,
       count(*) FILTER(WHERE published_at IS NULL) AS operationally_unpublished
FROM event_outbox;
SELECT alert_type,severity,realtime_alert_eligible,provisional,chain_finality,count(*)
FROM alert_events GROUP BY alert_type,severity,realtime_alert_eligible,provisional,chain_finality
ORDER BY alert_type;
