\pset pager off
SELECT current_version,target_version,state,manifest_sha256,asset_sha256,
       signature_key_id,schema_compatible,rollback_safe,error,rollback_result,
       started_at,completed_at
FROM update_jobs ORDER BY started_at;

SELECT outcome,count(*) FROM release_history GROUP BY outcome ORDER BY outcome;

SELECT seq,event_type,payload->>'old_version' AS old_version,
       payload->>'new_version' AS new_version,payload->>'realtime_alert_eligible' AS realtime,
       created_at
FROM event_outbox
WHERE event_type LIKE 'system.update_%'
ORDER BY seq;

SELECT alert_type,severity,status,realtime_alert_eligible,provisional,
       event_effective_at,created_at
FROM alert_events
WHERE alert_type IN ('SYSTEM_UPDATE','SYSTEM_WARNING')
ORDER BY created_at;
