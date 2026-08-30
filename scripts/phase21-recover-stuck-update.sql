UPDATE update_jobs
SET state = 'FAILED',
    error = 'Phase 21 detected helper cgroup termination; superseded by fixed release',
    completed_at = now(),
    updated_at = now()
WHERE state IN ('INSTALLING', 'RESTARTING', 'VERIFYING_HEALTH');
