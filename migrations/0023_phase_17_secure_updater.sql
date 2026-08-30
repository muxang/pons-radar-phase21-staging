CREATE TABLE update_jobs (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
 state text NOT NULL CHECK(state IN('AVAILABLE','DOWNLOADING','VERIFYING','STAGED','INSTALLING','RESTARTING','VERIFYING_HEALTH','SUCCEEDED','FAILED','ROLLED_BACK','ROLLBACK_FAILED')),
 current_version text NOT NULL,
 target_version text NOT NULL,
 release_id bigint NOT NULL,
 release_tag text NOT NULL,
 channel text NOT NULL,
 manifest jsonb NOT NULL,
 manifest_sha256 text NOT NULL CHECK(manifest_sha256 ~ '^[0-9a-f]{64}$'),
 asset_filename text,
 asset_sha256 text CHECK(asset_sha256 IS NULL OR asset_sha256 ~ '^[0-9a-f]{64}$'),
 signature_key_id text NOT NULL,
 schema_compatible boolean NOT NULL,
 rollback_safe boolean NOT NULL,
 admin_user_id uuid REFERENCES users(id),
 staging_path text,
 backup_path text,
 error text,
 rollback_result text,
 started_at timestamptz NOT NULL DEFAULT now(),
 completed_at timestamptz,
 updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX one_active_update_install ON update_jobs((true))
 WHERE state IN('DOWNLOADING','VERIFYING','STAGED','INSTALLING','RESTARTING','VERIFYING_HEALTH');
CREATE INDEX update_jobs_history ON update_jobs(started_at DESC,id DESC);

CREATE TABLE release_history (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
 update_job_id uuid UNIQUE NOT NULL REFERENCES update_jobs(id),
 old_version text NOT NULL,
 new_version text NOT NULL,
 release_tag text NOT NULL,
 manifest_sha256 text NOT NULL CHECK(manifest_sha256 ~ '^[0-9a-f]{64}$'),
 frontend_build_id text NOT NULL,
 api_schema_version integer NOT NULL,
 outcome text NOT NULL CHECK(outcome IN('SUCCEEDED','FAILED','ROLLED_BACK','ROLLBACK_FAILED')),
 rollback_result text,
 completed_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE updater_state (
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 health text NOT NULL DEFAULT 'UNKNOWN' CHECK(health IN('UNKNOWN','HEALTHY','DEGRADED')),
 last_checked_at timestamptz,
 last_successful_check_at timestamptz,
 last_error text,
 latest_release jsonb,
 updated_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO updater_state(singleton) VALUES(true);

COMMENT ON TABLE update_jobs IS 'Durable updater state. The partial unique index is the database-wide install lock.';
COMMENT ON COLUMN update_jobs.rollback_safe IS 'Signed release policy: binary rollback is permitted only when the previous binary remains compatible with target_db_schema.';
