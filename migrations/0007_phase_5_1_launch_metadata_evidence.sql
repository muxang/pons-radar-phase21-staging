ALTER TABLE token_metadata_jobs
    ADD COLUMN requested_block uint64_numeric,
    ADD COLUMN historical_attempts INTEGER NOT NULL DEFAULT 0 CHECK (historical_attempts >= 0);

UPDATE token_metadata_jobs j
SET requested_block = t.launch_block
FROM tokens t
WHERE t.id = j.token_id AND j.requested_block IS NULL;

ALTER TABLE token_metadata_original
    ADD COLUMN capture_mode TEXT NOT NULL DEFAULT 'LEGACY_FIRST_AVAILABLE'
        CHECK (capture_mode IN ('LAUNCH_BLOCK', 'FIRST_AVAILABLE', 'LEGACY_FIRST_AVAILABLE')),
    ADD COLUMN exact_launch_snapshot BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN requested_block uint64_numeric,
    ADD CONSTRAINT token_metadata_original_exact_evidence CHECK (
        (capture_mode = 'LAUNCH_BLOCK' AND exact_launch_snapshot AND requested_block = observed_block)
        OR (capture_mode <> 'LAUNCH_BLOCK' AND NOT exact_launch_snapshot)
    );

ALTER TABLE token_metadata_current
    ADD COLUMN capture_mode TEXT NOT NULL DEFAULT 'LEGACY_FIRST_AVAILABLE'
        CHECK (capture_mode IN ('LAUNCH_BLOCK', 'FIRST_AVAILABLE', 'CURRENT', 'LEGACY_FIRST_AVAILABLE')),
    ADD COLUMN exact_launch_snapshot BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN requested_block uint64_numeric,
    ADD CONSTRAINT token_metadata_current_exact_evidence CHECK (
        (capture_mode = 'LAUNCH_BLOCK' AND exact_launch_snapshot AND requested_block = observed_block)
        OR (capture_mode <> 'LAUNCH_BLOCK' AND NOT exact_launch_snapshot)
    );

ALTER TABLE token_metadata_snapshots
    ADD COLUMN capture_mode TEXT NOT NULL DEFAULT 'LEGACY_FIRST_AVAILABLE'
        CHECK (capture_mode IN ('LAUNCH_BLOCK', 'FIRST_AVAILABLE', 'CURRENT', 'LEGACY_FIRST_AVAILABLE')),
    ADD COLUMN exact_launch_snapshot BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN requested_block uint64_numeric,
    ADD CONSTRAINT token_metadata_snapshots_exact_evidence CHECK (
        (capture_mode = 'LAUNCH_BLOCK' AND exact_launch_snapshot AND requested_block = observed_block)
        OR (capture_mode <> 'LAUNCH_BLOCK' AND NOT exact_launch_snapshot)
    );

COMMENT ON COLUMN token_metadata_original.capture_mode IS
    'LAUNCH_BLOCK is exact; FIRST_AVAILABLE is an explicit historical fallback; LEGACY_FIRST_AVAILABLE predates exact capture evidence';
