ALTER TABLE protocol_deployments
    ADD COLUMN interface_fingerprint TEXT NOT NULL DEFAULT 'pons-v2-factory:v1',
    ADD COLUMN verification_evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN verification_error TEXT;

ALTER TABLE protocol_deployments
    ADD CONSTRAINT protocol_deployments_protocol_check CHECK (protocol = 'PONS'),
    ADD CONSTRAINT protocol_deployments_generation_check CHECK (generation = 'V2'),
    ADD CONSTRAINT protocol_deployments_health_check
        CHECK (health IN ('UNVERIFIED', 'VERIFIED', 'DEGRADED')),
    ADD CONSTRAINT protocol_deployments_block_range_check
        CHECK (end_block IS NULL OR end_block >= start_block);

CREATE INDEX protocol_deployments_active_idx
    ON protocol_deployments (chain_id, start_block)
    WHERE enabled AND health = 'VERIFIED';

-- Documentation is provenance, not trust: the seed is disabled and unverified.
INSERT INTO protocol_deployments
    (protocol, generation, chain_id, address, start_block, enabled,
     expected_event_topics, source, interface_fingerprint, health)
VALUES
    ('PONS', 'V2', 4663, decode('7ed598bcef8bd9edd8c97a195c6d13f40801ec7e', 'hex'),
     0, FALSE, '[]'::jsonb, 'https://docs.ponsfamily.com/v2',
     'pons-v2-factory:v1', 'UNVERIFIED')
ON CONFLICT (chain_id, address, start_block) DO NOTHING;
