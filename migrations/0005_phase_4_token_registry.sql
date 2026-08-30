ALTER TABLE tokens
    ADD COLUMN deployment_id UUID REFERENCES protocol_deployments(id),
    ADD COLUMN launch_config_id uint256_text,
    ADD COLUMN graduation_threshold_raw uint256_text,
    ADD COLUMN launch_transaction_index uint64_numeric,
    ADD COLUMN launch_raw_log_id UUID REFERENCES raw_chain_logs(id),
    ADD COLUMN launch_normalized_event_id BYTEA,
    ADD CONSTRAINT tokens_launch_raw_log_unique UNIQUE (launch_raw_log_id),
    ADD CONSTRAINT tokens_launch_event_unique UNIQUE (launch_normalized_event_id);

CREATE TABLE pons_curves (
    chain_id uint64_numeric NOT NULL,
    curve_address evm_address NOT NULL,
    token_id UUID NOT NULL REFERENCES tokens(id),
    token_address evm_address NOT NULL,
    deployment_id UUID NOT NULL REFERENCES protocol_deployments(id),
    launch_raw_log_id UUID NOT NULL REFERENCES raw_chain_logs(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chain_id, curve_address),
    UNIQUE (chain_id, token_address),
    UNIQUE (token_id)
);

CREATE TABLE chain_ingestion_errors (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    deployment_id UUID NOT NULL REFERENCES protocol_deployments(id),
    chain_id uint64_numeric NOT NULL,
    block_number uint64_numeric NOT NULL,
    block_hash evm_hash NOT NULL,
    tx_hash evm_hash NOT NULL,
    log_index uint64_numeric NOT NULL,
    emitter evm_address NOT NULL,
    topics JSONB NOT NULL,
    data BYTEA NOT NULL,
    parser_version INTEGER NOT NULL,
    schema_version INTEGER NOT NULL,
    error TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (deployment_id, chain_id, tx_hash, log_index, parser_version, schema_version)
);
