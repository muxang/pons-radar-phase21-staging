CREATE DOMAIN evm_address AS BYTEA CHECK (octet_length(VALUE) = 20);
CREATE DOMAIN evm_hash AS BYTEA CHECK (octet_length(VALUE) = 32);
CREATE DOMAIN uint64_numeric AS NUMERIC(20, 0)
    CHECK (VALUE BETWEEN 0 AND 18446744073709551615);
CREATE DOMAIN uint256_text AS TEXT CHECK (
    VALUE ~ '^(0|[1-9][0-9]*)$'
    AND (
        length(VALUE) < 78
        OR (length(VALUE) = 78 AND VALUE <= '115792089237316195423570985008687907853269984665640564039457584007913129639935')
    )
);

CREATE TABLE protocol_deployments (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    protocol TEXT NOT NULL,
    generation TEXT NOT NULL,
    chain_id uint64_numeric NOT NULL,
    address evm_address NOT NULL,
    start_block uint64_numeric NOT NULL,
    end_block uint64_numeric CHECK (end_block >= start_block),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    expected_event_topics JSONB NOT NULL DEFAULT '[]'::jsonb,
    expected_code_hash evm_hash,
    source TEXT NOT NULL,
    last_verified_at TIMESTAMPTZ,
    health TEXT NOT NULL DEFAULT 'UNVERIFIED',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (chain_id, address, start_block)
);

CREATE TABLE chain_cursors (
    stream TEXT PRIMARY KEY,
    chain_id uint64_numeric NOT NULL,
    last_processed_block uint64_numeric NOT NULL,
    last_processed_hash evm_hash NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE chain_blocks (
    chain_id uint64_numeric NOT NULL,
    block_number uint64_numeric NOT NULL,
    block_hash evm_hash NOT NULL,
    parent_hash evm_hash NOT NULL,
    block_time TIMESTAMPTZ,
    canonical BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chain_id, block_number),
    UNIQUE (chain_id, block_hash)
);

CREATE TABLE raw_chain_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    chain_id uint64_numeric NOT NULL,
    block_number uint64_numeric NOT NULL,
    block_hash evm_hash NOT NULL,
    tx_hash evm_hash NOT NULL,
    log_index uint64_numeric NOT NULL,
    address evm_address NOT NULL,
    topics JSONB NOT NULL,
    data BYTEA NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'CONFIRMED', 'ORPHANED')),
    observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (chain_id, tx_hash, log_index),
    UNIQUE (id, chain_id, tx_hash, log_index)
);
CREATE INDEX raw_chain_logs_block_idx ON raw_chain_logs (chain_id, block_number);

CREATE TABLE normalized_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    raw_log_id UUID NOT NULL,
    chain_id uint64_numeric NOT NULL,
    tx_hash evm_hash NOT NULL,
    log_index uint64_numeric NOT NULL,
    event_type TEXT NOT NULL,
    schema_version INTEGER NOT NULL CHECK (schema_version > 0),
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (raw_log_id, event_type),
    UNIQUE (chain_id, tx_hash, log_index, event_type),
    FOREIGN KEY (raw_log_id, chain_id, tx_hash, log_index)
        REFERENCES raw_chain_logs (id, chain_id, tx_hash, log_index)
);

CREATE TABLE tokens (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    chain_id uint64_numeric NOT NULL,
    address evm_address NOT NULL,
    curve_address evm_address,
    factory_address evm_address,
    deployer evm_address,
    pair_token evm_address,
    name TEXT,
    symbol TEXT,
    decimals SMALLINT CHECK (decimals BETWEEN 0 AND 255),
    total_supply_raw uint256_text,
    launch_tx evm_hash,
    launch_block uint64_numeric,
    launch_log_index uint64_numeric,
    launch_time TIMESTAMPTZ,
    lifecycle TEXT NOT NULL DEFAULT 'DISCOVERED',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (chain_id, address),
    UNIQUE (chain_id, curve_address)
);

CREATE TABLE traders (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    handle TEXT NOT NULL UNIQUE,
    display_name TEXT,
    manual_tier TEXT,
    notes TEXT,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE trader_wallets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    trader_id UUID NOT NULL REFERENCES traders(id),
    chain_id uint64_numeric NOT NULL,
    address evm_address NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('PROFILE_ADDRESS', 'ROBINHOOD_EXECUTION_ADDRESS', 'HISTORICAL_EXECUTION_ADDRESS')),
    source TEXT NOT NULL CHECK (source IN ('MANUAL', 'CSV_IMPORT', 'OPERATOR_VERIFIED', 'AUTHORIZED_PROVIDER')),
    identity_confidence NUMERIC(5, 4) NOT NULL CHECK (identity_confidence BETWEEN 0 AND 1),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    valid_from TIMESTAMPTZ NOT NULL DEFAULT now(),
    valid_to TIMESTAMPTZ,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (valid_to IS NULL OR valid_to > valid_from),
    UNIQUE (chain_id, address, valid_from)
);
CREATE INDEX trader_wallets_active_address_idx ON trader_wallets (chain_id, address) WHERE enabled AND valid_to IS NULL;

CREATE TABLE event_outbox (
    seq BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    event_type TEXT NOT NULL,
    schema_version INTEGER NOT NULL CHECK (schema_version > 0),
    aggregate_type TEXT,
    aggregate_id UUID,
    dedupe_key TEXT NOT NULL UNIQUE,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE alert_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    seq BIGINT NOT NULL UNIQUE REFERENCES event_outbox(seq),
    token_id UUID REFERENCES tokens(id),
    alert_type TEXT NOT NULL,
    severity TEXT NOT NULL CHECK (severity IN ('INFO', 'WATCH', 'STRONG', 'HIGH', 'CRITICAL_SYSTEM')),
    title TEXT NOT NULL,
    message TEXT NOT NULL,
    speech_text TEXT,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    acknowledged_at TIMESTAMPTZ,
    dedupe_key TEXT NOT NULL UNIQUE
);

CREATE TABLE app_settings (
    key TEXT PRIMARY KEY,
    value JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE audit_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID REFERENCES users(id),
    action TEXT NOT NULL,
    target_type TEXT NOT NULL,
    target_id TEXT,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX audit_logs_created_at_idx ON audit_logs (created_at DESC);
