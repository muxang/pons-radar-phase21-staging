CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE FUNCTION normalized_event_id(
    normalized_chain_id uint64_numeric,
    normalized_tx_hash evm_hash,
    normalized_log_index uint64_numeric,
    normalized_event_type TEXT,
    normalized_parser_version INTEGER,
    normalized_schema_version INTEGER
) RETURNS BYTEA
LANGUAGE SQL
IMMUTABLE
STRICT
PARALLEL SAFE
RETURN digest(
    convert_to(
        normalized_chain_id::text || chr(31) ||
        encode(normalized_tx_hash, 'hex') || chr(31) ||
        normalized_log_index::text || chr(31) ||
        length(normalized_event_type)::text || ':' || normalized_event_type || chr(31) ||
        normalized_parser_version::text || chr(31) ||
        normalized_schema_version::text,
        'UTF8'
    ),
    'sha256'
);

ALTER TABLE normalized_events
    ADD COLUMN parser_version INTEGER NOT NULL DEFAULT 1
        CHECK (parser_version > 0),
    ADD COLUMN event_id BYTEA GENERATED ALWAYS AS (
        normalized_event_id(
            chain_id,
            tx_hash,
            log_index,
            event_type,
            parser_version,
            schema_version
        )
    ) STORED;

ALTER TABLE normalized_events
    DROP CONSTRAINT normalized_events_raw_log_id_event_type_key,
    DROP CONSTRAINT normalized_events_chain_id_tx_hash_log_index_event_type_key;

ALTER TABLE normalized_events
    ADD CONSTRAINT normalized_events_event_id_key UNIQUE (event_id),
    ADD CONSTRAINT normalized_events_raw_log_version_key
        UNIQUE (raw_log_id, event_type, parser_version, schema_version),
    ADD CONSTRAINT normalized_events_chain_log_version_key
        UNIQUE (
            chain_id,
            tx_hash,
            log_index,
            event_type,
            parser_version,
            schema_version
        );
