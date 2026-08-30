CREATE TABLE token_metadata_jobs (
    token_id UUID PRIMARY KEY REFERENCES tokens(id) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING', 'IN_PROGRESS', 'RETRY', 'SUCCEEDED')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error TEXT,
    locked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX token_metadata_jobs_due_idx
    ON token_metadata_jobs (next_attempt_at, token_id)
    WHERE status IN ('PENDING', 'RETRY', 'IN_PROGRESS', 'SUCCEEDED');

CREATE TABLE token_metadata_original (
    token_id UUID PRIMARY KEY REFERENCES tokens(id) ON DELETE RESTRICT,
    content_hash BYTEA NOT NULL CHECK (octet_length(content_hash) = 32),
    name TEXT NOT NULL CHECK (char_length(name) <= 256),
    symbol TEXT NOT NULL CHECK (char_length(symbol) <= 64),
    decimals SMALLINT NOT NULL CHECK (decimals BETWEEN 0 AND 255),
    total_supply_raw uint256_text NOT NULL,
    token_deployer evm_address NOT NULL,
    token_logo TEXT NOT NULL CHECK (char_length(token_logo) <= 2048),
    token_description TEXT NOT NULL CHECK (char_length(token_description) <= 8192),
    twitter TEXT NOT NULL CHECK (char_length(twitter) <= 2048),
    telegram TEXT NOT NULL CHECK (char_length(telegram) <= 2048),
    discord TEXT NOT NULL CHECK (char_length(discord) <= 2048),
    website TEXT NOT NULL CHECK (char_length(website) <= 2048),
    farcaster TEXT NOT NULL CHECK (char_length(farcaster) <= 2048),
    normalized_socials JSONB NOT NULL,
    raw_metadata JSONB NOT NULL,
    deployer_matches_launch BOOLEAN NOT NULL,
    integrity_warning TEXT,
    observed_block uint64_numeric NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL
);

CREATE FUNCTION reject_original_metadata_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'original token metadata is immutable';
END;
$$;
CREATE TRIGGER token_metadata_original_immutable
    BEFORE UPDATE OR DELETE ON token_metadata_original
    FOR EACH ROW EXECUTE FUNCTION reject_original_metadata_mutation();

CREATE TABLE token_metadata_current (
    token_id UUID PRIMARY KEY REFERENCES tokens(id) ON DELETE CASCADE,
    content_hash BYTEA NOT NULL CHECK (octet_length(content_hash) = 32),
    name TEXT NOT NULL CHECK (char_length(name) <= 256),
    symbol TEXT NOT NULL CHECK (char_length(symbol) <= 64),
    decimals SMALLINT NOT NULL CHECK (decimals BETWEEN 0 AND 255),
    total_supply_raw uint256_text NOT NULL,
    token_deployer evm_address NOT NULL,
    token_logo TEXT NOT NULL CHECK (char_length(token_logo) <= 2048),
    token_description TEXT NOT NULL CHECK (char_length(token_description) <= 8192),
    twitter TEXT NOT NULL CHECK (char_length(twitter) <= 2048),
    telegram TEXT NOT NULL CHECK (char_length(telegram) <= 2048),
    discord TEXT NOT NULL CHECK (char_length(discord) <= 2048),
    website TEXT NOT NULL CHECK (char_length(website) <= 2048),
    farcaster TEXT NOT NULL CHECK (char_length(farcaster) <= 2048),
    normalized_socials JSONB NOT NULL,
    raw_metadata JSONB NOT NULL,
    deployer_matches_launch BOOLEAN NOT NULL,
    integrity_warning TEXT,
    observed_block uint64_numeric NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE token_metadata_snapshots (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    token_id UUID NOT NULL REFERENCES tokens(id) ON DELETE CASCADE,
    content_hash BYTEA NOT NULL CHECK (octet_length(content_hash) = 32),
    metadata JSONB NOT NULL,
    deployer_matches_launch BOOLEAN NOT NULL,
    integrity_warning TEXT,
    observed_block uint64_numeric NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (token_id, content_hash)
);
CREATE INDEX token_metadata_snapshots_history_idx
    ON token_metadata_snapshots (token_id, observed_at DESC);

INSERT INTO token_metadata_jobs(token_id)
SELECT id FROM tokens
ON CONFLICT (token_id) DO NOTHING;
