ALTER TABLE traders
    ADD COLUMN status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE','DISABLED')),
    ADD CONSTRAINT traders_handle_length CHECK (char_length(handle) BETWEEN 1 AND 64),
    ADD CONSTRAINT traders_display_name_length CHECK (display_name IS NULL OR char_length(display_name) <= 128),
    ADD CONSTRAINT traders_manual_tier CHECK (manual_tier IS NULL OR manual_tier IN ('S','A','B','C')),
    ADD CONSTRAINT traders_notes_length CHECK (notes IS NULL OR char_length(notes) <= 4096);

ALTER TABLE trader_wallets
    ADD COLUMN verified BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD CONSTRAINT trader_wallets_robinhood_chain CHECK (
        role <> 'ROBINHOOD_EXECUTION_ADDRESS' OR chain_id = 4663
    ),
    ADD CONSTRAINT trader_wallets_notes_length CHECK (notes IS NULL OR char_length(notes) <= 4096),
    ADD CONSTRAINT trader_wallets_source_phase7 CHECK (source IN ('MANUAL','CSV_IMPORT','OPERATOR_VERIFIED'));

-- An open, enabled execution identity can belong to only one trader. Historical rows remain.
CREATE UNIQUE INDEX trader_wallets_current_execution_identity
    ON trader_wallets(chain_id,address)
    WHERE role='ROBINHOOD_EXECUTION_ADDRESS' AND enabled AND valid_to IS NULL;

CREATE INDEX trader_wallets_runtime_matcher_idx
    ON trader_wallets(chain_id,address,valid_from,valid_to)
    WHERE role='ROBINHOOD_EXECUTION_ADDRESS' AND enabled AND verified;

CREATE FUNCTION reject_overlapping_execution_identity() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.role = 'ROBINHOOD_EXECUTION_ADDRESS' AND NEW.enabled THEN
        PERFORM pg_advisory_xact_lock(hashtextextended(NEW.chain_id::text || encode(NEW.address,'hex'), 0));
        IF EXISTS (
            SELECT 1 FROM trader_wallets w
            WHERE w.id <> NEW.id AND w.chain_id=NEW.chain_id AND w.address=NEW.address
              AND w.role='ROBINHOOD_EXECUTION_ADDRESS' AND w.enabled
              AND tstzrange(w.valid_from,w.valid_to,'[)') && tstzrange(NEW.valid_from,NEW.valid_to,'[)')
              AND w.trader_id <> NEW.trader_id
        ) THEN
            RAISE EXCEPTION 'execution address has overlapping trader identity' USING ERRCODE='23505';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER trader_wallets_execution_identity_guard
    BEFORE INSERT OR UPDATE ON trader_wallets
    FOR EACH ROW EXECUTE FUNCTION reject_overlapping_execution_identity();
