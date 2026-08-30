ALTER TABLE smart_trades
  DROP CONSTRAINT smart_trades_classification_source_check,
  ADD CONSTRAINT smart_trades_classification_source_check
    CHECK (classification_source IN ('LIVE','CHAIN_BACKFILL','IDENTITY_BACKFILL'));
