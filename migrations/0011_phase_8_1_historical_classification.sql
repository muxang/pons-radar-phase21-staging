ALTER TABLE smart_trades
  ADD COLUMN classification_source text NOT NULL DEFAULT 'LIVE'
    CHECK (classification_source IN ('LIVE','IDENTITY_BACKFILL')),
  ADD COLUMN realtime_alert_eligible boolean NOT NULL DEFAULT true,
  ADD CONSTRAINT smart_trades_backfill_not_realtime
    CHECK (classification_source = 'LIVE' OR NOT realtime_alert_eligible);

CREATE TABLE identity_classification_jobs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  trader_wallet_id uuid NOT NULL REFERENCES trader_wallets(id),
  trader_id uuid NOT NULL REFERENCES traders(id),
  wallet_address bytea NOT NULL CHECK (octet_length(wallet_address)=20),
  identity_snapshot jsonb NOT NULL,
  valid_from timestamptz NOT NULL,
  valid_to timestamptz,
  scan_cutoff timestamptz NOT NULL DEFAULT now(),
  cursor_created_at timestamptz,
  cursor_trade_id uuid,
  status text NOT NULL DEFAULT 'PENDING'
    CHECK (status IN ('PENDING','PROCESSING','RETRY','COMPLETED')),
  attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  scanned_count bigint NOT NULL DEFAULT 0 CHECK (scanned_count >= 0),
  candidates_created bigint NOT NULL DEFAULT 0 CHECK (candidates_created >= 0),
  next_attempt_at timestamptz NOT NULL DEFAULT now(),
  locked_at timestamptz,
  last_error text,
  requested_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (valid_to IS NULL OR valid_to > valid_from),
  CHECK ((cursor_created_at IS NULL) = (cursor_trade_id IS NULL))
);

CREATE UNIQUE INDEX identity_classification_one_active_wallet
  ON identity_classification_jobs(trader_wallet_id)
  WHERE status IN ('PENDING','PROCESSING','RETRY');
CREATE INDEX identity_classification_due
  ON identity_classification_jobs(next_attempt_at, id)
  WHERE status IN ('PENDING','PROCESSING','RETRY');
CREATE INDEX token_trades_identity_backfill
  ON token_trades(created_at,id);
