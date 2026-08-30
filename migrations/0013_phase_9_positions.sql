CREATE TABLE position_rebuild_jobs (
  token_id uuid NOT NULL REFERENCES tokens(id),
  trader_wallet_id uuid NOT NULL REFERENCES trader_wallets(id),
  wallet_address evm_address NOT NULL,
  generation bigint NOT NULL DEFAULT 1 CHECK (generation > 0),
  claimed_generation bigint,
  status text NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING','PROCESSING','RETRY','COMPLETED')),
  attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  next_attempt_at timestamptz NOT NULL DEFAULT now(),
  locked_at timestamptz,
  last_error text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(token_id,trader_wallet_id)
);
CREATE INDEX position_rebuild_jobs_due ON position_rebuild_jobs(next_attempt_at,token_id,trader_wallet_id)
  WHERE status IN ('PENDING','PROCESSING','RETRY');

CREATE TABLE wallet_token_positions (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  chain_id uint64_numeric NOT NULL DEFAULT 4663 CHECK (chain_id=4663),
  token_id uuid NOT NULL REFERENCES tokens(id),
  trader_id uuid NOT NULL REFERENCES traders(id),
  trader_wallet_id uuid NOT NULL REFERENCES trader_wallets(id),
  wallet_address evm_address NOT NULL,
  balance_raw uint256_text NOT NULL DEFAULT '0',
  total_quote_in_raw uint256_text NOT NULL DEFAULT '0',
  total_quote_out_raw uint256_text NOT NULL DEFAULT '0',
  first_entry_at timestamptz,
  last_trade_at timestamptz,
  open boolean NOT NULL DEFAULT false,
  integrity_status text NOT NULL DEFAULT 'OK' CHECK (integrity_status IN ('OK','POSITION_INTEGRITY_WARNING')),
  position_basis text NOT NULL DEFAULT 'PONS_V2_CONFIRMED_TRADES' CHECK (position_basis='PONS_V2_CONFIRMED_TRADES'),
  calculation_version integer NOT NULL CHECK (calculation_version > 0),
  first_entry_price numeric,
  first_entry_market_cap numeric,
  first_entry_curve_progress numeric,
  rebuilt_at timestamptz NOT NULL DEFAULT now(),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(token_id,trader_wallet_id)
);

CREATE TABLE position_events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  position_id uuid NOT NULL REFERENCES wallet_token_positions(id) ON DELETE CASCADE,
  smart_trade_id uuid NOT NULL UNIQUE REFERENCES smart_trades(id),
  event_type text NOT NULL CHECK (event_type IN ('OPEN_POSITION','ADD_POSITION','REDUCE_POSITION','CLOSE_POSITION','POSITION_INTEGRITY_WARNING')),
  side text NOT NULL CHECK (side IN ('BUY','SELL')),
  token_amount_raw uint256_text NOT NULL,
  quote_amount_raw uint256_text NOT NULL,
  balance_before_raw uint256_text NOT NULL,
  balance_after_raw uint256_text NOT NULL,
  block_number uint64_numeric NOT NULL,
  transaction_index uint64_numeric,
  log_index uint64_numeric NOT NULL,
  block_time timestamptz NOT NULL,
  classification_source text NOT NULL CHECK (classification_source IN ('LIVE','CHAIN_BACKFILL','IDENTITY_BACKFILL')),
  calculation_version integer NOT NULL CHECK (calculation_version > 0),
  warning jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX position_events_timeline ON position_events(position_id,block_number,COALESCE(transaction_index,log_index),log_index);

CREATE INDEX token_trades_buyer_rank ON token_trades(token_id,side,status,actor,block_number,COALESCE(transaction_index,log_index),log_index);
CREATE INDEX smart_trades_smart_rank ON smart_trades(token_id,side,confirmation_level,wallet_address,block_number,log_index);

CREATE FUNCTION enqueue_position_rebuild_on_chain_status_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.status IS DISTINCT FROM OLD.status THEN
    INSERT INTO position_rebuild_jobs(token_id,trader_wallet_id,wallet_address)
      SELECT token_id,trader_wallet_id,wallet_address FROM smart_trades WHERE token_trade_id=NEW.id
    ON CONFLICT(token_id,trader_wallet_id) DO UPDATE SET
      generation=position_rebuild_jobs.generation+1,status='PENDING',next_attempt_at=now(),
      locked_at=NULL,last_error=NULL,updated_at=now();
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER token_trade_position_finality_guard
  AFTER UPDATE OF status ON token_trades
  FOR EACH ROW EXECUTE FUNCTION enqueue_position_rebuild_on_chain_status_change();
