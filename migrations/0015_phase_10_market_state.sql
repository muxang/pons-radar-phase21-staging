CREATE TABLE token_transfers (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), chain_id uint64_numeric NOT NULL DEFAULT 4663,
 token_id uuid NOT NULL REFERENCES tokens(id), token_address evm_address NOT NULL,
 from_address evm_address NOT NULL, to_address evm_address NOT NULL, amount_raw uint256_text NOT NULL,
 block_number uint64_numeric NOT NULL, block_hash evm_hash NOT NULL, tx_hash evm_hash NOT NULL,
 transaction_index uint64_numeric, log_index uint64_numeric NOT NULL, block_time timestamptz NOT NULL,
 raw_log_id uuid NOT NULL REFERENCES raw_chain_logs(id), normalized_event_id bytea NOT NULL CHECK(octet_length(normalized_event_id)=32),
 status text NOT NULL DEFAULT 'PENDING' CHECK(status IN('PENDING','CONFIRMED','ORPHANED')), created_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(chain_id,tx_hash,log_index), UNIQUE(raw_log_id), UNIQUE(normalized_event_id)
);
CREATE INDEX token_transfers_replay ON token_transfers(token_id,block_number,COALESCE(transaction_index,log_index),log_index);

CREATE TABLE token_wallet_balances (
 token_id uuid NOT NULL REFERENCES tokens(id), wallet_address evm_address NOT NULL,
 balance_raw uint256_text NOT NULL, excluded_from_holder_count boolean NOT NULL,
 exclusion_reason text, calculation_version integer NOT NULL, updated_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(token_id,wallet_address)
);

CREATE TABLE token_market_state (
 token_id uuid PRIMARY KEY REFERENCES tokens(id), calculation_version integer NOT NULL,
 buy_count bigint NOT NULL, sell_count bigint NOT NULL, unique_buyers bigint NOT NULL, unique_sellers bigint NOT NULL,
 user_buy_volume_raw uint256_text NOT NULL, user_sell_volume_raw uint256_text NOT NULL, user_net_flow_raw numeric(79,0) NOT NULL,
 curve_effective_in_raw uint256_text NOT NULL, curve_effective_out_raw uint256_text NOT NULL, curve_effective_net_flow_raw numeric(79,0) NOT NULL,
 refund_count bigint NOT NULL, refund_quote_total_raw uint256_text NOT NULL,
 smart_unique_buyers bigint NOT NULL, smart_unique_sellers bigint NOT NULL,
 smart_buy_quote_raw uint256_text NOT NULL, smart_sell_quote_raw uint256_text NOT NULL, smart_net_flow_raw numeric(79,0) NOT NULL,
 raw_holder_count bigint NOT NULL, integrity_status text NOT NULL DEFAULT 'OK', rebuilt_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE market_rebuild_jobs (
 token_id uuid PRIMARY KEY REFERENCES tokens(id), generation bigint NOT NULL DEFAULT 1,
 status text NOT NULL DEFAULT 'PENDING' CHECK(status IN('PENDING','PROCESSING','RETRY','COMPLETED')),
 attempts integer NOT NULL DEFAULT 0,next_attempt_at timestamptz NOT NULL DEFAULT now(),locked_at timestamptz,last_error text,updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE token_market_snapshots (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),token_id uuid NOT NULL REFERENCES tokens(id),snapshot_kind text NOT NULL,
 snapshot_at timestamptz NOT NULL,snapshot_block uint64_numeric NOT NULL,age_since_launch_ms bigint NOT NULL,
 buy_count bigint NOT NULL,sell_count bigint NOT NULL,unique_buyers bigint NOT NULL,unique_sellers bigint NOT NULL,
 user_buy_volume_raw uint256_text NOT NULL,user_sell_volume_raw uint256_text NOT NULL,user_net_flow_raw numeric(79,0) NOT NULL,
 curve_effective_in_raw uint256_text NOT NULL,curve_effective_out_raw uint256_text NOT NULL,curve_effective_net_flow_raw numeric(79,0) NOT NULL,
 smart_unique_buyers bigint NOT NULL,smart_unique_sellers bigint NOT NULL,smart_buy_quote_raw uint256_text NOT NULL,smart_sell_quote_raw uint256_text NOT NULL,smart_net_flow_raw numeric(79,0) NOT NULL,
 holder_count bigint NOT NULL,sellable_tokens_raw uint256_text,reserved_tokens_raw uint256_text,real_quote_reserve_raw uint256_text,graduation_threshold_raw uint256_text,
 curve_progress numeric,quote_progress numeric,spot_price_quote numeric,curve_implied_fdv_quote numeric,
 price_basis text,calculation_version integer NOT NULL,state_exact boolean NOT NULL,evidence jsonb NOT NULL,created_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(token_id,snapshot_kind,snapshot_block)
);

CREATE TABLE curve_state_observations (
 token_id uuid NOT NULL REFERENCES tokens(id),block_number uint64_numeric NOT NULL,
 quote_reserve_raw uint256_text,token_reserve_raw uint256_text,sellable_tokens_raw uint256_text,
 reserved_tokens_raw uint256_text,real_quote_reserve_raw uint256_text,graduation_threshold_raw uint256_text,
 ready_to_graduate boolean,token_decimals smallint,quote_decimals smallint,
 curve_progress numeric,quote_progress numeric,spot_price_quote numeric,curve_implied_fdv_quote numeric,
 integrity_warning text,state_exact boolean NOT NULL,evidence jsonb NOT NULL,observed_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(token_id,block_number)
);

ALTER TABLE smart_trades ADD COLUMN entry_price_quote numeric,ADD COLUMN entry_curve_progress numeric,
 ADD COLUMN entry_implied_fdv_quote numeric,ADD COLUMN entry_market_state_exact boolean;

CREATE OR REPLACE FUNCTION enqueue_market_rebuild() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 INSERT INTO market_rebuild_jobs(token_id) VALUES(NEW.token_id) ON CONFLICT(token_id) DO UPDATE SET generation=market_rebuild_jobs.generation+1,status='PENDING',next_attempt_at=now(),locked_at=NULL,last_error=NULL,updated_at=now();RETURN NEW;
END;$$;
CREATE TRIGGER token_trade_market_dirty AFTER INSERT OR UPDATE OF status ON token_trades FOR EACH ROW EXECUTE FUNCTION enqueue_market_rebuild();
CREATE TRIGGER token_transfer_market_dirty AFTER INSERT OR UPDATE OF status ON token_transfers FOR EACH ROW EXECUTE FUNCTION enqueue_market_rebuild();
CREATE TRIGGER curve_accounting_market_dirty AFTER INSERT ON curve_accounting_events FOR EACH ROW EXECUTE FUNCTION enqueue_market_rebuild();
CREATE TRIGGER smart_trade_market_dirty AFTER INSERT OR UPDATE OF confirmation_level ON smart_trades FOR EACH ROW EXECUTE FUNCTION enqueue_market_rebuild();
