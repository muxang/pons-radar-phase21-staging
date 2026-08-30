CREATE TABLE smart_trades (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    token_id UUID NOT NULL REFERENCES tokens(id),
    token_trade_id UUID NOT NULL UNIQUE REFERENCES token_trades(id),
    trader_id UUID NOT NULL REFERENCES traders(id),
    trader_wallet_id UUID NOT NULL REFERENCES trader_wallets(id),
    wallet_address evm_address NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('BUY','SELL')),
    confirmation_level TEXT NOT NULL CHECK (confirmation_level IN ('BUY_STRONG','SELL_STRONG','BUY_CONFIRMED','SELL_CONFIRMED','INTEGRITY_CONFLICT','REJECTED')),
    confirmation_confidence NUMERIC(5,4) NOT NULL CHECK (confirmation_confidence BETWEEN 0 AND 1),
    confirmation_version INTEGER NOT NULL CHECK (confirmation_version > 0),
    token_amount_raw uint256_text NOT NULL,
    quote_amount_raw uint256_text NOT NULL,
    fee_raw uint256_text NOT NULL,
    tax_raw uint256_text NOT NULL,
    block_number uint64_numeric NOT NULL,
    tx_hash evm_hash NOT NULL,
    log_index uint64_numeric NOT NULL,
    block_time TIMESTAMPTZ NOT NULL,
    launch_age_ms BIGINT,
    launch_age_blocks uint64_numeric,
    buyer_rank BIGINT,
    smart_buyer_rank BIGINT,
    identity_snapshot JSONB NOT NULL,
    evidence JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    confirmed_at TIMESTAMPTZ,
    CHECK ((confirmation_level IN ('BUY_CONFIRMED','SELL_CONFIRMED')) = (confirmed_at IS NOT NULL))
);
CREATE INDEX smart_trades_trader_time_idx ON smart_trades(trader_id,block_number,log_index);
CREATE INDEX smart_trades_token_time_idx ON smart_trades(token_id,block_number,log_index);

CREATE TABLE trade_confirmation_jobs (
    smart_trade_id UUID PRIMARY KEY REFERENCES smart_trades(id) ON DELETE CASCADE,
    token_trade_id UUID NOT NULL UNIQUE REFERENCES token_trades(id),
    status TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING','PROCESSING','RETRY','CONFIRMED','REJECTED')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX trade_confirmation_jobs_due_idx ON trade_confirmation_jobs(next_attempt_at,smart_trade_id)
    WHERE status IN ('PENDING','RETRY','PROCESSING');

CREATE TABLE trade_transfer_evidence (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    smart_trade_id UUID NOT NULL REFERENCES smart_trades(id) ON DELETE CASCADE,
    token_address evm_address NOT NULL,
    from_address evm_address NOT NULL,
    to_address evm_address NOT NULL,
    amount_raw uint256_text NOT NULL,
    log_index uint64_numeric NOT NULL,
    tx_hash evm_hash NOT NULL,
    raw_log JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(smart_trade_id,log_index)
);
