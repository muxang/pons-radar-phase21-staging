DROP INDEX token_trades_buyer_rank;
CREATE INDEX token_trades_buyer_rank
  ON token_trades(token_id,side,status,recipient,block_number,COALESCE(transaction_index,log_index),log_index);

-- Existing buyer-based ranks are derived state. Mark every confirmed wallet/token ledger dirty
-- so the normal Phase 9 worker replaces them using recipient identity after upgrade.
INSERT INTO position_rebuild_jobs(token_id,trader_wallet_id,wallet_address)
  SELECT DISTINCT token_id,trader_wallet_id,wallet_address
  FROM smart_trades
  WHERE confirmation_level IN ('BUY_CONFIRMED','SELL_CONFIRMED')
ON CONFLICT(token_id,trader_wallet_id) DO UPDATE SET
  generation=position_rebuild_jobs.generation+1,
  status='PENDING',next_attempt_at=now(),locked_at=NULL,last_error=NULL,updated_at=now();
