ALTER TABLE curve_state_observations
  ADD COLUMN state_scope text NOT NULL DEFAULT 'BLOCK_STATE_EXACT'
    CHECK(state_scope IN('BLOCK_STATE_EXACT','UNAVAILABLE'));

ALTER TABLE token_market_snapshots
  ADD COLUMN state_scope text NOT NULL DEFAULT 'UNAVAILABLE'
    CHECK(state_scope IN('BLOCK_STATE_EXACT','UNAVAILABLE'));
UPDATE token_market_snapshots SET state_scope=CASE WHEN state_exact THEN'BLOCK_STATE_EXACT'ELSE'UNAVAILABLE'END;

ALTER TABLE smart_trades
  ADD COLUMN entry_net_execution_price_quote numeric,
  ADD COLUMN execution_price_scope text CHECK(execution_price_scope IN('EVENT_POSITION_EXACT','UNAVAILABLE')),
  ADD COLUMN entry_context_scope text CHECK(entry_context_scope IN('BLOCK_STATE_EXACT','UNAVAILABLE'));

-- Phase 10 used block-end spot state as if it were event-position entry state.
-- Clear that derived claim; the deterministic market rebuild recalculates the
-- execution price from immutable event amounts and restores block-end context
-- under its narrower evidence scope.
UPDATE smart_trades SET entry_price_quote=NULL,entry_curve_progress=NULL,
 entry_implied_fdv_quote=NULL,entry_market_state_exact=false,
 entry_net_execution_price_quote=NULL,execution_price_scope='UNAVAILABLE',entry_context_scope='UNAVAILABLE';

ALTER TABLE wallet_token_positions
  ADD COLUMN first_entry_net_execution_price numeric,
  ADD COLUMN first_entry_price_scope text CHECK(first_entry_price_scope IN('EVENT_POSITION_EXACT','UNAVAILABLE')),
  ADD COLUMN first_entry_market_scope text CHECK(first_entry_market_scope IN('BLOCK_STATE_EXACT','UNAVAILABLE'));
UPDATE wallet_token_positions SET first_entry_price=NULL,first_entry_market_cap=NULL,
 first_entry_curve_progress=NULL,first_entry_net_execution_price=NULL,
 first_entry_price_scope='UNAVAILABLE',first_entry_market_scope='UNAVAILABLE';

-- Existing Phase 10 derived rows are deterministically rebuilt under v2
-- semantics. A position created after the market worker ran also re-dirties
-- its token so entry evidence is eventually populated without a race.
INSERT INTO market_rebuild_jobs(token_id)
 SELECT DISTINCT token_id FROM smart_trades
ON CONFLICT(token_id) DO UPDATE SET generation=market_rebuild_jobs.generation+1,
 status='PENDING',next_attempt_at=now(),locked_at=NULL,last_error=NULL,updated_at=now();
CREATE TRIGGER position_entry_market_dirty AFTER INSERT ON wallet_token_positions
 FOR EACH ROW EXECUTE FUNCTION enqueue_market_rebuild();
