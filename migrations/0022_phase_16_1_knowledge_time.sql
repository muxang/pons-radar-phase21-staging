ALTER TABLE trader_position_episodes ADD COLUMN knowledge_available_at timestamptz DEFAULT now();
UPDATE trader_position_episodes e SET knowledge_available_at=x.known_at FROM(
 SELECT e2.id,max(COALESCE(s.confirmed_at,s.created_at))known_at FROM trader_position_episodes e2
 JOIN position_events p ON p.block_time BETWEEN e2.opened_at AND COALESCE(e2.closed_at,'infinity')
 JOIN wallet_token_positions w ON w.id=p.position_id AND w.trader_wallet_id=e2.trader_wallet_id AND w.token_id=e2.token_id
 JOIN smart_trades s ON s.id=p.smart_trade_id GROUP BY e2.id)x WHERE e.id=x.id;
UPDATE trader_position_episodes SET knowledge_available_at=created_at WHERE knowledge_available_at IS NULL;
ALTER TABLE trader_position_episodes ALTER COLUMN knowledge_available_at SET NOT NULL;

ALTER TABLE trader_score_history ADD COLUMN knowledge_available_at timestamptz;
UPDATE trader_score_history SET knowledge_available_at=calculated_at;
ALTER TABLE trader_score_history ALTER COLUMN knowledge_available_at SET NOT NULL;
ALTER TABLE trader_score_history ADD COLUMN as_of_mode text NOT NULL DEFAULT 'KNOWLEDGE_TIME'
 CHECK(as_of_mode IN('KNOWLEDGE_TIME','EVENT_TIME_RECONSTRUCTED'));
ALTER TABLE trader_score_history DROP CONSTRAINT trader_score_history_trader_id_effective_at_calculation_ver_key;
ALTER TABLE trader_score_history ADD CONSTRAINT trader_score_history_mode_identity
 UNIQUE(trader_id,effective_at,as_of_mode,calculation_version,inputs_hash);
DROP INDEX trader_score_as_of;
CREATE INDEX trader_score_as_of ON trader_score_history(trader_id,as_of_mode,effective_at DESC)WHERE current_generation;

ALTER TABLE current_trader_scores ADD COLUMN as_of_mode text NOT NULL DEFAULT 'KNOWLEDGE_TIME'
 CHECK(as_of_mode='KNOWLEDGE_TIME');

COMMENT ON COLUMN trader_position_episodes.opened_at IS 'EVENT TIME: first confirmed chain trade in the episode';
COMMENT ON COLUMN trader_position_episodes.knowledge_available_at IS 'KNOWLEDGE TIME: latest source-fact confirmation time required to know this episode';
COMMENT ON COLUMN trader_episode_outcomes.target_time IS 'EVENT TIME horizon target';
COMMENT ON COLUMN trader_episode_outcomes.available_at IS 'KNOWLEDGE TIME: horizon maturity and required persisted market evidence availability';
COMMENT ON COLUMN trader_score_history.effective_at IS 'As-of cursor in the named as_of_mode';
COMMENT ON COLUMN trader_score_history.knowledge_available_at IS 'When pons-radar could actually use all score inputs';

INSERT INTO trader_analytics_jobs(trader_id)SELECT id FROM traders ON CONFLICT(trader_id)DO UPDATE SET
 generation=trader_analytics_jobs.generation+1,status='PENDING',next_attempt_at=now(),locked_at=NULL,last_error=NULL,updated_at=now();
