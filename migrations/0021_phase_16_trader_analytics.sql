CREATE TABLE trader_position_episodes (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), trader_id uuid NOT NULL REFERENCES traders(id), trader_wallet_id uuid NOT NULL REFERENCES trader_wallets(id),
 token_id uuid NOT NULL REFERENCES tokens(id), episode_number integer NOT NULL CHECK(episode_number>0), opened_at timestamptz NOT NULL, closed_at timestamptz,
 opening_smart_trade_id uuid NOT NULL REFERENCES smart_trades(id), closing_smart_trade_id uuid REFERENCES smart_trades(id),
 first_entry_price numeric, first_entry_age_ms bigint, first_buyer_rank bigint, first_smart_buyer_rank bigint,
 initial_buy_quote_raw uint256_text NOT NULL, total_buy_quote_raw uint256_text NOT NULL, total_sell_quote_raw uint256_text NOT NULL,
 relative_initial_size numeric, relative_size_history_samples integer NOT NULL DEFAULT 0, relative_size_confidence numeric NOT NULL DEFAULT 0,
 add_count integer NOT NULL, reduce_count integer NOT NULL, classification_sources jsonb NOT NULL,
 position_integrity text NOT NULL CHECK(position_integrity IN('OK','POSITION_INTEGRITY_WARNING')),
 calculation_version integer NOT NULL, current_generation bigint NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), rebuilt_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(trader_wallet_id,token_id,episode_number,current_generation)
);
CREATE INDEX trader_episodes_history ON trader_position_episodes(trader_id,opened_at,id);

CREATE TABLE trader_episode_outcomes (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), episode_id uuid NOT NULL REFERENCES trader_position_episodes(id) ON DELETE CASCADE,
 horizon_seconds integer NOT NULL CHECK(horizon_seconds IN(300,900,3600,21600,86400)), target_time timestamptz NOT NULL,
 observation_time timestamptz, observation_block uint64_numeric, entry_price numeric, observed_price numeric, price_change numeric,
 status text NOT NULL CHECK(status IN('AVAILABLE','PENDING','CENSORED_POST_GRADUATION','UNAVAILABLE','INVALIDATED')),
 evidence_scope text NOT NULL CHECK(evidence_scope IN('BLOCK_STATE_EXACT','UNAVAILABLE')),
 evidence jsonb NOT NULL, calculation_version integer NOT NULL, available_at timestamptz NOT NULL,
 UNIQUE(episode_id,horizon_seconds)
);

CREATE TABLE trader_episode_excursions (
 episode_id uuid PRIMARY KEY REFERENCES trader_position_episodes(id) ON DELETE CASCADE,
 observed_mfe numeric, observed_mae numeric, window_seconds integer NOT NULL, snapshot_count integer NOT NULL,
 observation_basis text NOT NULL DEFAULT 'PERSISTED_MARKET_SNAPSHOTS' CHECK(observation_basis='PERSISTED_MARKET_SNAPSHOTS'),
 calculation_version integer NOT NULL
);

CREATE TABLE trader_analytics_jobs (
 trader_id uuid PRIMARY KEY REFERENCES traders(id), generation bigint NOT NULL DEFAULT 1, status text NOT NULL DEFAULT 'PENDING' CHECK(status IN('PENDING','PROCESSING','RETRY','COMPLETED')),
 attempts integer NOT NULL DEFAULT 0,next_attempt_at timestamptz NOT NULL DEFAULT now(),locked_at timestamptz,last_error text,updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX trader_analytics_jobs_due ON trader_analytics_jobs(next_attempt_at,trader_id) WHERE status IN('PENDING','PROCESSING','RETRY');

CREATE TABLE trader_score_history (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),trader_id uuid NOT NULL REFERENCES traders(id),pons_score numeric NOT NULL CHECK(pons_score BETWEEN 0 AND 100),
 pons_score_confidence numeric NOT NULL CHECK(pons_score_confidence BETWEEN 0 AND 100),component_scores jsonb NOT NULL,component_inputs jsonb NOT NULL,
 sample_size integer NOT NULL,matured_horizons integer NOT NULL,pending_horizons integer NOT NULL,censored_horizons integer NOT NULL,
 calculation_version integer NOT NULL,rule_version integer NOT NULL,weight_version integer NOT NULL,effective_at timestamptz NOT NULL,calculated_at timestamptz NOT NULL DEFAULT now(),
 inputs_hash bytea NOT NULL CHECK(octet_length(inputs_hash)=32),current_generation boolean NOT NULL DEFAULT true,
 UNIQUE(trader_id,effective_at,calculation_version,inputs_hash)
);
CREATE INDEX trader_score_as_of ON trader_score_history(trader_id,effective_at DESC) WHERE current_generation;
CREATE TABLE current_trader_scores (
 trader_id uuid PRIMARY KEY REFERENCES traders(id),score_history_id uuid NOT NULL REFERENCES trader_score_history(id),pons_score numeric NOT NULL,
 pons_score_confidence numeric NOT NULL,sample_size integer NOT NULL,effective_at timestamptz NOT NULL,updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE FUNCTION dirty_trader_analytics(p_trader uuid) RETURNS void LANGUAGE plpgsql AS $$ BEGIN
 INSERT INTO trader_analytics_jobs(trader_id)VALUES(p_trader)ON CONFLICT(trader_id)DO UPDATE SET generation=trader_analytics_jobs.generation+1,status='PENDING',next_attempt_at=now(),locked_at=NULL,last_error=NULL,updated_at=now();
END;$$;
CREATE FUNCTION dirty_analytics_from_position()RETURNS trigger LANGUAGE plpgsql AS $$ DECLARE v uuid;BEGIN SELECT trader_id INTO v FROM wallet_token_positions WHERE id=COALESCE(NEW.position_id,OLD.position_id);PERFORM dirty_trader_analytics(v);RETURN COALESCE(NEW,OLD);END;$$;
CREATE TRIGGER position_event_analytics_dirty AFTER INSERT OR UPDATE OR DELETE ON position_events FOR EACH ROW EXECUTE FUNCTION dirty_analytics_from_position();
CREATE FUNCTION dirty_analytics_from_snapshot()RETURNS trigger LANGUAGE plpgsql AS $$BEGIN PERFORM dirty_trader_analytics(x.trader_id)FROM(SELECT DISTINCT trader_id FROM smart_trades WHERE token_id=NEW.token_id)x;RETURN NEW;END;$$;
CREATE TRIGGER market_snapshot_analytics_dirty AFTER INSERT OR UPDATE ON token_market_snapshots FOR EACH ROW EXECUTE FUNCTION dirty_analytics_from_snapshot();
CREATE FUNCTION dirty_analytics_from_smart()RETURNS trigger LANGUAGE plpgsql AS $$BEGIN PERFORM dirty_trader_analytics(NEW.trader_id);RETURN NEW;END;$$;
CREATE TRIGGER smart_trade_analytics_dirty AFTER INSERT OR UPDATE OF confirmation_level ON smart_trades FOR EACH ROW EXECUTE FUNCTION dirty_analytics_from_smart();
CREATE FUNCTION dirty_analytics_from_finality()RETURNS trigger LANGUAGE plpgsql AS $$BEGIN PERFORM dirty_trader_analytics(s.trader_id)FROM smart_trades s WHERE s.token_trade_id=NEW.id;RETURN NEW;END;$$;
CREATE TRIGGER trade_finality_analytics_dirty AFTER UPDATE OF status ON token_trades FOR EACH ROW EXECUTE FUNCTION dirty_analytics_from_finality();
CREATE FUNCTION dirty_analytics_from_lifecycle()RETURNS trigger LANGUAGE plpgsql AS $$BEGIN PERFORM dirty_trader_analytics(s.trader_id)FROM smart_trades s WHERE s.token_id=NEW.id;RETURN NEW;END;$$;
CREATE TRIGGER token_lifecycle_analytics_dirty AFTER UPDATE OF lifecycle ON tokens FOR EACH ROW EXECUTE FUNCTION dirty_analytics_from_lifecycle();

INSERT INTO trader_analytics_jobs(trader_id)SELECT id FROM traders ON CONFLICT DO NOTHING;
