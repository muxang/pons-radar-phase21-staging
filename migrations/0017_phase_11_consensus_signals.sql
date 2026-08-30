CREATE TABLE signal_rule_sets (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),version integer NOT NULL UNIQUE,
 weight_version integer NOT NULL,rule_version integer NOT NULL,calculation_version integer NOT NULL,
 config jsonb NOT NULL,active boolean NOT NULL DEFAULT false,created_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX one_active_signal_rule_set ON signal_rule_sets(active) WHERE active;

CREATE TABLE signal_rules (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),rule_set_id uuid NOT NULL REFERENCES signal_rule_sets(id),
 rule_id text NOT NULL,rule_version integer NOT NULL,enabled boolean NOT NULL DEFAULT true,
 thresholds jsonb NOT NULL,description text NOT NULL,UNIQUE(rule_set_id,rule_id)
);

CREATE TABLE signal_rebuild_jobs (
 token_id uuid PRIMARY KEY REFERENCES tokens(id),generation bigint NOT NULL DEFAULT 1,
 status text NOT NULL DEFAULT 'PENDING' CHECK(status IN('PENDING','PROCESSING','RETRY','COMPLETED')),
 attempts integer NOT NULL DEFAULT 0,next_attempt_at timestamptz NOT NULL DEFAULT now(),locked_at timestamptz,
 trigger_effective_at timestamptz,trigger_origin text NOT NULL DEFAULT 'DERIVED_REBUILD',
 trigger_realtime_eligible boolean NOT NULL DEFAULT false,last_error text,updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE consensus_snapshots (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),token_id uuid NOT NULL REFERENCES tokens(id),rebuild_generation bigint NOT NULL,
 effective_at timestamptz NOT NULL,window_seconds integer NOT NULL,
 raw_smart_buyers bigint NOT NULL,qualified_smart_buyers bigint NOT NULL,independent_smart_buyers bigint NOT NULL,
 smart_buy_volume_quote_raw uint256_text NOT NULL,smart_sell_volume_quote_raw uint256_text NOT NULL,smart_net_flow_quote_raw numeric(79,0) NOT NULL,
 first_smart_buy_age_ms bigint,median_smart_buy_age_ms bigint,
 smart_open_count bigint NOT NULL,smart_add_count bigint NOT NULL,smart_reduce_count bigint NOT NULL,smart_close_count bigint NOT NULL,
 wallet_exit_ratio numeric NOT NULL,quote_exit_ratio numeric NOT NULL,weighted_smart_consensus numeric NOT NULL,
 independence_weight numeric NOT NULL DEFAULT 1,cluster_id text,
 timing_component jsonb NOT NULL,rank_component jsonb NOT NULL,position_component jsonb NOT NULL,
 rule_version integer NOT NULL,calculation_version integer NOT NULL,inputs jsonb NOT NULL,content_hash bytea NOT NULL CHECK(octet_length(content_hash)=32),
 classification_origin text NOT NULL,realtime_alert_eligible boolean NOT NULL,signal_finality text NOT NULL CHECK(signal_finality IN('PENDING','CONFIRMED')),
 calculated_at timestamptz NOT NULL DEFAULT now(),current_generation boolean NOT NULL DEFAULT true,
 UNIQUE(token_id,rebuild_generation,effective_at,window_seconds)
);
CREATE INDEX consensus_history ON consensus_snapshots(token_id,effective_at,window_seconds) WHERE current_generation;

CREATE TABLE signal_snapshots (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),token_id uuid NOT NULL REFERENCES tokens(id),rebuild_generation bigint NOT NULL,
 effective_at timestamptz NOT NULL,state text NOT NULL CHECK(state IN('NO_SIGNAL','WATCH','STRONG_WATCH','HIGH_PRIORITY','COOLING','DISTRIBUTION','CLOSED')),
 score numeric NOT NULL CHECK(score BETWEEN 0 AND 100),confidence numeric NOT NULL CHECK(confidence BETWEEN 0 AND 100),
 component_scores jsonb NOT NULL,component_inputs jsonb NOT NULL,component_confidence jsonb NOT NULL,
 applied_weights jsonb NOT NULL,reason_codes jsonb NOT NULL,negative_reasons jsonb NOT NULL,matched_rules jsonb NOT NULL,
 rule_version integer NOT NULL,weight_version integer NOT NULL,calculation_version integer NOT NULL,
 content_hash bytea NOT NULL CHECK(octet_length(content_hash)=32),classification_origin text NOT NULL,
 realtime_alert_eligible boolean NOT NULL,signal_finality text NOT NULL CHECK(signal_finality IN('PENDING','CONFIRMED')),
 calculated_at timestamptz NOT NULL DEFAULT now(),current_generation boolean NOT NULL DEFAULT true,
 UNIQUE(token_id,rebuild_generation,effective_at)
);
CREATE INDEX signal_snapshot_history ON signal_snapshots(token_id,effective_at) WHERE current_generation;

CREATE TABLE signal_transitions (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),token_id uuid NOT NULL REFERENCES tokens(id),rebuild_generation bigint NOT NULL,
 signal_snapshot_id uuid NOT NULL REFERENCES signal_snapshots(id),effective_at timestamptz NOT NULL,
 from_state text NOT NULL,to_state text NOT NULL,score numeric NOT NULL,confidence numeric NOT NULL,
 reason_codes jsonb NOT NULL,matched_rules jsonb NOT NULL,classification_origin text NOT NULL,
 realtime_alert_eligible boolean NOT NULL,calculated_at timestamptz NOT NULL DEFAULT now(),current_generation boolean NOT NULL DEFAULT true,
 UNIQUE(token_id,rebuild_generation,effective_at,from_state,to_state)
);

CREATE TABLE current_signal_states (
 token_id uuid PRIMARY KEY REFERENCES tokens(id),state text NOT NULL,score numeric NOT NULL,confidence numeric NOT NULL,
 effective_at timestamptz NOT NULL,rebuild_generation bigint NOT NULL,signal_snapshot_id uuid NOT NULL REFERENCES signal_snapshots(id),updated_at timestamptz NOT NULL DEFAULT now()
);

-- Dedicated functions avoid referencing columns that do not exist on every source table.
CREATE FUNCTION enqueue_signal_from_smart_trade() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN
 INSERT INTO signal_rebuild_jobs(token_id,trigger_effective_at,trigger_origin,trigger_realtime_eligible)VALUES(NEW.token_id,NEW.block_time,NEW.classification_source,NEW.realtime_alert_eligible)
 ON CONFLICT(token_id)DO UPDATE SET generation=signal_rebuild_jobs.generation+1,status='PENDING',next_attempt_at=now(),locked_at=NULL,last_error=NULL,
 trigger_effective_at=EXCLUDED.trigger_effective_at,trigger_origin=EXCLUDED.trigger_origin,trigger_realtime_eligible=EXCLUDED.trigger_realtime_eligible,updated_at=now();RETURN NEW;END;$$;
CREATE TRIGGER smart_trade_signal_dirty AFTER INSERT OR UPDATE OF confirmation_level ON smart_trades FOR EACH ROW EXECUTE FUNCTION enqueue_signal_from_smart_trade();

CREATE FUNCTION enqueue_signal_from_market_snapshot() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN
 INSERT INTO signal_rebuild_jobs(token_id,trigger_effective_at,trigger_origin,trigger_realtime_eligible)VALUES(NEW.token_id,NEW.snapshot_at,'MARKET_SNAPSHOT',false)
 ON CONFLICT(token_id)DO UPDATE SET generation=signal_rebuild_jobs.generation+1,status='PENDING',next_attempt_at=now(),locked_at=NULL,last_error=NULL,
 trigger_effective_at=EXCLUDED.trigger_effective_at,trigger_origin='MARKET_SNAPSHOT',trigger_realtime_eligible=false,updated_at=now();RETURN NEW;END;$$;
CREATE TRIGGER market_snapshot_signal_dirty AFTER INSERT ON token_market_snapshots FOR EACH ROW EXECUTE FUNCTION enqueue_signal_from_market_snapshot();

CREATE FUNCTION enqueue_signal_from_position_event() RETURNS trigger LANGUAGE plpgsql AS $$DECLARE tid uuid;BEGIN
 SELECT token_id INTO tid FROM wallet_token_positions WHERE id=NEW.position_id;
 INSERT INTO signal_rebuild_jobs(token_id,trigger_effective_at,trigger_origin,trigger_realtime_eligible)VALUES(tid,NEW.block_time,NEW.classification_source,false)
 ON CONFLICT(token_id)DO UPDATE SET generation=signal_rebuild_jobs.generation+1,status='PENDING',next_attempt_at=now(),locked_at=NULL,last_error=NULL,
 trigger_effective_at=EXCLUDED.trigger_effective_at,trigger_origin=EXCLUDED.trigger_origin,trigger_realtime_eligible=false,updated_at=now();RETURN NEW;END;$$;
CREATE TRIGGER position_event_signal_dirty AFTER INSERT ON position_events FOR EACH ROW EXECUTE FUNCTION enqueue_signal_from_position_event();

CREATE FUNCTION enqueue_signal_from_trade_finality() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN
 IF NEW.status IS DISTINCT FROM OLD.status THEN INSERT INTO signal_rebuild_jobs(token_id,trigger_effective_at,trigger_origin,trigger_realtime_eligible)VALUES(NEW.token_id,NEW.block_time,'CHAIN_FINALITY_REBUILD',false)
 ON CONFLICT(token_id)DO UPDATE SET generation=signal_rebuild_jobs.generation+1,status='PENDING',next_attempt_at=now(),locked_at=NULL,last_error=NULL,
 trigger_effective_at=EXCLUDED.trigger_effective_at,trigger_origin='CHAIN_FINALITY_REBUILD',trigger_realtime_eligible=false,updated_at=now();END IF;RETURN NEW;END;$$;
CREATE TRIGGER token_trade_signal_finality AFTER UPDATE OF status ON token_trades FOR EACH ROW EXECUTE FUNCTION enqueue_signal_from_trade_finality();
