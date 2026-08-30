CREATE TABLE content_providers (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  provider_key text NOT NULL UNIQUE CHECK(char_length(provider_key) BETWEEN 1 AND 64),
  provider_type text NOT NULL CHECK(provider_type IN('AUTHORIZED_FOMO','USER_AUTHORIZED_IMPORT','MANUAL_REFERENCE','OTHER_AUTHORIZED_PROVIDER')),
  display_name text NOT NULL CHECK(char_length(display_name) BETWEEN 1 AND 128),
  authorization_basis text NOT NULL CHECK(authorization_basis IN('OFFICIAL_API','WRITTEN_PERMISSION','USER_PROVIDED','MANUAL_REFERENCE')),
  capabilities jsonb NOT NULL DEFAULT '{}'::jsonb,
  provenance jsonb NOT NULL DEFAULT '{}'::jsonb,
  automatic_fetch_enabled boolean NOT NULL DEFAULT false,
  raw_storage_allowed boolean NOT NULL DEFAULT false,
  health text NOT NULL DEFAULT 'UNAVAILABLE' CHECK(health IN('UNAVAILABLE','DISABLED','HEALTHY','DEGRADED')),
  last_success_at timestamptz,last_error text,
  created_at timestamptz NOT NULL DEFAULT now(),updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK(NOT automatic_fetch_enabled OR authorization_basis IN('OFFICIAL_API','WRITTEN_PERMISSION','USER_PROVIDED')),
  CHECK(NOT raw_storage_allowed OR authorization_basis IN('WRITTEN_PERMISSION','USER_PROVIDED'))
);
INSERT INTO content_providers(provider_key,provider_type,display_name,authorization_basis,capabilities,provenance,health)
VALUES('manual-reference','MANUAL_REFERENCE','Manual Reference','MANUAL_REFERENCE','{"manual_import":true,"automatic_fetch":false}'::jsonb,'{"operator_authored":true}'::jsonb,'DISABLED');

CREATE TABLE trader_content_items (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  trader_id uuid REFERENCES traders(id),
  provider_id uuid NOT NULL REFERENCES content_providers(id),
  provider_trader_reference text,
  platform text NOT NULL CHECK(char_length(platform) BETWEEN 1 AND 64),
  content_type text NOT NULL CHECK(content_type IN('TRADE_THESIS','POST','COMMENT','OTHER')),
  external_id text,external_reference text CHECK(external_reference IS NULL OR char_length(external_reference)<=2048),
  published_at timestamptz NOT NULL,observed_at timestamptz NOT NULL DEFAULT now(),
  content_hash bytea NOT NULL CHECK(octet_length(content_hash)=32),
  content_availability text NOT NULL DEFAULT 'REFERENCE_ONLY' CHECK(content_availability IN('REFERENCE_ONLY','SUMMARY_AVAILABLE','AUTHORIZED_RAW_AVAILABLE','UNAVAILABLE')),
  title text CHECK(title IS NULL OR char_length(title)<=256),
  summary text CHECK(summary IS NULL OR char_length(summary)<=4096),
  stance text CHECK(stance IS NULL OR stance IN('BULLISH','BEARISH','NEUTRAL','UNKNOWN')),
  narratives jsonb NOT NULL DEFAULT '[]'::jsonb,structured_analysis jsonb NOT NULL DEFAULT '{}'::jsonb,
  provenance jsonb NOT NULL,authorization_basis text NOT NULL CHECK(authorization_basis IN('OFFICIAL_API','WRITTEN_PERMISSION','USER_PROVIDED','MANUAL_REFERENCE')),
  raw_content_available boolean NOT NULL DEFAULT false,raw_content_authorized boolean NOT NULL DEFAULT false,
  realtime_alert_eligible boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(),updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK(NOT raw_content_authorized OR raw_content_available),
  CHECK(authorization_basis NOT IN('MANUAL_REFERENCE') OR NOT realtime_alert_eligible),
  UNIQUE(provider_id,content_hash)
);
CREATE UNIQUE INDEX trader_content_external_identity ON trader_content_items(provider_id,external_id) WHERE external_id IS NOT NULL;
CREATE INDEX trader_content_by_trader_time ON trader_content_items(trader_id,published_at DESC);

CREATE TABLE authorized_raw_content (
  content_id uuid PRIMARY KEY REFERENCES trader_content_items(id) ON DELETE CASCADE,
  raw_content text NOT NULL CHECK(char_length(raw_content)<=100000),
  authorization_evidence jsonb NOT NULL,created_at timestamptz NOT NULL DEFAULT now()
);
CREATE FUNCTION enforce_authorized_raw_content() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM trader_content_items c JOIN content_providers p ON p.id=c.provider_id
   WHERE c.id=NEW.content_id AND c.raw_content_authorized AND p.raw_storage_allowed) THEN
   RAISE EXCEPTION 'raw content storage is not authorized' USING ERRCODE='23514';
 END IF; RETURN NEW;
END;$$;
CREATE TRIGGER authorized_raw_content_guard BEFORE INSERT OR UPDATE ON authorized_raw_content FOR EACH ROW EXECUTE FUNCTION enforce_authorized_raw_content();

CREATE TABLE token_content_links (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),content_id uuid NOT NULL REFERENCES trader_content_items(id) ON DELETE CASCADE,
  token_id uuid NOT NULL REFERENCES tokens(id),relation_type text NOT NULL CHECK(relation_type IN('DIRECT_TOKEN','CONTRACT_MENTION','SYMBOL_MENTION','NARRATIVE_MATCH','MANUAL_LINK')),
  confidence numeric(5,4) NOT NULL CHECK(confidence BETWEEN 0 AND 1),evidence jsonb NOT NULL,created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(content_id,token_id,relation_type)
);
CREATE INDEX token_content_timeline ON token_content_links(token_id,content_id);

CREATE TABLE content_relation_jobs (
  trader_id uuid NOT NULL REFERENCES traders(id),token_id uuid NOT NULL REFERENCES tokens(id),generation bigint NOT NULL DEFAULT 1,
  claimed_generation bigint,status text NOT NULL DEFAULT 'PENDING' CHECK(status IN('PENDING','PROCESSING','RETRY','COMPLETED')),
  attempts integer NOT NULL DEFAULT 0,next_attempt_at timestamptz NOT NULL DEFAULT now(),locked_at timestamptz,last_error text,
  updated_at timestamptz NOT NULL DEFAULT now(),PRIMARY KEY(trader_id,token_id)
);
CREATE INDEX content_relation_jobs_due ON content_relation_jobs(next_attempt_at,trader_id,token_id) WHERE status IN('PENDING','PROCESSING','RETRY');

CREATE TABLE content_trade_relations (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),content_id uuid NOT NULL REFERENCES trader_content_items(id) ON DELETE CASCADE,
  trader_id uuid NOT NULL REFERENCES traders(id),token_id uuid NOT NULL REFERENCES tokens(id),
  relation_type text NOT NULL CHECK(relation_type IN('THESIS_BEFORE_BUY','THESIS_AFTER_BUY','THESIS_BEFORE_SELL','THESIS_AFTER_SELL','THESIS_WHILE_HOLDING','CONTENT_WITHOUT_POSITION','CONTENT_POSITION_ALIGNED','CONTENT_POSITION_DIVERGENT')),
  content_time timestamptz NOT NULL,trade_event_time timestamptz,delta_ms bigint,
  smart_trade_id uuid REFERENCES smart_trades(id),position_event_id uuid REFERENCES position_events(id),
  evidence jsonb NOT NULL,calculation_version integer NOT NULL CHECK(calculation_version>0),created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE NULLS NOT DISTINCT(content_id,relation_type,smart_trade_id,position_event_id,calculation_version)
);
CREATE INDEX content_relations_research ON content_trade_relations(token_id,content_time,content_id);

CREATE FUNCTION enqueue_content_relation_pair(tid uuid,kid uuid) RETURNS void LANGUAGE plpgsql AS $$BEGIN
 INSERT INTO content_relation_jobs(trader_id,token_id)VALUES(tid,kid)
 ON CONFLICT(trader_id,token_id)DO UPDATE SET generation=content_relation_jobs.generation+1,status='PENDING',next_attempt_at=now(),locked_at=NULL,last_error=NULL,updated_at=now();
END;$$;
CREATE FUNCTION content_link_relation_dirty() RETURNS trigger LANGUAGE plpgsql AS $$DECLARE tid uuid;BEGIN
 SELECT trader_id INTO tid FROM trader_content_items WHERE id=NEW.content_id; IF tid IS NOT NULL THEN PERFORM enqueue_content_relation_pair(tid,NEW.token_id);END IF;RETURN NEW;END;$$;
CREATE TRIGGER content_link_dirty AFTER INSERT OR UPDATE ON token_content_links FOR EACH ROW EXECUTE FUNCTION content_link_relation_dirty();
CREATE FUNCTION smart_trade_content_dirty() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN
 PERFORM enqueue_content_relation_pair(NEW.trader_id,NEW.token_id);RETURN NEW;END;$$;
CREATE TRIGGER smart_trade_content_relation_dirty AFTER INSERT OR UPDATE OF confirmation_level ON smart_trades FOR EACH ROW EXECUTE FUNCTION smart_trade_content_dirty();
