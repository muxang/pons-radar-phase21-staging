ALTER TABLE ai_research_reports ADD COLUMN knowledge_available_at timestamptz;
UPDATE ai_research_reports SET knowledge_available_at=created_at;
ALTER TABLE ai_research_reports ALTER COLUMN knowledge_available_at SET NOT NULL;
ALTER TABLE ai_research_reports ALTER COLUMN knowledge_available_at SET DEFAULT now();
COMMENT ON COLUMN ai_research_reports.knowledge_available_at IS 'When the validated AI report became available to pons-radar; predictive backtests must use this, not knowledge_cutoff.';

CREATE TABLE backtest_experiments (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), name text NOT NULL CHECK(char_length(name) BETWEEN 1 AND 160),
 created_by uuid NOT NULL REFERENCES users(id), created_at timestamptz NOT NULL DEFAULT now(),
 knowledge_mode text NOT NULL CHECK(knowledge_mode IN('KNOWLEDGE_TIME','EVENT_TIME_RECONSTRUCTED')),
 research_only boolean NOT NULL DEFAULT false,
 dataset_start timestamptz NOT NULL,dataset_end timestamptz NOT NULL,
 train_start timestamptz NOT NULL,train_end timestamptz NOT NULL,
 validation_start timestamptz NOT NULL,validation_end timestamptz NOT NULL,
 feature_set jsonb NOT NULL,signal_rule_version integer NOT NULL,score_calculation_version integer NOT NULL,
 weights jsonb NOT NULL,thresholds jsonb NOT NULL,bucket_definitions jsonb NOT NULL,outcome_definition jsonb NOT NULL,
 minimum_sample_size integer NOT NULL CHECK(minimum_sample_size>0),number_of_trials integer NOT NULL DEFAULT 1 CHECK(number_of_trials>0),
 selection_criteria text NOT NULL,code_version text NOT NULL,frontend_build_id text NOT NULL,api_schema_version integer NOT NULL,
 dataset_watermark jsonb NOT NULL,input_hash bytea NOT NULL CHECK(octet_length(input_hash)=32),config_version integer NOT NULL DEFAULT 1,
 CHECK(dataset_start<dataset_end),CHECK(train_start<train_end),CHECK(validation_start<validation_end),
 CHECK(train_end<=validation_start),CHECK(dataset_start<=train_start),CHECK(validation_end<=dataset_end),
 CHECK((knowledge_mode='EVENT_TIME_RECONSTRUCTED')=research_only),
 UNIQUE(input_hash)
);
CREATE INDEX backtest_experiments_created ON backtest_experiments(created_at DESC,id DESC);

CREATE FUNCTION reject_backtest_experiment_mutation()RETURNS trigger LANGUAGE plpgsql AS $$BEGIN
 RAISE EXCEPTION 'backtest experiment configuration is immutable';END;$$;
CREATE TRIGGER backtest_experiment_immutable BEFORE UPDATE OR DELETE ON backtest_experiments
 FOR EACH ROW EXECUTE FUNCTION reject_backtest_experiment_mutation();

CREATE TABLE backtest_experiment_runs (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),experiment_id uuid NOT NULL REFERENCES backtest_experiments(id),run_number integer NOT NULL,
 status text NOT NULL DEFAULT 'PENDING' CHECK(status IN('PENDING','RUNNING','RETRY','COMPLETED','FAILED','LOOKAHEAD_VIOLATION')),
 progress integer NOT NULL DEFAULT 0 CHECK(progress BETWEEN 0 AND 100),attempts integer NOT NULL DEFAULT 0,
 next_attempt_at timestamptz NOT NULL DEFAULT now(),locked_at timestamptz,last_error text,
 dataset_watermark jsonb NOT NULL,started_at timestamptz,completed_at timestamptz,
 train_result jsonb,validation_result jsonb,factor_result jsonb,warnings jsonb NOT NULL DEFAULT '[]',
 leakage_checks jsonb NOT NULL DEFAULT '[]',result_hash bytea CHECK(result_hash IS NULL OR octet_length(result_hash)=32),
 created_at timestamptz NOT NULL DEFAULT now(),updated_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(experiment_id,run_number)
);
CREATE INDEX backtest_runs_due ON backtest_experiment_runs(next_attempt_at,id)WHERE status IN('PENDING','RUNNING','RETRY');

CREATE TABLE historical_decision_points (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),run_id uuid NOT NULL REFERENCES backtest_experiment_runs(id)ON DELETE CASCADE,
 token_id uuid NOT NULL REFERENCES tokens(id),cohort text NOT NULL,
 event_effective_at timestamptz NOT NULL,decision_as_of timestamptz NOT NULL,knowledge_mode text NOT NULL,
 split text NOT NULL CHECK(split IN('IN_SAMPLE','OUT_OF_SAMPLE')),signal_snapshot_id uuid REFERENCES signal_snapshots(id),
 signal_state text NOT NULL,signal_score numeric,signal_confidence numeric,evidence_manifest jsonb NOT NULL,
 evidence_max_known_at timestamptz NOT NULL,leakage_valid boolean NOT NULL,
 UNIQUE(run_id,token_id,cohort,decision_as_of)
);
CREATE INDEX historical_decisions_run ON historical_decision_points(run_id,split,cohort,decision_as_of);

COMMENT ON TABLE backtest_experiments IS 'Immutable validation configuration; never production signal configuration.';
COMMENT ON COLUMN historical_decision_points.decision_as_of IS 'Knowledge-time decision cursor used by predictive validation.';
COMMENT ON COLUMN backtest_experiment_runs.dataset_watermark IS 'Frozen outbox/block/knowledge cutoff for reproducibility.';
