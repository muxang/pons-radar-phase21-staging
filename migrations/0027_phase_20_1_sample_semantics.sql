ALTER TABLE backtest_experiments
 ADD COLUMN sample_unit text NOT NULL DEFAULT 'UNIQUE_TOKEN_FIRST_STATE_ENTRY'
  CHECK(sample_unit='UNIQUE_TOKEN_FIRST_STATE_ENTRY'),
 ADD COLUMN outcome_anchor text NOT NULL DEFAULT 'DECISION_TIME'
  CHECK(outcome_anchor IN('DECISION_TIME','LAUNCH_TIME')),
 ADD COLUMN baseline_definition jsonb NOT NULL DEFAULT '{"type":"AGE_MATCHED","version":1,"buckets_ms":[0,60000,180000,300000,900000]}'::jsonb;

ALTER TABLE historical_decision_points
 ADD COLUMN decision_at timestamptz,
 ADD COLUMN launch_age_ms bigint,
 ADD COLUMN sample_identity text,
 ADD COLUMN first_qualifying_decision_point boolean NOT NULL DEFAULT true,
 ADD COLUMN outcome_anchor text NOT NULL DEFAULT 'DECISION_TIME'
  CHECK(outcome_anchor IN('DECISION_TIME','LAUNCH_TIME')),
 ADD COLUMN age_bucket text,
 ADD COLUMN matched_cohort text,
 ADD COLUMN knowledge_cutoff timestamptz;

UPDATE historical_decision_points d
SET decision_at=d.decision_as_of,
    launch_age_ms=greatest(0,(extract(epoch FROM(d.decision_as_of-t.launch_time))*1000)::bigint),
    sample_identity=d.run_id::text||':'||d.token_id::text||':'||d.cohort,
    age_bucket='LEGACY',
    knowledge_cutoff=d.decision_as_of
FROM tokens t WHERE t.id=d.token_id;

ALTER TABLE historical_decision_points
 ALTER COLUMN decision_at SET NOT NULL,
 ALTER COLUMN launch_age_ms SET NOT NULL,
 ALTER COLUMN sample_identity SET NOT NULL,
 ALTER COLUMN age_bucket SET NOT NULL,
 ALTER COLUMN knowledge_cutoff SET NOT NULL;

DO $$
DECLARE previous_unique text;
BEGIN
 SELECT conname INTO previous_unique FROM pg_constraint
 WHERE conrelid='historical_decision_points'::regclass AND contype='u'
   AND pg_get_constraintdef(oid) LIKE 'UNIQUE (run_id, token_id, cohort, decision_as_of)%';
 IF previous_unique IS NOT NULL THEN
  EXECUTE format('ALTER TABLE historical_decision_points DROP CONSTRAINT %I',previous_unique);
 END IF;
END $$;
ALTER TABLE historical_decision_points
 ADD CONSTRAINT historical_decision_unique_sample UNIQUE(run_id,sample_identity),
 ADD CONSTRAINT historical_decision_nonnegative_age CHECK(launch_age_ms>=0);

CREATE INDEX historical_decisions_token_split
 ON historical_decision_points(run_id,split,token_id);

COMMENT ON COLUMN backtest_experiments.sample_unit IS 'Default statistical unit: one token at its first qualifying entry into each signal state.';
COMMENT ON COLUMN historical_decision_points.decision_at IS 'Outcome anchor. In KNOWLEDGE_TIME this is when pons-radar could first act on the state.';
COMMENT ON COLUMN historical_decision_points.sample_identity IS 'Immutable per-run statistical sample identity; unique constraint prevents repeated snapshot weighting.';
COMMENT ON COLUMN historical_decision_points.launch_age_ms IS 'Decision age used for versioned age-matched baseline strata.';
COMMENT ON COLUMN historical_decision_points.knowledge_cutoff IS 'Maximum knowledge time permitted for decision features.';
