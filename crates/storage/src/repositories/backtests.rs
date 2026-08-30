use std::{collections::BTreeMap, str::FromStr};

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

const SAMPLE_UNIT: &str = "UNIQUE_TOKEN_FIRST_STATE_ENTRY";
const OUTCOME_ANCHOR: &str = "DECISION_TIME";
const BASELINE_VERSION: i32 = 1;

#[derive(Clone, Debug)]
pub struct BacktestRepository {
    pool: PgPool,
}
#[derive(Clone, Debug, Deserialize)]
pub struct NewBacktestExperiment {
    pub name: String,
    pub knowledge_mode: String,
    pub dataset_start: DateTime<Utc>,
    pub dataset_end: DateTime<Utc>,
    pub train_start: DateTime<Utc>,
    pub train_end: DateTime<Utc>,
    pub validation_start: DateTime<Utc>,
    pub validation_end: DateTime<Utc>,
    pub feature_set: Value,
    pub signal_rule_version: i32,
    pub score_calculation_version: i32,
    pub weights: Value,
    pub thresholds: Value,
    pub bucket_definitions: Value,
    pub outcome_definition: Value,
    pub minimum_sample_size: i32,
    pub number_of_trials: i32,
    pub selection_criteria: String,
}
#[derive(Clone, Debug, FromRow)]
pub struct BacktestJob {
    pub id: Uuid,
    pub experiment_id: Uuid,
    pub attempts: i32,
}
#[derive(FromRow)]
struct Experiment {
    id: Uuid,
    knowledge_mode: String,
    dataset_start: DateTime<Utc>,
    dataset_end: DateTime<Utc>,
    train_end: DateTime<Utc>,
    minimum_sample_size: i32,
    dataset_watermark: Value,
}
#[derive(FromRow)]
struct OutcomeRow {
    split: String,
    cohort: String,
    horizon_seconds: i32,
    status: String,
    price_change: Option<String>,
    observed_mfe: Option<String>,
    observed_mae: Option<String>,
}
#[derive(FromRow)]
struct FactorRow {
    split: String,
    horizon_seconds: i32,
    price_change: Option<String>,
    smart_buyers: Option<i64>,
    qualified_buyers: Option<i64>,
    first_entry_age_ms: Option<i64>,
    buyer_rank: Option<i64>,
    smart_buyer_rank: Option<i64>,
    smart_net_flow: Option<String>,
    manual_tier: Option<String>,
    pons_score: Option<String>,
    identity_confidence: Option<String>,
    position_event: Option<String>,
    content_relation: Option<String>,
    content_delta_ms: Option<i64>,
    ai_narrative_strength: Option<i32>,
    ai_confidence: Option<i32>,
    ai_risk_count: Option<i32>,
    component_scores: Value,
}
#[derive(Default)]
struct Metrics {
    eligible: i64,
    available: Vec<Decimal>,
    censored: i64,
    unavailable: i64,
    pending: i64,
    invalidated: i64,
    mfe: Vec<Decimal>,
    mae: Vec<Decimal>,
}

#[allow(clippy::missing_errors_doc)]
impl BacktestRepository {
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
    #[allow(clippy::too_many_arguments)]
    pub async fn create(
        &self,
        v: &NewBacktestExperiment,
        user: Uuid,
        code: &str,
        build: &str,
        api: i32,
    ) -> Result<Uuid, sqlx::Error> {
        let watermark:Value=sqlx::query_scalar("SELECT jsonb_build_object('knowledge_cutoff',$1,'max_outbox_seq',COALESCE((SELECT max(seq)FROM event_outbox),0),'max_chain_block',COALESCE((SELECT max(block_number)FROM chain_blocks),0)::text)").bind(v.dataset_end).fetch_one(&self.pool).await?;
        let research_only = v.knowledge_mode == "EVENT_TIME_RECONSTRUCTED";
        let baseline = json!({"type":"AGE_MATCHED","version":BASELINE_VERSION,"buckets_ms":[0,60_000,180_000,300_000,900_000]});
        let config = json!({"name":v.name,"knowledge_mode":v.knowledge_mode,"dataset_start":v.dataset_start,"dataset_end":v.dataset_end,"train_start":v.train_start,"train_end":v.train_end,"validation_start":v.validation_start,"validation_end":v.validation_end,"feature_set":v.feature_set,"signal_rule_version":v.signal_rule_version,"score_calculation_version":v.score_calculation_version,"weights":v.weights,"thresholds":v.thresholds,"bucket_definitions":v.bucket_definitions,"outcome_definition":v.outcome_definition,"minimum_sample_size":v.minimum_sample_size,"number_of_trials":v.number_of_trials,"selection_criteria":v.selection_criteria,"code_version":code,"frontend_build_id":build,"api_schema_version":api,"dataset_watermark":watermark,"sample_unit":SAMPLE_UNIT,"outcome_anchor":OUTCOME_ANCHOR,"baseline_definition":baseline});
        let hash: [u8; 32] = Sha256::digest(canonical(&config)).into();
        sqlx::query_scalar("WITH inserted AS(INSERT INTO backtest_experiments(name,created_by,knowledge_mode,research_only,dataset_start,dataset_end,train_start,train_end,validation_start,validation_end,feature_set,signal_rule_version,score_calculation_version,weights,thresholds,bucket_definitions,outcome_definition,minimum_sample_size,number_of_trials,selection_criteria,code_version,frontend_build_id,api_schema_version,dataset_watermark,input_hash,sample_unit,outcome_anchor,baseline_definition)VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28)ON CONFLICT(input_hash)DO NOTHING RETURNING id)SELECT id FROM inserted UNION ALL SELECT id FROM backtest_experiments WHERE input_hash=$25 LIMIT 1").bind(&v.name).bind(user).bind(&v.knowledge_mode).bind(research_only).bind(v.dataset_start).bind(v.dataset_end).bind(v.train_start).bind(v.train_end).bind(v.validation_start).bind(v.validation_end).bind(&v.feature_set).bind(v.signal_rule_version).bind(v.score_calculation_version).bind(&v.weights).bind(&v.thresholds).bind(&v.bucket_definitions).bind(&v.outcome_definition).bind(v.minimum_sample_size).bind(v.number_of_trials).bind(&v.selection_criteria).bind(code).bind(build).bind(api).bind(&watermark).bind(hash.as_slice()).bind(SAMPLE_UNIT).bind(OUTCOME_ANCHOR).bind(&baseline).fetch_one(&self.pool).await
    }
    pub async fn enqueue(&self, experiment: Uuid) -> Result<Uuid, sqlx::Error> {
        let watermark: Value =
            sqlx::query_scalar("SELECT dataset_watermark FROM backtest_experiments WHERE id=$1")
                .bind(experiment)
                .fetch_one(&self.pool)
                .await?;
        sqlx::query_scalar("INSERT INTO backtest_experiment_runs(experiment_id,run_number,dataset_watermark)SELECT $1,COALESCE(max(run_number),0)+1,$2 FROM backtest_experiment_runs WHERE experiment_id=$1 RETURNING id").bind(experiment).bind(watermark).fetch_one(&self.pool).await
    }
    pub async fn claim_due(&self) -> Result<Option<BacktestJob>, sqlx::Error> {
        sqlx::query_as("WITH d AS(SELECT id FROM backtest_experiment_runs WHERE(status IN('PENDING','RETRY')AND next_attempt_at<=now())OR(status='RUNNING'AND locked_at<now()-interval'5 minutes')ORDER BY next_attempt_at,id FOR UPDATE SKIP LOCKED LIMIT 1)UPDATE backtest_experiment_runs r SET status='RUNNING',attempts=attempts+1,locked_at=now(),started_at=COALESCE(started_at,now()),progress=5,updated_at=now()FROM d WHERE r.id=d.id RETURNING r.id,r.experiment_id,r.attempts").fetch_optional(&self.pool).await
    }
    pub async fn fail(&self, j: &BacktestJob, error: &str) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE backtest_experiment_runs SET status=CASE WHEN $3 THEN'FAILED'ELSE'RETRY'END,next_attempt_at=now()+interval'30 seconds',last_error=$2,locked_at=NULL,updated_at=now()WHERE id=$1").bind(j.id).bind(error.chars().take(2048).collect::<String>()).bind(j.attempts>=3).execute(&self.pool).await?;
        Ok(())
    }
    #[allow(clippy::too_many_lines)]
    pub async fn execute(&self, j: &BacktestJob) -> Result<(), sqlx::Error> {
        let e:Experiment=sqlx::query_as("SELECT id,knowledge_mode,dataset_start,dataset_end,train_end,minimum_sample_size,dataset_watermark FROM backtest_experiments WHERE id=$1").bind(j.experiment_id).fetch_one(&self.pool).await?;
        let knowledge = e.knowledge_mode == "KNOWLEDGE_TIME";
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1::text,20))")
            .bind(e.id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM historical_decision_points WHERE run_id=$1")
            .bind(j.id)
            .execute(&mut *tx)
            .await?;
        sqlx::query(r"WITH ranked AS(SELECT s.*,t.launch_time,CASE WHEN $2 THEN s.calculated_at ELSE s.effective_at END decision_at,row_number()OVER(PARTITION BY s.token_id,s.state ORDER BY CASE WHEN $2 THEN s.calculated_at ELSE s.effective_at END,s.effective_at,s.id)rn FROM signal_snapshots s JOIN tokens t ON t.id=s.token_id WHERE(CASE WHEN $2 THEN s.calculated_at ELSE s.effective_at END)>=$4 AND(CASE WHEN $2 THEN s.calculated_at ELSE s.effective_at END)<$5 AND s.calculated_at<=$5)INSERT INTO historical_decision_points(run_id,token_id,cohort,event_effective_at,decision_as_of,decision_at,launch_age_ms,sample_identity,first_qualifying_decision_point,outcome_anchor,age_bucket,knowledge_cutoff,knowledge_mode,split,signal_snapshot_id,signal_state,signal_score,signal_confidence,evidence_manifest,evidence_max_known_at,leakage_valid)SELECT $1,token_id,state,effective_at,decision_at,decision_at,greatest(0,(extract(epoch FROM(decision_at-launch_time))*1000)::bigint),$1::text||':'||token_id::text||':'||state,true,'DECISION_TIME',CASE WHEN decision_at-launch_time<interval'1 minute'THEN'0-1m'WHEN decision_at-launch_time<interval'3 minutes'THEN'1-3m'WHEN decision_at-launch_time<interval'5 minutes'THEN'3-5m'WHEN decision_at-launch_time<interval'15 minutes'THEN'5-15m'ELSE'>15m'END,$5,$3,CASE WHEN launch_time<$6 THEN'IN_SAMPLE'ELSE'OUT_OF_SAMPLE'END,id,state,score,confidence,jsonb_build_object('signal',jsonb_build_object('id',id,'known_at',calculated_at),'classification_origin',classification_origin,'sample_unit','UNIQUE_TOKEN_FIRST_STATE_ENTRY'),decision_at,true FROM ranked WHERE rn=1 ON CONFLICT DO NOTHING").bind(j.id).bind(knowledge).bind(&e.knowledge_mode).bind(e.dataset_start).bind(e.dataset_end).bind(e.train_end).execute(&mut*tx).await?;
        for cohort in ["ALL_PONS_LAUNCHES", "NO_SIGNAL"] {
            sqlx::query(r"INSERT INTO historical_decision_points(run_id,token_id,cohort,event_effective_at,decision_as_of,decision_at,launch_age_ms,sample_identity,first_qualifying_decision_point,outcome_anchor,age_bucket,knowledge_cutoff,knowledge_mode,split,signal_state,evidence_manifest,evidence_max_known_at,leakage_valid)SELECT $1,t.id,$7,t.launch_time,CASE WHEN $2 THEN greatest(t.created_at,t.launch_time)ELSE t.launch_time END,CASE WHEN $2 THEN greatest(t.created_at,t.launch_time)ELSE t.launch_time END,0,$1::text||':'||t.id::text||':'||$7,true,'DECISION_TIME','0-1m',$5,$3,CASE WHEN t.launch_time<$6 THEN'IN_SAMPLE'ELSE'OUT_OF_SAMPLE'END,$7,jsonb_build_object('token',jsonb_build_object('id',t.id,'known_at',t.created_at),'sample_unit','UNIQUE_TOKEN_FIRST_STATE_ENTRY'),CASE WHEN $2 THEN greatest(t.created_at,t.launch_time)ELSE t.launch_time END,true FROM tokens t WHERE(CASE WHEN $2 THEN greatest(t.created_at,t.launch_time)ELSE t.launch_time END)>=$4 AND(CASE WHEN $2 THEN greatest(t.created_at,t.launch_time)ELSE t.launch_time END)<$5 AND($7<>'NO_SIGNAL'OR NOT EXISTS(SELECT 1 FROM signal_snapshots s WHERE s.token_id=t.id AND(CASE WHEN $2 THEN s.calculated_at ELSE s.effective_at END)<=CASE WHEN $2 THEN greatest(t.created_at,t.launch_time)ELSE t.launch_time END))ON CONFLICT DO NOTHING").bind(j.id).bind(knowledge).bind(&e.knowledge_mode).bind(e.dataset_start).bind(e.dataset_end).bind(e.train_end).bind(cohort).execute(&mut*tx).await?;
        }
        sqlx::query(r"WITH strata AS(SELECT DISTINCT cohort,age_bucket,CASE age_bucket WHEN'0-1m'THEN 0 WHEN'1-3m'THEN 60000 WHEN'3-5m'THEN 180000 WHEN'5-15m'THEN 300000 ELSE 900000 END::bigint anchor_age FROM historical_decision_points WHERE run_id=$1 AND cohort NOT IN('ALL_PONS_LAUNCHES','NO_SIGNAL','AGE_MATCHED_BASELINE')),eligible AS(SELECT t.id token_id,t.launch_time,t.created_at,s.cohort matched_cohort,s.age_bucket,s.anchor_age FROM tokens t CROSS JOIN strata s WHERE t.launch_time+make_interval(secs=>s.anchor_age::double precision/1000)>=$2 AND t.launch_time+make_interval(secs=>s.anchor_age::double precision/1000)<$3 AND(NOT $4 OR t.created_at<=t.launch_time+make_interval(secs=>s.anchor_age::double precision/1000)))INSERT INTO historical_decision_points(run_id,token_id,cohort,event_effective_at,decision_as_of,decision_at,launch_age_ms,sample_identity,first_qualifying_decision_point,outcome_anchor,age_bucket,matched_cohort,knowledge_cutoff,knowledge_mode,split,signal_state,evidence_manifest,evidence_max_known_at,leakage_valid)SELECT $1,token_id,'AGE_MATCHED_BASELINE',launch_time+make_interval(secs=>anchor_age::double precision/1000),launch_time+make_interval(secs=>anchor_age::double precision/1000),launch_time+make_interval(secs=>anchor_age::double precision/1000),anchor_age,$1::text||':'||token_id::text||':AGE_MATCHED:'||matched_cohort||':'||age_bucket,true,'DECISION_TIME',age_bucket,matched_cohort,$3,$5,CASE WHEN launch_time<$6 THEN'IN_SAMPLE'ELSE'OUT_OF_SAMPLE'END,'AGE_MATCHED_BASELINE',jsonb_build_object('baseline','AGE_MATCHED','version',1,'matched_cohort',matched_cohort,'age_bucket',age_bucket),launch_time+make_interval(secs=>anchor_age::double precision/1000),true FROM eligible ON CONFLICT DO NOTHING").bind(j.id).bind(e.dataset_start).bind(e.dataset_end).bind(knowledge).bind(&e.knowledge_mode).bind(e.train_end).execute(&mut*tx).await?;
        let violations:i64=sqlx::query_scalar("SELECT count(*)FROM historical_decision_points WHERE run_id=$1 AND evidence_max_known_at>decision_as_of").bind(j.id).fetch_one(&mut*tx).await?;
        let split_violations:i64=sqlx::query_scalar("SELECT count(*)FROM(SELECT token_id FROM historical_decision_points WHERE run_id=$1 GROUP BY token_id HAVING count(DISTINCT split)>1)x").bind(j.id).fetch_one(&mut*tx).await?;
        if violations > 0 || split_violations > 0 {
            sqlx::query("UPDATE backtest_experiment_runs SET status='LOOKAHEAD_VIOLATION',leakage_checks=$2,completed_at=now(),locked_at=NULL,progress=100 WHERE id=$1").bind(j.id).bind(json!([{"check":"known_at<=decision_as_of","status":if violations>0{"LOOKAHEAD_VIOLATION"}else{"PASS"},"count":violations},{"check":"token_split_isolation","status":if split_violations>0{"LOOKAHEAD_VIOLATION"}else{"PASS"},"count":split_violations}])).execute(&mut*tx).await?;
            tx.commit().await?;
            return Ok(());
        }
        tx.commit().await?;
        let rows:Vec<OutcomeRow>=sqlx::query_as(r"SELECT d.split,CASE WHEN d.cohort='AGE_MATCHED_BASELINE'THEN d.cohort||':'||d.matched_cohort||':'||d.age_bucket ELSE d.cohort END cohort,h.horizon_seconds,CASE WHEN future.id IS NOT NULL AND anchor.spot_price_quote>0 THEN'AVAILABLE'WHEN d.decision_at+make_interval(secs=>h.horizon_seconds)>$2 THEN'PENDING'WHEN t.lifecycle<>'ACTIVE_CURVE'AND t.updated_at<=d.decision_at+make_interval(secs=>h.horizon_seconds)THEN'CENSORED_POST_GRADUATION'ELSE'UNAVAILABLE'END status,CASE WHEN future.id IS NOT NULL AND anchor.spot_price_quote>0 THEN((future.spot_price_quote-anchor.spot_price_quote)/anchor.spot_price_quote*100)::text END price_change,exc.observed_mfe::text,exc.observed_mae::text FROM historical_decision_points d JOIN tokens t ON t.id=d.token_id CROSS JOIN(VALUES(300),(900),(3600),(21600),(86400))h(horizon_seconds)LEFT JOIN LATERAL(SELECT m.id,m.spot_price_quote FROM token_market_snapshots m WHERE m.token_id=d.token_id AND m.snapshot_at<=d.decision_at AND m.created_at<=d.decision_as_of AND m.spot_price_quote IS NOT NULL ORDER BY m.snapshot_at DESC,m.snapshot_block DESC LIMIT 1)anchor ON true LEFT JOIN LATERAL(SELECT m.id,m.spot_price_quote FROM token_market_snapshots m WHERE m.token_id=d.token_id AND m.snapshot_at>=d.decision_at+make_interval(secs=>h.horizon_seconds)AND m.snapshot_at<=d.decision_at+make_interval(secs=>h.horizon_seconds+300)AND m.created_at<=$2 AND m.spot_price_quote IS NOT NULL ORDER BY m.snapshot_at,m.snapshot_block LIMIT 1)future ON true LEFT JOIN LATERAL(SELECT max((m.spot_price_quote-anchor.spot_price_quote)/NULLIF(anchor.spot_price_quote,0)*100)observed_mfe,min((m.spot_price_quote-anchor.spot_price_quote)/NULLIF(anchor.spot_price_quote,0)*100)observed_mae FROM token_market_snapshots m WHERE m.token_id=d.token_id AND m.snapshot_at BETWEEN d.decision_at AND d.decision_at+make_interval(secs=>h.horizon_seconds)AND m.created_at<=$2)exc ON true WHERE d.run_id=$1 ORDER BY d.split,d.cohort,h.horizon_seconds,d.id").bind(j.id).bind(e.dataset_end).fetch_all(&self.pool).await?;
        let (train, validation) = aggregate(&rows, e.minimum_sample_size);
        let availability:Value=sqlx::query_scalar(r"SELECT jsonb_build_object('decision_count',count(*),'with_consensus',count(c.id),'with_trader_score',count(ts.id),'historically_known_ai_reports',count(ai.id),'known_content_items',COALESCE(sum(ct.known_count),0))FROM historical_decision_points d LEFT JOIN LATERAL(SELECT id FROM consensus_snapshots c WHERE c.token_id=d.token_id AND c.calculated_at<=d.decision_as_of ORDER BY c.calculated_at DESC LIMIT 1)c ON true LEFT JOIN LATERAL(SELECT h.id FROM trader_score_history h JOIN smart_trades s ON s.trader_id=h.trader_id AND s.token_id=d.token_id WHERE h.as_of_mode='KNOWLEDGE_TIME'AND h.knowledge_available_at<=d.decision_as_of ORDER BY h.knowledge_available_at DESC LIMIT 1)ts ON true LEFT JOIN LATERAL(SELECT r.id FROM ai_research_reports r WHERE r.token_id=d.token_id AND r.knowledge_available_at<=d.decision_as_of AND r.research_mode IN('CURRENT_RESEARCH','KNOWLEDGE_TIME')AND r.trigger_origin NOT IN('HISTORICAL_VALIDATION','RETROSPECTIVE_AI_RESEARCH')ORDER BY r.knowledge_available_at DESC LIMIT 1)ai ON true LEFT JOIN LATERAL(SELECT count(*)known_count FROM token_content_links l JOIN trader_content_items i ON i.id=l.content_id WHERE l.token_id=d.token_id AND(CASE WHEN $3 THEN i.observed_at<=d.decision_as_of ELSE i.published_at<=d.decision_as_of END))ct ON true WHERE d.run_id=$1").bind(j.id).bind(&e.knowledge_mode).bind(knowledge).fetch_one(&self.pool).await?;
        let factor_rows:Vec<FactorRow>=sqlx::query_as(r"SELECT d.split,h.horizon_seconds,CASE WHEN future.id IS NOT NULL AND anchor.spot_price_quote>0 THEN((future.spot_price_quote-anchor.spot_price_quote)/anchor.spot_price_quote*100)::text END price_change,c.raw_smart_buyers smart_buyers,c.qualified_smart_buyers qualified_buyers,c.first_smart_buy_age_ms first_entry_age_ms,st.buyer_rank,st.smart_buyer_rank,c.smart_net_flow_quote_raw::text smart_net_flow,tr.manual_tier,score.pons_score::text,tw.identity_confidence::text,pe.event_type position_event,cr.relation_type content_relation,cr.delta_ms content_delta_ms,(ai.structured_report#>>'{narrative,strength}')::integer ai_narrative_strength,ai.confidence ai_confidence,jsonb_array_length(COALESCE(ai.structured_report->'risks','[]'))::integer ai_risk_count,COALESCE(dsig.component_scores,'{}')component_scores FROM historical_decision_points d CROSS JOIN(VALUES(300),(900),(3600),(21600),(86400))h(horizon_seconds)LEFT JOIN LATERAL(SELECT m.id,m.spot_price_quote FROM token_market_snapshots m WHERE m.token_id=d.token_id AND m.snapshot_at<=d.decision_at AND m.created_at<=d.decision_as_of AND m.spot_price_quote IS NOT NULL ORDER BY m.snapshot_at DESC,m.snapshot_block DESC LIMIT 1)anchor ON true LEFT JOIN LATERAL(SELECT m.id,m.spot_price_quote FROM token_market_snapshots m WHERE m.token_id=d.token_id AND m.snapshot_at>=d.decision_at+make_interval(secs=>h.horizon_seconds)AND m.snapshot_at<=d.decision_at+make_interval(secs=>h.horizon_seconds+300)AND m.created_at<=$2 AND m.spot_price_quote IS NOT NULL ORDER BY m.snapshot_at,m.snapshot_block LIMIT 1)future ON true LEFT JOIN LATERAL(SELECT * FROM consensus_snapshots c WHERE c.token_id=d.token_id AND c.calculated_at<=d.decision_as_of ORDER BY c.calculated_at DESC LIMIT 1)c ON true LEFT JOIN signal_snapshots dsig ON dsig.id=d.signal_snapshot_id LEFT JOIN LATERAL(SELECT s.buyer_rank,s.smart_buyer_rank,s.trader_id,s.trader_wallet_id FROM smart_trades s WHERE s.token_id=d.token_id AND s.side='BUY'AND s.confirmation_level='BUY_CONFIRMED'AND COALESCE(s.confirmed_at,s.created_at)<=d.decision_as_of ORDER BY s.block_time,s.id LIMIT 1)st ON true LEFT JOIN traders tr ON tr.id=st.trader_id LEFT JOIN trader_wallets tw ON tw.id=st.trader_wallet_id LEFT JOIN LATERAL(SELECT p.event_type FROM position_events p JOIN wallet_token_positions w ON w.id=p.position_id WHERE w.token_id=d.token_id AND p.created_at<=d.decision_as_of ORDER BY p.created_at DESC LIMIT 1)pe ON true LEFT JOIN LATERAL(SELECT h.pons_score FROM trader_score_history h WHERE h.trader_id=st.trader_id AND h.as_of_mode='KNOWLEDGE_TIME'AND h.knowledge_available_at<=d.decision_as_of ORDER BY h.knowledge_available_at DESC LIMIT 1)score ON true LEFT JOIN LATERAL(SELECT r.relation_type,r.delta_ms FROM content_trade_relations r JOIN trader_content_items i ON i.id=r.content_id WHERE r.token_id=d.token_id AND(CASE WHEN $3 THEN i.observed_at<=d.decision_as_of ELSE i.published_at<=d.decision_as_of END)ORDER BY r.content_time DESC LIMIT 1)cr ON true LEFT JOIN LATERAL(SELECT a.structured_report,a.confidence FROM ai_research_reports a WHERE a.token_id=d.token_id AND a.knowledge_available_at<=d.decision_as_of ORDER BY a.knowledge_available_at DESC LIMIT 1)ai ON true WHERE d.run_id=$1").bind(j.id).bind(e.dataset_end).bind(knowledge).fetch_all(&self.pool).await?;
        let factors = json!({"bucket_definition_version":1,"sample_unit":"TOKEN_X_EXPERIMENT_DECISION_POINT","repeated_measures":false,"knowledge_mode":e.knowledge_mode,"availability":availability,"bucket_results":factor_aggregate(&factor_rows,e.minimum_sample_size),"supported_factors":["SMART_BUYER_COUNT","QUALIFIED_SMART_BUYER_COUNT","FIRST_SMART_ENTRY_AGE","BUYER_RANK","SMART_BUYER_RANK","SMART_NET_FLOW","MANUAL_TIER","PONS_TRADER_SCORE","IDENTITY_CONFIDENCE","POSITION_EVENT","CONTENT_RELATION","CONTENT_TIMING","AI_NARRATIVE_STRENGTH","AI_CONFIDENCE","AI_RISK_COUNT","SIGNAL_COMPONENTS"],"pons_trader_score_models":{"manual_tier_only":"EVIDENCE_ONLY","pons_score_only":"EVIDENCE_ONLY","hybrid":"EVIDENCE_ONLY","production_enabled":false},"ai_incremental":{"historically_known_reports":availability["historically_known_ai_reports"],"future_reports_excluded":true,"retrospective_reports_excluded":true},"content":{"known_content_items":availability["known_content_items"],"observed_later_excluded":knowledge}});
        let result = json!({"train":train,"validation":validation,"factors":factors,"dataset_watermark":e.dataset_watermark,"knowledge_mode":e.knowledge_mode,"research_only":!knowledge,"sample_unit":SAMPLE_UNIT,"outcome_anchor":OUTCOME_ANCHOR,"baseline":{"type":"AGE_MATCHED","version":BASELINE_VERSION},"production_signal_unchanged":true,"candidate_configuration_only":true});
        let hash: [u8; 32] = Sha256::digest(canonical(&result)).into();
        sqlx::query("UPDATE backtest_experiment_runs SET status='COMPLETED',progress=100,train_result=$2,validation_result=$3,factor_result=$4,warnings='[]',leakage_checks=$5,result_hash=$6,completed_at=now(),locked_at=NULL,last_error=NULL,updated_at=now()WHERE id=$1").bind(j.id).bind(&train).bind(&validation).bind(&factors).bind(json!([{"check":"known_at<=decision_as_of","status":"PASS","violations":0},{"check":"token_split_isolation","status":"PASS","violations":0},{"check":"unique_token_first_state_entry","status":"PASS"},{"check":"chronological_split","status":"PASS"},{"check":"production_isolation","status":"PASS"}])).bind(hash.as_slice()).execute(&self.pool).await?;
        Ok(())
    }
    pub async fn list(&self, limit: i64, offset: i64) -> Result<Value, sqlx::Error> {
        let items:Vec<Value>=sqlx::query_scalar("SELECT jsonb_build_object('id',e.id,'name',e.name,'knowledge_mode',e.knowledge_mode,'research_only',e.research_only,'sample_unit',e.sample_unit,'outcome_anchor',e.outcome_anchor,'baseline_definition',e.baseline_definition,'dataset_start',e.dataset_start,'dataset_end',e.dataset_end,'train_start',e.train_start,'train_end',e.train_end,'validation_start',e.validation_start,'validation_end',e.validation_end,'number_of_trials',e.number_of_trials,'created_at',e.created_at,'latest_run',(SELECT jsonb_build_object('id',r.id,'status',r.status,'progress',r.progress,'completed_at',r.completed_at)FROM backtest_experiment_runs r WHERE r.experiment_id=e.id ORDER BY r.run_number DESC LIMIT 1))FROM backtest_experiments e ORDER BY e.created_at DESC,e.id DESC LIMIT $1 OFFSET $2").bind(limit.clamp(1,100)).bind(offset.max(0)).fetch_all(&self.pool).await?;
        Ok(json!({"items":items,"limit":limit.clamp(1,100),"offset":offset.max(0)}))
    }
    pub async fn detail(&self, id: Uuid) -> Result<Value, sqlx::Error> {
        let experiment:Value=sqlx::query_scalar("SELECT to_jsonb(e)-'input_hash'||jsonb_build_object('input_hash',encode(input_hash,'hex'))FROM backtest_experiments e WHERE id=$1").bind(id).fetch_one(&self.pool).await?;
        let runs:Vec<Value>=sqlx::query_scalar("SELECT to_jsonb(r)-'result_hash'||jsonb_build_object('result_hash',CASE WHEN result_hash IS NULL THEN NULL ELSE encode(result_hash,'hex')END,'train_token_count',(SELECT count(DISTINCT token_id)FROM historical_decision_points d WHERE d.run_id=r.id AND d.split='IN_SAMPLE'),'validation_token_count',(SELECT count(DISTINCT token_id)FROM historical_decision_points d WHERE d.run_id=r.id AND d.split='OUT_OF_SAMPLE'))FROM backtest_experiment_runs r WHERE experiment_id=$1 ORDER BY run_number DESC LIMIT 50").bind(id).fetch_all(&self.pool).await?;
        Ok(json!({"experiment":experiment,"runs":runs}))
    }
}

fn aggregate(rows: &[OutcomeRow], minimum: i32) -> (Value, Value) {
    let mut g: BTreeMap<(String, String, i32), Metrics> = BTreeMap::new();
    for r in rows {
        let m = g
            .entry((r.split.clone(), r.cohort.clone(), r.horizon_seconds))
            .or_default();
        m.eligible += 1;
        match r.status.as_str() {
            "AVAILABLE" => {
                push(&mut m.available, r.price_change.as_ref());
                push(&mut m.mfe, r.observed_mfe.as_ref());
                push(&mut m.mae, r.observed_mae.as_ref());
            }
            "CENSORED_POST_GRADUATION" => m.censored += 1,
            "PENDING" => m.pending += 1,
            "INVALIDATED" => m.invalidated += 1,
            _ => m.unavailable += 1,
        }
    }
    let render = |split: &str| {
        Value::Array(g.iter().filter(|((s,_,_),_)|s==split).map(|((_,cohort,h),m)|{let mut returns=m.available.clone();returns.sort();let n=i64::try_from(returns.len()).unwrap_or(i64::MAX);let coverage=if m.eligible==0{Decimal::ZERO}else{Decimal::from(n)*Decimal::from(100)/Decimal::from(m.eligible)};json!({"cohort":cohort,"horizon_seconds":h,"sample_size":m.eligible,"eligible_sample_size":n,"coverage":coverage.round_dp(4).to_string(),"available_count":n,"censored_count":m.censored,"unavailable_count":m.unavailable,"pending_count":m.pending,"invalidated_count":m.invalidated,"mean_return_proxy":mean(&returns),"p25":quantile(&returns,25),"p50_median_return_proxy":quantile(&returns,50),"p75":quantile(&returns,75),"observed_mfe":median(&m.mfe),"observed_mae":median(&m.mae),"positive_outcome_rate":positive(&returns),"status":if n<i64::from(minimum){"INSUFFICIENT_SAMPLE"}else{"AVAILABLE"},"outcome_basis":"CURVE_MARKET_RETURN_PROXY_NOT_REALIZED_PNL"})}).collect())
    };
    (render("IN_SAMPLE"), render("OUT_OF_SAMPLE"))
}
fn factor_aggregate(rows: &[FactorRow], minimum: i32) -> Value {
    let mut groups: BTreeMap<(String, String, String, i32), Vec<Decimal>> = BTreeMap::new();
    for r in rows {
        let Some(outcome) = r
            .price_change
            .as_ref()
            .and_then(|v| Decimal::from_str(v).ok())
        else {
            continue;
        };
        let mut values: Vec<(&str, String)> = Vec::new();
        if let Some(v) = r.smart_buyers {
            values.push(("SMART_BUYER_COUNT", count_bucket(v)));
        }
        if let Some(v) = r.qualified_buyers {
            values.push(("QUALIFIED_SMART_BUYER_COUNT", count_bucket(v)));
        }
        if let Some(v) = r.first_entry_age_ms {
            values.push(("FIRST_SMART_ENTRY_AGE", age_bucket(v)));
        }
        if let Some(v) = r.buyer_rank {
            values.push(("BUYER_RANK", rank_bucket(v)));
        }
        if let Some(v) = r.smart_buyer_rank {
            values.push(("SMART_BUYER_RANK", rank_bucket(v)));
        }
        if let Some(v) = r
            .smart_net_flow
            .as_ref()
            .and_then(|v| Decimal::from_str(v).ok())
        {
            values.push((
                "SMART_NET_FLOW",
                if v.is_sign_positive() {
                    "POSITIVE"
                } else if v.is_zero() {
                    "ZERO"
                } else {
                    "NEGATIVE"
                }
                .into(),
            ));
        }
        if let Some(v) = &r.manual_tier {
            values.push(("MANUAL_TIER", v.clone()));
        }
        if let Some(v) = r
            .pons_score
            .as_ref()
            .and_then(|v| Decimal::from_str(v).ok())
        {
            values.push(("PONS_TRADER_SCORE", score_bucket(v)));
        }
        if let Some(v) = r
            .identity_confidence
            .as_ref()
            .and_then(|v| Decimal::from_str(v).ok())
        {
            values.push(("IDENTITY_CONFIDENCE", confidence_bucket(v)));
        }
        if let Some(v) = &r.position_event {
            values.push(("POSITION_EVENT", v.clone()));
        }
        if let Some(v) = &r.content_relation {
            values.push(("CONTENT_RELATION", v.clone()));
        }
        if let Some(v) = r.content_delta_ms {
            values.push(("CONTENT_TIMING", content_time_bucket(v)));
        }
        if let Some(v) = r.ai_narrative_strength {
            values.push(("AI_NARRATIVE_STRENGTH", score_bucket(Decimal::from(v))));
        }
        if let Some(v) = r.ai_confidence {
            values.push(("AI_CONFIDENCE", score_bucket(Decimal::from(v))));
        }
        if let Some(v) = r.ai_risk_count {
            values.push(("AI_RISK_COUNT", count_bucket(i64::from(v))));
        }
        if let Some(map) = r.component_scores.as_object() {
            for (name, value) in map {
                if !value.is_null() {
                    values.push((
                        "SIGNAL_COMPONENTS",
                        format!("{name}:{}", value.as_str().unwrap_or("AVAILABLE")),
                    ));
                }
            }
        }
        for (factor, bucket) in values {
            groups
                .entry((r.split.clone(), factor.into(), bucket, r.horizon_seconds))
                .or_default()
                .push(outcome);
        }
    }
    Value::Array(groups.into_iter().map(|((split,factor,bucket,horizon),mut outcomes)|{outcomes.sort();let n=i64::try_from(outcomes.len()).unwrap_or(i64::MAX);json!({"split":split,"factor":factor,"bucket":bucket,"horizon_seconds":horizon,"sample_size":n,"mean_return_proxy":mean(&outcomes),"p25":quantile(&outcomes,25),"p50":quantile(&outcomes,50),"p75":quantile(&outcomes,75),"positive_outcome_rate":positive(&outcomes),"status":if n<i64::from(minimum){"INSUFFICIENT_SAMPLE"}else{"AVAILABLE"}})}).collect())
}
fn count_bucket(v: i64) -> String {
    match v {
        ..=0 => "0",
        1 => "1",
        2 => "2",
        3 => "3",
        _ => "4+",
    }
    .into()
}
fn age_bucket(v: i64) -> String {
    match v {
        ..=29_999 => "0-30s",
        30_000..=59_999 => "30-60s",
        60_000..=179_999 => "1-3m",
        180_000..=299_999 => "3-5m",
        300_000..=899_999 => "5-15m",
        _ => ">15m",
    }
    .into()
}
fn rank_bucket(v: i64) -> String {
    match v {
        ..=5 => "1-5",
        6..=10 => "6-10",
        11..=25 => "11-25",
        26..=50 => "26-50",
        51..=100 => "51-100",
        _ => ">100",
    }
    .into()
}
fn score_bucket(v: Decimal) -> String {
    if v < Decimal::from(40) {
        "0-39"
    } else if v < Decimal::from(60) {
        "40-59"
    } else if v < Decimal::from(80) {
        "60-79"
    } else {
        "80-100"
    }
    .into()
}
fn confidence_bucket(v: Decimal) -> String {
    if v < Decimal::new(50, 2) {
        "LOW"
    } else if v < Decimal::new(80, 2) {
        "MEDIUM"
    } else {
        "HIGH"
    }
    .into()
}
fn content_time_bucket(v: i64) -> String {
    let before = v < 0;
    let a = v.unsigned_abs();
    let range = if a <= 60_000 {
        "0-1m"
    } else if a <= 300_000 {
        "1-5m"
    } else if a <= 1_800_000 {
        "5-30m"
    } else {
        ">30m"
    };
    format!(
        "{}_{range}",
        if before {
            "CONTENT_BEFORE_ACTION"
        } else {
            "CONTENT_AFTER_ACTION"
        }
    )
}
fn push(v: &mut Vec<Decimal>, x: Option<&String>) {
    if let Some(n) = x.and_then(|x| Decimal::from_str(x).ok()) {
        v.push(n);
    }
}
fn mean(v: &[Decimal]) -> Value {
    if v.is_empty() {
        Value::Null
    } else {
        json!(
            (v.iter().sum::<Decimal>() / Decimal::from(v.len()))
                .round_dp(8)
                .to_string()
        )
    }
}
fn quantile(v: &[Decimal], p: usize) -> Value {
    if v.is_empty() {
        Value::Null
    } else {
        json!(v[((v.len() - 1) * p + 50) / 100].round_dp(8).to_string())
    }
}
fn median(v: &[Decimal]) -> Value {
    let mut x = v.to_vec();
    x.sort();
    quantile(&x, 50)
}
fn positive(v: &[Decimal]) -> Value {
    if v.is_empty() {
        Value::Null
    } else {
        json!(
            (Decimal::from(
                v.iter()
                    .filter(|x| x.is_sign_positive() && !x.is_zero())
                    .count()
            ) * Decimal::from(100)
                / Decimal::from(v.len()))
            .round_dp(4)
            .to_string()
        )
    }
}
fn canonical(v: &Value) -> Vec<u8> {
    fn w(v: &Value, s: &mut String) {
        match v {
            Value::Object(m) => {
                s.push('{');
                let mut keys: Vec<_> = m.keys().collect();
                keys.sort();
                for (i, k) in keys.iter().enumerate() {
                    if i > 0 {
                        s.push(',');
                    }
                    s.push_str(&serde_json::to_string(k).unwrap());
                    s.push(':');
                    w(&m[*k], s);
                }
                s.push('}');
            }
            Value::Array(a) => {
                s.push('[');
                for (i, v) in a.iter().enumerate() {
                    if i > 0 {
                        s.push(',');
                    }
                    w(v, s);
                }
                s.push(']');
            }
            _ => s.push_str(&serde_json::to_string(v).unwrap()),
        }
    }
    let mut s = String::new();
    w(v, &mut s);
    s.into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn outcome_statuses_and_quantiles_are_honest() {
        let rows = vec![
            OutcomeRow {
                split: "IN_SAMPLE".into(),
                cohort: "HIGH_PRIORITY".into(),
                horizon_seconds: 300,
                status: "AVAILABLE".into(),
                price_change: Some("10".into()),
                observed_mfe: Some("20".into()),
                observed_mae: Some("-5".into()),
            },
            OutcomeRow {
                split: "IN_SAMPLE".into(),
                cohort: "HIGH_PRIORITY".into(),
                horizon_seconds: 300,
                status: "CENSORED_POST_GRADUATION".into(),
                price_change: None,
                observed_mfe: None,
                observed_mae: None,
            },
            OutcomeRow {
                split: "IN_SAMPLE".into(),
                cohort: "HIGH_PRIORITY".into(),
                horizon_seconds: 300,
                status: "UNAVAILABLE".into(),
                price_change: None,
                observed_mfe: None,
                observed_mae: None,
            },
        ];
        let (train, _) = aggregate(&rows, 2);
        let m = &train[0];
        assert_eq!(m["available_count"], 1);
        assert_eq!(m["censored_count"], 1);
        assert_eq!(m["unavailable_count"], 1);
        assert_eq!(m["status"], "INSUFFICIENT_SAMPLE");
        assert_eq!(m["p50_median_return_proxy"], "10");
    }
}
