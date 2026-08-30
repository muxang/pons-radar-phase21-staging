use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

use super::{EventOutboxRepository, NewOutboxEvent};

#[derive(Clone, Debug)]
pub struct SignalRepository {
    pool: PgPool,
}
#[derive(Clone, Debug, FromRow)]
pub struct SignalJob {
    pub token_id: Uuid,
    pub generation: i64,
    pub attempts: i32,
    pub trigger_effective_at: Option<DateTime<Utc>>,
    pub trigger_origin: String,
    pub trigger_realtime_eligible: bool,
}
#[derive(Clone, Debug, FromRow)]
pub struct SignalSmartTrade {
    pub id: Uuid,
    pub trader_id: Uuid,
    pub wallet_address: Vec<u8>,
    pub side: String,
    pub quote_amount_raw: String,
    pub block_time: DateTime<Utc>,
    pub launch_age_ms: Option<i64>,
    pub buyer_rank: Option<i64>,
    pub smart_buyer_rank: Option<i64>,
    pub classification_source: String,
    pub realtime_alert_eligible: bool,
    pub identity_confidence: String,
    pub manual_tier: Option<String>,
    pub chain_status: String,
}
#[derive(Clone, Debug, FromRow, serde::Serialize)]
pub struct SignalMarketSnapshot {
    pub effective_at: DateTime<Utc>,
    pub snapshot_kind: String,
    pub unique_buyers: i64,
    pub unique_sellers: i64,
    pub buy_count: i64,
    pub sell_count: i64,
    pub user_net_flow_raw: String,
    pub curve_effective_net_flow_raw: String,
    pub holder_count: i64,
    pub curve_progress: Option<String>,
    pub state_scope: String,
    pub integrity_status: String,
}
#[derive(Clone, Debug, FromRow)]
pub struct SignalPositionEvent {
    pub event_type: String,
    pub block_time: DateTime<Utc>,
    pub classification_source: String,
    pub wallet_address: Vec<u8>,
}
#[derive(Clone, Debug)]
pub struct SignalInput {
    pub launch_time: DateTime<Utc>,
    pub trades: Vec<SignalSmartTrade>,
    pub market: Vec<SignalMarketSnapshot>,
    pub positions: Vec<SignalPositionEvent>,
}

pub struct ConsensusWrite {
    pub effective_at: DateTime<Utc>,
    pub window_seconds: i32,
    pub raw: i64,
    pub qualified: i64,
    pub independent: i64,
    pub buy_raw: String,
    pub sell_raw: String,
    pub net_raw: String,
    pub first_age: Option<i64>,
    pub median_age: Option<i64>,
    pub open: i64,
    pub add: i64,
    pub reduce: i64,
    pub close: i64,
    pub wallet_exit: String,
    pub quote_exit: String,
    pub weighted: String,
    pub timing: Value,
    pub rank: Value,
    pub position: Value,
    pub inputs: Value,
    pub hash: [u8; 32],
    pub origin: String,
    pub realtime: bool,
    pub finality: String,
}
pub struct SignalWrite {
    pub effective_at: DateTime<Utc>,
    pub state: String,
    pub score: String,
    pub confidence: String,
    pub component_scores: Value,
    pub component_inputs: Value,
    pub component_confidence: Value,
    pub weights: Value,
    pub reasons: Value,
    pub negatives: Value,
    pub rules: Value,
    pub hash: [u8; 32],
    pub origin: String,
    pub realtime: bool,
    pub finality: String,
}
pub struct TransitionWrite {
    pub signal_index: usize,
    pub effective_at: DateTime<Utc>,
    pub from: String,
    pub to: String,
    pub score: String,
    pub confidence: String,
    pub reasons: Value,
    pub rules: Value,
    pub origin: String,
    pub realtime: bool,
}
pub struct SignalRebuild {
    pub consensus: Vec<ConsensusWrite>,
    pub signals: Vec<SignalWrite>,
    pub transitions: Vec<TransitionWrite>,
    pub rule_version: i32,
    pub weight_version: i32,
    pub calculation_version: i32,
}

#[allow(clippy::missing_errors_doc)]
impl SignalRepository {
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
    pub async fn activate_rule_set(
        &self,
        rule_version: i32,
        weight_version: i32,
        calculation_version: i32,
        config: &Value,
    ) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("UPDATE signal_rule_sets SET active=false WHERE active")
            .execute(&mut *tx)
            .await?;
        let id:Uuid=sqlx::query_scalar("INSERT INTO signal_rule_sets(version,weight_version,rule_version,calculation_version,config,active)VALUES($1,$2,$3,$4,$5,true)ON CONFLICT(version)DO UPDATE SET weight_version=EXCLUDED.weight_version,rule_version=EXCLUDED.rule_version,calculation_version=EXCLUDED.calculation_version,config=EXCLUDED.config,active=true RETURNING id").bind(calculation_version).bind(weight_version).bind(rule_version).bind(calculation_version).bind(config).fetch_one(&mut*tx).await?;
        for (rule, description) in [
            (
                "HIGH_PRIORITY_CONSENSUS",
                "Qualified independent consensus with positive flow and sufficient confidence",
            ),
            (
                "SMART_DISTRIBUTION",
                "Smart wallet and quote exit evidence crosses the distribution threshold",
            ),
        ] {
            sqlx::query("INSERT INTO signal_rules(rule_set_id,rule_id,rule_version,thresholds,description)VALUES($1,$2,$3,$4,$5)ON CONFLICT(rule_set_id,rule_id)DO UPDATE SET rule_version=EXCLUDED.rule_version,thresholds=EXCLUDED.thresholds,description=EXCLUDED.description").bind(id).bind(rule).bind(rule_version).bind(config).bind(description).execute(&mut*tx).await?;
        }
        tx.commit().await
    }
    pub async fn claim_due(&self) -> Result<Option<SignalJob>, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let row=sqlx::query_as("WITH d AS(SELECT token_id FROM signal_rebuild_jobs WHERE(status IN('PENDING','RETRY')AND next_attempt_at<=now())OR(status='PROCESSING'AND locked_at<now()-interval '5 minutes')ORDER BY next_attempt_at FOR UPDATE SKIP LOCKED LIMIT 1)UPDATE signal_rebuild_jobs j SET status='PROCESSING',attempts=attempts+1,locked_at=now(),updated_at=now()FROM d WHERE j.token_id=d.token_id RETURNING j.token_id,j.generation,j.attempts,j.trigger_effective_at,j.trigger_origin,j.trigger_realtime_eligible").fetch_optional(&mut*tx).await?;
        tx.commit().await?;
        Ok(row)
    }
    pub async fn retry(
        &self,
        j: &SignalJob,
        error: &str,
        next: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE signal_rebuild_jobs SET status='RETRY',next_attempt_at=$2,last_error=$3,locked_at=NULL,updated_at=now()WHERE token_id=$1").bind(j.token_id).bind(next).bind(error.chars().take(2048).collect::<String>()).execute(&self.pool).await?;
        Ok(())
    }
    pub async fn load(&self, token: Uuid) -> Result<SignalInput, sqlx::Error> {
        let launch_time = sqlx::query_scalar("SELECT launch_time FROM tokens WHERE id=$1")
            .bind(token)
            .fetch_one(&self.pool)
            .await?;
        let trades=sqlx::query_as(r"SELECT st.id,st.trader_id,st.wallet_address,st.side,st.quote_amount_raw,st.block_time,st.launch_age_ms,st.buyer_rank,st.smart_buyer_rank,st.classification_source,st.realtime_alert_eligible,
   COALESCE(st.identity_snapshot#>>'{tracked_wallet_match,identity_confidence}',st.identity_snapshot#>>'{identity,identity_confidence}',tw.identity_confidence::text,'0') identity_confidence,tr.manual_tier,tt.status chain_status
   FROM smart_trades st JOIN token_trades tt ON tt.id=st.token_trade_id JOIN traders tr ON tr.id=st.trader_id LEFT JOIN trader_wallets tw ON tw.id=st.trader_wallet_id
   WHERE st.token_id=$1 AND st.confirmation_level IN('BUY_CONFIRMED','SELL_CONFIRMED')AND tt.status<>'ORPHANED'ORDER BY st.block_time,tt.block_number,COALESCE(tt.transaction_index,tt.log_index),tt.log_index,st.id").bind(token).fetch_all(&self.pool).await?;
        let market=sqlx::query_as(r"SELECT s.snapshot_at effective_at,s.snapshot_kind,s.unique_buyers,s.unique_sellers,s.buy_count,s.sell_count,s.user_net_flow_raw::text,s.curve_effective_net_flow_raw::text,s.holder_count,s.curve_progress::text,s.state_scope,m.integrity_status FROM token_market_snapshots s JOIN token_market_state m ON m.token_id=s.token_id WHERE s.token_id=$1 ORDER BY s.snapshot_at,s.snapshot_block,s.id").bind(token).fetch_all(&self.pool).await?;
        let positions=sqlx::query_as("SELECT p.event_type,p.block_time,p.classification_source,w.wallet_address FROM position_events p JOIN wallet_token_positions w ON w.id=p.position_id WHERE w.token_id=$1 ORDER BY p.block_time,p.block_number,COALESCE(p.transaction_index,p.log_index),p.log_index,p.id").bind(token).fetch_all(&self.pool).await?;
        Ok(SignalInput {
            launch_time,
            trades,
            market,
            positions,
        })
    }
    #[allow(clippy::too_many_lines)]
    pub async fn persist(&self, j: &SignalJob, r: &SignalRebuild) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1::text,11))")
            .bind(j.token_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE consensus_snapshots SET current_generation=false WHERE token_id=$1 AND current_generation")
            .bind(j.token_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE signal_snapshots SET current_generation=false WHERE token_id=$1 AND current_generation")
            .bind(j.token_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE signal_transitions SET current_generation=false WHERE token_id=$1 AND current_generation")
            .bind(j.token_id)
            .execute(&mut *tx)
            .await?;
        for v in &r.consensus {
            sqlx::query("INSERT INTO consensus_snapshots(token_id,rebuild_generation,effective_at,window_seconds,raw_smart_buyers,qualified_smart_buyers,independent_smart_buyers,smart_buy_volume_quote_raw,smart_sell_volume_quote_raw,smart_net_flow_quote_raw,first_smart_buy_age_ms,median_smart_buy_age_ms,smart_open_count,smart_add_count,smart_reduce_count,smart_close_count,wallet_exit_ratio,quote_exit_ratio,weighted_smart_consensus,timing_component,rank_component,position_component,rule_version,calculation_version,inputs,content_hash,classification_origin,realtime_alert_eligible,signal_finality)VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::numeric,$11,$12,$13,$14,$15,$16,$17::numeric,$18::numeric,$19::numeric,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29)").bind(j.token_id).bind(j.generation).bind(v.effective_at).bind(v.window_seconds).bind(v.raw).bind(v.qualified).bind(v.independent).bind(&v.buy_raw).bind(&v.sell_raw).bind(&v.net_raw).bind(v.first_age).bind(v.median_age).bind(v.open).bind(v.add).bind(v.reduce).bind(v.close).bind(&v.wallet_exit).bind(&v.quote_exit).bind(&v.weighted).bind(&v.timing).bind(&v.rank).bind(&v.position).bind(r.rule_version).bind(r.calculation_version).bind(&v.inputs).bind(v.hash.as_slice()).bind(&v.origin).bind(v.realtime).bind(&v.finality).execute(&mut*tx).await?;
        }
        let mut ids = Vec::with_capacity(r.signals.len());
        for v in &r.signals {
            let id:Uuid=sqlx::query_scalar("INSERT INTO signal_snapshots(token_id,rebuild_generation,effective_at,state,score,confidence,component_scores,component_inputs,component_confidence,applied_weights,reason_codes,negative_reasons,matched_rules,rule_version,weight_version,calculation_version,content_hash,classification_origin,realtime_alert_eligible,signal_finality)VALUES($1,$2,$3,$4,$5::numeric,$6::numeric,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)RETURNING id").bind(j.token_id).bind(j.generation).bind(v.effective_at).bind(&v.state).bind(&v.score).bind(&v.confidence).bind(&v.component_scores).bind(&v.component_inputs).bind(&v.component_confidence).bind(&v.weights).bind(&v.reasons).bind(&v.negatives).bind(&v.rules).bind(r.rule_version).bind(r.weight_version).bind(r.calculation_version).bind(v.hash.as_slice()).bind(&v.origin).bind(v.realtime).bind(&v.finality).fetch_one(&mut*tx).await?;
            ids.push(id);
        }
        for v in &r.transitions {
            let snapshot = ids[v.signal_index];
            sqlx::query("INSERT INTO signal_transitions(token_id,rebuild_generation,signal_snapshot_id,effective_at,from_state,to_state,score,confidence,reason_codes,matched_rules,classification_origin,realtime_alert_eligible)VALUES($1,$2,$3,$4,$5,$6,$7::numeric,$8::numeric,$9,$10,$11,$12)").bind(j.token_id).bind(j.generation).bind(snapshot).bind(v.effective_at).bind(&v.from).bind(&v.to).bind(&v.score).bind(&v.confidence).bind(&v.reasons).bind(&v.rules).bind(&v.origin).bind(v.realtime).execute(&mut*tx).await?;
            if v.realtime {
                let event_type = match v.to.as_str() {
                    "WATCH" => "signal.watch",
                    "STRONG_WATCH" => "signal.strong_watch",
                    "HIGH_PRIORITY" => "signal.high_priority",
                    "COOLING" => "signal.cooling",
                    "DISTRIBUTION" => "signal.distribution",
                    _ => "signal.state_changed",
                };
                let dedupe = format!(
                    "signal:{}:{}:{}:v1",
                    j.token_id,
                    v.effective_at.timestamp_micros(),
                    v.to
                );
                let payload = serde_json::json!({"token_id":j.token_id,"from":v.from,"to":v.to,"score":v.score,"confidence":v.confidence,"event_effective_at":v.effective_at,"calculated_at":Utc::now(),"realtime_alert_eligible":true});
                EventOutboxRepository::append_in_transaction(
                    &mut tx,
                    &NewOutboxEvent {
                        event_type,
                        schema_version: 1,
                        aggregate_type: Some("token"),
                        aggregate_id: Some(j.token_id),
                        dedupe_key: &dedupe,
                        payload: &payload,
                    },
                )
                .await?;
            }
        }
        if let Some((v, id)) = r.signals.last().zip(ids.last()) {
            sqlx::query("INSERT INTO current_signal_states(token_id,state,score,confidence,effective_at,rebuild_generation,signal_snapshot_id)VALUES($1,$2,$3::numeric,$4::numeric,$5,$6,$7)ON CONFLICT(token_id)DO UPDATE SET state=EXCLUDED.state,score=EXCLUDED.score,confidence=EXCLUDED.confidence,effective_at=EXCLUDED.effective_at,rebuild_generation=EXCLUDED.rebuild_generation,signal_snapshot_id=EXCLUDED.signal_snapshot_id,updated_at=now()")
                .bind(j.token_id)
                .bind(&v.state)
                .bind(&v.score)
                .bind(&v.confidence)
                .bind(v.effective_at)
                .bind(j.generation)
                .bind(id)
                .execute(&mut *tx)
                .await?;
        } else {
            sqlx::query("DELETE FROM current_signal_states WHERE token_id = $1")
                .bind(j.token_id)
                .execute(&mut *tx)
                .await?;
        }
        sqlx::query("UPDATE signal_rebuild_jobs SET status=CASE WHEN generation=$2 THEN'COMPLETED'ELSE'PENDING'END,locked_at=NULL,last_error=NULL,updated_at=now()WHERE token_id=$1").bind(j.token_id).bind(j.generation).execute(&mut*tx).await?;
        tx.commit().await
    }
}
