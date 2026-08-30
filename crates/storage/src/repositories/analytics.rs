#![allow(clippy::needless_raw_string_hashes)]
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

pub const TRADER_ANALYTICS_CALCULATION_VERSION: i32 = 1;
pub const TRADER_SCORE_RULE_VERSION: i32 = 1;
pub const TRADER_SCORE_WEIGHT_VERSION: i32 = 1;
const HORIZONS: [i32; 5] = [300, 900, 3600, 21600, 86400];

#[derive(Clone, Debug)]
pub struct TraderAnalyticsRepository {
    pool: PgPool,
}
#[derive(Clone, Debug, FromRow)]
pub struct TraderAnalyticsJob {
    pub trader_id: Uuid,
    pub generation: i64,
    pub attempts: i32,
}
#[derive(Clone, Debug)]
pub struct ScoreResult {
    pub score: Decimal,
    pub confidence: Decimal,
    pub sample_size: i32,
    pub effective_at: DateTime<Utc>,
    pub components: Value,
    pub inputs: Value,
}

#[derive(FromRow)]
struct Stats {
    episodes: i64,
    early: Option<Decimal>,
    rank: Option<Decimal>,
    outcome: Option<Decimal>,
    adds: Option<Decimal>,
    hold: Option<Decimal>,
    matured: i64,
    pending: i64,
    censored: i64,
    identity: Option<Decimal>,
    launches: i64,
    entries_30d: i64,
}

#[allow(clippy::missing_errors_doc)]
impl TraderAnalyticsRepository {
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
    pub async fn claim_due(&self) -> Result<Option<TraderAnalyticsJob>, sqlx::Error> {
        sqlx::query_as("WITH d AS(SELECT trader_id FROM trader_analytics_jobs WHERE(status IN('PENDING','RETRY')AND next_attempt_at<=now())OR(status='PROCESSING'AND locked_at<now()-interval '5 minutes')ORDER BY next_attempt_at,trader_id FOR UPDATE SKIP LOCKED LIMIT 1)UPDATE trader_analytics_jobs j SET status='PROCESSING',attempts=attempts+1,locked_at=now(),updated_at=now()FROM d WHERE j.trader_id=d.trader_id RETURNING j.trader_id,j.generation,j.attempts").fetch_optional(&self.pool).await
    }
    pub async fn enqueue_matured(&self, now: DateTime<Utc>) -> Result<u64, sqlx::Error> {
        Ok(sqlx::query("INSERT INTO trader_analytics_jobs(trader_id)SELECT DISTINCT e.trader_id FROM trader_position_episodes e JOIN trader_episode_outcomes o ON o.episode_id=e.id WHERE o.status='PENDING'AND o.target_time<=$1 ON CONFLICT(trader_id)DO UPDATE SET generation=trader_analytics_jobs.generation+1,status='PENDING',next_attempt_at=now(),locked_at=NULL,last_error=NULL,updated_at=now()")
            .bind(now).execute(&self.pool).await?.rows_affected())
    }
    pub async fn retry(
        &self,
        j: &TraderAnalyticsJob,
        error: &str,
        next: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE trader_analytics_jobs SET status='RETRY',next_attempt_at=$2,last_error=$3,locked_at=NULL,updated_at=now()WHERE trader_id=$1").bind(j.trader_id).bind(next).bind(error.chars().take(2048).collect::<String>()).execute(&self.pool).await?;
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    pub async fn rebuild(
        &self,
        j: &TraderAnalyticsJob,
        now: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1::text,16))")
            .bind(j.trader_id)
            .execute(&mut *tx)
            .await?;
        let generation: i64 = sqlx::query_scalar(
            "SELECT generation FROM trader_analytics_jobs WHERE trader_id=$1 FOR UPDATE",
        )
        .bind(j.trader_id)
        .fetch_one(&mut *tx)
        .await?;
        sqlx::query("DELETE FROM trader_position_episodes WHERE trader_id=$1")
            .bind(j.trader_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query(r#"WITH ordered AS(SELECT p.*,w.trader_id,w.trader_wallet_id,w.token_id,sum((p.event_type='CLOSE_POSITION')::int)OVER(PARTITION BY w.trader_wallet_id,w.token_id ORDER BY p.block_number,COALESCE(p.transaction_index,p.log_index),p.log_index ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) prior_closes FROM position_events p JOIN wallet_token_positions w ON w.id=p.position_id WHERE w.trader_id=$1 AND p.event_type<>'POSITION_INTEGRITY_WARNING'),grouped AS(SELECT *,COALESCE(prior_closes,0)+1 episode_number FROM ordered),agg AS(SELECT trader_id,trader_wallet_id,token_id,episode_number,min(block_time)opened_at,max(block_time)FILTER(WHERE event_type='CLOSE_POSITION')closed_at,(array_agg(smart_trade_id ORDER BY block_number,COALESCE(transaction_index,log_index),log_index))[1]opening_trade,(array_agg(smart_trade_id ORDER BY block_number DESC,COALESCE(transaction_index,log_index)DESC,log_index DESC)FILTER(WHERE event_type='CLOSE_POSITION'))[1]closing_trade,sum(quote_amount_raw::numeric)FILTER(WHERE side='BUY')buy_quote,sum(quote_amount_raw::numeric)FILTER(WHERE side='SELL')sell_quote,count(*)FILTER(WHERE event_type='ADD_POSITION')adds,count(*)FILTER(WHERE event_type='REDUCE_POSITION')reduces,jsonb_agg(DISTINCT classification_source)sources,bool_or(warning IS NOT NULL)warning FROM grouped GROUP BY trader_id,trader_wallet_id,token_id,episode_number)INSERT INTO trader_position_episodes(trader_id,trader_wallet_id,token_id,episode_number,opened_at,closed_at,opening_smart_trade_id,closing_smart_trade_id,first_entry_price,first_entry_age_ms,first_buyer_rank,first_smart_buyer_rank,initial_buy_quote_raw,total_buy_quote_raw,total_sell_quote_raw,add_count,reduce_count,classification_sources,position_integrity,calculation_version,current_generation)SELECT a.trader_id,a.trader_wallet_id,a.token_id,a.episode_number,a.opened_at,a.closed_at,a.opening_trade,a.closing_trade,s.entry_price_quote,s.launch_age_ms,s.buyer_rank,s.smart_buyer_rank,s.quote_amount_raw,COALESCE(a.buy_quote,0)::text,COALESCE(a.sell_quote,0)::text,a.adds,a.reduces,a.sources,CASE WHEN a.warning THEN'POSITION_INTEGRITY_WARNING'ELSE'OK'END,$2,$3 FROM agg a JOIN smart_trades s ON s.id=a.opening_trade"#).bind(j.trader_id).bind(TRADER_ANALYTICS_CALCULATION_VERSION).bind(generation).execute(&mut*tx).await?;
        sqlx::query("UPDATE trader_position_episodes e SET knowledge_available_at=x.known_at FROM(SELECT e2.id,max(COALESCE(s.confirmed_at,s.created_at))known_at FROM trader_position_episodes e2 JOIN position_events p ON p.block_time BETWEEN e2.opened_at AND COALESCE(e2.closed_at,'infinity')JOIN wallet_token_positions w ON w.id=p.position_id AND w.trader_wallet_id=e2.trader_wallet_id AND w.token_id=e2.token_id JOIN smart_trades s ON s.id=p.smart_trade_id WHERE e2.trader_id=$1 GROUP BY e2.id)x WHERE e.id=x.id").bind(j.trader_id).execute(&mut*tx).await?;
        sqlx::query("WITH h AS(SELECT e.id,count(p.id)n,percentile_cont(0.5)WITHIN GROUP(ORDER BY p.initial_buy_quote_raw::numeric)median FROM trader_position_episodes e LEFT JOIN trader_position_episodes p ON p.trader_id=e.trader_id AND p.opened_at<e.opened_at WHERE e.trader_id=$1 GROUP BY e.id)UPDATE trader_position_episodes e SET relative_size_history_samples=h.n,relative_initial_size=CASE WHEN h.median>0 THEN e.initial_buy_quote_raw::numeric/h.median END,relative_size_confidence=least(100,h.n*10)FROM h WHERE e.id=h.id").bind(j.trader_id).execute(&mut*tx).await?;
        for horizon in HORIZONS {
            sqlx::query(r#"INSERT INTO trader_episode_outcomes(episode_id,horizon_seconds,target_time,observation_time,observation_block,entry_price,observed_price,price_change,status,evidence_scope,evidence,calculation_version,available_at)SELECT e.id,$2,e.opened_at+make_interval(secs=>$2),o.snapshot_at,o.snapshot_block,e.first_entry_price,o.spot_price_quote,CASE WHEN e.first_entry_price>0 AND o.spot_price_quote IS NOT NULL THEN(o.spot_price_quote-e.first_entry_price)/e.first_entry_price*100 END,CASE WHEN o.id IS NOT NULL AND e.first_entry_price>0 THEN'AVAILABLE'WHEN $3<e.opened_at+make_interval(secs=>$2)THEN'PENDING'WHEN t.lifecycle<>'ACTIVE_CURVE'AND t.updated_at<=e.opened_at+make_interval(secs=>$2)THEN'CENSORED_POST_GRADUATION'ELSE'UNAVAILABLE'END,CASE WHEN o.state_scope='BLOCK_STATE_EXACT'THEN'BLOCK_STATE_EXACT'ELSE'UNAVAILABLE'END,jsonb_build_object('time_basis','EPISODE_ENTRY_TIME','selection','FIRST_SNAPSHOT_AT_OR_AFTER_TARGET_WITHIN_5_MINUTES','snapshot_id',o.id,'token_lifecycle',t.lifecycle),$4,COALESCE(o.snapshot_at,e.opened_at+make_interval(secs=>$2))FROM trader_position_episodes e JOIN tokens t ON t.id=e.token_id LEFT JOIN LATERAL(SELECT *FROM token_market_snapshots m WHERE m.token_id=e.token_id AND m.snapshot_at>=e.opened_at+make_interval(secs=>$2)AND m.snapshot_at<=e.opened_at+make_interval(secs=>$2+300)ORDER BY m.snapshot_at,m.snapshot_block LIMIT 1)o ON true WHERE e.trader_id=$1"#).bind(j.trader_id).bind(horizon).bind(now).bind(TRADER_ANALYTICS_CALCULATION_VERSION).execute(&mut*tx).await?;
        }
        sqlx::query("UPDATE trader_episode_outcomes o SET available_at=greatest(o.target_time,m.created_at),evidence=o.evidence||jsonb_build_object('market_evidence_known_at',m.created_at)FROM token_market_snapshots m WHERE o.evidence->>'snapshot_id'=m.id::text AND o.episode_id IN(SELECT id FROM trader_position_episodes WHERE trader_id=$1)").bind(j.trader_id).execute(&mut*tx).await?;
        sqlx::query(r#"INSERT INTO trader_episode_excursions(episode_id,observed_mfe,observed_mae,window_seconds,snapshot_count,calculation_version)SELECT e.id,max((m.spot_price_quote-e.first_entry_price)/NULLIF(e.first_entry_price,0)*100),min((m.spot_price_quote-e.first_entry_price)/NULLIF(e.first_entry_price,0)*100),86400,count(m.id),$2 FROM trader_position_episodes e LEFT JOIN token_market_snapshots m ON m.token_id=e.token_id AND m.snapshot_at BETWEEN e.opened_at AND LEAST(COALESCE(e.closed_at,e.opened_at+interval'24 hours'),e.opened_at+interval'24 hours')WHERE e.trader_id=$1 GROUP BY e.id"#).bind(j.trader_id).bind(TRADER_ANALYTICS_CALCULATION_VERSION).execute(&mut*tx).await?;
        sqlx::query("UPDATE trader_score_history SET current_generation=false WHERE trader_id=$1")
            .bind(j.trader_id)
            .execute(&mut *tx)
            .await?;
        let points:Vec<DateTime<Utc>>=sqlx::query_scalar("SELECT DISTINCT x FROM(SELECT knowledge_available_at x FROM trader_position_episodes WHERE trader_id=$1 AND knowledge_available_at<=$2 UNION SELECT available_at FROM trader_episode_outcomes o JOIN trader_position_episodes e ON e.id=o.episode_id WHERE e.trader_id=$1 AND e.knowledge_available_at<=o.available_at AND available_at<=$2)s ORDER BY x").bind(j.trader_id).bind(now).fetch_all(&mut*tx).await?;
        let mut last = None;
        for at in points {
            let score = calculate_in(&mut tx, j.trader_id, at, true).await?;
            let hash = Sha256::digest(
                serde_json::to_vec(&score.inputs).map_err(|e| sqlx::Error::Decode(Box::new(e)))?,
            );
            let id:Uuid=sqlx::query_scalar("INSERT INTO trader_score_history(trader_id,pons_score,pons_score_confidence,component_scores,component_inputs,sample_size,matured_horizons,pending_horizons,censored_horizons,calculation_version,rule_version,weight_version,effective_at,knowledge_available_at,as_of_mode,inputs_hash,current_generation)VALUES($1,$2,$3,$4,$5,$6,($5->>'matured_horizons')::int,($5->>'pending_horizons')::int,($5->>'censored_horizons')::int,$7,$8,$9,$10,$10,'KNOWLEDGE_TIME',$11,true)ON CONFLICT(trader_id,effective_at,as_of_mode,calculation_version,inputs_hash)DO UPDATE SET current_generation=true,calculated_at=now()RETURNING id").bind(j.trader_id).bind(score.score).bind(score.confidence).bind(&score.components).bind(&score.inputs).bind(score.sample_size).bind(TRADER_ANALYTICS_CALCULATION_VERSION).bind(TRADER_SCORE_RULE_VERSION).bind(TRADER_SCORE_WEIGHT_VERSION).bind(at).bind(hash.as_slice()).fetch_one(&mut*tx).await?;
            last = Some((id, score));
        }
        let event_points:Vec<DateTime<Utc>>=sqlx::query_scalar("SELECT DISTINCT x FROM(SELECT opened_at x FROM trader_position_episodes WHERE trader_id=$1 AND opened_at<=$2 UNION SELECT target_time FROM trader_episode_outcomes o JOIN trader_position_episodes e ON e.id=o.episode_id WHERE e.trader_id=$1 AND target_time<=$2)s ORDER BY x").bind(j.trader_id).bind(now).fetch_all(&mut*tx).await?;
        for at in event_points {
            let score = calculate_in(&mut tx, j.trader_id, at, false).await?;
            let hash = Sha256::digest(
                serde_json::to_vec(&score.inputs).map_err(|e| sqlx::Error::Decode(Box::new(e)))?,
            );
            sqlx::query("INSERT INTO trader_score_history(trader_id,pons_score,pons_score_confidence,component_scores,component_inputs,sample_size,matured_horizons,pending_horizons,censored_horizons,calculation_version,rule_version,weight_version,effective_at,knowledge_available_at,as_of_mode,inputs_hash,current_generation)VALUES($1,$2,$3,$4,$5,$6,($5->>'matured_horizons')::int,($5->>'pending_horizons')::int,($5->>'censored_horizons')::int,$7,$8,$9,$10,$11,'EVENT_TIME_RECONSTRUCTED',$12,true)ON CONFLICT(trader_id,effective_at,as_of_mode,calculation_version,inputs_hash)DO UPDATE SET current_generation=true,knowledge_available_at=EXCLUDED.knowledge_available_at,calculated_at=now()").bind(j.trader_id).bind(score.score).bind(score.confidence).bind(&score.components).bind(&score.inputs).bind(score.sample_size).bind(TRADER_ANALYTICS_CALCULATION_VERSION).bind(TRADER_SCORE_RULE_VERSION).bind(TRADER_SCORE_WEIGHT_VERSION).bind(at).bind(now).bind(hash.as_slice()).execute(&mut*tx).await?;
        }
        if let Some((id, s)) = last {
            sqlx::query("INSERT INTO current_trader_scores(trader_id,score_history_id,pons_score,pons_score_confidence,sample_size,effective_at)VALUES($1,$2,$3,$4,$5,$6)ON CONFLICT(trader_id)DO UPDATE SET score_history_id=EXCLUDED.score_history_id,pons_score=EXCLUDED.pons_score,pons_score_confidence=EXCLUDED.pons_score_confidence,sample_size=EXCLUDED.sample_size,effective_at=EXCLUDED.effective_at,updated_at=now()").bind(j.trader_id).bind(id).bind(s.score).bind(s.confidence).bind(s.sample_size).bind(s.effective_at).execute(&mut*tx).await?;
        } else {
            sqlx::query("DELETE FROM current_trader_scores WHERE trader_id=$1")
                .bind(j.trader_id)
                .execute(&mut *tx)
                .await?;
        }
        sqlx::query("UPDATE trader_analytics_jobs SET status=CASE WHEN generation=$2 THEN'COMPLETED'ELSE'PENDING'END,locked_at=NULL,last_error=NULL,updated_at=now()WHERE trader_id=$1").bind(j.trader_id).bind(generation).execute(&mut*tx).await?;
        tx.commit().await
    }
    pub async fn score_as_of(
        &self,
        trader: Uuid,
        at: DateTime<Utc>,
        mode: &str,
    ) -> Result<Option<Value>, sqlx::Error> {
        sqlx::query_scalar("SELECT jsonb_build_object('as_of',$2,'mode',as_of_mode,'score',pons_score::text,'confidence',pons_score_confidence::text,'sample_size',sample_size,'components',component_scores,'inputs',component_inputs,'effective_at',effective_at,'knowledge_available_at',knowledge_available_at,'evidence_cutoff',CASE WHEN as_of_mode='KNOWLEDGE_TIME'THEN knowledge_available_at ELSE effective_at END,'calculation_version',calculation_version,'rule_version',rule_version,'weight_version',weight_version)FROM trader_score_history WHERE trader_id=$1 AND current_generation AND effective_at<=$2 AND as_of_mode=$3 ORDER BY effective_at DESC LIMIT 1").bind(trader).bind(at).bind(mode).fetch_optional(&self.pool).await
    }
    pub async fn analytics(&self, trader: Uuid, limit: i64) -> Result<Value, sqlx::Error> {
        let episodes:Vec<Value>=sqlx::query_scalar("SELECT jsonb_build_object('id',e.id,'token_id',e.token_id,'token_address','0x'||encode(t.address,'hex'),'episode_number',e.episode_number,'opened_at',e.opened_at,'closed_at',e.closed_at,'first_entry_price',e.first_entry_price::text,'first_entry_age_ms',e.first_entry_age_ms,'first_buyer_rank',e.first_buyer_rank,'first_smart_buyer_rank',e.first_smart_buyer_rank,'initial_buy_quote_raw',e.initial_buy_quote_raw,'relative_initial_size',e.relative_initial_size::text,'relative_size_history_samples',e.relative_size_history_samples,'relative_size_confidence',e.relative_size_confidence::text,'total_buy_quote_raw',e.total_buy_quote_raw,'total_sell_quote_raw',e.total_sell_quote_raw,'add_count',e.add_count,'reduce_count',e.reduce_count,'position_integrity',e.position_integrity,'outcomes',COALESCE((SELECT jsonb_agg(jsonb_build_object('horizon_seconds',o.horizon_seconds,'target_time',o.target_time,'observation_time',o.observation_time,'entry_price',o.entry_price::text,'observed_price',o.observed_price::text,'price_change',o.price_change::text,'status',o.status,'evidence_scope',o.evidence_scope)ORDER BY o.horizon_seconds)FROM trader_episode_outcomes o WHERE o.episode_id=e.id),'[]'),'excursion',(SELECT jsonb_build_object('observed_mfe',x.observed_mfe::text,'observed_mae',x.observed_mae::text,'snapshot_count',x.snapshot_count,'observation_basis',x.observation_basis)FROM trader_episode_excursions x WHERE x.episode_id=e.id))FROM trader_position_episodes e JOIN tokens t ON t.id=e.token_id WHERE e.trader_id=$1 ORDER BY e.opened_at DESC,e.id LIMIT $2").bind(trader).bind(limit).fetch_all(&self.pool).await?;
        let history:Vec<Value>=sqlx::query_scalar("SELECT jsonb_build_object('score',pons_score::text,'confidence',pons_score_confidence::text,'sample_size',sample_size,'component_scores',component_scores,'component_inputs',component_inputs,'matured_horizons',matured_horizons,'pending_horizons',pending_horizons,'censored_horizons',censored_horizons,'effective_at',effective_at,'knowledge_available_at',knowledge_available_at,'as_of_mode',as_of_mode,'calculated_at',calculated_at,'calculation_version',calculation_version,'rule_version',rule_version,'weight_version',weight_version)FROM trader_score_history WHERE trader_id=$1 AND current_generation ORDER BY as_of_mode,effective_at DESC LIMIT $2").bind(trader).bind(limit).fetch_all(&self.pool).await?;
        let current:Option<Value>=sqlx::query_scalar("SELECT jsonb_build_object('pons_score',c.pons_score::text,'confidence',c.pons_score_confidence::text,'sample_size',c.sample_size,'effective_at',c.effective_at,'components',h.component_scores,'inputs',h.component_inputs)FROM current_trader_scores c JOIN trader_score_history h ON h.id=c.score_history_id WHERE c.trader_id=$1").bind(trader).fetch_optional(&self.pool).await?;
        let stats:Value=sqlx::query_scalar("SELECT jsonb_build_object('episode_count',count(*),'open_episode_count',count(*)FILTER(WHERE closed_at IS NULL),'closed_episode_count',count(*)FILTER(WHERE closed_at IS NOT NULL),'early_entry_ratio',avg((first_entry_age_ms<=180000)::int)::text,'median_launch_age_ms',percentile_cont(0.5)WITHIN GROUP(ORDER BY first_entry_age_ms),'median_buyer_rank',percentile_cont(0.5)WITHIN GROUP(ORDER BY first_buyer_rank),'median_smart_buyer_rank',percentile_cont(0.5)WITHIN GROUP(ORDER BY first_smart_buyer_rank),'median_initial_quote_size',percentile_cont(0.5)WITHIN GROUP(ORDER BY initial_buy_quote_raw::numeric)::text,'median_position_duration_ms',extract(epoch FROM percentile_cont(0.5)WITHIN GROUP(ORDER BY closed_at-opened_at))*1000,'add_frequency',avg(add_count)::text,'reduce_frequency',avg(reduce_count)::text,'censored_count',(SELECT count(*)FROM trader_episode_outcomes o JOIN trader_position_episodes x ON x.id=o.episode_id WHERE x.trader_id=$1 AND o.status LIKE'CENSORED%'))FROM trader_position_episodes WHERE trader_id=$1").bind(trader).fetch_one(&self.pool).await?;
        Ok(
            json!({"current":current,"stats":stats,"episodes":episodes,"score_history":history,"score_basis":"PONS_V2_CURVE_MARKET_OUTCOME_PROXY","use_dynamic_trader_score":false}),
        )
    }
}

async fn calculate_in(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    trader: Uuid,
    at: DateTime<Utc>,
    knowledge_time: bool,
) -> Result<ScoreResult, sqlx::Error> {
    let s:Stats=sqlx::query_as(r#"SELECT count(DISTINCT e.id)episodes,avg(CASE WHEN first_entry_age_ms<=60000 THEN 100 WHEN first_entry_age_ms<=180000 THEN 80 WHEN first_entry_age_ms<=900000 THEN 60 ELSE 30 END)::numeric early,avg(CASE WHEN first_buyer_rank<=10 THEN 100 WHEN first_buyer_rank<=50 THEN 75 WHEN first_buyer_rank<=200 THEN 50 ELSE 25 END)::numeric rank,avg(50+greatest(-50,least(50,o.price_change)))FILTER(WHERE o.status='AVAILABLE')::numeric outcome,avg(least(100,50+add_count*10))::numeric adds,avg(CASE WHEN closed_at IS NULL THEN 50 WHEN closed_at-opened_at>=interval'5 minutes'THEN 70 ELSE 30 END)::numeric hold,count(*)FILTER(WHERE o.status='AVAILABLE')matured,count(*)FILTER(WHERE o.status='PENDING')pending,count(*)FILTER(WHERE o.status LIKE'CENSORED%')censored,(SELECT avg(identity_confidence)FROM trader_wallets WHERE trader_id=$1)identity,(SELECT count(*)FROM tokens WHERE launch_time BETWEEN $2-interval'30 days'AND $2)launches,count(DISTINCT e.id)FILTER(WHERE e.opened_at>=$2-interval'30 days')entries_30d FROM trader_position_episodes e LEFT JOIN trader_episode_outcomes o ON o.episode_id=e.id AND (($3 AND o.available_at<=$2)OR(NOT $3 AND o.target_time<=$2)) WHERE e.trader_id=$1 AND (($3 AND e.knowledge_available_at<=$2)OR(NOT $3 AND e.opened_at<=$2))"#).bind(trader).bind(at).bind(knowledge_time).fetch_one(&mut**tx).await?;
    let fifty = Decimal::from(50);
    let mut weighted = Decimal::ZERO;
    let mut weights = Decimal::ZERO;
    let values = [
        ("EARLY_ENTRY", s.early, 25_i64),
        ("BUYER_RANK", s.rank, 20),
        ("MARKET_OUTCOME", s.outcome, 35),
        ("CONVICTION_CALIBRATION", s.adds, 5),
        ("HOLD_BEHAVIOR", s.hold, 5),
    ];
    let mut components = serde_json::Map::new();
    for (name, value, weight) in values {
        components.insert(
            name.into(),
            value.map_or(Value::Null, |v| json!(v.to_string())),
        );
        if let Some(v) = value {
            weighted += v * Decimal::from(weight);
            weights += Decimal::from(weight);
        }
    }
    let selectivity = if s.launches > 0 {
        Some(
            (Decimal::ONE - Decimal::from(s.entries_30d) / Decimal::from(s.launches))
                .max(Decimal::ZERO)
                * Decimal::from(100),
        )
    } else {
        None
    };
    components.insert(
        "SELECTIVITY".into(),
        selectivity.map_or(Value::Null, |v| json!(v.to_string())),
    );
    if let Some(v) = selectivity {
        weighted += v * Decimal::from(10);
        weights += Decimal::from(10);
    }
    let raw = if weights.is_zero() {
        fifty
    } else {
        weighted / weights
    };
    let n = Decimal::from(s.episodes);
    let score = (raw * n + fifty * Decimal::from(5)) / (n + Decimal::from(5));
    let completeness = if s.matured + s.pending + s.censored == 0 {
        Decimal::ZERO
    } else {
        Decimal::from(s.matured) * Decimal::from(100)
            / Decimal::from(s.matured + s.pending + s.censored)
    };
    let identity = s.identity.unwrap_or(Decimal::ZERO) * Decimal::from(100);
    let confidence = (Decimal::from(s.episodes.min(10)) * Decimal::from(6)
        + completeness * Decimal::new(3, 1)
        + identity * Decimal::new(1, 1))
    .min(Decimal::from(100));
    let inputs = json!({"episode_count":s.episodes,"entries_30d":s.entries_30d,"matured_horizons":s.matured,"pending_horizons":s.pending,"censored_horizons":s.censored,"launches_30d":s.launches,"shrinkage":{"prior_score":"50","prior_strength":5},"content_in_score":false,"use_dynamic_trader_score":false});
    Ok(ScoreResult {
        score: score.round_dp(4),
        confidence: confidence.round_dp(4),
        sample_size: i32::try_from(s.episodes).unwrap_or(i32::MAX),
        effective_at: at,
        components: Value::Object(components),
        inputs,
    })
}
