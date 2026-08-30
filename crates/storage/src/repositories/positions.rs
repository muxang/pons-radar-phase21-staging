use alloy_primitives::U256;
use chrono::{DateTime, Utc};
use sqlx::{FromRow, PgPool, Postgres, Transaction};
use uuid::Uuid;

pub const POSITION_CALCULATION_VERSION: i32 = 1;
pub const POSITION_BASIS: &str = "PONS_V2_CONFIRMED_TRADES";

#[derive(Clone, Debug)]
pub struct PositionRepository {
    pool: PgPool,
}
#[derive(Clone, Debug, FromRow)]
pub struct PositionRebuildJob {
    pub token_id: Uuid,
    pub trader_wallet_id: Uuid,
    pub wallet_address: Vec<u8>,
    pub generation: i64,
    pub attempts: i32,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PositionRebuildResult {
    pub events: u64,
    pub warnings: u64,
    pub balance_raw: String,
    pub generation: i64,
}

#[derive(FromRow)]
struct LedgerRow {
    smart_trade_id: Uuid,
    trader_id: Uuid,
    side: String,
    token_amount_raw: String,
    quote_amount_raw: String,
    block_number: String,
    transaction_index: Option<String>,
    log_index: String,
    block_time: DateTime<Utc>,
    classification_source: String,
}
struct DerivedEvent {
    trade: Uuid,
    event: &'static str,
    side: String,
    token: String,
    quote: String,
    before: String,
    after: String,
    block: String,
    tx_index: Option<String>,
    log: String,
    time: DateTime<Utc>,
    source: String,
    warning: Option<serde_json::Value>,
}

#[allow(clippy::missing_errors_doc)]
impl PositionRepository {
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
    pub async fn mark_dirty_for_smart_trade(
        &self,
        smart_trade_id: Uuid,
    ) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        Self::mark_dirty_in_transaction(&mut tx, smart_trade_id).await?;
        tx.commit().await
    }
    pub async fn mark_dirty_in_transaction(
        tx: &mut Transaction<'_, Postgres>,
        smart_trade_id: Uuid,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(r"INSERT INTO position_rebuild_jobs(token_id,trader_wallet_id,wallet_address)
    SELECT token_id,trader_wallet_id,wallet_address FROM smart_trades WHERE id=$1
    ON CONFLICT(token_id,trader_wallet_id) DO UPDATE SET generation=position_rebuild_jobs.generation+1,status='PENDING',next_attempt_at=now(),locked_at=NULL,last_error=NULL,updated_at=now()")
   .bind(smart_trade_id).execute(&mut **tx).await?;
        Ok(())
    }
    pub async fn claim_due(&self) -> Result<Option<PositionRebuildJob>, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let row=sqlx::query_as(r"WITH due AS (SELECT token_id,trader_wallet_id FROM position_rebuild_jobs WHERE (status IN ('PENDING','RETRY') AND next_attempt_at<=now()) OR (status='PROCESSING' AND locked_at<now()-interval '5 minutes') ORDER BY next_attempt_at,token_id,trader_wallet_id FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE position_rebuild_jobs j SET status='PROCESSING',claimed_generation=generation,attempts=attempts+1,locked_at=now(),updated_at=now() FROM due WHERE j.token_id=due.token_id AND j.trader_wallet_id=due.trader_wallet_id RETURNING j.token_id,j.trader_wallet_id,j.wallet_address,j.generation,j.attempts").fetch_optional(&mut *tx).await?;
        tx.commit().await?;
        Ok(row)
    }
    pub async fn retry(
        &self,
        job: &PositionRebuildJob,
        error: &str,
        next: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE position_rebuild_jobs SET status='RETRY',next_attempt_at=$3,last_error=$4,locked_at=NULL,updated_at=now() WHERE token_id=$1 AND trader_wallet_id=$2").bind(job.token_id).bind(job.trader_wallet_id).bind(next).bind(error.chars().take(2048).collect::<String>()).execute(&self.pool).await?;
        Ok(())
    }
    #[allow(clippy::too_many_lines)]
    pub async fn rebuild(
        &self,
        job: &PositionRebuildJob,
    ) -> Result<PositionRebuildResult, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1::text,0))")
            .bind(job.token_id)
            .execute(&mut *tx)
            .await?;
        let generation:i64=sqlx::query_scalar("SELECT generation FROM position_rebuild_jobs WHERE token_id=$1 AND trader_wallet_id=$2 FOR UPDATE").bind(job.token_id).bind(job.trader_wallet_id).fetch_one(&mut *tx).await?;
        let ledger:Vec<LedgerRow>=sqlx::query_as(r"SELECT s.id smart_trade_id,s.trader_id,s.side,s.token_amount_raw,s.quote_amount_raw,s.block_number::text,t.transaction_index::text transaction_index,s.log_index::text,s.block_time,s.classification_source FROM smart_trades s JOIN token_trades t ON t.id=s.token_trade_id WHERE s.token_id=$1 AND s.trader_wallet_id=$2 AND s.confirmation_level IN ('BUY_CONFIRMED','SELL_CONFIRMED') AND t.status<>'ORPHANED' ORDER BY s.block_number,COALESCE(t.transaction_index,t.log_index),s.log_index,s.tx_hash")
   .bind(job.token_id).bind(job.trader_wallet_id).fetch_all(&mut *tx).await?;
        recompute_ranks(&mut tx, job.token_id).await?;
        if ledger.is_empty() {
            sqlx::query(
                "DELETE FROM wallet_token_positions WHERE token_id=$1 AND trader_wallet_id=$2",
            )
            .bind(job.token_id)
            .bind(job.trader_wallet_id)
            .execute(&mut *tx)
            .await?;
            complete(&mut tx, job, generation).await?;
            tx.commit().await?;
            return Ok(PositionRebuildResult {
                events: 0,
                warnings: 0,
                balance_raw: "0".into(),
                generation,
            });
        }
        let mut balance = U256::ZERO;
        let mut quote_in = U256::ZERO;
        let mut quote_out = U256::ZERO;
        let mut events = Vec::with_capacity(ledger.len());
        let mut warnings = 0_u64;
        for row in &ledger {
            let amount = row
                .token_amount_raw
                .parse::<U256>()
                .map_err(|e| decode_msg(&e.to_string()))?;
            let quote = row
                .quote_amount_raw
                .parse::<U256>()
                .map_err(|e| decode_msg(&e.to_string()))?;
            let before = balance;
            let (kind, warning) = if row.side == "BUY" {
                balance = balance
                    .checked_add(amount)
                    .ok_or_else(|| decode_msg("position balance overflow"))?;
                quote_in = quote_in
                    .checked_add(quote)
                    .ok_or_else(|| decode_msg("quote-in overflow"))?;
                (
                    if before == U256::ZERO {
                        "OPEN_POSITION"
                    } else {
                        "ADD_POSITION"
                    },
                    None,
                )
            } else if amount > balance {
                warnings += 1;
                (
                    "POSITION_INTEGRITY_WARNING",
                    Some(
                        serde_json::json!({"code":"SELL_OVERBALANCE","balance_raw":before.to_string(),"sell_amount_raw":amount.to_string()}),
                    ),
                )
            } else {
                balance -= amount;
                quote_out = quote_out
                    .checked_add(quote)
                    .ok_or_else(|| decode_msg("quote-out overflow"))?;
                (
                    if balance == U256::ZERO {
                        "CLOSE_POSITION"
                    } else {
                        "REDUCE_POSITION"
                    },
                    None,
                )
            };
            events.push(DerivedEvent {
                trade: row.smart_trade_id,
                event: kind,
                side: row.side.clone(),
                token: row.token_amount_raw.clone(),
                quote: row.quote_amount_raw.clone(),
                before: before.to_string(),
                after: balance.to_string(),
                block: row.block_number.clone(),
                tx_index: row.transaction_index.clone(),
                log: row.log_index.clone(),
                time: row.block_time,
                source: row.classification_source.clone(),
                warning,
            });
        }
        let first = ledger
            .iter()
            .find(|v| v.side == "BUY")
            .map(|v| v.block_time);
        let last = ledger.last().map(|v| v.block_time);
        let trader = ledger[0].trader_id;
        let position:Uuid=sqlx::query_scalar(r"INSERT INTO wallet_token_positions(token_id,trader_id,trader_wallet_id,wallet_address,balance_raw,total_quote_in_raw,total_quote_out_raw,first_entry_at,last_trade_at,open,integrity_status,position_basis,calculation_version,rebuilt_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,now()) ON CONFLICT(token_id,trader_wallet_id) DO UPDATE SET trader_id=EXCLUDED.trader_id,wallet_address=EXCLUDED.wallet_address,balance_raw=EXCLUDED.balance_raw,total_quote_in_raw=EXCLUDED.total_quote_in_raw,total_quote_out_raw=EXCLUDED.total_quote_out_raw,first_entry_at=EXCLUDED.first_entry_at,last_trade_at=EXCLUDED.last_trade_at,open=EXCLUDED.open,integrity_status=EXCLUDED.integrity_status,position_basis=EXCLUDED.position_basis,calculation_version=EXCLUDED.calculation_version,rebuilt_at=now(),updated_at=now() RETURNING id")
   .bind(job.token_id).bind(trader).bind(job.trader_wallet_id).bind(&job.wallet_address).bind(balance.to_string()).bind(quote_in.to_string()).bind(quote_out.to_string()).bind(first).bind(last).bind(balance!=U256::ZERO).bind(if warnings==0{"OK"}else{"POSITION_INTEGRITY_WARNING"}).bind(POSITION_BASIS).bind(POSITION_CALCULATION_VERSION).fetch_one(&mut *tx).await?;
        sqlx::query("DELETE FROM position_events WHERE position_id=$1")
            .bind(position)
            .execute(&mut *tx)
            .await?;
        for v in &events {
            sqlx::query("INSERT INTO position_events(position_id,smart_trade_id,event_type,side,token_amount_raw,quote_amount_raw,balance_before_raw,balance_after_raw,block_number,transaction_index,log_index,block_time,classification_source,calculation_version,warning) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9::numeric,$10::numeric,$11::numeric,$12,$13,$14,$15)").bind(position).bind(v.trade).bind(v.event).bind(&v.side).bind(&v.token).bind(&v.quote).bind(&v.before).bind(&v.after).bind(&v.block).bind(&v.tx_index).bind(&v.log).bind(v.time).bind(&v.source).bind(POSITION_CALCULATION_VERSION).bind(&v.warning).execute(&mut *tx).await?;
            let event_type = match v.event {
                "OPEN_POSITION" => "position.open",
                "ADD_POSITION" => "position.add",
                "REDUCE_POSITION" => "position.reduce",
                "CLOSE_POSITION" => "position.close",
                _ => continue,
            };
            let payload = serde_json::json!({"token_id":job.token_id,"trader_id":trader,"smart_trade_id":v.trade,"event_type":v.event,"token_amount_raw":v.token,"quote_amount_raw":v.quote,"balance_before_raw":v.before,"balance_after_raw":v.after,"event_effective_at":v.time,"classification_source":v.source,"realtime_alert_eligible":v.source=="LIVE"});
            super::EventOutboxRepository::append_in_transaction(
                &mut tx,
                &super::NewOutboxEvent {
                    event_type,
                    schema_version: 1,
                    aggregate_type: Some("smart_trade"),
                    aggregate_id: Some(v.trade),
                    dedupe_key: &format!(
                        "position:{}:{}:v{}",
                        v.event, v.trade, POSITION_CALCULATION_VERSION
                    ),
                    payload: &payload,
                },
            )
            .await?;
        }
        complete(&mut tx, job, generation).await?;
        tx.commit().await?;
        Ok(PositionRebuildResult {
            events: events.len() as u64,
            warnings,
            balance_raw: balance.to_string(),
            generation,
        })
    }
}
async fn complete(
    tx: &mut Transaction<'_, Postgres>,
    job: &PositionRebuildJob,
    generation: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE position_rebuild_jobs SET status=CASE WHEN generation=$3 THEN 'COMPLETED' ELSE 'PENDING' END,claimed_generation=NULL,locked_at=NULL,last_error=NULL,updated_at=now() WHERE token_id=$1 AND trader_wallet_id=$2").bind(job.token_id).bind(job.trader_wallet_id).bind(generation).execute(&mut **tx).await?;
    Ok(())
}
async fn recompute_ranks(
    tx: &mut Transaction<'_, Postgres>,
    token: Uuid,
) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE smart_trades SET buyer_rank=NULL,smart_buyer_rank=NULL WHERE token_id=$1 AND side='BUY'").bind(token).execute(&mut **tx).await?;
    sqlx::query(r"WITH general_first AS (SELECT DISTINCT ON(recipient) recipient,block_number,COALESCE(transaction_index,log_index) txi,log_index FROM token_trades WHERE token_id=$1 AND side='BUY' AND status<>'ORPHANED' ORDER BY recipient,block_number,COALESCE(transaction_index,log_index),log_index,tx_hash), smart_first AS (SELECT DISTINCT ON(s.wallet_address) s.wallet_address,s.block_number,COALESCE(t.transaction_index,t.log_index) txi,s.log_index FROM smart_trades s JOIN token_trades t ON t.id=s.token_trade_id WHERE s.token_id=$1 AND s.side='BUY' AND s.confirmation_level='BUY_CONFIRMED' AND t.status<>'ORPHANED' ORDER BY s.wallet_address,s.block_number,COALESCE(t.transaction_index,t.log_index),s.log_index,s.tx_hash), ranked AS (SELECT sf.wallet_address,(SELECT count(*) FROM general_first g WHERE (g.block_number,g.txi,g.log_index)<=(sf.block_number,sf.txi,sf.log_index)) buyer_rank,row_number() OVER(ORDER BY sf.block_number,sf.txi,sf.log_index,sf.wallet_address) smart_rank FROM smart_first sf) UPDATE smart_trades s SET buyer_rank=r.buyer_rank,smart_buyer_rank=r.smart_rank FROM ranked r WHERE s.token_id=$1 AND s.side='BUY' AND s.confirmation_level='BUY_CONFIRMED' AND s.wallet_address=r.wallet_address AND EXISTS(SELECT 1 FROM token_trades t WHERE t.id=s.token_trade_id AND t.status<>'ORPHANED')").bind(token).execute(&mut **tx).await?;
    Ok(())
}
fn decode(e: impl std::error::Error + Send + Sync + 'static) -> sqlx::Error {
    sqlx::Error::Decode(Box::new(e))
}
fn decode_msg(v: &str) -> sqlx::Error {
    decode(std::io::Error::other(v.to_owned()))
}
