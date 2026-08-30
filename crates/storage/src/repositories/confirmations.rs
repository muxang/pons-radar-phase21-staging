use chrono::{DateTime, Utc};
use pons_domain::{
    BlockHash, BlockNumber, CurveAddress, LogIndex, TokenAddress, TxHash, WalletAddress,
};
use serde_json::Value;
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct ConfirmationJob {
    pub smart_trade_id: Uuid,
    pub token_trade_id: Uuid,
    pub token_id: Uuid,
    pub token: TokenAddress,
    pub curve: CurveAddress,
    pub side: String,
    pub wallet: WalletAddress,
    pub token_amount_raw: String,
    pub quote_amount_raw: String,
    pub fee_raw: String,
    pub tax_raw: String,
    pub block_number: BlockNumber,
    pub block_hash: BlockHash,
    pub tx_hash: TxHash,
    pub log_index: LogIndex,
    pub block_time: DateTime<Utc>,
    pub trade_status: String,
    pub attempts: i32,
    pub classification_source: String,
    pub realtime_alert_eligible: bool,
}
#[derive(FromRow)]
struct JobRow {
    smart_trade_id: Uuid,
    token_trade_id: Uuid,
    token_id: Uuid,
    token: Vec<u8>,
    curve: Vec<u8>,
    side: String,
    wallet: Vec<u8>,
    token_amount_raw: String,
    quote_amount_raw: String,
    fee_raw: String,
    tax_raw: String,
    block_number: String,
    block_hash: Vec<u8>,
    tx_hash: Vec<u8>,
    log_index: String,
    block_time: DateTime<Utc>,
    trade_status: String,
    attempts: i32,
    classification_source: String,
    realtime_alert_eligible: bool,
}
pub struct TransferRecord<'a> {
    pub token: TokenAddress,
    pub from: WalletAddress,
    pub to: WalletAddress,
    pub amount_raw: &'a str,
    pub log_index: LogIndex,
    pub tx_hash: TxHash,
    pub raw_log: &'a Value,
}
#[derive(Clone, Debug)]
pub struct ConfirmationRepository {
    pool: PgPool,
}
#[allow(clippy::missing_errors_doc)]
impl ConfirmationRepository {
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
    pub async fn claim_due(&self) -> Result<Option<ConfirmationJob>, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let row:Option<JobRow>=sqlx::query_as(r"WITH due AS (SELECT smart_trade_id FROM trade_confirmation_jobs WHERE (status IN ('PENDING','RETRY') AND next_attempt_at<=now()) OR (status='PROCESSING' AND locked_at<now()-interval '5 minutes') ORDER BY next_attempt_at,smart_trade_id FOR UPDATE SKIP LOCKED LIMIT 1), claimed AS (UPDATE trade_confirmation_jobs j SET status='PROCESSING',attempts=attempts+1,locked_at=now(),updated_at=now() FROM due WHERE j.smart_trade_id=due.smart_trade_id RETURNING j.smart_trade_id,j.token_trade_id,j.attempts) SELECT c.smart_trade_id,c.token_trade_id,t.token_id,t.token_address token,t.curve_address curve,t.side,cst.wallet_address wallet,t.token_amount_raw,t.quote_amount_raw,t.fee_raw,t.tax_raw,t.block_number::text block_number,t.block_hash,t.tx_hash,t.log_index::text log_index,t.block_time,t.status trade_status,c.attempts,cst.classification_source,cst.realtime_alert_eligible FROM claimed c JOIN token_trades t ON t.id=c.token_trade_id JOIN smart_trades cst ON cst.id=c.smart_trade_id").fetch_optional(&mut *tx).await?;
        tx.commit().await?;
        row.map(decode_job).transpose()
    }
    pub async fn retry(
        &self,
        id: Uuid,
        error: &str,
        next: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE trade_confirmation_jobs SET status='RETRY',last_error=$2,next_attempt_at=$3,locked_at=NULL,updated_at=now() WHERE smart_trade_id=$1").bind(id).bind(error).bind(next).execute(&self.pool).await?;
        Ok(())
    }
    pub async fn reject(
        &self,
        id: Uuid,
        level: &str,
        evidence: &Value,
        error: &str,
    ) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("UPDATE smart_trades SET confirmation_level=$2,confirmation_confidence=0,evidence=$3 WHERE id=$1 AND confirmed_at IS NULL").bind(id).bind(level).bind(evidence).execute(&mut *tx).await?;
        sqlx::query("UPDATE trade_confirmation_jobs SET status='REJECTED',last_error=$2,locked_at=NULL,updated_at=now() WHERE smart_trade_id=$1").bind(id).bind(error).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
    pub async fn confirm(
        &self,
        job: &ConfirmationJob,
        evidence: &Value,
        transfers: &[TransferRecord<'_>],
        outbox: &Value,
    ) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        for v in transfers {
            sqlx::query("INSERT INTO trade_transfer_evidence(smart_trade_id,token_address,from_address,to_address,amount_raw,log_index,tx_hash,raw_log) VALUES($1,$2,$3,$4,$5,$6::numeric,$7,$8) ON CONFLICT(smart_trade_id,log_index) DO NOTHING").bind(job.smart_trade_id).bind(v.token.as_bytes().as_slice()).bind(v.from.as_bytes().as_slice()).bind(v.to.as_bytes().as_slice()).bind(v.amount_raw).bind(v.log_index.get().to_string()).bind(v.tx_hash.as_bytes().as_slice()).bind(v.raw_log).execute(&mut *tx).await?;
        }
        let level = if job.side == "BUY" {
            "BUY_CONFIRMED"
        } else {
            "SELL_CONFIRMED"
        };
        sqlx::query("UPDATE smart_trades SET confirmation_level=$2,confirmation_confidence=1,evidence=$3,confirmed_at=COALESCE(confirmed_at,now()) WHERE id=$1 AND confirmation_level NOT IN ('INTEGRITY_CONFLICT','REJECTED')").bind(job.smart_trade_id).bind(level).bind(evidence).execute(&mut *tx).await?;
        sqlx::query("UPDATE trade_confirmation_jobs SET status='CONFIRMED',last_error=NULL,locked_at=NULL,updated_at=now() WHERE smart_trade_id=$1").bind(job.smart_trade_id).execute(&mut *tx).await?;
        super::PositionRepository::mark_dirty_in_transaction(&mut tx, job.smart_trade_id).await?;
        let event = if job.classification_source != "LIVE" && job.side == "BUY" {
            "smart_trade.buy_backfilled"
        } else if job.classification_source != "LIVE" {
            "smart_trade.sell_backfilled"
        } else if job.side == "BUY" {
            "smart_trade.buy_confirmed"
        } else {
            "smart_trade.sell_confirmed"
        };
        let dedupe = format!("{event}:{}", job.smart_trade_id);
        super::EventOutboxRepository::append_in_transaction(
            &mut tx,
            &super::NewOutboxEvent {
                event_type: event,
                schema_version: 1,
                aggregate_type: Some("smart_trade"),
                aggregate_id: Some(job.smart_trade_id),
                dedupe_key: &dedupe,
                payload: outbox,
            },
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }
}
fn decode_job(r: JobRow) -> Result<ConfirmationJob, sqlx::Error> {
    Ok(ConfirmationJob {
        smart_trade_id: r.smart_trade_id,
        token_trade_id: r.token_trade_id,
        token_id: r.token_id,
        token: TokenAddress::from_slice(&r.token).map_err(decode)?,
        curve: CurveAddress::from_slice(&r.curve).map_err(decode)?,
        side: r.side,
        wallet: WalletAddress::from_slice(&r.wallet).map_err(decode)?,
        token_amount_raw: r.token_amount_raw,
        quote_amount_raw: r.quote_amount_raw,
        fee_raw: r.fee_raw,
        tax_raw: r.tax_raw,
        block_number: BlockNumber::new(r.block_number.parse().map_err(decode)?),
        block_hash: BlockHash::from_slice(&r.block_hash).map_err(decode)?,
        tx_hash: TxHash::from_slice(&r.tx_hash).map_err(decode)?,
        log_index: LogIndex::new(r.log_index.parse().map_err(decode)?),
        block_time: r.block_time,
        trade_status: r.trade_status,
        attempts: r.attempts,
        classification_source: r.classification_source,
        realtime_alert_eligible: r.realtime_alert_eligible,
    })
}
fn decode(e: impl std::error::Error + Send + Sync + 'static) -> sqlx::Error {
    sqlx::Error::Decode(Box::new(e))
}
