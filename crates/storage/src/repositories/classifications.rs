use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct IdentityClassificationRepository {
    pool: PgPool,
}

#[derive(Clone, Debug, FromRow)]
pub struct IdentityClassificationJob {
    pub id: Uuid,
    pub trader_wallet_id: Uuid,
    pub trader_id: Uuid,
    pub wallet_address: Vec<u8>,
    pub identity_snapshot: Value,
    pub valid_from: DateTime<Utc>,
    pub valid_to: Option<DateTime<Utc>>,
    pub scan_cutoff: DateTime<Utc>,
    pub cursor_created_at: Option<DateTime<Utc>>,
    pub cursor_trade_id: Option<Uuid>,
    pub attempts: i32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClassificationPage {
    pub scanned: u64,
    pub created: u64,
    pub complete: bool,
}

#[derive(FromRow)]
struct TradeRow {
    id: Uuid,
    token_id: Uuid,
    side: String,
    token_amount_raw: String,
    quote_amount_raw: String,
    fee_raw: String,
    tax_raw: String,
    block_number: String,
    tx_hash: Vec<u8>,
    log_index: String,
    block_time: DateTime<Utc>,
    created_at: DateTime<Utc>,
}

#[allow(clippy::missing_errors_doc)]
impl IdentityClassificationRepository {
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Enqueues only an identity which currently satisfies the configured policy.
    /// The immutable job snapshot makes later administrative changes unable to rewrite history.
    pub async fn enqueue_eligible(
        &self,
        wallet_id: Uuid,
        minimum_confidence: &str,
    ) -> Result<Option<Uuid>, sqlx::Error> {
        let row: Option<(Uuid,)> = sqlx::query_as(r"
          INSERT INTO identity_classification_jobs(trader_wallet_id,trader_id,wallet_address,identity_snapshot,valid_from,valid_to)
          SELECT w.id,w.trader_id,w.address,
            jsonb_build_object('trader_id',w.trader_id,'trader_handle',t.handle,'trader_wallet_id',w.id,
              'wallet','0x'||encode(w.address,'hex'),'chain_id',w.chain_id,'role',w.role,'source',w.source,
              'identity_confidence',w.identity_confidence::text,'verified',w.verified,'valid_from',w.valid_from,
              'valid_to',w.valid_to,'identity_recorded_at',w.created_at,'identity_last_updated_at',w.updated_at,
              'registry_evidence',w.evidence,'classification_enqueued_at',now()),
            w.valid_from,w.valid_to
          FROM trader_wallets w JOIN traders t ON t.id=w.trader_id
          WHERE w.id=$1 AND w.chain_id=4663 AND w.role='ROBINHOOD_EXECUTION_ADDRESS'
            AND w.enabled AND w.verified AND t.enabled AND t.status='ACTIVE'
            AND w.identity_confidence >= $2::numeric
          ON CONFLICT DO NOTHING RETURNING id")
          .bind(wallet_id).bind(minimum_confidence).fetch_optional(&self.pool).await?;
        Ok(row.map(|v| v.0))
    }

    pub async fn claim_due(&self) -> Result<Option<IdentityClassificationJob>, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query_as(
            r"WITH due AS (
          SELECT id FROM identity_classification_jobs
          WHERE (status IN ('PENDING','RETRY') AND next_attempt_at<=now())
             OR (status='PROCESSING' AND locked_at<now()-interval '5 minutes')
          ORDER BY next_attempt_at,id FOR UPDATE SKIP LOCKED LIMIT 1)
          UPDATE identity_classification_jobs j SET status='PROCESSING',attempts=attempts+1,
            locked_at=now(),updated_at=now() FROM due WHERE j.id=due.id
          RETURNING j.id,j.trader_wallet_id,j.trader_id,j.wallet_address,j.identity_snapshot,
            j.valid_from,j.valid_to,j.scan_cutoff,j.cursor_created_at,j.cursor_trade_id,j.attempts",
        )
        .fetch_optional(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(row)
    }

    pub async fn process_page(
        &self,
        job: &IdentityClassificationJob,
        batch_size: i64,
    ) -> Result<ClassificationPage, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let rows:Vec<TradeRow>=sqlx::query_as(r"SELECT id,token_id,side,token_amount_raw,quote_amount_raw,fee_raw,tax_raw,
          block_number::text,tx_hash,log_index::text,block_time,created_at FROM token_trades
          WHERE chain_id=4663 AND created_at<=$1 AND block_time >= $2 AND ($3::timestamptz IS NULL OR block_time < $3)
            AND ((side='BUY' AND recipient=$4) OR (side='SELL' AND actor=$4))
            AND ($5::timestamptz IS NULL OR (created_at,id)>($5,$6))
          ORDER BY created_at,id LIMIT $7")
          .bind(job.scan_cutoff).bind(job.valid_from).bind(job.valid_to).bind(&job.wallet_address)
          .bind(job.cursor_created_at).bind(job.cursor_trade_id).bind(batch_size)
          .fetch_all(&mut *tx).await?;
        let mut created = 0_u64;
        for trade in &rows {
            let level = if trade.side == "BUY" {
                "BUY_STRONG"
            } else {
                "SELL_STRONG"
            };
            let snapshot = json!({"identity":job.identity_snapshot,"trade_occurred_at":trade.block_time,
              "historically_classified_at":Utc::now(),"classification_source":"IDENTITY_BACKFILL"});
            let evidence = json!({"confirmation_version":1,"status":"PENDING_CONFIRMATION",
              "token_trade_id":trade.id,"classification_source":"IDENTITY_BACKFILL",
              "realtime_alert_eligible":false,"identity_snapshot":snapshot});
            let smart:Option<Uuid>=sqlx::query_scalar(r"INSERT INTO smart_trades(
              token_id,token_trade_id,trader_id,trader_wallet_id,wallet_address,side,confirmation_level,
              confirmation_confidence,confirmation_version,token_amount_raw,quote_amount_raw,fee_raw,tax_raw,
              block_number,tx_hash,log_index,block_time,launch_age_ms,launch_age_blocks,identity_snapshot,evidence,
              classification_source,realtime_alert_eligible)
              SELECT $1,$2,$3,$4,$5,$6,$7,0.7500,1,$8,$9,$10,$11,$12::numeric,$13,$14::numeric,$15,
                CASE WHEN t.launch_time IS NULL THEN NULL ELSE GREATEST(0,(extract(epoch FROM ($15-t.launch_time))*1000)::bigint) END,
                CASE WHEN t.launch_block IS NULL THEN NULL ELSE GREATEST(0,$12::numeric-t.launch_block) END,$16,$17,
                'IDENTITY_BACKFILL',false FROM tokens t WHERE t.id=$1
              ON CONFLICT(token_trade_id) DO NOTHING RETURNING id")
              .bind(trade.token_id).bind(trade.id).bind(job.trader_id).bind(job.trader_wallet_id)
              .bind(&job.wallet_address).bind(&trade.side).bind(level).bind(&trade.token_amount_raw)
              .bind(&trade.quote_amount_raw).bind(&trade.fee_raw).bind(&trade.tax_raw).bind(&trade.block_number)
              .bind(&trade.tx_hash).bind(&trade.log_index).bind(trade.block_time).bind(&snapshot).bind(&evidence)
              .fetch_optional(&mut *tx).await?;
            if let Some(smart) = smart {
                sqlx::query("INSERT INTO trade_confirmation_jobs(smart_trade_id,token_trade_id) VALUES($1,$2) ON CONFLICT DO NOTHING")
                    .bind(smart).bind(trade.id).execute(&mut *tx).await?;
                created += 1;
            }
        }
        let complete = rows.len() < usize::try_from(batch_size).unwrap_or(usize::MAX);
        let last = rows.last();
        sqlx::query(r"UPDATE identity_classification_jobs SET status=CASE WHEN $2 THEN 'COMPLETED' ELSE 'PENDING' END,
          scanned_count=scanned_count+$3,candidates_created=candidates_created+$4,
          cursor_created_at=COALESCE($5,cursor_created_at),cursor_trade_id=COALESCE($6,cursor_trade_id),
          completed_at=CASE WHEN $2 THEN now() ELSE NULL END,locked_at=NULL,last_error=NULL,updated_at=now()
          WHERE id=$1").bind(job.id).bind(complete).bind(i64::try_from(rows.len()).unwrap_or(i64::MAX))
          .bind(i64::try_from(created).unwrap_or(i64::MAX)).bind(last.map(|v|v.created_at)).bind(last.map(|v|v.id))
          .execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(ClassificationPage {
            scanned: rows.len() as u64,
            created,
            complete,
        })
    }

    pub async fn retry(
        &self,
        id: Uuid,
        error: &str,
        next: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE identity_classification_jobs SET status='RETRY',last_error=$2,next_attempt_at=$3,locked_at=NULL,updated_at=now() WHERE id=$1")
          .bind(id).bind(error.chars().take(2048).collect::<String>()).bind(next).execute(&self.pool).await?;
        Ok(())
    }
}
