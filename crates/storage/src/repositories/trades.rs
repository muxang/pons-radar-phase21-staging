use chrono::{DateTime, Utc};
use pons_domain::{
    BlockHash, BlockNumber, ChainId, CurveAddress, LogIndex, TokenAddress, TxHash, WalletAddress,
};
use serde_json::Value;
use sqlx::PgPool;
use thiserror::Error;
use uuid::Uuid;

use super::{EventOutboxRepository, NewOutboxEvent};

pub struct PersistCurveTrade<'a> {
    pub chain_id: ChainId,
    pub token_id: Uuid,
    pub token: TokenAddress,
    pub curve: CurveAddress,
    pub event_type: &'a str,
    pub side: &'a str,
    pub actor: WalletAddress,
    pub recipient: WalletAddress,
    pub quote_amount_raw: &'a str,
    pub token_amount_raw: &'a str,
    pub fee_raw: &'a str,
    pub tax_raw: &'a str,
    pub block_number: BlockNumber,
    pub block_hash: BlockHash,
    pub tx_hash: TxHash,
    pub transaction_index: Option<u64>,
    pub log_index: LogIndex,
    pub block_time: DateTime<Utc>,
    pub topics: &'a Value,
    pub data: &'a [u8],
    pub parser_version: i32,
    pub schema_version: i32,
    pub normalized_payload: &'a Value,
    pub outbox_payload: &'a Value,
    pub candidate: Option<&'a TradeCandidateIdentity>,
    pub classification_source: &'a str,
    pub realtime_alert_eligible: bool,
}

#[derive(Clone, Debug)]
pub struct TradeCandidateIdentity {
    pub trader_id: Uuid,
    pub trader_wallet_id: Uuid,
    pub wallet: WalletAddress,
    pub confidence: String,
    pub verified: bool,
    pub role: String,
    pub source: String,
    pub snapshot: Value,
}

pub struct PersistCurveRefund<'a> {
    pub chain_id: ChainId,
    pub token_id: Uuid,
    pub curve: CurveAddress,
    pub buyer: WalletAddress,
    pub refund_raw: &'a str,
    pub block_number: BlockNumber,
    pub block_hash: BlockHash,
    pub tx_hash: TxHash,
    pub transaction_index: Option<u64>,
    pub log_index: LogIndex,
    pub block_time: DateTime<Utc>,
    pub topics: &'a Value,
    pub data: &'a [u8],
    pub parser_version: i32,
    pub schema_version: i32,
    pub normalized_payload: &'a Value,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PersistedTrade {
    pub trade_id: Uuid,
    pub raw_log_id: Uuid,
    pub event_id: Vec<u8>,
    pub outbox_seq: i64,
}

#[derive(Debug, Error)]
pub enum TradePersistenceError {
    #[error("curve trade conflicts with immutable chain evidence")]
    Conflict,
    #[error("normalized event payload conflicts with this parser/schema version")]
    NormalizedConflict,
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
}

#[derive(Clone, Debug)]
pub struct TradeRepository {
    pool: PgPool,
}

#[allow(clippy::missing_errors_doc)]
impl TradeRepository {
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn persist_trade(
        &self,
        v: &PersistCurveTrade<'_>,
    ) -> Result<PersistedTrade, TradePersistenceError> {
        let mut tx = self.pool.begin().await?;
        let raw_id = insert_raw(
            &mut tx,
            v.chain_id,
            v.block_number,
            v.block_hash,
            v.tx_hash,
            v.log_index,
            v.curve,
            v.topics,
            v.data,
        )
        .await?;
        let event_id = insert_normalized(
            &mut tx,
            raw_id,
            v.chain_id,
            v.tx_hash,
            v.log_index,
            v.event_type,
            v.parser_version,
            v.schema_version,
            v.normalized_payload,
        )
        .await?;
        let trade_id:Option<Uuid>=sqlx::query_scalar(
            r"INSERT INTO token_trades(chain_id,token_id,token_address,curve_address,event_type,side,actor,recipient,quote_amount_raw,token_amount_raw,fee_raw,tax_raw,block_number,block_hash,tx_hash,transaction_index,log_index,block_time,raw_log_id,normalized_event_id)
              VALUES($1::numeric,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::numeric,$14,$15,$16::numeric,$17::numeric,$18,$19,$20)
              ON CONFLICT(chain_id,tx_hash,log_index,event_type) DO UPDATE SET tx_hash=EXCLUDED.tx_hash
              WHERE token_trades.token_id=EXCLUDED.token_id AND token_trades.token_address=EXCLUDED.token_address
                AND token_trades.curve_address=EXCLUDED.curve_address AND token_trades.side=EXCLUDED.side
                AND token_trades.actor=EXCLUDED.actor AND token_trades.recipient=EXCLUDED.recipient
                AND token_trades.quote_amount_raw=EXCLUDED.quote_amount_raw AND token_trades.token_amount_raw=EXCLUDED.token_amount_raw
                AND token_trades.fee_raw=EXCLUDED.fee_raw AND token_trades.tax_raw=EXCLUDED.tax_raw
                AND token_trades.block_number=EXCLUDED.block_number AND token_trades.block_hash=EXCLUDED.block_hash
              RETURNING id",
        ).bind(v.chain_id.get().to_string()).bind(v.token_id).bind(v.token.as_bytes().as_slice())
         .bind(v.curve.as_bytes().as_slice()).bind(v.event_type).bind(v.side)
         .bind(v.actor.as_bytes().as_slice()).bind(v.recipient.as_bytes().as_slice())
         .bind(v.quote_amount_raw).bind(v.token_amount_raw).bind(v.fee_raw).bind(v.tax_raw)
         .bind(v.block_number.get().to_string()).bind(v.block_hash.as_bytes().as_slice())
         .bind(v.tx_hash.as_bytes().as_slice()).bind(v.transaction_index.map(|n|n.to_string()))
         .bind(v.log_index.get().to_string()).bind(v.block_time).bind(raw_id).bind(&event_id)
         .fetch_optional(&mut *tx).await?;
        let trade_id = trade_id.ok_or(TradePersistenceError::Conflict)?;
        if let Some(candidate) = v.candidate {
            let initial_level = if v.side == "BUY" {
                "BUY_STRONG"
            } else {
                "SELL_STRONG"
            };
            let evidence = serde_json::json!({"confirmation_version":1,"status":"PENDING_CONFIRMATION","protocol_event":v.event_type,"token_trade_id":trade_id,"tracked_wallet_match":{"trader_id":candidate.trader_id,"trader_wallet_id":candidate.trader_wallet_id,"wallet":candidate.wallet.to_string(),"identity_confidence":candidate.confidence,"verified":candidate.verified,"role":candidate.role,"source":candidate.source}});
            let smart_id:Uuid=sqlx::query_scalar(r"INSERT INTO smart_trades(token_id,token_trade_id,trader_id,trader_wallet_id,wallet_address,side,confirmation_level,confirmation_confidence,confirmation_version,token_amount_raw,quote_amount_raw,fee_raw,tax_raw,block_number,tx_hash,log_index,block_time,launch_age_ms,launch_age_blocks,identity_snapshot,evidence,classification_source,realtime_alert_eligible)
              SELECT $1,$2,$3,$4,$5,$6,$7,0.7500,1,$8,$9,$10,$11,$12::numeric,$13,$14::numeric,$15,
                CASE WHEN t.launch_time IS NULL THEN NULL ELSE GREATEST(0,(extract(epoch FROM ($15-t.launch_time))*1000)::bigint) END,
                CASE WHEN t.launch_block IS NULL THEN NULL ELSE GREATEST(0,$12::numeric-t.launch_block) END,$16,$17,$18,$19 FROM tokens t WHERE t.id=$1
              ON CONFLICT(token_trade_id) DO UPDATE SET token_trade_id=EXCLUDED.token_trade_id RETURNING id")
              .bind(v.token_id).bind(trade_id).bind(candidate.trader_id).bind(candidate.trader_wallet_id).bind(candidate.wallet.as_bytes().as_slice()).bind(v.side).bind(initial_level).bind(v.token_amount_raw).bind(v.quote_amount_raw).bind(v.fee_raw).bind(v.tax_raw).bind(v.block_number.get().to_string()).bind(v.tx_hash.as_bytes().as_slice()).bind(v.log_index.get().to_string()).bind(v.block_time).bind(&candidate.snapshot).bind(&evidence).bind(v.classification_source).bind(v.realtime_alert_eligible).fetch_one(&mut *tx).await?;
            sqlx::query("INSERT INTO trade_confirmation_jobs(smart_trade_id,token_trade_id) VALUES($1,$2) ON CONFLICT(token_trade_id) DO NOTHING").bind(smart_id).bind(trade_id).execute(&mut *tx).await?;
        }
        let dedupe = format!(
            "{}:{}",
            if v.side == "BUY" {
                "trade.buy"
            } else {
                "trade.sell"
            },
            hex(&event_id)
        );
        let outbox = EventOutboxRepository::append_in_transaction(
            &mut tx,
            &NewOutboxEvent {
                event_type: if v.side == "BUY" {
                    "trade.buy"
                } else {
                    "trade.sell"
                },
                schema_version: v.schema_version,
                aggregate_type: Some("token_trade"),
                aggregate_id: Some(trade_id),
                dedupe_key: &dedupe,
                payload: v.outbox_payload,
            },
        )
        .await?;
        tx.commit().await?;
        Ok(PersistedTrade {
            trade_id,
            raw_log_id: raw_id,
            event_id,
            outbox_seq: outbox.seq,
        })
    }

    pub async fn persist_refund(
        &self,
        v: &PersistCurveRefund<'_>,
    ) -> Result<(), TradePersistenceError> {
        let mut tx = self.pool.begin().await?;
        let raw_id = insert_raw(
            &mut tx,
            v.chain_id,
            v.block_number,
            v.block_hash,
            v.tx_hash,
            v.log_index,
            v.curve,
            v.topics,
            v.data,
        )
        .await?;
        let event_type = "PONS_V2_CURVE_BUY_REFUNDED";
        let event_id = insert_normalized(
            &mut tx,
            raw_id,
            v.chain_id,
            v.tx_hash,
            v.log_index,
            event_type,
            v.parser_version,
            v.schema_version,
            v.normalized_payload,
        )
        .await?;
        let ok:Option<i32>=sqlx::query_scalar(
            r"INSERT INTO curve_accounting_events(chain_id,token_id,curve_address,event_type,actor,amount_raw,block_number,block_hash,tx_hash,transaction_index,log_index,block_time,raw_log_id,normalized_event_id)
              VALUES($1::numeric,$2,$3,$4,$5,$6,$7::numeric,$8,$9,$10::numeric,$11::numeric,$12,$13,$14)
              ON CONFLICT(chain_id,tx_hash,log_index,event_type) DO UPDATE SET tx_hash=EXCLUDED.tx_hash
              WHERE curve_accounting_events.token_id=EXCLUDED.token_id AND curve_accounting_events.curve_address=EXCLUDED.curve_address
                AND curve_accounting_events.actor=EXCLUDED.actor AND curve_accounting_events.amount_raw=EXCLUDED.amount_raw
              RETURNING 1",
        ).bind(v.chain_id.get().to_string()).bind(v.token_id).bind(v.curve.as_bytes().as_slice()).bind(event_type)
         .bind(v.buyer.as_bytes().as_slice()).bind(v.refund_raw).bind(v.block_number.get().to_string())
         .bind(v.block_hash.as_bytes().as_slice()).bind(v.tx_hash.as_bytes().as_slice())
         .bind(v.transaction_index.map(|n|n.to_string())).bind(v.log_index.get().to_string())
         .bind(v.block_time).bind(raw_id).bind(event_id).fetch_optional(&mut *tx).await?;
        if ok.is_none() {
            return Err(TradePersistenceError::Conflict);
        }
        tx.commit().await?;
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
async fn insert_raw(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    chain: ChainId,
    block: BlockNumber,
    block_hash: BlockHash,
    tx_hash: TxHash,
    log_index: LogIndex,
    curve: CurveAddress,
    topics: &Value,
    data: &[u8],
) -> Result<Uuid, sqlx::Error> {
    sqlx::query_scalar(r"INSERT INTO raw_chain_logs(chain_id,block_number,block_hash,tx_hash,log_index,address,topics,data,status) VALUES($1::numeric,$2::numeric,$3,$4,$5::numeric,$6,$7,$8,'PENDING') ON CONFLICT(chain_id,tx_hash,log_index) DO UPDATE SET tx_hash=EXCLUDED.tx_hash WHERE raw_chain_logs.block_number=EXCLUDED.block_number AND raw_chain_logs.block_hash=EXCLUDED.block_hash AND raw_chain_logs.address=EXCLUDED.address AND raw_chain_logs.topics=EXCLUDED.topics AND raw_chain_logs.data=EXCLUDED.data RETURNING id")
      .bind(chain.get().to_string()).bind(block.get().to_string()).bind(block_hash.as_bytes().as_slice()).bind(tx_hash.as_bytes().as_slice()).bind(log_index.get().to_string()).bind(curve.as_bytes().as_slice()).bind(topics).bind(data).fetch_one(&mut **tx).await
}

#[allow(clippy::too_many_arguments)]
async fn insert_normalized(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    raw: Uuid,
    chain: ChainId,
    tx_hash: TxHash,
    log_index: LogIndex,
    event_type: &str,
    parser: i32,
    schema: i32,
    payload: &Value,
) -> Result<Vec<u8>, TradePersistenceError> {
    sqlx::query_scalar(r"INSERT INTO normalized_events(raw_log_id,chain_id,tx_hash,log_index,event_type,parser_version,schema_version,payload) VALUES($1,$2::numeric,$3,$4::numeric,$5,$6,$7,$8) ON CONFLICT(raw_log_id,event_type,parser_version,schema_version) DO UPDATE SET payload=normalized_events.payload WHERE normalized_events.payload=EXCLUDED.payload RETURNING event_id")
      .bind(raw).bind(chain.get().to_string()).bind(tx_hash.as_bytes().as_slice()).bind(log_index.get().to_string()).bind(event_type).bind(parser).bind(schema).bind(payload).fetch_optional(&mut **tx).await?.ok_or(TradePersistenceError::NormalizedConflict)
}

fn hex(value: &[u8]) -> String {
    use std::fmt::Write as _;
    value.iter().fold(
        String::with_capacity(value.len() * 2),
        |mut output, byte| {
            write!(output, "{byte:02x}").expect("writing to a String cannot fail");
            output
        },
    )
}
