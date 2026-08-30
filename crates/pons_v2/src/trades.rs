use std::{collections::HashMap, sync::Arc};

use alloy_primitives::{Address, B256, Bytes, Log};
use alloy_sol_types::{SolEvent, sol};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use pons_chain::{
    BatchHandler, BlockHeader, ChainBatch, ChainLog, ChainRpc, IngestionSource, LogFilter, RunError,
};
use pons_domain::{BlockNumber, ContractAddress, CurveAddress, LogTopic, WalletAddress};
use pons_storage::repositories::{
    PersistCurveRefund, PersistCurveTrade, StoredCurve, TradeCandidateIdentity, TradeRepository,
};
use serde_json::{Value, json};
use thiserror::Error;

use crate::CurveRegistry;

pub const CURVE_TRADE_PARSER_VERSION: i32 = 1;
pub const CURVE_TRADE_SCHEMA_VERSION: i32 = 1;
pub const DEFAULT_CURVE_FILTER_BATCH_SIZE: usize = 500;
pub const DEFAULT_CURVE_STREAM_SHARDS: usize = 32;

sol! {
    event CurveBuy(address indexed buyer,address indexed recipient,uint256 quoteIn,uint256 tokensOut,uint256 fee,uint256 tax);
    event CurveSell(address indexed seller,address indexed recipient,uint256 tokensIn,uint256 quoteOut,uint256 fee,uint256 tax);
    event CurveBuyRefunded(address indexed buyer,uint256 refund);
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DecodedCurveEvent {
    Buy {
        actor: WalletAddress,
        recipient: WalletAddress,
        quote_in: String,
        tokens_out: String,
        fee: String,
        tax: String,
    },
    Sell {
        actor: WalletAddress,
        recipient: WalletAddress,
        tokens_in: String,
        quote_out: String,
        fee: String,
        tax: String,
    },
    BuyRefunded {
        buyer: WalletAddress,
        refund: String,
    },
}

impl DecodedCurveEvent {
    /// BUY recipient or SELL seller: the identity used by participant analytics.
    #[must_use]
    pub const fn market_participant(&self) -> Option<WalletAddress> {
        match self {
            Self::Buy { recipient, .. } => Some(*recipient),
            Self::Sell { actor, .. } => Some(*actor),
            Self::BuyRefunded { .. } => None,
        }
    }
    /// `CurveBuy` buyer or `CurveSell` seller as emitted by the protocol.
    #[must_use]
    pub const fn execution_actor(&self) -> WalletAddress {
        match self {
            Self::Buy { actor, .. } | Self::Sell { actor, .. } => *actor,
            Self::BuyRefunded { buyer, .. } => *buyer,
        }
    }
    /// SELL quote destination; this is never seller identity.
    #[must_use]
    pub const fn proceeds_recipient(&self) -> Option<WalletAddress> {
        match self {
            Self::Sell { recipient, .. } => Some(*recipient),
            _ => None,
        }
    }
}

#[derive(Debug, Error)]
pub enum CurveTradeError {
    #[error("event emitter is not a known Pons V2 curve")]
    UnknownCurve,
    #[error("removed log cannot create a curve fact")]
    Removed,
    #[error("unsupported curve event topic")]
    UnsupportedTopic,
    #[error("invalid curve event ABI: {0}")]
    Decode(String),
    #[error("block {0} unavailable for trade timestamp")]
    MissingBlock(u64),
    #[error("invalid block timestamp {0}")]
    InvalidTimestamp(u64),
}

#[must_use]
pub fn curve_buy_topic() -> LogTopic {
    LogTopic::new(CurveBuy::SIGNATURE_HASH)
}
#[must_use]
pub fn curve_sell_topic() -> LogTopic {
    LogTopic::new(CurveSell::SIGNATURE_HASH)
}
#[must_use]
pub fn curve_buy_refunded_topic() -> LogTopic {
    LogTopic::new(CurveBuyRefunded::SIGNATURE_HASH)
}

#[must_use]
pub fn curve_log_filter(curves: &[StoredCurve]) -> LogFilter {
    LogFilter {
        addresses: curves
            .iter()
            .map(|value| ContractAddress::new(*value.curve.as_address()))
            .collect(),
        topics: vec![Some(vec![
            curve_buy_topic(),
            curve_sell_topic(),
            curve_buy_refunded_topic(),
        ])],
    }
}

#[must_use]
pub fn batched_curve_filters(curves: &[StoredCurve], batch_size: usize) -> Vec<LogFilter> {
    if batch_size == 0 {
        return Vec::new();
    }
    let mut sorted = curves.to_vec();
    sorted.sort_by_key(|value| value.curve);
    sorted.chunks(batch_size).map(curve_log_filter).collect()
}

#[must_use]
pub fn stable_curve_shards(
    curves: &[StoredCurve],
    shard_count: usize,
) -> Vec<(usize, Vec<StoredCurve>)> {
    if shard_count == 0 {
        return Vec::new();
    }
    let mut shards: Vec<Vec<StoredCurve>> = (0..shard_count).map(|_| Vec::new()).collect();
    for curve in curves {
        let hash = curve
            .curve
            .as_bytes()
            .iter()
            .fold(0xcbf2_9ce4_8422_2325_u64, |state, byte| {
                (state ^ u64::from(*byte)).wrapping_mul(0x0100_0000_01b3)
            });
        let shard = usize::try_from(hash % u64::try_from(shard_count).unwrap_or(u64::MAX))
            .unwrap_or_default();
        shards[shard].push(curve.clone());
    }
    shards
        .into_iter()
        .enumerate()
        .filter(|(_, values)| !values.is_empty())
        .map(|(index, mut values)| {
            values.sort_by_key(|value| value.curve);
            (index, values)
        })
        .collect()
}

/// Chooses the smallest stable hash-shard count whose address filters stay bounded.
/// This keeps small registries on a small number of RPC streams while preserving the
/// configured per-filter ceiling as the registry grows.
#[must_use]
pub fn bounded_curve_shards(
    curves: &[StoredCurve],
    maximum_addresses: usize,
) -> Vec<(usize, Vec<StoredCurve>)> {
    if curves.is_empty() || maximum_addresses == 0 {
        return Vec::new();
    }
    let mut shard_count = curves.len().div_ceil(maximum_addresses).max(1);
    loop {
        let shards = stable_curve_shards(curves, shard_count);
        if shards
            .iter()
            .all(|(_, values)| values.len() <= maximum_addresses)
        {
            return shards;
        }
        shard_count += 1;
    }
}

/// Strictly decodes a trade/accounting event only when its emitter is registered.
///
/// # Errors
///
/// Rejects unknown curves, removed logs, unrelated topics, and malformed ABI.
pub fn decode_curve_event(
    log: &ChainLog,
    known: &StoredCurve,
) -> Result<DecodedCurveEvent, CurveTradeError> {
    if log.removed {
        return Err(CurveTradeError::Removed);
    }
    if log.address.as_bytes() != known.curve.as_bytes() {
        return Err(CurveTradeError::UnknownCurve);
    }
    let topic = log
        .topics
        .first()
        .ok_or(CurveTradeError::UnsupportedTopic)?;
    let alloy = alloy_log(log)?;
    if *topic == curve_buy_topic() {
        let v = CurveBuy::decode_log_validate(&alloy).map_err(decode)?.data;
        Ok(DecodedCurveEvent::Buy {
            actor: wallet(v.buyer)?,
            recipient: wallet(v.recipient)?,
            quote_in: v.quoteIn.to_string(),
            tokens_out: v.tokensOut.to_string(),
            fee: v.fee.to_string(),
            tax: v.tax.to_string(),
        })
    } else if *topic == curve_sell_topic() {
        let v = CurveSell::decode_log_validate(&alloy).map_err(decode)?.data;
        Ok(DecodedCurveEvent::Sell {
            actor: wallet(v.seller)?,
            recipient: wallet(v.recipient)?,
            tokens_in: v.tokensIn.to_string(),
            quote_out: v.quoteOut.to_string(),
            fee: v.fee.to_string(),
            tax: v.tax.to_string(),
        })
    } else if *topic == curve_buy_refunded_topic() {
        let v = CurveBuyRefunded::decode_log_validate(&alloy)
            .map_err(decode)?
            .data;
        Ok(DecodedCurveEvent::BuyRefunded {
            buyer: wallet(v.buyer)?,
            refund: v.refund.to_string(),
        })
    } else {
        Err(CurveTradeError::UnsupportedTopic)
    }
}

pub struct CurveTradeHandler {
    curves: CurveRegistry,
    trades: TradeRepository,
    rpc: Arc<dyn ChainRpc>,
    candidates: Option<Arc<dyn TradeCandidateMatcher>>,
}
#[async_trait]
pub trait TradeCandidateMatcher: Send + Sync {
    async fn matched_identity(
        &self,
        address: WalletAddress,
        at: DateTime<Utc>,
    ) -> Result<Option<TradeCandidateIdentity>, RunError>;
}
impl CurveTradeHandler {
    #[must_use]
    pub fn new(curves: CurveRegistry, trades: TradeRepository, rpc: Arc<dyn ChainRpc>) -> Self {
        Self {
            curves,
            trades,
            rpc,
            candidates: None,
        }
    }
    #[must_use]
    pub fn with_candidate_matcher(mut self, matcher: Arc<dyn TradeCandidateMatcher>) -> Self {
        self.candidates = Some(matcher);
        self
    }
}

#[allow(clippy::too_many_lines)]
#[async_trait]
impl BatchHandler for CurveTradeHandler {
    async fn handle(&self, batch: ChainBatch) -> Result<(), RunError> {
        let mut blocks: HashMap<BlockNumber, BlockHeader> = HashMap::new();
        for log in batch.logs {
            let curve = CurveAddress::from_slice(log.address.as_bytes()).map_err(handler)?;
            let known = self
                .curves
                .curve(curve)
                .await
                .ok_or_else(|| handler(CurveTradeError::UnknownCurve))?;
            let decoded = decode_curve_event(&log, &known).map_err(handler)?;
            let block = if let Some(value) = blocks.get(&log.block_number) {
                value.clone()
            } else {
                let value = self
                    .rpc
                    .block(log.block_number)
                    .await?
                    .ok_or(CurveTradeError::MissingBlock(log.block_number.get()))
                    .map_err(handler)?;
                if value.hash != log.block_hash {
                    return Err(RunError::Handler("curve log block hash mismatch".into()));
                }
                blocks.insert(log.block_number, value.clone());
                value
            };
            let time = DateTime::<Utc>::from_timestamp(
                i64::try_from(block.timestamp)
                    .map_err(|_| handler(CurveTradeError::InvalidTimestamp(block.timestamp)))?,
                0,
            )
            .ok_or(CurveTradeError::InvalidTimestamp(block.timestamp))
            .map_err(handler)?;
            let topics = Value::Array(
                log.topics
                    .iter()
                    .map(|value| Value::String(value.to_string()))
                    .collect(),
            );
            match decoded {
                DecodedCurveEvent::Buy {
                    actor,
                    recipient,
                    quote_in,
                    tokens_out,
                    fee,
                    tax,
                } => {
                    let candidate = match &self.candidates {
                        Some(v) => v.matched_identity(recipient, time).await?,
                        None => None,
                    };
                    let payload = trade_json(
                        &known,
                        "BUY",
                        actor,
                        recipient,
                        &quote_in,
                        &tokens_out,
                        &fee,
                        &tax,
                        &log,
                        time,
                    );
                    self.trades
                        .persist_trade(&PersistCurveTrade {
                            chain_id: batch.chain_id,
                            token_id: known.token_id,
                            token: known.token,
                            curve: known.curve,
                            event_type: "PONS_V2_CURVE_BUY",
                            side: "BUY",
                            actor,
                            recipient,
                            quote_amount_raw: &quote_in,
                            token_amount_raw: &tokens_out,
                            fee_raw: &fee,
                            tax_raw: &tax,
                            block_number: log.block_number,
                            block_hash: log.block_hash,
                            tx_hash: log.tx_hash,
                            transaction_index: log.transaction_index,
                            log_index: log.log_index,
                            block_time: time,
                            topics: &topics,
                            data: &log.data,
                            parser_version: CURVE_TRADE_PARSER_VERSION,
                            schema_version: CURVE_TRADE_SCHEMA_VERSION,
                            normalized_payload: &payload,
                            outbox_payload: &payload,
                            candidate: candidate.as_ref(),
                            classification_source: match batch.source {
                                IngestionSource::Live => "LIVE",
                                IngestionSource::ChainBackfill => "CHAIN_BACKFILL",
                            },
                            realtime_alert_eligible: batch.source == IngestionSource::Live,
                        })
                        .await
                        .map_err(handler)?;
                }
                DecodedCurveEvent::Sell {
                    actor,
                    recipient,
                    tokens_in,
                    quote_out,
                    fee,
                    tax,
                } => {
                    let candidate = match &self.candidates {
                        Some(v) => v.matched_identity(actor, time).await?,
                        None => None,
                    };
                    let payload = trade_json(
                        &known, "SELL", actor, recipient, &quote_out, &tokens_in, &fee, &tax, &log,
                        time,
                    );
                    self.trades
                        .persist_trade(&PersistCurveTrade {
                            chain_id: batch.chain_id,
                            token_id: known.token_id,
                            token: known.token,
                            curve: known.curve,
                            event_type: "PONS_V2_CURVE_SELL",
                            side: "SELL",
                            actor,
                            recipient,
                            quote_amount_raw: &quote_out,
                            token_amount_raw: &tokens_in,
                            fee_raw: &fee,
                            tax_raw: &tax,
                            block_number: log.block_number,
                            block_hash: log.block_hash,
                            tx_hash: log.tx_hash,
                            transaction_index: log.transaction_index,
                            log_index: log.log_index,
                            block_time: time,
                            topics: &topics,
                            data: &log.data,
                            parser_version: CURVE_TRADE_PARSER_VERSION,
                            schema_version: CURVE_TRADE_SCHEMA_VERSION,
                            normalized_payload: &payload,
                            outbox_payload: &payload,
                            candidate: candidate.as_ref(),
                            classification_source: match batch.source {
                                IngestionSource::Live => "LIVE",
                                IngestionSource::ChainBackfill => "CHAIN_BACKFILL",
                            },
                            realtime_alert_eligible: batch.source == IngestionSource::Live,
                        })
                        .await
                        .map_err(handler)?;
                }
                DecodedCurveEvent::BuyRefunded { buyer, refund } => {
                    let payload = json!({"event":"CurveBuyRefunded","token":known.token.to_string(),"curve":known.curve.to_string(),"buyer":buyer.to_string(),"refund_raw":refund,"block_number":log.block_number.get(),"tx_hash":log.tx_hash.to_string(),"log_index":log.log_index.get()});
                    self.trades
                        .persist_refund(&PersistCurveRefund {
                            chain_id: batch.chain_id,
                            token_id: known.token_id,
                            curve: known.curve,
                            buyer,
                            refund_raw: &refund,
                            block_number: log.block_number,
                            block_hash: log.block_hash,
                            tx_hash: log.tx_hash,
                            transaction_index: log.transaction_index,
                            log_index: log.log_index,
                            block_time: time,
                            topics: &topics,
                            data: &log.data,
                            parser_version: CURVE_TRADE_PARSER_VERSION,
                            schema_version: CURVE_TRADE_SCHEMA_VERSION,
                            normalized_payload: &payload,
                        })
                        .await
                        .map_err(handler)?;
                }
            }
        }
        Ok(())
    }
}

fn alloy_log(log: &ChainLog) -> Result<Log, CurveTradeError> {
    let topics = log
        .topics
        .iter()
        .map(|v| B256::from_slice(v.as_bytes()))
        .collect();
    Log::new(
        Address::from_slice(log.address.as_bytes()),
        topics,
        Bytes::copy_from_slice(&log.data),
    )
    .ok_or_else(|| CurveTradeError::Decode("more than four topics".into()))
}
fn wallet(value: Address) -> Result<WalletAddress, CurveTradeError> {
    WalletAddress::from_slice(value.as_slice()).map_err(|e| CurveTradeError::Decode(e.to_string()))
}
fn decode(error: impl std::fmt::Display) -> CurveTradeError {
    CurveTradeError::Decode(error.to_string())
}
fn handler(error: impl std::fmt::Display) -> RunError {
    RunError::Handler(error.to_string())
}
#[allow(clippy::too_many_arguments)]
fn trade_json(
    known: &StoredCurve,
    side: &str,
    actor: WalletAddress,
    recipient: WalletAddress,
    quote: &str,
    token: &str,
    fee: &str,
    tax: &str,
    log: &ChainLog,
    time: DateTime<Utc>,
) -> Value {
    json!({"token":known.token.to_string(),"curve":known.curve.to_string(),"side":side,"actor":actor.to_string(),"recipient":recipient.to_string(),"quote_amount_raw":quote,"token_amount_raw":token,"fee_raw":fee,"tax_raw":tax,"block_number":log.block_number.get(),"block_time":time,"tx_hash":log.tx_hash.to_string(),"transaction_index":log.transaction_index,"log_index":log.log_index.get()})
}
