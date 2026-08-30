use alloy_primitives::U256;
use alloy_sol_types::{SolCall, sol};
use async_trait::async_trait;
use chrono::{TimeDelta, Utc};
use num_bigint::BigUint;
use pons_chain::{BatchHandler, ChainBatch, ChainRpc, LogFilter, RunError, transfer_topic};
use pons_domain::{ContractAddress, LogTopic, WalletAddress};
use pons_storage::repositories::{
    CurveObservation, MarketRepository, PersistTransfer, StoredCurve,
};
use serde_json::Value;
use std::{collections::HashMap, sync::Arc, time::Duration};
use thiserror::Error;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

sol! {
    function getReserves() external view returns (uint256 quoteReserve, uint256 tokenReserve);
    function sellableTokens() external view returns (uint256 value);
    function reservedTokens() external view returns (uint256 value);
    function realQuoteReserve() external view returns (uint256 value);
    function graduationThreshold() external view returns (uint256 value);
    function readyToGraduate() external view returns (bool value);
    function decimals() external view returns (uint8 value);
}

pub const TRANSFER_PARSER_VERSION: i32 = 1;
pub const TRANSFER_SCHEMA_VERSION: i32 = 1;
#[derive(Clone)]
pub struct TokenTransferHandler {
    tokens: Arc<HashMap<ContractAddress, StoredCurve>>,
    repo: MarketRepository,
    rpc: Arc<dyn ChainRpc>,
}
impl TokenTransferHandler {
    #[must_use]
    /// # Panics
    /// Stored token addresses are domain-validated and therefore always 20 bytes.
    pub fn new(tokens: &[StoredCurve], repo: MarketRepository, rpc: Arc<dyn ChainRpc>) -> Self {
        Self {
            tokens: Arc::new(
                tokens
                    .iter()
                    .map(|v| {
                        (
                            ContractAddress::from_slice(v.token.as_bytes())
                                .expect("token address is valid"),
                            v.clone(),
                        )
                    })
                    .collect(),
            ),
            repo,
            rpc,
        }
    }
}
#[async_trait]
impl BatchHandler for TokenTransferHandler {
    async fn handle(&self, batch: ChainBatch) -> Result<(), RunError> {
        let mut blocks: HashMap<pons_domain::BlockNumber, pons_chain::BlockHeader> = HashMap::new();
        for log in batch.logs {
            let known = self
                .tokens
                .get(&log.address)
                .ok_or_else(|| RunError::Handler("unknown Pons token transfer emitter".into()))?;
            if log.removed
                || log.topics.len() != 3
                || log.topics[0] != transfer_topic()
                || log.data.len() != 32
            {
                return Err(RunError::Handler("malformed ERC20 Transfer".into()));
            }
            let from = topic_address(&log.topics[1])?;
            let to = topic_address(&log.topics[2])?;
            let amount = alloy_primitives::U256::from_be_slice(&log.data).to_string();
            let block = if let Some(v) = blocks.get(&log.block_number) {
                v.clone()
            } else {
                let v = self
                    .rpc
                    .block(log.block_number)
                    .await?
                    .ok_or(RunError::MissingBlock(log.block_number.get()))?;
                blocks.insert(log.block_number, v.clone());
                v
            };
            let time = chrono::DateTime::from_timestamp(
                i64::try_from(block.timestamp)
                    .map_err(|_| RunError::Handler("invalid timestamp".into()))?,
                0,
            )
            .ok_or_else(|| RunError::Handler("invalid timestamp".into()))?;
            let topics = Value::Array(
                log.topics
                    .iter()
                    .map(ToString::to_string)
                    .map(Value::String)
                    .collect(),
            );
            self.repo
                .persist_transfer(&PersistTransfer {
                    chain_id: batch.chain_id,
                    token_id: known.token_id,
                    token: known.token,
                    from,
                    to,
                    amount_raw: &amount,
                    block_number: log.block_number,
                    block_hash: log.block_hash,
                    tx_hash: log.tx_hash,
                    transaction_index: log.transaction_index,
                    log_index: log.log_index,
                    block_time: time,
                    topics: &topics,
                    data: &log.data,
                })
                .await?;
        }
        Ok(())
    }
}
fn topic_address(v: &LogTopic) -> Result<WalletAddress, RunError> {
    WalletAddress::from_slice(&v.as_bytes()[12..]).map_err(|e| RunError::Handler(e.to_string()))
}
#[must_use]
/// # Panics
/// Stored token addresses are domain-validated and therefore always 20 bytes.
pub fn token_transfer_filter(tokens: &[StoredCurve]) -> LogFilter {
    LogFilter {
        addresses: tokens
            .iter()
            .map(|v| {
                ContractAddress::from_slice(v.token.as_bytes()).expect("token address is valid")
            })
            .collect(),
        topics: vec![Some(vec![transfer_topic()])],
    }
}
#[must_use]
pub fn batched_token_transfer_filters(tokens: &[StoredCurve], size: usize) -> Vec<LogFilter> {
    tokens
        .chunks(size.max(1))
        .map(token_transfer_filter)
        .collect()
}

#[derive(Clone, Copy, Debug)]
pub struct MarketWorkerSettings {
    pub concurrency: usize,
    pub poll_interval: Duration,
    pub retry_minimum: Duration,
    pub retry_maximum: Duration,
}
#[derive(Debug, Error)]
pub enum MarketError {
    #[error("invalid market worker settings")]
    Invalid,
    #[error("market storage: {0}")]
    Storage(String),
    #[error("market worker task: {0}")]
    Task(String),
}
#[derive(Clone)]
pub struct MarketWorker {
    repo: MarketRepository,
    rpc: Arc<dyn ChainRpc>,
    settings: MarketWorkerSettings,
}
impl MarketWorker {
    #[allow(clippy::missing_errors_doc)]
    pub fn new(
        repo: MarketRepository,
        rpc: Arc<dyn ChainRpc>,
        settings: MarketWorkerSettings,
    ) -> Result<Self, MarketError> {
        if settings.concurrency == 0
            || settings.poll_interval.is_zero()
            || settings.retry_minimum.is_zero()
            || settings.retry_minimum > settings.retry_maximum
        {
            return Err(MarketError::Invalid);
        }
        Ok(Self {
            repo,
            rpc,
            settings,
        })
    }
    #[allow(clippy::missing_errors_doc)]
    pub async fn run_until(self, c: CancellationToken) -> Result<(), MarketError> {
        let mut set = JoinSet::new();
        for _ in 0..self.settings.concurrency {
            let w = self.clone();
            let c = c.clone();
            set.spawn(async move { w.loop_until(c).await });
        }
        while let Some(v) = set.join_next().await {
            v.map_err(|e| MarketError::Task(e.to_string()))??;
        }
        Ok(())
    }
    async fn loop_until(&self, c: CancellationToken) -> Result<(), MarketError> {
        loop {
            if c.is_cancelled() {
                return Ok(());
            }
            self.repo.enqueue_due_snapshots().await.map_err(storage)?;
            if let Some(j) = self.repo.claim_due().await.map_err(storage)? {
                self.observe(&j).await?;
                if let Err(e) = self.repo.rebuild(&j).await {
                    let d = self
                        .settings
                        .retry_minimum
                        .saturating_mul(
                            2_u32.saturating_pow(
                                u32::try_from(j.attempts.saturating_sub(1))
                                    .unwrap_or(0)
                                    .min(20),
                            ),
                        )
                        .min(self.settings.retry_maximum);
                    self.repo
                        .retry(
                            &j,
                            &e.to_string(),
                            Utc::now() + TimeDelta::from_std(d).unwrap_or(TimeDelta::MAX),
                        )
                        .await
                        .map_err(storage)?;
                }
                continue;
            }
            tokio::select! {()=c.cancelled()=>return Ok(()),()=tokio::time::sleep(self.settings.poll_interval)=>{}}
        }
    }
    async fn observe(
        &self,
        job: &pons_storage::repositories::MarketJob,
    ) -> Result<(), MarketError> {
        let subject = self.repo.subject(job.token_id).await.map_err(storage)?;
        let (Some(token_decimals), Some(total_supply)) =
            (subject.token_decimals, subject.total_supply_raw.as_deref())
        else {
            return Ok(());
        };
        let curve = ContractAddress::from_slice(subject.curve.as_bytes())
            .map_err(|error| MarketError::Storage(error.to_string()))?;
        for block in self
            .repo
            .observation_blocks(job.token_id)
            .await
            .map_err(storage)?
        {
            // Archive state is evidence enrichment: unavailable historical state remains
            // explicitly non-exact in the snapshot and never blocks durable market replay.
            let Ok(value) = self
                .observe_block(
                    curve,
                    subject.pair_token,
                    block,
                    total_supply,
                    token_decimals,
                )
                .await
            else {
                continue;
            };
            self.repo
                .save_observation(&CurveObservation {
                    token_id: job.token_id,
                    block_number: block,
                    quote_reserve_raw: &value.0,
                    token_reserve_raw: &value.1,
                    sellable_tokens_raw: &value.2,
                    reserved_tokens_raw: &value.3,
                    real_quote_reserve_raw: &value.4,
                    graduation_threshold_raw: &value.5,
                    ready_to_graduate: value.6,
                    token_decimals,
                    quote_decimals: value.7,
                    curve_progress: &value.8.token_progress,
                    quote_progress: &value.8.quote_progress,
                    spot_price_quote: &value.8.spot_price_quote,
                    curve_implied_fdv_quote: &value.8.implied_fdv_quote,
                    integrity_warning: value.9.as_deref(),
                    evidence: &serde_json::json!({
                        "source":"ETH_CALL_AT_BLOCK", "block":block.get(),
                        "price_basis":"PONS_V2_GET_RESERVES_MARGINAL_V1"
                    }),
                })
                .await
                .map_err(storage)?;
        }
        Ok(())
    }
    #[allow(clippy::type_complexity, clippy::similar_names)]
    async fn observe_block(
        &self,
        curve: ContractAddress,
        pair: Option<pons_domain::TokenAddress>,
        block: pons_domain::BlockNumber,
        supply: &str,
        token_decimals: u8,
    ) -> Result<
        (
            String,
            String,
            String,
            String,
            String,
            String,
            bool,
            u8,
            CurveMath,
            Option<String>,
        ),
        MarketError,
    > {
        let reserves = getReservesCall::abi_decode_returns(
            &self
                .rpc
                .call(curve, getReservesCall {}.abi_encode(), block)
                .await
                .map_err(rpc_error)?,
        )
        .map_err(decode_call)?;
        let sellable = read_u256(
            self.rpc.as_ref(),
            curve,
            sellableTokensCall {}.abi_encode(),
            block,
        )
        .await?;
        let reserved = read_u256(
            self.rpc.as_ref(),
            curve,
            reservedTokensCall {}.abi_encode(),
            block,
        )
        .await?;
        let real = read_u256(
            self.rpc.as_ref(),
            curve,
            realQuoteReserveCall {}.abi_encode(),
            block,
        )
        .await?;
        let threshold = read_u256(
            self.rpc.as_ref(),
            curve,
            graduationThresholdCall {}.abi_encode(),
            block,
        )
        .await?;
        let ready = readyToGraduateCall::abi_decode_returns(
            &self
                .rpc
                .call(curve, readyToGraduateCall {}.abi_encode(), block)
                .await
                .map_err(rpc_error)?,
        )
        .map_err(decode_call)?;
        let quote_decimals = match pair {
            Some(address) if address.as_bytes().iter().any(|byte| *byte != 0) => {
                let address = ContractAddress::from_slice(address.as_bytes())
                    .map_err(|e| MarketError::Storage(e.to_string()))?;
                decimalsCall::abi_decode_returns(
                    &self
                        .rpc
                        .call(address, decimalsCall {}.abi_encode(), block)
                        .await
                        .map_err(rpc_error)?,
                )
                .map_err(decode_call)?
            }
            _ => 18,
        };
        let initial = U256::from_str_radix(supply, 10)
            .ok()
            .and_then(|v| v.checked_sub(reserved))
            .ok_or_else(|| MarketError::Storage("invalid initial sellable supply".into()))?;
        let math = calculate_curve_math(
            &initial.to_string(),
            &sellable.to_string(),
            &real.to_string(),
            &threshold.to_string(),
            &reserves.quoteReserve.to_string(),
            &reserves.tokenReserve.to_string(),
            supply,
            token_decimals,
            quote_decimals,
        )
        .ok_or_else(|| MarketError::Storage("invalid curve state math".into()))?;
        let warning = progress_diverges(&math.token_progress, &math.quote_progress)
            .then(|| "TOKEN_QUOTE_PROGRESS_DIVERGENCE".into());
        Ok((
            reserves.quoteReserve.to_string(),
            reserves.tokenReserve.to_string(),
            sellable.to_string(),
            reserved.to_string(),
            real.to_string(),
            threshold.to_string(),
            ready,
            quote_decimals,
            math,
            warning,
        ))
    }
}
async fn read_u256(
    rpc: &dyn ChainRpc,
    address: ContractAddress,
    data: Vec<u8>,
    block: pons_domain::BlockNumber,
) -> Result<U256, MarketError> {
    let bytes = rpc.call(address, data, block).await.map_err(rpc_error)?;
    if bytes.len() != 32 {
        return Err(MarketError::Storage(
            "invalid uint256 curve call response".into(),
        ));
    }
    Ok(U256::from_be_slice(&bytes))
}
#[allow(clippy::needless_pass_by_value)]
fn rpc_error(error: RunError) -> MarketError {
    MarketError::Storage(error.to_string())
}
#[allow(clippy::needless_pass_by_value)]
fn decode_call(error: alloy_sol_types::Error) -> MarketError {
    MarketError::Storage(error.to_string())
}
fn progress_diverges(a: &str, b: &str) -> bool {
    let Some(a) = big_decimal_scaled(a, 18) else {
        return false;
    };
    let Some(b) = big_decimal_scaled(b, 18) else {
        return false;
    };
    let gap = if a > b { a - b } else { b - a };
    gap > BigUint::from(5_u8) * BigUint::from(10_u8).pow(16)
}
#[allow(clippy::needless_pass_by_value)]
fn storage(e: sqlx::Error) -> MarketError {
    MarketError::Storage(e.to_string())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CurveMath {
    pub token_progress: String,
    pub quote_progress: String,
    pub spot_price_quote: String,
    pub implied_fdv_quote: String,
}
/// Exact fixed-scale calculations; callers must provide verified decimals.
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn calculate_curve_math(
    initial_sellable: &str,
    current_sellable: &str,
    real_quote: &str,
    threshold: &str,
    quote_reserve: &str,
    token_reserve: &str,
    total_supply: &str,
    token_decimals: u8,
    quote_decimals: u8,
) -> Option<CurveMath> {
    let i = big(initial_sellable)?;
    let c = big(current_sellable)?;
    if i == BigUint::ZERO || c > i {
        return None;
    }
    let rq = big(real_quote)?;
    let th = big(threshold)?;
    let qr = big(quote_reserve)?;
    let tr = big(token_reserve)?;
    let supply = big(total_supply)?;
    if th == BigUint::ZERO || tr == BigUint::ZERO {
        return None;
    }
    let progress = ratio(&(i.clone() - c), &i, 18);
    let qprogress = ratio(&rq, &th, 18);
    let price_num = qr * BigUint::from(10_u8).pow(u32::from(token_decimals));
    let price_den = tr * BigUint::from(10_u8).pow(u32::from(quote_decimals));
    let price = ratio(&price_num, &price_den, 18);
    let fdv_scaled = big_decimal_scaled(&price, 18)? * supply
        / BigUint::from(10_u8).pow(u32::from(token_decimals));
    Some(CurveMath {
        token_progress: progress,
        quote_progress: qprogress,
        spot_price_quote: price,
        implied_fdv_quote: format_scaled(&fdv_scaled, 18),
    })
}
fn big(v: &str) -> Option<BigUint> {
    v.parse().ok()
}
fn ratio(n: &BigUint, d: &BigUint, scale: u32) -> String {
    let ten = BigUint::from(10_u8).pow(scale);
    let q = n * &ten / d;
    let s = q.to_string();
    if scale == 0 {
        return s;
    }
    let width = usize::try_from(scale).ok().unwrap_or(18);
    if s.len() <= width {
        format!("0.{}{}", "0".repeat(width - s.len()), s)
    } else {
        format!("{}.{}", &s[..s.len() - width], &s[s.len() - width..])
    }
}
fn big_decimal_scaled(v: &str, scale: u32) -> Option<BigUint> {
    let digits = v.replace('.', "");
    let mut n = big(&digits)?;
    let fractional = u32::try_from(v.split('.').nth(1).map_or(0, str::len)).ok()?;
    if fractional < scale {
        n *= BigUint::from(10_u8).pow(scale - fractional);
    }
    Some(n)
}
fn format_scaled(n: &BigUint, scale: u32) -> String {
    let s = n.to_string();
    let width = usize::try_from(scale).unwrap_or(18);
    if s.len() <= width {
        format!("0.{}{}", "0".repeat(width - s.len()), s)
    } else {
        format!("{}.{}", &s[..s.len() - width], &s[s.len() - width..])
    }
}
