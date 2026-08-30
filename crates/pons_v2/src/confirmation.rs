use alloy_primitives::U256;
use chrono::{TimeDelta, Utc};
use pons_chain::{ChainRpc, TransferEvidence, extract_erc20_transfers};
use pons_storage::repositories::{ConfirmationJob, ConfirmationRepository, TransferRecord};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use thiserror::Error;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

pub const CONFIRMATION_VERSION: i32 = 1;
#[derive(Clone, Copy, Debug)]
pub struct ConfirmationWorkerSettings {
    pub concurrency: usize,
    pub rpc_timeout: Duration,
    pub poll_interval: Duration,
    pub retry_minimum: Duration,
    pub retry_maximum: Duration,
}
#[derive(Debug, Error)]
pub enum ConfirmationError {
    #[error("receipt RPC timed out")]
    Timeout,
    #[error("receipt RPC failed: {0}")]
    Rpc(String),
    #[error("confirmation storage failed: {0}")]
    Storage(String),
    #[error("confirmation worker task failed: {0}")]
    Task(String),
}
#[derive(Clone)]
pub struct TradeConfirmationWorker {
    repository: ConfirmationRepository,
    rpc: Arc<dyn ChainRpc>,
    settings: ConfirmationWorkerSettings,
}
#[allow(clippy::missing_errors_doc)]
impl TradeConfirmationWorker {
    pub fn new(
        repository: ConfirmationRepository,
        rpc: Arc<dyn ChainRpc>,
        settings: ConfirmationWorkerSettings,
    ) -> Result<Self, ConfirmationError> {
        if settings.concurrency == 0
            || settings.rpc_timeout.is_zero()
            || settings.retry_minimum.is_zero()
            || settings.retry_minimum > settings.retry_maximum
        {
            return Err(ConfirmationError::Task(
                "invalid confirmation worker settings".into(),
            ));
        }
        Ok(Self {
            repository,
            rpc,
            settings,
        })
    }
    pub async fn run_until(self, cancellation: CancellationToken) -> Result<(), ConfirmationError> {
        let mut tasks = JoinSet::new();
        for _ in 0..self.settings.concurrency {
            let worker = self.clone();
            let token = cancellation.clone();
            tasks.spawn(async move { worker.claim_loop(token).await });
        }
        while let Some(v) = tasks.join_next().await {
            v.map_err(|e| ConfirmationError::Task(e.to_string()))??;
        }
        Ok(())
    }
    async fn claim_loop(&self, cancellation: CancellationToken) -> Result<(), ConfirmationError> {
        loop {
            if cancellation.is_cancelled() {
                return Ok(());
            }
            if let Some(job) = self.repository.claim_due().await.map_err(storage)? {
                if let Err(error) = self.process(&job).await {
                    let delay = retry_delay(
                        job.attempts,
                        self.settings.retry_minimum,
                        self.settings.retry_maximum,
                    );
                    self.repository
                        .retry(
                            job.smart_trade_id,
                            &bounded(&error.to_string()),
                            Utc::now() + TimeDelta::from_std(delay).unwrap_or(TimeDelta::MAX),
                        )
                        .await
                        .map_err(storage)?;
                }
                continue;
            }
            tokio::select! {()=cancellation.cancelled()=>return Ok(()),()=tokio::time::sleep(self.settings.poll_interval)=>{}}
        }
    }
    #[allow(clippy::too_many_lines)]
    pub async fn process(&self, job: &ConfirmationJob) -> Result<(), ConfirmationError> {
        if job.trade_status == "ORPHANED" {
            return self
                .reject(
                    job,
                    "REJECTED",
                    json!({"codes":["ORPHANED_PROTOCOL_EVENT"]}),
                    "protocol event is orphaned",
                )
                .await;
        }
        let receipt =
            tokio::time::timeout(self.settings.rpc_timeout, self.rpc.receipt(job.tx_hash))
                .await
                .map_err(|_| ConfirmationError::Timeout)?
                .map_err(|e| ConfirmationError::Rpc(e.to_string()))?
                .ok_or_else(|| ConfirmationError::Rpc("receipt unavailable".into()))?;
        if !receipt.succeeded
            || receipt.tx_hash != job.tx_hash
            || receipt.block_number != job.block_number
            || receipt.block_hash != job.block_hash
        {
            return self
                .reject(
                    job,
                    "INTEGRITY_CONFLICT",
                    json!({"codes":["RECEIPT_CHAIN_EVIDENCE_MISMATCH"]}),
                    "receipt chain evidence mismatch",
                )
                .await;
        }
        let transfers = match extract_erc20_transfers(&receipt) {
            Ok(values) => values,
            Err(error) => return self.reject(job,"INTEGRITY_CONFLICT",json!({"codes":["MALFORMED_TOKEN_TRANSFER_EVIDENCE"],"error":error.to_string()}),"malformed token transfer evidence").await,
        };
        let matched: Vec<_> = transfers
            .into_iter()
            .filter(|v| {
                v.token == job.token
                    && if job.side == "BUY" {
                        v.from.as_bytes() == job.curve.as_bytes() && v.to == job.wallet
                    } else {
                        v.from == job.wallet && v.to.as_bytes() == job.curve.as_bytes()
                    }
            })
            .collect();
        let total = matched
            .iter()
            .try_fold(U256::ZERO, |sum, v| {
                v.amount_raw
                    .parse::<U256>()
                    .ok()
                    .and_then(|n| sum.checked_add(n))
            })
            .ok_or_else(|| {
                ConfirmationError::Rpc("invalid or overflowing transfer amount".into())
            })?;
        let expected = job
            .token_amount_raw
            .parse::<U256>()
            .map_err(|e| ConfirmationError::Rpc(e.to_string()))?;
        if matched.is_empty() || total != expected {
            return self.reject(job,"INTEGRITY_CONFLICT",json!({"codes":["PONS_CURVE_TRADE","TRACKED_WALLET_MATCH","TOKEN_TRANSFER_AMOUNT_MISMATCH"],"expected_raw":job.token_amount_raw,"observed_raw":total.to_string()}),"token transfer evidence does not match protocol amount").await;
        }
        let owned: Vec<_> = matched
            .iter()
            .map(|v| OwnedTransfer {
                value: v.clone(),
                raw: transfer_json(v),
            })
            .collect();
        let records: Vec<_> = owned
            .iter()
            .map(|v| TransferRecord {
                token: v.value.token,
                from: v.value.from,
                to: v.value.to,
                amount_raw: &v.value.amount_raw,
                log_index: v.value.log_index,
                tx_hash: v.value.tx_hash,
                raw_log: &v.raw,
            })
            .collect();
        let codes = if job.side == "BUY" {
            vec![
                "PONS_CURVE_BUY",
                "TRACKED_RECIPIENT_MATCH",
                "TOKEN_TRANSFER_TO_RECIPIENT",
                "TOKEN_AMOUNT_MATCH",
            ]
        } else {
            vec![
                "PONS_CURVE_SELL",
                "TRACKED_SELLER_MATCH",
                "TOKEN_TRANSFER_FROM_SELLER",
                "TOKEN_AMOUNT_MATCH",
            ]
        };
        let evidence = json!({"confirmation_version":CONFIRMATION_VERSION,"codes":codes,"receipt":{"tx_hash":receipt.tx_hash.to_string(),"block_number":receipt.block_number.get(),"block_hash":receipt.block_hash.to_string(),"succeeded":receipt.succeeded},"transfer_semantics":"SUM_MATCHING_TOKEN_CURVE_WALLET_TRANSFERS","transfer_count":records.len(),"expected_amount_raw":job.token_amount_raw,"observed_amount_raw":total.to_string(),"transfers":owned.iter().map(|v|&v.raw).collect::<Vec<_>>()});
        let outbox = json!({"trader_wallet":job.wallet.to_string(),"token":job.token.to_string(),"side":job.side,"token_amount_raw":job.token_amount_raw,"quote_amount_raw":job.quote_amount_raw,"block_number":job.block_number.get(),"block_time":job.block_time,"tx_hash":job.tx_hash.to_string(),"confirmation_confidence":"1.0000","classification_source":job.classification_source,"realtime_alert_eligible":job.realtime_alert_eligible});
        self.repository
            .confirm(job, &evidence, &records, &outbox)
            .await
            .map_err(storage)
    }
    async fn reject(
        &self,
        job: &ConfirmationJob,
        level: &str,
        evidence: Value,
        error: &str,
    ) -> Result<(), ConfirmationError> {
        self.repository
            .reject(job.smart_trade_id, level, &evidence, error)
            .await
            .map_err(storage)
    }
}
#[derive(Clone)]
struct OwnedTransfer {
    value: TransferEvidence,
    raw: Value,
}
fn transfer_json(v: &TransferEvidence) -> Value {
    json!({"token":v.token.to_string(),"from":v.from.to_string(),"to":v.to.to_string(),"amount_raw":v.amount_raw,"log_index":v.log_index.get(),"tx_hash":v.tx_hash.to_string()})
}
#[allow(clippy::needless_pass_by_value)]
fn storage(e: sqlx::Error) -> ConfirmationError {
    ConfirmationError::Storage(e.to_string())
}
fn retry_delay(attempts: i32, min: Duration, max: Duration) -> Duration {
    min.saturating_mul(
        2_u32.saturating_pow(
            u32::try_from(attempts.saturating_sub(1))
                .unwrap_or(0)
                .min(20),
        ),
    )
    .min(max)
}
fn bounded(v: &str) -> String {
    v.chars().take(2048).collect()
}
