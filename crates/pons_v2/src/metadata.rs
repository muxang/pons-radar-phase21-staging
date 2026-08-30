use std::{sync::Arc, time::Duration};

use alloy_sol_types::{SolCall, sol};
use chrono::{DateTime, TimeDelta, Utc};
use pons_chain::{ChainRpc, RunError};
use pons_domain::{BlockNumber, ContractAddress, TokenAddress, WalletAddress};
use pons_storage::repositories::{MetadataJob, MetadataObservation, TokenMetadataRepository};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use url::Url;

sol! {
    struct TokenSocials { string twitter; string telegram; string discord; string website; string farcaster; }
    function name() external view returns (string value);
    function symbol() external view returns (string value);
    function decimals() external view returns (uint8 value);
    function totalSupply() external view returns (uint256 value);
    function getTokenInfo() external view returns (address tokenDeployer, string tokenLogo, string tokenDescription, TokenSocials tokenSocials);
}

pub const NAME_LIMIT: usize = 256;
pub const SYMBOL_LIMIT: usize = 64;
pub const URI_LIMIT: usize = 2_048;
pub const DESCRIPTION_LIMIT: usize = 8_192;

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct TokenMetadataProfile {
    pub name: String,
    pub symbol: String,
    pub decimals: u8,
    pub total_supply_raw: String,
    pub token_deployer: WalletAddress,
    pub token_logo: String,
    pub token_description: String,
    pub twitter: String,
    pub telegram: String,
    pub discord: String,
    pub website: String,
    pub farcaster: String,
    pub normalized_socials: Value,
    pub truncated_fields: Vec<&'static str>,
}

impl TokenMetadataProfile {
    /// Computes the canonical SHA-256 hash of the bounded profile.
    ///
    /// # Errors
    ///
    /// Returns an error if the profile cannot be serialized as canonical JSON.
    pub fn content_hash(&self) -> Result<[u8; 32], serde_json::Error> {
        Ok(Sha256::digest(serde_json::to_vec(self)?).into())
    }

    fn raw_json(&self) -> Result<Value, serde_json::Error> {
        serde_json::to_value(self)
    }
}

#[derive(Debug, Error)]
pub enum MetadataError {
    #[error("metadata RPC timed out")]
    Timeout,
    #[error("metadata RPC failed: {0}")]
    Rpc(String),
    #[error("invalid metadata ABI response for {method}: {error}")]
    Decode { method: &'static str, error: String },
    #[error("metadata storage failed: {0}")]
    Storage(String),
    #[error("metadata serialization failed: {0}")]
    Serialization(String),
    #[error("metadata worker task failed: {0}")]
    Task(String),
}

#[derive(Clone, Copy, Debug)]
pub struct MetadataWorkerSettings {
    pub concurrency: usize,
    pub rpc_timeout: Duration,
    pub poll_interval: Duration,
    pub retry_minimum: Duration,
    pub retry_maximum: Duration,
    pub refresh_interval: Duration,
    pub historical_attempts_before_fallback: i32,
}

#[derive(Clone)]
pub struct TokenMetadataWorker {
    repository: TokenMetadataRepository,
    rpc: Arc<dyn ChainRpc>,
    settings: MetadataWorkerSettings,
}

impl TokenMetadataWorker {
    /// Builds a bounded metadata worker.
    ///
    /// # Errors
    ///
    /// Returns an error for zero concurrency or invalid retry settings.
    pub fn new(
        repository: TokenMetadataRepository,
        rpc: Arc<dyn ChainRpc>,
        settings: MetadataWorkerSettings,
    ) -> Result<Self, MetadataError> {
        if settings.concurrency == 0
            || settings.retry_minimum.is_zero()
            || settings.retry_minimum > settings.retry_maximum
            || settings.refresh_interval.is_zero()
            || settings.historical_attempts_before_fallback < 1
        {
            return Err(MetadataError::Task(
                "invalid metadata worker settings".into(),
            ));
        }
        Ok(Self {
            repository,
            rpc,
            settings,
        })
    }

    /// Runs independent claim loops until cancellation. A failed token is retried
    /// without terminating or delaying claims made by the other loops.
    ///
    /// # Errors
    ///
    /// Returns only if a worker task panics or its persistence loop fails.
    pub async fn run_until(self, cancellation: CancellationToken) -> Result<(), MetadataError> {
        let mut tasks = JoinSet::new();
        for _ in 0..self.settings.concurrency {
            let worker = self.clone();
            let token = cancellation.clone();
            tasks.spawn(async move { worker.claim_loop(token).await });
        }
        while let Some(result) = tasks.join_next().await {
            result.map_err(|error| MetadataError::Task(error.to_string()))??;
        }
        Ok(())
    }

    async fn claim_loop(&self, cancellation: CancellationToken) -> Result<(), MetadataError> {
        loop {
            if cancellation.is_cancelled() {
                return Ok(());
            }
            let job = self
                .repository
                .claim_due()
                .await
                .map_err(|error| MetadataError::Storage(error.to_string()))?;
            if let Some(job) = job {
                if let Err(error) = self.process(&job).await {
                    let delay = retry_delay(
                        job.attempts,
                        self.settings.retry_minimum,
                        self.settings.retry_maximum,
                    );
                    let next = add_duration(Utc::now(), delay);
                    self.repository
                        .retry(
                            job.token_id,
                            &bounded_error(&error.to_string()),
                            next,
                            self.try_historical(&job),
                        )
                        .await
                        .map_err(|e| MetadataError::Storage(e.to_string()))?;
                }
                continue;
            }
            tokio::select! {
                () = cancellation.cancelled() => return Ok(()),
                () = tokio::time::sleep(self.settings.poll_interval) => {}
            }
        }
    }

    /// Processes one claimed job, exposed for deterministic integration tests.
    ///
    /// # Errors
    ///
    /// Returns RPC, ABI, timeout, or storage errors without mutating chain ingestion state.
    pub async fn process(&self, job: &MetadataJob) -> Result<(), MetadataError> {
        let (block, profile, capture_mode, exact) =
            tokio::time::timeout(self.settings.rpc_timeout, async {
                let historical_block = job.requested_block.filter(|_| self.try_historical(job));
                let (block, capture_mode, exact) = if let Some(block) = historical_block {
                    (block, "LAUNCH_BLOCK", true)
                } else {
                    let block = self
                        .rpc
                        .block_number()
                        .await
                        .map_err(|error| MetadataError::Rpc(error.to_string()))?;
                    (
                        block,
                        if job.original_exists {
                            "CURRENT"
                        } else {
                            "FIRST_AVAILABLE"
                        },
                        false,
                    )
                };
                let profile = fetch_profile(self.rpc.as_ref(), job.token, block).await?;
                Ok::<_, MetadataError>((block, profile, capture_mode, exact))
            })
            .await
            .map_err(|_| MetadataError::Timeout)??;
        let deployer_matches = profile.token_deployer == job.launch_deployer;
        let warning = (!deployer_matches)
            .then_some("getTokenInfo tokenDeployer differs from TokenLaunched deployer");
        let hash = profile
            .content_hash()
            .map_err(|error| MetadataError::Serialization(error.to_string()))?;
        let raw = profile
            .raw_json()
            .map_err(|error| MetadataError::Serialization(error.to_string()))?;
        let observed_at = Utc::now();
        let normalized = profile.normalized_socials.clone();
        self.repository
            .persist(
                &MetadataObservation {
                    token_id: job.token_id,
                    content_hash: &hash,
                    name: &profile.name,
                    symbol: &profile.symbol,
                    decimals: profile.decimals,
                    total_supply_raw: &profile.total_supply_raw,
                    token_deployer: profile.token_deployer,
                    token_logo: &profile.token_logo,
                    token_description: &profile.token_description,
                    twitter: &profile.twitter,
                    telegram: &profile.telegram,
                    discord: &profile.discord,
                    website: &profile.website,
                    farcaster: &profile.farcaster,
                    normalized_socials: &normalized,
                    raw_metadata: &raw,
                    deployer_matches_launch: deployer_matches,
                    integrity_warning: warning,
                    observed_block: block,
                    observed_at,
                    capture_mode,
                    exact_launch_snapshot: exact,
                    requested_block: job.requested_block,
                },
                add_duration(observed_at, self.settings.refresh_interval),
            )
            .await
            .map_err(|error| MetadataError::Storage(error.to_string()))?;
        Ok(())
    }

    fn try_historical(&self, job: &MetadataJob) -> bool {
        !job.original_exists
            && job.requested_block.is_some()
            && job.historical_attempts < self.settings.historical_attempts_before_fallback
    }
}

async fn fetch_profile(
    rpc: &dyn ChainRpc,
    token: TokenAddress,
    block: BlockNumber,
) -> Result<TokenMetadataProfile, MetadataError> {
    let address = ContractAddress::from_slice(token.as_bytes())
        .map_err(|error| decode("token address", error))?;
    let (name, symbol, decimals, supply, info) = tokio::try_join!(
        call(rpc, address, nameCall {}.abi_encode(), block, "name"),
        call(rpc, address, symbolCall {}.abi_encode(), block, "symbol"),
        call(
            rpc,
            address,
            decimalsCall {}.abi_encode(),
            block,
            "decimals"
        ),
        call(
            rpc,
            address,
            totalSupplyCall {}.abi_encode(),
            block,
            "totalSupply"
        ),
        call(
            rpc,
            address,
            getTokenInfoCall {}.abi_encode(),
            block,
            "getTokenInfo"
        ),
    )?;
    let name = nameCall::abi_decode_returns(&name).map_err(|e| decode("name", e))?;
    let symbol = symbolCall::abi_decode_returns(&symbol).map_err(|e| decode("symbol", e))?;
    let decimals =
        decimalsCall::abi_decode_returns(&decimals).map_err(|e| decode("decimals", e))?;
    let total_supply =
        totalSupplyCall::abi_decode_returns(&supply).map_err(|e| decode("totalSupply", e))?;
    let info =
        getTokenInfoCall::abi_decode_returns(&info).map_err(|e| decode("getTokenInfo", e))?;
    let mut truncated = Vec::new();
    let name = limit(name, NAME_LIMIT, "name", &mut truncated);
    let symbol = limit(symbol, SYMBOL_LIMIT, "symbol", &mut truncated);
    let logo = limit(info.tokenLogo, URI_LIMIT, "logo", &mut truncated);
    let description = limit(
        info.tokenDescription,
        DESCRIPTION_LIMIT,
        "description",
        &mut truncated,
    );
    let twitter = limit(
        info.tokenSocials.twitter,
        URI_LIMIT,
        "twitter",
        &mut truncated,
    );
    let telegram = limit(
        info.tokenSocials.telegram,
        URI_LIMIT,
        "telegram",
        &mut truncated,
    );
    let discord = limit(
        info.tokenSocials.discord,
        URI_LIMIT,
        "discord",
        &mut truncated,
    );
    let website = limit(
        info.tokenSocials.website,
        URI_LIMIT,
        "website",
        &mut truncated,
    );
    let farcaster = limit(
        info.tokenSocials.farcaster,
        URI_LIMIT,
        "farcaster",
        &mut truncated,
    );
    Ok(TokenMetadataProfile {
        name,
        symbol,
        decimals,
        total_supply_raw: total_supply.to_string(),
        token_deployer: WalletAddress::from_slice(info.tokenDeployer.as_slice())
            .map_err(|error| decode("getTokenInfo.tokenDeployer", error))?,
        token_logo: logo.clone(),
        token_description: description,
        normalized_socials: json!({
            "logo": normalize_url(&logo), "twitter": normalize_url(&twitter),
            "telegram": normalize_url(&telegram), "discord": normalize_url(&discord),
            "website": normalize_url(&website), "farcaster": normalize_url(&farcaster)
        }),
        twitter,
        telegram,
        discord,
        website,
        farcaster,
        truncated_fields: truncated,
    })
}

async fn call(
    rpc: &dyn ChainRpc,
    address: ContractAddress,
    data: Vec<u8>,
    block: BlockNumber,
    method: &'static str,
) -> Result<Vec<u8>, MetadataError> {
    rpc.call(address, data, block)
        .await
        .map_err(|error: RunError| MetadataError::Rpc(format!("{method}: {error}")))
}

fn decode(method: &'static str, error: impl std::fmt::Display) -> MetadataError {
    MetadataError::Decode {
        method,
        error: error.to_string(),
    }
}

fn limit(
    value: String,
    max: usize,
    field: &'static str,
    truncated: &mut Vec<&'static str>,
) -> String {
    if value.chars().count() <= max {
        value
    } else {
        truncated.push(field);
        value.chars().take(max).collect()
    }
}

fn normalize_url(value: &str) -> Option<String> {
    let parsed = Url::parse(value).ok()?;
    matches!(parsed.scheme(), "http" | "https").then(|| parsed.to_string())
}

fn retry_delay(attempts: i32, minimum: Duration, maximum: Duration) -> Duration {
    let exponent = u32::try_from(attempts.saturating_sub(1))
        .unwrap_or(0)
        .min(20);
    minimum
        .saturating_mul(2_u32.saturating_pow(exponent))
        .min(maximum)
}

fn add_duration(value: DateTime<Utc>, duration: Duration) -> DateTime<Utc> {
    value + TimeDelta::from_std(duration).unwrap_or(TimeDelta::MAX)
}

fn bounded_error(value: &str) -> String {
    value.chars().take(2_048).collect()
}
