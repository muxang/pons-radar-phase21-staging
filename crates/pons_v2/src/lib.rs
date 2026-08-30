//! Pons V2 deployment registry and on-chain verification. No event decoding lives here yet.
mod classification;
mod confirmation;
mod launch;
mod market;
mod metadata;
mod positions;
mod signals;
mod trades;
pub use classification::{
    ClassificationError, ClassificationWorkerSettings, IdentityClassificationWorker,
};
pub use confirmation::{
    CONFIRMATION_VERSION, ConfirmationError, ConfirmationWorkerSettings, TradeConfirmationWorker,
};
pub use launch::{
    CurveRegistry, DecodedTokenLaunch, LaunchError, TOKEN_LAUNCHED_PARSER_VERSION,
    TOKEN_LAUNCHED_SCHEMA_VERSION, TokenLaunchHandler, decode_token_launched, factory_log_filter,
    token_launched_topic,
};
pub use market::{
    CurveMath, MarketError, MarketWorker, MarketWorkerSettings, TRANSFER_PARSER_VERSION,
    TRANSFER_SCHEMA_VERSION, TokenTransferHandler, batched_token_transfer_filters,
    calculate_curve_math, token_transfer_filter,
};
pub use metadata::{
    DESCRIPTION_LIMIT, MetadataError, MetadataWorkerSettings, NAME_LIMIT, SYMBOL_LIMIT,
    TokenMetadataProfile, TokenMetadataWorker, URI_LIMIT,
};
pub use positions::{PositionError, PositionWorker, PositionWorkerSettings};
pub use signals::{
    SignalEngineConfig, SignalError, SignalWorker, SignalWorkerSettings,
    evaluate as evaluate_signals,
};
pub use trades::{
    CURVE_TRADE_PARSER_VERSION, CURVE_TRADE_SCHEMA_VERSION, CurveTradeError, CurveTradeHandler,
    DEFAULT_CURVE_FILTER_BATCH_SIZE, DEFAULT_CURVE_STREAM_SHARDS, DecodedCurveEvent,
    TradeCandidateMatcher, batched_curve_filters, bounded_curve_shards, curve_buy_refunded_topic,
    curve_buy_topic, curve_log_filter, curve_sell_topic, decode_curve_event, stable_curve_shards,
};

use std::sync::Arc;

use pons_chain::{ChainRpc, ROBINHOOD_CHAIN_ID};
use pons_domain::{BlockNumber, ChainId};
use pons_storage::repositories::{DeploymentRepository, ProtocolDeployment};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha3::{Digest, Keccak256};
use thiserror::Error;
use uuid::Uuid;

pub const PONS_V2_FACTORY_FINGERPRINT: &str = "pons-v2-factory:v1";

#[derive(Debug, Error)]
pub enum RegistryError {
    #[error("deployment not found")]
    NotFound,
    #[error("invalid deployment: {0}")]
    Invalid(String),
    #[error("deployment must be VERIFIED before it can be enabled")]
    NotVerified,
    #[error("storage error: {0}")]
    Storage(#[from] sqlx::Error),
    #[error("chain error: {0}")]
    Chain(String),
    #[error("verification evidence serialization error: {0}")]
    Serialization(String),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DeploymentHealth {
    Unverified,
    Verified,
    Degraded,
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VerificationEvidence {
    pub expected_chain_id: u64,
    pub actual_chain_id: Option<u64>,
    pub chain_id_matches: bool,
    pub bytecode_present: bool,
    pub bytecode_length: usize,
    pub observed_code_hash: Option<String>,
    pub expected_code_hash: Option<String>,
    pub code_hash_matches: Option<bool>,
    pub interface_fingerprint: String,
    pub interface_fingerprint_valid: bool,
    pub configured_event_topics_valid: bool,
    pub checks: Vec<String>,
}

#[derive(Clone)]
pub struct DeploymentRegistry {
    repository: DeploymentRepository,
    rpc: Arc<dyn ChainRpc>,
}

impl DeploymentRegistry {
    #[must_use]
    pub fn new(repository: DeploymentRepository, rpc: Arc<dyn ChainRpc>) -> Self {
        Self { repository, rpc }
    }
    #[must_use]
    pub const fn repository(&self) -> &DeploymentRepository {
        &self.repository
    }
    #[must_use]
    pub fn rpc(&self) -> Arc<dyn ChainRpc> {
        self.rpc.clone()
    }

    /// Returns only enabled, verified deployments active at the requested block.
    ///
    /// # Errors
    ///
    /// Returns a storage error if the registry cannot be loaded.
    pub async fn active_at(
        &self,
        block: BlockNumber,
    ) -> Result<Vec<ProtocolDeployment>, RegistryError> {
        Ok(self
            .repository
            .active_verified(ChainId::new(ROBINHOOD_CHAIN_ID), block)
            .await?)
    }

    /// Verifies chain identity, deployed bytecode, exact optional code hash, and configured interface evidence.
    ///
    /// # Errors
    ///
    /// Returns a chain, persistence, or explainable verification failure.
    pub async fn verify(&self, id: Uuid) -> Result<ProtocolDeployment, RegistryError> {
        let deployment = self
            .repository
            .get(id)
            .await?
            .ok_or(RegistryError::NotFound)?;
        let expected = ChainId::new(ROBINHOOD_CHAIN_ID);
        let mut evidence = VerificationEvidence {
            expected_chain_id: expected.get(),
            actual_chain_id: None,
            chain_id_matches: false,
            bytecode_present: false,
            bytecode_length: 0,
            observed_code_hash: None,
            expected_code_hash: deployment.expected_code_hash.map(|v| v.to_string()),
            code_hash_matches: None,
            interface_fingerprint: deployment.interface_fingerprint.clone(),
            interface_fingerprint_valid: deployment.interface_fingerprint
                == PONS_V2_FACTORY_FINGERPRINT,
            configured_event_topics_valid: valid_topics(&deployment.expected_event_topics),
            checks: Vec::new(),
        };
        let actual = match self.rpc.chain_id().await {
            Ok(actual) => actual,
            Err(error) => {
                evidence.checks.push("chain_id_rpc_error".into());
                return self.failed(id, evidence, &error.to_string()).await;
            }
        };
        evidence.actual_chain_id = Some(actual.get());
        evidence.chain_id_matches = actual == expected && deployment.chain_id == expected;
        if !evidence.chain_id_matches {
            evidence.checks.push("chain_id_mismatch".into());
            return self.failed(id, evidence, "chain_id mismatch").await;
        }
        let code = match self.rpc.code(deployment.address).await {
            Ok(code) => code,
            Err(error) => {
                evidence.checks.push("get_code_rpc_error".into());
                return self.failed(id, evidence, &error.to_string()).await;
            }
        };
        evidence.bytecode_present = !code.is_empty();
        evidence.bytecode_length = code.len();
        if code.is_empty() {
            evidence.checks.push("empty_bytecode".into());
            return self
                .failed(id, evidence, "address has no deployed bytecode")
                .await;
        }
        let observed = pons_domain::BlockHash::from_slice(&Keccak256::digest(&code))
            .map_err(|e| RegistryError::Invalid(e.to_string()))?;
        evidence.observed_code_hash = Some(observed.to_string());
        evidence.code_hash_matches = deployment
            .expected_code_hash
            .map(|expected| expected == observed);
        if evidence.code_hash_matches == Some(false) {
            evidence.checks.push("code_hash_mismatch".into());
            return self
                .failed(id, evidence, "expected code hash mismatch")
                .await;
        }
        if !evidence.interface_fingerprint_valid || !evidence.configured_event_topics_valid {
            evidence.checks.push("invalid_interface_evidence".into());
            return self
                .failed(id, evidence, "configured interface evidence is invalid")
                .await;
        }
        evidence.checks.push("verified".into());
        let value = serde_json::to_value(evidence)
            .map_err(|error| RegistryError::Serialization(error.to_string()))?;
        Ok(self
            .repository
            .save_verification(id, "VERIFIED", &value, None)
            .await?)
    }

    async fn failed(
        &self,
        id: Uuid,
        evidence: VerificationEvidence,
        message: &str,
    ) -> Result<ProtocolDeployment, RegistryError> {
        let value = serde_json::to_value(evidence)
            .map_err(|error| RegistryError::Serialization(error.to_string()))?;
        self.repository
            .save_verification(id, "DEGRADED", &value, Some(message))
            .await?;
        Err(RegistryError::Invalid(message.into()))
    }
}

fn valid_topics(value: &Value) -> bool {
    value.as_array().is_some_and(|topics| {
        topics.iter().all(|topic| {
            topic.as_str().is_some_and(|v| {
                v.len() == 66
                    && v.starts_with("0x")
                    && v[2..].bytes().all(|b| b.is_ascii_hexdigit())
            })
        })
    })
}

pub fn deployment_json(value: &ProtocolDeployment) -> Value {
    json!({"id":value.id,"protocol":value.protocol,"generation":value.generation,"chain_id":value.chain_id.get(),
      "address":value.address.to_string(),"start_block":value.start_block.get(),"end_block":value.end_block.map(BlockNumber::get),
      "enabled":value.enabled,"expected_event_topics":value.expected_event_topics,"expected_code_hash":value.expected_code_hash.map(|v|v.to_string()),
      "source":value.source,"interface_fingerprint":value.interface_fingerprint,"last_verified_at":value.last_verified_at,
      "health":value.health,"verification_evidence":value.verification_evidence,"verification_error":value.verification_error,
      "trust_basis":value.trust_basis,"approved_by":value.approved_by,"approved_at":value.approved_at})
}
