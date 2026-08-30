use std::{net::SocketAddr, path::Path};

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct AppConfig {
    pub app: AppSettings,
    #[serde(default)]
    pub database: DatabaseSettings,
    #[serde(default)]
    pub chain: ChainSettings,
    #[serde(default)]
    pub security: SecuritySettings,
    #[serde(default)]
    pub metadata: MetadataSettings,
    #[serde(default)]
    pub research: ResearchSettings,
    #[serde(default)]
    pub wallet_intelligence: WalletIntelligenceSettings,
    #[serde(default)]
    pub confirmation: ConfirmationSettings,
    #[serde(default)]
    pub identity_classification: IdentityClassificationSettings,
    #[serde(default)]
    pub positions: PositionSettings,
    #[serde(default)]
    pub market_worker: MarketWorkerSettings,
    #[serde(default)]
    pub signals: SignalSettings,
    #[serde(default)]
    pub realtime: RealtimeSettings,
    #[serde(default)]
    pub updater: UpdaterSettings,
    #[serde(default)]
    pub ai: AiSettings,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct AiSettings {
    pub enabled: bool,
    pub provider: String,
    pub model: String,
    pub max_concurrency: usize,
    pub requests_per_minute: u32,
    pub timeout_seconds: u64,
    pub poll_interval_ms: u64,
    pub retry_min_seconds: u64,
    pub retry_max_seconds: u64,
    pub max_attempts: i32,
    pub max_input_bytes: usize,
    pub max_output_bytes: usize,
    pub minimum_signal_score: u32,
    pub minimum_smart_buyers: u32,
    pub minimum_refresh_interval_seconds: u64,
    pub use_ai_research_in_signal: bool,
}
impl Default for AiSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            provider: "DISABLED".into(),
            model: "none".into(),
            max_concurrency: 1,
            requests_per_minute: 10,
            timeout_seconds: 30,
            poll_interval_ms: 500,
            retry_min_seconds: 5,
            retry_max_seconds: 300,
            max_attempts: 3,
            max_input_bytes: 256 * 1024,
            max_output_bytes: 64 * 1024,
            minimum_signal_score: 65,
            minimum_smart_buyers: 2,
            minimum_refresh_interval_seconds: 900,
            use_ai_research_in_signal: false,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct UpdaterSettings {
    pub enabled: bool,
    pub channel: String,
    pub github_owner: String,
    pub github_repo: String,
    pub check_interval_seconds: u64,
    pub auto_check: bool,
    pub auto_install: bool,
    pub request_timeout_seconds: u64,
    pub max_manifest_bytes: usize,
    pub max_asset_bytes: u64,
    pub service_name: String,
    pub health_base_url: String,
    /// Deployment-level trust pins. These extend, and cannot replace, the
    /// public roots compiled into the binary.
    pub deployment_pinned_trusted_keys: Vec<TrustedPublicKey>,
}
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct TrustedPublicKey {
    pub key_id: String,
    pub public_key_hex: String,
}
impl Default for UpdaterSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            channel: "stable".into(),
            github_owner: String::new(),
            github_repo: String::new(),
            check_interval_seconds: 21_600,
            auto_check: true,
            auto_install: false,
            request_timeout_seconds: 120,
            max_manifest_bytes: 256 * 1024,
            max_asset_bytes: 256 * 1024 * 1024,
            service_name: "pons-radar.service".into(),
            health_base_url: "http://127.0.0.1:3000".into(),
            deployment_pinned_trusted_keys: vec![],
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct RealtimeSettings {
    pub poll_interval_ms: u64,
    pub heartbeat_interval_seconds: u64,
    pub client_queue_capacity: usize,
    pub replay_limit_max: i64,
}
impl Default for RealtimeSettings {
    fn default() -> Self {
        Self {
            poll_interval_ms: 100,
            heartbeat_interval_seconds: 15,
            client_queue_capacity: 256,
            replay_limit_max: 500,
        }
    }
}

#[derive(Clone, Debug, Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct SignalSettings {
    pub concurrency: usize,
    pub poll_interval_ms: u64,
    pub retry_min_seconds: u64,
    pub retry_max_seconds: u64,
    pub rule_version: i32,
    pub weight_version: i32,
    pub calculation_version: i32,
    pub weights: SignalWeights,
    pub thresholds: SignalThresholds,
    pub consensus: SignalConsensus,
    pub timing: SignalTiming,
    pub tier_weights: SignalTierWeights,
}
impl Default for SignalSettings {
    fn default() -> Self {
        Self {
            concurrency: 2,
            poll_interval_ms: 500,
            retry_min_seconds: 2,
            retry_max_seconds: 300,
            rule_version: 1,
            weight_version: 1,
            calculation_version: 1,
            weights: SignalWeights::default(),
            thresholds: SignalThresholds::default(),
            consensus: SignalConsensus::default(),
            timing: SignalTiming::default(),
            tier_weights: SignalTierWeights::default(),
        }
    }
}
#[derive(Clone, Debug, Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct SignalWeights {
    pub smart_wallet: u32,
    pub pons_momentum: u32,
    pub capital_flow: u32,
    pub holder_distribution: u32,
    pub research_narrative: u32,
}
impl Default for SignalWeights {
    fn default() -> Self {
        Self {
            smart_wallet: 45,
            pons_momentum: 25,
            capital_flow: 15,
            holder_distribution: 10,
            research_narrative: 5,
        }
    }
}
#[derive(Clone, Debug, Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct SignalThresholds {
    pub watch: String,
    pub strong_watch: String,
    pub high_priority: String,
    pub cooling_score: String,
    pub cooling_exit_ratio: String,
    pub distribution_exit_ratio: String,
    pub minimum_confidence: String,
    pub high_max_launch_age_seconds: i64,
}
impl Default for SignalThresholds {
    fn default() -> Self {
        Self {
            watch: "45".into(),
            strong_watch: "65".into(),
            high_priority: "80".into(),
            cooling_score: "70".into(),
            cooling_exit_ratio: "0.40".into(),
            distribution_exit_ratio: "0.60".into(),
            minimum_confidence: "60".into(),
            high_max_launch_age_seconds: 900,
        }
    }
}
#[derive(Clone, Debug, Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct SignalConsensus {
    pub windows_seconds: Vec<i32>,
    pub minimum_independent_buyers: i64,
    pub minimum_qualified_buyers: i64,
    pub minimum_identity_confidence: String,
}
impl Default for SignalConsensus {
    fn default() -> Self {
        Self {
            windows_seconds: vec![30, 60, 180, 300, 900],
            minimum_independent_buyers: 2,
            minimum_qualified_buyers: 2,
            minimum_identity_confidence: "0.90".into(),
        }
    }
}
#[derive(Clone, Debug, Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct SignalTiming {
    pub first_minute: String,
    pub three_minutes: String,
    pub five_minutes: String,
    pub fifteen_minutes: String,
    pub later: String,
}
impl Default for SignalTiming {
    fn default() -> Self {
        Self {
            first_minute: "1.00".into(),
            three_minutes: "0.90".into(),
            five_minutes: "0.75".into(),
            fifteen_minutes: "0.60".into(),
            later: "0.40".into(),
        }
    }
}
#[derive(Clone, Debug, Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct SignalTierWeights {
    pub s: String,
    pub a: String,
    pub b: String,
    pub c: String,
    pub unranked: String,
}
impl Default for SignalTierWeights {
    fn default() -> Self {
        Self {
            s: "1.25".into(),
            a: "1.10".into(),
            b: "1.00".into(),
            c: "0.85".into(),
            unranked: "0.90".into(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct MarketWorkerSettings {
    pub concurrency: usize,
    pub poll_interval_ms: u64,
    pub retry_min_seconds: u64,
    pub retry_max_seconds: u64,
}
impl Default for MarketWorkerSettings {
    fn default() -> Self {
        Self {
            concurrency: 2,
            poll_interval_ms: 500,
            retry_min_seconds: 2,
            retry_max_seconds: 300,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct PositionSettings {
    pub concurrency: usize,
    pub poll_interval_ms: u64,
    pub retry_min_seconds: u64,
    pub retry_max_seconds: u64,
}
impl Default for PositionSettings {
    fn default() -> Self {
        Self {
            concurrency: 2,
            poll_interval_ms: 250,
            retry_min_seconds: 2,
            retry_max_seconds: 300,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct IdentityClassificationSettings {
    pub concurrency: usize,
    pub batch_size: i64,
    pub poll_interval_ms: u64,
    pub retry_min_seconds: u64,
    pub retry_max_seconds: u64,
}
impl Default for IdentityClassificationSettings {
    fn default() -> Self {
        Self {
            concurrency: 2,
            batch_size: 500,
            poll_interval_ms: 500,
            retry_min_seconds: 2,
            retry_max_seconds: 300,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ConfirmationSettings {
    pub concurrency: usize,
    pub rpc_timeout_seconds: u64,
    pub poll_interval_ms: u64,
    pub retry_min_seconds: u64,
    pub retry_max_seconds: u64,
}
impl Default for ConfirmationSettings {
    fn default() -> Self {
        Self {
            concurrency: 4,
            rpc_timeout_seconds: 10,
            poll_interval_ms: 250,
            retry_min_seconds: 2,
            retry_max_seconds: 300,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct WalletIntelligenceSettings {
    pub minimum_identity_confidence: String,
}
impl Default for WalletIntelligenceSettings {
    fn default() -> Self {
        Self {
            minimum_identity_confidence: "0.90".into(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ResearchSettings {
    pub external_enrichment_enabled: bool,
    pub metadata_recheck_seconds: u64,
}
impl Default for ResearchSettings {
    fn default() -> Self {
        Self {
            external_enrichment_enabled: false,
            metadata_recheck_seconds: 300,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct MetadataSettings {
    pub concurrency: usize,
    pub rpc_timeout_seconds: u64,
    pub poll_interval_ms: u64,
    pub retry_min_seconds: u64,
    pub retry_max_seconds: u64,
    pub historical_attempts_before_fallback: i32,
}
impl Default for MetadataSettings {
    fn default() -> Self {
        Self {
            concurrency: 4,
            rpc_timeout_seconds: 10,
            poll_interval_ms: 500,
            retry_min_seconds: 5,
            retry_max_seconds: 300,
            historical_attempts_before_fallback: 3,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct SecuritySettings {
    pub session_cookie_secure: bool,
    pub session_hours: i64,
    pub allowed_origin: String,
}
impl Default for SecuritySettings {
    fn default() -> Self {
        Self {
            session_cookie_secure: false,
            session_hours: 8,
            allowed_origin: "http://localhost:3000".into(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ChainSettings {
    pub chain_id: u64,
    pub backfill_chunk_blocks: u64,
    pub ws_reconnect_min_ms: u64,
    pub ws_reconnect_max_ms: u64,
}

impl Default for ChainSettings {
    fn default() -> Self {
        Self {
            chain_id: pons_chain::ROBINHOOD_CHAIN_ID,
            backfill_chunk_blocks: 1_000,
            ws_reconnect_min_ms: 1_000,
            ws_reconnect_max_ms: 30_000,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct DatabaseSettings {
    pub max_connections: u32,
    pub acquire_timeout_seconds: u64,
}

impl Default for DatabaseSettings {
    fn default() -> Self {
        Self {
            max_connections: 10,
            acquire_timeout_seconds: 5,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct AppSettings {
    pub bind: SocketAddr,
    pub environment: String,
    pub data_dir: String,
    pub log_level: String,
}

impl AppConfig {
    /// Loads the non-secret application settings from a TOML file.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be read or its values do not match the
    /// Phase 0 configuration schema.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        config::Config::builder()
            .add_source(config::File::from(path.as_ref()).required(true))
            .build()
            .context("failed to load configuration")?
            .try_deserialize()
            .context("invalid configuration")
    }

    /// Enforces the deployment trust-boundary on Unix production hosts. Local
    /// pins share the main configuration file, so that file must remain under
    /// root control and must not be writable by the service account.
    ///
    /// # Errors
    ///
    /// Returns an error when file metadata cannot be read or production Unix
    /// ownership/write permissions do not preserve that boundary.
    pub fn validate_release_trust_permissions(&self, config_path: impl AsRef<Path>) -> Result<()> {
        if self.app.environment != "production"
            || self.updater.deployment_pinned_trusted_keys.is_empty()
        {
            return Ok(());
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let metadata = std::fs::metadata(config_path.as_ref())
                .context("cannot inspect deployment trust configuration")?;
            if metadata.uid() != 0 || metadata.mode() & 0o022 != 0 {
                anyhow::bail!(
                    "deployment trusted keys require a root-owned config not writable by group/world"
                );
            }
        }
        #[cfg(not(unix))]
        let _ = config_path;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_phase_zero_config() {
        let parsed: AppConfig = config::Config::builder()
            .add_source(config::File::from_str(
                r#"
                [app]
                bind = "127.0.0.1:3100"
                environment = "test"
                data_dir = "./tmp"
                log_level = "debug"

                [chain]
                chain_id = 4663
                "#,
                config::FileFormat::Toml,
            ))
            .build()
            .unwrap()
            .try_deserialize()
            .unwrap();

        assert_eq!(parsed.app.bind, "127.0.0.1:3100".parse().unwrap());
        assert_eq!(parsed.app.environment, "test");
        assert_eq!(parsed.app.log_level, "debug");
        assert_eq!(parsed.database, DatabaseSettings::default());
        assert_eq!(parsed.chain.chain_id, 4663);
        assert_eq!(parsed.metadata, MetadataSettings::default());
        assert_eq!(parsed.research, ResearchSettings::default());
        assert_eq!(
            parsed.wallet_intelligence,
            WalletIntelligenceSettings::default()
        );
        assert_eq!(parsed.confirmation, ConfirmationSettings::default());
        assert_eq!(
            parsed.identity_classification,
            IdentityClassificationSettings::default()
        );
        assert_eq!(parsed.positions, PositionSettings::default());
        assert_eq!(parsed.market_worker, MarketWorkerSettings::default());
        assert_eq!(parsed.signals, SignalSettings::default());
        assert_eq!(parsed.updater, UpdaterSettings::default());
        assert_eq!(parsed.ai, AiSettings::default());
    }

    #[test]
    fn production_example_is_valid_and_keeps_research_signal_features_disabled() {
        let config: AppConfig = config::Config::builder()
            .add_source(config::File::from_str(
                include_str!("../../../config.example.toml"),
                config::FileFormat::Toml,
            ))
            .build()
            .expect("config.example.toml must remain valid TOML")
            .try_deserialize()
            .expect("config.example.toml must remain deployable");
        assert_eq!(config.chain.chain_id, 4663);
        assert!(!config.updater.auto_install);
        assert!(!config.ai.enabled);
        assert!(!config.ai.use_ai_research_in_signal);
    }
}
