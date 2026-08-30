use std::{env, path::PathBuf, time::Duration};

use anyhow::{Context, Result};
use pons_chain::{
    BackfillCoordinator, BackfillSettings, ChainHealth, ChainRpc, HttpRpcProvider,
    ROBINHOOD_CHAIN_ID, ReconnectPolicy, WsRpcProvider, WsStartupState, probe_ws_startup,
    reconnect_ws_until_ready, verify_chain_id,
};
use pons_domain::ChainId;
use pons_radar::{
    ai::{
        AiProvider, AiWorkerSettings, DisabledAiProvider, OpenAiCompatibleProvider, run_ai_worker,
    },
    alerts::run_alert_engine,
    analytics::run_trader_analytics_worker,
    auth::{AuthConfig, AuthService},
    backtests::run_backtest_worker,
    config::AppConfig,
    content::run_content_relation_worker,
    realtime::{EventHub, RealtimeSettings},
    server,
    traders::ExecutionWalletRegistry,
    updates::UpdateService,
    version::APP_VERSION,
};
use pons_storage::repositories::{
    AiResearchRepository, AlertRepository, AuthRepository, BacktestRepository,
    ChainCursorRepository, ConfirmationRepository, ContentRepository, DeploymentRepository,
    EventOutboxRepository, IdentityClassificationRepository, MarketRepository, PositionRepository,
    SignalRepository, TokenLaunchRepository, TokenMetadataRepository, TradeRepository,
    TraderAnalyticsRepository, TraderRepository, UpdateRepository,
};
use pons_storage::{Database, DatabaseConfig};
use pons_v2::{
    ClassificationWorkerSettings, ConfirmationWorkerSettings, CurveRegistry, CurveTradeHandler,
    DEFAULT_CURVE_FILTER_BATCH_SIZE, DeploymentRegistry, IdentityClassificationWorker,
    MarketWorker, MarketWorkerSettings, MetadataWorkerSettings, PositionWorker,
    PositionWorkerSettings, SignalEngineConfig, SignalWorker, SignalWorkerSettings,
    TokenLaunchHandler, TokenMetadataWorker, TokenTransferHandler, TradeConfirmationWorker,
    bounded_curve_shards, curve_log_filter, factory_log_filter, token_transfer_filter,
};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

#[allow(clippy::too_many_lines)]
#[tokio::main]
async fn main() -> Result<()> {
    if env::args().any(|value| value == "--version-json") {
        println!(
            "{}",
            serde_json::to_string(&pons_radar::version::VersionInfo::new(chrono::Utc::now()))?
        );
        return Ok(());
    }
    let config_path =
        env::var_os("PONS_CONFIG").map_or_else(|| PathBuf::from("config.toml"), PathBuf::from);
    let config = AppConfig::load(&config_path)?;
    config.validate_release_trust_permissions(&config_path)?;
    init_tracing(&config.app.log_level)?;

    let database_url = env::var("DATABASE_URL").context("DATABASE_URL must be set")?;
    let database = Database::connect(
        &database_url,
        DatabaseConfig {
            max_connections: config.database.max_connections,
            acquire_timeout: Duration::from_secs(config.database.acquire_timeout_seconds),
        },
    )
    .await
    .context("failed to initialize PostgreSQL")?;

    if config.chain.chain_id != ROBINHOOD_CHAIN_ID {
        anyhow::bail!(
            "configured chain_id {} is not Robinhood Chain {}",
            config.chain.chain_id,
            ROBINHOOD_CHAIN_ID
        );
    }
    let rpc_http_url = env::var("RH_RPC_HTTP_URL").context("RH_RPC_HTTP_URL must be set")?;
    let rpc_ws_url = env::var("RH_RPC_WS_URL").context("RH_RPC_WS_URL must be set")?;
    let expected_chain_id = ChainId::new(ROBINHOOD_CHAIN_ID);
    let chain_health = ChainHealth::default();
    let http_rpc = std::sync::Arc::new(HttpRpcProvider::new(rpc_http_url));
    verify_chain_id(http_rpc.as_ref(), expected_chain_id)
        .await
        .context("HTTP RPC chain verification failed")?;
    chain_health.mark_http_healthy().await;
    let ws_rpc = std::sync::Arc::new(WsRpcProvider::new(rpc_ws_url));
    let cancellation = CancellationToken::new();
    if probe_ws_startup(ws_rpc.as_ref(), expected_chain_id, &chain_health).await?
        == WsStartupState::Degraded
    {
        warn!("WebSocket RPC unavailable at startup; reconnecting in background");
        let ws_rpc = ws_rpc.clone();
        let health = chain_health.clone();
        let monitor_cancellation = cancellation.clone();
        tokio::spawn(async move {
            if let Err(error) = reconnect_ws_until_ready(
                ws_rpc,
                expected_chain_id,
                health,
                ReconnectPolicy {
                    minimum: Duration::from_secs(1),
                    maximum: Duration::from_secs(30),
                },
                monitor_cancellation,
            )
            .await
            {
                tracing::error!(%error, "WebSocket RPC rejected while reconnecting");
            }
        });
    }

    let started_at = chrono::Utc::now();
    let listener = TcpListener::bind(config.app.bind)
        .await
        .with_context(|| format!("failed to bind {}", config.app.bind))?;
    info!(
        bind = %config.app.bind,
        environment = %config.app.environment,
        app_version = APP_VERSION,
        chain_id = expected_chain_id.get(),
        "pons-radar started"
    );

    let deployments = DeploymentRegistry::new(
        DeploymentRepository::new(database.pool().clone()),
        http_rpc.clone(),
    );
    let launch_repository = TokenLaunchRepository::new(database.pool().clone());
    let curves = CurveRegistry::rebuild(&launch_repository).await?;
    let traders = ExecutionWalletRegistry::rebuild(
        TraderRepository::new(database.pool().clone()),
        config
            .wallet_intelligence
            .minimum_identity_confidence
            .clone(),
    )
    .await
    .context("failed to rebuild execution wallet registry")?;
    start_trader_registry_refresh(traders.clone(), cancellation.clone());
    tokio::spawn(run_trader_analytics_worker(
        TraderAnalyticsRepository::new(database.pool().clone()),
        cancellation.clone(),
    ));
    if config.ai.enabled {
        let provider: std::sync::Arc<dyn AiProvider> =
            if let Ok(secret) = env::var("PONS_AI_API_KEY") {
                std::sync::Arc::new(OpenAiCompatibleProvider::new(
                    &env::var("PONS_AI_BASE_URL")
                        .unwrap_or_else(|_| "https://api.openai.com/v1".into()),
                    secret,
                    config.ai.provider.clone(),
                    config.ai.model.clone(),
                )?)
            } else {
                tracing::warn!("AI provider is disabled because PONS_AI_API_KEY is absent");
                std::sync::Arc::new(DisabledAiProvider)
            };
        let identity = provider.identity();
        server::set_ai_runtime_status(
            &identity,
            if identity.provider == "DISABLED" {
                "DISABLED"
            } else {
                "CONFIGURED"
            },
        );
        tokio::spawn(run_ai_worker(
            AiResearchRepository::new(database.pool().clone()),
            provider,
            AiWorkerSettings {
                max_concurrency: config.ai.max_concurrency,
                request_interval: Duration::from_millis(
                    60_000 / u64::from(config.ai.requests_per_minute.max(1)),
                ),
                poll_interval: Duration::from_millis(config.ai.poll_interval_ms),
                timeout: Duration::from_secs(config.ai.timeout_seconds),
                retry_minimum: Duration::from_secs(config.ai.retry_min_seconds),
                retry_maximum: Duration::from_secs(config.ai.retry_max_seconds),
                max_attempts: config.ai.max_attempts,
                max_input_bytes: config.ai.max_input_bytes,
                max_output_bytes: config.ai.max_output_bytes,
                minimum_signal_score: i32::try_from(config.ai.minimum_signal_score).unwrap_or(100),
                minimum_smart_buyers: i64::from(config.ai.minimum_smart_buyers),
                minimum_refresh_interval: Duration::from_secs(
                    config.ai.minimum_refresh_interval_seconds,
                ),
            },
            cancellation.clone(),
        ));
    }
    tokio::spawn(run_backtest_worker(
        BacktestRepository::new(database.pool().clone()),
        cancellation.clone(),
    ));
    start_factory_indexers(
        &database,
        &deployments,
        http_rpc.clone(),
        ws_rpc.clone(),
        &config,
        chain_health.clone(),
        cancellation.clone(),
        curves.clone(),
    )
    .await?;
    start_curve_trade_supervisor(
        database.pool().clone(),
        curves.clone(),
        traders.clone(),
        deployments.rpc(),
        ws_rpc.clone(),
        &config,
        chain_health.clone(),
        cancellation.clone(),
    );
    start_token_transfer_supervisor(
        database.pool().clone(),
        curves,
        deployments.rpc(),
        ws_rpc.clone(),
        &config,
        chain_health.clone(),
        cancellation.clone(),
    );
    start_metadata_workers(
        &database,
        http_rpc_for_metadata(&deployments),
        &config,
        cancellation.clone(),
    )?;
    start_confirmation_workers(
        &database,
        http_rpc_for_metadata(&deployments),
        &config,
        cancellation.clone(),
    )?;
    start_identity_classification_workers(&database, &config, cancellation.clone())?;
    start_position_workers(&database, &config, cancellation.clone())?;
    start_market_workers(&database, deployments.rpc(), &config, cancellation.clone())?;
    start_signal_workers(&database, &config, cancellation.clone()).await?;
    let auth = build_auth(&database, &config)?;
    let update_service = if config.updater.enabled {
        let service = UpdateService::new(
            config.updater.clone(),
            UpdateRepository::new(database.pool().clone()),
            PathBuf::from(&config.app.data_dir),
            env::var("GITHUB_TOKEN").ok(),
            cancellation.clone(),
        )?;
        if config.updater.auto_check {
            let periodic = service.clone();
            let pool = database.pool().clone();
            let token = cancellation.clone();
            let interval = Duration::from_secs(config.updater.check_interval_seconds.max(60));
            tokio::spawn(async move {
                loop {
                    let schema: Result<i64, _> = sqlx::query_scalar(
                        "SELECT COALESCE(max(version),0) FROM _sqlx_migrations WHERE success",
                    )
                    .fetch_one(&pool)
                    .await;
                    if let Ok(schema) = schema {
                        if let Err(error) = periodic.check(schema).await {
                            tracing::warn!(%error,"GitHub update check degraded");
                        }
                    }
                    tokio::select! {()=token.cancelled()=>return,()=tokio::time::sleep(interval)=>{}}
                }
            });
        }
        let recovery = service.clone();
        tokio::spawn(async move {
            if let Err(error) = recovery.recover_after_start().await {
                tracing::error!(%error,"pending update recovery failed");
            }
        });
        Some(service)
    } else {
        None
    };
    let realtime = EventHub::new(
        EventOutboxRepository::new(database.pool().clone()),
        RealtimeSettings {
            poll_interval: Duration::from_millis(config.realtime.poll_interval_ms),
            heartbeat_interval: Duration::from_secs(config.realtime.heartbeat_interval_seconds),
            client_queue_capacity: config.realtime.client_queue_capacity,
            replay_limit_max: config.realtime.replay_limit_max,
        },
    );
    tokio::spawn(realtime.clone().run(cancellation.clone()));
    tokio::spawn(run_alert_engine(
        AlertRepository::new(database.pool().clone()),
        cancellation.clone(),
    ));
    tokio::spawn(run_content_relation_worker(
        ContentRepository::new(database.pool().clone()),
        cancellation.clone(),
    ));
    axum::serve(
        listener,
        server::router_with_runtime(
            started_at,
            database,
            deployments,
            auth,
            traders,
            realtime,
            chain_health,
            update_service,
        ),
    )
    .with_graceful_shutdown(shutdown_signal(cancellation))
    .await
    .context("server failed")?;
    info!("pons-radar stopped gracefully");
    Ok(())
}

async fn start_signal_workers(
    database: &Database,
    config: &AppConfig,
    cancellation: CancellationToken,
) -> Result<()> {
    use std::str::FromStr;
    let value = &config.signals;
    let decimal = |raw: &str| {
        rust_decimal::Decimal::from_str(raw)
            .map_err(|error| anyhow::anyhow!("invalid signal decimal {raw}: {error}"))
    };
    let engine = SignalEngineConfig {
        windows: value.consensus.windows_seconds.clone(),
        minimum_independent: value.consensus.minimum_independent_buyers,
        minimum_qualified: value.consensus.minimum_qualified_buyers,
        minimum_identity: decimal(&value.consensus.minimum_identity_confidence)?,
        watch: decimal(&value.thresholds.watch)?,
        strong: decimal(&value.thresholds.strong_watch)?,
        high: decimal(&value.thresholds.high_priority)?,
        cooling: decimal(&value.thresholds.cooling_score)?,
        cooling_exit: decimal(&value.thresholds.cooling_exit_ratio)?,
        distribution_exit: decimal(&value.thresholds.distribution_exit_ratio)?,
        minimum_confidence: decimal(&value.thresholds.minimum_confidence)?,
        high_max_age_seconds: value.thresholds.high_max_launch_age_seconds,
        weights: [
            value.weights.smart_wallet,
            value.weights.pons_momentum,
            value.weights.capital_flow,
            value.weights.holder_distribution,
            value.weights.research_narrative,
        ],
        timing: [
            decimal(&value.timing.first_minute)?,
            decimal(&value.timing.three_minutes)?,
            decimal(&value.timing.five_minutes)?,
            decimal(&value.timing.fifteen_minutes)?,
            decimal(&value.timing.later)?,
        ],
        tiers: [
            decimal(&value.tier_weights.s)?,
            decimal(&value.tier_weights.a)?,
            decimal(&value.tier_weights.b)?,
            decimal(&value.tier_weights.c)?,
            decimal(&value.tier_weights.unranked)?,
        ],
        rule_version: value.rule_version,
        weight_version: value.weight_version,
        calculation_version: value.calculation_version,
    };
    let repository = SignalRepository::new(database.pool().clone());
    repository
        .activate_rule_set(
            value.rule_version,
            value.weight_version,
            value.calculation_version,
            &serde_json::to_value(value).context("signal configuration serialization")?,
        )
        .await?;
    let worker = SignalWorker::new(
        repository,
        engine,
        SignalWorkerSettings {
            concurrency: value.concurrency,
            poll_interval: Duration::from_millis(value.poll_interval_ms),
            retry_minimum: Duration::from_secs(value.retry_min_seconds),
            retry_maximum: Duration::from_secs(value.retry_max_seconds),
        },
    )?;
    tokio::spawn(async move {
        if let Err(error) = worker.run_until(cancellation).await {
            tracing::error!(%error,"signal worker stopped");
        }
    });
    Ok(())
}

fn start_market_workers(
    database: &Database,
    rpc: std::sync::Arc<dyn ChainRpc>,
    config: &AppConfig,
    cancellation: CancellationToken,
) -> Result<()> {
    let v = &config.market_worker;
    let worker = MarketWorker::new(
        MarketRepository::new(database.pool().clone()),
        rpc,
        MarketWorkerSettings {
            concurrency: v.concurrency,
            poll_interval: Duration::from_millis(v.poll_interval_ms),
            retry_minimum: Duration::from_secs(v.retry_min_seconds),
            retry_maximum: Duration::from_secs(v.retry_max_seconds),
        },
    )?;
    tokio::spawn(async move {
        if let Err(error) = worker.run_until(cancellation).await {
            tracing::error!(%error,"market rebuild worker stopped");
        }
    });
    Ok(())
}

fn start_token_transfer_supervisor(
    pool: sqlx::PgPool,
    curves: CurveRegistry,
    http: std::sync::Arc<dyn ChainRpc>,
    websocket: std::sync::Arc<WsRpcProvider>,
    config: &AppConfig,
    health: ChainHealth,
    cancellation: CancellationToken,
) {
    let chunk = config.chain.backfill_chunk_blocks;
    let reconnect = ReconnectPolicy {
        minimum: Duration::from_millis(config.chain.ws_reconnect_min_ms),
        maximum: Duration::from_millis(config.chain.ws_reconnect_max_ms),
    };
    tokio::spawn(async move {
        let mut registered = std::collections::HashSet::new();
        let mut group = CancellationToken::new();
        loop {
            let records = curves.records().await;
            let current: std::collections::HashSet<_> = records.iter().map(|v| v.token).collect();
            if current != registered {
                let added: Vec<_> = records
                    .iter()
                    .filter(|value| !registered.contains(&value.token))
                    .cloned()
                    .collect();
                let mut caught_up = true;
                if !registered.is_empty() {
                    for token_record in &added {
                        let one = std::slice::from_ref(token_record);
                        let coordinator = BackfillCoordinator::new(
                            ChainId::new(ROBINHOOD_CHAIN_ID),
                            format!("pons-v2:token:{}:transfers:v1", token_record.token),
                            http.clone(),
                            websocket.clone(),
                            ChainCursorRepository::new(pool.clone()),
                            std::sync::Arc::new(TokenTransferHandler::new(
                                one,
                                MarketRepository::new(pool.clone()),
                                http.clone(),
                            )),
                            token_transfer_filter(one),
                            BackfillSettings {
                                start_block: token_record.launch_block,
                                chunk_blocks: chunk,
                            },
                            reconnect,
                            health.clone(),
                        );
                        if let Err(error) = coordinator.sync_once().await {
                            tracing::error!(token=%token_record.token,%error,"new token Transfer historical backfill failed; grouped registration deferred");
                            caught_up = false;
                            break;
                        }
                    }
                }
                if !caught_up {
                    tokio::select! {()=cancellation.cancelled()=>{group.cancel();return},()=tokio::time::sleep(Duration::from_secs(1))=>{}}
                    continue;
                }
                group.cancel();
                group = CancellationToken::new();
                for (shard, values) in
                    bounded_curve_shards(&records, DEFAULT_CURVE_FILTER_BATCH_SIZE)
                {
                    let Some(start) = values.iter().map(|v| v.launch_block).min() else {
                        continue;
                    };
                    let handler = std::sync::Arc::new(TokenTransferHandler::new(
                        &values,
                        MarketRepository::new(pool.clone()),
                        http.clone(),
                    ));
                    let coordinator = BackfillCoordinator::new(
                        ChainId::new(ROBINHOOD_CHAIN_ID),
                        format!("pons-v2:token-transfers:shard-{shard}:v1"),
                        http.clone(),
                        websocket.clone(),
                        ChainCursorRepository::new(pool.clone()),
                        handler,
                        token_transfer_filter(&values),
                        BackfillSettings {
                            start_block: start,
                            chunk_blocks: chunk,
                        },
                        reconnect,
                        health.clone(),
                    );
                    let token = group.clone();
                    tokio::spawn(async move {
                        if let Err(error) = coordinator.run_until(token).await {
                            tracing::error!(shard,%error,"token transfer shard stopped");
                        }
                    });
                }
                registered = current;
            }
            tokio::select! {()=cancellation.cancelled()=>{group.cancel();return},()=tokio::time::sleep(Duration::from_secs(1))=>{}}
        }
    });
}

fn start_position_workers(
    database: &Database,
    config: &AppConfig,
    cancellation: CancellationToken,
) -> Result<()> {
    let v = &config.positions;
    let worker = PositionWorker::new(
        PositionRepository::new(database.pool().clone()),
        PositionWorkerSettings {
            concurrency: v.concurrency,
            poll_interval: Duration::from_millis(v.poll_interval_ms),
            retry_minimum: Duration::from_secs(v.retry_min_seconds),
            retry_maximum: Duration::from_secs(v.retry_max_seconds),
        },
    )?;
    tokio::spawn(async move {
        if let Err(error) = worker.run_until(cancellation).await {
            tracing::error!(%error,"position rebuild worker stopped");
        }
    });
    Ok(())
}

fn start_identity_classification_workers(
    database: &Database,
    config: &AppConfig,
    cancellation: CancellationToken,
) -> Result<()> {
    let v = &config.identity_classification;
    let worker = IdentityClassificationWorker::new(
        IdentityClassificationRepository::new(database.pool().clone()),
        ClassificationWorkerSettings {
            concurrency: v.concurrency,
            batch_size: v.batch_size,
            poll_interval: Duration::from_millis(v.poll_interval_ms),
            retry_minimum: Duration::from_secs(v.retry_min_seconds),
            retry_maximum: Duration::from_secs(v.retry_max_seconds),
        },
    )?;
    tokio::spawn(async move {
        if let Err(error) = worker.run_until(cancellation).await {
            tracing::error!(%error,"identity classification worker stopped");
        }
    });
    Ok(())
}

fn start_trader_registry_refresh(
    registry: ExecutionWalletRegistry,
    cancellation: CancellationToken,
) {
    tokio::spawn(async move {
        loop {
            tokio::select! {()=cancellation.cancelled()=>return,()=tokio::time::sleep(Duration::from_secs(5))=>{if let Err(error)=registry.refresh().await{tracing::error!(%error,"execution wallet registry refresh failed; retaining last known-good snapshot");}}}
        }
    });
}

fn http_rpc_for_metadata(deployments: &DeploymentRegistry) -> std::sync::Arc<dyn ChainRpc> {
    deployments.rpc()
}

fn start_metadata_workers(
    database: &Database,
    rpc: std::sync::Arc<dyn ChainRpc>,
    config: &AppConfig,
    cancellation: CancellationToken,
) -> Result<()> {
    let value = &config.metadata;
    let worker = TokenMetadataWorker::new(
        TokenMetadataRepository::new(database.pool().clone()),
        rpc,
        MetadataWorkerSettings {
            concurrency: value.concurrency,
            rpc_timeout: Duration::from_secs(value.rpc_timeout_seconds),
            poll_interval: Duration::from_millis(value.poll_interval_ms),
            retry_minimum: Duration::from_secs(value.retry_min_seconds),
            retry_maximum: Duration::from_secs(value.retry_max_seconds),
            refresh_interval: Duration::from_secs(config.research.metadata_recheck_seconds),
            historical_attempts_before_fallback: value.historical_attempts_before_fallback,
        },
    )?;
    tokio::spawn(async move {
        if let Err(error) = worker.run_until(cancellation).await {
            tracing::error!(%error, "token metadata worker stopped");
        }
    });
    Ok(())
}

fn start_confirmation_workers(
    database: &Database,
    rpc: std::sync::Arc<dyn ChainRpc>,
    config: &AppConfig,
    cancellation: CancellationToken,
) -> Result<()> {
    let v = &config.confirmation;
    let worker = TradeConfirmationWorker::new(
        ConfirmationRepository::new(database.pool().clone()),
        rpc,
        ConfirmationWorkerSettings {
            concurrency: v.concurrency,
            rpc_timeout: Duration::from_secs(v.rpc_timeout_seconds),
            poll_interval: Duration::from_millis(v.poll_interval_ms),
            retry_minimum: Duration::from_secs(v.retry_min_seconds),
            retry_maximum: Duration::from_secs(v.retry_max_seconds),
        },
    )?;
    tokio::spawn(async move {
        if let Err(error) = worker.run_until(cancellation).await {
            tracing::error!(%error,"trade confirmation worker stopped");
        }
    });
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn start_factory_indexers(
    database: &Database,
    deployments: &DeploymentRegistry,
    http: std::sync::Arc<HttpRpcProvider>,
    websocket: std::sync::Arc<WsRpcProvider>,
    config: &AppConfig,
    health: ChainHealth,
    cancellation: CancellationToken,
    curves: CurveRegistry,
) -> Result<()> {
    let head = http.block_number().await?;
    let active = deployments.active_at(head).await?;
    let launches = TokenLaunchRepository::new(database.pool().clone());
    for deployment in active {
        let handler = std::sync::Arc::new(TokenLaunchHandler::new(
            deployment.id,
            DeploymentRepository::new(database.pool().clone()),
            launches.clone(),
            http.clone(),
            curves.clone(),
        ));
        let coordinator = BackfillCoordinator::new(
            ChainId::new(ROBINHOOD_CHAIN_ID),
            format!("pons-v2:{}:token-launched:v1", deployment.id),
            http.clone(),
            websocket.clone(),
            ChainCursorRepository::new(database.pool().clone()),
            handler,
            factory_log_filter(&deployment),
            BackfillSettings {
                start_block: deployment.start_block,
                chunk_blocks: config.chain.backfill_chunk_blocks,
            },
            ReconnectPolicy {
                minimum: Duration::from_millis(config.chain.ws_reconnect_min_ms),
                maximum: Duration::from_millis(config.chain.ws_reconnect_max_ms),
            },
            health.clone(),
        );
        let worker_cancellation = cancellation.clone();
        tokio::spawn(async move {
            if let Err(error) = coordinator.run_until(worker_cancellation).await {
                tracing::error!(deployment_id=%deployment.id,%error,"Pons V2 factory indexer stopped");
            }
        });
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn start_curve_trade_supervisor(
    pool: sqlx::PgPool,
    curves: CurveRegistry,
    traders: ExecutionWalletRegistry,
    http: std::sync::Arc<dyn ChainRpc>,
    websocket: std::sync::Arc<WsRpcProvider>,
    config: &AppConfig,
    health: ChainHealth,
    cancellation: CancellationToken,
) {
    let chunk_blocks = config.chain.backfill_chunk_blocks;
    let reconnect = ReconnectPolicy {
        minimum: Duration::from_millis(config.chain.ws_reconnect_min_ms),
        maximum: Duration::from_millis(config.chain.ws_reconnect_max_ms),
    };
    tokio::spawn(async move {
        let mut registered = std::collections::HashSet::new();
        let mut group_cancellation = CancellationToken::new();
        loop {
            let records = curves.records().await;
            let current: std::collections::HashSet<_> =
                records.iter().map(|value| value.curve).collect();
            if current != registered {
                let added: Vec<_> = records
                    .iter()
                    .filter(|value| !registered.contains(&value.curve))
                    .cloned()
                    .collect();
                let mut caught_up = true;
                if !registered.is_empty() {
                    for curve in &added {
                        let handler = std::sync::Arc::new(
                            CurveTradeHandler::new(
                                curves.clone(),
                                TradeRepository::new(pool.clone()),
                                http.clone(),
                            )
                            .with_candidate_matcher(std::sync::Arc::new(traders.clone())),
                        );
                        let coordinator = BackfillCoordinator::new(
                            ChainId::new(ROBINHOOD_CHAIN_ID),
                            format!("pons-v2:curve:{}:trades:v1", curve.curve),
                            http.clone(),
                            websocket.clone(),
                            ChainCursorRepository::new(pool.clone()),
                            handler,
                            curve_log_filter(std::slice::from_ref(curve)),
                            BackfillSettings {
                                start_block: curve.launch_block,
                                chunk_blocks,
                            },
                            reconnect,
                            health.clone(),
                        );
                        if let Err(error) = coordinator.sync_once().await {
                            tracing::error!(curve=%curve.curve,%error,"new curve historical backfill failed; live registration deferred");
                            caught_up = false;
                            break;
                        }
                    }
                }
                if caught_up {
                    group_cancellation.cancel();
                    group_cancellation = CancellationToken::new();
                    for (shard, values) in
                        bounded_curve_shards(&records, DEFAULT_CURVE_FILTER_BATCH_SIZE)
                    {
                        let Some(start_block) = values.iter().map(|value| value.launch_block).min()
                        else {
                            continue;
                        };
                        let handler = std::sync::Arc::new(
                            CurveTradeHandler::new(
                                curves.clone(),
                                TradeRepository::new(pool.clone()),
                                http.clone(),
                            )
                            .with_candidate_matcher(std::sync::Arc::new(traders.clone())),
                        );
                        let coordinator = BackfillCoordinator::new(
                            ChainId::new(ROBINHOOD_CHAIN_ID),
                            format!("pons-v2:curve-trades:shard-{shard}:v1"),
                            http.clone(),
                            websocket.clone(),
                            ChainCursorRepository::new(pool.clone()),
                            handler,
                            curve_log_filter(&values),
                            BackfillSettings {
                                start_block,
                                chunk_blocks,
                            },
                            reconnect,
                            health.clone(),
                        );
                        let token = group_cancellation.clone();
                        tokio::spawn(async move {
                            if let Err(error) = coordinator.run_until(token).await {
                                tracing::error!(shard,%error,"curve trade shard stopped");
                            }
                        });
                    }
                    registered = current;
                }
            }
            tokio::select! {
                () = cancellation.cancelled() => { group_cancellation.cancel(); return; }
                () = tokio::time::sleep(Duration::from_secs(1)) => {}
            }
        }
    });
}

fn build_auth(database: &Database, config: &AppConfig) -> Result<AuthService> {
    let production = config.app.environment.eq_ignore_ascii_case("production");
    if production && !config.security.session_cookie_secure {
        anyhow::bail!("production requires security.session_cookie_secure=true");
    }
    if !(1..=168).contains(&config.security.session_hours) {
        anyhow::bail!("security.session_hours must be between 1 and 168");
    }
    if production && !config.security.allowed_origin.starts_with("https://") {
        anyhow::bail!("production security.allowed_origin must use HTTPS");
    }
    let setup_token = env::var("ADMIN_SETUP_TOKEN").ok();
    if setup_token.as_ref().is_some_and(|token| token.len() < 32) {
        anyhow::bail!("ADMIN_SETUP_TOKEN must contain at least 32 characters");
    }
    Ok(AuthService::new(
        AuthRepository::new(database.pool().clone()),
        AuthConfig {
            secure_cookie: config.security.session_cookie_secure,
            session_hours: config.security.session_hours,
            allowed_origin: config.security.allowed_origin.clone(),
            setup_token,
        },
    ))
}

fn init_tracing(default_level: &str) -> Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(default_level))
        .context("invalid log filter")?;
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .json()
        .try_init()
        .map_err(|error| anyhow::anyhow!("failed to initialize tracing: {error}"))
}

async fn shutdown_signal(cancellation: CancellationToken) {
    let update_handoff = cancellation.cancelled();
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            warn!(%error, "failed to install Ctrl+C handler");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(error) => warn!(%error, "failed to install SIGTERM handler"),
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = update_handoff => info!(reason = "update_handoff", "shutdown requested"),
        () = ctrl_c => info!(signal = "SIGINT", "shutdown requested"),
        () = terminate => info!(signal = "SIGTERM", "shutdown requested"),
    }
    cancellation.cancel();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn updater_cancellation_requests_graceful_server_shutdown() {
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        tokio::time::timeout(Duration::from_secs(1), shutdown_signal(cancellation))
            .await
            .expect("updater handoff cancellation must stop the server");
    }
}
