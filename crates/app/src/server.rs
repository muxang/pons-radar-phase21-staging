use std::sync::{Arc, OnceLock, RwLock};
use std::time::Instant;

use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{
        Path, Query, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use pons_storage::Database;
use pons_storage::repositories::{
    AiResearchRepository, AlertPreferenceChanges, AlertRepository, AuthRepository,
    BacktestRepository, ContentRepository, DeploymentChanges, NewBacktestExperiment,
    NewContentReference, NewProtocolDeployment, NewTrader, NewTraderWallet, TokenListQuery,
    TraderAnalyticsRepository, TraderChanges, WalletChanges, WebRepository,
};
use pons_v2::{DeploymentRegistry, PONS_V2_FACTORY_FINGERPRINT, RegistryError, deployment_json};
use rust_embed::Embed;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tower_http::{request_id::MakeRequestUuid, trace::TraceLayer};
use uuid::Uuid;

static AI_RUNTIME_STATUS: OnceLock<RwLock<Value>> = OnceLock::new();

pub fn set_ai_runtime_status(identity: &crate::ai::AiProviderIdentity, health: &str) {
    let value = json!({"provider":identity.provider,"model":identity.model,"capabilities":identity.capabilities,"health":health,"api_key_exposed":false,"use_ai_research_in_signal":false});
    let lock = AI_RUNTIME_STATUS.get_or_init(|| RwLock::new(json!({})));
    if let Ok(mut current) = lock.write() {
        *current = value;
    }
}

use crate::{
    auth::{AuthConfig, AuthError, AuthService},
    realtime::{EventEnvelope, EventHub, HelloEnvelope},
    traders::ExecutionWalletRegistry,
    version::VersionInfo,
};

#[derive(Embed)]
#[folder = "../../frontend/dist"]
struct FrontendAssets;

#[derive(Clone)]
struct AppState {
    started_at: DateTime<Utc>,
    readiness: Readiness,
    deployments: Option<DeploymentRegistry>,
    auth: Option<AuthService>,
    traders: Option<ExecutionWalletRegistry>,
    realtime: Option<EventHub>,
    chain_health: Option<pons_chain::ChainHealth>,
    updates: Option<crate::updates::UpdateService>,
}

#[derive(Clone)]
enum Readiness {
    #[cfg(test)]
    Bootstrap,
    Postgres(Database),
}

#[derive(Serialize)]
struct Health {
    status: &'static str,
}

#[cfg(test)]
pub fn router(started_at: DateTime<Utc>) -> Router {
    build_router(
        started_at,
        Readiness::Bootstrap,
        None,
        None,
        None,
        None,
        None,
        None,
    )
}

pub fn router_with_database(started_at: DateTime<Utc>, database: Database) -> Router {
    build_router(
        started_at,
        Readiness::Postgres(database),
        None,
        None,
        None,
        None,
        None,
        None,
    )
}

pub fn router_with_registry(
    started_at: DateTime<Utc>,
    database: Database,
    deployments: DeploymentRegistry,
) -> Router {
    let auth = AuthService::new(
        AuthRepository::new(database.pool().clone()),
        AuthConfig {
            secure_cookie: false,
            session_hours: 8,
            allowed_origin: "http://localhost".into(),
            setup_token: Some("test-setup-token".into()),
        },
    );
    build_router(
        started_at,
        Readiness::Postgres(database),
        Some(deployments),
        Some(auth),
        None,
        None,
        None,
        None,
    )
}

pub fn router_with_registry_and_auth(
    started_at: DateTime<Utc>,
    database: Database,
    deployments: DeploymentRegistry,
    auth: AuthService,
) -> Router {
    build_router(
        started_at,
        Readiness::Postgres(database),
        Some(deployments),
        Some(auth),
        None,
        None,
        None,
        None,
    )
}

pub fn router_with_services(
    started_at: DateTime<Utc>,
    database: Database,
    deployments: DeploymentRegistry,
    auth: AuthService,
    traders: ExecutionWalletRegistry,
) -> Router {
    build_router(
        started_at,
        Readiness::Postgres(database),
        Some(deployments),
        Some(auth),
        Some(traders),
        None,
        None,
        None,
    )
}

pub fn router_with_realtime(
    started_at: DateTime<Utc>,
    database: Database,
    deployments: DeploymentRegistry,
    auth: AuthService,
    traders: ExecutionWalletRegistry,
    realtime: EventHub,
) -> Router {
    build_router(
        started_at,
        Readiness::Postgres(database),
        Some(deployments),
        Some(auth),
        Some(traders),
        Some(realtime),
        None,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn router_with_runtime(
    started_at: DateTime<Utc>,
    database: Database,
    deployments: DeploymentRegistry,
    auth: AuthService,
    traders: ExecutionWalletRegistry,
    realtime: EventHub,
    chain_health: pons_chain::ChainHealth,
    updates: Option<crate::updates::UpdateService>,
) -> Router {
    build_router(
        started_at,
        Readiness::Postgres(database),
        Some(deployments),
        Some(auth),
        Some(traders),
        Some(realtime),
        Some(chain_health),
        updates,
    )
}

pub fn router_with_auth_and_realtime(
    started_at: DateTime<Utc>,
    database: Database,
    auth: AuthService,
    realtime: EventHub,
) -> Router {
    build_router(
        started_at,
        Readiness::Postgres(database),
        None,
        Some(auth),
        None,
        Some(realtime),
        None,
        None,
    )
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn build_router(
    started_at: DateTime<Utc>,
    readiness: Readiness,
    deployments: Option<DeploymentRegistry>,
    auth: Option<AuthService>,
    traders: Option<ExecutionWalletRegistry>,
    realtime: Option<EventHub>,
    chain_health: Option<pons_chain::ChainHealth>,
    updates: Option<crate::updates::UpdateService>,
) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/api/v1/system/version", get(version))
        .route("/api/v1/auth/setup-status", get(setup_status))
        .route("/api/v1/auth/setup", post(setup_admin))
        .route("/api/v1/auth/login", post(login))
        .route("/api/v1/auth/logout", post(logout))
        .route("/api/v1/auth/me", get(current_user))
        .route("/api/v1/events", get(replay_events))
        .route("/api/v1/alerts", get(list_alerts))
        .route("/api/v1/dashboard", get(dashboard))
        .route("/api/v1/tokens", get(list_tokens))
        .route("/api/v1/tokens/{address}", get(token_detail))
        .route("/api/v1/tokens/{address}/timeline", get(token_timeline))
        .route(
            "/api/v1/tokens/{address}/smart-money",
            get(token_smart_money),
        )
        .route(
            "/api/v1/tokens/{address}/market-snapshots",
            get(token_snapshots),
        )
        .route("/api/v1/tokens/{address}/research", get(token_research))
        .route(
            "/api/v1/tokens/{address}/ai-research",
            get(token_ai_research),
        )
        .route(
            "/api/v1/tokens/{address}/ai-research/history",
            get(token_ai_research_history),
        )
        .route(
            "/api/v1/admin/tokens/{address}/ai-research",
            post(run_token_ai_research),
        )
        .route("/api/v1/admin/ai/provider", get(ai_provider_status))
        .route("/api/v1/backtests", get(list_backtests))
        .route("/api/v1/backtests/{id}", get(get_backtest))
        .route("/api/v1/backtests/{id}/runs", get(get_backtest))
        .route("/api/v1/admin/backtests", post(create_backtest))
        .route("/api/v1/admin/backtests/{id}/run", post(run_backtest))
        .route("/api/v1/system/health", get(system_health))
        .route("/api/v1/admin/updates", get(update_status))
        .route("/api/v1/admin/updates/check", post(update_check))
        .route("/api/v1/admin/updates/install", post(update_install))
        .route("/api/v1/admin/updates/history", get(update_history))
        .route("/api/v1/content-providers", get(content_providers))
        .route("/api/v1/tokens/{address}/content", get(token_content))
        .route("/api/v1/alerts/{id}", axum::routing::patch(mark_alert))
        .route(
            "/api/v1/alert-preferences",
            get(alert_preferences).put(save_alert_preferences),
        )
        .route("/ws", get(websocket))
        .route(
            "/api/v1/admin/deployments",
            get(list_deployments).post(create_deployment),
        )
        .route("/api/v1/traders", get(list_traders))
        .route("/api/v1/traders/{id}", get(get_trader))
        .route("/api/v1/traders/{id}/activity", get(trader_activity))
        .route("/api/v1/traders/{id}/content", get(trader_content))
        .route("/api/v1/traders/{id}/analytics", get(trader_analytics))
        .route("/api/v1/traders/{id}/score", get(trader_score_as_of))
        .route(
            "/api/v1/admin/content-references",
            post(create_content_reference),
        )
        .route("/api/v1/traders/{id}/wallets", get(list_trader_wallets))
        .route("/api/v1/admin/traders", post(create_trader))
        .route(
            "/api/v1/admin/traders/{id}",
            axum::routing::patch(update_trader),
        )
        .route(
            "/api/v1/admin/traders/{id}/wallets",
            post(add_trader_wallet),
        )
        .route(
            "/api/v1/admin/trader-wallets/{id}",
            axum::routing::patch(update_trader_wallet),
        )
        .route("/api/v1/admin/traders/import-csv", post(import_traders_csv))
        .route(
            "/api/v1/admin/deployments/{id}",
            axum::routing::patch(update_deployment),
        )
        .route(
            "/api/v1/admin/deployments/{id}/verify",
            post(verify_deployment),
        )
        .route("/", get(index))
        .route("/{*path}", get(asset_or_index))
        .with_state(Arc::new(AppState {
            started_at,
            readiness,
            deployments,
            auth,
            traders,
            realtime,
            chain_health,
            updates,
        }))
        .layer(TraceLayer::new_for_http())
        .layer(axum::middleware::from_fn(add_request_id))
}

#[derive(Deserialize)]
struct Credentials {
    username: String,
    password: String,
}
fn auth(state: &AppState) -> Result<&AuthService, (StatusCode, Json<Value>)> {
    state.auth.as_ref().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({"error":"authentication unavailable"})),
    ))
}

fn realtime(state: &AppState) -> Result<&EventHub, (StatusCode, Json<Value>)> {
    state.realtime.as_ref().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({"error":"realtime service unavailable"})),
    ))
}

#[allow(clippy::unnecessary_wraps)]
fn alert_repository(state: &AppState) -> Result<AlertRepository, (StatusCode, Json<Value>)> {
    match &state.readiness {
        Readiness::Postgres(database) => Ok(AlertRepository::new(database.pool().clone())),
        #[cfg(test)]
        Readiness::Bootstrap => Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error":"alerts unavailable"})),
        )),
    }
}

#[allow(clippy::unnecessary_wraps)]
fn web_repository(state: &AppState) -> Result<WebRepository, (StatusCode, Json<Value>)> {
    match &state.readiness {
        Readiness::Postgres(database) => Ok(WebRepository::new(database.pool().clone())),
        #[cfg(test)]
        Readiness::Bootstrap => Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error":"web read models unavailable"})),
        )),
    }
}

#[allow(clippy::unnecessary_wraps)]
fn content_repository(state: &AppState) -> Result<ContentRepository, (StatusCode, Json<Value>)> {
    match &state.readiness {
        Readiness::Postgres(database) => Ok(ContentRepository::new(database.pool().clone())),
        #[cfg(test)]
        Readiness::Bootstrap => Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error":"content intelligence unavailable"})),
        )),
    }
}

#[allow(clippy::unnecessary_wraps)]
fn ai_repository(state: &AppState) -> Result<AiResearchRepository, (StatusCode, Json<Value>)> {
    match &state.readiness {
        Readiness::Postgres(database) => Ok(AiResearchRepository::new(database.pool().clone())),
        #[cfg(test)]
        Readiness::Bootstrap => Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error":"AI research unavailable"})),
        )),
    }
}
#[allow(clippy::unnecessary_wraps)]
fn backtest_repository(state: &AppState) -> Result<BacktestRepository, (StatusCode, Json<Value>)> {
    match &state.readiness {
        Readiness::Postgres(database) => Ok(BacktestRepository::new(database.pool().clone())),
        #[cfg(test)]
        Readiness::Bootstrap => Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error":"historical validation unavailable"})),
        )),
    }
}

#[allow(clippy::unnecessary_wraps)]
fn analytics_repository(
    state: &AppState,
) -> Result<TraderAnalyticsRepository, (StatusCode, Json<Value>)> {
    match &state.readiness {
        Readiness::Postgres(database) => {
            Ok(TraderAnalyticsRepository::new(database.pool().clone()))
        }
        #[cfg(test)]
        Readiness::Bootstrap => Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error":"trader analytics unavailable"})),
        )),
    }
}

fn update_service(
    state: &AppState,
) -> Result<&crate::updates::UpdateService, (StatusCode, Json<Value>)> {
    state.updates.as_ref().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({"error":"secure updater disabled or unavailable"})),
    ))
}

async fn db_schema(state: &AppState) -> Result<i64, (StatusCode, Json<Value>)> {
    match &state.readiness {
        Readiness::Postgres(database) => sqlx::query_scalar(
            "SELECT COALESCE(max(version),0) FROM _sqlx_migrations WHERE success",
        )
        .fetch_one(database.pool())
        .await
        .map_err(internal),
        #[cfg(test)]
        Readiness::Bootstrap => Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error":"database unavailable"})),
        )),
    }
}

async fn update_status(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    require_read(&state, &headers).await?;
    let value = update_service(&state)?
        .repository()
        .status()
        .await
        .map_err(internal)?;
    Ok(Json(
        json!({"current_version":crate::version::APP_VERSION,"frontend_build_id":crate::version::FRONTEND_BUILD_ID,"api_schema_version":crate::version::API_SCHEMA_VERSION,"updater":value}),
    ))
}

async fn update_check(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let session = auth(&state)?
        .require_mutation(&headers)
        .await
        .map_err(auth_error)?;
    let result = update_service(&state)?
        .check(db_schema(&state).await?)
        .await;
    auth(&state)?
        .repository()
        .audit(
            Some(session.user_id),
            "update.check",
            "updater",
            None,
            &json!({"success":result.is_ok(),"error":result.as_ref().err().map(ToString::to_string)}),
        )
        .await
        .map_err(internal)?;
    Ok(Json(json!({"available":result.map_err(internal_anyhow)?})))
}

#[derive(Deserialize)]
struct InstallConfirmation {
    confirm: bool,
}
async fn update_install(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<InstallConfirmation>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, Json<Value>)> {
    let session = auth(&state)?
        .require_mutation(&headers)
        .await
        .map_err(auth_error)?;
    if !body.confirm {
        return Err(bad("explicit install confirmation is required"));
    }
    let service = update_service(&state)?;
    let checked = service
        .check_for_install(db_schema(&state).await?)
        .await
        .map_err(internal_anyhow)?
        .ok_or_else(|| bad("no newer compatible stable release is available"))?;
    let job = service
        .stage_and_handoff(session.user_id, &checked)
        .await
        .map_err(|error| {
            if error.to_string().contains("one_active_update_install") {
                (
                    StatusCode::CONFLICT,
                    Json(json!({"error":"update already in progress"})),
                )
            } else {
                internal_anyhow(error)
            }
        })?;
    auth(&state)?
        .repository()
        .audit(
            Some(session.user_id),
            "update.install",
            "update_job",
            Some(&job.to_string()),
            &json!({"target_version":checked.verified.manifest.app_version.to_string()}),
        )
        .await
        .map_err(internal)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({"job_id":job,"state":"INSTALLING"})),
    ))
}

#[derive(Deserialize)]
struct HistoryLimit {
    limit: Option<i64>,
}
async fn update_history(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(q): Query<HistoryLimit>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    require_read(&state, &headers).await?;
    let rows = update_service(&state)?
        .repository()
        .history(q.limit.unwrap_or(50))
        .await
        .map_err(internal)?;
    Ok(Json(json!({"items":rows})))
}

async fn require_read(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<(), (StatusCode, Json<Value>)> {
    auth(state)?
        .authenticate(headers)
        .await
        .map_err(auth_error)?;
    Ok(())
}

async fn dashboard(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    require_read(&state, &headers).await?;
    Ok(Json(
        web_repository(&state)?
            .dashboard()
            .await
            .map_err(internal)?,
    ))
}

#[derive(Deserialize)]
struct TokenQuery {
    search: Option<String>,
    signal: Option<String>,
    lifecycle: Option<String>,
    has_smart_money: Option<bool>,
    min_progress: Option<String>,
    max_age_seconds: Option<i64>,
    sort: Option<String>,
    descending: Option<bool>,
    limit: Option<i64>,
    offset: Option<i64>,
}

async fn list_tokens(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(q): Query<TokenQuery>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    require_read(&state, &headers).await?;
    let sort = q.sort.as_deref().unwrap_or("launch_time");
    if !["launch_time", "score", "progress", "buyers", "holders"].contains(&sort) {
        return Err(bad("invalid sort"));
    }
    let query = TokenListQuery {
        search: q.search.as_deref().filter(|v| !v.trim().is_empty()),
        signal: q.signal.as_deref().filter(|v| !v.is_empty()),
        lifecycle: q.lifecycle.as_deref().filter(|v| !v.is_empty()),
        has_smart_money: q.has_smart_money,
        min_progress: q.min_progress.as_deref(),
        max_age_seconds: q.max_age_seconds,
        sort,
        descending: q.descending.unwrap_or(true),
        limit: q.limit.unwrap_or(50).clamp(1, 200),
        offset: q.offset.unwrap_or(0).max(0),
    };
    Ok(Json(
        web_repository(&state)?
            .tokens(&query)
            .await
            .map_err(internal)?,
    ))
}

async fn resolve_token(
    repository: &WebRepository,
    address: &str,
) -> Result<(Uuid, Value), (StatusCode, Json<Value>)> {
    let address: pons_domain::TokenAddress = address.parse().map_err(bad)?;
    let value = repository
        .token(address.as_bytes())
        .await
        .map_err(internal)?
        .ok_or((
            StatusCode::NOT_FOUND,
            Json(json!({"error":"token not found"})),
        ))?;
    let id = value
        .get("id")
        .and_then(Value::as_str)
        .and_then(|v| v.parse().ok())
        .ok_or_else(|| internal(sqlx::Error::Protocol("token read model missing id".into())))?;
    Ok((id, value))
}

async fn token_detail(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(address): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    require_read(&state, &headers).await?;
    let (_, value) = resolve_token(&web_repository(&state)?, &address).await?;
    Ok(Json(value))
}

#[derive(Deserialize)]
struct PageTimeQuery {
    before: Option<DateTime<Utc>>,
    limit: Option<i64>,
}
async fn token_timeline(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(address): Path<String>,
    Query(q): Query<PageTimeQuery>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    require_read(&state, &headers).await?;
    let repository = web_repository(&state)?;
    let (id, _) = resolve_token(&repository, &address).await?;
    Ok(Json(
        repository
            .timeline(id, q.before, q.limit.unwrap_or(100).clamp(1, 200))
            .await
            .map_err(internal)?,
    ))
}

#[derive(Deserialize)]
struct PageQuery {
    limit: Option<i64>,
    offset: Option<i64>,
}
async fn token_smart_money(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(address): Path<String>,
    Query(q): Query<PageQuery>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    require_read(&state, &headers).await?;
    let repository = web_repository(&state)?;
    let (id, _) = resolve_token(&repository, &address).await?;
    Ok(Json(
        repository
            .smart_money(
                id,
                q.limit.unwrap_or(50).clamp(1, 200),
                q.offset.unwrap_or(0).max(0),
            )
            .await
            .map_err(internal)?,
    ))
}

#[derive(Deserialize)]
struct SnapshotQuery {
    range: Option<String>,
    limit: Option<i64>,
}
async fn token_snapshots(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(address): Path<String>,
    Query(q): Query<SnapshotQuery>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    require_read(&state, &headers).await?;
    let repository = web_repository(&state)?;
    let (id, _) = resolve_token(&repository, &address).await?;
    let duration = match q.range.as_deref().unwrap_or("1h") {
        "1h" => chrono::Duration::hours(1),
        "6h" => chrono::Duration::hours(6),
        "24h" => chrono::Duration::hours(24),
        "all" => chrono::Duration::days(3650),
        _ => return Err(bad("range must be 1h, 6h, 24h, or all")),
    };
    Ok(Json(
        json!({"items":repository.snapshots(id,Utc::now()-duration,q.limit.unwrap_or(500).clamp(1,2000)).await.map_err(internal)?}),
    ))
}

async fn token_research(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(address): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    require_read(&state, &headers).await?;
    let repository = web_repository(&state)?;
    let (id, _) = resolve_token(&repository, &address).await?;
    Ok(Json(repository.research(id).await.map_err(internal)?))
}

async fn token_ai_research(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(address): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    require_read(&state, &headers).await?;
    let web = web_repository(&state)?;
    let (id, _) = resolve_token(&web, &address).await?;
    Ok(Json(
        ai_repository(&state)?
            .reports(id, 1)
            .await
            .map_err(internal)?,
    ))
}
async fn token_ai_research_history(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(address): Path<String>,
    Query(q): Query<PageQuery>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    require_read(&state, &headers).await?;
    let web = web_repository(&state)?;
    let (id, _) = resolve_token(&web, &address).await?;
    Ok(Json(
        ai_repository(&state)?
            .reports(id, q.limit.unwrap_or(50).clamp(1, 100))
            .await
            .map_err(internal)?,
    ))
}
async fn run_token_ai_research(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(address): Path<String>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, Json<Value>)> {
    let session = auth(&state)?
        .require_mutation(&headers)
        .await
        .map_err(auth_error)?;
    let web = web_repository(&state)?;
    let (token, _) = resolve_token(&web, &address).await?;
    let cutoff = Utc::now();
    let job = ai_repository(&state)?
        .enqueue(
            token,
            "CURRENT_RESEARCH",
            cutoff,
            "MANUAL",
            "ADMIN_MANUAL",
            false,
            100,
            Some(session.user_id),
        )
        .await
        .map_err(internal)?;
    audit(
        &state,
        session.user_id,
        "ai_research.enqueue",
        "token",
        token,
        json!({"job_id":job,"research_mode":"CURRENT_RESEARCH","trigger_origin":"ADMIN_MANUAL","knowledge_cutoff":cutoff}),
    )
    .await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({"job_id":job,"status":"PENDING","knowledge_cutoff":cutoff})),
    ))
}
async fn ai_provider_status(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    auth(&state)?
        .authenticate(&headers)
        .await
        .map_err(auth_error)?;
    let value = AI_RUNTIME_STATUS
        .get()
        .and_then(|lock| lock.read().ok().map(|value| value.clone()))
        .unwrap_or_else(|| json!({"provider":"DISABLED","model":"none","capabilities":[],"health":"DISABLED","api_key_exposed":false,"use_ai_research_in_signal":false}));
    Ok(Json(value))
}

async fn list_backtests(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(q): Query<PageQuery>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    require_read(&state, &headers).await?;
    Ok(Json(
        backtest_repository(&state)?
            .list(q.limit.unwrap_or(50), q.offset.unwrap_or(0))
            .await
            .map_err(internal)?,
    ))
}
async fn get_backtest(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    require_read(&state, &headers).await?;
    Ok(Json(
        backtest_repository(&state)?
            .detail(id)
            .await
            .map_err(internal)?,
    ))
}
async fn create_backtest(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, Json<Value>)> {
    let session = auth(&state)?
        .require_mutation(&headers)
        .await
        .map_err(auth_error)?;
    let input: NewBacktestExperiment = serde_json::from_value(body).map_err(|error| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({"error":format!("invalid experiment: {error}")})),
        )
    })?;
    if !matches!(
        input.knowledge_mode.as_str(),
        "KNOWLEDGE_TIME" | "EVENT_TIME_RECONSTRUCTED"
    ) {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error":"knowledge_mode must be explicit"})),
        ));
    }
    let id = backtest_repository(&state)?
        .create(
            &input,
            session.user_id,
            crate::version::APP_VERSION,
            crate::version::FRONTEND_BUILD_ID,
            i32::try_from(crate::version::API_SCHEMA_VERSION).unwrap_or(i32::MAX),
        )
        .await
        .map_err(internal)?;
    audit(&state,session.user_id,"backtest.create","backtest_experiment",id,json!({"knowledge_mode":input.knowledge_mode,"dataset_start":input.dataset_start,"dataset_end":input.dataset_end,"number_of_trials":input.number_of_trials})).await?;
    Ok((
        StatusCode::CREATED,
        Json(json!({"id":id,"status":"CREATED"})),
    ))
}
async fn run_backtest(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, Json<Value>)> {
    let session = auth(&state)?
        .require_mutation(&headers)
        .await
        .map_err(auth_error)?;
    let run = backtest_repository(&state)?
        .enqueue(id)
        .await
        .map_err(internal)?;
    audit(
        &state,
        session.user_id,
        "backtest.run",
        "backtest_experiment",
        id,
        json!({"run_id":run}),
    )
    .await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({"run_id":run,"status":"PENDING"})),
    ))
}

async fn content_providers(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    require_read(&state, &headers).await?;
    Ok(Json(
        json!({"items":content_repository(&state)?.providers().await.map_err(internal)?}),
    ))
}

async fn token_content(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(address): Path<String>,
    Query(q): Query<PageQuery>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    require_read(&state, &headers).await?;
    let web = web_repository(&state)?;
    let (token, _) = resolve_token(&web, &address).await?;
    Ok(Json(
        content_repository(&state)?
            .token_content(
                token,
                q.limit.unwrap_or(50).clamp(1, 200),
                q.offset.unwrap_or(0).max(0),
            )
            .await
            .map_err(internal)?,
    ))
}

async fn trader_content(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Query(q): Query<PageQuery>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    require_read(&state, &headers).await?;
    Ok(Json(
        content_repository(&state)?
            .trader_content(
                id,
                q.limit.unwrap_or(50).clamp(1, 200),
                q.offset.unwrap_or(0).max(0),
            )
            .await
            .map_err(internal)?,
    ))
}

async fn trader_analytics(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Query(q): Query<PageQuery>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    require_read(&state, &headers).await?;
    Ok(Json(
        analytics_repository(&state)?
            .analytics(id, q.limit.unwrap_or(100).clamp(1, 500))
            .await
            .map_err(internal)?,
    ))
}
#[derive(Deserialize)]
struct ScoreAsOfQuery {
    as_of: Option<DateTime<Utc>>,
    mode: Option<String>,
}
async fn trader_score_as_of(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Query(q): Query<ScoreAsOfQuery>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    require_read(&state, &headers).await?;
    let at = q.as_of.unwrap_or_else(Utc::now);
    let mode = q
        .mode
        .as_deref()
        .unwrap_or("KNOWLEDGE_TIME")
        .to_ascii_uppercase();
    if !matches!(mode.as_str(), "KNOWLEDGE_TIME" | "EVENT_TIME_RECONSTRUCTED") {
        return Err(bad(
            "mode must be KNOWLEDGE_TIME or EVENT_TIME_RECONSTRUCTED",
        ));
    }
    let value = analytics_repository(&state)?
        .score_as_of(id, at, &mode)
        .await
        .map_err(internal)?
        .ok_or((
            StatusCode::NOT_FOUND,
            Json(json!({"error":"no score was available as of the requested timestamp"})),
        ))?;
    Ok(Json(value))
}

#[derive(Deserialize, Serialize)]
struct CreateContentReference {
    trader_id: Uuid,
    token_id: Option<Uuid>,
    content_type: String,
    published_at: DateTime<Utc>,
    external_reference: Option<String>,
    title: Option<String>,
    summary: Option<String>,
    stance: Option<String>,
    #[serde(default)]
    narratives: Vec<String>,
}

async fn create_content_reference(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<(StatusCode, Json<Value>), (StatusCode, Json<Value>)> {
    let session = auth(&state)?
        .require_mutation(&headers)
        .await
        .map_err(auth_error)?;
    let value: CreateContentReference =
        serde_json::from_slice(&body).map_err(|e| bad(e.to_string()))?;
    if !matches!(
        value.content_type.as_str(),
        "TRADE_THESIS" | "POST" | "COMMENT" | "OTHER"
    ) {
        return Err(bad("invalid content_type"));
    }
    if value
        .stance
        .as_deref()
        .is_some_and(|v| !matches!(v, "BULLISH" | "BEARISH" | "NEUTRAL" | "UNKNOWN"))
    {
        return Err(bad("invalid stance"));
    }
    if value
        .title
        .as_ref()
        .is_some_and(|v| v.chars().count() > 256)
        || value
            .summary
            .as_ref()
            .is_some_and(|v| v.chars().count() > 4096)
        || value
            .external_reference
            .as_ref()
            .is_some_and(|v| v.len() > 2048)
        || value.narratives.len() > 32
        || value.narratives.iter().any(|v| v.chars().count() > 128)
    {
        return Err(bad("content reference exceeds a bounded field limit"));
    }
    if let Some(reference) = value.external_reference.as_deref() {
        let parsed = url::Url::parse(reference)
            .map_err(|_| bad("external_reference must be an absolute URL"))?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err(bad("external_reference must use http or https"));
        }
    }
    let canonical = serde_json::to_vec(&value).map_err(|e| bad(e.to_string()))?;
    let content_hash: [u8; 32] = Sha256::digest(canonical).into();
    let narratives = json!(value.narratives);
    let provenance =
        json!({"source":"operator_manual_reference","operator_user_id":session.user_id});
    let (id, created) = content_repository(&state)?
        .create_manual(&NewContentReference {
            trader_id: value.trader_id,
            token_id: value.token_id,
            platform: "FOMO",
            content_type: &value.content_type,
            published_at: value.published_at,
            external_reference: value.external_reference.as_deref(),
            title: value.title.as_deref(),
            summary: value.summary.as_deref(),
            stance: value.stance.as_deref(),
            narratives: &narratives,
            provenance: &provenance,
            content_hash: &content_hash,
        })
        .await
        .map_err(|e| bad(e.to_string()))?;
    if created {
        audit(&state, session.user_id, "content_reference.create", "trader_content", id,
            json!({"trader_id":value.trader_id,"token_id":value.token_id,"content_type":value.content_type,"authorization_basis":"MANUAL_REFERENCE"})).await?;
    }
    Ok((
        if created {
            StatusCode::CREATED
        } else {
            StatusCode::OK
        },
        Json(json!({"id":id,"created":created,"realtime_alert_eligible":false})),
    ))
}

async fn trader_activity(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    require_read(&state, &headers).await?;
    Ok(Json(
        web_repository(&state)?
            .trader(id)
            .await
            .map_err(internal)?
            .ok_or((StatusCode::NOT_FOUND, Json(json!({"error":"not found"}))))?,
    ))
}

async fn system_health(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    require_read(&state, &headers).await?;
    let mut value = web_repository(&state)?.system().await.map_err(internal)?;
    if let Some(health) = &state.chain_health {
        let chain = health.snapshot().await;
        value["robinhood_rpc"] = json!({
            "http":format!("{:?}",chain.http).to_uppercase(),
            "websocket":format!("{:?}",chain.websocket).to_uppercase(),
            "head":chain.head.map(pons_domain::BlockNumber::get),
            "cursor":chain.cursor.map(pons_domain::BlockNumber::get),
            "lag_blocks":chain.lag_blocks,
            "ws_reconnects":chain.ws_reconnects,
            "last_error":chain.last_error,
        });
    } else {
        value["robinhood_rpc"] = json!({"http":"UNAVAILABLE","websocket":"UNAVAILABLE"});
    }
    Ok(Json(value))
}

#[derive(Deserialize)]
struct AlertQuery {
    before: Option<DateTime<Utc>>,
    limit: Option<i64>,
}
async fn list_alerts(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(q): Query<AlertQuery>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    auth(&state)?
        .authenticate(&headers)
        .await
        .map_err(auth_error)?;
    let values = alert_repository(&state)?
        .list(q.before, q.limit.unwrap_or(50).clamp(1, 200))
        .await
        .map_err(internal)?;
    Ok(Json(Value::Array(values.into_iter().map(|v|json!({"id":v.id,"seq":v.seq,"type":v.alert_type,"severity":v.severity,"token_id":v.token_id,"trader_id":v.trader_id,"title":v.title,"message":v.message,"speech_text":v.speech_text,"payload":v.payload,"realtime_alert_eligible":v.realtime_alert_eligible,"provisional":v.provisional,"chain_finality":v.chain_finality,"event_effective_at":v.event_effective_at,"classification_source":v.classification_source,"status":v.status,"read_at":v.read_at,"acknowledged_at":v.acknowledged_at,"target_reference":v.target_reference,"created_at":v.created_at})).collect())))
}
#[derive(Deserialize)]
struct MarkAlert {
    #[serde(default)]
    read: bool,
    #[serde(default)]
    acknowledged: bool,
}
async fn mark_alert(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(v): Json<MarkAlert>,
) -> Result<StatusCode, (StatusCode, Json<Value>)> {
    auth(&state)?
        .require_mutation(&headers)
        .await
        .map_err(auth_error)?;
    if !alert_repository(&state)?
        .mark(id, v.read, v.acknowledged)
        .await
        .map_err(internal)?
    {
        return Err((StatusCode::NOT_FOUND, Json(json!({"error":"not found"}))));
    }
    Ok(StatusCode::NO_CONTENT)
}
async fn alert_preferences(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let session = auth(&state)?
        .authenticate(&headers)
        .await
        .map_err(auth_error)?;
    let v = alert_repository(&state)?
        .preferences(session.user_id)
        .await
        .map_err(internal)?;
    Ok(Json(serde_json::to_value(v).expect("serializable")))
}
async fn save_alert_preferences(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(v): Json<AlertPreferenceChanges>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let session = auth(&state)?
        .require_mutation(&headers)
        .await
        .map_err(auth_error)?;
    let saved = alert_repository(&state)?
        .save_preferences(session.user_id, &v)
        .await
        .map_err(|e| bad(e.to_string()))?;
    auth(&state)?
        .repository()
        .audit(
            Some(session.user_id),
            "alert_preferences.update",
            "alert_preferences",
            Some(&session.user_id.to_string()),
            &json!({}),
        )
        .await
        .map_err(internal)?;
    Ok(Json(serde_json::to_value(saved).expect("serializable")))
}

#[derive(Deserialize)]
struct ReplayQuery {
    #[serde(default)]
    after_seq: i64,
    through_seq: Option<i64>,
    limit: Option<i64>,
}

async fn replay_events(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<ReplayQuery>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    auth(&state)?
        .authenticate(&headers)
        .await
        .map_err(auth_error)?;
    if query.after_seq < 0 || query.through_seq.is_some_and(|v| v < query.after_seq) {
        return Err(bad("invalid replay cursor"));
    }
    let hub = realtime(&state)?;
    let high = match query.through_seq {
        Some(value) => value,
        None => hub.repository().high_watermark().await.map_err(internal)?,
    };
    let limit = query
        .limit
        .unwrap_or(100)
        .clamp(1, hub.settings().replay_limit_max);
    let events = hub
        .repository()
        .range(query.after_seq, high, limit)
        .await
        .map_err(internal)?;
    let envelopes: Vec<EventEnvelope> = events.into_iter().map(Into::into).collect();
    let next = envelopes.last().map_or(query.after_seq, |event| event.seq);
    Ok(Json(
        json!({"events":envelopes,"next_seq":next,"high_watermark":high,"has_more":next<high}),
    ))
}

async fn websocket(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Result<Response, (StatusCode, Json<Value>)> {
    auth(&state)?
        .authenticate_websocket(&headers)
        .await
        .map_err(auth_error)?;
    let hub = realtime(&state)?.clone();
    let receiver = hub.subscribe();
    let watermark = hub.repository().high_watermark().await.map_err(internal)?;
    Ok(upgrade.on_upgrade(move |socket| websocket_client(socket, hub, receiver, watermark)))
}

async fn websocket_client(
    socket: WebSocket,
    hub: EventHub,
    mut receiver: tokio::sync::broadcast::Receiver<EventEnvelope>,
    watermark: i64,
) {
    let (mut sender, mut incoming) = socket.split();
    let Ok(hello) = serde_json::to_string(&HelloEnvelope::new(watermark)) else {
        return;
    };
    if sender.send(Message::Text(hello.into())).await.is_err() {
        return;
    }
    let mut heartbeat = tokio::time::interval(hub.settings().heartbeat_interval);
    let mut last_activity = Instant::now();
    loop {
        tokio::select! {
            event = receiver.recv() => match event {
                Ok(event) if event.seq > watermark => {
                    let Ok(text) = serde_json::to_string(&event) else { continue };
                    if sender.send(Message::Text(text.into())).await.is_err() { break }
                }
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    let _ = sender.send(Message::Close(None)).await;
                    break;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            },
            message = incoming.next() => match message {
                Some(Ok(Message::Pong(_) | Message::Ping(_))) => last_activity = Instant::now(),
                Some(Ok(Message::Close(_)) | Err(_)) | None => break,
                Some(Ok(_)) => {
                    let _ = sender.send(Message::Close(None)).await;
                    break;
                }
            },
            _ = heartbeat.tick() => {
                if last_activity.elapsed() > hub.settings().heartbeat_interval.saturating_mul(2) {
                    let _ = sender.send(Message::Close(None)).await;
                    break;
                }
                if sender.send(Message::Ping(Vec::new().into())).await.is_err() { break }
            }
        }
    }
}

#[allow(clippy::needless_pass_by_value)]
fn internal(error: sqlx::Error) -> (StatusCode, Json<Value>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({"error":error.to_string()})),
    )
}
#[allow(clippy::needless_pass_by_value)]
fn internal_anyhow(error: anyhow::Error) -> (StatusCode, Json<Value>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({"error":error.to_string()})),
    )
}
#[allow(clippy::needless_pass_by_value)]
fn auth_error(error: AuthError) -> (StatusCode, Json<Value>) {
    let status = match error {
        AuthError::Unauthorized => StatusCode::UNAUTHORIZED,
        AuthError::Forbidden => StatusCode::FORBIDDEN,
        AuthError::SetupUnavailable => StatusCode::CONFLICT,
        AuthError::Invalid(_) => StatusCode::BAD_REQUEST,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, Json(json!({"error":error.to_string()})))
}
async fn setup_status(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    Ok(Json(
        json!({"setup_required":auth(&state)?.setup_required().await.map_err(auth_error)?}),
    ))
}
async fn setup_admin(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(input): Json<Credentials>,
) -> Result<StatusCode, (StatusCode, Json<Value>)> {
    auth(&state)?
        .setup(&headers, input.username, input.password)
        .await
        .map_err(auth_error)?;
    Ok(StatusCode::CREATED)
}
async fn login(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(input): Json<Credentials>,
) -> Result<Response, (StatusCode, Json<Value>)> {
    let service = auth(&state)?;
    let (session, token, csrf) = service
        .login(&headers, input.username, input.password)
        .await
        .map_err(auth_error)?;
    let mut response=Json(json!({"user":{"id":session.user_id,"username":session.username,"role":session.role},"expires_at":session.expires_at})).into_response();
    response.headers_mut().append(
        header::SET_COOKIE,
        HeaderValue::from_str(&service.session_cookie(&token)).expect("cookie is ASCII"),
    );
    response.headers_mut().append(
        header::SET_COOKIE,
        HeaderValue::from_str(&service.csrf_cookie(&csrf)).expect("cookie is ASCII"),
    );
    Ok(response)
}
async fn current_user(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let session = auth(&state)?
        .authenticate(&headers)
        .await
        .map_err(auth_error)?;
    Ok(Json(
        json!({"id":session.user_id,"username":session.username,"role":session.role,"expires_at":session.expires_at}),
    ))
}
async fn logout(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Response, (StatusCode, Json<Value>)> {
    let service = auth(&state)?;
    service.logout(&headers).await.map_err(auth_error)?;
    let mut response = StatusCode::NO_CONTENT.into_response();
    for cookie in service.clear_cookies() {
        response.headers_mut().append(
            header::SET_COOKIE,
            HeaderValue::from_str(&cookie).expect("cookie is ASCII"),
        );
    }
    Ok(response)
}

#[derive(Deserialize)]
struct CreateDeployment {
    chain_id: u64,
    address: String,
    start_block: u64,
    end_block: Option<u64>,
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    expected_event_topics: Value,
    expected_code_hash: Option<String>,
    source: String,
    interface_fingerprint: Option<String>,
}
#[derive(Deserialize)]
struct PatchDeployment {
    start_block: Option<u64>,
    end_block: Option<Value>,
    enabled: Option<bool>,
    expected_event_topics: Option<Value>,
    expected_code_hash: Option<Value>,
    source: Option<String>,
    interface_fingerprint: Option<String>,
}

fn registry(state: &AppState) -> Result<&DeploymentRegistry, (StatusCode, Json<Value>)> {
    state.deployments.as_ref().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({"error":"deployment registry unavailable"})),
    ))
}
#[allow(clippy::needless_pass_by_value)]
fn bad(message: impl ToString) -> (StatusCode, Json<Value>) {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({"error":message.to_string()})),
    )
}

async fn list_deployments(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    auth(&state)?
        .authenticate(&headers)
        .await
        .map_err(auth_error)?;
    let values = registry(&state)?.repository().list().await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error":e.to_string()})),
        )
    })?;
    Ok(Json(Value::Array(
        values.iter().map(deployment_json).collect(),
    )))
}
async fn create_deployment(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<(StatusCode, Json<Value>), (StatusCode, Json<Value>)> {
    let registry = registry(&state)?;
    let session = auth(&state)?
        .require_mutation(&headers)
        .await
        .map_err(auth_error)?;
    let input: CreateDeployment =
        serde_json::from_slice(&body).map_err(|error| bad(error.to_string()))?;
    if input.chain_id != pons_chain::ROBINHOOD_CHAIN_ID {
        return Err(bad("chain_id must be 4663"));
    }
    if input.source.trim().is_empty() {
        return Err(bad("source is required"));
    }
    if input.end_block.is_some_and(|v| v < input.start_block) {
        return Err(bad("end_block must be >= start_block"));
    }
    if input.enabled {
        return Err(bad("new deployments must be verified before enabling"));
    }
    let address = input.address.parse().map_err(bad)?;
    let hash = input
        .expected_code_hash
        .as_deref()
        .map(str::parse)
        .transpose()
        .map_err(bad)?;
    let topics = if input.expected_event_topics.is_null() {
        json!([])
    } else {
        input.expected_event_topics
    };
    let value = registry
        .repository()
        .create(&NewProtocolDeployment {
            chain_id: pons_domain::ChainId::new(input.chain_id),
            address,
            start_block: pons_domain::BlockNumber::new(input.start_block),
            end_block: input.end_block.map(pons_domain::BlockNumber::new),
            enabled: false,
            expected_event_topics: &topics,
            expected_code_hash: hash,
            source: &input.source,
            interface_fingerprint: input
                .interface_fingerprint
                .as_deref()
                .unwrap_or(PONS_V2_FACTORY_FINGERPRINT),
        })
        .await
        .map_err(|e| bad(e.to_string()))?;
    auth(&state)?
        .repository()
        .audit(
            Some(session.user_id),
            "deployment.create",
            "protocol_deployment",
            Some(&value.id.to_string()),
            &json!({"address":value.address.to_string()}),
        )
        .await
        .map_err(|e| auth_error(AuthError::Storage(e)))?;
    Ok((StatusCode::CREATED, Json(deployment_json(&value))))
}
async fn update_deployment(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    body: Bytes,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let registry = registry(&state)?;
    let session = auth(&state)?
        .require_mutation(&headers)
        .await
        .map_err(auth_error)?;
    let input: PatchDeployment =
        serde_json::from_slice(&body).map_err(|error| bad(error.to_string()))?;
    let current = registry
        .repository()
        .get(id)
        .await
        .map_err(|e| bad(e.to_string()))?
        .ok_or((StatusCode::NOT_FOUND, Json(json!({"error":"not found"}))))?;
    let sensitive = input.start_block.is_some()
        || input.end_block.is_some()
        || input.expected_event_topics.is_some()
        || input.expected_code_hash.is_some()
        || input.source.is_some()
        || input.interface_fingerprint.is_some();
    if input.enabled == Some(true) && (current.health != "VERIFIED" || sensitive) {
        return Err(bad("deployment must remain VERIFIED before enabling"));
    }
    let end = input
        .end_block
        .as_ref()
        .map(|v| {
            if v.is_null() {
                Ok(None)
            } else {
                v.as_u64()
                    .map(|n| Some(pons_domain::BlockNumber::new(n)))
                    .ok_or_else(|| bad("end_block must be uint64 or null"))
            }
        })
        .transpose()?;
    let hash = input
        .expected_code_hash
        .as_ref()
        .map(|v| {
            if v.is_null() {
                Ok(None)
            } else {
                v.as_str()
                    .ok_or_else(|| bad("expected_code_hash must be hash or null"))?
                    .parse()
                    .map(Some)
                    .map_err(bad)
            }
        })
        .transpose()?;
    let changes = DeploymentChanges {
        start_block: input.start_block.map(pons_domain::BlockNumber::new),
        end_block: end,
        enabled: input.enabled,
        expected_event_topics: input.expected_event_topics.as_ref(),
        expected_code_hash: hash,
        source: input.source.as_deref(),
        interface_fingerprint: input.interface_fingerprint.as_deref(),
    };
    let value = registry
        .repository()
        .update(id, &changes)
        .await
        .map_err(|e| bad(e.to_string()))?
        .ok_or((StatusCode::NOT_FOUND, Json(json!({"error":"not found"}))))?;
    auth(&state)?
        .repository()
        .audit(
            Some(session.user_id),
            "deployment.update",
            "protocol_deployment",
            Some(&id.to_string()),
            &json!({"enabled":value.enabled,"health":value.health}),
        )
        .await
        .map_err(|e| auth_error(AuthError::Storage(e)))?;
    Ok(Json(deployment_json(&value)))
}
async fn verify_deployment(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let session = auth(&state)?
        .require_mutation(&headers)
        .await
        .map_err(auth_error)?;
    let value = registry(&state)?
        .verify(id)
        .await
        .map(|v| Json(deployment_json(&v)))
        .map_err(|e| match e {
            RegistryError::NotFound => {
                (StatusCode::NOT_FOUND, Json(json!({"error":e.to_string()})))
            }
            _ => (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({"error":e.to_string()})),
            ),
        })?;
    registry(&state)?
        .repository()
        .set_approver(id, session.user_id)
        .await
        .map_err(|e| auth_error(AuthError::Storage(e)))?;
    auth(&state)?
        .repository()
        .audit(
            Some(session.user_id),
            "deployment.verify",
            "protocol_deployment",
            Some(&id.to_string()),
            &json!({"health":"VERIFIED"}),
        )
        .await
        .map_err(|e| auth_error(AuthError::Storage(e)))?;
    Ok(value)
}

fn trader_registry(
    state: &AppState,
) -> Result<&ExecutionWalletRegistry, (StatusCode, Json<Value>)> {
    state.traders.as_ref().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({"error":"trader registry unavailable"})),
    ))
}
fn trader_json(v: &pons_storage::repositories::Trader) -> Value {
    json!({"id":v.id,"handle":v.handle,"display_name":v.display_name,"manual_tier":v.manual_tier,"status":v.status,"notes":v.notes,"created_at":v.created_at,"updated_at":v.updated_at})
}
fn wallet_json(v: &pons_storage::repositories::TraderWallet) -> Value {
    json!({"id":v.id,"trader_id":v.trader_id,"trader_handle":v.trader_handle,"chain_id":v.chain_id.get(),"address":v.address.to_string(),"role":v.role,"source":v.source,"confidence":v.confidence,"verified":v.verified,"enabled":v.enabled,"valid_from":v.valid_from,"valid_to":v.valid_to,"notes":v.notes,"evidence":v.evidence})
}

async fn list_traders(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(q): Query<PageQuery>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    require_read(&state, &headers).await?;
    Ok(Json(
        web_repository(&state)?
            .traders(
                q.limit.unwrap_or(50).clamp(1, 200),
                q.offset.unwrap_or(0).max(0),
            )
            .await
            .map_err(internal)?,
    ))
}
async fn get_trader(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    require_read(&state, &headers).await?;
    let value = trader_registry(&state)?
        .repository()
        .get(id)
        .await
        .map_err(|e| bad(e.to_string()))?
        .ok_or((StatusCode::NOT_FOUND, Json(json!({"error":"not found"}))))?;
    Ok(Json(trader_json(&value)))
}
async fn list_trader_wallets(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    require_read(&state, &headers).await?;
    let values = trader_registry(&state)?
        .repository()
        .wallets(Some(id))
        .await
        .map_err(|e| bad(e.to_string()))?;
    Ok(Json(Value::Array(values.iter().map(wallet_json).collect())))
}

#[derive(Deserialize)]
struct CreateTrader {
    handle: String,
    display_name: Option<String>,
    manual_tier: Option<String>,
    notes: Option<String>,
}
#[allow(clippy::option_option)]
#[derive(Deserialize)]
struct PatchTrader {
    display_name: Option<Option<String>>,
    manual_tier: Option<Option<String>>,
    status: Option<String>,
    notes: Option<Option<String>>,
}
#[derive(Deserialize)]
struct CreateWallet {
    chain_id: u64,
    address: String,
    role: String,
    source: String,
    confidence: String,
    verified: bool,
    #[serde(default = "yes")]
    enabled: bool,
    valid_from: Option<DateTime<Utc>>,
    valid_to: Option<DateTime<Utc>>,
    notes: Option<String>,
    #[serde(default)]
    evidence: Value,
}
#[allow(clippy::option_option)]
#[derive(Deserialize)]
struct PatchWallet {
    enabled: Option<bool>,
    verified: Option<bool>,
    confidence: Option<String>,
    valid_from: Option<DateTime<Utc>>,
    valid_to: Option<Option<DateTime<Utc>>>,
    notes: Option<Option<String>>,
    evidence: Option<Value>,
}
const fn yes() -> bool {
    true
}
fn validate_trader(handle: &str, tier: Option<&str>) -> Result<(), (StatusCode, Json<Value>)> {
    if handle.is_empty()
        || handle.len() > 64
        || !handle
            .bytes()
            .all(|v| v.is_ascii_alphanumeric() || matches!(v, b'_' | b'-' | b'.'))
    {
        return Err(bad("handle must be 1-64 ASCII letters, digits, ., _ or -"));
    }
    if tier.is_some_and(|v| !matches!(v, "S" | "A" | "B" | "C")) {
        return Err(bad("manual_tier must be S, A, B, C or null"));
    }
    Ok(())
}
fn validate_wallet(
    v: &CreateWallet,
) -> Result<pons_domain::WalletAddress, (StatusCode, Json<Value>)> {
    use std::str::FromStr as _;
    if v.chain_id != pons_chain::ROBINHOOD_CHAIN_ID {
        return Err(bad("execution registry chain_id must be 4663"));
    }
    if !matches!(
        v.role.as_str(),
        "PROFILE_ADDRESS" | "ROBINHOOD_EXECUTION_ADDRESS" | "HISTORICAL_EXECUTION_ADDRESS"
    ) {
        return Err(bad("invalid wallet role"));
    }
    if !matches!(
        v.source.as_str(),
        "MANUAL" | "CSV_IMPORT" | "OPERATOR_VERIFIED"
    ) {
        return Err(bad("invalid wallet source"));
    }
    let confidence = rust_decimal::Decimal::from_str(&v.confidence).map_err(bad)?;
    if confidence < rust_decimal::Decimal::ZERO || confidence > rust_decimal::Decimal::ONE {
        return Err(bad("confidence must be between 0 and 1"));
    }
    if v.valid_to
        .is_some_and(|to| to <= v.valid_from.unwrap_or_else(Utc::now))
    {
        return Err(bad("valid_to must be after valid_from"));
    }
    v.address.parse().map_err(bad)
}
async fn audit(
    state: &AppState,
    user: Uuid,
    action: &str,
    target: &str,
    id: Uuid,
    details: Value,
) -> Result<(), (StatusCode, Json<Value>)> {
    auth(state)?
        .repository()
        .audit(Some(user), action, target, Some(&id.to_string()), &details)
        .await
        .map_err(|e| auth_error(AuthError::Storage(e)))
}
async fn create_trader(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<(StatusCode, Json<Value>), (StatusCode, Json<Value>)> {
    let session = auth(&state)?
        .require_mutation(&headers)
        .await
        .map_err(auth_error)?;
    let v: CreateTrader = serde_json::from_slice(&body).map_err(|e| bad(e.to_string()))?;
    validate_trader(&v.handle, v.manual_tier.as_deref())?;
    let value = trader_registry(&state)?
        .repository()
        .create(&NewTrader {
            handle: &v.handle,
            display_name: v.display_name.as_deref(),
            manual_tier: v.manual_tier.as_deref(),
            notes: v.notes.as_deref(),
        })
        .await
        .map_err(|e| bad(e.to_string()))?;
    audit(
        &state,
        session.user_id,
        "trader.create",
        "trader",
        value.id,
        json!({"handle":value.handle}),
    )
    .await?;
    Ok((StatusCode::CREATED, Json(trader_json(&value))))
}
async fn update_trader(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    body: Bytes,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let session = auth(&state)?
        .require_mutation(&headers)
        .await
        .map_err(auth_error)?;
    let v: PatchTrader = serde_json::from_slice(&body).map_err(|e| bad(e.to_string()))?;
    validate_trader("valid", v.manual_tier.as_ref().and_then(|v| v.as_deref()))?;
    if v.status
        .as_deref()
        .is_some_and(|x| !matches!(x, "ACTIVE" | "DISABLED"))
    {
        return Err(bad("invalid trader status"));
    }
    let value = trader_registry(&state)?
        .repository()
        .update(
            id,
            &TraderChanges {
                display_name: v.display_name.as_ref().map(|x| x.as_deref()),
                manual_tier: v.manual_tier.as_ref().map(|x| x.as_deref()),
                status: v.status.as_deref(),
                notes: v.notes.as_ref().map(|x| x.as_deref()),
            },
        )
        .await
        .map_err(|e| bad(e.to_string()))?
        .ok_or((StatusCode::NOT_FOUND, Json(json!({"error":"not found"}))))?;
    trader_registry(&state)?
        .refresh()
        .await
        .map_err(|e| bad(e.to_string()))?;
    audit(
        &state,
        session.user_id,
        "trader.update",
        "trader",
        id,
        json!({"status":value.status}),
    )
    .await?;
    Ok(Json(trader_json(&value)))
}
async fn add_trader_wallet(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    body: Bytes,
) -> Result<(StatusCode, Json<Value>), (StatusCode, Json<Value>)> {
    let session = auth(&state)?
        .require_mutation(&headers)
        .await
        .map_err(auth_error)?;
    let v: CreateWallet = serde_json::from_slice(&body).map_err(|e| bad(e.to_string()))?;
    let address = validate_wallet(&v)?;
    let value = trader_registry(&state)?
        .repository()
        .add_wallet(&NewTraderWallet {
            trader_id: id,
            chain_id: pons_domain::ChainId::new(v.chain_id),
            address,
            role: &v.role,
            source: &v.source,
            confidence: &v.confidence,
            verified: v.verified,
            enabled: v.enabled,
            valid_from: v.valid_from.unwrap_or_else(Utc::now),
            valid_to: v.valid_to,
            notes: v.notes.as_deref(),
            evidence: &v.evidence,
        })
        .await
        .map_err(|e| bad(e.to_string()))?;
    trader_registry(&state)?
        .refresh()
        .await
        .map_err(|e| bad(e.to_string()))?;
    let classification_job_id = trader_registry(&state)?
        .enqueue_historical_classification(value.id)
        .await
        .map_err(|e| bad(e.to_string()))?;
    audit(
        &state,
        session.user_id,
        "trader_wallet.create",
        "trader_wallet",
        value.id,
        json!({"address":value.address.to_string(),"role":value.role,"historical_classification_job_id":classification_job_id}),
    )
    .await?;
    Ok((StatusCode::CREATED, Json(wallet_json(&value))))
}
async fn update_trader_wallet(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    body: Bytes,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    use std::str::FromStr as _;
    let session = auth(&state)?
        .require_mutation(&headers)
        .await
        .map_err(auth_error)?;
    let v: PatchWallet = serde_json::from_slice(&body).map_err(|e| bad(e.to_string()))?;
    if let Some(c) = &v.confidence {
        let n = rust_decimal::Decimal::from_str(c).map_err(bad)?;
        if n < rust_decimal::Decimal::ZERO || n > rust_decimal::Decimal::ONE {
            return Err(bad("confidence must be between 0 and 1"));
        }
    }
    let value = trader_registry(&state)?
        .repository()
        .update_wallet(
            id,
            &WalletChanges {
                enabled: v.enabled,
                verified: v.verified,
                confidence: v.confidence.as_deref(),
                valid_from: v.valid_from,
                valid_to: v.valid_to,
                notes: v.notes.as_ref().map(|x| x.as_deref()),
                evidence: v.evidence.as_ref(),
            },
        )
        .await
        .map_err(|e| bad(e.to_string()))?
        .ok_or((StatusCode::NOT_FOUND, Json(json!({"error":"not found"}))))?;
    trader_registry(&state)?
        .refresh()
        .await
        .map_err(|e| bad(e.to_string()))?;
    let classification_job_id = trader_registry(&state)?
        .enqueue_historical_classification(value.id)
        .await
        .map_err(|e| bad(e.to_string()))?;
    audit(
        &state,
        session.user_id,
        "trader_wallet.update",
        "trader_wallet",
        id,
        json!({"enabled":value.enabled,"verified":value.verified,"historical_classification_job_id":classification_job_id}),
    )
    .await?;
    Ok(Json(wallet_json(&value)))
}

#[derive(Deserialize)]
struct CsvRow {
    handle: String,
    address: String,
    role: String,
    tier: Option<String>,
    confidence: String,
    notes: Option<String>,
}
#[allow(clippy::too_many_lines)]
async fn import_traders_csv(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let session = auth(&state)?
        .require_mutation(&headers)
        .await
        .map_err(auth_error)?;
    let mut reader = csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .from_reader(body.as_ref());
    let mut results = Vec::new();
    for (index, row) in reader.deserialize::<CsvRow>().enumerate() {
        let line = index + 2;
        let outcome = async {
            let row = row.map_err(|e| e.to_string())?;
            validate_trader(&row.handle, row.tier.as_deref()).map_err(|e| {
                e.1.0["error"]
                    .as_str()
                    .unwrap_or("invalid trader")
                    .to_owned()
            })?;
            let input = CreateWallet {
                chain_id: 4663,
                address: row.address,
                role: row.role,
                source: "CSV_IMPORT".into(),
                confidence: row.confidence,
                verified: false,
                enabled: true,
                valid_from: None,
                valid_to: None,
                notes: row.notes,
                evidence: json!({"csv_line":line}),
            };
            let address = validate_wallet(&input).map_err(|e| {
                e.1.0["error"]
                    .as_str()
                    .unwrap_or("invalid wallet")
                    .to_owned()
            })?;
            let wallet = trader_registry(&state)
                .unwrap()
                .repository()
                .import_wallet(
                    &row.handle,
                    row.tier.as_deref(),
                    &NewTraderWallet {
                        trader_id: Uuid::nil(),
                        chain_id: pons_domain::ChainId::new(4663),
                        address,
                        role: &input.role,
                        source: &input.source,
                        confidence: &input.confidence,
                        verified: false,
                        enabled: true,
                        valid_from: Utc::now(),
                        valid_to: None,
                        notes: input.notes.as_deref(),
                        evidence: &input.evidence,
                    },
                )
                .await
                .map_err(|e| e.to_string())?;
            Ok::<_, String>(wallet)
        }
        .await;
        match outcome {
            Ok(wallet) => {
                audit(
                    &state,
                    session.user_id,
                    "trader_wallet.csv_import",
                    "trader_wallet",
                    wallet.id,
                    json!({"line":line,"handle":wallet.trader_handle}),
                )
                .await?;
                results.push(json!({"line":line,"success":true,"wallet_id":wallet.id}));
            }
            Err(error) => results.push(json!({"line":line,"success":false,"error":error})),
        }
    }
    trader_registry(&state)?
        .refresh()
        .await
        .map_err(|e| bad(e.to_string()))?;
    audit(
        &state,
        session.user_id,
        "trader.csv_import",
        "trader_registry",
        Uuid::nil(),
        json!({"rows":results.len()}),
    )
    .await?;
    Ok(Json(json!({"results":results})))
}

async fn add_request_id(request: axum::extract::Request, next: axum::middleware::Next) -> Response {
    use tower_http::request_id::MakeRequestId;

    let mut request = request;
    if request.headers().get("x-request-id").is_none() {
        if let Some(value) = MakeRequestUuid.make_request_id(&request) {
            request
                .headers_mut()
                .insert("x-request-id", value.header_value().clone());
        }
    }
    next.run(request).await
}

async fn healthz() -> Json<Health> {
    Json(Health { status: "ok" })
}

async fn readyz(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let ready = match &state.readiness {
        #[cfg(test)]
        Readiness::Bootstrap => true,
        Readiness::Postgres(database) => database.ready().await,
    };
    if ready {
        (StatusCode::OK, Json(Health { status: "ready" }))
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(Health {
                status: "not_ready",
            }),
        )
    }
}

async fn version(State(state): State<Arc<AppState>>) -> Json<VersionInfo> {
    Json(VersionInfo::new(state.started_at))
}

async fn index() -> Response {
    embedded_response("index.html", true)
}

async fn asset_or_index(Path(path): Path<String>) -> Response {
    if FrontendAssets::get(&path).is_some() {
        embedded_response(&path, false)
    } else if path.starts_with("api/") {
        StatusCode::NOT_FOUND.into_response()
    } else {
        embedded_response("index.html", true)
    }
}

fn embedded_response(path: &str, app_shell: bool) -> Response {
    let Some(asset) = FrontendAssets::get(path) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let mime = mime_guess::from_path(path).first_or_octet_stream();
    let cache = if app_shell {
        "no-store"
    } else {
        "public, max-age=31536000, immutable"
    };

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, mime.as_ref())
        .header(header::CACHE_CONTROL, HeaderValue::from_static(cache))
        .body(Body::from(asset.data))
        .expect("static response headers are valid")
}

#[cfg(test)]
mod tests {
    use axum::http::Request;
    use chrono::TimeZone;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use super::*;
    use crate::version::{API_SCHEMA_VERSION, APP_VERSION, FRONTEND_BUILD_ID};

    fn app() -> Router {
        router(Utc.with_ymd_and_hms(2026, 8, 28, 0, 0, 0).unwrap())
    }

    async fn get(path: &str) -> Response {
        app()
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn health_endpoints_are_available() {
        for (path, expected) in [("/healthz", "ok"), ("/readyz", "ready")] {
            let response = get(path).await;
            assert_eq!(response.status(), StatusCode::OK);
            let body = response.into_body().collect().await.unwrap().to_bytes();
            let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(json["status"], expected);
        }
    }

    #[tokio::test]
    async fn version_endpoint_reports_compile_time_identifiers() {
        let response = get("/api/v1/system/version").await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["app_version"], APP_VERSION);
        assert_eq!(json["frontend_build_id"], FRONTEND_BUILD_ID);
        assert_eq!(json["api_schema_version"], API_SCHEMA_VERSION);
        assert_eq!(json["started_at"], "2026-08-28T00:00:00Z");
    }

    #[tokio::test]
    async fn embedded_frontend_is_served_for_root_and_client_routes() {
        for path in ["/", "/dashboard"] {
            let response = get(path).await;
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
            let body = response.into_body().collect().await.unwrap().to_bytes();
            assert!(String::from_utf8_lossy(&body).contains("Pons Radar"));
        }
    }

    #[tokio::test]
    async fn unknown_api_route_is_not_frontend_fallback() {
        assert_eq!(get("/api/v1/unknown").await.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn readiness_fails_when_postgres_is_unavailable() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .acquire_timeout(std::time::Duration::from_millis(50))
            .connect_lazy("postgres://postgres:postgres@127.0.0.1:1/pons")
            .unwrap();
        let database = pons_storage::Database::from_pool(pool);
        let app = router_with_database(
            Utc.with_ymd_and_hms(2026, 8, 28, 0, 0, 0).unwrap(),
            database,
        );
        let response = app
            .oneshot(Request::get("/readyz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}
