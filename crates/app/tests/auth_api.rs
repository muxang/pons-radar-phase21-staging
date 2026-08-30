use async_trait::async_trait;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use chrono::Utc;
use pons_chain::{BlockHeader, ChainLog, ChainRpc, LogFilter, Receipt, RunError};
use pons_domain::{BlockNumber, ChainId, ContractAddress, TxHash};
use pons_radar::{
    auth::{AuthConfig, AuthService},
    server::router_with_registry_and_auth,
};
use pons_storage::{
    Database,
    repositories::{AuthRepository, DeploymentRepository},
};
use pons_v2::DeploymentRegistry;
use serde_json::json;
use sqlx::PgPool;
use std::sync::Arc;
use tower::ServiceExt;

struct Rpc;
#[async_trait]
impl ChainRpc for Rpc {
    async fn chain_id(&self) -> Result<ChainId, RunError> {
        Ok(ChainId::new(4663))
    }
    async fn block_number(&self) -> Result<BlockNumber, RunError> {
        Ok(BlockNumber::new(0))
    }
    async fn code(&self, _: ContractAddress) -> Result<Vec<u8>, RunError> {
        Ok(vec![1])
    }
    async fn block(&self, _: BlockNumber) -> Result<Option<BlockHeader>, RunError> {
        Ok(None)
    }
    async fn receipt(&self, _: TxHash) -> Result<Option<Receipt>, RunError> {
        Ok(None)
    }
    async fn logs(
        &self,
        _: BlockNumber,
        _: BlockNumber,
        _: &LogFilter,
    ) -> Result<Vec<ChainLog>, RunError> {
        Ok(vec![])
    }
}
fn app(pool: PgPool) -> axum::Router {
    let auth = AuthService::new(
        AuthRepository::new(pool.clone()),
        AuthConfig {
            secure_cookie: true,
            session_hours: 8,
            allowed_origin: "https://radar.example".into(),
            setup_token: Some("bootstrap-secret".into()),
        },
    );
    let registry = DeploymentRegistry::new(DeploymentRepository::new(pool.clone()), Arc::new(Rpc));
    router_with_registry_and_auth(Utc::now(), Database::from_pool(pool), registry, auth)
}
async fn send(
    app: axum::Router,
    method: &str,
    path: &str,
    body: serde_json::Value,
    headers: &[(&str, &str)],
) -> axum::response::Response {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json");
    for (key, value) in headers {
        request = request.header(*key, *value);
    }
    app.oneshot(request.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap()
}

#[sqlx::test(migrations = "../../migrations")]
async fn ai_manual_research_requires_admin_session(pool: PgPool) {
    let response = send(
        app(pool),
        "POST",
        "/api/v1/admin/tokens/0x1111111111111111111111111111111111111111/ai-research",
        json!({}),
        &[("origin", "https://radar.example")],
    )
    .await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[sqlx::test(migrations = "../../migrations")]
async fn backtest_mutations_require_admin_session(pool: PgPool) {
    let response = send(
        app(pool),
        "POST",
        "/api/v1/admin/backtests",
        json!({}),
        &[("origin", "https://radar.example")],
    )
    .await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}
async fn setup_and_login(pool: PgPool) -> (axum::Router, String, String) {
    let application = app(pool);
    assert_eq!(
        send(
            application.clone(),
            "POST",
            "/api/v1/auth/setup",
            json!({"username":"admin","password":"correct horse battery staple"}),
            &[
                ("origin", "https://radar.example"),
                ("x-setup-token", "bootstrap-secret")
            ]
        )
        .await
        .status(),
        StatusCode::CREATED
    );
    let response = send(
        application.clone(),
        "POST",
        "/api/v1/auth/login",
        json!({"username":"admin","password":"correct horse battery staple"}),
        &[("origin", "https://radar.example")],
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let set: Vec<_> = response
        .headers()
        .get_all("set-cookie")
        .iter()
        .map(|v| v.to_str().unwrap().to_owned())
        .collect();
    assert!(set.iter().any(|v| v.starts_with("pons_session=")
        && v.contains("HttpOnly")
        && v.contains("Secure")
        && v.contains("SameSite=Strict")));
    let pairs: Vec<_> = set
        .iter()
        .map(|v| v.split(';').next().unwrap().to_owned())
        .collect();
    let csrf = pairs
        .iter()
        .find(|v| v.starts_with("pons_csrf="))
        .unwrap()
        .split_once('=')
        .unwrap()
        .1
        .to_owned();
    (application, pairs.join("; "), csrf)
}

#[sqlx::test(migrations = "../../migrations")]
async fn updater_install_requires_authenticated_admin_before_service_access(pool: PgPool) {
    let response = send(
        app(pool),
        "POST",
        "/api/v1/admin/updates/install",
        json!({"confirm":true}),
        &[("origin", "https://radar.example")],
    )
    .await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[sqlx::test(migrations = "../../migrations")]
async fn web_admin_has_no_release_trust_root_mutation_surface(pool: PgPool) {
    let (application, cookie, csrf) = setup_and_login(pool).await;
    let response = send(
        application,
        "POST",
        "/api/v1/admin/updates/trust-roots",
        json!({"key_id":"remote","public_key":"00"}),
        &[
            ("origin", "https://radar.example"),
            ("cookie", &cookie),
            ("x-csrf-token", &csrf),
        ],
    )
    .await;
    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
}

#[sqlx::test(migrations = "../../migrations")]
async fn first_run_requires_out_of_band_secret_and_stores_argon2id_only(pool: PgPool) {
    let application = app(pool.clone());
    assert_eq!(
        send(
            application.clone(),
            "POST",
            "/api/v1/auth/setup",
            json!({"username":"admin","password":"correct horse battery staple"}),
            &[
                ("origin", "https://radar.example"),
                ("x-setup-token", "wrong")
            ]
        )
        .await
        .status(),
        StatusCode::CONFLICT
    );
    assert_eq!(
        send(
            application.clone(),
            "POST",
            "/api/v1/auth/setup",
            json!({"username":"admin","password":"correct horse battery staple"}),
            &[
                ("origin", "https://radar.example"),
                ("x-setup-token", "bootstrap-secret")
            ]
        )
        .await
        .status(),
        StatusCode::CREATED
    );
    assert_eq!(
        send(
            application,
            "POST",
            "/api/v1/auth/setup",
            json!({"username":"other","password":"another correct battery staple"}),
            &[
                ("origin", "https://radar.example"),
                ("x-setup-token", "bootstrap-secret")
            ]
        )
        .await
        .status(),
        StatusCode::CONFLICT
    );
    let hash: String = sqlx::query_scalar("SELECT password_hash FROM users WHERE username='admin'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(hash.starts_with("$argon2id$"));
    assert!(!hash.contains("correct horse"));
}

#[sqlx::test(migrations = "../../migrations")]
async fn admin_requires_session_origin_and_csrf_and_logout_revokes(pool: PgPool) {
    let unauth = app(pool.clone());
    assert_eq!(
        send(
            unauth.clone(),
            "POST",
            "/api/v1/admin/deployments",
            json!({}),
            &[("origin", "https://radar.example")]
        )
        .await
        .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        unauth
            .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let (application, cookie, csrf) = setup_and_login(pool.clone()).await;
    assert_eq!(
        send(
            application.clone(),
            "POST",
            "/api/v1/admin/deployments",
            json!({}),
            &[("origin", "https://radar.example"), ("cookie", &cookie)]
        )
        .await
        .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        send(
            application.clone(),
            "POST",
            "/api/v1/admin/deployments",
            json!({}),
            &[
                ("origin", "https://evil.example"),
                ("cookie", &cookie),
                ("x-csrf-token", &csrf)
            ]
        )
        .await
        .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        send(
            application.clone(),
            "GET",
            "/api/v1/auth/me",
            json!({}),
            &[("cookie", &cookie)]
        )
        .await
        .status(),
        StatusCode::OK
    );
    assert_eq!(
        send(
            application.clone(),
            "POST",
            "/api/v1/auth/logout",
            json!({}),
            &[
                ("origin", "https://radar.example"),
                ("cookie", &cookie),
                ("x-csrf-token", &csrf)
            ]
        )
        .await
        .status(),
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        send(
            application,
            "GET",
            "/api/v1/auth/me",
            json!({}),
            &[("cookie", &cookie)]
        )
        .await
        .status(),
        StatusCode::UNAUTHORIZED
    );
    let actions: Vec<String> =
        sqlx::query_scalar("SELECT action FROM audit_logs ORDER BY created_at")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert!(
        actions.contains(&"auth.setup".into())
            && actions.contains(&"auth.login".into())
            && actions.contains(&"auth.logout".into())
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn expired_session_is_rejected_and_raw_tokens_are_not_stored(pool: PgPool) {
    let (application, cookie, _) = setup_and_login(pool.clone()).await;
    let stored: Vec<u8> = sqlx::query_scalar("SELECT token_hash FROM admin_sessions")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(stored.len(), 32);
    assert!(
        !cookie
            .as_bytes()
            .windows(stored.len())
            .any(|window| window == stored)
    );
    sqlx::query("UPDATE admin_sessions SET created_at=now()-interval '2 hours', expires_at=now()-interval '1 hour'")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        send(
            application,
            "GET",
            "/api/v1/auth/me",
            json!({}),
            &[("cookie", &cookie)]
        )
        .await
        .status(),
        StatusCode::UNAUTHORIZED
    );
}
