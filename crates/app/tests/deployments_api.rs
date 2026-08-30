use std::sync::Arc;

use async_trait::async_trait;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use chrono::Utc;
use pons_chain::{BlockHeader, ChainLog, ChainRpc, LogFilter, Receipt, RunError};
use pons_domain::{BlockNumber, ChainId, ContractAddress, TxHash};
use pons_radar::server::router_with_registry;
use pons_storage::{Database, repositories::DeploymentRepository};
use pons_v2::DeploymentRegistry;
use sqlx::PgPool;
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
        Ok(vec![0x60, 1])
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
    let registry = DeploymentRegistry::new(DeploymentRepository::new(pool.clone()), Arc::new(Rpc));
    router_with_registry(Utc::now(), Database::from_pool(pool), registry)
}
async fn json_request(
    app: axum::Router,
    method: &str,
    path: &str,
    value: serde_json::Value,
    auth: Option<(&str, &str)>,
) -> axum::response::Response {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json")
        .header("origin", "http://localhost")
        .header("x-setup-token", "test-setup-token");
    if let Some((cookie, csrf)) = auth {
        request = request
            .header("cookie", cookie)
            .header("x-csrf-token", csrf);
    }
    app.oneshot(request.body(Body::from(value.to_string())).unwrap())
        .await
        .unwrap()
}

async fn authenticated(pool: PgPool) -> (axum::Router, String, String) {
    let application = app(pool);
    let setup = json_request(
        application.clone(),
        "POST",
        "/api/v1/auth/setup",
        serde_json::json!({"username":"admin","password":"correct horse battery staple"}),
        None,
    )
    .await;
    // Supply the out-of-band bootstrap secret only for setup.
    assert_eq!(setup.status(), StatusCode::CREATED, "setup failed");
    let login = json_request(
        application.clone(),
        "POST",
        "/api/v1/auth/login",
        serde_json::json!({"username":"admin","password":"correct horse battery staple"}),
        None,
    )
    .await;
    assert_eq!(login.status(), StatusCode::OK);
    let cookies: Vec<_> = login
        .headers()
        .get_all("set-cookie")
        .iter()
        .map(|v| v.to_str().unwrap().split(';').next().unwrap().to_owned())
        .collect();
    let csrf = cookies
        .iter()
        .find(|v| v.starts_with("pons_csrf="))
        .unwrap()
        .split_once('=')
        .unwrap()
        .1
        .to_owned();
    (application, cookies.join("; "), csrf)
}

#[sqlx::test(migrations = "../../migrations")]
async fn admin_mutations_reject_wrong_chain_invalid_range_and_unverified_enable(pool: PgPool) {
    let (application, cookie, csrf) = authenticated(pool).await;
    let base = serde_json::json!({"chain_id":4663,"address":"0x2222222222222222222222222222222222222222","start_block":10,"end_block":20,"enabled":false,"expected_event_topics":[],"source":"operator"});
    let mut wrong = base.clone();
    wrong["chain_id"] = 1.into();
    assert_eq!(
        json_request(
            application.clone(),
            "POST",
            "/api/v1/admin/deployments",
            wrong,
            Some((&cookie, &csrf))
        )
        .await
        .status(),
        StatusCode::BAD_REQUEST
    );
    let mut range = base.clone();
    range["end_block"] = 9.into();
    assert_eq!(
        json_request(
            application.clone(),
            "POST",
            "/api/v1/admin/deployments",
            range,
            Some((&cookie, &csrf))
        )
        .await
        .status(),
        StatusCode::BAD_REQUEST
    );
    let mut enabled = base;
    enabled["enabled"] = true.into();
    assert_eq!(
        json_request(
            application,
            "POST",
            "/api/v1/admin/deployments",
            enabled,
            Some((&cookie, &csrf))
        )
        .await
        .status(),
        StatusCode::BAD_REQUEST
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn admin_create_verify_enable_and_list_round_trip(pool: PgPool) {
    let (application, cookie, csrf) = authenticated(pool.clone()).await;
    let create = serde_json::json!({"chain_id":4663,"address":"0x3333333333333333333333333333333333333333","start_block":10,"enabled":false,"expected_event_topics":[],"source":"operator"});
    let response = json_request(
        application.clone(),
        "POST",
        "/api/v1/admin/deployments",
        create,
        Some((&cookie, &csrf)),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let body = http_body_util::BodyExt::collect(response.into_body())
        .await
        .unwrap()
        .to_bytes();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let id = value["id"].as_str().unwrap();
    assert_eq!(
        json_request(
            application.clone(),
            "POST",
            &format!("/api/v1/admin/deployments/{id}/verify"),
            serde_json::json!({}),
            Some((&cookie, &csrf))
        )
        .await
        .status(),
        StatusCode::OK
    );
    assert_eq!(
        json_request(
            application.clone(),
            "PATCH",
            &format!("/api/v1/admin/deployments/{id}"),
            serde_json::json!({"enabled":true}),
            Some((&cookie, &csrf))
        )
        .await
        .status(),
        StatusCode::OK
    );
    let active = DeploymentRepository::new(pool.clone())
        .active_verified(ChainId::new(4663), BlockNumber::new(10))
        .await
        .unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].trust_basis, "OPERATOR_APPROVED");
    assert!(active[0].approved_by.is_some());
    let actions: Vec<String> = sqlx::query_scalar("SELECT action FROM audit_logs")
        .fetch_all(&pool)
        .await
        .unwrap();
    for action in [
        "deployment.create",
        "deployment.verify",
        "deployment.update",
    ] {
        assert!(actions.iter().any(|value| value == action));
    }
    let response = application
        .oneshot(
            Request::get("/api/v1/admin/deployments")
                .header("cookie", cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}
