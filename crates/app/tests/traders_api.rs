use async_trait::async_trait;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use chrono::{Duration, Utc};
use http_body_util::BodyExt;
use pons_chain::{BlockHeader, ChainLog, ChainRpc, LogFilter, Receipt, RunError};
use pons_domain::{BlockNumber, ChainId, ContractAddress, TxHash, WalletAddress};
use pons_radar::{
    auth::{AuthConfig, AuthService},
    server::router_with_services,
    traders::ExecutionWalletRegistry,
};
use pons_storage::{
    Database,
    repositories::{
        AuthRepository, DeploymentRepository, NewTrader, NewTraderWallet, TraderRepository,
        WalletChanges,
    },
};
use pons_v2::DeploymentRegistry;
use serde_json::{Value, json};
use sqlx::PgPool;
use std::sync::Arc;
use tower::ServiceExt;

const ADDRESS: &str = "0xAbCd000000000000000000000000000000001234";
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
        Ok(vec![])
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
async fn app(pool: PgPool) -> (axum::Router, ExecutionWalletRegistry) {
    let repo = TraderRepository::new(pool.clone());
    let matcher = ExecutionWalletRegistry::rebuild(repo, "0.90")
        .await
        .unwrap();
    let auth = AuthService::new(
        AuthRepository::new(pool.clone()),
        AuthConfig {
            secure_cookie: false,
            session_hours: 8,
            allowed_origin: "http://localhost".into(),
            setup_token: Some("test-setup-token".into()),
        },
    );
    let deployments =
        DeploymentRegistry::new(DeploymentRepository::new(pool.clone()), Arc::new(Rpc));
    (
        router_with_services(
            Utc::now(),
            Database::from_pool(pool),
            deployments,
            auth,
            matcher.clone(),
        ),
        matcher,
    )
}
async fn request(
    app: &axum::Router,
    method: &str,
    path: &str,
    body: impl Into<Body>,
    auth: Option<(&str, &str)>,
    content: &str,
) -> axum::response::Response {
    let mut b = Request::builder()
        .method(method)
        .uri(path)
        .header("origin", "http://localhost")
        .header("content-type", content)
        .header("x-setup-token", "test-setup-token");
    if let Some((cookie, csrf)) = auth {
        b = b.header("cookie", cookie).header("x-csrf-token", csrf);
    }
    app.clone()
        .oneshot(b.body(body.into()).unwrap())
        .await
        .unwrap()
}
async fn login(app: &axum::Router) -> (String, String) {
    assert_eq!(
        request(
            app,
            "POST",
            "/api/v1/auth/setup",
            json!({"username":"admin","password":"correct horse battery staple"}).to_string(),
            None,
            "application/json"
        )
        .await
        .status(),
        StatusCode::CREATED
    );
    let response = request(
        app,
        "POST",
        "/api/v1/auth/login",
        json!({"username":"admin","password":"correct horse battery staple"}).to_string(),
        None,
        "application/json",
    )
    .await;
    let cookies: Vec<_> = response
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
    (cookies.join("; "), csrf)
}
async fn json_body(response: axum::response::Response) -> Value {
    serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap()
}

#[sqlx::test(migrations = "../../migrations")]
async fn admin_crud_hot_updates_matcher_and_audits(pool: PgPool) {
    let (app, matcher) = app(pool.clone()).await;
    let (cookie, csrf) = login(&app).await;
    assert_eq!(
        request(
            &app,
            "POST",
            "/api/v1/admin/traders",
            "{}",
            None,
            "application/json"
        )
        .await
        .status(),
        StatusCode::UNAUTHORIZED
    );
    let trader=json_body(request(&app,"POST","/api/v1/admin/traders",json!({"handle":"Alice","display_name":"Alice","manual_tier":"S","notes":"operator research"}).to_string(),Some((&cookie,&csrf)),"application/json").await).await;
    let id = trader["id"].as_str().unwrap();
    for invalid in [
        json!({"chain_id":1,"address":ADDRESS,"role":"ROBINHOOD_EXECUTION_ADDRESS","source":"MANUAL","confidence":"1","verified":true}),
        json!({"chain_id":4663,"address":"bad","role":"ROBINHOOD_EXECUTION_ADDRESS","source":"MANUAL","confidence":"1","verified":true}),
    ] {
        assert_eq!(
            request(
                &app,
                "POST",
                &format!("/api/v1/admin/traders/{id}/wallets"),
                invalid.to_string(),
                Some((&cookie, &csrf)),
                "application/json"
            )
            .await
            .status(),
            StatusCode::BAD_REQUEST
        );
    }
    let wallet=json_body(request(&app,"POST",&format!("/api/v1/admin/traders/{id}/wallets"),json!({"chain_id":4663,"address":ADDRESS,"role":"ROBINHOOD_EXECUTION_ADDRESS","source":"OPERATOR_VERIFIED","confidence":"0.95","verified":true}).to_string(),Some((&cookie,&csrf)),"application/json").await).await;
    let address: WalletAddress = ADDRESS.parse().unwrap();
    assert_eq!(
        address.to_string(),
        "0xabcd000000000000000000000000000000001234"
    );
    assert_eq!(
        matcher.match_address(address).await.unwrap().trader_handle,
        "alice"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM identity_classification_jobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1,
        "admin request durably enqueues work instead of scanning trades inline"
    );
    let wid = wallet["id"].as_str().unwrap();
    request(
        &app,
        "PATCH",
        &format!("/api/v1/admin/trader-wallets/{wid}"),
        json!({"enabled":false}).to_string(),
        Some((&cookie, &csrf)),
        "application/json",
    )
    .await;
    assert!(matcher.match_address(address).await.is_none());
    let actions: Vec<String> = sqlx::query_scalar("SELECT action FROM audit_logs")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert!(actions.contains(&"trader.create".into()));
    assert!(actions.contains(&"trader_wallet.create".into()));
    assert!(actions.contains(&"trader_wallet.update".into()));
}

#[sqlx::test(migrations = "../../migrations")]
async fn roles_confidence_validity_conflicts_and_restart_are_fail_closed(pool: PgPool) {
    let repo = TraderRepository::new(pool.clone());
    let a = repo
        .create(&NewTrader {
            handle: "a",
            display_name: None,
            manual_tier: None,
            notes: None,
        })
        .await
        .unwrap();
    let b = repo
        .create(&NewTrader {
            handle: "b",
            display_name: None,
            manual_tier: None,
            notes: None,
        })
        .await
        .unwrap();
    let evidence = json!({});
    let now = Utc::now();
    for (role, address, confidence, verified, from, to) in [
        (
            "PROFILE_ADDRESS",
            "0x0000000000000000000000000000000000000001",
            "1",
            true,
            now,
            None,
        ),
        (
            "HISTORICAL_EXECUTION_ADDRESS",
            "0x0000000000000000000000000000000000000002",
            "1",
            true,
            now,
            None,
        ),
        (
            "ROBINHOOD_EXECUTION_ADDRESS",
            "0x0000000000000000000000000000000000000003",
            "0.89",
            true,
            now,
            None,
        ),
        (
            "ROBINHOOD_EXECUTION_ADDRESS",
            "0x0000000000000000000000000000000000000004",
            "1",
            false,
            now,
            None,
        ),
        (
            "ROBINHOOD_EXECUTION_ADDRESS",
            "0x0000000000000000000000000000000000000005",
            "1",
            true,
            now + Duration::hours(1),
            None,
        ),
        (
            "ROBINHOOD_EXECUTION_ADDRESS",
            "0x0000000000000000000000000000000000000006",
            "1",
            true,
            now - Duration::hours(2),
            Some(now - Duration::hours(1)),
        ),
    ] {
        repo.add_wallet(&NewTraderWallet {
            trader_id: a.id,
            chain_id: ChainId::new(4663),
            address: address.parse().unwrap(),
            role,
            source: "MANUAL",
            confidence,
            verified,
            enabled: true,
            valid_from: from,
            valid_to: to,
            notes: None,
            evidence: &evidence,
        })
        .await
        .unwrap();
    }
    let active = "0x0000000000000000000000000000000000000007"
        .parse()
        .unwrap();
    let first = repo
        .add_wallet(&NewTraderWallet {
            trader_id: a.id,
            chain_id: ChainId::new(4663),
            address: active,
            role: "ROBINHOOD_EXECUTION_ADDRESS",
            source: "OPERATOR_VERIFIED",
            confidence: "0.90",
            verified: true,
            enabled: true,
            valid_from: now,
            valid_to: None,
            notes: None,
            evidence: &evidence,
        })
        .await
        .unwrap();
    assert!(
        repo.add_wallet(&NewTraderWallet {
            trader_id: b.id,
            chain_id: ChainId::new(4663),
            address: active,
            role: "ROBINHOOD_EXECUTION_ADDRESS",
            source: "MANUAL",
            confidence: "1",
            verified: true,
            enabled: true,
            valid_from: now,
            valid_to: None,
            notes: None,
            evidence: &evidence
        })
        .await
        .is_err()
    );
    let matcher = ExecutionWalletRegistry::rebuild(repo.clone(), "0.90")
        .await
        .unwrap();
    assert_eq!(matcher.len().await, 1);
    assert!(matcher.match_address(active).await.is_some());
    matcher.refresh_at(now + Duration::hours(2)).await.unwrap();
    assert!(
        matcher
            .match_address(
                "0x0000000000000000000000000000000000000005"
                    .parse()
                    .unwrap()
            )
            .await
            .is_some()
    );
    repo.update_wallet(
        first.id,
        &WalletChanges {
            enabled: Some(false),
            verified: None,
            confidence: None,
            valid_from: None,
            valid_to: None,
            notes: None,
            evidence: None,
        },
    )
    .await
    .unwrap();
    matcher.refresh().await.unwrap();
    assert_eq!(matcher.len().await, 0);
    assert_eq!(
        ExecutionWalletRegistry::rebuild(repo, "0.90")
            .await
            .unwrap()
            .len()
            .await,
        0
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn expired_now_execution_identity_still_matches_at_historical_trade_time(pool: PgPool) {
    let repo = TraderRepository::new(pool.clone());
    let trader = repo
        .create(&NewTrader {
            handle: "historical-alice",
            display_name: None,
            manual_tier: None,
            notes: None,
        })
        .await
        .unwrap();
    let occurred = Utc::now() - Duration::days(19);
    let evidence = json!({"operator":"phase-8.2"});
    let wallet = repo
        .add_wallet(&NewTraderWallet {
            trader_id: trader.id,
            chain_id: pons_domain::ChainId::new(4663),
            address: ADDRESS.parse().unwrap(),
            role: "ROBINHOOD_EXECUTION_ADDRESS",
            source: "OPERATOR_VERIFIED",
            confidence: "0.95",
            verified: true,
            enabled: true,
            valid_from: occurred - Duration::days(9),
            valid_to: Some(occurred + Duration::days(10)),
            notes: None,
            evidence: &evidence,
        })
        .await
        .unwrap();
    let matcher = ExecutionWalletRegistry::rebuild(repo, "0.90")
        .await
        .unwrap();
    assert!(
        matcher.match_address(wallet.address).await.is_none(),
        "expired identity is absent from the current O(1) matcher"
    );
    assert_eq!(
        matcher
            .match_at(wallet.address, occurred)
            .await
            .unwrap()
            .unwrap()
            .trader_id,
        trader.id
    );
    assert!(
        matcher
            .match_at(wallet.address, occurred - Duration::days(10))
            .await
            .unwrap()
            .is_none()
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn csv_partial_success_reports_bad_and_duplicate_rows(pool: PgPool) {
    let (app, _) = app(pool.clone()).await;
    let (cookie, csrf) = login(&app).await;
    let csv = "handle,address,role,tier,confidence,notes\nalice,0x0000000000000000000000000000000000000011,ROBINHOOD_EXECUTION_ADDRESS,A,0.95,ok\nbad,not-an-address,ROBINHOOD_EXECUTION_ADDRESS,B,0.95,bad\nbob,0x0000000000000000000000000000000000000011,ROBINHOOD_EXECUTION_ADDRESS,C,0.95,duplicate\n";
    let response = request(
        &app,
        "POST",
        "/api/v1/admin/traders/import-csv",
        csv,
        Some((&cookie, &csrf)),
        "text/csv",
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let value = json_body(response).await;
    assert_eq!(value["results"][0]["success"], true);
    assert_eq!(value["results"][1]["success"], false);
    assert_eq!(value["results"][2]["success"], false);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM trader_wallets")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM traders")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM audit_logs WHERE action='trader.csv_import'"
        )
        .fetch_one(&pool)
        .await
        .unwrap()
            > 0
    );
}
