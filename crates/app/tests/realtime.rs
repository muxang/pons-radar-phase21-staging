use std::time::Duration;

use axum::{
    body::Body,
    http::{self, Request, StatusCode},
};
use futures_util::StreamExt;
use pons_radar::{
    auth::{AuthConfig, AuthService},
    realtime::{EventHub, RealtimeSettings},
    server::router_with_auth_and_realtime,
};
use pons_storage::{
    Database,
    repositories::{AlertRepository, AuthRepository, EventOutboxRepository, NewOutboxEvent},
};
use serde_json::{Value, json};
use sqlx::PgPool;
use tokio::net::TcpListener;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async, tungstenite::client::IntoClientRequest,
};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

async fn request(
    app: axum::Router,
    method: &str,
    path: &str,
    body: Value,
    headers: &[(&str, &str)],
) -> axum::response::Response {
    let mut builder = Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json");
    for (key, value) in headers {
        builder = builder.header(*key, *value);
    }
    app.oneshot(builder.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap()
}

async fn application(pool: &PgPool) -> (axum::Router, EventHub, String) {
    let auth = AuthService::new(
        AuthRepository::new(pool.clone()),
        AuthConfig {
            secure_cookie: false,
            session_hours: 8,
            allowed_origin: "http://radar.example".into(),
            setup_token: Some("setup-secret".into()),
        },
    );
    let hub = EventHub::new(
        EventOutboxRepository::new(pool.clone()),
        RealtimeSettings {
            poll_interval: Duration::from_millis(10),
            heartbeat_interval: Duration::from_millis(100),
            client_queue_capacity: 2,
            replay_limit_max: 2,
        },
    );
    let app = router_with_auth_and_realtime(
        chrono::Utc::now(),
        Database::from_pool(pool.clone()),
        auth,
        hub.clone(),
    );
    assert_eq!(
        request(
            app.clone(),
            "POST",
            "/api/v1/auth/setup",
            json!({"username":"admin","password":"correct horse battery staple"}),
            &[
                ("origin", "http://radar.example"),
                ("x-setup-token", "setup-secret")
            ]
        )
        .await
        .status(),
        StatusCode::CREATED
    );
    let login = request(
        app.clone(),
        "POST",
        "/api/v1/auth/login",
        json!({"username":"admin","password":"correct horse battery staple"}),
        &[("origin", "http://radar.example")],
    )
    .await;
    let cookie = login
        .headers()
        .get_all("set-cookie")
        .iter()
        .map(|v| v.to_str().unwrap().split(';').next().unwrap())
        .collect::<Vec<_>>()
        .join("; ");
    (app, hub, cookie)
}

async fn spawn(app: axum::Router) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("ws://{address}/ws"), task)
}

fn ws_request(url: &str, cookie: Option<&str>, origin: Option<&str>) -> http::Request<()> {
    let mut request = url.into_client_request().unwrap();
    if let Some(value) = cookie {
        request
            .headers_mut()
            .insert("cookie", value.parse().unwrap());
    }
    if let Some(value) = origin {
        request
            .headers_mut()
            .insert("origin", value.parse().unwrap());
    }
    request
}

async fn next_json(socket: &mut WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>) -> Value {
    loop {
        let message = socket.next().await.unwrap().unwrap();
        if message.is_text() {
            return serde_json::from_str(message.to_text().unwrap()).unwrap();
        }
    }
}

#[sqlx::test(migrations = "../../migrations")]
async fn authenticated_ws_hello_delivery_multi_client_and_security(pool: PgPool) {
    let (app, hub, cookie) = application(&pool).await;
    let (url, server) = spawn(app).await;
    assert!(
        connect_async(ws_request(&url, None, Some("http://radar.example")))
            .await
            .is_err()
    );
    assert!(
        connect_async(ws_request(&url, Some(&cookie), Some("http://evil.example")))
            .await
            .is_err()
    );
    let (mut first, _) = connect_async(ws_request(
        &url,
        Some(&cookie),
        Some("http://radar.example"),
    ))
    .await
    .unwrap();
    let (mut second, _) = connect_async(ws_request(
        &url,
        Some(&cookie),
        Some("http://radar.example"),
    ))
    .await
    .unwrap();
    let hello = next_json(&mut first).await;
    assert_eq!(hello["type"], "system.hello");
    assert!(hello["current_outbox_seq"].is_i64());
    let _ = next_json(&mut second).await;
    let cancellation = CancellationToken::new();
    tokio::spawn(hub.clone().run(cancellation.clone()));
    tokio::time::sleep(Duration::from_millis(20)).await;
    let payload = json!({"classification_source":"CHAIN_BACKFILL","realtime_alert_eligible":false,"confirmation_level":"CONFIRMED","chain_finality":"PENDING"});
    let persisted = hub
        .repository()
        .append(&NewOutboxEvent {
            event_type: "smart_trade.buy_backfilled",
            schema_version: 1,
            aggregate_type: None,
            aggregate_id: None,
            dedupe_key: "phase12:one",
            payload: &payload,
        })
        .await
        .unwrap();
    for socket in [&mut first, &mut second] {
        let event = next_json(socket).await;
        assert_eq!(event["seq"], persisted.seq);
        assert_eq!(event["realtime_alert_eligible"], false);
        assert_eq!(event["trade_evidence"], "CONFIRMED");
        assert_eq!(event["chain_finality"], "PENDING");
        assert_eq!(event["provisional"], true);
    }
    first.close(None).await.unwrap();
    let live_payload = json!({"classification_source":"LIVE","realtime_alert_eligible":true,"chain_finality":"CONFIRMED"});
    hub.repository()
        .append(&NewOutboxEvent {
            event_type: "signal.watch",
            schema_version: 1,
            aggregate_type: None,
            aggregate_id: None,
            dedupe_key: "phase12:two",
            payload: &live_payload,
        })
        .await
        .unwrap();
    let event = next_json(&mut second).await;
    assert!(event["seq"].as_i64().unwrap() > persisted.seq);
    cancellation.cancel();
    server.abort();
}

#[sqlx::test(migrations = "../../migrations")]
async fn replay_is_authenticated_bounded_and_stable_during_live_race(pool: PgPool) {
    let (app, hub, cookie) = application(&pool).await;
    let payload = json!({"realtime_alert_eligible":true});
    for number in 1..=4 {
        hub.repository()
            .append(&NewOutboxEvent {
                event_type: "fixture",
                schema_version: 1,
                aggregate_type: None,
                aggregate_id: None,
                dedupe_key: &format!("replay:{number}"),
                payload: &payload,
            })
            .await
            .unwrap();
    }
    assert_eq!(
        request(
            app.clone(),
            "GET",
            "/api/v1/events?after_seq=0",
            json!(null),
            &[]
        )
        .await
        .status(),
        StatusCode::UNAUTHORIZED
    );
    let page = request(
        app.clone(),
        "GET",
        "/api/v1/events?after_seq=0&through_seq=4&limit=100",
        json!(null),
        &[("cookie", &cookie)],
    )
    .await;
    assert_eq!(page.status(), StatusCode::OK);
    let body = http_body_util::BodyExt::collect(page.into_body())
        .await
        .unwrap()
        .to_bytes();
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["events"].as_array().unwrap().len(), 2);
    assert_eq!(body["next_seq"], 2);
    let next = request(
        app,
        "GET",
        "/api/v1/events?after_seq=2&through_seq=4&limit=2",
        json!(null),
        &[("cookie", &cookie)],
    )
    .await;
    let body = http_body_util::BodyExt::collect(next.into_body())
        .await
        .unwrap()
        .to_bytes();
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["events"][0]["seq"], 3);
    assert_eq!(body["events"][1]["seq"], 4);
}

#[sqlx::test(migrations = "../../migrations")]
async fn expired_and_revoked_sessions_cannot_upgrade(pool: PgPool) {
    let (app, _, cookie) = application(&pool).await;
    let (url, server) = spawn(app).await;
    sqlx::query("UPDATE admin_sessions SET created_at=now()-interval '2 hours',expires_at=now()-interval '1 second'")
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        connect_async(ws_request(
            &url,
            Some(&cookie),
            Some("http://radar.example")
        ))
        .await
        .is_err()
    );
    sqlx::query("UPDATE admin_sessions SET expires_at=now()+interval '1 hour',revoked_at=now()")
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        connect_async(ws_request(
            &url,
            Some(&cookie),
            Some("http://radar.example")
        ))
        .await
        .is_err()
    );
    server.abort();
}

#[sqlx::test(migrations = "../../migrations")]
async fn alert_center_and_preferences_require_auth_origin_and_csrf(pool: PgPool) {
    let (app, hub, cookie) = application(&pool).await;
    let payload =
        json!({"realtime_alert_eligible":true,"event_effective_at":"2026-08-01T00:00:00Z"});
    hub.repository()
        .append(&NewOutboxEvent {
            event_type: "system.warning",
            schema_version: 1,
            aggregate_type: None,
            aggregate_id: None,
            dedupe_key: "api-alert",
            payload: &payload,
        })
        .await
        .unwrap();
    let repo = AlertRepository::new(pool.clone());
    while repo.process_next().await.unwrap() {}
    assert_eq!(
        request(app.clone(), "GET", "/api/v1/alerts", json!(null), &[])
            .await
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let listed = request(
        app.clone(),
        "GET",
        "/api/v1/alerts?limit=1",
        json!(null),
        &[("cookie", &cookie)],
    )
    .await;
    assert_eq!(listed.status(), StatusCode::OK);
    let bytes = http_body_util::BodyExt::collect(listed.into_body())
        .await
        .unwrap()
        .to_bytes();
    let values: Value = serde_json::from_slice(&bytes).unwrap();
    let id = values[0]["id"].as_str().unwrap();
    assert_eq!(
        request(
            app.clone(),
            "PATCH",
            &format!("/api/v1/alerts/{id}"),
            json!({"read":true,"acknowledged":true}),
            &[("cookie", &cookie)]
        )
        .await
        .status(),
        StatusCode::FORBIDDEN
    );
    let csrf = cookie
        .split("; ")
        .find_map(|v| v.strip_prefix("pons_csrf="))
        .unwrap();
    assert_eq!(
        request(
            app.clone(),
            "PATCH",
            &format!("/api/v1/alerts/{id}"),
            json!({"read":true,"acknowledged":true}),
            &[
                ("cookie", &cookie),
                ("origin", "http://radar.example"),
                ("x-csrf-token", csrf)
            ]
        )
        .await
        .status(),
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        request(
            app.clone(),
            "GET",
            "/api/v1/alert-preferences",
            json!(null),
            &[("cookie", &cookie)]
        )
        .await
        .status(),
        StatusCode::OK
    );
    let body = json!({"sound_enabled":false,"voice_enabled":false,"desktop_notifications_enabled":false,"speak_strong":true,"speak_high_priority":true,"speak_wallet_close":true,"speak_distribution":true,"speak_system_update":true,"smart_buy_alerts":false,"provisional_alerts":false,"minimum_signal_score":"50","minimum_smart_trade_amount":null});
    assert_eq!(
        request(
            app,
            "PUT",
            "/api/v1/alert-preferences",
            body,
            &[
                ("cookie", &cookie),
                ("origin", "http://radar.example"),
                ("x-csrf-token", csrf)
            ]
        )
        .await
        .status(),
        StatusCode::OK
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn production_web_read_models_require_session_and_are_bounded(pool: PgPool) {
    let (app, _hub, cookie) = application(&pool).await;
    assert_eq!(
        request(app.clone(), "GET", "/api/v1/dashboard", json!({}), &[])
            .await
            .status(),
        StatusCode::UNAUTHORIZED
    );
    for path in [
        "/api/v1/dashboard",
        "/api/v1/tokens?limit=25&offset=0",
        "/api/v1/system/health",
    ] {
        let response = request(app.clone(), "GET", path, json!({}), &[("cookie", &cookie)]).await;
        assert_eq!(response.status(), StatusCode::OK, "{path}");
    }
}
