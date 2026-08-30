use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use chrono::{TimeZone, Utc};
use pons_radar::server::router_with_database;
use pons_storage::Database;
use sqlx::PgPool;
use tower::ServiceExt;

#[sqlx::test(migrations = "../../migrations")]
async fn readyz_reports_ready_with_a_live_postgres(pool: PgPool) {
    let app = router_with_database(
        Utc.with_ymd_and_hms(2026, 8, 28, 0, 0, 0).unwrap(),
        Database::from_pool(pool),
    );
    let response = app
        .oneshot(Request::get("/readyz").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}
