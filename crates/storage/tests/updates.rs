use pons_storage::repositories::{NewUpdateJob, UpdateRepository};
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

#[sqlx::test(migrations = "../../migrations")]
async fn update_lock_state_and_installing_event_are_durable(pool: PgPool) {
    let user: Uuid = sqlx::query_scalar(
        "INSERT INTO users(username,password_hash)VALUES('update-admin','argon2id')RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let repo = UpdateRepository::new(pool.clone());
    let manifest = json!({"app_version":"1.1.0"});
    let new = NewUpdateJob {
        current_version: "1.0.0",
        target_version: "1.1.0",
        release_id: 1,
        release_tag: "v1.1.0",
        channel: "stable",
        manifest: &manifest,
        manifest_sha256: &"11".repeat(32),
        asset_filename: "pons-radar-linux-x86_64.tar.gz",
        asset_sha256: &"22".repeat(32),
        signature_key_id: "release-2026",
        admin_user_id: user,
    };
    let job = repo.create_install(&new).await.unwrap();
    assert!(repo.create_install(&new).await.is_err());
    repo.set_paths(job.id, "/safe/staging", "/safe/backup")
        .await
        .unwrap();
    assert!(
        repo.mark_installing(job.id, "1.0.0", "1.1.0", "web-1")
            .await
            .unwrap()
    );
    assert!(
        !repo
            .mark_installing(job.id, "1.0.0", "1.1.0", "web-1")
            .await
            .unwrap()
    );
    let event: (String, serde_json::Value) =
        sqlx::query_as("SELECT event_type,payload FROM event_outbox WHERE aggregate_id=$1")
            .bind(job.id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(event.0, "system.update_installing");
    assert_eq!(event.1["new_version"], "1.1.0");
    assert_eq!(repo.recoverable().await.unwrap().len(), 1);
}
