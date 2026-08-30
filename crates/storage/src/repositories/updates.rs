#![allow(clippy::missing_errors_doc)]

use super::{EventOutboxRepository, NewOutboxEvent};
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct UpdateRepository {
    pool: PgPool,
}

#[derive(Clone, Debug, FromRow, Serialize)]
pub struct UpdateJob {
    pub id: Uuid,
    pub state: String,
    pub current_version: String,
    pub target_version: String,
    pub release_id: i64,
    pub release_tag: String,
    pub channel: String,
    pub manifest: Value,
    pub manifest_sha256: String,
    pub asset_filename: Option<String>,
    pub asset_sha256: Option<String>,
    pub signature_key_id: String,
    pub schema_compatible: bool,
    pub rollback_safe: bool,
    pub admin_user_id: Option<Uuid>,
    pub staging_path: Option<String>,
    pub backup_path: Option<String>,
    pub error: Option<String>,
    pub rollback_result: Option<String>,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, FromRow, Serialize)]
pub struct ReleaseHistory {
    pub id: Uuid,
    pub update_job_id: Uuid,
    pub old_version: String,
    pub new_version: String,
    pub release_tag: String,
    pub manifest_sha256: String,
    pub frontend_build_id: String,
    pub api_schema_version: i32,
    pub outcome: String,
    pub rollback_result: Option<String>,
    pub completed_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct NewUpdateJob<'a> {
    pub current_version: &'a str,
    pub target_version: &'a str,
    pub release_id: i64,
    pub release_tag: &'a str,
    pub channel: &'a str,
    pub manifest: &'a Value,
    pub manifest_sha256: &'a str,
    pub asset_filename: &'a str,
    pub asset_sha256: &'a str,
    pub signature_key_id: &'a str,
    pub admin_user_id: Uuid,
}

impl UpdateRepository {
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn status(&self) -> Result<Value, sqlx::Error> {
        let state: Value = sqlx::query_scalar("SELECT jsonb_build_object('health',health,'last_checked_at',last_checked_at,'last_successful_check_at',last_successful_check_at,'last_error',last_error,'latest_release',latest_release) FROM updater_state WHERE singleton").fetch_one(&self.pool).await?;
        let active: Option<Value> = sqlx::query_scalar("SELECT to_jsonb(u) FROM update_jobs u WHERE state IN('DOWNLOADING','VERIFYING','STAGED','INSTALLING','RESTARTING','VERIFYING_HEALTH') ORDER BY started_at DESC LIMIT 1").fetch_optional(&self.pool).await?;
        Ok(serde_json::json!({"state":state,"active_job":active}))
    }

    pub async fn record_check(
        &self,
        release: Option<&Value>,
        error: Option<&str>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE updater_state SET health=CASE WHEN $2::text IS NULL THEN'HEALTHY'ELSE'DEGRADED'END,last_checked_at=now(),last_successful_check_at=CASE WHEN $2::text IS NULL THEN now()ELSE last_successful_check_at END,last_error=$2,latest_release=COALESCE($1,latest_release),updated_at=now()WHERE singleton").bind(release).bind(error).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn create_install(&self, v: &NewUpdateJob<'_>) -> Result<UpdateJob, sqlx::Error> {
        sqlx::query_as("INSERT INTO update_jobs(state,current_version,target_version,release_id,release_tag,channel,manifest,manifest_sha256,asset_filename,asset_sha256,signature_key_id,schema_compatible,rollback_safe,admin_user_id)VALUES('DOWNLOADING',$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,true,true,$11)RETURNING *")
            .bind(v.current_version).bind(v.target_version).bind(v.release_id).bind(v.release_tag).bind(v.channel).bind(v.manifest).bind(v.manifest_sha256).bind(v.asset_filename).bind(v.asset_sha256).bind(v.signature_key_id).bind(v.admin_user_id).fetch_one(&self.pool).await
    }

    pub async fn transition(
        &self,
        id: Uuid,
        from: &[&str],
        to: &str,
        error: Option<&str>,
    ) -> Result<bool, sqlx::Error> {
        Ok(sqlx::query("UPDATE update_jobs SET state=$3,error=$4,completed_at=CASE WHEN $3 IN('SUCCEEDED','FAILED','ROLLED_BACK','ROLLBACK_FAILED')THEN now()ELSE completed_at END,updated_at=now() WHERE id=$1 AND state=ANY($2)").bind(id).bind(from).bind(to).bind(error).execute(&self.pool).await?.rows_affected()==1)
    }

    pub async fn set_paths(
        &self,
        id: Uuid,
        staging: &str,
        backup: &str,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE update_jobs SET staging_path=$2,backup_path=$3,state='STAGED',updated_at=now()WHERE id=$1 AND state IN('DOWNLOADING','VERIFYING')").bind(id).bind(staging).bind(backup).execute(&self.pool).await?;
        Ok(())
    }
    pub async fn mark_installing(
        &self,
        id: Uuid,
        old_version: &str,
        new_version: &str,
        frontend_build_id: &str,
    ) -> Result<bool, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let changed=sqlx::query("UPDATE update_jobs SET state='INSTALLING',updated_at=now()WHERE id=$1 AND state='STAGED'").bind(id).execute(&mut*tx).await?.rows_affected()==1;
        if changed {
            let payload = serde_json::json!({"job_id":id,"old_version":old_version,"new_version":new_version,"frontend_build_id":frontend_build_id,"realtime_alert_eligible":true,"event_effective_at":Utc::now()});
            EventOutboxRepository::append_in_transaction(
                &mut tx,
                &NewOutboxEvent {
                    event_type: "system.update_installing",
                    schema_version: 1,
                    aggregate_type: Some("update_job"),
                    aggregate_id: Some(id),
                    dedupe_key: &format!("system.update_installing:{id}"),
                    payload: &payload,
                },
            )
            .await?;
        }
        tx.commit().await?;
        Ok(changed)
    }

    pub async fn history(&self, limit: i64) -> Result<Vec<ReleaseHistory>, sqlx::Error> {
        sqlx::query_as("SELECT * FROM release_history ORDER BY completed_at DESC,id DESC LIMIT $1")
            .bind(limit.clamp(1, 100))
            .fetch_all(&self.pool)
            .await
    }

    pub async fn recoverable(&self) -> Result<Vec<UpdateJob>, sqlx::Error> {
        sqlx::query_as("SELECT * FROM update_jobs WHERE state IN('INSTALLING','RESTARTING','VERIFYING_HEALTH') ORDER BY started_at").fetch_all(&self.pool).await
    }
    pub async fn complete_recovered(
        &self,
        job: &UpdateJob,
        frontend_build_id: &str,
        api_schema_version: i32,
    ) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let changed=sqlx::query("UPDATE update_jobs SET state='SUCCEEDED',completed_at=now(),updated_at=now()WHERE id=$1 AND state IN('INSTALLING','RESTARTING','VERIFYING_HEALTH')").bind(job.id).execute(&mut*tx).await?.rows_affected()==1;
        if changed {
            sqlx::query("INSERT INTO release_history(update_job_id,old_version,new_version,release_tag,manifest_sha256,frontend_build_id,api_schema_version,outcome)VALUES($1,$2,$3,$4,$5,$6,$7,'SUCCEEDED')ON CONFLICT(update_job_id)DO NOTHING").bind(job.id).bind(&job.current_version).bind(&job.target_version).bind(&job.release_tag).bind(&job.manifest_sha256).bind(frontend_build_id).bind(api_schema_version).execute(&mut*tx).await?;
            let payload = serde_json::json!({"job_id":job.id,"old_version":job.current_version,"new_version":job.target_version,"frontend_build_id":frontend_build_id,"completed_at":Utc::now(),"realtime_alert_eligible":true,"event_effective_at":Utc::now()});
            EventOutboxRepository::append_in_transaction(
                &mut tx,
                &NewOutboxEvent {
                    event_type: "system.update_applied",
                    schema_version: 1,
                    aggregate_type: Some("update_job"),
                    aggregate_id: Some(job.id),
                    dedupe_key: &format!("system.update_applied:{}", job.id),
                    payload: &payload,
                },
            )
            .await?;
        }
        tx.commit().await
    }
    pub async fn fail(&self, id: Uuid, error: &str) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let changed=sqlx::query("UPDATE update_jobs SET state='FAILED',error=$2,completed_at=now(),updated_at=now()WHERE id=$1 AND state IN('DOWNLOADING','VERIFYING','STAGED')").bind(id).bind(error).execute(&mut*tx).await?.rows_affected()==1;
        if changed {
            sqlx::query("INSERT INTO release_history(update_job_id,old_version,new_version,release_tag,manifest_sha256,frontend_build_id,api_schema_version,outcome)SELECT id,current_version,target_version,release_tag,manifest_sha256,manifest->>'frontend_build_id',(manifest->>'api_schema_version')::integer,'FAILED' FROM update_jobs WHERE id=$1 ON CONFLICT(update_job_id)DO NOTHING").bind(id).execute(&mut*tx).await?;
            let payload = serde_json::json!({"job_id":id,"error":error,"realtime_alert_eligible":true,"event_effective_at":Utc::now()});
            EventOutboxRepository::append_in_transaction(
                &mut tx,
                &NewOutboxEvent {
                    event_type: "system.update_failed",
                    schema_version: 1,
                    aggregate_type: Some("update_job"),
                    aggregate_id: Some(id),
                    dedupe_key: &format!("system.update_failed:{id}"),
                    payload: &payload,
                },
            )
            .await?;
        }
        tx.commit().await
    }
}
