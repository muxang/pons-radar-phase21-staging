use std::{env, path::PathBuf, time::Duration};

use anyhow::{Context, Result};
use pons_updater::{HandoffPlan, atomic_replace_binary, wait_for_target_health};
use serde_json::json;
use sqlx::PgPool;
use tokio::process::Command;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<()> {
    let plan_path = env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .context("usage: pons-radar-updater <handoff-plan.json>")?;
    let plan: HandoffPlan = serde_json::from_slice(&tokio::fs::read(&plan_path).await?)?;
    wait_for_exit(plan.parent_pid, Duration::from_secs(plan.timeout_seconds)).await?;
    replace(&plan).await?;
    let installed = match restart(&plan.service_name).await {
        Ok(()) => wait_for_target_health(&reqwest::Client::new(), &plan).await,
        Err(error) => Err(error),
    };
    if let Err(error) = installed {
        match rollback(&plan).await {
            Ok(()) => finalize(&plan, "ROLLED_BACK", Some(&error.to_string())).await?,
            Err(rollback_error) => {
                finalize(
                    &plan,
                    "ROLLBACK_FAILED",
                    Some(&format!("new release: {error}; rollback: {rollback_error}")),
                )
                .await?;
                return Err(rollback_error).context("new release and rollback failed");
            }
        }
    } else {
        finalize(&plan, "SUCCEEDED", None).await?;
    }
    Ok(())
}

async fn finalize(plan: &HandoffPlan, outcome: &str, error: Option<&str>) -> Result<()> {
    let url = env::var("DATABASE_URL")
        .context("DATABASE_URL is required for durable updater completion")?;
    let pool = PgPool::connect(&url).await?;
    let id: Uuid = plan.job_id.parse()?;
    let mut tx = pool.begin().await?;
    sqlx::query("UPDATE update_jobs SET state=$2,error=$3,rollback_result=CASE WHEN $2='ROLLED_BACK'THEN'previous binary restored and healthy'WHEN $2='ROLLBACK_FAILED'THEN'previous service did not recover'ELSE NULL END,completed_at=now(),updated_at=now()WHERE id=$1").bind(id).bind(outcome).bind(error).execute(&mut*tx).await?;
    sqlx::query("INSERT INTO release_history(update_job_id,old_version,new_version,release_tag,manifest_sha256,frontend_build_id,api_schema_version,outcome,rollback_result)SELECT id,current_version,target_version,release_tag,manifest_sha256,$2,$3,$4,rollback_result FROM update_jobs WHERE id=$1 ON CONFLICT(update_job_id)DO NOTHING").bind(id).bind(&plan.frontend_build_id).bind(i32::try_from(plan.api_schema_version)?).bind(outcome).execute(&mut*tx).await?;
    let event = match outcome {
        "SUCCEEDED" => "system.update_applied",
        "ROLLED_BACK" => "system.update_rolled_back",
        _ => "system.update_rollback_failed",
    };
    let payload = json!({"job_id":id,"old_version":plan.previous_version,"new_version":plan.target_version,"frontend_build_id":plan.frontend_build_id,"completed_at":chrono::Utc::now(),"realtime_alert_eligible":true,"event_effective_at":chrono::Utc::now(),"error":error});
    sqlx::query("LOCK TABLE event_outbox IN EXCLUSIVE MODE")
        .execute(&mut *tx)
        .await?;
    sqlx::query("INSERT INTO event_outbox(event_type,schema_version,aggregate_type,aggregate_id,dedupe_key,payload)VALUES($1,1,'update_job',$2,$3,$4)ON CONFLICT(dedupe_key)DO NOTHING").bind(event).bind(id).bind(format!("{event}:{id}")).bind(payload).execute(&mut*tx).await?;
    tx.commit().await?;
    Ok(())
}

async fn wait_for_exit(pid: u32, timeout: Duration) -> Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let status = Command::new("kill")
            .args(["-0", &pid.to_string()])
            .status()
            .await?;
        if !status.success() {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!("main process did not exit before handoff timeout");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn replace(plan: &HandoffPlan) -> Result<()> {
    if let Some(parent) = plan.backup_binary.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::copy(&plan.current_binary, &plan.backup_binary).await?;
    atomic_replace_binary(&plan.staged_binary, &plan.current_binary).await?;
    Ok(())
}

async fn restart(service: &str) -> Result<()> {
    let status = Command::new("systemctl")
        .args(["restart", service])
        .status()
        .await?;
    if !status.success() {
        anyhow::bail!("systemctl restart failed");
    }
    Ok(())
}

async fn rollback(plan: &HandoffPlan) -> Result<()> {
    atomic_replace_binary(&plan.backup_binary, &plan.current_binary).await?;
    restart(&plan.service_name).await?;
    let mut old = plan.clone();
    old.target_version.clear();
    // Rollback success is based on liveness/readiness; the prior version is intentionally not guessed.
    let client = reqwest::Client::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(plan.timeout_seconds);
    loop {
        let health = client
            .get(format!("{}/healthz", plan.health_base_url))
            .send()
            .await;
        let ready = client
            .get(format!("{}/readyz", plan.health_base_url))
            .send()
            .await;
        if health.is_ok_and(|r| r.status().is_success())
            && ready.is_ok_and(|r| r.status().is_success())
        {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!("previous service failed health verification");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
