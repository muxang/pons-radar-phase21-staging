use pons_storage::repositories::{TraderAnalyticsJob, TraderAnalyticsRepository};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

pub async fn run_trader_analytics_worker(
    repository: TraderAnalyticsRepository,
    cancellation: CancellationToken,
) {
    loop {
        tokio::select! {()=cancellation.cancelled()=>return,()=tokio::time::sleep(Duration::from_millis(500))=>{if let Err(error)=repository.enqueue_matured(chrono::Utc::now()).await{tracing::error!(%error,"trader outcome maturity scheduling failed");}match repository.claim_due().await{Ok(Some(job))=>process(&repository,&job).await,Ok(None)=>{},Err(error)=>tracing::error!(%error,"trader analytics claim failed")}}}
    }
}
async fn process(repository: &TraderAnalyticsRepository, job: &TraderAnalyticsJob) {
    if let Err(error) = repository.rebuild(job, chrono::Utc::now()).await {
        let exponent = u32::try_from(job.attempts.clamp(0, 8)).unwrap_or(8);
        let delay = Duration::from_secs(2_u64.saturating_pow(exponent).min(300));
        let next = chrono::Utc::now()
            + chrono::Duration::from_std(delay).unwrap_or(chrono::Duration::minutes(5));
        if let Err(retry) = repository.retry(job, &error.to_string(), next).await {
            tracing::error!(%retry,"trader analytics retry persistence failed");
        }
    }
}
