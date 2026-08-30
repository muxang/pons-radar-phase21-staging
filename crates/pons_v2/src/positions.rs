use chrono::{TimeDelta, Utc};
use pons_storage::repositories::PositionRepository;
use std::time::Duration;
use thiserror::Error;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug)]
pub struct PositionWorkerSettings {
    pub concurrency: usize,
    pub poll_interval: Duration,
    pub retry_minimum: Duration,
    pub retry_maximum: Duration,
}
#[derive(Debug, Error)]
pub enum PositionError {
    #[error("invalid position worker settings")]
    InvalidSettings,
    #[error("position storage failed: {0}")]
    Storage(String),
    #[error("position task failed: {0}")]
    Task(String),
}
#[derive(Clone)]
pub struct PositionWorker {
    repository: PositionRepository,
    settings: PositionWorkerSettings,
}
impl PositionWorker {
    #[allow(clippy::missing_errors_doc)]
    pub fn new(
        repository: PositionRepository,
        settings: PositionWorkerSettings,
    ) -> Result<Self, PositionError> {
        if settings.concurrency == 0
            || settings.poll_interval.is_zero()
            || settings.retry_minimum.is_zero()
            || settings.retry_minimum > settings.retry_maximum
        {
            return Err(PositionError::InvalidSettings);
        }
        Ok(Self {
            repository,
            settings,
        })
    }
    #[allow(clippy::missing_errors_doc)]
    pub async fn run_until(self, cancellation: CancellationToken) -> Result<(), PositionError> {
        let mut tasks = JoinSet::new();
        for _ in 0..self.settings.concurrency {
            let worker = self.clone();
            let token = cancellation.clone();
            tasks.spawn(async move { worker.loop_until(token).await });
        }
        while let Some(v) = tasks.join_next().await {
            v.map_err(|e| PositionError::Task(e.to_string()))??;
        }
        Ok(())
    }
    async fn loop_until(&self, cancellation: CancellationToken) -> Result<(), PositionError> {
        loop {
            if cancellation.is_cancelled() {
                return Ok(());
            }
            if let Some(job) = self.repository.claim_due().await.map_err(storage)? {
                if let Err(error) = self.repository.rebuild(&job).await {
                    let delay = self
                        .settings
                        .retry_minimum
                        .saturating_mul(
                            2_u32.saturating_pow(
                                u32::try_from(job.attempts.saturating_sub(1))
                                    .unwrap_or(0)
                                    .min(20),
                            ),
                        )
                        .min(self.settings.retry_maximum);
                    self.repository
                        .retry(
                            &job,
                            &error.to_string(),
                            Utc::now() + TimeDelta::from_std(delay).unwrap_or(TimeDelta::MAX),
                        )
                        .await
                        .map_err(storage)?;
                }
                continue;
            }
            tokio::select! {()=cancellation.cancelled()=>return Ok(()),()=tokio::time::sleep(self.settings.poll_interval)=>{}}
        }
    }
}
#[allow(clippy::needless_pass_by_value)]
fn storage(e: sqlx::Error) -> PositionError {
    PositionError::Storage(e.to_string())
}
