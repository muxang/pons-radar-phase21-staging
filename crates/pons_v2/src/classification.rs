use chrono::{TimeDelta, Utc};
use pons_storage::repositories::IdentityClassificationRepository;
use std::time::Duration;
use thiserror::Error;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug)]
pub struct ClassificationWorkerSettings {
    pub concurrency: usize,
    pub batch_size: i64,
    pub poll_interval: Duration,
    pub retry_minimum: Duration,
    pub retry_maximum: Duration,
}
#[derive(Debug, Error)]
pub enum ClassificationError {
    #[error("invalid identity classification worker settings")]
    InvalidSettings,
    #[error("identity classification storage failed: {0}")]
    Storage(String),
    #[error("identity classification task failed: {0}")]
    Task(String),
}
#[derive(Clone)]
pub struct IdentityClassificationWorker {
    repository: IdentityClassificationRepository,
    settings: ClassificationWorkerSettings,
}
impl IdentityClassificationWorker {
    #[allow(clippy::missing_errors_doc)]
    pub fn new(
        repository: IdentityClassificationRepository,
        settings: ClassificationWorkerSettings,
    ) -> Result<Self, ClassificationError> {
        if settings.concurrency == 0
            || settings.batch_size <= 0
            || settings.poll_interval.is_zero()
            || settings.retry_minimum.is_zero()
            || settings.retry_minimum > settings.retry_maximum
        {
            return Err(ClassificationError::InvalidSettings);
        }
        Ok(Self {
            repository,
            settings,
        })
    }
    #[allow(clippy::missing_errors_doc)]
    pub async fn run_until(
        self,
        cancellation: CancellationToken,
    ) -> Result<(), ClassificationError> {
        let mut tasks = JoinSet::new();
        for _ in 0..self.settings.concurrency {
            let worker = self.clone();
            let token = cancellation.clone();
            tasks.spawn(async move { worker.loop_until(token).await });
        }
        while let Some(v) = tasks.join_next().await {
            v.map_err(|e| ClassificationError::Task(e.to_string()))??;
        }
        Ok(())
    }
    async fn loop_until(&self, cancellation: CancellationToken) -> Result<(), ClassificationError> {
        loop {
            if cancellation.is_cancelled() {
                return Ok(());
            }
            if let Some(job) = self.repository.claim_due().await.map_err(storage)? {
                if let Err(error) = self
                    .repository
                    .process_page(&job, self.settings.batch_size)
                    .await
                {
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
                            job.id,
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
fn storage(e: sqlx::Error) -> ClassificationError {
    ClassificationError::Storage(e.to_string())
}
