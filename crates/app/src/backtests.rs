use std::time::Duration;

use pons_storage::repositories::BacktestRepository;
use tokio_util::sync::CancellationToken;

pub async fn run_backtest_worker(repository: BacktestRepository, cancellation: CancellationToken) {
    loop {
        tokio::select! {()=cancellation.cancelled()=>return,()=tokio::time::sleep(Duration::from_secs(1))=>{match repository.claim_due().await{Ok(Some(job))=>{if let Err(error)=repository.execute(&job).await{tracing::error!(%error,run_id=%job.id,"historical validation run failed");if let Err(db)=repository.fail(&job,&error.to_string()).await{tracing::error!(%db,"backtest retry persistence failed");}}},Ok(None)=>{},Err(error)=>tracing::error!(%error,"backtest job claim failed")}}}
    }
}
