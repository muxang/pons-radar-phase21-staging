use pons_storage::repositories::AlertRepository;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

pub async fn run_alert_engine(repository: AlertRepository, shutdown: CancellationToken) {
    let mut interval = tokio::time::interval(Duration::from_millis(100));
    loop {
        tokio::select! {()=shutdown.cancelled()=>break,_=interval.tick()=>loop{match repository.process_next().await{Ok(true)=>{},Ok(false)=>break,Err(error)=>{tracing::error!(%error,"alert engine failed");break}}}}
    }
}
