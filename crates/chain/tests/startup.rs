use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use pons_chain::{
    ChainHealth, ReconnectPolicy, RpcStatus, RunError, WsChainProbe, WsStartupState,
    probe_ws_startup, reconnect_ws_until_ready,
};
use pons_domain::ChainId;
use tokio_util::sync::CancellationToken;

struct Probe(Mutex<VecDeque<Result<ChainId, RunError>>>);

#[async_trait]
impl WsChainProbe for Probe {
    async fn chain_id(&self) -> Result<ChainId, RunError> {
        self.0
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Ok(ChainId::new(4663)))
    }
}

#[tokio::test]
async fn unavailable_ws_is_degraded_then_recovers_in_background() {
    let probe = Arc::new(Probe(Mutex::new(VecDeque::from([
        Err(RunError::WebSocket("temporarily unavailable".into())),
        Ok(ChainId::new(4663)),
    ]))));
    let health = ChainHealth::default();
    assert_eq!(
        probe_ws_startup(probe.as_ref(), ChainId::new(4663), &health)
            .await
            .unwrap(),
        WsStartupState::Degraded
    );
    assert_eq!(health.snapshot().await.websocket, RpcStatus::Reconnecting);
    reconnect_ws_until_ready(
        probe,
        ChainId::new(4663),
        health.clone(),
        ReconnectPolicy {
            minimum: Duration::from_millis(1),
            maximum: Duration::from_millis(2),
        },
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(health.snapshot().await.websocket, RpcStatus::Healthy);
}

#[tokio::test]
async fn reachable_wrong_chain_ws_fails_closed() {
    let probe = Probe(Mutex::new(VecDeque::from([Ok(ChainId::new(1))])));
    let health = ChainHealth::default();
    assert!(matches!(
        probe_ws_startup(&probe, ChainId::new(4663), &health).await,
        Err(RunError::WrongChain { .. })
    ));
    assert_eq!(health.snapshot().await.websocket, RpcStatus::Degraded);
}
