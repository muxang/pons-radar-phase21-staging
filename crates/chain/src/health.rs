use std::sync::Arc;

use pons_domain::BlockNumber;
use tokio::sync::RwLock;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RpcStatus {
    Connecting,
    Reconnecting,
    Healthy,
    Degraded,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChainHealthSnapshot {
    pub http: RpcStatus,
    pub websocket: RpcStatus,
    pub head: Option<BlockNumber>,
    pub cursor: Option<BlockNumber>,
    pub lag_blocks: Option<u64>,
    pub ws_reconnects: u64,
    pub last_error: Option<String>,
}

impl Default for ChainHealthSnapshot {
    fn default() -> Self {
        Self {
            http: RpcStatus::Connecting,
            websocket: RpcStatus::Connecting,
            head: None,
            cursor: None,
            lag_blocks: None,
            ws_reconnects: 0,
            last_error: None,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct ChainHealth(Arc<RwLock<ChainHealthSnapshot>>);

impl ChainHealth {
    pub async fn snapshot(&self) -> ChainHealthSnapshot {
        self.0.read().await.clone()
    }

    pub async fn mark_http_healthy(&self) {
        self.update(|state| state.http = RpcStatus::Healthy).await;
    }

    pub(crate) async fn update(&self, update: impl FnOnce(&mut ChainHealthSnapshot)) {
        let mut state = self.0.write().await;
        update(&mut state);
    }
}
