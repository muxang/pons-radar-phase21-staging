use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use pons_domain::ChainId;
use tokio_util::sync::CancellationToken;

use crate::{ChainHealth, ReconnectPolicy, RpcStatus, RunError, WsRpcProvider};

#[async_trait]
pub trait WsChainProbe: Send + Sync {
    async fn chain_id(&self) -> Result<ChainId, RunError>;
}

#[async_trait]
impl WsChainProbe for WsRpcProvider {
    async fn chain_id(&self) -> Result<ChainId, RunError> {
        WsRpcProvider::chain_id(self).await
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WsStartupState {
    Healthy,
    Degraded,
}

/// Performs one non-fatal availability probe, while still failing closed if the
/// endpoint answers for a different chain.
///
/// # Errors
///
/// Returns [`RunError::WrongChain`] when a reachable endpoint identifies another chain.
pub async fn probe_ws_startup(
    probe: &dyn WsChainProbe,
    expected: ChainId,
    health: &ChainHealth,
) -> Result<WsStartupState, RunError> {
    match probe.chain_id().await {
        Ok(actual) if actual == expected => {
            health
                .update(|state| {
                    state.websocket = RpcStatus::Healthy;
                    state.last_error = None;
                })
                .await;
            Ok(WsStartupState::Healthy)
        }
        Ok(actual) => {
            let error = RunError::WrongChain { expected, actual };
            health
                .update(|state| {
                    state.websocket = RpcStatus::Degraded;
                    state.last_error = Some(error.to_string());
                })
                .await;
            Err(error)
        }
        Err(error) => {
            health
                .update(|state| {
                    state.websocket = RpcStatus::Reconnecting;
                    state.ws_reconnects = state.ws_reconnects.saturating_add(1);
                    state.last_error = Some(error.to_string());
                })
                .await;
            Ok(WsStartupState::Degraded)
        }
    }
}

/// Retries an unavailable WS endpoint with bounded exponential backoff. A later
/// wrong-chain response terminates the monitor without ever enabling the endpoint.
///
/// # Errors
///
/// Returns [`RunError::WrongChain`] when a retry reaches an endpoint on another chain.
pub async fn reconnect_ws_until_ready(
    probe: Arc<dyn WsChainProbe>,
    expected: ChainId,
    health: ChainHealth,
    policy: ReconnectPolicy,
    cancellation: CancellationToken,
) -> Result<(), RunError> {
    let mut attempt = 0_u32;
    loop {
        if cancellation.is_cancelled() {
            return Ok(());
        }
        match probe_ws_startup(probe.as_ref(), expected, &health).await? {
            WsStartupState::Healthy => return Ok(()),
            WsStartupState::Degraded => {
                let delay: Duration = policy.delay(attempt);
                attempt = attempt.saturating_add(1);
                tokio::select! {
                    () = cancellation.cancelled() => return Ok(()),
                    () = tokio::time::sleep(delay) => {}
                }
            }
        }
    }
}
