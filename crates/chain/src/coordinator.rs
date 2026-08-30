use std::{sync::Arc, time::Duration};

use futures_util::FutureExt;
use pons_domain::{BlockNumber, ChainId};
use pons_storage::repositories::ChainCursorRepository;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{
    BatchHandler, ChainBatch, ChainHealth, ChainRpc, IngestionSource, LogFilter, RpcStatus,
    SubscriptionFactory,
};

#[derive(Debug, Error)]
pub enum RunError {
    #[error("RPC error: {0}")]
    Rpc(String),
    #[error("WebSocket error: {0}")]
    WebSocket(String),
    #[error("WebSocket disconnected")]
    Disconnected,
    #[error("invalid RPC response: {0}")]
    InvalidResponse(String),
    #[error("expected chain {expected:?}, received {actual:?}")]
    WrongChain { expected: ChainId, actual: ChainId },
    #[error("block {0} was not returned by the RPC")]
    MissingBlock(u64),
    #[error("RPC head {head} is behind durable cursor {cursor}")]
    RpcBehindCursor { head: u64, cursor: u64 },
    #[error("durable cursor block hash no longer matches block {block}")]
    CursorHashMismatch { block: u64 },
    #[error("storage error: {0}")]
    Storage(String),
    #[error("batch handler error: {0}")]
    Handler(String),
}

impl From<sqlx::Error> for RunError {
    fn from(error: sqlx::Error) -> Self {
        Self::Storage(error.to_string())
    }
}

#[derive(Clone, Copy, Debug)]
pub struct BackfillSettings {
    pub start_block: BlockNumber,
    pub chunk_blocks: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct ReconnectPolicy {
    pub minimum: Duration,
    pub maximum: Duration,
}

impl ReconnectPolicy {
    #[must_use]
    pub fn delay(self, attempt: u32) -> Duration {
        let multiplier = 1_u32.checked_shl(attempt.min(31)).unwrap_or(u32::MAX);
        self.minimum.saturating_mul(multiplier).min(self.maximum)
    }
}

pub struct BackfillCoordinator {
    expected_chain_id: ChainId,
    stream: String,
    http: Arc<dyn ChainRpc>,
    websocket: Arc<dyn SubscriptionFactory>,
    cursors: ChainCursorRepository,
    handler: Arc<dyn BatchHandler>,
    filter: LogFilter,
    settings: BackfillSettings,
    reconnect: ReconnectPolicy,
    health: ChainHealth,
}

impl BackfillCoordinator {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        expected_chain_id: ChainId,
        stream: impl Into<String>,
        http: Arc<dyn ChainRpc>,
        websocket: Arc<dyn SubscriptionFactory>,
        cursors: ChainCursorRepository,
        handler: Arc<dyn BatchHandler>,
        filter: LogFilter,
        settings: BackfillSettings,
        reconnect: ReconnectPolicy,
        health: ChainHealth,
    ) -> Self {
        Self {
            expected_chain_id,
            stream: stream.into(),
            http,
            websocket,
            cursors,
            handler,
            filter,
            settings,
            reconnect,
            health,
        }
    }

    /// Verifies the authoritative HTTP endpoint and fails closed on chain mismatch.
    ///
    /// # Errors
    ///
    /// Returns an RPC or wrong-chain error.
    pub async fn verify_http_chain(&self) -> Result<(), RunError> {
        let actual = self.http.chain_id().await?;
        if actual != self.expected_chain_id {
            return Err(RunError::WrongChain {
                expected: self.expected_chain_id,
                actual,
            });
        }
        self.health
            .update(|state| state.http = RpcStatus::Healthy)
            .await;
        Ok(())
    }

    /// Reconciles the durable cursor to the current HTTP head in bounded chunks.
    ///
    /// Handler completion precedes cursor persistence. A failure therefore causes safe
    /// at-least-once replay on the next attempt.
    ///
    /// # Errors
    ///
    /// Returns an RPC, handler, or cursor persistence error.
    pub async fn sync_once(&self) -> Result<(), RunError> {
        let head = self.http.block_number().await?;
        self.catch_up_to(head, IngestionSource::ChainBackfill).await
    }

    async fn catch_up_to(
        &self,
        head: BlockNumber,
        source: IngestionSource,
    ) -> Result<(), RunError> {
        let cursor = self.cursors.get(&self.stream).await?;
        if let Some(cursor) = &cursor {
            if cursor.chain_id != self.expected_chain_id {
                return Err(RunError::WrongChain {
                    expected: self.expected_chain_id,
                    actual: cursor.chain_id,
                });
            }
            if head < cursor.last_processed_block {
                return Err(RunError::RpcBehindCursor {
                    head: head.get(),
                    cursor: cursor.last_processed_block.get(),
                });
            }
            let restored_block = self
                .http
                .block(cursor.last_processed_block)
                .await?
                .ok_or(RunError::MissingBlock(cursor.last_processed_block.get()))?;
            if restored_block.hash != cursor.last_processed_hash {
                return Err(RunError::CursorHashMismatch {
                    block: cursor.last_processed_block.get(),
                });
            }
        }
        let mut from = cursor
            .as_ref()
            .map_or(self.settings.start_block.get(), |value| {
                value.last_processed_block.get().saturating_add(1)
            });
        self.update_progress(
            head,
            cursor.as_ref().map(|value| value.last_processed_block),
        )
        .await;

        while from <= head.get() {
            let to = from
                .saturating_add(self.settings.chunk_blocks.saturating_sub(1))
                .min(head.get());
            let to_block = BlockNumber::new(to);
            let mut logs = self
                .http
                .logs(BlockNumber::new(from), to_block, &self.filter)
                .await?;
            // `log_index` is canonical within a block, so it is also the stable fallback
            // for RPCs that omit `transactionIndex`. Hash bytes break malformed ties.
            logs.sort_by(|left, right| {
                (
                    left.block_number.get(),
                    left.transaction_index.unwrap_or(left.log_index.get()),
                    left.log_index.get(),
                    left.tx_hash.as_bytes(),
                )
                    .cmp(&(
                        right.block_number.get(),
                        right.transaction_index.unwrap_or(right.log_index.get()),
                        right.log_index.get(),
                        right.tx_hash.as_bytes(),
                    ))
            });
            let terminal = self
                .http
                .block(to_block)
                .await?
                .ok_or(RunError::MissingBlock(to))?;
            self.handler
                .handle(ChainBatch {
                    source,
                    chain_id: self.expected_chain_id,
                    from_block: BlockNumber::new(from),
                    to_block,
                    terminal_hash: terminal.hash,
                    logs,
                })
                .await?;
            self.cursors
                .upsert(
                    &self.stream,
                    self.expected_chain_id,
                    to_block,
                    terminal.hash,
                )
                .await?;
            self.update_progress(head, Some(to_block)).await;
            if to == u64::MAX {
                break;
            }
            from = to + 1;
        }
        self.health
            .update(|state| {
                state.http = RpcStatus::Healthy;
                state.last_error = None;
            })
            .await;
        Ok(())
    }

    /// Runs reconnecting WS notifications with HTTP reconciliation until cancelled.
    ///
    /// # Errors
    ///
    /// Returns only for initial HTTP chain verification failure. Runtime transport
    /// failures are marked degraded and retried until cancellation.
    pub async fn run_until(&self, cancellation: CancellationToken) -> Result<(), RunError> {
        self.verify_http_chain().await?;
        let mut attempt = 0_u32;
        while !cancellation.is_cancelled() {
            if let Err(error) = self.sync_once().await {
                self.mark_error(&error, false).await;
            }
            let connection = self
                .websocket
                .connect(self.expected_chain_id, &self.filter)
                .await;
            let mut subscription = match connection {
                Ok(subscription) => {
                    attempt = 0;
                    self.health
                        .update(|state| state.websocket = RpcStatus::Healthy)
                        .await;
                    subscription
                }
                Err(error) => {
                    self.mark_error(&error, true).await;
                    if matches!(error, RunError::WrongChain { .. }) {
                        return Err(error);
                    }
                    attempt = attempt.saturating_add(1);
                    if sleep_or_cancel(self.reconnect.delay(attempt - 1), &cancellation).await {
                        break;
                    }
                    continue;
                }
            };

            // Closes the sync/subscribe race. Until this succeeds the stream is explicitly
            // reconnect-catching-up; a WS wakeup is not proof that its missing range is live.
            let mut steady_live = match self.sync_once().await {
                Ok(()) => true,
                Err(error) => {
                    self.mark_error(&error, false).await;
                    false
                }
            };
            loop {
                tokio::select! {
                    () = cancellation.cancelled() => return Ok(()),
                    event = subscription.next() => match event {
                        Some(Ok(event)) => {
                            let mut target = event_block(&event);
                            let mut reconnect = false;
                            // Coalesce the already-buffered burst into one target-head catch-up.
                            // This task is the sole owner of the catch-up loop for this stream.
                            for _ in 0..1_024 {
                                match subscription.next().now_or_never() {
                                    Some(Some(Ok(next))) => target = target.max(event_block(&next)),
                                    Some(Some(Err(error))) => { self.mark_error(&error, true).await; reconnect = true; break; }
                                    Some(None) => { self.mark_error(&RunError::Disconnected, true).await; reconnect = true; break; }
                                    None => break,
                                }
                            }
                            if steady_live {
                                if let Err(error) = self.catch_up_to(target, IngestionSource::Live).await {
                                    self.mark_error(&error, false).await;
                                    steady_live = false;
                                }
                            } else {
                                // Reconcile to at least the notification target, but retain
                                // historical semantics for the entire gap-closing batch.
                                let reconnect_head = match self.http.block_number().await {
                                    Ok(head) => head.max(target),
                                    Err(error) => { self.mark_error(&error, false).await; target }
                                };
                                match self.catch_up_to(reconnect_head, IngestionSource::ChainBackfill).await {
                                    Ok(()) => steady_live = true,
                                    Err(error) => self.mark_error(&error, false).await,
                                }
                            }
                            if reconnect { break; }
                        }
                        Some(Err(error)) => { self.mark_error(&error, true).await; break; }
                        None => { self.mark_error(&RunError::Disconnected, true).await; break; }
                    }
                }
            }
            attempt = attempt.saturating_add(1);
            if sleep_or_cancel(self.reconnect.delay(attempt - 1), &cancellation).await {
                break;
            }
        }
        info!(stream = %self.stream, "chain coordinator stopped");
        Ok(())
    }

    async fn update_progress(&self, head: BlockNumber, cursor: Option<BlockNumber>) {
        self.health
            .update(|state| {
                state.head = Some(head);
                state.cursor = cursor;
                state.lag_blocks = cursor.map_or(Some(head.get()), |value| {
                    Some(head.get().saturating_sub(value.get()))
                });
            })
            .await;
    }

    async fn mark_error(&self, error: &RunError, websocket: bool) {
        warn!(stream = %self.stream, %error, "chain provider degraded");
        self.health
            .update(|state| {
                if websocket {
                    state.websocket = RpcStatus::Reconnecting;
                    state.ws_reconnects = state.ws_reconnects.saturating_add(1);
                } else {
                    state.http = RpcStatus::Degraded;
                }
                state.last_error = Some(error.to_string());
            })
            .await;
    }
}

fn event_block(event: &crate::SubscriptionEvent) -> BlockNumber {
    match event {
        crate::SubscriptionEvent::NewHead(number) => *number,
        crate::SubscriptionEvent::Log(log) => log.block_number,
    }
}

async fn sleep_or_cancel(duration: Duration, cancellation: &CancellationToken) -> bool {
    tokio::select! {
        () = cancellation.cancelled() => true,
        () = tokio::time::sleep(duration) => false,
    }
}
