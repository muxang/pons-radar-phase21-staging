# Phase 2 chain provider design

The `pons-chain` crate is protocol-independent. It contains no Pons ABI, deployment
address, or trade semantics.

## Transport roles

- HTTP JSON-RPC is authoritative for `eth_chainId`, current head, blocks, receipts,
  and bounded `eth_getLogs` ranges.
- WebSocket JSON-RPC subscribes to `newHeads` and raw logs. Notifications are wake-up
  signals; they are never the only source of facts.
- Both configured endpoints are checked against chain ID 4663 during application
  startup. A mismatch fails closed.

## Recovery and cursor ordering

Each logical filtered stream owns a PostgreSQL `chain_cursors` row. On startup and
after every WebSocket connection, notification, or reconnect, the coordinator reads
the HTTP head and fetches every range from `cursor + 1` in configured chunks.

For each chunk the order is:

1. HTTP `getLogs` and terminal block fetch.
2. `BatchHandler` durably handles the at-least-once batch.
3. Cursor advances to the terminal block/hash.

If handling or cursor persistence fails, the chunk is fetched again. Existing raw-log
database uniqueness provides effectively-once durable source facts. On recovery, the
stored cursor hash is compared with the HTTP block hash. A stale RPC behind the cursor
or a hash mismatch fails closed for later generic reorg recovery.

## Reconnect and health

WebSocket reconnect uses bounded exponential backoff. Health state records separate
HTTP/WS status, head, cursor, lag, reconnect count, and last error. Cancellation is
cooperative. Phase 2 exposes this state to later system APIs but does not add an
application WebSocket endpoint.
