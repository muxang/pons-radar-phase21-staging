# Phase 1 storage design

Phase 1 establishes durable primitives only. It does not connect to Robinhood RPC,
decode Pons events, classify trades, or expose event replay APIs.

## Lossless identifiers and amounts

- `TokenAddress`, `CurveAddress`, `WalletAddress`, and `ContractAddress` wrap Alloy's
  20-byte `Address`. Text input is validated and canonical text output is lower-case,
  fixed-width `0x` hex. PostgreSQL stores the bytes in the `evm_address` domain.
- `TxHash` and `BlockHash` wrap Alloy's 32-byte `B256` and use the `evm_hash` domain.
- `BlockNumber`, `LogIndex`, and `ChainId` use Rust `u64`. PostgreSQL's reusable
  `uint64_numeric` domain preserves their entire range, unlike signed `BIGINT`.
- `RawAmount` wraps Alloy `U256`. PostgreSQL uses canonical base-10 `uint256_text`,
  constrained both syntactically and to `U256::MAX`. Text is intentional: PostgreSQL
  `NUMERIC` declarations can impose precision limits, while canonical text remains
  lossless and portable. `NormalizedAmount` wraps `rust_decimal::Decimal` for future
  human-readable calculations; it is not used for raw asset accounting.

## Migrations and repositories

SQLx applies the forward-only migration embedded in `pons-storage`. Startup fails if
connection or migration fails. SQL lives in repository modules, not HTTP handlers.
The initial repositories cover raw-log idempotent insertion and durable outbox append/
replay foundations. Later phase repositories should follow the same boundary.

`raw_chain_logs` is unique on `(chain_id, tx_hash, log_index)`. Repeating identical
input returns the same UUID; a conflicting payload at the same chain location fails
closed. A normalized result is idempotent on
`(raw_log_id, event_type, parser_version, schema_version)`. New parser/schema versions
can coexist with historical results so derived state remains rebuildable and auditable.
The separate deterministic `event_id` is SHA-256 over the stable chain coordinates,
event type, parser version, and schema version. It does not depend on the random
database row UUID, so a full rebuild recreates the same ID.
The chain-coordinate uniqueness key contains both versions, and a composite foreign
key prevents source-coordinate mismatch.

## Outbox replay sequence

`event_outbox.seq` is a PostgreSQL identity `BIGINT` and `dedupe_key` is the semantic
idempotency key. Appends lock the outbox table inside the same transaction before the
identity is allocated. This deliberately serializes durable event commits so a client
that has observed sequence N cannot later miss a lower sequence committed out of
order. Gaps are allowed and replay means `seq > cursor ORDER BY seq`.

`append_in_transaction` lets future domain repositories write source state and their
outbox row atomically. Delivery remains a later phase and must only happen after that
transaction commits.
