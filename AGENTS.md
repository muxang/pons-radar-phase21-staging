This project is a read-only Pons V2 research/monitoring system.

Never implement:
- transaction signing
- private-key storage
- automatic buy/sell
- copy trading

Only Pons V2 is supported.

Chain facts are authoritative.

Do not use Fomo WebSocket as a required dependency.
Do not use relayer address or tx.from as trader identity.

A Smart Money BUY requires Pons V2 CurveBuy evidence and a monitored execution
address as the real token recipient, with token-flow confirmation according to SPEC.

A Smart Money SELL requires Pons V2 CurveSell evidence and monitored-wallet
seller/token-flow confirmation according to SPEC.

Do not classify a plain ERC20 transfer as a BUY.

All asset amounts use U256 / Decimal. Never f64.

All chain ingestion must be idempotent.
All durable alerts must be committed before WebSocket push.
All derived state must be rebuildable from durable source events.

Original token metadata is immutable.
Metadata/research changes are snapshots.

Application upgrades require signed manifest + SHA256 + rollback.
Old frontend build IDs must be detected after upgrade and the browser must be
prompted to refresh. Never force refresh if there are unsaved admin changes.

Every phase must include tests.
Do not silently alter SPEC architecture.

