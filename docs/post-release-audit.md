# Post-release production audit

Audit date: 2026-08-30. Scope: Phase 0 through Phase 21 implementation after
the v0.1.11 WSS refresh fix. This audit does not add product functionality and
does not change Phase 11 production signal rules.

## Executive result

- No forbidden signing, private-key storage, automatic trading, copy-trading,
  `tx.from` identity, `f64`, handler SQL, unsafe Rust, `innerHTML`, or `eval`
  implementation was found by repository-wide static checks.
- Staging retains zero semantic duplicate `token_trades` and zero duplicate
  `smart_trades`; migration 27 is current.
- `/healthz` remains public (200), while `/api/v1/tokens` without a session is
  rejected (401) and a wrong-Origin admin mutation is rejected (403).
- Root-owned configuration and EnvironmentFile are `root:pons-radar 0640`.
  Application and updater binaries are `pons-radar:pons-radar 0750`.
- Frontend production dependencies and development dependencies report zero
  npm audit vulnerabilities.

## Findings and remediation

### High-frequency Tokens REST refresh — fixed

Root cause: every durable trade/signal WSS envelope immediately invalidated the
active Tokens query. A busy chain or replay burst could therefore turn WSS
delivery into one full REST request per event.

Remediation: invalidations are now coalesced for 1.5 seconds. One burst causes
at most one dashboard refresh, one Tokens list refresh, and one predicate-based
refresh covering all affected token detail queries. WSS remains the ordered
notification/replay transport; REST remains the authoritative page read model.
Search input is separately debounced so typing does not issue one API request
per keystroke.

### Tokens information hierarchy — improved

The list now shows matching-result count, explicitly describes WSS-synced/REST-
authoritative semantics, provides a single clear-filter action, separates Score
from Confidence, labels Smart net flow, and visually prioritizes HIGH_PRIORITY
and STRONG_WATCH rows without obscuring other data. The table has an accessible
caption; address-copy controls have an accessible label and cannot accidentally
submit a surrounding form.

### Release workflow warning — fixed

The inline YAML value `components: rustfmt, clippy` was interpreted as an
unexpected `clippy` action input. It is now quoted as one components value.

## Dependency review

`npm audit` reports zero vulnerabilities. RustSec reports two advisories and one
maintenance warning in the resolved lock metadata:

- `rsa 0.9.10` is an unconditional transitive dependency of SQLx PostgreSQL.
  RustSec lists no fixed compatible release. Pons Radar performs no RSA private-
  key operations and stores no transaction/signing private keys, so the Marvin
  private-key timing attack is not exposed by application behavior. Track the
  upstream SQLx resolution; do not suppress the advisory silently.
- `rkyv 0.7.46` is present in Cargo lock metadata as an optional
  `rust_decimal` dependency but is absent from the enabled workspace feature
  graph (`cargo tree -i rkyv` has no path). Direct `rust_decimal` dependencies
  explicitly disable default features. Track upstream/auditor feature-aware
  handling.
- `paste 1.0.15` is an unmaintained build-time proc-macro dependency through
  Alloy. It is not runtime code; track Alloy's migration.

These are recorded residual supply-chain items, not silently reported as a
clean RustSec scan.

## Architecture boundary review

- Chain events remain authoritative and ingestion remains database-idempotent.
- Smart BUY continues to require known Pons CurveBuy recipient plus Transfer
  evidence; Smart SELL continues to use seller plus token-flow evidence.
- Plain Transfer does not become a BUY.
- SQL remains in repositories rather than API handlers.
- Durable outbox remains authoritative before WSS delivery.
- Derived state remains rebuildable from durable facts.
- Historical/backfill events retain non-realtime semantics.
- Admin mutation protection remains session + Origin + CSRF.
- AI and dynamic PonsTraderScore remain disabled in the production Signal.
- No automatic trading, transaction signing, private-key storage, or copy
  trading was introduced.

## Verification commands

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test --workspace` with PostgreSQL
- `npm run typecheck`, `npm test`, `npm run build`, `npm audit`
- repository static boundary scans
- staging health, readiness, authorization, filesystem permissions, migration,
  and duplicate-identity SQL checks

