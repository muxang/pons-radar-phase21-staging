# Codex Development Guide
## Pons V2 + Fomo Smart Wallet Radar

Use together with `SPEC.md`.

The development method is intentionally incremental.

**Do not ask Codex to implement the entire system in one pass.**

For every phase:
1. Codex reads `SPEC.md`.
2. Implement only the requested phase.
3. Run all old + new tests.
4. Update documentation if implementation details were finalized.
5. Report exact test counts and known limitations.
6. Only then move to the next phase.

---

# Mandatory Codex Rules

Paste these rules into the repository-level `AGENTS.md` or equivalent:

```text
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
```

---

# Phase 0 — Repository Bootstrap

Implement only:

```text
Rust workspace
Axum server
Tokio runtime
config loader
tracing
graceful shutdown
embedded Preact/Vite frontend shell
health/version endpoints
basic CI
```

Endpoints:

```text
GET /healthz
GET /readyz
GET /api/v1/system/version
```

Build constants:

```text
APP_VERSION
FRONTEND_BUILD_ID
API_SCHEMA_VERSION
```

Frontend shows:

```text
server status
app version
client build
```

Tests:

```text
config parse
health endpoints
version endpoint
embedded frontend response
```

Do not add blockchain code yet.

### First Codex prompt

```text
Read SPEC.md and CODEX_DEVELOPMENT_GUIDE.md completely.

Implement Phase 0 only.

Requirements:
- Rust stable
- Cargo workspace
- Tokio + Axum
- Preact + TypeScript + Vite
- production frontend embedded into the Rust binary
- config.toml for non-secrets, environment variables for secrets
- /healthz, /readyz, /api/v1/system/version
- compile-time APP_VERSION, FRONTEND_BUILD_ID and API_SCHEMA_VERSION
- graceful SIGTERM/SIGINT shutdown
- structured tracing
- GitHub Actions CI
- tests
- cargo fmt --check
- cargo clippy --workspace -- -D warnings
- cargo test --workspace
- frontend typecheck and build

Do not implement any later phase.
Do not implement trading, wallet signing or private keys.

When finished report using the Mandatory Completion Report format from this guide.
```

---

# Phase 1 — PostgreSQL + Domain Core

Create migrations and typed domain primitives.

Tables initially:

```text
protocol_deployments
chain_cursors
chain_blocks
raw_chain_logs
normalized_events

tokens

traders
trader_wallets

event_outbox
alert_events

app_settings
users
audit_logs
```

Domain newtypes:

```text
ChainId
BlockNumber
TokenAddress
CurveAddress
WalletAddress
TxHash
LogIndex
RawAmount
```

Use:

```text
alloy primitives / U256
rust_decimal
```

Tests:

```text
empty migration
migration re-run
address normalization
invalid address rejection
unique constraints
raw amount normalization
```

---

# Phase 2 — Robinhood Chain Provider

Implement:

```text
HTTP RPC
WS RPC
eth_chainId == 4663
new block/log subscription primitives
receipt
getLogs
RPC health
cursor
```

Do not parse Pons yet.

Implement reconnect/backfill framework.

Tests with mock/fixture provider.

---

# Phase 3 — Pons V2 Deployment Registry

Implement:

```text
PonsV2Deployment
DeploymentRegistry
DeploymentVerifier
DeploymentHealth
```

Admin API:

```text
GET  /api/v1/admin/deployments
POST /api/v1/admin/deployments
PATCH /api/v1/admin/deployments/{id}
POST /api/v1/admin/deployments/{id}/verify
```

Seed the current documented deployment only as data.

Never rely on a scattered constant.

Test:

```text
empty bytecode rejected
wrong chain rejected
disabled deployment ignored
valid fixture accepted
```

---

# Phase 4 — TokenLaunched + Token Registry

Use verified Pons V2 ABI.

Implement:

```text
TokenLaunched decode
token
curve
deployer
quote/pair asset
launch block
launch tx
launch log
```

On launch:

```text
insert token idempotently
register curve
create token.launched durable event
```

WSS broadcast can remain minimal for now.

Tests:

```text
realistic fixture decode
duplicate launch
restart rebuild curve registry
```

---

# Phase 5 — On-Chain Token Metadata

Implement:

```text
token_metadata_original
token_metadata_current
token_metadata_snapshots
```

Read the current verified Pons V2 token metadata ABI.

Persist:

```text
name
symbol
logo
description
twitter
telegram
discord
website
farcaster
raw metadata
```

Original row never changes.

Current row updates.

Snapshot only when hash changes.

Tests:

```text
first metadata
same metadata no new snapshot
changed metadata new snapshot
original unchanged
```

---

# Phase 6 — CurveBuy / CurveSell Ingestion

Dynamic curve registry.

Decode:

```text
CurveBuy
CurveSell
CurveBuyRefunded
CurveCompleted
```

Store all curve trades, not only monitored-wallet trades.

Create:

```text
token_trades
```

Every event has deterministic unique key.

Tests:

```text
buy decode
sell decode
duplicate ingest
unknown curve rejected
```

---

# Phase 7 — Trader / Execution Wallet Registry

Implement:

```text
traders
trader_wallets
```

Roles:

```text
PROFILE_ADDRESS
ROBINHOOD_EXECUTION_ADDRESS
HISTORICAL_EXECUTION_ADDRESS
```

Only enabled execution addresses match Smart Money.

Admin:

```text
create trader
add wallet
disable
CSV import
```

CSV:

```text
handle,address,role,tier,notes
```

Build in-memory `HashMap<Address, TrackedWallet>`.

Tests:

```text
duplicate address handling
invalid address
disabled address
execution role matching
CSV partial failure report
```

---

# Phase 8 — Trade Confirmation Engine

This is one of the most important phases.

Create:

```text
trade_confirmation/
  buy.rs
  sell.rs
  transfers.rs
  evidence.rs
  confidence.rs
```

BUY rules from SPEC:

```text
CurveBuy from known curve
recipient is tracked execution address
same tx token movement consistent
```

Do not inspect relayer to decide identity.

SELL rules from SPEC.

Create `TradeEvidence[]`.

Outputs:

```text
BUY_CONFIRMED
BUY_STRONG
SELL_CONFIRMED
SELL_STRONG
TOKEN_RECEIVED
```

Fixture tests must include:

```text
tx.from = relayer
recipient = tracked wallet
→ BUY_CONFIRMED

tx.from = tracked wallet but recipient not tracked
→ not enough by itself

plain Transfer → tracked wallet
→ TOKEN_RECEIVED, not BUY
```

Do not proceed until these tests are very strong.

---

# Phase 9 — Positions + Entry Metrics

Implement:

```text
smart_trades
wallet_token_positions
position_events
```

State transitions:

```text
OPEN
ADD
REDUCE
CLOSE
```

Compute:

```text
launch_age
buyer_rank
smart_buyer_rank
entry market state
```

Tests for all transitions.

---

# Phase 10 — Market / Holder / Snapshots

Implement:

```text
token_transfers
token_wallet_balances
token_market_snapshots
```

Metrics:

```text
buyers
sellers
buy volume
sell volume
net flow
smart buyers
smart sellers
smart net flow
holders
price/MC
curve progress
```

Time snapshots:

```text
30s 1m 3m 5m 15m 30m 1h
```

Event snapshots on Smart Money milestones.

---

# Phase 11 — Consensus + Signal Engine

Implement:

```text
consensus_snapshots
signal_rules
signal_snapshots
signal_transitions
```

Weights are configuration/database values, not constants.

Output:

```text
score
confidence
component values
reason codes
```

States:

```text
NO_SIGNAL
WATCH
STRONG_WATCH
HIGH_PRIORITY
COOLING
DISTRIBUTION
CLOSED
```

Tests must prove exact transition behavior.

---

# Phase 12 — Application WSS + Reliable Event Replay

Implement:

```text
/ws
event_outbox
monotonic seq
heartbeat
reconnect support
GET /events?after_seq=
```

Important order:

```text
DB commit
then WSS push
```

Frontend:

```text
LIVE
RECONNECTING
OFFLINE
```

Tests:

```text
event persisted if no client
replay after reconnect
no duplicate semantic event
```

---

# Phase 13 — Alert UX: Toast + Sound + TTS + Notifications

Implement:

```text
Alert Center
toast
sound preferences
speechSynthesis
Notification API
```

First explicit user interaction enables audio.

Templates:

```text
Smart BUY
STRONG
HIGH_PRIORITY
Smart CLOSE
DISTRIBUTION
SYSTEM UPDATE
```

Do not let AI generate TTS text.

Tests:

```text
preference filtering
dedupe
mute
severity routing
```

---

# Phase 14 — Web Application

Build production pages:

```text
Dashboard
Token Detail
Timeline
Smart Money
Market
Research
Trader
Alert Center
System
Admin
```

Use WSS for live event deltas and REST for initial/recovery state.

Do not make WSS the only state store.

---

# Phase 15 — External Research Evidence

Implement provider traits and DB:

```text
token_external_profiles
token_social_snapshots
token_website_snapshots
token_research_evidence
```

No unauthorized scraping requirement.

Provider failure must not affect chain indexer.

---

# Phase 16 — GitHub Release Updater

Implement:

```text
release check
signed manifest
Ed25519 verification
SHA256
architecture selection
staging
schema compatibility
updater helper
atomic replacement
systemd restart
readyz
rollback
release_history
update_jobs
```

Frontend Updates page.

Do not enable auto-install by default.

---

# Phase 17 — Frontend Upgrade Refresh Protocol

This phase is mandatory before considering the updater complete.

Backend and frontend have build IDs.

Implement:

```text
WSS system.hello
server_version
frontend_build_id
api_schema_version
```

If browser build != server build:

```text
CLIENT_REFRESH_REQUIRED
```

UI:

```text
persistent modal
update sound
optional Chinese TTS
Refresh Now button
```

Read-only pages:

```text
optional countdown auto-refresh
```

Unsaved admin forms:

```text
block auto-refresh
```

HTTP:

```text
index/app shell no-store
hashed assets immutable
```

No Service Worker MVP.

Tests:

```text
old build detects new server
refresh modal appears
read-only auto refresh logic
unsaved admin blocks forced refresh
update-applied alert recovered after restart
```

---

# Phase 18 — Historical Trader Statistics

Now use collected data.

Implement:

```text
trader_stats
trader_score_history
future token snapshots
MFE/MAE
sample-size confidence
```

Do not optimize score weights before enough history exists.

---

# Phase 19 — AI Research

Only after deterministic data is stable.

Implement provider trait.

AI input:

```text
structured research package
```

Output JSON schema.

AI failure is non-fatal.

AI has no transaction capability.

---

# Phase 20 — Optional Pons V2 Post-Graduation V4 Adapter

Only if needed.

Implement as separate Pons V2 lifecycle market adapter.

Do not alter curve-stage confirmation semantics.

---

# Mandatory Completion Report

At the end of every Codex phase, require exactly:

```text
## Phase
<phase number/name>

## Changed Files
...

## Database Migrations
...

## New APIs / Events
...

## Important Implementation Decisions
...

## Tests Added
...

## Test Results
- cargo test ...
- frontend ...
- exact pass/fail/ignored counts

## Security / Data Integrity
...

## Known Limitations
...

## SPEC / Docs Changes
...

## Next Phase Prerequisites
...
```

Never accept only:

```text
“done”
```

---

# How You Should Work With Codex

Recommended workflow:

```text
1. Put SPEC.md + CODEX_DEVELOPMENT_GUIDE.md + config.example.toml in repo root.
2. Give Codex only Phase 0.
3. Send its completion report back for review.
4. Fix issues.
5. Commit/tag checkpoint.
6. Move to Phase 1.
7. Repeat.
```

For risky/core phases:

```text
Phase 4
Phase 6
Phase 8
Phase 9
Phase 11
Phase 12
Phase 16
Phase 17
```

do not move on until fixtures/integration tests are convincing.

The most important correctness checkpoint is Phase 8:
`CurveBuy/CurveSell → monitored execution address → chain-confirmed trade`.
