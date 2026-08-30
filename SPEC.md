# Pons V2 + Fomo Smart Wallet Radar
## Final System Specification — Rust Backend + Web Frontend + PostgreSQL + GitHub Release Updater

**Spec version:** 2.0-final  
**Date:** 2026-08-28  
**System role:** Read-only research, monitoring, alerting, historical analysis  
**Trading execution:** Explicitly out of scope

---

# 0. Executive Summary

Build a production-grade monitoring application focused only on **Pons V2 tokens on Robinhood Chain**.

The system must:

1. Detect every new Pons V2 launch from the Pons V2 Factory.
2. Immediately persist the token's identity, curve, creator, launch transaction and on-chain metadata.
3. Maintain a manually/operationally curated registry of Fomo traders and their verified Robinhood execution addresses.
4. Listen to Pons V2 `CurveBuy` and `CurveSell` events.
5. Confirm that a monitored address really bought/sold the token on-chain.
6. Maintain positions for each monitored wallet × token.
7. Compute early Smart Money consensus and Pons market momentum.
8. Store all relevant token metadata, descriptions, social links and research evidence for later analysis.
9. Persist historical snapshots so later research/backtesting uses information that existed at the time.
10. Provide a Rust/Axum backend and embedded Web frontend.
11. Push real-time UI events through the application's own WebSocket/WSS endpoint.
12. Support toast notifications, sounds, browser notifications and Chinese TTS voice alerts.
13. Persist alerts so a closed/disconnected browser does not lose important events.
14. Upgrade through signed GitHub Releases with rollback.
15. After an application upgrade, reliably tell every open frontend that it must refresh, and optionally refresh automatically when safe.

The core rule is:

```text
Pons V2 is the source of trade truth.
A monitored wallet is counted as a BUY only when chain evidence confirms that wallet
as the recipient of a real Pons V2 CurveBuy and the token transfer is consistent.

A monitored wallet is counted as a SELL only when chain evidence confirms that wallet
as the seller/source of a real Pons V2 CurveSell and its token position decreases.
```

Relayers, Fomo APIs and Fomo WebSocket messages are not required to decide whether a wallet bought.

---

# 1. Product Definition

Product working name:

```text
pons-radar
```

The application is a **Pons V2 Smart Money Research Radar**.

It answers:

```text
What Pons V2 token just launched?
What did the creator publish about it?
Which monitored Fomo execution wallets bought it?
How early did they buy?
How much did they buy?
How many independent monitored wallets are buying?
Are those wallets historically selective/useful on Pons?
Are they adding, reducing or closing?
What is happening to the curve and general market activity?
What was known about the project at each point in time?
Why did the system raise or lower the signal?
```

It does **not** answer or perform:

```text
automatic buy
automatic sell
private-key custody
transaction signing
trade mirroring
copy trading
```

No trading private key should ever be requested by this application.

---

# 2. Network Baseline

Network:

```text
Robinhood Chain Mainnet
Chain ID: 4663
Native gas asset: ETH
EVM compatible: yes
```

Use an RPC provider with both HTTP and WebSocket support.

Recommended production architecture:

```text
HTTP RPC:
historical queries
getLogs
receipts
eth_call
backfill

WebSocket RPC:
new block / log subscriptions
real-time event ingestion
```

Public RPC may be configured as fallback, but should not be the only production dependency.

On startup:

```text
eth_chainId
```

must equal:

```text
4663
```

Otherwise fail closed.

---

# 3. Pons V2 Scope

Only Pons V2 is supported.

Do not implement Pons V1 adapters.

Current design baseline:

```text
Pons V2 Factory
    ↓
TokenLaunched
    ↓
Token + dedicated Bonding Curve
    ↓
CurveBuy / CurveSell
    ↓
CurveCompleted
    ↓
LaunchSwept
    ↓
PoolGraduated
    ↓
Uniswap V4
```

## 3.1 MVP boundary

MVP Smart Money classification is required during the **Pons V2 bonding-curve stage**.

Required:

```text
TokenLaunched
CurveBuy
CurveSell
CurveBuyRefunded
CurveCompleted
LaunchSwept
PoolGraduated
```

The system must record graduation state.

Post-graduation Uniswap V4 trade monitoring is a later phase and must be implemented behind a separate adapter without changing curve-stage code.

---

# 4. Current Pons V2 Deployment Policy

The current public Pons V2 documentation lists a Robinhood Chain Factory address.

However, deployments can change.

Therefore:

**Never scatter a Pons Factory address as a constant throughout business logic.**

Use a `protocol_deployments` registry.

Each deployment record contains:

```text
id
protocol
generation
chain_id
address
start_block
end_block nullable
enabled
expected_event_topics
expected_code_hash nullable
source
last_verified_at
health
```

Startup verifies:

```text
chain_id == 4663
eth_getCode(address) != empty
configured event ABI/fingerprint is valid
optional bytecode fingerprint
```

If verification fails:

```text
deployment = DEGRADED
do not silently index it
raise system alert
```

The shipped config may include the currently documented Factory as a seed, but runtime always reads it from the registry.

---

# 5. Core Data Flow

```text
                    Robinhood Chain
                           │
                    RPC HTTP + WSS
                           │
                           ▼
                   Chain Ingestion
                           │
                           ▼
                  Pons V2 Adapter
                           │
        ┌──────────────────┼───────────────────┐
        │                  │                   │
 TokenLaunched         CurveBuy            CurveSell
        │                  │                   │
        ▼                  └─────────┬─────────┘
 Token Registry                      ▼
        │                    Trade Confirmation
        │                            │
        │                     Watched address?
        │                            │
        │                       YES / NO
        │                            │
        │                            ▼
        │                      Smart Trade
        │                            │
        │                            ▼
        │                      Position Engine
        │                            │
        └──────────────┬─────────────┘
                       ▼
                 Market Snapshots
                       │
                       ▼
                 Consensus Engine
                       │
                       ▼
                   Signal Engine
                       │
             ┌─────────┴─────────┐
             │                   │
             ▼                   ▼
        PostgreSQL           Alert Engine
                                 │
                                 ▼
                          Application WSS
                                 │
                                 ▼
                              Browser
                    ┌────────────┼────────────┐
                    ▼            ▼            ▼
                  Live UI       Sound        TTS
```

---

# 6. Smart Wallet Identity Model

Do not equate a Fomo handle with a single permanent address.

Model:

```text
Trader
  └── TraderWallet[]
```

Example:

```text
Alice
├── PROFILE_ADDRESS
├── ROBINHOOD_EXECUTION_ADDRESS
└── HISTORICAL_EXECUTION_ADDRESS
```

Only addresses with role:

```text
ROBINHOOD_EXECUTION_ADDRESS
```

and:

```text
enabled = true
identity_confidence >= configured minimum
```

participate in Smart Money classification.

## 6.1 Wallet data sources

MVP supported sources:

```text
MANUAL
CSV_IMPORT
OPERATOR_VERIFIED
```

Optional later:

```text
AUTHORIZED_PROVIDER
```

Do not make the system dependent on scraping Fomo pages or reverse-engineered login sessions.

## 6.2 Address registry

Maintain an in-memory:

```rust
HashMap<Address, TrackedWallet>
```

for O(1) matching.

Database remains the durable source of truth.

---

# 7. Chain-Confirmed BUY Logic

This is a critical invariant.

Pons V2 exposes a curve buy event conceptually equivalent to:

```text
CurveBuy(
  buyer,
  recipient,
  quoteIn,
  tokensOut,
  fee,
  tax
)
```

A monitored address is classified as a confirmed BUY only when evidence satisfies the configured confirmation level.

## 7.1 BUY_CONFIRMED

Required evidence:

```text
A. Event is emitted by the known curve of a known Pons V2 token.
B. Event type is CurveBuy.
C. CurveBuy.recipient == monitored execution address.
D. The same transaction receipt contains the expected token movement
   to the monitored address, or balance-effect evidence equivalent to it.
E. Amount is consistent with tokensOut within exact protocol semantics.
F. Log is not orphaned by a reorg.
```

Result:

```text
BUY_CONFIRMED
confidence = 1.0
```

This event participates fully in Smart Money scoring.

## 7.2 BUY_STRONG

Fallback only if event data is authoritative but one secondary evidence source is unavailable.

Example:

```text
CurveBuy.recipient == tracked address
but Transfer extraction temporarily unavailable
```

Result:

```text
BUY_STRONG
confidence < 1.0
```

Whether this participates in scoring is configurable.

Default:

```text
include BUY_CONFIRMED
exclude BUY_STRONG from HIGH_PRIORITY threshold
```

until replay confirms it.

## 7.3 TOKEN_RECEIVED

A plain token transfer to a tracked wallet without matching Pons V2 BUY evidence is:

```text
TOKEN_RECEIVED
```

It must not be counted as a Smart Money BUY.

Possible reasons include transfers, distributions or other non-curve activity.

---

# 8. Chain-Confirmed SELL Logic

Pons V2 exposes a curve sell event conceptually equivalent to:

```text
CurveSell(
  seller,
  recipient,
  tokensIn,
  quoteOut,
  fee,
  tax
)
```

A monitored address is classified as SELL when:

```text
A. Event is CurveSell from the token's known curve.
B. seller == monitored execution address
   OR transaction token-flow evidence proves the monitored wallet supplied tokens.
C. Monitored wallet token position decreases consistently with tokensIn.
D. Event survives confirmation/reorg handling.
```

Result types:

```text
SELL_CONFIRMED
SELL_STRONG
```

The `recipient` of quote proceeds does not need to equal the monitored wallet.

---

# 9. Position State Machine

For each:

```text
tracked_wallet × token
```

maintain a position.

States/events:

```text
NO_POSITION
    │
    └── confirmed buy
            ↓
      OPEN_POSITION
            │
            ├── buy  → ADD_POSITION
            ├── sell → REDUCE_POSITION
            └── sell all → CLOSE_POSITION
```

Persist both:

```text
position current state
position event history
```

Position fields:

```text
wallet
token
balance_raw
first_entry_at
last_trade_at
first_entry_market_cap
first_entry_curve_progress
total_quote_in
total_quote_out
open
```

Do not use floating point for balances or money.

Use:

```text
U256 / signed integer for raw amounts
Decimal / NUMERIC for normalized values
```

---

# 10. Entry Metrics

Every confirmed monitored-wallet buy should capture the state at that exact time.

Required:

```text
launch_age_ms
launch_age_blocks
buyer_rank
smart_buyer_rank
quote_in
tokens_out
entry_price
entry_market_cap
entry_curve_progress
general_unique_buyers
general_unique_sellers
smart_buyers_so_far
holder_count_if_available
```

## 10.1 Buyer rank

`buyer_rank`:

```text
rank of the wallet's first confirmed curve buy among all unique buyers
```

`smart_buyer_rank`:

```text
rank among monitored execution wallets
```

Do not count protocol contracts as buyers.

---

# 11. Pons V2 Market State

Track the curve-stage market continuously.

Per token:

```text
unique_buyers
unique_sellers
buy_count
sell_count

buy_quote_volume
sell_quote_volume
net_quote_flow

smart_unique_buyers
smart_unique_sellers
smart_buy_volume
smart_sell_volume
smart_net_flow

price
estimated_market_cap
curve_progress
holder_count
```

Snapshots:

```text
T+30s
T+1m
T+3m
T+5m
T+15m
T+30m
T+1h
```

Also support event-triggered snapshots on:

```text
first smart buy
second smart buy
third smart buy
signal transition
curve completion
graduation
```

All snapshots must be historical and immutable.

---

# 12. Holder Index

Do not scan the whole chain on every update.

Maintain balances incrementally using token transfer events:

```text
token_wallet_balances
```

Transitions:

```text
0 → positive
holder_count +1

positive → 0
holder_count -1
```

Exclude:

```text
zero address
curve contract
known protocol infrastructure
```

Store both raw holder count and classified holder count if classification is available.

---

# 13. Token Research Profile

Every launch creates a permanent research record.

The system must preserve:

```text
what the token claimed at launch
what the current metadata says
what changed later
what external evidence was observed
what chain state existed at the time
```

## 13.1 Core token identity

`tokens`:

```text
id
chain_id
address
curve_address
factory_address
deployer
pair_token

name
symbol
decimals
total_supply_raw

launch_tx
launch_block
launch_log_index
launch_time

lifecycle
created_at
updated_at
```

---

# 14. On-Chain Metadata

Pons launches include project metadata such as:

```text
name
symbol
logo/image
description
social links
```

Social links can include:

```text
twitter/X
telegram
discord
website
farcaster
```

The exact ABI must be generated from the verified current Pons V2 source/ABI used by the deployment.

At launch:

```text
read metadata
persist original
persist current
create snapshot
```

## 14.1 Original metadata is immutable

`token_metadata_original`:

```text
token_id
logo_uri
description
twitter
telegram
discord
website
farcaster
raw_metadata_json
source_block
observed_at
```

Never update this row.

## 14.2 Current metadata

`token_metadata_current`:

```text
token_id
...
last_checked_block
last_checked_at
```

May be updated.

## 14.3 Metadata history

`token_metadata_snapshots`:

```text
id
token_id
logo_uri
description
twitter
telegram
discord
website
farcaster
content_hash
source_block
observed_at
change_reason
```

Create a new snapshot only when:

```text
content_hash changes
```

---

# 15. External Research Evidence

External research is optional and separate from chain truth.

Tables:

```text
token_external_profiles
token_social_snapshots
token_website_snapshots
token_research_evidence
```

## 15.1 External profile

```text
token_id
platform
identifier
url
status
first_seen_at
last_checked_at
source
confidence
```

Platforms:

```text
X
WEBSITE
TELEGRAM
DISCORD
FARCASTER
GITHUB
OTHER
```

## 15.2 Social snapshots

When an authorized/public provider supports it:

```text
followers
following
posts
account_created_at
verified
observed_at
raw_json
```

Do not make unauthorized scraping a required system capability.

## 15.3 Website snapshot

Store research metadata, not unlimited webpage archives:

```text
url
reachable
http_status
final_url
title
meta_description
content_hash
observed_at
```

Optionally:

```text
linked_x
linked_github
linked_telegram
docs_url
```

## 15.4 Research evidence

Generic table:

```text
id
token_id
evidence_type
source
source_reference
structured_data JSONB
content_hash
confidence
observed_at
```

This is the canonical input pool for later AI research.

---

# 16. Trader Analytics

Manual tiers can exist, but the long-term system must learn a Pons-specific historical score.

Store:

```text
sample_size
early_entry_count
median_launch_age
median_buyer_rank
median_initial_position
selectivity
median_holding_time

future outcome metrics
MFE_5M
MFE_15M
MFE_1H
MFE_6H
MFE_24H
MAE equivalents
```

Score fields:

```text
pons_score
pons_score_confidence
score_version
calculated_at
```

If sample size is too small:

```text
confidence must be low
```

Do not pretend a new trader has a statistically strong score.

---

# 17. Smart Consensus Engine

Evaluate rolling windows:

```text
30s
1m
3m
5m
15m
```

Metrics:

```text
raw_smart_buyers
qualified_smart_buyers
independent_smart_buyers

weighted_smart_buy_score
smart_buy_volume
smart_sell_volume
smart_net_flow

first_smart_buy_age
median_smart_buy_age
smart_exit_ratio
```

Future copy/correlation detection can assign:

```text
independence_weight
cluster_id
```

MVP schema should reserve those fields even if clustering is initially disabled.

---

# 18. Opportunity Signal

The signal is a **research priority**, not a price prediction.

Score:

```text
0..100
```

Default component weights:

```text
Smart Wallet / Consensus   45
Pons Momentum              25
Capital Flow               15
Holder / Distribution      10
Research / Narrative        5
```

All weights must be configurable.

Signal states:

```text
NO_SIGNAL
WATCH
STRONG_WATCH
HIGH_PRIORITY
COOLING
DISTRIBUTION
CLOSED
```

Every transition must persist:

```text
from_state
to_state
score
confidence
reason_codes
snapshot_id
created_at
```

The frontend must be able to explain why the state changed.

---

# 19. Hard Rule Engine

Do not rely only on a weighted score.

Rules are versioned and configurable.

Example research rule:

```text
independent_smart_buyers >= configured threshold
AND qualified_smart_buyers >= configured threshold
AND smart_net_flow >= threshold
AND launch_age <= window
AND data_integrity != DEGRADED
```

Possible result:

```text
HIGH_PRIORITY
```

Exit rule examples:

```text
smart_exit_ratio crosses threshold
high-quality tracked wallet closes
smart_net_flow becomes sharply negative
```

Possible result:

```text
COOLING
DISTRIBUTION
```

Every rule evaluation returns:

```text
rule_id
rule_version
matched
values
thresholds
reason
```

---

# 20. Backend WebSocket / WSS

The application must expose its own real-time WebSocket endpoint:

```text
/ws
```

Production through TLS:

```text
wss://domain.example/ws
```

This is separate from:

```text
Robinhood RPC WSS
```

The system does not require:

```text
Fomo WSS
```

## 20.1 Real-time event types

Version all messages.

Examples:

```text
token.launched
token.metadata_changed

smart_trade.buy_confirmed
smart_trade.sell_confirmed

position.opened
position.added
position.reduced
position.closed

signal.watch
signal.strong_watch
signal.high_priority
signal.cooling
signal.distribution

system.rpc_degraded
system.rpc_recovered
system.indexer_lag

system.update_available
system.update_installing
system.update_applied
system.client_refresh_required
```

Envelope:

```json
{
  "seq": 12345,
  "type": "smart_trade.buy_confirmed",
  "schema_version": 1,
  "server_version": "1.3.0",
  "timestamp": "2026-08-28T08:00:00Z",
  "data": {}
}
```

---

# 21. WebSocket Reliability

PostgreSQL is the durable truth.

WSS is only delivery.

Correct flow:

```text
event
→ DB transaction
→ durable event/outbox row
→ WSS broadcast
```

If browser is offline, data still exists.

## 21.1 Sequence IDs

Every deliverable event receives monotonic:

```text
seq
```

Frontend stores:

```text
last_seen_seq
```

After reconnect:

```text
GET /api/v1/events?after_seq=...
```

or equivalent server replay.

No important event should be lost because a browser slept.

## 21.2 Heartbeat

Server:

```text
heartbeat every ~15s
```

Frontend states:

```text
LIVE
RECONNECTING
OFFLINE
```

## 21.3 Reconnect

Exponential backoff:

```text
1s
2s
4s
8s
15s
30s max
```

After reconnect:

```text
sync missed events
then mark LIVE
```

---

# 22. Alert System

Every alert is first stored in PostgreSQL.

`alert_events`:

```text
id
seq
token_id nullable
alert_type
severity
title
message
speech_text
payload JSONB
created_at
acknowledged_at nullable
dedupe_key
```

Severity:

```text
INFO
WATCH
STRONG
HIGH
CRITICAL_SYSTEM
```

---

# 23. Browser Alert Features

The Web UI must support:

```text
toast notification
alert center/history
sound effects
Chinese TTS
browser desktop notifications
```

## 23.1 Default alert behavior

Suggested defaults:

```text
Token launch:
visual only

First tracked wallet BUY:
toast + optional short sound

STRONG_WATCH:
toast + strong sound + optional TTS

HIGH_PRIORITY:
toast + high-priority sound + TTS

Tracked wallet CLOSE:
toast + sound + TTS

DISTRIBUTION:
toast + urgent sound + TTS

System degradation:
toast + system sound

Upgrade completed / refresh required:
persistent modal + update sound + optional TTS
```

## 23.2 TTS

Browser-side first implementation:

```text
window.speechSynthesis
```

No server TTS dependency required.

Use templates, not free-form AI speech.

Example:

```text
“高优先级提醒，ABC，三个重点钱包已经买入。”
“风险提醒，ABC，重点钱包正在集中退出。”
“系统升级已经完成，请刷新页面。”
```

## 23.3 Browser autoplay restrictions

Settings screen must include:

```text
Enable Alerts
Enable Sound
Enable Voice
Enable Desktop Notifications
```

User interaction initializes audio/TTS permission.

Frontend must clearly show whether alerts are actually enabled.

---

# 24. Alert Preferences

Persist per user/browser preference as appropriate:

```text
sound_enabled
voice_enabled
desktop_notifications_enabled

speak_strong
speak_high_priority
speak_wallet_close
speak_distribution
speak_system_update

minimum_smart_trade_amount
minimum_signal_score
```

A global mute button should be visible in the main navigation.

---

# 25. Frontend Pages

## 25.1 Dashboard

Sections:

```text
Live Pons V2 Launches
High Priority
Strong Watch
Recent Smart Money
Live Feed
System Health
```

Token row:

```text
symbol
age
curve progress
market cap
buyers
sellers
net flow
smart buyers
independent smart buyers
smart net flow
signal score
signal trend
```

## 25.2 Token Detail

Tabs:

```text
Overview
Timeline
Smart Money
Market
Holders
Research
Metadata
AI Research
Raw Evidence
```

## 25.3 Timeline

Example:

```text
00:00 Token launched
00:21 Alice BUY confirmed
00:44 Bob BUY confirmed
00:45 signal → WATCH
01:12 Charlie BUY confirmed
01:13 signal → HIGH_PRIORITY
08:31 Alice reduced 30%
12:05 Bob closed
12:06 signal → COOLING
```

## 25.4 Trader

```text
handle
execution wallets
manual tier
Pons historical score
score confidence
early entry stats
buyer-rank stats
position-size stats
MFE/MAE outcome history
recent activity
```

## 25.5 Research

```text
original description
current description
logo
X
website
Telegram
Discord
Farcaster

metadata change history
social snapshots
website snapshots
research evidence
AI report history
```

## 25.6 Admin

```text
Trader Registry
Wallet Registry
Deployment Registry
Signal Settings
Alert Settings
Research Providers
System Health
Updates
Audit Log
```

---

# 26. Frontend Upgrade Refresh Protocol

This is mandatory.

Because the frontend is embedded in the Rust binary, an upgraded backend may be running new JS/CSS assets while an already-open browser tab still runs the old frontend.

The system must detect this and explicitly refresh the client.

## 26.1 Build identifiers

Compile both backend and frontend with:

```text
app_version
frontend_build_id
api_schema_version
```

Frontend contains at build time:

```text
CLIENT_BUILD_ID
CLIENT_APP_VERSION
```

Backend exposes:

```text
GET /api/v1/system/version
```

Response:

```json
{
  "app_version": "1.4.0",
  "frontend_build_id": "gitsha-or-content-hash",
  "api_schema_version": 3,
  "started_at": "..."
}
```

## 26.2 WebSocket hello

Immediately after WSS connection:

```json
{
  "type": "system.hello",
  "server_version": "1.4.0",
  "frontend_build_id": "...",
  "api_schema_version": 3
}
```

Frontend compares it with its embedded build ID.

If mismatch:

```text
CLIENT_REFRESH_REQUIRED
```

## 26.3 Upgrade restart behavior

During upgrade:

```text
old backend running
↓
updater validates release
↓
system.update_installing persisted
↓
old backend exits
↓
binary replaced
↓
new backend starts
↓
readyz passes
↓
release_history marked SUCCESS
↓
system.update_applied durable alert created
↓
old browser reconnects
↓
system.hello shows new build ID
↓
frontend displays refresh-required modal
```

## 26.4 Refresh modal

Persistent modal:

```text
系统升级已经完成
当前页面仍在运行旧版本前端。

新版本：1.4.0

[立即刷新]
```

Optional:

```text
“10 秒后自动刷新”
```

Voice, if enabled:

```text
“系统升级已经完成，请刷新页面。”
```

## 26.5 Safe auto-refresh

Dashboard/read-only pages:

```text
auto refresh allowed after configurable countdown
```

Admin pages with unsaved changes:

```text
do not auto-refresh
show blocking prompt
user must choose refresh
```

Track:

```text
has_unsaved_changes
```

## 26.6 Hard reload / caching

Recommended HTTP caching:

```text
index.html / application shell:
Cache-Control: no-store

hashed JS/CSS assets:
Cache-Control: public, max-age=31536000, immutable
```

Do not introduce a Service Worker in MVP.

On refresh:

```text
load new index
→ new hashed frontend assets
```

If necessary, append a version query to the top-level navigation.

---

# 27. GitHub Release Upgrade System

The application must support upgrade checks from GitHub Releases.

Default:

```text
auto_check = true
auto_install = false
```

Installation requires an administrator action unless explicitly changed later.

## 27.1 Release assets

Example:

```text
pons-radar-linux-x86_64.tar.gz
pons-radar-linux-aarch64.tar.gz
release-manifest.json
release-manifest.sig
SHA256SUMS
```

Package contains:

```text
pons-radar
pons-radar-updater
VERSION
```

Frontend is embedded in `pons-radar`.

## 27.2 Manifest

```json
{
  "version": "1.4.0",
  "channel": "stable",
  "published_at": "...",
  "min_schema": 14,
  "max_schema": 14,
  "api_schema_version": 3,
  "frontend_build_id": "...",
  "assets": {
    "linux-x86_64": {
      "name": "...",
      "sha256": "..."
    }
  }
}
```

## 27.3 Verification

Updater contains only the release signing **public key**.

Verify:

```text
manifest Ed25519 signature
asset SHA256
platform/architecture
schema compatibility
binary --version sanity check
```

Never ship the signing private key.

## 27.4 Upgrade transaction

```text
check release
↓
download to staging
↓
verify signature
↓
verify SHA256
↓
extract
↓
validate
↓
record update job
↓
backup old binary
↓
handoff to updater helper
↓
graceful main-process exit
↓
atomic replace
↓
restart systemd service
↓
run migrations/startup
↓
healthz
↓
readyz
↓
mark SUCCESS
```

Failure:

```text
rollback old binary
restart
record failure
raise system alert
```

## 27.5 Database migrations

Use SQLx migrations.

Rules:

```text
forward-only
transactional where possible
migration compatibility declared in manifest
never silently downgrade schema
```

---

# 28. Rust Technology Stack

Backend:

```text
Rust stable
Tokio
Axum
Alloy
SQLx
PostgreSQL
serde
serde_json
rust_decimal
reqwest
tracing
tower-http
utoipa
argon2
rust-embed
ed25519-dalek
sha2
```

Frontend:

```text
Preact
TypeScript
Vite
TanStack Query
lightweight-charts
```

MVP uses one Rust process for:

```text
API
WSS
chain indexer
analytics
embedded frontend
```

Workers are logical Tokio tasks.

---

# 29. Suggested Workspace

```text
pons-radar/
├── Cargo.toml
├── Cargo.lock
├── rust-toolchain.toml
├── README.md
├── SPEC.md
├── CODEX_DEVELOPMENT_GUIDE.md
├── config.example.toml
│
├── crates/
│   ├── app/
│   ├── domain/
│   ├── chain/
│   ├── pons_v2/
│   ├── trade_confirmation/
│   ├── wallet_intel/
│   ├── market/
│   ├── analytics/
│   ├── research/
│   ├── alerts/
│   ├── storage/
│   ├── api/
│   └── updater/
│
├── updater-helper/
├── frontend/
├── migrations/
├── tests/
├── fixtures/
├── scripts/
└── .github/workflows/
```

---

# 30. Module Responsibilities

## `chain`

```text
HTTP/WS providers
chain ID verification
block/log subscription
receipt retrieval
getLogs backfill
cursor
reorg
RPC health
```

## `pons_v2`

```text
deployment registry
verified ABI
TokenLaunched decode
CurveBuy/Sell decode
curve registry
lifecycle/graduation events
on-chain metadata reads
```

## `trade_confirmation`

```text
receipt log extraction
Transfer matching
BUY confirmation
SELL confirmation
evidence collection
confidence
```

## `wallet_intel`

```text
traders
wallet identities
execution-address registry
O(1) matcher
address roles
```

## `market`

```text
all Pons trades
buyers/sellers
volumes
holders
curve state
snapshots
```

## `analytics`

```text
entry rank
position
trader stats
consensus
signal
historical outcomes
```

## `research`

```text
metadata
metadata history
external profiles
website/social evidence
AI report input/output
```

## `alerts`

```text
durable alert creation
dedupe
WSS outbox
sound/TTS metadata
alert preferences
```

## `updater`

```text
GitHub release check
manifest verification
download
stage
install orchestration
rollback
release history
```

---

# 31. PostgreSQL Schema

Minimum tables:

```text
schema_migrations

protocol_deployments
chain_cursors
chain_blocks
raw_chain_logs
normalized_events

tokens
token_lifecycle_events
token_metadata_original
token_metadata_current
token_metadata_snapshots

token_trades
token_transfers
token_wallet_balances
token_market_snapshots

traders
trader_wallets
wallet_labels

smart_trades
wallet_token_positions
position_events

trader_stats
trader_score_history
consensus_snapshots

signal_rules
signal_snapshots
signal_transitions

token_external_profiles
token_social_snapshots
token_website_snapshots
token_research_evidence
ai_research_reports

event_outbox
alert_events
alert_preferences

users
app_settings
audit_logs

update_jobs
release_history
```

---

# 32. Important Database Constraints

Raw chain log uniqueness:

```text
(chain_id, tx_hash, log_index)
```

Token:

```text
(chain_id, address)
```

Curve:

```text
(chain_id, curve_address)
```

Trade:

```text
(chain_id, tx_hash, log_index, event_type)
```

Trader wallet:

```text
(chain_id, address, valid_from)
```

Current position:

```text
(wallet, token)
```

Outbox:

```text
seq unique
```

Derived data must be rebuildable from durable source events.

---

# 33. Chain Cursor and Backfill

Persist:

```text
stream
last_processed_block
last_processed_hash
updated_at
```

WSS disconnect:

```text
detect gap
→ HTTP getLogs from cursor+1
→ replay
→ resume live
```

Ingestion semantics:

```text
at least once
```

Database idempotency turns it into effectively-once derived state.

---

# 34. Reorg Handling

Store block hashes.

Live events may be marked:

```text
PENDING
```

After configured confirmations:

```text
CONFIRMED
```

On block-hash mismatch:

```text
mark affected events ORPHANED
rollback/rebuild derived state from stable boundary
replay
```

A Smart Money voice alert may optionally wait for one confirmation, while UI can display pending events immediately.

Make this configurable.

---

# 35. API

Base:

```text
/api/v1
```

Health:

```text
GET /healthz
GET /readyz
```

System:

```text
GET /system/version
GET /system/health
GET /system/indexer
```

Tokens:

```text
GET /tokens
GET /tokens/{address}
GET /tokens/{address}/timeline
GET /tokens/{address}/trades
GET /tokens/{address}/smart-money
GET /tokens/{address}/market-snapshots
GET /tokens/{address}/research
GET /tokens/{address}/metadata-history
```

Signals:

```text
GET /signals
GET /signals/{token}
```

Traders:

```text
GET /traders
GET /traders/{id}
GET /traders/{id}/wallets
GET /traders/{id}/trades
GET /traders/{id}/stats
```

Alerts:

```text
GET /alerts
POST /alerts/{id}/ack
GET /events?after_seq=
```

Admin:

```text
POST  /admin/traders
PATCH /admin/traders/{id}

POST  /admin/trader-wallets
PATCH /admin/trader-wallets/{id}
POST  /admin/trader-wallets/import

GET   /admin/deployments
POST  /admin/deployments
PATCH /admin/deployments/{id}
POST  /admin/deployments/{id}/verify

GET   /admin/settings
PATCH /admin/settings

GET   /admin/updates
POST  /admin/updates/check
POST  /admin/updates/install
```

---

# 36. Authentication

MVP:

```text
single/admin multi-user local accounts
```

Password hashing:

```text
Argon2id
```

Session cookie:

```text
HttpOnly
Secure in production
SameSite
```

No default password.

First-run setup requires creating admin credentials.

All config/admin/update mutations create audit-log entries.

---

# 37. Configuration

Non-secret config:

```text
config.toml
```

Secrets through environment:

```text
DATABASE_URL
RH_RPC_HTTP_URL
RH_RPC_WS_URL
AI_API_KEY
GITHUB_TOKEN (only if private repository)
```

No wallet private key exists in application config.

---

# 38. Observability

Structured tracing fields:

```text
chain_id
block_number
tx_hash
log_index
token
curve
wallet
trader_id
event_id
deployment_id
```

Metrics:

```text
rpc_requests_total
rpc_errors_total
ws_reconnect_total
indexer_lag_blocks
events_ingested_total
pons_launches_total
curve_buys_total
curve_sells_total
confirmed_smart_buys_total
confirmed_smart_sells_total
signal_transitions_total
alerts_total
processing_latency_ms
db_query_latency_ms
```

System page should expose the important operational health without requiring shell access.

---

# 39. Data Integrity

Reconciliation jobs:

```text
sample indexed wallet token balance
vs eth_call balanceOf

indexed launch count
vs factory log range

curve registry
vs TokenLaunched history

smart position
vs indexed transfer balance
```

If inconsistent:

```text
DATA_INTEGRITY_DEGRADED
```

Signals must carry:

```text
confidence
```

and must not claim full confidence when the indexer is behind or incomplete.

---

# 40. AI Research

AI is optional and comes after the deterministic system works.

AI input is a structured research package:

```json
{
  "token": {},
  "original_metadata": {},
  "current_metadata": {},
  "metadata_changes": [],
  "market": {},
  "smart_money": {},
  "trader_context": {},
  "external_evidence": []
}
```

Output must validate against a JSON schema:

```text
category
summary
narrative_strength
originality
positive_evidence[]
risks[]
open_questions[]
confidence
```

AI cannot:

```text
modify chain facts
modify wallet identities
create confirmed trades
execute transactions
```

Store every AI report with:

```text
model/provider
prompt_schema_version
evidence_hash
report_version
created_at
```

Never overwrite old AI reports.

---

# 41. GitHub Actions CI/CD

PR CI:

```text
cargo fmt --check
cargo clippy --workspace -- -D warnings
cargo test --workspace

frontend npm ci
frontend typecheck
frontend test
frontend build
```

Release workflow:

```text
build frontend
embed frontend
build Rust release targets
integration tests
package assets
generate SHA256
generate release manifest
sign manifest
publish GitHub Release
```

Signing private key exists only in GitHub Actions secret storage.

---

# 42. Testing Strategy

## Unit tests

Must cover:

```text
TokenLaunched decode
CurveBuy decode
CurveSell decode

tracked recipient BUY confirmation
non-tracked recipient rejection
token transfer without CurveBuy rejection
amount mismatch behavior

SELL confirmation
position open/add/reduce/close

buyer rank
smart buyer rank

consensus
signal transitions

metadata hash/change detection

WSS event envelope
sequence replay

version mismatch
refresh-required state

release manifest signature
SHA256 verification
```

## Fixture tests

Keep representative chain fixtures:

```text
V2 launch
V2 tracked wallet CurveBuy
V2 untracked CurveBuy
V2 tracked CurveSell
router/relayer buy where tx.from != tracked wallet
recipient differs from buyer
metadata read
curve completion/graduation
```

## Integration tests

PostgreSQL:

```text
duplicate log → one event
restart → cursor resumes
WS gap → backfill fills exactly
alert persisted before WSS push
browser replay can obtain missed events
```

## Updater integration tests

```text
bad signature rejected
bad hash rejected
wrong architecture rejected
schema incompatibility rejected
failed readyz triggers rollback
successful restart creates update-applied alert
old frontend build detects mismatch
```

---

# 43. Acceptance Scenarios

## A. New Pons V2 token

```text
TokenLaunched
→ token stored
→ curve registered
→ original metadata stored
→ current metadata stored
→ UI receives token.launched
```

## B. Watched wallet BUY

```text
CurveBuy
recipient == watched execution address
→ receipt Transfer confirmation
→ BUY_CONFIRMED
→ position OPEN/ADD
→ smart metrics recompute
→ signal recompute
→ alert persisted
→ WSS push
→ browser toast/sound/TTS according to settings
```

## C. Token sent to watched wallet but not bought

```text
Transfer to watched address
without matching CurveBuy
→ TOKEN_RECEIVED
→ no Smart Money BUY signal
```

## D. Relayer transaction

```text
tx.from = relayer/multicaller
CurveBuy.recipient = watched address
Transfer token → watched address
→ BUY_CONFIRMED
```

The relayer is irrelevant to buyer identity.

## E. Watched wallet SELL

```text
CurveSell seller/flow matches watched wallet
→ SELL_CONFIRMED
→ REDUCE/CLOSE
→ exit metrics
→ optional cooling/distribution alert
```

## F. Browser disconnected

```text
important alert occurs
→ saved in DB
→ browser offline
→ reconnect
→ sequence replay
→ alert appears
```

## G. Upgrade

```text
admin installs signed release
→ service restarts
→ new readyz passes
→ durable update-applied alert
→ old tab reconnects
→ build mismatch
→ refresh-required modal
→ update sound / optional TTS
→ dashboard auto-refreshes safely
```

## H. Unsaved admin form during upgrade

```text
build mismatch
→ no forced auto-refresh
→ modal explains update
→ user saves/discards
→ manual refresh
```

---

# 44. Performance Targets

Initial target:

```text
launch detection:
within one indexed block plus processing latency

CurveBuy → matched Smart Trade:
P95 < 500 ms after receipt/log availability

Backend → browser WSS:
P95 < 500 ms after durable event commit

tracked-wallet lookup:
O(1)
```

Do not sacrifice correctness for a lower latency number.

---

# 45. MVP Definition

MVP is complete only when all of the following are working:

```text
Robinhood HTTP RPC + WSS
Pons V2 Factory verification
TokenLaunched indexing
dynamic curve registration
CurveBuy / CurveSell indexing

tracked trader + execution wallet registry
CSV import
chain-confirmed BUY
chain-confirmed SELL
position state

token on-chain metadata
original/current/history metadata

market metrics
basic holder index
entry age
buyer rank
smart consensus
signal state

PostgreSQL durability
reorg/backfill/cursor

Web dashboard
token detail
trader detail
research page
admin registry

application WSS
event sequence/replay
toast
sound
browser TTS
browser notifications
alert center

GitHub signed update
rollback
frontend refresh-required protocol
```

---

# 46. Explicitly Deferred

Do not build these before the MVP is stable:

```text
automatic trading
copy trading
wallet private keys
Fomo WebSocket dependency
Fomo automated login/scraping
multi-chain support
Pons V1
ML prediction
multi-node distributed indexer
complex wallet clustering
post-graduation V4 Smart Money tracking
```

Post-graduation V4 is the first major protocol extension after MVP if historical data shows it is useful.

---

# 47. Non-Negotiable Engineering Rules

1. Only Pons V2.
2. Chain ID 4663 must be verified.
3. Chain data is authoritative.
4. Fomo WebSocket is not required.
5. Relayer address is not trader identity.
6. `tx.from` is not trader identity.
7. Confirm BUY using Pons V2 event + monitored recipient + token-flow evidence.
8. Confirm SELL using Pons V2 event + monitored seller/token-flow evidence.
9. Plain token receipt is not a BUY.
10. Use U256/Decimal, never `f64`, for asset accounting.
11. All chain ingestion is idempotent.
12. All important events persist before WSS delivery.
13. Derived state must be rebuildable.
14. Original token metadata is immutable.
15. Metadata/research changes are historical snapshots, not overwrites.
16. No private keys.
17. No trade execution.
18. External providers use adapter traits.
19. WSS must reconnect and replay missed events.
20. Upgrade assets require signature + SHA256 verification.
21. Upgrade failure must roll back.
22. Backend/frontend build mismatch must trigger refresh-required UI.
23. Never force-refresh a page with unsaved admin changes.
24. Every signal must be explainable using stored evidence.
25. Every phase must add tests before moving to the next phase.

---

# 48. Current Reference Baseline

Verify again at implementation time because deployments and docs may change.

Robinhood Chain:
- https://docs.robinhood.com/chain/connecting/
- https://docs.robinhood.com/chain/deploy-smart-contracts/

Pons V2:
- https://docs.ponsfamily.com/v2
- https://github.com/ponsdotdev/ponsfamily

Fomo wallet architecture:
- https://fomo.family/blog/learn/fomo-security-wallet-architecture

Relay sender-attribution background:
- https://docs.relay.link/references/api/api_core_concepts/contract-compatibility

---

# 49. Final Architecture Summary

```text
Robinhood WSS/HTTP
        ↓
Pons V2 Factory + Curve Indexer
        ↓
Durable Raw/Normalized Events
        ↓
Trade Confirmation
        ↓
Fomo Execution Address Registry Match
        ↓
Smart Trade + Position
        ↓
Pons Market + Historical Snapshots
        ↓
Smart Consensus + Signal
        ↓
PostgreSQL
        ↓
Alert Outbox
        ↓
pons-radar WSS
        ↓
Web Dashboard
  ├─ real-time UI
  ├─ toast
  ├─ sound
  ├─ Chinese TTS
  ├─ desktop notification
  └─ upgrade refresh warning

Research side:
TokenLaunched
        ↓
Original On-chain Metadata
        ↓
Metadata History
        ↓
External Research Evidence
        ↓
Optional AI Research
        ↓
Historical Reports / Backtest
```

This is the implementation contract for the project.
