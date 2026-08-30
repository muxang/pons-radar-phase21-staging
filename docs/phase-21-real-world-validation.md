# Phase 21 — Real-World Validation & Production Readiness

This document is the evidence ledger for Phase 21. Automated fixture evidence and
real-world staging evidence are different columns and must never be substituted
for one another. Update a status to `PASS` only when the referenced artifact is
attached and independently reviewable.

Status vocabulary: `PASS`, `FAIL`, `BLOCKED`, `NOT OBSERVED`,
`MANUAL CONFIRMATION REQUIRED`.

## Current validation environment

| Field | Current evidence |
|---|---|
| Execution host | Ubuntu 24.04.4 LTS, Linux 6.8, x86_64, 4 vCPU, 7.4 GiB RAM |
| Target production host | `pons.43-165-167-100.sslip.io`, systemd + Caddy HTTPS |
| PostgreSQL | PostgreSQL 16.15 on staging; schema migration 27 |
| Application | `0.1.9`, embedded frontend `ui-terminal-20260830`, API schema 1 (redesigned UI deployment) |
| Frontend | Embedded Vite production build, HTTPS shell verified `Cache-Control: no-store` |
| Robinhood RPC | Operator-supplied Alchemy HTTP/WSS; endpoint credentials retained only in root-owned EnvironmentFile |
| Secrets | `/etc/pons-radar/environment` is `root:pons-radar 0640`; values are excluded from evidence and this report |

## Validation matrix

| Stage | Automated evidence | Real-world staging status | Required staging artifact |
|---|---|---|---|
| A Linux deployment | systemd/config asset regression tests PASS | PASS | Ubuntu/systemd/Caddy/PostgreSQL installed; service active; health and readiness 200; TLS and file modes verified |
| B Robinhood HTTP/WSS | mock reconnect, chain-id, lag tests PASS | PASS | chain ID 4663; authenticated HTTP and WSS healthy after adaptive stream deployment; durable factory cursor reached 49,847,018 during final capture |
| C TokenLaunched | fixed real-chain log fixture tests PASS | PASS | verified Factory code plus 1,425 live normalized launches, Token/Curve registry, metadata and outbox evidence observed on staging |
| D CurveBuy/CurveSell | fixed real-chain trade fixtures PASS | PASS | 22,985 BUY and 21,539 SELL durable live trades at final capture; raw U256 facts, holders/market jobs and zero semantic duplicates verified |
| E live Smart Wallet | real receipt fixture confirmation tests PASS | NOT OBSERVED | tracked identity effective at event time plus receipt Transfer and confirmed pipeline evidence |
| F WSS/browser alerts | automated replay, multi-client, alert tests PASS | MANUAL CONFIRMATION REQUIRED | real Chromium HTTPS login, two tabs, tab-local cursor, systemd restart/reconnect, replay and durable Alert Center passed; physical Sound/TTS/Desktop output requires a human browser session |
| G restart recovery | repository restart/stale-job tests PASS | PASS | application and PostgreSQL restarts recovered readiness; cursors/outbox advanced and semantic duplicate count remained zero |
| H updater A→B | signature/hash/archive/health unit tests PASS | PASS | signed 0.1.2→0.1.3, 0.1.3→0.1.5, 0.1.5→0.1.7 and 0.1.7→0.1.8 jobs reached SUCCEEDED with exact version/build readiness |
| H broken C→rollback B | rollback abstraction/tests PASS | PASS | rollback-safe broken 0.1.6 timed out readiness; 0.1.5 was atomically restored, ready, and job/event recorded ROLLED_BACK |
| I frontend refresh | build mismatch/dirty form/multi-tab tests PASS | PASS | real open 0.1.7 tabs detected 0.1.8; clean Dashboard auto-refreshed while dirty Admin tab retained the old build/form and blocked refresh |
| J soak | collection tooling supplied | FAIL | 15-minute preliminary run kept health/readiness 200 for all 60 samples, but is shorter than the required 24h and market backlog grew 990→1,159 |

Evidence capture began at `2026-08-30T06:19:58Z`. This ledger is updated during the
live run; stages not explicitly marked PASS remain open and prevent production sign-off.

## Stage A — Ubuntu installation gate

Follow `deploy/README.md`. Record:

```text
uname -a
dpkg-query -W postgresql
systemd-analyze verify /etc/systemd/system/pons-radar.service
stat -c '%U:%G %a %n' /opt/pons-radar /opt/pons-radar/* /etc/pons-radar/* /var/lib/pons-radar
systemctl show pons-radar.service -p User,Group,MainPID,ActiveState,SubState
curl --fail http://127.0.0.1:3000/healthz
curl --fail http://127.0.0.1:3000/readyz
```

Redact environment values. The reviewer should see variable names and file modes,
never credentials. Confirm the service receives SIGTERM and exits before
`TimeoutStopSec`, with durable cursor/job state committed.

## Stages B–D — live Robinhood/Pons evidence

Observed staging evidence (UTC):

- HTTP `eth_chainId` returned `0x1237`; WSS application health reported `HEALTHY`.
- At `2026-08-30T06:19:58Z` the head was `0x2f7beeb`; the configured Factory had
  24,177 bytes of runtime code. A fixed queried block returned the expected real
  `TokenLaunched` transaction, and the latest 1,000-block sample contained 46 launches.
- Registry deployment `b5b57cc9-37fd-4ee7-89fe-e6747b057b95` is enabled,
  `VERIFIED`, `OPERATOR_APPROVED`, chain 4663, with observed runtime code hash
  `0x89a27da6f703e0a7cdd4f233e7cb57604ff75b164530962d3ff7cf8483a67d84`.
- The first database capture contained 2,597 raw logs, 72 TokenLaunched events,
  429 CurveBuy events, 454 CurveSell events, and 1,643 ERC-20 Transfer events.
  All 72 metadata jobs had succeeded, the outbox high watermark was 955, and
  token-trade/SmartTrade semantic duplicate checks were zero.
- A later health capture reached chain lag zero, but HTTP health remained degraded
  by provider `429 Too Many Requests`. After adaptive sharding was deployed, final
  HTTP and WSS health were both healthy and live ingestion continued. The supplied
  provider tier still needs a longer capacity run before production approval.

Run from the repository checkout on staging:

```text
PHASE21_EVIDENCE_DIR=/secure/evidence/run-001 scripts/phase21-rpc-smoke.sh
```

The script fails closed unless HTTP and WSS both report chain ID `4663`. It saves
no endpoint URL. `scripts/phase21-validation-evidence.sql` records active Pons V2
deployments, cursors, normalized event versions, recent Tokens, trade counts,
worker state, duplicate checks, outbox, and alerts.

For a live TokenLaunched, attach one evidence record containing:

- deployment ID/address/trust state and active block range;
- block hash/number, transaction hash, log index and emitter;
- `raw_chain_logs.id`, normalized event ID/parser/schema version;
- Token and Curve registry IDs;
- metadata job and exact/fallback capture evidence;
- `token.launched` outbox sequence;
- authenticated Web Token detail screenshot.

For CurveBuy/Sell, additionally record actor and recipient separately, lossless raw
quote/token/fee/tax values, chain finality, market rebuild generation, holder delta,
and snapshot evidence scope. Refund evidence must not count as another BUY.

## Stage E — live Smart Wallet gate

Do not add a wallet retroactively just to manufacture a result. If a previously
verified, active execution wallet participates naturally, record:

1. CurveBuy recipient or CurveSell seller match at event time.
2. Identity snapshot and confidence policy.
3. Same-receipt ERC-20 Transfer evidence and exact/aggregate amount match.
4. SmartTrade confirmation and independent chain finality.
5. Position, Signal, Alert, outbox sequence, WSS envelope, and Web representation.

If no such transaction occurs during the validation window, retain `NOT OBSERVED`.
The committed receipt fixture remains automated evidence only.

## Stage F — browser/WSS checklist

Use at least two tabs and a supported Chromium browser over HTTPS:

- authenticate both tabs without exposing the session token;
- record `system.hello`, high watermark, LIVE and heartbeat state;
- disconnect the network, create or wait for events, reconnect, and verify ordered replay;
- verify each tab retains its independent sessionStorage cursor;
- verify exactly one Alert Leader emits Sound/TTS/Desktop notification;
- close the leader and verify bounded takeover by the other tab;
- confirm a LIVE eligible alert may Toast/Sound/TTS according to preferences;
- confirm CHAIN_BACKFILL and IDENTITY_BACKFILL update history but emit no Sound/TTS/Desktop notification;
- revoke/expire the session and confirm reconnect is rejected;
- retain browser console/network export with cookies and URLs sanitized.

Browser permission denial is not a product failure when the UI reports the denied
capability honestly. A claimed PASS must test both allowed and denied permission paths.

## Stage G — restart recovery

Capture `phase21-validation-evidence.sql` before and after each operation:

1. `systemctl restart pons-radar.service`.
2. A controlled PostgreSQL restart during staging maintenance.
3. Wait for ready state and worker backlogs to resume/drain.
4. Compare cursor hashes/blocks, Curve Registry count, pending/stale jobs, outbox high watermark, SmartTrade IDs, Alert semantic keys, and duplicate checks.
5. Verify browser replay begins after its own last sequence.

Any duplicate durable Alert or SmartTrade is a FAIL, not a cosmetic issue.

## Stages H–I — updater and frontend refresh

Use three staging-only, correctly signed releases:

- A: installed starting release;
- B: valid rollback-safe, no incompatible migration;
- C: rollback-safe no-migration binary deliberately failing readiness.

For A→B retain signed manifest bytes/signature/key ID, asset SHA256, archive file
listing, update job transitions, helper journal, atomic replacement filesystem,
systemd restart, `/healthz`, `/readyz`, and exact version/build/schema response.

For C, the failure must be restricted to staging and must not corrupt durable data.
Verify B is restored, ready, and recorded as `ROLLED_BACK`; verify
`system.update_rolled_back` and the Web alert. Never test an unsafe irreversible DB
migration to demonstrate binary rollback.

Keep an A browser tab open during A→B. Confirm reconnect to B's `system.hello`
causes build mismatch, SYSTEM_UPDATE is spoken once by the leader, and the persistent
refresh modal appears. A clean Dashboard may auto-refresh; a dirty Deployment,
Trader, Wallet, Content, Preference, or Update form must not. A rolled-back same-build
server must not show a false refresh requirement.

## Stage J — soak validation

Run `scripts/phase21-soak.sh` for at least 24 hours; 72 hours is recommended before
production approval. Preserve the raw CSV rather than only a screenshot.

Review:

- HTTP/WSS reconnect counts and maximum/recovery chain lag;
- PostgreSQL connections and database/disk growth;
- metadata, confirmation, position, market, signal, AI, and alert backlogs;
- retry errors and time-to-drain;
- process memory and CPU trend;
- cursor/outbox monotonicity and semantic duplicate checks.

Suggested fail conditions: readiness remains non-200 beyond the agreed maintenance
window, cursor lag grows without recovery, a durable queue grows monotonically, any
duplicate semantic SmartTrade/Alert appears, memory grows without plateau, or disk
growth is unexplained. Record actual thresholds chosen for the staging capacity.

## Bug log

### P21-001 — duplicate TOML table in deployment example

- Root cause: `config.example.toml` declared `[frontend_update]` twice.
- Affected phase: Phase 18 configuration / Phase 21 deployment.
- Impact: a standard TOML parser rejects the production example before startup.
- Fix: remove the duplicate declaration.
- Regression: parse the complete example and assert updater auto-install, AI provider,
  and AI-in-Signal remain disabled.

### P21-002 — incomplete systemd runtime boundary

- Root cause: the unit had no explicit working directory, UMask, shutdown timeout,
  or deployable updater restart authorization.
- Affected phases: Phase 0 graceful shutdown and Phase 17 updater.
- Fix: explicit `/var/lib/pons-radar` working directory, UMask, SIGTERM timeout,
  additional hardening, tmpfiles layout, and a unit-scoped polkit rule.
- Regression: production asset tests assert these boundaries and reject embedded secrets.

### P21-003 — release workflow used a stale database schema constant

- Root cause: the signed manifest workflow hard-coded schema 23 while committed
  migrations had advanced to 27.
- Affected phase: Phase 17 updater / Phase 21 release validation.
- Impact: a correctly built release would be rejected by schema compatibility checks.
- Fix: derive the signed manifest schema fields from the highest committed migration.
- Regression: release asset validation covers manifest generation inputs.

### P21-004 — fixed 32-way streams overload small registries/providers

- Root cause: trade and token Transfer supervisors always opened up to 32 hash shards
  each, even when fewer than 500 addresses fit safely in one filter. Registry changes
  consequently restarted many unnecessary streams, and staging exposed sustained 429s.
- Affected phases: Phase 6/10 ingestion scalability and Phase 21 RPC operations.
- Fix: select the smallest stable shard count whose filters remain bounded at 500
  addresses. At 100 curves this is one stream; growth remains bounded for 1,000 and
  10,000 curve registries.
- Regression: the 100/1,000/10,000 registry test asserts complete coverage, bounded
  filters, and a single stream at 100 curves. Real B deployment remains the staging proof.

### P21-005 — updater cancellation did not reach Axum graceful shutdown

- Root cause: the update service cancelled the shared token after handoff, but the
  Axum shutdown future listened only for SIGINT/SIGTERM.
- Affected phase: Phase 17 self-update handoff.
- Impact: the helper waited for the main PID indefinitely and the update remained
  `INSTALLING` until an operator signal.
- Fix: graceful shutdown now also selects on the update handoff cancellation token.
- Regression: a Tokio test cancels the updater token and requires the server shutdown
  future to resolve within one second.

### P21-006 — systemd killed the detached updater with the main service cgroup

- Root cause: systemd's default `KillMode=control-group` terminated the spawned helper
  together with the main process.
- Affected phase: Phase 17 atomic replacement/rollback.
- Impact: the helper could not replace/restart the service after the main process exited.
- Fix: the narrowly scoped unit uses `KillMode=process`; the helper remains unprivileged,
  path-confined, and authorized only to restart `pons-radar.service` by the existing
  polkit rule.
- Regression: production asset tests require the explicit updater-safe kill mode. The
  fixed release must still pass real automatic handoff and rollback before sign-off.

### P21-007 — release download timeout was too short for the allowed asset size

- Root cause: the same 15-second request timeout covered both small GitHub API calls
  and a streamed release asset that may be up to 256 MiB.
- Affected phase: Phase 17 asset staging.
- Impact: a valid 9.8 MiB staging asset timed out mid-body and the job failed closed.
- Fix: retain bounded downloads and SHA256 verification while raising the explicit
  deployment default to 120 seconds.
- Regression: production configuration tests require the bounded 120-second value.

### P21-008 — rollback renamed a backup across filesystem/mount boundaries

- Root cause: install correctly copied to a sibling file before atomic rename, but
  rollback directly renamed `/var/lib/.../backup` over `/opt/.../pons-radar`.
- Affected phase: Phase 17 automatic rollback.
- Impact: the staging systemd mount namespace returned `EXDEV`; the deliberately
  broken release produced a real `ROLLBACK_FAILED` and required operator recovery.
- Fix: rollback now copies the backup into a destination sibling, fsyncs it, preserves
  executable permissions, then performs the atomic rename on the destination filesystem.
- Regression: the updater test uses distinct source/destination directories, verifies
  restored bytes and mode, and confirms the audit backup remains intact. Signed broken
  release `0.1.6` subsequently proved the corrected real rollback path.

The required revalidation used signed, rollback-safe release `0.1.6`. Its readiness
check timed out, the updater restored `0.1.5`, restarted systemd, verified readiness,
persisted `ROLLED_BACK`, and emitted `system.update_rolled_back`. The earlier
`ROLLBACK_FAILED` row remains in release history as audit evidence rather than being
deleted or rewritten.

## Final staging evidence — 2026-08-30

- Linux/PostgreSQL: Ubuntu 24.04.4 LTS, Linux 6.8 x86_64, PostgreSQL 16.15,
  migration 27. The service runs as `pons-radar`, binds only to loopback, and is
  published by Caddy at `https://pons.43-165-167-100.sslip.io`.
- Factory registry: chain 4663, enabled, `VERIFIED`, `OPERATOR_APPROVED`; runtime
  bytecode and its observed hash were recorded without treating documentation as
  the trust root.
- Final chain capture: 1,425 `PONS_V2_TOKEN_LAUNCHED`, 22,985 BUY trades and
  21,539 SELL trades. Token-trade and SmartTrade duplicate checks both returned zero.
  No monitored execution-wallet event occurred, so live Smart Wallet remains
  `NOT OBSERVED` and fixture evidence is not substituted.
- Restart: both application and PostgreSQL restarts recovered cursors, registries,
  durable workers and browser replay; no semantic duplicate was introduced.
- Application WSS/browser: two authenticated Chromium tabs used independent
  `sessionStorage` cursors. A real systemd restart produced reconnect/replay and both
  tabs returned LIVE. Durable system update/rollback alerts remained visible.
- Updater: all final releases used a detached Ed25519 signature, deployment-pinned
  public key, signed SHA256, schema 27 compatibility, safe archive extraction and
  exact post-start version/build checks. The redesigned embedded UI was subsequently
  released as signed `0.1.9 / ui-terminal-20260830`; its update job reached
  `SUCCEEDED`, health/readiness returned 200, and the new hashed CSS asset was served.
- Frontend refresh: tabs loaded from `0.1.7 / stage-refresh-final` observed
  `0.1.8 / stage-refresh-browser`. The Dashboard showed the persistent mismatch
  modal and auto-refreshed. A dirty Admin Trader form showed the unsaved-change
  guard, retained the old client build and form state, and did not refresh.
- Audible browser output: headless Chromium cannot establish that physical audio,
  Chinese speech, or desktop notification was perceived. Permission/state UI and
  automated leader/deduplication tests passed; physical output is
  `MANUAL CONFIRMATION REQUIRED`.
- Preliminary soak: 60 samples over 15 minutes, all `/healthz` and `/readyz` 200;
  cursor/outbox were monotonic. Memory ranged from roughly 79–250 MiB across update
  restarts. Metadata backlog ended at 26, while market backlog rose from 990 to 1,159.
  This is a `FAIL` for production sign-off because the required 24-hour duration was
  not completed and the market queue did not demonstrate drainage.

## Remaining production blockers

1. Complete at least a 24-hour (72-hour recommended) uninterrupted soak using the
   committed collector and retain raw CSV plus DB/disk growth evidence.
2. Demonstrate that the market rebuild backlog drains under realistic launch volume,
   or capacity-tune/fix it with a regression test. A monotonically growing derived
   queue is not accepted merely because readiness remains 200.
3. Perform a human browser confirmation of Sound, Chinese TTS, desktop notification,
   Alert Leader handoff, and historical/backfill silence on the target desktop.
4. Live Smart Wallet end-to-end evidence remains `NOT OBSERVED`; this is an explicit
   residual validation gap, not a fixture-backed PASS.

**Production Ready: NO.**

## Automated regression results

- `cargo fmt --all -- --check`: PASS.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: PASS.
- `cargo test --workspace` against real PostgreSQL 16.15 on the isolated staging
  host: PASS, 147 tests, 0 failed, 0 ignored. The application role received
  `CREATEDB` only for SQLx test database setup and the operator script revokes it
  in an EXIT trap.
- `cargo build --workspace --release`: PASS for version 0.1.8.
- Frontend typecheck: PASS.
- Frontend Vitest: PASS, 8 files / 26 tests, 0 failed.
- Frontend production build: PASS.

An initial local Rust test invocation without `DATABASE_URL`, followed by a staging
invocation before temporary SQLx test-database permission was granted, failed at
test setup rather than in assertions. Both environmental failures are preserved in
the operator log; the final full PostgreSQL run above is the acceptance result.

## Sign-off

Production readiness requires all critical real-world stages to be PASS, except live
Smart Wallet may remain NOT OBSERVED if explicitly accepted as residual validation
risk. Sign-off must include operator, reviewer, UTC timestamp, evidence directory
hash/inventory, soak duration, unresolved failures, and rollback decision.
