# pons-radar

Phase 0 repository bootstrap for the read-only Pons V2 research radar.

## Development

```text
cd frontend
npm ci
npm run build
cd ..
copy config.example.toml config.toml
set DATABASE_URL=postgres://postgres:postgres@localhost:5432/pons_radar
cargo run -p pons-radar
```

The server listens on the configured address and exposes `/healthz`, `/readyz`, and
`/api/v1/system/version`. The production frontend in `frontend/dist` is embedded in
the Rust executable at compile time.

Set `PONS_CONFIG` to select another configuration file and `RUST_LOG` to override the
structured tracing filter. Future secret values belong only in the environment and
must never be written to `config.toml`; no secrets are consumed in Phase 0.

For reproducible release builds, pass the same identifiers to both builds:

```text
VITE_APP_VERSION=0.1.0 VITE_FRONTEND_BUILD_ID=<build-id> npm run build
FRONTEND_BUILD_ID=<build-id> cargo build --release -p pons-radar
```

Phase 1 applies forward-only SQLx migrations at startup. PostgreSQL connection
settings are non-secret TOML values; the connection URL is read only from
`DATABASE_URL`.

No blockchain provider, wallet monitoring, transaction execution, signing, or
private-key handling exists through Phase 1.

Phase 2 requires both `RH_RPC_HTTP_URL` and `RH_RPC_WS_URL`. HTTP startup verification
must report Robinhood Chain ID 4663. A temporarily unavailable WS endpoint starts in
reconnecting state without blocking the Web/API server; any reachable wrong-chain WS
endpoint is rejected.
The protocol-independent chain worker foundation is documented in
`docs/phase-2-chain.md`; no Pons ABI or business event parsing is included yet.

Phase 3 adds the PostgreSQL-backed Pons V2 deployment registry and admin endpoints at
`/api/v1/admin/deployments`. The documented factory is seeded disabled and unverified.
Only records that are enabled, successfully verified on-chain, and active for the
requested block range are returned to future ingestion consumers. Verification stores
chain ID, bytecode, code-hash, interface-fingerprint, and topic-validation evidence.

Phase 3.1 protects admin routes with PostgreSQL-backed Argon2id administrator accounts
and expiring opaque sessions. Set a random `ADMIN_SETUP_TOKEN` of at least 32 characters
out of band before first-run setup. Production requires an HTTPS `security.allowed_origin`
and Secure cookies. Browser mutations must send the session-bound `pons_csrf` cookie value
as `X-CSRF-Token`; requests from any other Origin are rejected. Session cookies are
HttpOnly and raw session tokens are never stored in PostgreSQL.

Phase 4 indexes the current Pons V2 `TokenLaunched` event from active trusted deployment
records, using the existing backfill/cursor framework. See `docs/phase-4-token-registry.md`
for the ABI provenance, exact topic, atomic persistence boundary, and fixture source.

Phase 5 asynchronously reads bounded, untrusted ERC-20 and Pons V2 on-chain metadata.
Original, current, and hash-deduplicated snapshot history are durable in PostgreSQL;
RPC failures retry independently of chain ingestion. See `docs/phase-5-token-metadata.md`.
Phase 5.1 first attempts all Original metadata calls at the durable launch block and
explicitly distinguishes exact launch evidence from a bounded historical fallback.
