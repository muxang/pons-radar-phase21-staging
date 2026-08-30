# Phase 5 — Pons V2 Token Metadata

## ABI authority

The metadata reader follows the current verified Pons V2 documentation. It uses
the standard ERC-20 `name()`, `symbol()`, `decimals()`, and `totalSupply()` calls,
plus V2 `getTokenInfo()` returning deployer, logo, description, and the five-field
social tuple. The repository-root V1 ABI is not used.

All calls are read-only `eth_call` requests at one observed block. Metadata text
is untrusted evidence; no URI is fetched or rendered as HTML by this phase.

## Durable model

- `token_metadata_jobs` is created in the same transaction as a TokenLaunched
  token. Claims use `FOR UPDATE SKIP LOCKED`; stale claims are recoverable.
- `token_metadata_original` is insert-only and protected by a database trigger
  against update and delete.
- `token_metadata_current` is replaced after each successful observation.
- `token_metadata_snapshots` has a `(token_id, SHA-256 content_hash)` unique key,
  so identical observations do not create history noise.

Every observation records its block and wall-clock observation time. The launch
deployer remains authoritative. `getTokenInfo().tokenDeployer` is stored as
evidence and either marked matching or accompanied by an integrity warning.

## Worker isolation

Fixed independent claim loops provide bounded concurrency. Each token has an
overall RPC timeout. Failures record attempt count, bounded error text, and an
exponentially backed-off next attempt. Successful jobs schedule a later refresh,
allowing current metadata and history to evolve. Metadata failure never changes
the chain cursor or token launch transaction.

Strings are Unicode-safely bounded by character count before persistence. Raw
values are retained within those bounds, while HTTP(S)-only normalized URI
representations are stored separately. No website, social account, or logo is
downloaded.

The fixed Phase 4 token fixture was captured from Robinhood Chain and is decoded
offline in CI from `fixtures/pons_v2/token_metadata_0xf9b84b5f.json`.

## Phase 5.1 launch-time evidence

Each new metadata job durably records the token's `launch_block` as its requested
observation block. Before an Original row exists, every ERC-20 and V2 metadata
call targets that exact block. A successful capture is labeled `LAUNCH_BLOCK`
with `exact_launch_snapshot=true` and equal requested/observed blocks.

Historical failures increment a separate counter and retry at the same requested
block. After the configured attempt limit, the worker may read one current head;
that Original is explicitly labeled `FIRST_AVAILABLE`, remains non-exact, and
retains both the requested launch block and actual observed block. Existing
Original rows migrate as `LEGACY_FIRST_AVAILABLE` and are never rewritten.
