# Phase 8 Chain-Confirmed Smart Wallet Trades

Curve ingestion performs only the cheap identity candidate check. A BUY candidate is created
from `CurveBuy.recipient`; a SELL candidate is created from `CurveSell.seller`. The candidate,
immutable identity snapshot, `smart_trades` STRONG state, and durable confirmation job are
committed in the same transaction as `token_trades`. Receipt RPC is never part of the chain
cursor transaction.

The confirmation worker claims jobs with `FOR UPDATE SKIP LOCKED`, recovers five-minute stale
claims, applies bounded concurrency, timeout, and exponential retry, and resumes after restart.

`BUY_CONFIRMED` requires a non-orphaned Pons BUY plus an event-time execution-wallet match and
a successful same-transaction/same-block receipt whose ERC20 Transfer evidence sums exactly to
`tokensOut` for `token + curve -> recipient`. `SELL_CONFIRMED` uses the corresponding exact sum
for `token + tracked seller -> curve`. Matching transfers may be split across multiple logs;
unrelated transfers are excluded before checked-U256 aggregation.

Receipt chain mismatch, orphaning, malformed Transfer evidence, or final amount mismatch is
fail-closed. Successful state, normalized transfer evidence, and the confirmed outbox event are
committed atomically. `tx.from`, relayers, routers, and quote recipients are not identity inputs.

Confirmation version is `1`. Buyer and smart-buyer ranks remain nullable for later phases.

## Historical and late-arriving classification

Identity backfill scans trades that already exist when an execution identity becomes eligible.
The inverse race is handled directly by chain ingestion: every Curve trade batch carries an
explicit `LIVE` or `CHAIN_BACKFILL` source, and candidate matching receives the authoritative
block timestamp. Registry lookup therefore applies `valid_from <= trade.block_time < valid_to`,
even when the wallet is no longer present in the current O(1) matcher when an older trade is
inserted by backfill or replay.

`CHAIN_BACKFILL` and `IDENTITY_BACKFILL` SmartTrades are durable historical facts but have
`realtime_alert_eligible=false`. Their successful outbox event is `smart_trade.buy_backfilled`
or `smart_trade.sell_backfilled`; only `LIVE` classifications use the real-time confirmed event
types. All three sources create the same Phase 8 confirmation job and use the same receipt,
Transfer aggregation, amount matching, identity snapshot, confirmation version, and database
uniqueness constraints.

The coordinator has three operational states: startup catch-up, reconnect catch-up, and steady
live. Startup and reconnect gaps are always fetched through HTTP as `CHAIN_BACKFILL`. Receiving
a WS notification does not itself make a range live: after any disconnect or failed live
catch-up, the durable cursor must successfully reconcile to at least the reconnect target before
the stream returns to steady live. Only a subsequent notification can produce a `LIVE` batch.
