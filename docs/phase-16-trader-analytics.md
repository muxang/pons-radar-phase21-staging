# Phase 16 trader historical analytics

`manual_tier` and `pons_score` are independent. PonsTraderScore V1 is research-only and `use_dynamic_trader_score` remains false; Phase 11 production Signal inputs are unchanged.

A position episode is one ordered `OPEN → ADD/REDUCE → CLOSE` sequence for a Trader, execution wallet and token. A BUY after CLOSE starts the next numbered episode. Episodes are rebuilt from durable Position Events in block/transaction/log order.

Fixed outcomes use episode entry time plus 5m, 15m, 1h, 6h and 24h. The first persisted block-state snapshot at or after the target and within five minutes is selected. Price change is a curve market-price outcome proxy relative to exact gross entry execution price, never realized Trader PnL. Missing, pending, unavailable, invalidated and post-graduation-censored observations remain distinct. Observed MFE/MAE means extrema across persisted snapshots only, not continuous tick-level excursion.

Historical scores expose two explicit time bases. `KNOWLEDGE_TIME` includes an episode only after its confirmed SmartTrade was available to pons-radar and includes an outcome only after both its horizon matured and its selected market evidence entered the database. It is the API default and the mandatory basis for future strategy backtests, signal replay, parameter optimization, and predictive validation. A late identity/chain backfill or late market snapshot never rewrites an earlier knowledge-time score.

`EVENT_TIME_RECONSTRUCTED` is retrospective research: episodes enter at their chain event time and outcomes at their target horizon when later evidence can reconstruct them. It answers what the wallet actually did historically, but must not be used to claim what the system knew then. Content follows the same contract (`published_at` is event time; `observed_at`/import time is knowledge time), although Content is not a Score V1 input.

Score history stores `effective_at`, `calculated_at`, `knowledge_available_at`, and `as_of_mode`. `GET /api/v1/traders/{id}/score?as_of=...` defaults to `KNOWLEDGE_TIME`; callers must opt into `mode=EVENT_TIME_RECONSTRUCTED`. Responses echo the mode, as-of timestamp, and evidence cutoff. Current scores continue to use every valid fact known now.

Components and weights are versioned: Early Entry 25, Buyer Rank 20, Market Outcome 35, Selectivity 10, Conviction 5 and Hold Behavior 5. Unavailable components are renormalized. V1 shrinks the raw score toward a 50-point prior with strength five episodes. Confidence is separate and uses episode sample size, matured-horizon completeness and durable identity confidence.

Position size relative to history uses only earlier episodes. Rebuild jobs are durable, generation-based, retryable and stale-claim safe; SmartTrade, Position, Market Snapshot, chain-finality and lifecycle changes dirty the affected Trader.
