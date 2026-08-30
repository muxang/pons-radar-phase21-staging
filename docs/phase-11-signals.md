# Phase 11 consensus and signal semantics

Phase 11 is derived exclusively from durable chain-derived facts. Consensus windows are 30 seconds,
1, 3, 5, and 15 minutes and use `block_time`; database insertion and confirmation times never alter
the effective history. Only evidence-confirmed Smart Trades whose underlying trade is not orphaned
participate.

The default opportunity weights are Smart Consensus 45, Pons Momentum 25, Capital Flow 15, Holder
Distribution 10, and Research/Narrative 5. An unavailable component is excluded from the denominator,
not scored as bad. Every snapshot stores configured weights, the renormalized denominator, component
availability/confidence, raw inputs, rule evaluations, and rule/weight/calculation versions. Arithmetic
uses U256/BigUint and Decimal; no `f64` participates.

Qualified wallet weight combines the configured manual tier, the identity confidence captured with the
trade, versioned launch timing buckets, buyer ranks, and position-event conviction. Independence is
currently the explicit `UNCLUSTERED_V1` model: weight 1 and no inferred copying cluster. Raw wallet and
quote exit ratios remain separate.

The state machine is `NO_SIGNAL -> WATCH -> STRONG_WATCH -> HIGH_PRIORITY`, with `COOLING`,
`DISTRIBUTION`, and `CLOSED` negative paths. Consecutive qualifying observations and asymmetric
thresholds provide hysteresis. `HIGH_PRIORITY_CONSENSUS` and `SMART_DISTRIBUTION` hard-rule results
persist their input values and thresholds.

Database triggers enqueue one durable, generation-numbered token rebuild job for Smart Trade, Position,
Market Snapshot, and finality changes. Rebuild replays event-time history deterministically, retains old
generations as non-current evidence, and atomically replaces current derived state. ORPHANED trades are
excluded and can remove a now-invalid current signal. Stale worker claims are recoverable after restart.

`LIVE` input may produce `realtime_alert_eligible=true`. `CHAIN_BACKFILL`, `IDENTITY_BACKFILL`, market
rebuilds, and finality rebuilds always remain historical. Only eligible transitions write normal
`signal.*` outbox events; historical reconstruction never masquerades as a real-time alert. Application
WebSocket delivery is intentionally outside Phase 11.
