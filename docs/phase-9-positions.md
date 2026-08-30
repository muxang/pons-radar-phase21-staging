# Phase 9 Smart Wallet Positions

Positions are derived only from `PONS_V2_CONFIRMED_TRADES`. They are not asserted to equal the
wallet's ERC20 balance because ordinary transfers and non-curve venues are outside this basis.
The basis and calculation version are persisted on every current position and event.

Successful Phase 8 confirmation marks the corresponding wallet/token rebuild job dirty in the
same transaction. The durable worker replays the entire non-orphaned confirmed SmartTrade ledger
in `(block_number, coalesce(transaction_index, log_index), log_index, tx_hash)` order. It never
uses creation or confirmation time. A generation counter prevents a confirmation arriving during
a rebuild from being lost; stale claims are recoverable after restart.

Rebuild replaces derived events deterministically. BUY transitions are OPEN then ADD. A SELL below
the derived balance is REDUCE and an exact SELL is CLOSE. A SELL above the derived balance emits
`POSITION_INTEGRITY_WARNING`, leaves the balance unchanged, and never fabricates a close. U256 is
used for every raw balance and quote accumulation.

Ranks are token-wide derived metrics and are recomputed whenever a wallet/token rebuild runs.
`buyer_rank` compares the wallet's first confirmed SmartTrade BUY position with the first on-chain
CurveBuy position of every unique token recipient. CurveBuy `buyer` remains the protocol execution
actor and is never the buyer-identity analytics key. `smart_buyer_rank` orders the first confirmed
BUY of every unique monitored execution address. Subsequent BUYs retain the wallet's first-entry
ranks. ORPHANED underlying trades participate in neither positions nor ranks.

The shared participant semantics for Phase 10 are: BUY `execution_actor = CurveBuy.buyer` and
`market_participant = CurveBuy.recipient`; SELL `market_participant/execution_actor =
CurveSell.seller` and `proceeds_recipient = CurveSell.recipient`. Unique buyers therefore use BUY
recipient, while unique sellers use SELL seller.
