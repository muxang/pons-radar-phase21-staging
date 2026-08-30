# Phase 4 Token Registry

Phase 4 uses the current Pons V2 `TokenLaunched` definition published by the
[official V2 documentation](https://docs.ponsfamily.com/v2):

```text
TokenLaunched(address indexed token,address indexed curve,address indexed deployer,address pairToken,uint256 launchConfigId,uint256 graduationThreshold)
```

Its canonical topic0 is:

```text
0x8d4aad4953d0ca700d468f3753aa14432d1b35b43ec6409f051fb6aa43a89607
```

The binding is generated with Alloy `sol!`; the repository-root V1 `abi.json` is
not consumed. The committed fixture was captured from Robinhood Chain transaction
`0xeaae35437dbb755774a11c22b9d3f4b81a4e4ca4273edc77b9e8d9fcc9cb1536`
using `eth_getTransactionReceipt` and is never refreshed from the network in tests.

Each active trusted deployment gets the existing Phase 2 `BackfillCoordinator`, an
independent durable cursor, and a filter containing both its registry-provided factory
address and typed `TokenLaunched` topic0. Its registry `start_block` is passed directly
to the coordinator; no deployment block is compiled into business code.

The handler strictly decodes and validates the emitter and deployment block range,
loads the block timestamp, then atomically persists raw log, parser/schema-versioned
normalized event, immutable token identity, durable curve mapping, and `token.launched`
outbox event. Exact replay is idempotent. Conflicting token or curve identity rolls the
transaction back and prevents cursor advancement. Rejected ABI evidence is recorded in
`chain_ingestion_errors`.

The in-memory curve lookup is rebuilt from `pons_curves` on process startup. Phase 4
does not decode curve trades, read token metadata, or push application WebSockets.
