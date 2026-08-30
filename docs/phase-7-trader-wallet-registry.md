# Phase 7 Trader / Execution Wallet Registry

PostgreSQL is the durable identity source. A trader owns zero or more versioned wallet
records; a handle is never treated as an address.

The runtime matcher contains only Robinhood Chain (`4663`) wallets whose role is
`ROBINHOOD_EXECUTION_ADDRESS`, whose trader and wallet are enabled, whose identity is
operator-verified, whose confidence meets `wallet_intelligence.minimum_identity_confidence`,
and whose half-open validity interval `[valid_from, valid_to)` contains the observation time.

Admin mutations refresh the immutable in-memory snapshot immediately. A five-second
background refresh handles time-bound activation/expiry and changes made by operational
jobs. Refresh failure retains the last complete snapshot.

Overlapping enabled execution identities for different traders are rejected by a
transaction-serialized PostgreSQL trigger. Historical, disabled, profile, and historical-role
rows remain queryable but never enter the matcher.

CSV columns are `handle,address,role,tier,confidence,notes`. Each row is validated and
committed independently; a failed row is rolled back and reported without undoing valid rows.
CSV identities start unverified and therefore require later operator verification before they
can match.
