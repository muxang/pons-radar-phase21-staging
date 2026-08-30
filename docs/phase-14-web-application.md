# Phase 14 production web application

The production UI treats PostgreSQL-backed REST read models as authoritative. The
application WebSocket only invalidates or incrementally refreshes affected query
caches; it never recalculates trade confirmation, positions, market state, consensus,
or signals in the browser.

## Routes

- `/dashboard`
- `/tokens` and `/tokens/:address/:tab`
- `/smart-money`, `/traders`, `/traders/:id`
- `/alerts`, `/research`, `/system`, `/admin`

Token tabs are Overview, Timeline, Smart Money, Market, Holders, Research, Metadata,
and Raw Evidence. All application pages require the existing HttpOnly administrator
session. Setup/login credentials are never persisted in browser storage.

## Read models and pagination

`WebRepository` owns all page aggregation SQL. Token lists, timelines, Smart Money
positions, snapshots, alerts, and trader activity have bounded limits. Decimal/U256
values cross the API as strings and are displayed without frontend accounting.

Timeline records use `event_effective_at`; classification origin and realtime
eligibility remain visible. Backfilled records are deliberately styled as historical.

## Research and safety

Metadata is rendered only as Preact text nodes. The UI does not use raw HTML.
External links are clickable only for parsed HTTP/HTTPS URLs and always use
`noopener noreferrer`. Unsafe values remain visible as non-clickable evidence.

Original/current/history capture modes and exact-launch evidence are shown directly.
External research and Fomo Content Intelligence are explicitly unavailable; Phase 14
does not fetch external sites.

## Realtime cache

TanStack Query core owns REST caching. Durable WSS events invalidate dashboard,
token, alert, or system query families. Reconnect/replay continues to use the
per-tab Phase 13 sequence cursor. Historical events refresh data without realtime
visual or audio semantics.

## Charts

Charts consume bounded immutable market and signal snapshots for 1h, 6h, 24h, or
All ranges. They visualize backend values only and perform no market/accounting
derivation. Evidence scope remains `BLOCK_STATE_EXACT` or `UNAVAILABLE`.
