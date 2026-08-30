# Phase 15 content intelligence boundary

The shipped provider is `MANUAL_REFERENCE`. It has no fetch capability, is disabled for automatic collection, and does not permit raw-content storage. An automatic provider is fail-closed unless its authorization basis is an official API, written permission, or a user-provided authorized import. No Fomo scraper, private API, login automation, or Fomo WebSocket exists.

Third-party content is represented by its external reference, publication time, content hash, a bounded operator/provider summary, structured stance and narratives, and provenance. Authorized raw text has a separate guarded table; both the content item and provider must explicitly permit raw retention.

Token links are explicit evidence. Direct contracts and operator links can carry high confidence; symbol mentions are not automatically bound. Trader identity uses the durable Trader ID and unresolved content remains nullable rather than fuzzy-matched.

Relations are rebuilt from durable content, confirmed SmartTrades, and Position Events. Ordering uses `published_at` versus the chain `block_time`; `observed_at` is evidence of collection latency only. Jobs are generation-based, retryable, restart-safe, and are dirtied by either a content/token link or a later SmartTrade. Structured alignment is emitted only for a known `BULLISH`/`BEARISH` stance. Manual and historical content is never realtime-alert eligible.
