# Phase 12 application realtime protocol

`GET /ws` is the authenticated application WebSocket. It is unrelated to Robinhood RPC WSS and
does not consume Fomo WSS. The handshake requires a valid, unexpired, non-revoked HttpOnly admin
session and an exact configured Origin. Client messages are limited to WebSocket protocol control
frames; application mutation messages close the connection.

The publisher polls committed `event_outbox` rows in global `seq` order and broadcasts through a
bounded Tokio channel. A slow receiver that overruns its queue is disconnected and recovers through
replay. `published_at` records publisher observation only; it is not a browser acknowledgement and
events are never deleted after a client send.

The first frame is `system.hello`, containing the server/frontend/API versions, server time and a
committed outbox high watermark. The client buffers live frames while replaying
`GET /api/v1/events?after_seq=X&through_seq=H&limit=N` through that watermark, then merges buffered
frames in sequence order. Delivery is at least once; the browser persists only `last_seen_seq` and
deduplicates by sequence. Each tab owns its own cursor.

Every event envelope preserves `realtime_alert_eligible`, classification origin, trade evidence,
chain finality and signal finality when present. `provisional` means chain finality is `PENDING`;
evidence confirmation and chain confirmation are deliberately separate. Historical/backfilled events
remain usable as timeline deltas but retain non-realtime semantics.

The server sends configured Ping frames and closes clients that stop responding. The browser reconnects
with capped 1/2/4/8/15/30-second exponential backoff plus jitter and always replays its missing sequence
range. REST remains the authoritative source for full page state; WebSocket and replay provide deltas.
