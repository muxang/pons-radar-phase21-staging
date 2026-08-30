# Phase 13 durable alerts

The Alert Engine reads committed `event_outbox` rows in global sequence order. Recognized SmartTrade,
Position, Signal, and system events are converted to `alert_events`; the alert row and its `alert.*`
outbox event commit atomically. A durable cursor makes processing restart-safe. Database semantic keys
make source replay idempotent.

Historical classifications remain visible in Alert Center but retain `realtime_alert_eligible=false`.
They never produce toast, sound, speech, or desktop notifications. Pending chain events are shown as
provisional. A later confirmed update changes the same Alert ID and does not speak twice. ORPHANED input
marks that Alert `RETRACTED` and publishes an explicit correction delta.

Preferences are persisted per admin user. Browser permission/capability remains device-local: database
preferences never imply that Notification permission or autoplay permission was granted on another
device. `Enable Alerts` is the required user gesture that resumes Web Audio and requests Notification
permission when configured. Chinese speech uses fixed templates through `speechSynthesis`; it does not
use AI or an external service.

All tabs keep independent replay cursors in `sessionStorage`. A non-sensitive localStorage lease elects
one alert leader for sound, TTS, and desktop notifications; all tabs still receive WSS events and render
Alert Center changes. The lease heartbeat expires and another tab takes over after the leader closes.
