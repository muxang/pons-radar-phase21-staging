ALTER TABLE event_outbox ADD COLUMN published_at timestamptz;
COMMENT ON COLUMN event_outbox.published_at IS
  'Operational publisher observation time only; never a per-client acknowledgement or retention cursor.';
