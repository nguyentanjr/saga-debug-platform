-- F6 / I3: keep timestamp provenance with the observation in the same local transaction.
-- DECLARED means a usable mapped event-time value (payload, header, etc.), not proof
-- that the producer's clock is accurate. TRANSPORT means fallback to transport time.
-- Nullable, with no default or backfill: the origin of an older timestamp is unknown.
-- Comparing timestamp to received_at or re-reading payload cannot recover that fact.
-- Older writers remain compatible after migration: omitting the column stores NULL.
ALTER TABLE saga_events ADD COLUMN event_time_source VARCHAR(20);
ALTER TABLE saga_events ADD CONSTRAINT chk_saga_events_event_time_source
    CHECK (event_time_source IN ('DECLARED', 'TRANSPORT'));
