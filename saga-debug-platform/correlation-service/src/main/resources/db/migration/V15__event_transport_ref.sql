-- Which record on the transport carried each observation.

-- The duplicate detector fired on exactly the wrong occasions. It lived behind a
-- database lookup that only runs when the in-memory deduplication cache misses, and that
-- cache misses when the observer restarts or its consumer offsets are reset -- so
-- re-reading a topic reported every event on it as a duplicate delivery by the observed
-- system, while a genuine duplicate arriving inside one run was swallowed by the cache
-- and never reported at all. After RCA promotion landed, one of those fabricated findings
-- could become the promoted root cause of a saga.

-- The two cases are distinguishable, but only with something the platform was not
-- keeping: an observation re-read after an offset reset comes from the SAME record on the
-- transport, while a source that published an event twice produced TWO records. Kafka
-- names a record by topic-partition-offset and the consumer already computes it; it was
-- simply discarded once the event id had been read from the payload.

-- Nullable, and null means "the transport offered no identity for this record" -- true of
-- every row written before this column existed, and of HTTP ingestion, where a repeated
-- POST really is a second delivery. Nothing infers replay from a null: the detector falls
-- back to its previous behaviour rather than guessing.
ALTER TABLE saga_events ADD COLUMN transport_ref VARCHAR(300);

-- Answering "was this same record already stored" is the read the detector now makes on
-- every duplicate, so it is worth an index alongside the identity it is checked with.
CREATE INDEX IF NOT EXISTS idx_saga_events_source_transport
    ON saga_events (source_name, transport_ref);
