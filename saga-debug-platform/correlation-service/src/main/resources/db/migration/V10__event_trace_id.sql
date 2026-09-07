-- The trace this event belongs to, read through the source's mapping.
--
-- V9 gave events their causal columns but left the trace id behind, so the only way to
-- recover it was to re-parse the payload looking for a field literally named traceId.
-- That name is this platform's own convention. A system carrying its trace id anywhere
-- else -- nested, or in a header, which is where W3C traceparent actually travels --
-- had its trace id silently dropped: no Jaeger link in the RCA report, and the
-- trace-context detector permanently blind for that system.
--
-- SourceMapping has always been able to name where the trace id lives. Nothing read it.
-- This column is where the mapped value lands, alongside the causal ones.
--
-- Nullable: a source that reports no trace correlates exactly as before.
ALTER TABLE saga_events
    ADD COLUMN trace_id VARCHAR(64);

-- "Every event of this trace", which is how a report crosses from one saga to the
-- traces behind it.
CREATE INDEX IF NOT EXISTS idx_saga_events_trace
    ON saga_events (trace_id);
