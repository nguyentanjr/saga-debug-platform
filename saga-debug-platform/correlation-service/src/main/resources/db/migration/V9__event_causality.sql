-- Causal links between events.
--
-- Until now the only way to say that one step led to another was to read it out of the
-- workflow specification: step B follows step A because the spec declares that edge.
-- That is a statement about the design, not about the run — it cannot distinguish
-- "B happened because A finished" from "B happened while A was finishing".
--
-- These columns record the link as the producer observed it:
--   causation_id    the event this one was produced in response to
--   span_id         this event's own OpenTelemetry span
--   parent_span_id  the span that caused it
--
-- All nullable. An event without them still correlates exactly as before; the
-- projection falls back to inferring boundaries from the specification and labels the
-- result as inferred.
ALTER TABLE saga_events
    ADD COLUMN causation_id   VARCHAR(100),
    ADD COLUMN span_id        VARCHAR(64),
    ADD COLUMN parent_span_id VARCHAR(64);

-- Resolving "which event caused this one" walks these two paths.
CREATE INDEX IF NOT EXISTS idx_saga_events_causation
    ON saga_events (saga_id, causation_id);

CREATE INDEX IF NOT EXISTS idx_saga_events_span
    ON saga_events (saga_id, span_id);
