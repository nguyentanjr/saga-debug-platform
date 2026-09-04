package com.example.correlation.ingestion;

/**
 * What became of one observation.
 *
 * <p>Returned rather than signalled by exception or by silence, because each transport
 * has to react differently: Kafka needs to know whether to retry the record, an HTTP
 * caller needs a status code, and both need to tell "we already had this" apart from
 * "we could not read this".
 *
 * <p>A transient failure — the database being unavailable — is not represented here. It
 * propagates as an exception, so no adapter can mistake it for a decision.
 */
public record IngestionResult(Outcome outcome, String eventId, String reason) {

    public enum Outcome {
        /** Stored and correlated. */
        ACCEPTED,
        /** Already processed; nothing to do. */
        DUPLICATE,
        /** The payload could not be parsed. Retrying will not change that. */
        UNREADABLE,
        /**
         * No correlation key, so the observation cannot be attached to any execution.
         * Usually a mapping that does not match the source's actual shape.
         */
        NOT_CORRELATABLE
    }

    public static IngestionResult accepted(String eventId) {
        return new IngestionResult(Outcome.ACCEPTED, eventId, null);
    }

    public static IngestionResult duplicate(String eventId) {
        return new IngestionResult(Outcome.DUPLICATE, eventId, null);
    }

    public static IngestionResult unreadable(String reason) {
        return new IngestionResult(Outcome.UNREADABLE, null, reason);
    }

    public static IngestionResult notCorrelatable(String reason) {
        return new IngestionResult(Outcome.NOT_CORRELATABLE, null, reason);
    }

    public boolean isAccepted() {
        return outcome == Outcome.ACCEPTED;
    }

    /** True when nothing about the observation would change on a second attempt. */
    public boolean isPermanentlyRejected() {
        return outcome == Outcome.UNREADABLE || outcome == Outcome.NOT_CORRELATABLE;
    }
}
