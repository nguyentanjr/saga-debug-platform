package com.example.correlation.domain;

import java.time.Instant;

/**
 * One observation as it arrived, before anything has been correlated.
 *
 * <p>Replaces an eight-argument call into the correlation service. That signature was
 * already at the point where argument order was the only thing keeping the caller honest;
 * carrying causality took it to eleven and namespaced identity would have taken it to
 * fifteen, so the fields that belong together now travel together.
 *
 * <p>The grouping is not cosmetic. {@link Identity} is what decides <em>which run this
 * is</em> and on what grounds, and every field in it has to be set as one decision — the
 * source, the namespaced key, the basis. Splitting them across a flat argument list is
 * what let the source be dropped on the floor for as long as it was.
 *
 * @param eventTime  when the work happened, per the producer; not when it was delivered
 * @param receivedAt when this platform saw it
 */
public record IngestedEvent(
        Identity identity,
        /**
         * The workflow the source's mapping says its events belong to, or null.
         *
         * <p>Carried on the observation rather than looked up later because the mapping is
         * only in hand at ingestion, and because it is a statement about how to interpret
         * this event — the same kind of thing as its type.
         */
        String declaredWorkflow,
        String eventType,
        String stepName,
        String serviceName,
        Instant eventTime,
        String payload,
        Instant receivedAt,
        Causality causality,
        /**
         * The transport's own name for the record this observation arrived on, or null.
         *
         * <p>Carried alongside the event rather than folded into {@link Identity} because
         * it says nothing about <em>which run</em> this is — it says which record on the
         * wire produced it, which is what tells a re-read of one record apart from two
         * records carrying the same event id. See {@code SagaEvent#transportRef}.
         */
        String transportRef) {

    /**
     * An observation whose transport offered no identity for its record.
     *
     * <p>Kept so callers that never came off a broker — the admin injection path, and
     * tests exercising correlation rather than ingestion — read unchanged. Their
     * {@code transportRef} is null, which the duplicate detector reads as "cannot tell"
     * rather than as evidence either way.
     */
    public IngestedEvent(Identity identity, String declaredWorkflow, String eventType,
            String stepName, String serviceName, Instant eventTime, String payload,
            Instant receivedAt, Causality causality) {
        this(identity, declaredWorkflow, eventType, stepName, serviceName, eventTime,
                payload, receivedAt, causality, null);
    }

    /**
     * Which execution this observation belongs to, and why the platform thinks so.
     *
     * @param sourceName    the mapping that read it — the namespace half of the identity
     * @param executionKey  source and raw correlation key together; what {@code saga_id}
     *                      stores
     * @param eventId       the producer's identity for this observation, unique only
     *                      within its own source
     * @param businessKey   what an operator searches by — an order number, a booking
     *                      reference — or null
     * @param correlationBasis on what grounds this was filed here; never inferred without
     *                      evidence, and never null
     */
    public record Identity(
            String sourceName,
            ExecutionKey executionKey,
            String eventId,
            String businessKey,
            CorrelationBasis correlationBasis) {

        public Identity {
            if (executionKey != null && sourceName != null
                    && !sourceName.equals(executionKey.source())) {
                throw new IllegalArgumentException(
                        "identity disagrees with itself: source '" + sourceName
                                + "' but key '" + executionKey.canonical() + "'");
            }
        }
    }

    /**
     * What the producer said about what caused this observation.
     *
     * @param causationId  the event this one was produced in response to, or null. The
     *                     strongest signal available: an observed causal link rather than
     *                     one inferred from the workflow specification
     * @param spanId       this observation's own OpenTelemetry span, or null
     * @param parentSpanId the span that caused it, used when {@code causationId} is absent
     * @param traceId      the trace this observation belongs to, or null. Read through the
     *                     source's mapping rather than from a field this platform names,
     *                     so a system that keeps it elsewhere is not treated as untraced
     */
    public record Causality(
            String causationId,
            String spanId,
            String parentSpanId,
            String traceId) {

        public static final Causality NONE = new Causality(null, null, null, null);
    }

    // ── the shape callers had before, so correlation code reads unchanged ──────

    /** The namespaced execution key, which is what {@code saga_events.saga_id} stores. */
    public String sagaId() {
        return identity.executionKey() != null ? identity.executionKey().canonical() : null;
    }

    public String sourceName() { return identity.sourceName(); }
    public ExecutionKey executionKey() { return identity.executionKey(); }
    public String eventId() { return identity.eventId(); }
    public String businessKey() { return identity.businessKey(); }
    public CorrelationBasis correlationBasis() { return identity.correlationBasis(); }

    public String causationId() { return causality.causationId(); }
    public String spanId() { return causality.spanId(); }
    public String parentSpanId() { return causality.parentSpanId(); }
    public String traceId() { return causality.traceId(); }

    public boolean hasCausalLink() {
        return causality.causationId() != null || causality.parentSpanId() != null;
    }

    /**
     * An observation from a source that declares nothing, carrying no causal metadata.
     *
     * <p>The identity is namespaced under {@link ExecutionKey#DEFAULT_SOURCE} like any
     * other, so an event injected by hand cannot collide with a real system's run.
     */
    public static IngestedEvent withoutCausality(String eventId, String sagaId, String eventType,
            String stepName, String serviceName, Instant eventTime, String payload,
            Instant receivedAt) {
        return new IngestedEvent(
                new Identity(ExecutionKey.DEFAULT_SOURCE,
                        ExecutionKey.of(ExecutionKey.DEFAULT_SOURCE, sagaId),
                        eventId, null, CorrelationBasis.DECLARED_KEY),
                null, eventType, stepName, serviceName, eventTime, payload, receivedAt,
                Causality.NONE);
    }
}
