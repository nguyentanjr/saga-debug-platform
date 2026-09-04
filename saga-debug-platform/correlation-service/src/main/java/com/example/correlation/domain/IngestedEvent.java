package com.example.correlation.domain;

import java.time.Instant;

/**
 * One observation as it arrived, before anything has been correlated.
 *
 * <p>Replaces an eight-argument call into the correlation service. That signature was
 * already at the point where argument order was the only thing keeping the caller
 * honest, and causal fields would have taken it to eleven.
 *
 * @param eventTime  when the work happened, per the producer; not when it was delivered
 * @param receivedAt when this platform saw it
 * @param causationId  the event this one was produced in response to, or null. The
 *                     strongest signal available: an observed causal link rather than
 *                     one inferred from the workflow specification
 * @param spanId       this observation's own OpenTelemetry span, or null
 * @param parentSpanId the span that caused it, used when {@code causationId} is absent
 */
public record IngestedEvent(
        String eventId,
        String sagaId,
        String eventType,
        String stepName,
        String serviceName,
        Instant eventTime,
        String payload,
        Instant receivedAt,
        String causationId,
        String spanId,
        String parentSpanId) {

    /** An observation carrying no causal metadata, which is what most producers emit. */
    public static IngestedEvent withoutCausality(String eventId, String sagaId, String eventType,
            String stepName, String serviceName, Instant eventTime, String payload,
            Instant receivedAt) {
        return new IngestedEvent(eventId, sagaId, eventType, stepName, serviceName,
                eventTime, payload, receivedAt, null, null, null);
    }

    public boolean hasCausalLink() {
        return causationId != null || parentSpanId != null;
    }
}
