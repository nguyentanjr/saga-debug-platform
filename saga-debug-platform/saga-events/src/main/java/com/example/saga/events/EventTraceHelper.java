package com.example.saga.events;

/**
 * Utility to populate trace context fields into events.
 * Services pass the current trace/span IDs when publishing events.
 */
public final class EventTraceHelper {

    private EventTraceHelper() {}

    /**
     * Enrich event with trace context from the current OpenTelemetry span.
     * Call this in producers right before publishing to Kafka.
     */
    public static <T extends BaseEvent> T withTraceContext(T event, String traceId, String spanId) {
        event.setTraceId(traceId);
        event.setSpanId(spanId);
        return event;
    }
}
