package com.example.saga.events;


public final class EventTraceHelper {

    private EventTraceHelper() {}
    public static <T extends BaseEvent> T withTraceContext(T event, String traceId, String spanId) {
        event.setTraceId(traceId);
        event.setSpanId(spanId);
        return event;
    }

    /**
     * Copies the correlation key and the causal link from the event being responded to.
     *
     * <p>Every producer here already copied the saga id by hand; doing the causal link
     * in the same place keeps the two from drifting apart, which is how a chain ends up
     * half-populated.
     */
    public static <T extends BaseEvent> T inResponseTo(T event, BaseEvent cause) {
        if (cause != null) {
            event.setSagaId(cause.getSagaId());
            event.setCausationId(cause.getEventId());
            event.setParentSpanId(cause.getSpanId());
        }
        return event;
    }
}
