package com.example.saga.events;


public final class EventTraceHelper {

    private EventTraceHelper() {}
    public static <T extends BaseEvent> T withTraceContext(T event, String traceId, String spanId) {
        event.setTraceId(traceId);
        event.setSpanId(spanId);
        return event;
    }
}
