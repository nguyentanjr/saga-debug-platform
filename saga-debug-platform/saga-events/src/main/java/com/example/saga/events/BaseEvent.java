package com.example.saga.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;
import java.util.UUID;

/**
 * Base event schema for all saga business events.
 * Follows business-events-spec: every event MUST have these fields.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class BaseEvent {

    /** Unique identifier for this event instance (UUID v4) */
    private String eventId;

    /** Event type enum string, e.g. ORDER_CREATED, PAYMENT_COMPLETED */
    private String eventType;

    /** Saga instance identifier — links all events in one saga */
    private UUID sagaId;

    /** ISO 8601 timestamp of when the event was created */
    private Instant timestamp;

    /** Name of the saga step, e.g. CREATE_ORDER, PROCESS_PAYMENT */
    private String stepName;

    /** Name of the service that produced this event */
    private String serviceName;

    /** W3C trace context — trace ID from OpenTelemetry */
    private String traceId;

    /** W3C trace context — span ID from OpenTelemetry */
    private String spanId;

    // --- Constructors ---

    protected BaseEvent() {
        this.eventId = UUID.randomUUID().toString();
        this.timestamp = Instant.now();
    }

    protected BaseEvent(String eventType, String stepName, String serviceName) {
        this();
        this.eventType = eventType;
        this.stepName = stepName;
        this.serviceName = serviceName;
    }

    // --- Getters & Setters ---

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }

    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }

    public UUID getSagaId() { return sagaId; }
    public void setSagaId(UUID sagaId) { this.sagaId = sagaId; }

    public Instant getTimestamp() { return timestamp; }
    public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }

    public String getStepName() { return stepName; }
    public void setStepName(String stepName) { this.stepName = stepName; }

    public String getServiceName() { return serviceName; }
    public void setServiceName(String serviceName) { this.serviceName = serviceName; }

    public String getTraceId() { return traceId; }
    public void setTraceId(String traceId) { this.traceId = traceId; }

    public String getSpanId() { return spanId; }
    public void setSpanId(String spanId) { this.spanId = spanId; }
}
