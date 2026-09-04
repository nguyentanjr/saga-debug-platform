package com.example.saga.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;
import java.util.UUID;


@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class BaseEvent {

    private String eventId;

    private String eventType;

    private UUID sagaId;

    private Instant timestamp;

    private String stepName;

    private String serviceName;

    private String traceId;

    private String spanId;

    /**
     * The event this one was produced in response to.
     *
     * <p>Turns a sequence of observations into a chain of stated causes. Without it a
     * consumer of this stream can only infer what led to what, from a specification
     * describing what usually happens or from the order things arrived — neither of
     * which survives parallelism, retries or a lost message.
     *
     * <p>Null on an event that starts an execution, and on anything a producer emits
     * without having been triggered.
     */
    private String causationId;

    /** OpenTelemetry equivalent of {@link #causationId}, for consumers that use spans. */
    private String parentSpanId;


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

    public String getCausationId() { return causationId; }
    public void setCausationId(String causationId) { this.causationId = causationId; }

    public String getParentSpanId() { return parentSpanId; }
    public void setParentSpanId(String parentSpanId) { this.parentSpanId = parentSpanId; }

    /**
     * Marks this event as the response to {@code cause}, carrying the causal chain
     * forward.
     *
     * @return this event, for use at the point of construction
     */
    public <T extends BaseEvent> T causedBy(BaseEvent cause) {
        if (cause != null) {
            this.causationId = cause.getEventId();
            this.parentSpanId = cause.getSpanId();
        }
        @SuppressWarnings("unchecked")
        T self = (T) this;
        return self;
    }
}
