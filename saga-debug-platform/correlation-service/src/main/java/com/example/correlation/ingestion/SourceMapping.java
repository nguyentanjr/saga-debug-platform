package com.example.correlation.ingestion;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * How to read one external system's events into this platform's envelope.
 *
 * <p>Without this the platform can only ingest events that already use its own field
 * names — {@code sagaId}, {@code eventId}, {@code eventType}, {@code timestamp}. No
 * existing system emits those, so attaching the platform to one meant changing that
 * system's producers. A mapping states where each field lives in <em>their</em> payload
 * instead, so an unmodified system can be observed.
 *
 * <p>Paths are dotted and may descend into nested objects ({@code meta.causedBy}).
 * Nothing more expressive: a mapping that needs computation is a sign the source is
 * emitting something this platform should not be guessing at.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SourceMapping {

    private String name;
    private String topicPattern;
    private Fields fields = new Fields();

    /**
     * Source event type to the name the workflow specification uses.
     *
     * <p>Optional. Systems rarely name their events the way a specification written
     * about them does — {@code order.placed} against {@code ORDER_CREATED} — and asking
     * either side to rename is asking for a change this platform exists to avoid.
     */
    private Map<String, String> eventTypes = new LinkedHashMap<>();

    private Pattern compiledPattern;

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getTopicPattern() { return topicPattern; }
    public void setTopicPattern(String topicPattern) {
        this.topicPattern = topicPattern;
        this.compiledPattern = topicPattern != null ? Pattern.compile(topicPattern) : null;
    }

    public Fields getFields() { return fields; }
    public void setFields(Fields fields) { this.fields = fields != null ? fields : new Fields(); }

    public Map<String, String> getEventTypes() { return eventTypes; }
    public void setEventTypes(Map<String, String> eventTypes) {
        this.eventTypes = eventTypes != null ? eventTypes : new LinkedHashMap<>();
    }

    public boolean matches(String topic) {
        return compiledPattern != null && topic != null && compiledPattern.matcher(topic).matches();
    }

    /** The declared name for a source event type, or the type unchanged. */
    public String translateEventType(String sourceType) {
        if (sourceType == null) return null;
        return eventTypes.getOrDefault(sourceType, sourceType);
    }

    public Optional<String> eventId(JsonNode payload) { return read(payload, fields.getEventId()); }
    public Optional<String> executionId(JsonNode payload) { return read(payload, fields.getExecutionId()); }
    public Optional<String> eventType(JsonNode payload) {
        return read(payload, fields.getEventType()).map(this::translateEventType);
    }
    public Optional<String> stepName(JsonNode payload) { return read(payload, fields.getStepName()); }
    public Optional<String> serviceName(JsonNode payload) { return read(payload, fields.getServiceName()); }
    public Optional<String> eventTime(JsonNode payload) { return read(payload, fields.getEventTime()); }
    public Optional<String> causationId(JsonNode payload) { return read(payload, fields.getCausationId()); }
    public Optional<String> spanId(JsonNode payload) { return read(payload, fields.getSpanId()); }
    public Optional<String> parentSpanId(JsonNode payload) { return read(payload, fields.getParentSpanId()); }
    public Optional<String> traceId(JsonNode payload) { return read(payload, fields.getTraceId()); }

    /**
     * Follows a dotted path.
     *
     * @return empty when the path is undeclared, absent from this payload, or explicitly
     *         null — all three mean the same thing to a caller: this source does not
     *         provide the field for this event
     */
    static Optional<String> read(JsonNode payload, String path) {
        if (payload == null || path == null || path.isBlank()) {
            return Optional.empty();
        }

        JsonNode cursor = payload;
        for (String segment : path.split("\\.")) {
            if (cursor == null || !cursor.has(segment)) {
                return Optional.empty();
            }
            cursor = cursor.get(segment);
        }

        if (cursor == null || cursor.isNull() || cursor.isMissingNode()) {
            return Optional.empty();
        }
        String value = cursor.isValueNode() ? cursor.asText() : cursor.toString();
        return value.isBlank() ? Optional.empty() : Optional.of(value);
    }

    /**
     * Where each envelope field lives in the source payload.
     *
     * <p>Defaults are this platform's own field names, so a producer that already speaks
     * the envelope needs no mapping at all and an existing deployment is unaffected.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Fields {
        private String eventId = "eventId";
        /** The correlation key: what ties observations into one execution. */
        private String executionId = "sagaId";
        private String eventType = "eventType";
        private String eventTime = "timestamp";
        private String stepName = "stepName";
        private String serviceName = "serviceName";
        private String causationId = "causationId";
        private String spanId = "spanId";
        private String parentSpanId = "parentSpanId";
        private String traceId = "traceId";

        public String getEventId() { return eventId; }
        public void setEventId(String eventId) { this.eventId = eventId; }

        public String getExecutionId() { return executionId; }
        public void setExecutionId(String executionId) { this.executionId = executionId; }

        public String getEventType() { return eventType; }
        public void setEventType(String eventType) { this.eventType = eventType; }

        public String getEventTime() { return eventTime; }
        public void setEventTime(String eventTime) { this.eventTime = eventTime; }

        public String getStepName() { return stepName; }
        public void setStepName(String stepName) { this.stepName = stepName; }

        public String getServiceName() { return serviceName; }
        public void setServiceName(String serviceName) { this.serviceName = serviceName; }

        public String getCausationId() { return causationId; }
        public void setCausationId(String causationId) { this.causationId = causationId; }

        public String getSpanId() { return spanId; }
        public void setSpanId(String spanId) { this.spanId = spanId; }

        public String getParentSpanId() { return parentSpanId; }
        public void setParentSpanId(String parentSpanId) { this.parentSpanId = parentSpanId; }

        public String getTraceId() { return traceId; }
        public void setTraceId(String traceId) { this.traceId = traceId; }
    }

    /** The identity mapping: a source already speaking this platform's envelope. */
    public static SourceMapping identity() {
        SourceMapping mapping = new SourceMapping();
        mapping.setName("default");
        return mapping;
    }
}
