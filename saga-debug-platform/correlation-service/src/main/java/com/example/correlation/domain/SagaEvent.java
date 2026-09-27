package com.example.correlation.domain;

import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;

@Entity
@Table(name = "saga_events")
public class SagaEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false, unique = true)
    private String eventId;

    /**
     * The namespaced execution key: {@code source::rawId}. See {@link ExecutionKey}.
     *
     * <p>Not the identifier the producer published. Two observed systems are two id
     * spaces, and filing both under a raw string merged unrelated runs into one.
     */
    @Column(name = "saga_id", nullable = false)
    private String sagaId;

    /**
     * The mapping that read this observation.
     *
     * <p>Redundant with the prefix of {@code sagaId} on purpose: it is what queries filter
     * and group by, and a CHECK constraint ties the two together so the invariant holds
     * for writers that never went through {@link ExecutionKey}.
     */
    @Column(name = "source_name", nullable = false)
    private String sourceName;

    /**
     * On what grounds this observation was filed under {@code sagaId}.
     *
     * <p>Every finding downstream is a statement about a sequence, and the sequence is
     * only as sound as the correlation that built it. Recording the basis keeps a link the
     * platform derived from reading like an identifier the producer stamped.
     */
    @Enumerated(EnumType.STRING)
    @Column(name = "correlation_basis")
    private CorrelationBasis correlationBasis;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Column(name = "step_name")
    private String stepName;

    @Column(name = "service_name")
    private String serviceName;

    @Column(nullable = false)
    private Instant timestamp;

    /**
     * Origin of timestamp, not evidence of clock accuracy. DECLARED includes mapped
     * headers as well as payload fields. Null means the origin was not recorded (legacy
     * rows/callers); equality with receivedAt does not establish an origin.
     */
    @Enumerated(EnumType.STRING)
    @Column(name = "event_time_source", length = 20)
    private EventTimeSource eventTimeSource;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(nullable = false, columnDefinition = "jsonb")
    private String payload;

    @Column(name = "received_at", nullable = false)
    private Instant receivedAt = Instant.now();

    /**
     * The event this one was produced in response to, when the producer said so.
     *
     * <p>An observed causal link, as opposed to one inferred from the workflow
     * specification. Null when the producer does not report it, which is the only case
     * the platform supported before.
     */
    @Column(name = "causation_id")
    private String causationId;

    @Column(name = "span_id")
    private String spanId;

    /** OpenTelemetry equivalent of {@code causationId}, used when that is absent. */
    @Column(name = "parent_span_id")
    private String parentSpanId;

    /**
     * The trace this event belongs to, as the source's mapping said to read it.
     *
     * <p>Stored rather than re-derived from the payload, because deriving it meant
     * looking for a field named {@code traceId} — this platform's own name for it. A
     * source that keeps its trace id elsewhere reported no trace at all.
     */
    @Column(name = "trace_id")
    private String traceId;

    /**
     * The transport's own name for the record that carried this observation, or null.
     *
     * <p>For Kafka, {@code topic-partition-offset}: it identifies a position in a log, not
     * a business event, and that is exactly what makes it useful here. Re-reading a topic
     * presents the same position again; a producer that published an event twice occupies
     * two. Without it the platform could see that an event id had arrived before but not
     * whether it had arrived <em>from the same record</em>, and reported its own replay as
     * the observed system's duplicate delivery.
     *
     * <p>Null when the transport offers no identity — HTTP ingestion, and rows written
     * before this was recorded. Null is read as "cannot tell", never as "replay".
     */
    @Column(name = "transport_ref")
    private String transportRef;

    public String getCausationId() { return causationId; }
    public void setCausationId(String causationId) { this.causationId = causationId; }

    public String getSpanId() { return spanId; }
    public void setSpanId(String spanId) { this.spanId = spanId; }

    public String getParentSpanId() { return parentSpanId; }
    public void setParentSpanId(String parentSpanId) { this.parentSpanId = parentSpanId; }

    public String getTraceId() { return traceId; }
    public void setTraceId(String traceId) { this.traceId = traceId; }

    public String getTransportRef() { return transportRef; }
    public void setTransportRef(String transportRef) { this.transportRef = transportRef; }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }

    public String getSagaId() { return sagaId; }
    public void setSagaId(String sagaId) { this.sagaId = sagaId; }

    public String getSourceName() { return sourceName; }
    public void setSourceName(String sourceName) { this.sourceName = sourceName; }

    public CorrelationBasis getCorrelationBasis() { return correlationBasis; }
    public void setCorrelationBasis(CorrelationBasis basis) { this.correlationBasis = basis; }

    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }

    public String getStepName() { return stepName; }
    public void setStepName(String stepName) { this.stepName = stepName; }

    public String getServiceName() { return serviceName; }
    public void setServiceName(String serviceName) { this.serviceName = serviceName; }

    public Instant getTimestamp() { return timestamp; }
    public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }

    public EventTimeSource getEventTimeSource() { return eventTimeSource; }
    public void setEventTimeSource(EventTimeSource eventTimeSource) { this.eventTimeSource = eventTimeSource; }

    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }

    public Instant getReceivedAt() { return receivedAt; }
    public void setReceivedAt(Instant receivedAt) { this.receivedAt = receivedAt; }
}
