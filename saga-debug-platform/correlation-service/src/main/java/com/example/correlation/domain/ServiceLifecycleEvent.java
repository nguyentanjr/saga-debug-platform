package com.example.correlation.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

/**
 * One fact the container runtime reported about a service of the observed system: its
 * process exited, or it was started.
 *
 * <p>This is the only evidence the platform has that does not come from the saga's own
 * events. It exists for one question those events cannot answer: when a run went silent,
 * was the service it was waiting on actually down? See R23.
 */
@Entity
@Table(name = "service_lifecycle_events")
public class ServiceLifecycleEvent {

    public enum State { DOWN, UP }

    /**
     * How the state was established. PROCESS: the runtime managing the process reported it.
     * PROBE: something outside could or could not reach the service, which a network
     * partition also produces. See V20.
     */
    public enum Basis { PROCESS, PROBE }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /** The service as the workflow names it (step {@code service:}). */
    @Column(name = "service_name", nullable = false)
    private String serviceName;

    @Enumerated(EnumType.STRING)
    @Column(name = "state", nullable = false)
    private State state;

    /** When the runtime says it happened. */
    @Column(name = "occurred_at", nullable = false)
    private Instant occurredAt;

    @Column(name = "received_at", nullable = false)
    private Instant receivedAt;

    /** Which runtime reported it, e.g. {@code docker}. */
    @Column(name = "source", nullable = false)
    private String source;

    /** The runtime's own wording, kept for the evidence text: {@code die exitCode=137}. */
    @Column(name = "detail")
    private String detail;

    @Column(name = "external_id", nullable = false)
    private String externalId;

    @Enumerated(EnumType.STRING)
    @Column(name = "basis", nullable = false)
    private Basis basis = Basis.PROCESS;

    protected ServiceLifecycleEvent() {
    }

    public ServiceLifecycleEvent(String serviceName, State state, Instant occurredAt,
            String source, String detail, String externalId) {
        this.serviceName = serviceName;
        this.state = state;
        this.occurredAt = occurredAt;
        this.receivedAt = Instant.now();
        this.source = source;
        this.detail = detail;
        this.externalId = externalId;
    }

    public ServiceLifecycleEvent(String serviceName, State state, Instant occurredAt,
            String source, String detail, String externalId, Basis basis) {
        this(serviceName, state, occurredAt, source, detail, externalId);
        this.basis = basis;
    }

    public Long getId() { return id; }
    public String getServiceName() { return serviceName; }
    public State getState() { return state; }
    public Instant getOccurredAt() { return occurredAt; }
    public Instant getReceivedAt() { return receivedAt; }
    public String getSource() { return source; }
    public String getDetail() { return detail; }
    public String getExternalId() { return externalId; }
    public Basis getBasis() { return basis; }
}
