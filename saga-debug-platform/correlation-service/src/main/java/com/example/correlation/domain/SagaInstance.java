package com.example.correlation.domain;

import jakarta.persistence.*;
import java.time.Instant;

@Entity
@Table(name = "saga_instances")
public class SagaInstance {

    @Id
    @Column(name = "saga_id")
    private String sagaId;

    /**
     * The workflow this execution belongs to, resolved from its first event.
     *
     * <p>Defaulted to ORDER_SAGA before the registry existed, which meant an execution
     * of any other workflow — or one whose first event was never recognised — was
     * silently evaluated against the order saga's steps, timeouts and transitions.
     * {@link #UNRESOLVED} makes that state explicit: no spec is found for it, so the
     * spec-driven detectors abstain instead of measuring against the wrong workflow.
     */
    public static final String UNRESOLVED = "UNRESOLVED";

    @Column(name = "workflow_type", nullable = false)
    private String workflowType = UNRESOLVED;

    @Column(name = "order_id")
    private String orderId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private SagaStatus status;

    @Column(name = "start_time", nullable = false)
    private Instant startTime;

    @Column(name = "end_time")
    private Instant endTime;

    @Column(name = "last_updated", nullable = false)
    private Instant lastUpdated;

    /**
     * Optimistic lock. Guards the read-modify-write of {@code status} in processEvent
     * against concurrent writers; a losing writer gets an exception instead of having
     * its update silently overwritten.
     */
    @Version
    @Column(nullable = false)
    private long version;

    public String getSagaId() { return sagaId; }
    public void setSagaId(String sagaId) { this.sagaId = sagaId; }

    public String getWorkflowType() { return workflowType; }
    public void setWorkflowType(String workflowType) { this.workflowType = workflowType; }

    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }

    public SagaStatus getStatus() { return status; }
    public void setStatus(SagaStatus status) { this.status = status; }

    public Instant getStartTime() { return startTime; }
    public void setStartTime(Instant startTime) { this.startTime = startTime; }

    public Instant getEndTime() { return endTime; }
    public void setEndTime(Instant endTime) { this.endTime = endTime; }

    public Instant getLastUpdated() { return lastUpdated; }
    public void setLastUpdated(Instant lastUpdated) { this.lastUpdated = lastUpdated; }

    public long getVersion() { return version; }
    public void setVersion(long version) { this.version = version; }
}
