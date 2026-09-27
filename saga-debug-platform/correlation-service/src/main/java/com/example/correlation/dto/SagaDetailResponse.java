package com.example.correlation.dto;

import com.example.correlation.domain.*;

import java.time.Instant;
import java.util.List;

public class SagaDetailResponse {
    private String sagaId;

    /**
     * The observed system this run belongs to; the namespace half of {@code sagaId}.
     *
     * <p>Sent separately as well as inside the key so a client can group and filter by
     * system without parsing identifiers.
     */
    private String sourceName;

    /**
     * Every identifier this execution answers to, each with the observation that revealed
     * it.
     *
     * <p>A run assembled from services that each knew a different id is a claim, and a
     * report that makes claims has to show its working. Without this the merged execution
     * looks like a single id that every service happened to agree on, which is the one
     * thing it is not.
     */
    private List<IdentityLink> identities;
    private String workflowType;
    private List<String> workflowConflicts = List.of();
    /**
     * Stored event id -> the step its workflow places it in, for events the workflow knows.
     * Empty while the workflow is disputed. The stored stepName is what the source sent (or
     * the ingestion placeholder), and is left as it was (L27).
     */
    private java.util.Map<String, String> workflowSteps = java.util.Map.of();
    public java.util.Map<String, String> getWorkflowSteps() { return workflowSteps; }
    public void setWorkflowSteps(java.util.Map<String, String> steps) { workflowSteps = steps; }
    public List<String> getWorkflowConflicts() { return workflowConflicts; }
    public void setWorkflowConflicts(List<String> conflicts) { workflowConflicts = conflicts; }
    private SagaStatus status;
    private Instant startTime;
    private Instant endTime;
    private Long durationMs;
    private List<SagaEvent> events;
    private List<Anomaly> anomalies;
    private List<RcaResult> rcaResults;

    public String getSagaId() { return sagaId; }
    public void setSagaId(String sagaId) { this.sagaId = sagaId; }

    public String getSourceName() { return sourceName; }
    public void setSourceName(String sourceName) { this.sourceName = sourceName; }

    public List<IdentityLink> getIdentities() { return identities; }
    public void setIdentities(List<IdentityLink> identities) { this.identities = identities; }

    /**
     * One identifier this execution is known by.
     *
     * @param identifier the value as the observed system published it
     * @param source     the system whose namespace it belongs to
     * @param linkedByEventId the observation that carried this identifier alongside
     *                        another, which is the evidence for the link; null for the
     *                        execution's own declared key
     */
    public record IdentityLink(String identifier, String source, String linkedByEventId) {
    }

    public String getWorkflowType() { return workflowType; }
    public void setWorkflowType(String workflowType) { this.workflowType = workflowType; }

    public SagaStatus getStatus() { return status; }
    public void setStatus(SagaStatus status) { this.status = status; }

    public Instant getStartTime() { return startTime; }
    public void setStartTime(Instant startTime) { this.startTime = startTime; }

    public Instant getEndTime() { return endTime; }
    public void setEndTime(Instant endTime) { this.endTime = endTime; }

    public Long getDurationMs() { return durationMs; }
    public void setDurationMs(Long durationMs) { this.durationMs = durationMs; }

    public List<SagaEvent> getEvents() { return events; }
    public void setEvents(List<SagaEvent> events) { this.events = events; }

    public List<Anomaly> getAnomalies() { return anomalies; }
    public void setAnomalies(List<Anomaly> anomalies) { this.anomalies = anomalies; }

    public List<RcaResult> getRcaResults() { return rcaResults; }
    public void setRcaResults(List<RcaResult> rcaResults) { this.rcaResults = rcaResults; }
}
