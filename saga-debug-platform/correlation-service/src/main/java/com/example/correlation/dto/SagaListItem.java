package com.example.correlation.dto;

import com.example.correlation.domain.SagaStatus;

import java.time.Instant;

public class SagaListItem {
    private String sagaId;
    private String workflowType;
    private java.util.List<String> workflowConflicts = java.util.List.of();
    public java.util.List<String> getWorkflowConflicts() { return workflowConflicts; }
    public void setWorkflowConflicts(java.util.List<String> conflicts) { workflowConflicts = conflicts; }
    private SagaStatus status;
    private Instant startTime;
    private Instant endTime;
    private Long durationMs;
    private long eventCount;

    public String getSagaId() { return sagaId; }
    public void setSagaId(String sagaId) { this.sagaId = sagaId; }

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

    public long getEventCount() { return eventCount; }
    public void setEventCount(long eventCount) { this.eventCount = eventCount; }
}
