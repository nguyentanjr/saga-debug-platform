package com.example.correlation.dto;

import com.example.correlation.domain.*;

import java.time.Instant;
import java.util.List;

public class SagaDetailResponse {
    private String sagaId;
    private String workflowType;
    private SagaStatus status;
    private Instant startTime;
    private Instant endTime;
    private Long durationMs;
    private List<SagaEvent> events;
    private List<Anomaly> anomalies;
    private List<RcaResult> rcaResults;

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

    public List<SagaEvent> getEvents() { return events; }
    public void setEvents(List<SagaEvent> events) { this.events = events; }

    public List<Anomaly> getAnomalies() { return anomalies; }
    public void setAnomalies(List<Anomaly> anomalies) { this.anomalies = anomalies; }

    public List<RcaResult> getRcaResults() { return rcaResults; }
    public void setRcaResults(List<RcaResult> rcaResults) { this.rcaResults = rcaResults; }
}
