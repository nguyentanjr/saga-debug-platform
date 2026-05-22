package com.example.correlation.service.rca;

import com.example.correlation.domain.Anomaly;
import com.example.correlation.domain.AnomalyType;
import com.example.correlation.domain.rca.CandidateAssessment;

import java.util.List;

public class SelectionContext {

    private final List<CandidateAssessment> candidates;
    private final List<Anomaly> anomalies;

    public SelectionContext(List<CandidateAssessment> candidates, List<Anomaly> anomalies) {
        this.candidates = candidates != null ? candidates : List.of();
        this.anomalies = anomalies != null ? anomalies : List.of();
    }

    public boolean hasAnomaly(AnomalyType anomalyType) {
        return anomalies.stream().anyMatch(a -> a.getAnomalyType() == anomalyType);
    }

    public boolean hasAnyNonFallbackCandidate() {
        return candidates.stream()
                .anyMatch(c -> !"R20".equals(c.getRuleId()) && !"R21".equals(c.getRuleId()));
    }
}
