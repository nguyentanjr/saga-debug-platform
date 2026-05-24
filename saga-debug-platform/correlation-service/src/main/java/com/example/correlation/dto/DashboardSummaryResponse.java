package com.example.correlation.dto;

import java.time.Instant;
import java.util.List;

public record DashboardSummaryResponse(
        int windowHours,
        Instant since,
        long totalSagas,
        long completedSagas,
        long failedSagas,
        long compensatedSagas,
        long activeSagas,
        long totalAnomalies,
        long rcaSelected,
        long rcaInsufficientEvidence,
        long rcaNoDiagnosisNeeded,
        List<MetricItem> topAnomalyTypes,
        List<MetricItem> topRootCauses,
        List<MetricItem> topAffectedSteps) {
}
