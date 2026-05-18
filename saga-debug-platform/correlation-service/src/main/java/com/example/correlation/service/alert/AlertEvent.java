package com.example.correlation.service.alert;

import java.time.Instant;
import java.util.List;

public record AlertEvent(
        Instant timestamp,
        String sagaId,
        String rcaStatus,
        List<String> primaryRootCauses,
        String severity,
        List<String> evidenceSummary) {
}
