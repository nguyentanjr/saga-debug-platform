package com.example.correlation.service.scenario;

import java.util.List;

public record ScenarioRunResponse(
        String scenarioId,
        String name,
        String sagaId,
        String orderId,
        String expectedOutcome,
        String expectedRca,
        List<String> expectedAnomalies,
        List<String> notes) {
}
