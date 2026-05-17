package com.example.correlation.domain.rca;

import java.util.List;

public record Evidence(
        String fact,
        String observedValue,
        String expectedValue,
        EvidenceSource source,
        Long anomalyId,
        List<String> eventIds) {

    public Evidence {
        eventIds = eventIds == null ? List.of() : List.copyOf(eventIds);
    }

    public static Evidence of(String fact, String observedValue, String expectedValue,
            EvidenceSource source, Long anomalyId, List<String> eventIds) {
        return new Evidence(fact, observedValue, expectedValue, source, anomalyId, eventIds);
    }

    public static Evidence of(String fact, EvidenceSource source, Long anomalyId) {
        return new Evidence(fact, null, null, source, anomalyId, List.of());
    }

    public static Evidence ofEvent(String fact, String observedValue, String expectedValue,
            EvidenceSource source, String eventId) {
        return new Evidence(fact, observedValue, expectedValue, source, null, List.of(eventId));
    }
}
