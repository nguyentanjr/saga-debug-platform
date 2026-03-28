package com.example.correlation.service;

import com.example.correlation.domain.*;
import com.example.correlation.repository.AnomalyRepository;
import com.example.correlation.repository.RcaResultRepository;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Root Cause Analysis Service.
 *
 * Architecture:
 *   AnomalyDetectionService = Phase 1 (DETECT) — flags anomalies like STUCK_SAGA, OUT_OF_ORDER
 *   RcaService              = Phase 2 (EXPLAIN) — aggregates anomalies + events → root cause + suggestion
 *
 * Rules are implemented as Java methods for simplicity.
 * In production, a YAML/DSL-based configuration would allow non-developer modification.
 */
@Service
public class RcaService {

    private static final Logger log = LoggerFactory.getLogger(RcaService.class);

    private final SagaInstanceRepository sagaInstanceRepository;
    private final SagaEventRepository sagaEventRepository;
    private final AnomalyRepository anomalyRepository;
    private final RcaResultRepository rcaResultRepository;
    private final ObjectMapper objectMapper;

    public RcaService(SagaInstanceRepository sagaInstanceRepository,
                      SagaEventRepository sagaEventRepository,
                      AnomalyRepository anomalyRepository,
                      RcaResultRepository rcaResultRepository,
                      ObjectMapper objectMapper) {
        this.sagaInstanceRepository = sagaInstanceRepository;
        this.sagaEventRepository = sagaEventRepository;
        this.anomalyRepository = anomalyRepository;
        this.rcaResultRepository = rcaResultRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public RcaResult analyze(String sagaId) {
        SagaInstance saga = sagaInstanceRepository.findById(sagaId).orElse(null);
        if (saga == null) {
            log.warn("Saga {} not found for RCA", sagaId);
            return null;
        }

        List<SagaEvent> events = sagaEventRepository.findBySagaIdOrderByTimestampAsc(sagaId);
        List<Anomaly> anomalies = anomalyRepository.findBySagaId(sagaId);

        // Phase 2: Explain — run rules in priority order, pick first match
        String rootCause = null;
        String severity = null;
        List<String> evidence = new ArrayList<>();
        List<String> suggestions = new ArrayList<>();

        // ---- CRITICAL rules ----
        if (rootCause == null) {
            rootCause = checkCircuitBreakerOpen(events, anomalies, evidence, suggestions);
            if (rootCause != null) severity = "CRITICAL";
        }

        if (rootCause == null) {
            rootCause = checkTimeoutRetryStorm(events, anomalies, evidence, suggestions);
            if (rootCause != null) severity = "CRITICAL";
        }

        // ---- HIGH rules ----
        if (rootCause == null) {
            rootCause = checkCompensationFailed(saga, events, evidence, suggestions);
            if (rootCause != null) severity = "HIGH";
        }

        if (rootCause == null) {
            rootCause = checkStuckSaga(saga, anomalies, evidence, suggestions);
            if (rootCause != null) severity = "HIGH";
        }

        // ---- MEDIUM rules ----
        if (rootCause == null) {
            rootCause = checkMissingCompletion(saga, events, anomalies, evidence, suggestions);
            if (rootCause != null) severity = "MEDIUM";
        }

        if (rootCause == null) {
            rootCause = checkOutOfOrder(anomalies, evidence, suggestions);
            if (rootCause != null) severity = "MEDIUM";
        }

        // ---- LOW rules ----
        if (rootCause == null) {
            rootCause = checkDuplicateEvent(anomalies, evidence, suggestions);
            if (rootCause != null) severity = "LOW";
        }

        // ---- Fallback ----
        if (rootCause == null) {
            if (saga.getStatus() == SagaStatus.COMPLETED) {
                rootCause = "HEALTHY";
                severity = "NONE";
                evidence.add("Saga completed successfully with " + events.size() + " events");
                if (saga.getStartTime() != null && saga.getEndTime() != null) {
                    long durationMs = Duration.between(saga.getStartTime(), saga.getEndTime()).toMillis();
                    evidence.add("Total duration: " + durationMs + "ms");
                }
            } else {
                rootCause = "UNKNOWN";
                severity = "LOW";
                evidence.add("No matching RCA rules. Saga status: " + saga.getStatus());
                evidence.add("Events count: " + events.size());
                suggestions.add("Review saga events manually for anomalies");
            }
        }

        // Collect trace IDs for Jaeger links
        List<String> traceIds = events.stream()
                .map(e -> extractTraceId(e.getPayload()))
                .filter(Objects::nonNull)
                .distinct()
                .collect(Collectors.toList());

        if (!traceIds.isEmpty()) {
            evidence.add("traceIds: " + traceIds);
            suggestions.add("View traces: " + traceIds.stream()
                    .map(tid -> "http://localhost:16686/trace/" + tid)
                    .collect(Collectors.joining(", ")));
        }

        // Save RCA result
        try {
            RcaResult result = new RcaResult();
            result.setSagaId(sagaId);
            result.setRootCause(rootCause);
            result.setSeverity(severity);
            result.setEvidence(objectMapper.writeValueAsString(evidence));
            result.setSuggestions(objectMapper.writeValueAsString(suggestions));
            rcaResultRepository.save(result);

            log.info("RCA for saga {}: rootCause={}, severity={}", sagaId, rootCause, severity);
            return result;
        } catch (Exception e) {
            log.error("Failed to save RCA result for saga {}: {}", sagaId, e.getMessage());
            return null;
        }
    }

    public List<RcaResult> getResults(String sagaId) {
        return rcaResultRepository.findBySagaIdOrderByCreatedAtDesc(sagaId);
    }

    // =====================================================================
    // Hard-coded RCA Rules — ordered by priority
    // In production, these would be externalized to YAML/DSL configuration.
    // =====================================================================

    /**
     * CIRCUIT_BREAKER_OPEN: Payment failed with circuit breaker message.
     * Condition: PAYMENT_FAILED event with "circuit breaker" or "retry exhausted" in payload.
     */
    private String checkCircuitBreakerOpen(List<SagaEvent> events, List<Anomaly> anomalies,
                                           List<String> evidence, List<String> suggestions) {
        for (SagaEvent event : events) {
            if ("PAYMENT_FAILED".equals(event.getEventType())) {
                String payload = event.getPayload() != null ? event.getPayload().toLowerCase() : "";
                if (payload.contains("circuit breaker") || payload.contains("retry exhausted")) {
                    evidence.add("Payment failed with circuit breaker: " + event.getEventType());
                    evidence.add("Payload indicates resilience pattern triggered");
                    suggestions.add("Investigate downstream service health (payment gateway)");
                    suggestions.add("Check Resilience4j metrics at /actuator/circuitbreakers");
                    return "CIRCUIT_BREAKER_OPEN";
                }
            }
        }
        return null;
    }

    /**
     * TIMEOUT_RETRY_STORM: Detected when saga has multiple events of the same type
     * with short intervals, suggesting rapid retries.
     */
    private String checkTimeoutRetryStorm(List<SagaEvent> events, List<Anomaly> anomalies,
                                          List<String> evidence, List<String> suggestions) {
        // Count events by type
        Map<String, Long> eventTypeCounts = events.stream()
                .collect(Collectors.groupingBy(SagaEvent::getEventType, Collectors.counting()));

        for (Map.Entry<String, Long> entry : eventTypeCounts.entrySet()) {
            if (entry.getValue() >= 3) {
                evidence.add("Event type " + entry.getKey() + " appears " + entry.getValue() + " times (possible retry storm)");
                suggestions.add("Reduce retry count or increase backoff in Resilience4j config");
                suggestions.add("Check service logs for timeout root cause");
                return "TIMEOUT_RETRY_STORM";
            }
        }
        return null;
    }

    /**
     * COMPENSATION_FAILED: Saga is in COMPENSATING status but has no PAYMENT_REFUNDED event.
     * The compensation flow did not complete.
     */
    private String checkCompensationFailed(SagaInstance saga, List<SagaEvent> events,
                                           List<String> evidence, List<String> suggestions) {
        if (saga.getStatus() == SagaStatus.COMPENSATING) {
            boolean hasRefund = events.stream()
                    .anyMatch(e -> "PAYMENT_REFUNDED".equals(e.getEventType()));
            if (!hasRefund) {
                evidence.add("Saga stuck in COMPENSATING status — no PAYMENT_REFUNDED event received");
                evidence.add("Expected compensation step did not complete");
                suggestions.add("Check payment-service logs for refund processing errors");
                suggestions.add("Verify Kafka connectivity for refund command delivery");
                return "COMPENSATION_FAILED";
            }
        }
        return null;
    }

    /**
     * STUCK_SAGA: Saga has STUCK_SAGA anomaly — detected by AnomalyDetectionService.
     */
    private String checkStuckSaga(SagaInstance saga, List<Anomaly> anomalies,
                                  List<String> evidence, List<String> suggestions) {
        for (Anomaly anomaly : anomalies) {
            if ("STUCK_SAGA".equals(anomaly.getAnomalyType())) {
                evidence.add("Anomaly detected: " + anomaly.getDescription());
                evidence.add("Saga has been in " + saga.getStatus() + " status too long");
                suggestions.add("Check if consuming service is running and processing events");
                suggestions.add("Verify Kafka consumer group offsets");
                return "STUCK_SAGA";
            }
        }
        return null;
    }

    /**
     * MISSING_COMPLETION: Saga never reached COMPLETED/FAILED but has anomalies.
     */
    private String checkMissingCompletion(SagaInstance saga, List<SagaEvent> events,
                                          List<Anomaly> anomalies, List<String> evidence,
                                          List<String> suggestions) {
        for (Anomaly anomaly : anomalies) {
            if ("MISSING_COMPLETION".equals(anomaly.getAnomalyType())) {
                evidence.add("Anomaly detected: " + anomaly.getDescription());
                evidence.add("Events received: " + events.stream()
                        .map(SagaEvent::getEventType)
                        .collect(Collectors.joining(" → ")));
                suggestions.add("Check if the next service in the saga workflow is running");
                return "MISSING_COMPLETION";
            }
        }
        return null;
    }

    /**
     * OUT_OF_ORDER: Events arrived out of expected workflow order.
     */
    private String checkOutOfOrder(List<Anomaly> anomalies, List<String> evidence,
                                   List<String> suggestions) {
        for (Anomaly anomaly : anomalies) {
            if ("OUT_OF_ORDER".equals(anomaly.getAnomalyType())) {
                evidence.add("Anomaly detected: " + anomaly.getDescription());
                suggestions.add("Check Kafka partition ordering and consumer processing");
                return "OUT_OF_ORDER";
            }
        }
        return null;
    }

    /**
     * DUPLICATE_EVENT: Same event was received multiple times.
     */
    private String checkDuplicateEvent(List<Anomaly> anomalies, List<String> evidence,
                                       List<String> suggestions) {
        for (Anomaly anomaly : anomalies) {
            if ("DUPLICATE_EVENT".equals(anomaly.getAnomalyType())) {
                evidence.add("Anomaly detected: " + anomaly.getDescription());
                suggestions.add("Verify producer idempotency and consumer deduplication");
                return "DUPLICATE_EVENT";
            }
        }
        return null;
    }

    // =====================================================================
    // Utility
    // =====================================================================

    private String extractTraceId(String payloadJson) {
        if (payloadJson == null || payloadJson.isBlank()) return null;
        try {
            var node = objectMapper.readTree(payloadJson);
            if (node.has("traceId") && !node.get("traceId").isNull()) {
                return node.get("traceId").asText();
            }
        } catch (Exception e) {
            // Not valid JSON or no traceId
        }
        return null;
    }
}
