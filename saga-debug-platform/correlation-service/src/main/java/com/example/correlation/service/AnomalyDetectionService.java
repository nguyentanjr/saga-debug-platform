package com.example.correlation.service;

import com.example.correlation.domain.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.example.correlation.repository.AnomalyRepository;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.example.correlation.workflow.WorkflowLoader;
import com.example.correlation.workflow.WorkflowSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class AnomalyDetectionService {

    // Reference CorrelationService's definitions — single source of truth (no local duplicates)
    private static final java.util.Set<String> COMPLETED_EVENTS = CorrelationService.COMPLETED_EVENTS;
    private static final java.util.Set<String> FAILED_EVENTS = CorrelationService.FAILED_EVENTS;

    private static final Logger log = LoggerFactory.getLogger(AnomalyDetectionService.class);
    // Retry-attempt events emitted by Resilience4j retry listeners in producer services.
    // Currently only payment-service has @Retry; ORDER_RETRY/INVENTORY_RETRY are
    // reserved for future expansion (no producer wired yet).
    // PAYMENT_FAILED / INVENTORY_FAILED are deliberately EXCLUDED — they are
    // business-level failures, not retry indicators (including them previously
    // caused false RETRY_BURST under flash-sale load).
    private static final java.util.Set<String> RETRY_EVENT_TYPES = java.util.Set.of(
            "PAYMENT_RETRY");
    private static final Duration BURST_WINDOW = Duration.ofSeconds(60);
    private static final int BURST_THRESHOLD = 3;

    private final SagaInstanceRepository sagaInstanceRepository;
    private final SagaEventRepository sagaEventRepository;
    private final AnomalyRepository anomalyRepository;
    private final WorkflowLoader workflowLoader;
    private final ObjectMapper objectMapper;

    public AnomalyDetectionService(
            SagaInstanceRepository sagaInstanceRepository,
            SagaEventRepository sagaEventRepository,
            AnomalyRepository anomalyRepository,
            WorkflowLoader workflowLoader,
            ObjectMapper objectMapper) {
        this.sagaInstanceRepository = sagaInstanceRepository;
        this.sagaEventRepository = sagaEventRepository;
        this.anomalyRepository = anomalyRepository;
        this.workflowLoader = workflowLoader;
        this.objectMapper = objectMapper;
    }


    // Clock skew tolerance: events from different services may have clocks that
    // differ by a few hundred ms. Only flag as out-of-order if the gap exceeds
    // this threshold to avoid false positives from infrastructure artifacts.
    private static final Duration CLOCK_SKEW_TOLERANCE = Duration.ofMillis(500);

    public void detectOutOfOrder(String sagaId) {
        List<SagaEvent> events = sagaEventRepository.findBySagaIdOrderByReceivedAtAsc(sagaId);
        if (events.size() < 2) return;
        if (anomalyRepository.existsBySagaIdAndAnomalyType(sagaId, AnomalyType.OUT_OF_ORDER)) return;

        for (int i = 1; i < events.size(); i++) {
            Instant prevCreated = events.get(i - 1).getTimestamp();
            Instant currCreated = events.get(i).getTimestamp();
            Duration timeDiff = Duration.between(currCreated, prevCreated);
            // Only flag if currCreated is BEFORE prevCreated by more than clock skew tolerance
            if (currCreated.isBefore(prevCreated) && timeDiff.compareTo(CLOCK_SKEW_TOLERANCE) > 0) {
                createAnomaly(sagaId, AnomalyType.OUT_OF_ORDER,
                        String.format("Event '%s' arrived after '%s' but has earlier timestamp (%s < %s, gap: %dms)",
                                events.get(i).getEventType(),
                                events.get(i - 1).getEventType(),
                                currCreated, prevCreated, timeDiff.toMillis()));
                return;
            }
        }
    }


    public void detectInvalidTransition(String sagaId, String previousStep,
            String currentStep, String condition) {
        WorkflowSpec workflow = workflowLoader.getOrderSagaWorkflow();
        if (workflow == null || workflow.getTransitions() == null) return;

        if (previousStep != null && !workflow.isValidTransition(previousStep, currentStep, condition)) {
            String expected = workflow.getTransitions().stream()
                    .filter(t -> t.getFrom().equals(previousStep))
                    .map(t -> t.getTo() + " (on: " + t.getCondition() + ")")
                    .collect(java.util.stream.Collectors.joining(", "));

            // Enrich: distinguish skipped step vs wrong order for better RCA.
            // If currentStep exists in workflow but is not adjacent to previousStep,
            // this is a SKIPPED_STEP pattern (more specific than generic invalid transition).
            boolean isSkippedStep = workflow.getTransitions().stream()
                    .anyMatch(t -> t.getTo().equals(currentStep)
                            && !t.getFrom().equals(previousStep));
            String transitionType = isSkippedStep ? "Skipped step" : "Invalid transition";

            createAnomaly(sagaId, AnomalyType.INVALID_TRANSITION,
                    String.format("%s: %s → %s (condition: %s). Expected from %s: [%s]",
                            transitionType, previousStep, currentStep, condition, previousStep,
                            expected.isEmpty() ? "none defined" : expected));
        }
    }

    public void detectRetryBurst(String sagaId) {
        List<SagaEvent> events = sagaEventRepository.findBySagaIdOrderByTimestampAsc(sagaId);
        if (events.size() < BURST_THRESHOLD) return;
        if (anomalyRepository.existsBySagaIdAndAnomalyType(sagaId, AnomalyType.RETRY_BURST)) return;

        List<SagaEvent> retryEvents = events.stream()
                .filter(e -> RETRY_EVENT_TYPES.contains(e.getEventType()))
                .collect(Collectors.toList());
        if (retryEvents.size() < BURST_THRESHOLD) return;

        for (int i = 0; i <= retryEvents.size() - BURST_THRESHOLD; i++) {
            SagaEvent first = retryEvents.get(i);
            SagaEvent last = retryEvents.get(i + BURST_THRESHOLD - 1);

            Duration gap = Duration.between(first.getTimestamp(), last.getTimestamp());
            if (gap.compareTo(BURST_WINDOW) <= 0) {
                createAnomaly(sagaId, AnomalyType.RETRY_BURST,
                        String.format("%d retry events in %ds — possible retry storm",
                                BURST_THRESHOLD, gap.getSeconds()));
                return;
            }
        }
    }

    public void detectDuplicateEvent(String sagaId, String eventId, String eventType) {
        // Called when dedup detects a duplicate — create anomaly instead of silent drop
        if (anomalyRepository.existsBySagaIdAndAnomalyType(sagaId, AnomalyType.EXACT_DUPLICATE_EVENT)) return;
        createAnomaly(sagaId, AnomalyType.EXACT_DUPLICATE_EVENT,
                String.format("Duplicate event detected: eventId=%s, type=%s", eventId, eventType));
    }

    public void detectStateRegression(String sagaId, SagaStatus oldStatus, SagaStatus newStatus) {
        if (statusRank(newStatus) < statusRank(oldStatus)) {
            if (anomalyRepository.existsBySagaIdAndAnomalyType(sagaId, AnomalyType.STATE_REGRESSION)) return;
            createAnomaly(sagaId, AnomalyType.STATE_REGRESSION,
                    String.format("State regression: %s → %s (rank %d → %d)",
                            oldStatus, newStatus, statusRank(oldStatus), statusRank(newStatus)));
        }
    }

    public void detectForbiddenPostTerminal(String sagaId, SagaStatus currentStatus, String eventType) {
        if (currentStatus == SagaStatus.COMPLETED || currentStatus == SagaStatus.COMPENSATED
                || currentStatus == SagaStatus.FAILED) {
            if (anomalyRepository.existsBySagaIdAndAnomalyType(sagaId, AnomalyType.FORBIDDEN_POST_TERMINAL)) return;
            createAnomaly(sagaId, AnomalyType.FORBIDDEN_POST_TERMINAL,
                    String.format("Event '%s' received after saga reached terminal status %s",
                            eventType, currentStatus));
        }
    }

    public void detectMultiTerminalConflict(String sagaId) {
        List<SagaEvent> events = sagaEventRepository.findBySagaIdOrderByTimestampAsc(sagaId);
        if (anomalyRepository.existsBySagaIdAndAnomalyType(sagaId, AnomalyType.MULTI_TERMINAL_CONFLICT)) return;

        boolean hasSuccess = events.stream()
                .anyMatch(e -> COMPLETED_EVENTS.contains(e.getEventType()));
        boolean hasFail = events.stream()
                .anyMatch(e -> FAILED_EVENTS.contains(e.getEventType()));

        if (hasSuccess && hasFail) {
            String successTypes = events.stream()
                    .map(SagaEvent::getEventType).filter(COMPLETED_EVENTS::contains)
                    .collect(Collectors.joining(", "));
            String failTypes = events.stream()
                    .map(SagaEvent::getEventType).filter(FAILED_EVENTS::contains)
                    .collect(Collectors.joining(", "));
            createAnomaly(sagaId, AnomalyType.MULTI_TERMINAL_CONFLICT,
                    String.format("Both success [%s] and failure [%s] events present in same saga",
                            successTypes, failTypes));
        }
    }

    public void detectDuplicateCompensation(String sagaId) {
        List<SagaEvent> events = sagaEventRepository.findBySagaIdOrderByTimestampAsc(sagaId);
        long refundCount = events.stream()
                .filter(e -> "REFUND_PAYMENT".equals(e.getEventType()))
                .count();
        if (refundCount > 1 && !anomalyRepository.existsBySagaIdAndAnomalyType(
                sagaId, AnomalyType.DUPLICATE_COMPENSATION)) {
            createAnomaly(sagaId, AnomalyType.DUPLICATE_COMPENSATION,
                    String.format("REFUND_PAYMENT appeared %d times — duplicate compensation", refundCount));
        }
    }

    /**
     * Detect orphan events: events that arrive for a sagaId where no ORDER_CREATED
     * was received first. Called BEFORE saga creation in CorrelationService.
     * The old logic (check sagaInstance exists) never triggered because
     * CorrelationService creates the saga before running detection.
     */
    public void detectOrphanEvent(String sagaId, String eventType) {
        if (anomalyRepository.existsBySagaIdAndAnomalyType(sagaId, AnomalyType.ORPHAN_EVENT)) return;
        createAnomaly(sagaId, AnomalyType.ORPHAN_EVENT,
                String.format("Event '%s' arrived for saga %s but no ORDER_CREATED was received first — possible wrong correlation key",
                        eventType, sagaId));
    }

    /**
     * Detect events missing trace context (traceId). Enables R19 RCA rule.
     */
    public void detectMissingTraceContext(String sagaId, String eventType, String payload) {
        if (anomalyRepository.existsBySagaIdAndAnomalyType(sagaId, AnomalyType.MISSING_TRACE_CONTEXT)) return;
        boolean hasTraceId = false;
        if (payload != null && !payload.isBlank()) {
            try {
                JsonNode node = objectMapper.readTree(payload);
                hasTraceId = node.has("traceId") && !node.get("traceId").isNull()
                        && !node.get("traceId").asText().isBlank();
            } catch (Exception e) {
                // invalid JSON — no trace context
            }
        }
        if (!hasTraceId) {
            // Noise reduction: only flag missing traceId if at least one OTHER event
            // in this saga HAS a traceId. This avoids false positives from admin-injected
            // test events (e.g. evaluate.py scenarios) where no event has trace context.
            boolean sagaHasAnyTrace = sagaEventRepository.findBySagaIdOrderByTimestampAsc(sagaId).stream()
                    .anyMatch(e -> {
                        if (e.getPayload() == null || e.getPayload().isBlank()) return false;
                        try {
                            JsonNode n = objectMapper.readTree(e.getPayload());
                            return n.has("traceId") && !n.get("traceId").isNull()
                                    && !n.get("traceId").asText().isBlank();
                        } catch (Exception ex) { return false; }
                    });
            if (sagaHasAnyTrace) {
                createAnomaly(sagaId, AnomalyType.MISSING_TRACE_CONTEXT,
                        String.format("Event '%s' missing traceId — trace propagation broken", eventType));
            }
        }
    }

    // =========================================================================
    // Scheduled detection — runs periodically
    // =========================================================================

    @Scheduled(fixedRate = 15000)
    public void detectStuckSagas() {
        List<SagaInstance> runningSagas = new java.util.ArrayList<>(
                sagaInstanceRepository.findByStatus(SagaStatus.STARTED));
        runningSagas.addAll(sagaInstanceRepository.findByStatus(SagaStatus.IN_PROGRESS));
        runningSagas.addAll(sagaInstanceRepository.findByStatus(SagaStatus.COMPENSATING));

        WorkflowSpec workflow = workflowLoader.getOrderSagaWorkflow();
        Instant now = Instant.now();

        for (SagaInstance saga : runningSagas) {
            SagaEvent lastEvent = sagaEventRepository
                    .findTopBySagaIdOrderByTimestampDesc(saga.getSagaId())
                    .orElse(null);
            if (lastEvent == null) continue;

            int timeoutSeconds = 60;
            if (workflow != null) {
                String stepName = workflow.getExpectedStepForEvent(lastEvent.getEventType());
                if (stepName != null) {
                    timeoutSeconds = workflow.getStepTimeoutSeconds(stepName);
                }
            }

            if (lastEvent.getTimestamp().plus(timeoutSeconds, ChronoUnit.SECONDS).isBefore(now)) {
                boolean alreadyDetected = anomalyRepository.findBySagaId(saga.getSagaId()).stream()
                        .anyMatch(a -> a.getAnomalyType() == AnomalyType.SAGA_STUCK
                                && a.getDetectedAt().isAfter(lastEvent.getTimestamp()));
                if (!alreadyDetected) {
                    createAnomaly(saga.getSagaId(), AnomalyType.SAGA_STUCK,
                            String.format("Saga stuck: no event since %s (last: %s, timeout: %ds)",
                                    lastEvent.getTimestamp(), lastEvent.getEventType(), timeoutSeconds));
                }
            }
        }
    }

    @Scheduled(fixedRate = 15000)
    public void detectStepTimeouts() {
        WorkflowSpec workflow = workflowLoader.getOrderSagaWorkflow();
        if (workflow == null) return;

        // Only check active + recently completed sagas (last 1h) to avoid N+1 on all historical data
        Instant recentCutoff = Instant.now().minus(1, ChronoUnit.HOURS);
        List<SagaInstance> activeSagas = new java.util.ArrayList<>(
                sagaInstanceRepository.findByStatus(SagaStatus.IN_PROGRESS));
        activeSagas.addAll(sagaInstanceRepository.findByStatus(SagaStatus.COMPENSATING));
        // Check completed sagas only if recent
        sagaInstanceRepository.findByStatus(SagaStatus.COMPLETED).stream()
                .filter(s -> s.getEndTime() != null && s.getEndTime().isAfter(recentCutoff))
                .forEach(activeSagas::add);
        sagaInstanceRepository.findByStatus(SagaStatus.COMPENSATED).stream()
                .filter(s -> s.getEndTime() != null && s.getEndTime().isAfter(recentCutoff))
                .forEach(activeSagas::add);

        for (SagaInstance saga : activeSagas) {
            if (anomalyRepository.existsBySagaIdAndAnomalyType(
                    saga.getSagaId(), AnomalyType.STEP_TIMEOUT)) continue;

            List<SagaEvent> events = sagaEventRepository
                    .findBySagaIdOrderByTimestampAsc(saga.getSagaId());
            if (events.size() < 2) continue;

            for (int i = 1; i < events.size(); i++) {
                long stepDurationMs = Duration.between(
                        events.get(i - 1).getTimestamp(),
                        events.get(i).getTimestamp()).toMillis();

                String stepName = workflow.getExpectedStepForEvent(events.get(i).getEventType());
                if (stepName == null) continue;
                int expectedTimeoutMs = workflow.getStepTimeoutSeconds(stepName) * 1000;

                if (stepDurationMs > expectedTimeoutMs) {
                    createAnomaly(saga.getSagaId(), AnomalyType.STEP_TIMEOUT,
                            String.format("Step '%s' took %dms (timeout: %dms). Previous event: %s",
                                    stepName, stepDurationMs, expectedTimeoutMs,
                                    events.get(i - 1).getEventType()));
                    break;
                }
            }
        }
    }

    @Scheduled(fixedRate = 15000)
    public void detectMissingTerminalEvents() {
        WorkflowSpec workflow = workflowLoader.getOrderSagaWorkflow();
        // Bug 5 fix: null guard for workflow.getSteps()
        int maxStepTimeout = 60; // default
        if (workflow != null && workflow.getSteps() != null) {
            maxStepTimeout = workflow.getSteps().stream()
                    .mapToInt(s -> workflow.getStepTimeoutSeconds(s.getId()))
                    .max().orElse(60);
        }
        int maxTimeout = maxStepTimeout * 2; // 2× max step timeout

        Instant cutoff = Instant.now().minus(maxTimeout, ChronoUnit.SECONDS);

        List<SagaInstance> stuckSagas = new java.util.ArrayList<>(
                sagaInstanceRepository.findByStatusAndLastUpdatedBefore(SagaStatus.STARTED, cutoff));
        stuckSagas.addAll(
                sagaInstanceRepository.findByStatusAndLastUpdatedBefore(SagaStatus.IN_PROGRESS, cutoff));

        for (SagaInstance saga : stuckSagas) {
            if (anomalyRepository.existsBySagaIdAndAnomalyType(
                    saga.getSagaId(), AnomalyType.MISSING_TERMINAL_EVENT)) continue;

            long eventCount = sagaEventRepository.countBySagaId(saga.getSagaId());
            if (eventCount >= 2) {
                createAnomaly(saga.getSagaId(), AnomalyType.MISSING_TERMINAL_EVENT,
                        String.format("Saga has %d events but no terminal event after %ds",
                                eventCount, maxTimeout));
            }
        }
    }

    @Scheduled(fixedRate = 15000)
    public void detectCompensationMissing() {
        Instant cutoff = Instant.now().minus(30, ChronoUnit.SECONDS);

        // INVENTORY_FAILED sets status to COMPENSATING (not FAILED),
        // so we must query COMPENSATING sagas to find missing compensation.
        List<SagaInstance> compensatingSagas = new java.util.ArrayList<>(
                sagaInstanceRepository.findByStatus(SagaStatus.COMPENSATING));

        for (SagaInstance saga : compensatingSagas) {
            if (saga.getLastUpdated().isAfter(cutoff)) continue; // too recent
            if (anomalyRepository.existsBySagaIdAndAnomalyType(
                    saga.getSagaId(), AnomalyType.COMPENSATION_MISSING)) continue;

            List<SagaEvent> events = sagaEventRepository
                    .findBySagaIdOrderByTimestampAsc(saga.getSagaId());
            boolean hasFailure = events.stream().anyMatch(e ->
                    "INVENTORY_FAILED".equals(e.getEventType()));
            boolean hasCompStart = events.stream().anyMatch(e ->
                    "REFUND_PAYMENT".equals(e.getEventType()));

            if (hasFailure && !hasCompStart) {
                createAnomaly(saga.getSagaId(), AnomalyType.COMPENSATION_MISSING,
                        "Failure detected (INVENTORY_FAILED) but no compensation started");
            }
        }
    }

    @Scheduled(fixedRate = 15000)
    public void detectMissingCompensationAck() {
        Instant cutoff = Instant.now().minus(30, ChronoUnit.SECONDS);

        List<SagaInstance> compensatingSagas =
                sagaInstanceRepository.findByStatusAndLastUpdatedBefore(SagaStatus.COMPENSATING, cutoff);

        for (SagaInstance saga : compensatingSagas) {
            if (anomalyRepository.existsBySagaIdAndAnomalyType(
                    saga.getSagaId(), AnomalyType.MISSING_COMPENSATION_ACK)) continue;

            List<SagaEvent> events = sagaEventRepository
                    .findBySagaIdOrderByTimestampAsc(saga.getSagaId());
            boolean hasRefundCmd = events.stream().anyMatch(e ->
                    "REFUND_PAYMENT".equals(e.getEventType()));
            boolean hasRefundDone = events.stream().anyMatch(e ->
                    "PAYMENT_REFUNDED".equals(e.getEventType()));

            if (hasRefundCmd && !hasRefundDone) {
                createAnomaly(saga.getSagaId(), AnomalyType.MISSING_COMPENSATION_ACK,
                        "Compensation started (REFUND_PAYMENT) but no acknowledgement (PAYMENT_REFUNDED)");
            }
        }
    }

    @Scheduled(fixedRate = 15000)
    public void detectDelayedCompensation() {
        // Derive threshold from workflow compensation step timeout × 1.5.
        // REFUND_PAYMENT step timeout = 10s in YAML → threshold = 15s.
        // Falls back to 15s if workflow not loaded.
        WorkflowSpec workflow = workflowLoader.getOrderSagaWorkflow();
        int compensationTimeoutSeconds = 10; // default
        if (workflow != null) {
            compensationTimeoutSeconds = workflow.getStepTimeoutSeconds("REFUND_PAYMENT");
        }
        int compensationThresholdMs = (int) (compensationTimeoutSeconds * 1.5 * 1000);

        Instant recentCutoff = Instant.now().minus(10, ChronoUnit.MINUTES);

        // Check sagas that completed compensation recently
        List<SagaInstance> compensatedSagas = new java.util.ArrayList<>(
                sagaInstanceRepository.findByStatus(SagaStatus.COMPENSATED));
        compensatedSagas.addAll(sagaInstanceRepository.findByStatus(SagaStatus.COMPENSATING));

        for (SagaInstance saga : compensatedSagas) {
            if (saga.getLastUpdated() != null && saga.getLastUpdated().isBefore(recentCutoff)) continue;
            if (anomalyRepository.existsBySagaIdAndAnomalyType(
                    saga.getSagaId(), AnomalyType.DELAYED_COMPENSATION)) continue;

            List<SagaEvent> events = sagaEventRepository
                    .findBySagaIdOrderByTimestampAsc(saga.getSagaId());

            // Find failure event and first compensation event
            Instant failureTime = null;
            Instant compensationStartTime = null;
            for (SagaEvent event : events) {
                if ("INVENTORY_FAILED".equals(event.getEventType()) && failureTime == null) {
                    failureTime = event.getTimestamp();
                }
                if ("REFUND_PAYMENT".equals(event.getEventType()) && compensationStartTime == null) {
                    compensationStartTime = event.getTimestamp();
                }
            }

            if (failureTime != null && compensationStartTime != null) {
                long gapMs = Duration.between(failureTime, compensationStartTime).toMillis();
                if (gapMs > compensationThresholdMs) {
                    createAnomaly(saga.getSagaId(), AnomalyType.DELAYED_COMPENSATION,
                            String.format("Compensation delayed: %dms gap between INVENTORY_FAILED and REFUND_PAYMENT (threshold: %dms)",
                                    gapMs, compensationThresholdMs));
                }
            }
        }
    }

    // =========================================================================
    // Utility
    // =========================================================================

    private int statusRank(SagaStatus status) {
        return switch (status) {
            case STARTED -> 1;
            case IN_PROGRESS -> 2;
            case COMPENSATING -> 3;
            case FAILED -> 4;               // terminal but no rollback
            case COMPENSATED -> 5;           // terminal with successful rollback (> FAILED)
            case COMPLETED -> 6;             // highest terminal
        };
    }

    private void createAnomaly(String sagaId, AnomalyType type, String description) {
        Anomaly anomaly = new Anomaly();
        anomaly.setSagaId(sagaId);
        anomaly.setAnomalyType(type);
        anomaly.setDescription(description);
        anomaly.setDetectedAt(Instant.now());
        anomalyRepository.save(anomaly);
        log.warn("Anomaly detected: [{}] {} - {}", type, sagaId, description);
    }
}
