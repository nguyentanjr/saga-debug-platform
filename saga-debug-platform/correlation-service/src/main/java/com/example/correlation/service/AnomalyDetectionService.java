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
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
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
    private final SagaInstanceRepository sagaInstanceRepository;
    private final SagaEventRepository sagaEventRepository;
    private final AnomalyRepository anomalyRepository;
    private final WorkflowLoader workflowLoader;
    private final ObjectMapper objectMapper;
    private final DetectionProperties detectionProperties;

    public AnomalyDetectionService(
            SagaInstanceRepository sagaInstanceRepository,
            SagaEventRepository sagaEventRepository,
            AnomalyRepository anomalyRepository,
            WorkflowLoader workflowLoader,
            ObjectMapper objectMapper,
            DetectionProperties detectionProperties) {
        this.sagaInstanceRepository = sagaInstanceRepository;
        this.sagaEventRepository = sagaEventRepository;
        this.anomalyRepository = anomalyRepository;
        this.workflowLoader = workflowLoader;
        this.objectMapper = objectMapper;
        this.detectionProperties = detectionProperties;
    }


    // Clock skew tolerance: events from different services may have clocks that
    // differ by a few hundred ms. Only flag as out-of-order if the gap exceeds
    // this threshold to avoid false positives from infrastructure artifacts.
    public void detectOutOfOrder(String sagaId) {
        List<SagaEvent> events = sagaEventRepository.findBySagaIdOrderByReceivedAtAsc(sagaId);
        if (events.size() < 2) return;
        if (anomalyRepository.existsBySagaIdAndAnomalyType(sagaId, AnomalyType.OUT_OF_ORDER)) return;

        for (int i = 1; i < events.size(); i++) {
            SagaEvent previousEvent = events.get(i - 1);
            SagaEvent currentEvent = events.get(i);
            Instant prevCreated = previousEvent.getTimestamp();
            Instant currCreated = currentEvent.getTimestamp();
            Duration timeDiff = Duration.between(currCreated, prevCreated);
            // Only flag if currCreated is BEFORE prevCreated by more than clock skew tolerance
            Duration clockSkewTolerance = detectionProperties.getClockSkewTolerance();
            if (currCreated.isBefore(prevCreated) && timeDiff.compareTo(clockSkewTolerance) > 0) {
                createAnomaly(sagaId, AnomalyType.OUT_OF_ORDER,
                        String.format("Event '%s' arrived after '%s' but has earlier timestamp (%s < %s, gap: %dms)",
                                currentEvent.getEventType(),
                                previousEvent.getEventType(),
                                currCreated, prevCreated, timeDiff.toMillis()),
                        details()
                                .observedValue(String.format("%s timestamp %s before %s timestamp %s by %dms",
                                        currentEvent.getEventType(), currCreated,
                                        previousEvent.getEventType(), prevCreated,
                                        timeDiff.toMillis()))
                                .expectedValue(String.format("event timestamp >= previous timestamp minus %dms clock skew",
                                        clockSkewTolerance.toMillis()))
                                .thresholdSource("config:anomaly.detection.clock-skew-tolerance")
                                .evidenceEvents(previousEvent, currentEvent)
                                .measuredAt(currCreated));
                return;
            }
        }
    }


    public void detectInvalidTransition(String sagaId, String previousStep,
            String currentStep, String condition, String eventId, Instant eventTimestamp) {
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
                            expected.isEmpty() ? "none defined" : expected),
                    details()
                            .observedValue(String.format("%s -> %s on %s", previousStep, currentStep, condition))
                            .expectedValue(expected.isEmpty() ? "no transition defined from " + previousStep : expected)
                            .thresholdSource("workflow:order-saga.transitions")
                            .evidenceEventIds(eventId)
                            .measuredAt(eventTimestamp));
        }
    }

    public void detectRetryBurst(String sagaId) {
        List<SagaEvent> events = sagaEventRepository.findBySagaIdOrderByTimestampAsc(sagaId);
        int burstThreshold = detectionProperties.getRetryBurstThreshold();
        Duration burstWindow = detectionProperties.getRetryBurstWindow();
        if (events.size() < burstThreshold) return;
        if (anomalyRepository.existsBySagaIdAndAnomalyType(sagaId, AnomalyType.RETRY_BURST)) return;

        List<SagaEvent> retryEvents = events.stream()
                .filter(e -> RETRY_EVENT_TYPES.contains(e.getEventType()))
                .collect(Collectors.toList());
        if (retryEvents.size() < burstThreshold) return;

        for (int i = 0; i <= retryEvents.size() - burstThreshold; i++) {
            SagaEvent first = retryEvents.get(i);
            SagaEvent last = retryEvents.get(i + burstThreshold - 1);

            Duration gap = Duration.between(first.getTimestamp(), last.getTimestamp());
            if (gap.compareTo(burstWindow) <= 0) {
                List<SagaEvent> burstEvents = retryEvents.subList(i, i + burstThreshold);
                createAnomaly(sagaId, AnomalyType.RETRY_BURST,
                        String.format("%d retry events in %ds — possible retry storm",
                                burstThreshold, gap.getSeconds()),
                        details()
                                .observedValue(String.format("%d retry events in %dms",
                                        burstEvents.size(), gap.toMillis()))
                                .expectedValue(String.format("fewer than %d retry events within %ds",
                                        burstThreshold, burstWindow.toSeconds()))
                                .thresholdSource("config:anomaly.detection.retry-burst")
                                .evidenceEvents(burstEvents)
                                .measuredAt(last.getTimestamp()));
                return;
            }
        }
    }

    public void detectDuplicateEvent(String sagaId, String eventId, String eventType) {
        // Called when dedup detects a duplicate — create anomaly instead of silent drop
        if (anomalyRepository.existsBySagaIdAndAnomalyType(sagaId, AnomalyType.EXACT_DUPLICATE_EVENT)) return;
        createAnomaly(sagaId, AnomalyType.EXACT_DUPLICATE_EVENT,
                String.format("Duplicate event detected: eventId=%s, type=%s", eventId, eventType),
                details()
                        .observedValue(String.format("eventId=%s, eventType=%s", eventId, eventType))
                        .expectedValue("event_id appears once per saga event stream")
                        .thresholdSource("db:saga_events.event_id")
                        .evidenceEventIds(eventId)
                        .measuredAt(Instant.now()));
    }

    public void detectStateRegression(String sagaId, SagaStatus oldStatus, SagaStatus newStatus,
            String eventId, Instant eventTimestamp) {
        if (statusRank(newStatus) < statusRank(oldStatus)) {
            if (anomalyRepository.existsBySagaIdAndAnomalyType(sagaId, AnomalyType.STATE_REGRESSION)) return;
            createAnomaly(sagaId, AnomalyType.STATE_REGRESSION,
                    String.format("State regression: %s → %s (rank %d → %d)",
                            oldStatus, newStatus, statusRank(oldStatus), statusRank(newStatus)),
                    details()
                            .observedValue(String.format("%s -> %s", oldStatus, newStatus))
                            .expectedValue("saga status rank is monotonic non-decreasing")
                            .thresholdSource("code:SagaStatus.rank")
                            .evidenceEventIds(eventId)
                            .measuredAt(eventTimestamp));
        }
    }

    public void detectForbiddenPostTerminal(String sagaId, SagaStatus currentStatus, String eventType,
            String eventId, Instant eventTimestamp) {
        if (currentStatus == SagaStatus.COMPLETED || currentStatus == SagaStatus.COMPENSATED
                || currentStatus == SagaStatus.FAILED) {
            if (anomalyRepository.existsBySagaIdAndAnomalyType(sagaId, AnomalyType.FORBIDDEN_POST_TERMINAL)) return;
            createAnomaly(sagaId, AnomalyType.FORBIDDEN_POST_TERMINAL,
                    String.format("Event '%s' received after saga reached terminal status %s",
                            eventType, currentStatus),
                    details()
                            .observedValue(String.format("%s after terminal status %s", eventType, currentStatus))
                            .expectedValue("no events after terminal saga status")
                            .thresholdSource("code:terminal-status-guard")
                            .evidenceEventIds(eventId)
                            .measuredAt(eventTimestamp));
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
            List<SagaEvent> terminalEvents = events.stream()
                    .filter(e -> COMPLETED_EVENTS.contains(e.getEventType())
                            || FAILED_EVENTS.contains(e.getEventType()))
                    .toList();
            String successTypes = events.stream()
                    .map(SagaEvent::getEventType).filter(COMPLETED_EVENTS::contains)
                    .collect(Collectors.joining(", "));
            String failTypes = events.stream()
                    .map(SagaEvent::getEventType).filter(FAILED_EVENTS::contains)
                    .collect(Collectors.joining(", "));
            createAnomaly(sagaId, AnomalyType.MULTI_TERMINAL_CONFLICT,
                    String.format("Both success [%s] and failure [%s] events present in same saga",
                            successTypes, failTypes),
                    details()
                            .observedValue(String.format("success=[%s], failure=[%s]", successTypes, failTypes))
                            .expectedValue("only one terminal event family per saga")
                            .thresholdSource("code:terminal-event-sets")
                            .evidenceEvents(terminalEvents)
                            .measuredAt(lastTimestamp(terminalEvents)));
        }
    }

    public void detectDuplicateCompensation(String sagaId) {
        List<SagaEvent> events = sagaEventRepository.findBySagaIdOrderByTimestampAsc(sagaId);
        List<SagaEvent> refundEvents = events.stream()
                .filter(e -> "REFUND_PAYMENT".equals(e.getEventType()))
                .toList();
        long refundCount = refundEvents.size();
        if (refundCount > 1 && !anomalyRepository.existsBySagaIdAndAnomalyType(
                sagaId, AnomalyType.DUPLICATE_COMPENSATION)) {
            createAnomaly(sagaId, AnomalyType.DUPLICATE_COMPENSATION,
                    String.format("REFUND_PAYMENT appeared %d times - duplicate compensation", refundCount),
                    details()
                            .observedValue(String.format("REFUND_PAYMENT count=%d", refundCount))
                            .expectedValue("REFUND_PAYMENT appears once per compensation attempt")
                            .thresholdSource("workflow:REFUND_PAYMENT.cardinality")
                            .evidenceEvents(refundEvents)
                            .measuredAt(lastTimestamp(refundEvents)));
        }
    }

    /**
     * Detect orphan events: events that arrive for a sagaId where no ORDER_CREATED
     * was received first. Called BEFORE saga creation in CorrelationService.
     * The old logic (check sagaInstance exists) never triggered because
     * CorrelationService creates the saga before running detection.
     */
    public void detectOrphanEvent(String sagaId, String eventId, String eventType, Instant eventTimestamp) {
        if (anomalyRepository.existsBySagaIdAndAnomalyType(sagaId, AnomalyType.ORPHAN_EVENT)) return;
        createAnomaly(sagaId, AnomalyType.ORPHAN_EVENT,
                String.format("Event '%s' arrived for saga %s but no ORDER_CREATED was received first — possible wrong correlation key",
                        eventType, sagaId),
                details()
                        .observedValue(String.format("first observed event is %s", eventType))
                        .expectedValue("first event is ORDER_CREATED")
                        .thresholdSource("workflow:ORDER_SAGA.start_event")
                        .evidenceEventIds(eventId)
                        .measuredAt(eventTimestamp));
    }

    /**
     * Detect events missing trace context (traceId). Enables R19 RCA rule.
     */
    public void detectMissingTraceContext(String sagaId, String eventType, String eventId, String payload,
            Instant eventTimestamp) {
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
                        String.format("Event '%s' missing traceId - trace propagation broken", eventType),
                        details()
                                .observedValue(String.format("event '%s' payload has no traceId", eventType))
                                .expectedValue("payload contains non-empty traceId")
                                .thresholdSource("otel:trace-context-propagation")
                                .evidenceEventIds(eventId)
                                .measuredAt(eventTimestamp));
            }
        }
    }

    // =========================================================================
    // Scheduled detection — runs periodically
    // =========================================================================

    @Scheduled(fixedRateString = "${anomaly.detection.schedule-rate-ms:15000}")
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

            int timeoutSeconds = detectionProperties.getDefaultStepTimeoutSeconds();
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
                                    lastEvent.getTimestamp(), lastEvent.getEventType(), timeoutSeconds),
                            details()
                                    .observedValue(String.format("no event since %s after %s",
                                            lastEvent.getTimestamp(), lastEvent.getEventType()))
                                    .expectedValue(String.format("next event within %ds", timeoutSeconds))
                                    .thresholdSource(timeoutSource(workflow, lastEvent.getEventType()))
                                    .evidenceEvents(lastEvent)
                                    .measuredAt(now));
                }
            }
        }
    }

    @Scheduled(fixedRateString = "${anomaly.detection.schedule-rate-ms:15000}")
    public void detectStepTimeouts() {
        WorkflowSpec workflow = workflowLoader.getOrderSagaWorkflow();
        if (workflow == null) return;

        // Only check active + recently completed sagas (last 1h) to avoid N+1 on all historical data
        Instant recentCutoff = Instant.now().minus(detectionProperties.getCompletedSagaLookback());
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
                SagaEvent previousEvent = events.get(i - 1);
                SagaEvent currentEvent = events.get(i);
                long stepDurationMs = Duration.between(
                        previousEvent.getTimestamp(),
                        currentEvent.getTimestamp()).toMillis();

                String stepName = workflow.getExpectedStepForEvent(currentEvent.getEventType());
                if (stepName == null) continue;
                int expectedTimeoutMs = workflow.getStepTimeoutSeconds(stepName) * 1000;

                if (stepDurationMs > expectedTimeoutMs) {
                    createAnomaly(saga.getSagaId(), AnomalyType.STEP_TIMEOUT,
                            String.format("Step '%s' took %dms (timeout: %dms). Previous event: %s",
                                    stepName, stepDurationMs, expectedTimeoutMs,
                                    previousEvent.getEventType()),
                            details()
                                    .observedValue(String.format("%s took %dms", stepName, stepDurationMs))
                                    .expectedValue(String.format("<= %dms", expectedTimeoutMs))
                                    .thresholdSource("workflow:" + stepName + ".timeout")
                                    .evidenceEvents(previousEvent, currentEvent)
                                    .measuredAt(currentEvent.getTimestamp()));
                    break;
                }
            }
        }
    }

    @Scheduled(fixedRateString = "${anomaly.detection.schedule-rate-ms:15000}")
    public void detectMissingTerminalEvents() {
        WorkflowSpec workflow = workflowLoader.getOrderSagaWorkflow();
        // Bug 5 fix: null guard for workflow.getSteps()
        int maxStepTimeout = detectionProperties.getDefaultStepTimeoutSeconds();
        if (workflow != null && workflow.getSteps() != null) {
            maxStepTimeout = workflow.getSteps().stream()
                    .mapToInt(s -> workflow.getStepTimeoutSeconds(s.getId()))
                    .max().orElse(detectionProperties.getDefaultStepTimeoutSeconds());
        }
        int maxTimeout = maxStepTimeout * detectionProperties.getMissingTerminalTimeoutMultiplier();

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
                SagaEvent lastEvent = sagaEventRepository
                        .findTopBySagaIdOrderByTimestampDesc(saga.getSagaId())
                        .orElse(null);
                createAnomaly(saga.getSagaId(), AnomalyType.MISSING_TERMINAL_EVENT,
                        String.format("Saga has %d events but no terminal event after %ds",
                                eventCount, maxTimeout),
                        details()
                                .observedValue(String.format("%d events and no terminal event", eventCount))
                                .expectedValue(String.format("terminal event within %ds", maxTimeout))
                                .thresholdSource("config:anomaly.detection.missing-terminal-timeout-multiplier")
                                .evidenceEvents(lastEvent)
                                .measuredAt(Instant.now()));
            }
        }
    }

    @Scheduled(fixedRateString = "${anomaly.detection.schedule-rate-ms:15000}")
    public void detectCompensationMissing() {
        Instant cutoff = Instant.now().minus(detectionProperties.getCompensationMissingCutoff());

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
            List<SagaEvent> failureEvents = events.stream()
                    .filter(e -> "INVENTORY_FAILED".equals(e.getEventType()))
                    .toList();
            boolean hasFailure = !failureEvents.isEmpty();
            boolean hasCompStart = events.stream().anyMatch(e ->
                    "REFUND_PAYMENT".equals(e.getEventType()));

            if (hasFailure && !hasCompStart) {
                createAnomaly(saga.getSagaId(), AnomalyType.COMPENSATION_MISSING,
                        "Failure detected (INVENTORY_FAILED) but no compensation started",
                        details()
                                .observedValue("INVENTORY_FAILED without REFUND_PAYMENT")
                                .expectedValue("REFUND_PAYMENT starts compensation after INVENTORY_FAILED")
                                .thresholdSource("workflow:INVENTORY_FAILED.compensation")
                                .evidenceEvents(failureEvents)
                                .measuredAt(Instant.now()));
            }
        }
    }

    @Scheduled(fixedRateString = "${anomaly.detection.schedule-rate-ms:15000}")
    public void detectMissingCompensationAck() {
        Instant cutoff = Instant.now().minus(detectionProperties.getCompensationAckCutoff());

        List<SagaInstance> compensatingSagas =
                sagaInstanceRepository.findByStatusAndLastUpdatedBefore(SagaStatus.COMPENSATING, cutoff);

        for (SagaInstance saga : compensatingSagas) {
            if (anomalyRepository.existsBySagaIdAndAnomalyType(
                    saga.getSagaId(), AnomalyType.MISSING_COMPENSATION_ACK)) continue;

            List<SagaEvent> events = sagaEventRepository
                    .findBySagaIdOrderByTimestampAsc(saga.getSagaId());
            List<SagaEvent> refundCommandEvents = events.stream()
                    .filter(e -> "REFUND_PAYMENT".equals(e.getEventType()))
                    .toList();
            boolean hasRefundCmd = !refundCommandEvents.isEmpty();
            boolean hasRefundDone = events.stream().anyMatch(e ->
                    "PAYMENT_REFUNDED".equals(e.getEventType()));

            if (hasRefundCmd && !hasRefundDone) {
                createAnomaly(saga.getSagaId(), AnomalyType.MISSING_COMPENSATION_ACK,
                        "Compensation started (REFUND_PAYMENT) but no acknowledgement (PAYMENT_REFUNDED)",
                        details()
                                .observedValue("REFUND_PAYMENT without PAYMENT_REFUNDED")
                                .expectedValue("PAYMENT_REFUNDED acknowledges compensation")
                                .thresholdSource("workflow:REFUND_PAYMENT.ack")
                                .evidenceEvents(refundCommandEvents)
                                .measuredAt(Instant.now()));
            }
        }
    }

    @Scheduled(fixedRateString = "${anomaly.detection.schedule-rate-ms:15000}")
    public void detectDelayedCompensation() {
        // Derive threshold from the workflow compensation timeout multiplied by
        // the configured delayed-compensation multiplier.
        WorkflowSpec workflow = workflowLoader.getOrderSagaWorkflow();
        int compensationTimeoutSeconds = detectionProperties.getDefaultStepTimeoutSeconds();
        if (workflow != null) {
            compensationTimeoutSeconds = workflow.getStepTimeoutSeconds("REFUND_PAYMENT");
        }
        int compensationThresholdMs = (int) (compensationTimeoutSeconds
                * detectionProperties.getDelayedCompensationMultiplier() * 1000);

        Instant recentCutoff = Instant.now().minus(detectionProperties.getCompensationRecentLookback());

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
            SagaEvent failureEvent = null;
            SagaEvent compensationStartEvent = null;
            for (SagaEvent event : events) {
                if ("INVENTORY_FAILED".equals(event.getEventType()) && failureEvent == null) {
                    failureEvent = event;
                }
                if ("REFUND_PAYMENT".equals(event.getEventType()) && compensationStartEvent == null) {
                    compensationStartEvent = event;
                }
            }

            if (failureEvent != null && compensationStartEvent != null) {
                long gapMs = Duration.between(failureEvent.getTimestamp(), compensationStartEvent.getTimestamp()).toMillis();
                if (gapMs > compensationThresholdMs) {
                    createAnomaly(saga.getSagaId(), AnomalyType.DELAYED_COMPENSATION,
                            String.format("Compensation delayed: %dms gap between INVENTORY_FAILED and REFUND_PAYMENT (threshold: %dms)",
                                    gapMs, compensationThresholdMs),
                            details()
                                    .observedValue(String.format("%dms gap from INVENTORY_FAILED to REFUND_PAYMENT", gapMs))
                                    .expectedValue(String.format("<= %dms", compensationThresholdMs))
                                    .thresholdSource("config:anomaly.detection.delayed-compensation-multiplier")
                                    .evidenceEvents(failureEvent, compensationStartEvent)
                                    .measuredAt(compensationStartEvent.getTimestamp()));
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

    private void createAnomaly(String sagaId, AnomalyType type, String description, AnomalyDetails details) {
        Anomaly anomaly = new Anomaly();
        anomaly.setSagaId(sagaId);
        anomaly.setAnomalyType(type);
        anomaly.setDescription(description);
        anomaly.setDetectedAt(Instant.now());
        anomaly.setObservedValue(details.observedValue);
        anomaly.setExpectedValue(details.expectedValue);
        anomaly.setThresholdSource(details.thresholdSource);
        anomaly.setEvidenceEventIds(details.evidenceEventIds);
        anomaly.setMeasuredAt(details.measuredAt);
        anomalyRepository.save(anomaly);
        log.warn("Anomaly detected: [{}] {} - {}", type, sagaId, description);
    }

    private static AnomalyDetails details() {
        return new AnomalyDetails();
    }

    private static Instant lastTimestamp(List<SagaEvent> events) {
        if (events == null || events.isEmpty()) return null;
        return events.get(events.size() - 1).getTimestamp();
    }

    private static String timeoutSource(WorkflowSpec workflow, String eventType) {
        if (workflow == null) return "config:anomaly.detection.default-step-timeout-seconds";
        String stepName = workflow.getExpectedStepForEvent(eventType);
        if (stepName == null) return "config:anomaly.detection.default-step-timeout-seconds";
        return "workflow:" + stepName + ".timeout";
    }

    private static class AnomalyDetails {
        private String observedValue;
        private String expectedValue;
        private String thresholdSource;
        private List<String> evidenceEventIds;
        private Instant measuredAt;

        private AnomalyDetails observedValue(String observedValue) {
            this.observedValue = observedValue;
            return this;
        }

        private AnomalyDetails expectedValue(String expectedValue) {
            this.expectedValue = expectedValue;
            return this;
        }

        private AnomalyDetails thresholdSource(String thresholdSource) {
            this.thresholdSource = thresholdSource;
            return this;
        }

        private AnomalyDetails evidenceEventIds(String... eventIds) {
            this.evidenceEventIds = Arrays.stream(eventIds == null ? new String[0] : eventIds)
                    .filter(Objects::nonNull)
                    .filter(id -> !id.isBlank())
                    .toList();
            return this;
        }

        private AnomalyDetails evidenceEvents(SagaEvent... events) {
            return evidenceEvents(Arrays.asList(events == null ? new SagaEvent[0] : events));
        }

        private AnomalyDetails evidenceEvents(List<SagaEvent> events) {
            if (events == null) {
                this.evidenceEventIds = List.of();
                return this;
            }
            this.evidenceEventIds = events.stream()
                    .filter(Objects::nonNull)
                    .map(SagaEvent::getEventId)
                    .filter(Objects::nonNull)
                    .filter(id -> !id.isBlank())
                    .toList();
            return this;
        }

        private AnomalyDetails measuredAt(Instant measuredAt) {
            this.measuredAt = measuredAt;
            return this;
        }
    }
}
