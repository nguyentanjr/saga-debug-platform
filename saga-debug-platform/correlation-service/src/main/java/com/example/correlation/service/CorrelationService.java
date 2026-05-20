package com.example.correlation.service;

import com.example.correlation.domain.SagaEvent;
import com.example.correlation.domain.SagaInstance;
import com.example.correlation.domain.SagaStatus;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.example.correlation.workflow.WorkflowLoader;
import com.example.correlation.workflow.WorkflowSpec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Map;
import java.util.Set;

@Service
public class CorrelationService {

    private static final Logger log = LoggerFactory.getLogger(CorrelationService.class);

    private final SagaInstanceRepository sagaInstanceRepository;
    private final SagaEventRepository sagaEventRepository;
    private final AnomalyDetectionService anomalyDetectionService;
    private final DeduplicationCache deduplicationCache;
    private final WorkflowLoader workflowLoader;
    private final ObjectMapper objectMapper;

    // ============================================================================
    // Shared event classification constants — single source of truth.
    // AnomalyDetectionService references these to avoid definition drift.
    //
    // IMPORTANT naming note: these sets define which events trigger saga STATUS
    // transitions, NOT "all success/failure events". For example,
    // PAYMENT_COMPLETED is a success event but does NOT appear in COMPLETED_EVENTS
    // because it is an intermediate step — only the FINAL step (INVENTORY_RESERVED)
    // transitions the saga to COMPLETED status.
    // ============================================================================

    /** Events that transition saga → COMPLETED (final success step only). */
    public static final Set<String> COMPLETED_EVENTS = Set.of("INVENTORY_RESERVED");

    /** Events that transition saga → FAILED (unrecoverable failure). */
    public static final Set<String> FAILED_EVENTS = Set.of("PAYMENT_FAILED");

    /** Events that transition saga → COMPENSATING (trigger rollback). */
    public static final Set<String> COMPENSATING_EVENTS = Set.of("INVENTORY_FAILED", "REFUND_PAYMENT");

    /** Events that transition saga → COMPENSATED (rollback acknowledged). */
    public static final Set<String> COMPENSATED_EVENTS = Set.of("PAYMENT_REFUNDED");

    private static final Map<String, String> EVENT_TO_CONDITION = Map.of(
            "ORDER_CREATED", "SUCCESS",
            "PAYMENT_COMPLETED", "SUCCESS",
            "INVENTORY_RESERVED", "SUCCESS",
            "PAYMENT_FAILED", "FAILED",
            "INVENTORY_FAILED", "FAILED",
            "REFUND_PAYMENT", "COMPENSATION_START",       // command event (start compensation)
            "PAYMENT_REFUNDED", "COMPENSATION_COMPLETE"   // ack event (compensation done)
    );

    public CorrelationService(SagaInstanceRepository sagaInstanceRepository,
                              SagaEventRepository sagaEventRepository,
                              AnomalyDetectionService anomalyDetectionService,
                              DeduplicationCache deduplicationCache,
                              WorkflowLoader workflowLoader,
                              ObjectMapper objectMapper) {
        this.sagaInstanceRepository = sagaInstanceRepository;
        this.sagaEventRepository = sagaEventRepository;
        this.anomalyDetectionService = anomalyDetectionService;
        this.deduplicationCache = deduplicationCache;
        this.workflowLoader = workflowLoader;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public void processEvent(String eventId, String sagaId, String eventType,
                             String stepName, String serviceName,
                             Instant eventTimestamp, String payload,
                             Instant receivedAt) {

        if (sagaEventRepository.findByEventId(eventId).isPresent()) {
            log.info("Duplicate event {} for saga {} (DB hit). Skipping.", eventId, sagaId);
            anomalyDetectionService.detectDuplicateEvent(sagaId, eventId, eventType);
            return;
        }

        //  Find or create saga
        SagaInstance saga = sagaInstanceRepository.findById(sagaId).orElse(null);
        String previousStep = null;
        SagaStatus oldStatus = null;

        if (saga == null) {
            // Detect orphan: first event for this saga should be ORDER_CREATED
            if (!"ORDER_CREATED".equals(eventType)) {
                anomalyDetectionService.detectOrphanEvent(sagaId, eventId, eventType, eventTimestamp);
            }

            saga = new SagaInstance();
            saga.setSagaId(sagaId);
            saga.setStatus(SagaStatus.STARTED);
            saga.setStartTime(eventTimestamp);
            saga.setLastUpdated(eventTimestamp);
            // Extract orderId from payload for multi-key correlation
            saga.setOrderId(extractOrderId(payload));
            log.info("New saga detected: {}", sagaId);
        } else {
            oldStatus = saga.getStatus();
            // Detect forbidden post-terminal event
            anomalyDetectionService.detectForbiddenPostTerminal(sagaId, oldStatus, eventType, eventId, eventTimestamp);

            // Track previous event for transition validation.
            var previousEvents = sagaEventRepository.findBySagaIdOrderByTimestampAsc(sagaId);
            if (!previousEvents.isEmpty()) {
                previousStep = previousEvents.get(previousEvents.size() - 1).getEventType();
            }
        }

        //  State machine transition
        SagaStatus newStatus = determineSagaStatus(eventType, saga.getStatus());
        saga.setStatus(newStatus);
        saga.setLastUpdated(eventTimestamp);

        if (isTerminalStatus(newStatus)) {
            saga.setEndTime(eventTimestamp);
        }

        //  Detect state regression (before save)
        if (oldStatus != null) {
            anomalyDetectionService.detectStateRegression(sagaId, oldStatus, newStatus, eventId, eventTimestamp);
        }

        sagaInstanceRepository.save(saga);

        // Save event
        SagaEvent sagaEvent = new SagaEvent();
        sagaEvent.setEventId(eventId);
        sagaEvent.setSagaId(sagaId);
        sagaEvent.setEventType(eventType);
        sagaEvent.setStepName(stepName);
        sagaEvent.setServiceName(serviceName);
        sagaEvent.setTimestamp(eventTimestamp);
        sagaEvent.setPayload(payload);
        sagaEvent.setReceivedAt(receivedAt);
        sagaEventRepository.save(sagaEvent);

        log.info("Saga {} | event={} | step={} | status={}", sagaId, eventType, stepName, newStatus);

        //  Inline anomaly detection
        anomalyDetectionService.detectOutOfOrder(sagaId);
        anomalyDetectionService.detectRetryBurst(sagaId);
        anomalyDetectionService.detectMultiTerminalConflict(sagaId);
        anomalyDetectionService.detectDuplicateCompensation(sagaId);
        anomalyDetectionService.detectMissingTraceContext(sagaId, eventType, eventId, payload, eventTimestamp);

        // 8. Workflow transition validation
        var workflow = workflowLoader.getOrderSagaWorkflow();
        if (workflow != null && previousStep != null && !isCompensationStepEvent(workflow, eventType)) {
            String previousStepName = workflow.getExpectedStepForEvent(previousStep);
            String currentStepName = workflow.getExpectedStepForEvent(eventType);
            String condition = EVENT_TO_CONDITION.getOrDefault(eventType, "SUCCESS");
            if (previousStepName != null && currentStepName != null) {
                anomalyDetectionService.detectInvalidTransition(
                        sagaId, previousStepName, currentStepName, condition, eventId, eventTimestamp);
            }
        }
    }

    private boolean isCompensationStepEvent(WorkflowSpec workflow, String eventType) {
        if (workflow.getCompensationSteps() == null) return false;
        return workflow.getCompensationSteps().stream()
                .filter(step -> step.getEvents() != null)
                .anyMatch(step -> step.getEvents().containsValue(eventType));
    }

    private String extractOrderId(String payload) {
        if (payload == null) return null;
        try {
            JsonNode node = objectMapper.readTree(payload);
            if (node.has("orderId") && !node.get("orderId").isNull()) {
                return node.get("orderId").asText();
            }
        } catch (Exception e) {
            // ignore
        }
        return null;
    }

    private SagaStatus determineSagaStatus(String eventType, SagaStatus currentStatus) {
        if (COMPLETED_EVENTS.contains(eventType)) return SagaStatus.COMPLETED;
        if (FAILED_EVENTS.contains(eventType)) return SagaStatus.FAILED;
        if (COMPENSATING_EVENTS.contains(eventType)) return SagaStatus.COMPENSATING;
        if (COMPENSATED_EVENTS.contains(eventType)) return SagaStatus.COMPENSATED;
        if (currentStatus == SagaStatus.STARTED) return SagaStatus.IN_PROGRESS;
        return currentStatus;
    }

    private boolean isTerminalStatus(SagaStatus status) {
        return status == SagaStatus.COMPLETED
                || status == SagaStatus.FAILED
                || status == SagaStatus.COMPENSATED;
    }
}
