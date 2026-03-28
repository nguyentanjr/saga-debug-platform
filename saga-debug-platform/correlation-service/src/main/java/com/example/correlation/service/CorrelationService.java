package com.example.correlation.service;

import com.example.correlation.domain.SagaEvent;
import com.example.correlation.domain.SagaInstance;
import com.example.correlation.domain.SagaStatus;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.example.correlation.workflow.WorkflowLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Set;

@Service
public class CorrelationService {

    private static final Logger log = LoggerFactory.getLogger(CorrelationService.class);

    private final SagaInstanceRepository sagaInstanceRepository;
    private final SagaEventRepository sagaEventRepository;
    private final AnomalyDetectionService anomalyDetectionService;
    private final DeduplicationCache deduplicationCache;
    private final WorkflowLoader workflowLoader;

    private static final Set<String> COMPLETED_EVENTS = Set.of("INVENTORY_RESERVED");
    private static final Set<String> FAILED_EVENTS = Set.of("PAYMENT_FAILED");
    private static final Set<String> COMPENSATING_EVENTS = Set.of("INVENTORY_FAILED", "REFUND_PAYMENT");
    private static final Set<String> COMPENSATED_EVENTS = Set.of("PAYMENT_REFUNDED");

    public CorrelationService(SagaInstanceRepository sagaInstanceRepository,
                              SagaEventRepository sagaEventRepository,
                              AnomalyDetectionService anomalyDetectionService,
                              DeduplicationCache deduplicationCache,
                              WorkflowLoader workflowLoader) {
        this.sagaInstanceRepository = sagaInstanceRepository;
        this.sagaEventRepository = sagaEventRepository;
        this.anomalyDetectionService = anomalyDetectionService;
        this.deduplicationCache = deduplicationCache;
        this.workflowLoader = workflowLoader;
    }

    @Transactional
    public void processEvent(String eventId, String sagaId, String eventType,
                             String stepName, String serviceName,
                             Instant eventTimestamp, String payload) {

        // Idempotency key: {sagaId}-{stepName}-{eventType}
        // This ensures the same saga step event is not processed twice,
        // regardless of Kafka redelivery or producer retries.
        String idempotencyKey = sagaId + "-" + stepName + "-" + eventType;

        // 1. In-memory dedup (Caffeine cache — fast path)
        if (deduplicationCache.isDuplicate(idempotencyKey)) {
            log.info("Duplicate event for saga {} (key={}, cache hit). Skipping.", sagaId, idempotencyKey);
            return;
        }

        // 2. DB-level dedup (fallback for cache miss after restart)
        if (sagaEventRepository.findByEventId(eventId).isPresent()) {
            log.info("Duplicate event {} for saga {} (DB hit). Skipping.", eventId, sagaId);
            return;
        }

        // 3. Upsert saga instance
        SagaInstance saga = sagaInstanceRepository.findById(sagaId).orElse(null);
        String previousStep = null;
        if (saga == null) {
            saga = new SagaInstance();
            saga.setSagaId(sagaId);
            saga.setStatus(SagaStatus.STARTED);
            saga.setStartTime(eventTimestamp);
            saga.setLastUpdated(eventTimestamp);
            log.info("New saga detected: {}", sagaId);
        } else {
            // Track previous step for transition validation
            var previousEvents = sagaEventRepository.findBySagaIdOrderByTimestampAsc(sagaId);
            if (!previousEvents.isEmpty()) {
                previousStep = previousEvents.get(previousEvents.size() - 1).getEventType();
            }
        }

        // 4. Update saga status
        SagaStatus newStatus = determineSagaStatus(eventType, saga.getStatus());
        saga.setStatus(newStatus);
        saga.setLastUpdated(eventTimestamp);

        if (isTerminalStatus(newStatus)) {
            saga.setEndTime(eventTimestamp);
        }

        sagaInstanceRepository.save(saga);

        // 5. Store event
        SagaEvent sagaEvent = new SagaEvent();
        sagaEvent.setEventId(eventId);
        sagaEvent.setSagaId(sagaId);
        sagaEvent.setEventType(eventType);
        sagaEvent.setStepName(stepName);
        sagaEvent.setServiceName(serviceName);
        sagaEvent.setTimestamp(eventTimestamp);
        sagaEvent.setPayload(payload);
        sagaEventRepository.save(sagaEvent);

        log.info("Saga {} | event={} | step={} | status={}", sagaId, eventType, stepName, newStatus);

        // 6. Anomaly detection
        anomalyDetectionService.detectOutOfOrder(sagaId);
        anomalyDetectionService.detectMissingCompletion(sagaId);

        // 7. State machine transition validation
        String expectedStep = workflowLoader.getOrderSagaWorkflow().getExpectedStepForEvent(eventType);
        if (expectedStep != null && previousStep != null) {
            anomalyDetectionService.detectInvalidTransition(sagaId, previousStep, eventType);
        }
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
