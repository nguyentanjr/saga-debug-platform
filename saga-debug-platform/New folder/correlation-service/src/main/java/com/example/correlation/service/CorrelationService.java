package com.example.correlation.service;

import com.example.correlation.domain.SagaEvent;
import com.example.correlation.domain.SagaInstance;
import com.example.correlation.domain.SagaStatus;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Set;

/**
 * Correlates Kafka events into saga instances.
 */
@Service
public class CorrelationService {

    private static final Logger log = LoggerFactory.getLogger(CorrelationService.class);

    private final SagaInstanceRepository sagaInstanceRepository;
    private final SagaEventRepository sagaEventRepository;

    private static final Set<String> COMPLETED_EVENTS = Set.of("INVENTORY_RESERVED");
    private static final Set<String> FAILED_EVENTS = Set.of("PAYMENT_FAILED");
    private static final Set<String> COMPENSATING_EVENTS = Set.of("INVENTORY_FAILED", "REFUND_PAYMENT");
    private static final Set<String> COMPENSATED_EVENTS = Set.of("PAYMENT_REFUNDED");

    public CorrelationService(SagaInstanceRepository sagaInstanceRepository,
                              SagaEventRepository sagaEventRepository) {
        this.sagaInstanceRepository = sagaInstanceRepository;
        this.sagaEventRepository = sagaEventRepository;
    }

    @Transactional
    public void processEvent(String eventId, String sagaId, String eventType,
                             String stepName, String serviceName,
                             Instant eventTimestamp, String payload) {

        // Skip duplicate events
        if (sagaEventRepository.findByEventId(eventId).isPresent()) {
            log.info("Duplicate event {}. Skipping.", eventId);
            return;
        }

        // Upsert saga
        SagaInstance saga = sagaInstanceRepository.findById(sagaId).orElse(null);
        if (saga == null) {
            saga = new SagaInstance();
            saga.setSagaId(sagaId);
            saga.setStatus(SagaStatus.STARTED);
            saga.setStartTime(eventTimestamp);
            saga.setLastUpdated(eventTimestamp);
            log.info("New saga: {}", sagaId);
        }

        // Update status
        SagaStatus newStatus = determineSagaStatus(eventType, saga.getStatus());
        saga.setStatus(newStatus);
        saga.setLastUpdated(eventTimestamp);
        if (isTerminal(newStatus)) {
            saga.setEndTime(eventTimestamp);
        }
        sagaInstanceRepository.save(saga);

        // Store event
        SagaEvent sagaEvent = new SagaEvent();
        sagaEvent.setEventId(eventId);
        sagaEvent.setSagaId(sagaId);
        sagaEvent.setEventType(eventType);
        sagaEvent.setStepName(stepName);
        sagaEvent.setServiceName(serviceName);
        sagaEvent.setTimestamp(eventTimestamp);
        sagaEvent.setPayload(payload);
        sagaEventRepository.save(sagaEvent);

        log.info("Saga {} | {} | {} | {}", sagaId, eventType, stepName, newStatus);
    }

    private SagaStatus determineSagaStatus(String eventType, SagaStatus current) {
        if (COMPLETED_EVENTS.contains(eventType)) return SagaStatus.COMPLETED;
        if (FAILED_EVENTS.contains(eventType)) return SagaStatus.FAILED;
        if (COMPENSATING_EVENTS.contains(eventType)) return SagaStatus.COMPENSATING;
        if (COMPENSATED_EVENTS.contains(eventType)) return SagaStatus.COMPENSATED;
        if (current == SagaStatus.STARTED) return SagaStatus.IN_PROGRESS;
        return current;
    }

    private boolean isTerminal(SagaStatus status) {
        return status == SagaStatus.COMPLETED
                || status == SagaStatus.FAILED
                || status == SagaStatus.COMPENSATED;
    }
}
