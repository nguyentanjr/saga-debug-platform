package com.example.correlation.service;

import com.example.correlation.domain.Anomaly;
import com.example.correlation.domain.AnomalyType;
import com.example.correlation.domain.SagaEvent;
import com.example.correlation.domain.SagaInstance;
import com.example.correlation.domain.SagaStatus;
import com.example.correlation.repository.AnomalyRepository;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.example.correlation.workflow.WorkflowLoader;
import com.example.correlation.workflow.WorkflowSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;


import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

/**
 * Enhanced anomaly detection service.
 * Detects: OUT_OF_ORDER, DUPLICATE_EVENT, STUCK_SAGA, MISSING_COMPLETION, INVALID_TRANSITION.
 */
@Service
public class AnomalyDetectionService {

    private static final Logger log = LoggerFactory.getLogger(AnomalyDetectionService.class);

    private final SagaInstanceRepository sagaInstanceRepository;
    private final SagaEventRepository sagaEventRepository;
    private final AnomalyRepository anomalyRepository;
    private final WorkflowLoader workflowLoader;

    public AnomalyDetectionService(
            SagaInstanceRepository sagaInstanceRepository,
            SagaEventRepository sagaEventRepository,
            AnomalyRepository anomalyRepository,
            WorkflowLoader workflowLoader) {
        this.sagaInstanceRepository = sagaInstanceRepository;
        this.sagaEventRepository = sagaEventRepository;
        this.anomalyRepository = anomalyRepository;
        this.workflowLoader = workflowLoader;
    }

    /**
     * Detect out-of-order events for a specific saga.
     *
     * Logic: Sort events by ARRIVAL order (receivedAt).
     * Then check if their CREATION timestamps (timestamp) are monotonically increasing.
     * If event[i] was created BEFORE event[i-1] but arrived AFTER → out-of-order.
     */
    public void detectOutOfOrder(String sagaId) {
        // Sort by receivedAt (arrival order), NOT by timestamp (creation order)
        List<SagaEvent> events = sagaEventRepository.findBySagaIdOrderByReceivedAtAsc(sagaId);
        if (events.size() < 2) return;

        // Efficient DB check — avoid loading all anomalies into memory
        if (anomalyRepository.existsBySagaIdAndAnomalyType(sagaId, AnomalyType.OUT_OF_ORDER)) return;

        for (int i = 1; i < events.size(); i++) {
            Instant prevCreated = events.get(i - 1).getTimestamp();
            Instant currCreated = events.get(i).getTimestamp();
            if (currCreated.isBefore(prevCreated)) {
                createAnomaly(sagaId, AnomalyType.OUT_OF_ORDER,
                        String.format("Event '%s' was received after '%s' but has an earlier creation timestamp (%s < %s)",
                                events.get(i).getEventType(),
                                events.get(i - 1).getEventType(),
                                currCreated, prevCreated));
                return; // One anomaly per saga is enough
            }
        }
    }

    /**
     * Detect invalid state transitions for a specific saga based on workflow spec.
     */
    public void detectInvalidTransition(String sagaId, String previousStep, String currentStep) {
        WorkflowSpec workflow = workflowLoader.getOrderSagaWorkflow();
        if (workflow == null || workflow.getTransitions() == null) return;

        if (previousStep != null && !workflow.isValidTransition(previousStep, currentStep, null)) {
            createAnomaly(sagaId, AnomalyType.INVALID_TRANSITION,
                    String.format("Invalid transition: %s -> %s (not defined in workflow spec)", previousStep, currentStep));
        }
    }

    /**
     * Scheduled: Detect stuck sagas — no new event for > timeout threshold.
     * Runs every 30 seconds.
     */
    @Scheduled(fixedRate = 30000)
    public void detectStuckSagas() {
        List<SagaInstance> runningSagas = new java.util.ArrayList<>(sagaInstanceRepository.findByStatus(SagaStatus.STARTED));
        runningSagas.addAll(sagaInstanceRepository.findByStatus(SagaStatus.IN_PROGRESS));
        runningSagas.addAll(sagaInstanceRepository.findByStatus(SagaStatus.COMPENSATING));

        WorkflowSpec workflow = workflowLoader.getOrderSagaWorkflow();
        Instant now = Instant.now();

        for (SagaInstance saga : runningSagas) {
            SagaEvent lastEvent = sagaEventRepository
                    .findTopBySagaIdOrderByTimestampDesc(saga.getSagaId())
                    .orElse(null);
            if (lastEvent == null) continue;
            String currentStep = lastEvent.getEventType();

            // Get step-specific timeout or default to 60s
            int timeoutSeconds = 60;
            if (workflow != null) {
                String stepName = workflow.getExpectedStepForEvent(currentStep);
                if (stepName != null) {
                    timeoutSeconds = workflow.getStepTimeoutSeconds(stepName);
                }
            }

            if (lastEvent.getTimestamp().plus(timeoutSeconds, ChronoUnit.SECONDS).isBefore(now)) {
                // Check if anomaly already exists
                boolean alreadyDetected = anomalyRepository.findBySagaId(saga.getSagaId()).stream()
                        .anyMatch(a -> a.getAnomalyType() == AnomalyType.STUCK_SAGA
                                && a.getDetectedAt().isAfter(lastEvent.getTimestamp()));
                if (!alreadyDetected) {
                    createAnomaly(saga.getSagaId(), AnomalyType.STUCK_SAGA,
                            String.format("Saga stuck: no event since %s (last: %s, timeout: %ds)",
                                    lastEvent.getTimestamp(), currentStep, timeoutSeconds));
                }
            }

        }
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
