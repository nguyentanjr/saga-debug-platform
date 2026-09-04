package com.example.correlation.service;

import com.example.correlation.domain.IngestedEvent;
import com.example.correlation.domain.SagaEvent;
import com.example.correlation.domain.SagaInstance;
import com.example.correlation.domain.SagaStatus;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.example.correlation.workflow.WorkflowRegistry;
import com.example.correlation.workflow.WorkflowSpec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
public class CorrelationService {

    private static final Logger log = LoggerFactory.getLogger(CorrelationService.class);

    private final SagaInstanceRepository sagaInstanceRepository;
    private final SagaEventRepository sagaEventRepository;
    private final AnomalyDetectionService anomalyDetectionService;
    private final DeduplicationCache deduplicationCache;
    private final WorkflowRegistry workflowRegistry;
    private final ObjectMapper objectMapper;


    public CorrelationService(SagaInstanceRepository sagaInstanceRepository,
                              SagaEventRepository sagaEventRepository,
                              AnomalyDetectionService anomalyDetectionService,
                              DeduplicationCache deduplicationCache,
                              WorkflowRegistry workflowRegistry,
                              ObjectMapper objectMapper) {
        this.sagaInstanceRepository = sagaInstanceRepository;
        this.sagaEventRepository = sagaEventRepository;
        this.anomalyDetectionService = anomalyDetectionService;
        this.deduplicationCache = deduplicationCache;
        this.workflowRegistry = workflowRegistry;
        this.objectMapper = objectMapper;
    }

    /** Kept for callers that have no causal metadata to report. */
    @Transactional
    public void processEvent(String eventId, String sagaId, String eventType,
                             String stepName, String serviceName,
                             Instant eventTimestamp, String payload,
                             Instant receivedAt) {
        processEvent(IngestedEvent.withoutCausality(eventId, sagaId, eventType, stepName,
                serviceName, eventTimestamp, payload, receivedAt));
    }

    @Transactional
    public void processEvent(IngestedEvent observation) {
        String eventId = observation.eventId();
        String sagaId = observation.sagaId();
        String eventType = observation.eventType();
        String stepName = observation.stepName();
        String serviceName = observation.serviceName();
        Instant eventTimestamp = observation.eventTime();
        String payload = observation.payload();
        Instant receivedAt = observation.receivedAt();

        if (sagaEventRepository.findByEventId(eventId).isPresent()) {
            log.info("Duplicate event {} for saga {} (DB hit). Skipping.", eventId, sagaId);
            anomalyDetectionService.detectDuplicateEvent(sagaId, eventId, eventType);
            return;
        }

        
        SagaInstance saga = sagaInstanceRepository.findById(sagaId).orElse(null);
        String previousStep = null;
        SagaStatus oldStatus = null;

        // Loaded once and reused by every detector below. Each detector used to issue
        // its own findBySagaIdOrderBy... for the same rows, so processing one event
        // read the saga's whole history five to six times over.
        List<SagaEvent> priorEvents = sagaEventRepository.findBySagaIdOrderByTimestampAsc(sagaId);

        // Resolved per execution: by the saga's workflow type once it is known, and by
        // the event's own start-event membership when creating the saga.
        WorkflowSpec workflow = workflowRegistry
                .resolve(saga != null ? saga.getWorkflowType() : null, eventType)
                .orElse(null);

        if (saga == null) {
            boolean startsAWorkflow = workflow != null
                    && workflow.startEventTypes().contains(eventType);
            if (!startsAWorkflow) {
                anomalyDetectionService.detectOrphanEvent(sagaId, eventId, eventType, eventTimestamp);
            }

            saga = new SagaInstance();
            saga.setSagaId(sagaId);
            saga.setStatus(SagaStatus.STARTED);
            saga.setStartTime(eventTimestamp);
            saga.setLastUpdated(eventTimestamp);
            if (workflow != null) {
                saga.setWorkflowType(workflow.getName());
            }
            
            saga.setOrderId(extractOrderId(payload));
            log.info("New saga detected: {} (workflow={})", sagaId, saga.getWorkflowType());
        } else {
            oldStatus = saga.getStatus();
            
            anomalyDetectionService.detectForbiddenPostTerminal(sagaId, oldStatus, eventType, eventId, eventTimestamp);

            
            if (!priorEvents.isEmpty()) {
                previousStep = priorEvents.get(priorEvents.size() - 1).getEventType();
            }
        }

        
        SagaStatus newStatus = determineSagaStatus(workflow, eventType, saga.getStatus());
        saga.setStatus(newStatus);
        saga.setLastUpdated(eventTimestamp);

        if (isTerminalStatus(newStatus)) {
            saga.setEndTime(eventTimestamp);
        }

        
        if (oldStatus != null) {
            anomalyDetectionService.detectStateRegression(sagaId, oldStatus, newStatus, eventId, eventTimestamp);
        }

        sagaInstanceRepository.save(saga);

        
        SagaEvent sagaEvent = new SagaEvent();
        sagaEvent.setEventId(eventId);
        sagaEvent.setSagaId(sagaId);
        sagaEvent.setEventType(eventType);
        sagaEvent.setStepName(stepName);
        sagaEvent.setServiceName(serviceName);
        sagaEvent.setTimestamp(eventTimestamp);
        sagaEvent.setPayload(payload);
        sagaEvent.setReceivedAt(receivedAt);
        sagaEvent.setCausationId(observation.causationId());
        sagaEvent.setSpanId(observation.spanId());
        sagaEvent.setParentSpanId(observation.parentSpanId());
        sagaEventRepository.save(sagaEvent);

        log.info("Saga {} | event={} | step={} | status={}", sagaId, eventType, stepName, newStatus);

        
        List<SagaEvent> sagaEvents = new ArrayList<>(priorEvents);
        sagaEvents.add(sagaEvent);
        sagaEvents.sort(Comparator.comparing(SagaEvent::getTimestamp));

        anomalyDetectionService.detectOutOfOrder(sagaId, sagaEvents);
        anomalyDetectionService.detectRetryBurst(workflow, sagaId, sagaEvents);
        anomalyDetectionService.detectMultiTerminalConflict(workflow, sagaId, sagaEvents);
        anomalyDetectionService.detectDuplicateCompensation(workflow, sagaId, sagaEvents);
        anomalyDetectionService.detectMissingTraceContext(
                sagaId, eventType, eventId, payload, eventTimestamp, sagaEvents);

        
        if (workflow != null && previousStep != null && !isCompensationStepEvent(workflow, eventType)) {
            String previousStepName = workflow.getExpectedStepForEvent(previousStep);
            String currentStepName = workflow.getExpectedStepForEvent(eventType);
            String condition = transitionCondition(workflow, eventType);
            if (previousStepName != null && currentStepName != null) {
                anomalyDetectionService.detectInvalidTransition(workflow,
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
            
        }
        return null;
    }

    /**
     * The saga's status after this event, derived from the workflow graph.
     *
     * <p>Was four hard-coded event sets naming the order saga's events. Delegating to
     * the spec means the answer follows the workflow's shape: a step's success ends the
     * saga only when the spec says that step transitions to COMPLETED.
     *
     * @return the current status unchanged when the event says nothing about it, which
     *         includes retries and any event no loaded workflow recognises
     */
    private SagaStatus determineSagaStatus(WorkflowSpec workflow, String eventType,
            SagaStatus currentStatus) {
        if (workflow == null) {
            return currentStatus == SagaStatus.STARTED ? SagaStatus.IN_PROGRESS : currentStatus;
        }

        SagaStatus derived = workflow.classifyEvent(eventType)
                .flatMap(workflow::statusAfter)
                .orElse(currentStatus);

        // An unrecognised event should not leave a saga stuck in STARTED forever.
        if (derived == currentStatus && currentStatus == SagaStatus.STARTED) {
            return SagaStatus.IN_PROGRESS;
        }
        return derived;
    }

    /** The transition condition an event represents, per the step lifecycle it belongs to. */
    private String transitionCondition(WorkflowSpec workflow, String eventType) {
        return workflow.classifyEvent(eventType)
                // The workflow's own outcome name, so a step declaring APPROVED /
                // MANUAL_REVIEW / REJECTED is validated against the transition declared
                // for each, rather than being flattened to success-or-failure.
                .map(role -> role.outcome() != null
                        ? role.outcome()
                        : WorkflowSpec.SUCCESS_OUTCOME)
                .orElse(WorkflowSpec.SUCCESS_OUTCOME);
    }

    private boolean isTerminalStatus(SagaStatus status) {
        return status == SagaStatus.COMPLETED
                || status == SagaStatus.FAILED
                || status == SagaStatus.COMPENSATED;
    }
}
