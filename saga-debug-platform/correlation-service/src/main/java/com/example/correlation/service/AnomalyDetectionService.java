package com.example.correlation.service;

import com.example.correlation.domain.*;
import com.example.correlation.execution.ExecutionProjection;
import com.example.correlation.execution.ExecutionProjectionService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.example.correlation.repository.AnomalyRepository;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.example.correlation.workflow.WorkflowRegistry;
import com.example.correlation.workflow.WorkflowSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class AnomalyDetectionService {

    private static final Logger log = LoggerFactory.getLogger(AnomalyDetectionService.class);

    private static final List<SagaStatus> RUNNING_STATUSES =
            List.of(SagaStatus.STARTED, SagaStatus.IN_PROGRESS, SagaStatus.COMPENSATING);
    private static final List<SagaStatus> IN_FLIGHT_STATUSES =
            List.of(SagaStatus.IN_PROGRESS, SagaStatus.COMPENSATING);
    private static final List<SagaStatus> SETTLED_STATUSES =
            List.of(SagaStatus.COMPLETED, SagaStatus.COMPENSATED);
    private final SagaInstanceRepository sagaInstanceRepository;
    private final SagaEventRepository sagaEventRepository;
    private final AnomalyRepository anomalyRepository;
    private final WorkflowRegistry workflowRegistry;
    private final ObjectMapper objectMapper;
    private final DetectionProperties detectionProperties;
    private final ExecutionProjectionService executionProjectionService;

    public AnomalyDetectionService(
            SagaInstanceRepository sagaInstanceRepository,
            SagaEventRepository sagaEventRepository,
            AnomalyRepository anomalyRepository,
            WorkflowRegistry workflowRegistry,
            ObjectMapper objectMapper,
            DetectionProperties detectionProperties,
            ExecutionProjectionService executionProjectionService) {
        this.sagaInstanceRepository = sagaInstanceRepository;
        this.sagaEventRepository = sagaEventRepository;
        this.anomalyRepository = anomalyRepository;
        this.workflowRegistry = workflowRegistry;
        this.objectMapper = objectMapper;
        this.detectionProperties = detectionProperties;
        this.executionProjectionService = executionProjectionService;
    }


    /**
     * @param sagaEvents every event of the saga, including the one just stored.
     *                   Passed in rather than re-queried: the ingestion path used to
     *                   reload the whole event list once per detector, so a saga with
     *                   n events cost O(n^2) row reads over its lifetime.
     */
    public void detectOutOfOrder(String sagaId, List<SagaEvent> sagaEvents) {
        List<SagaEvent> events = sagaEvents.stream()
                .sorted(Comparator.comparing(SagaEvent::getReceivedAt,
                        Comparator.nullsLast(Comparator.naturalOrder())))
                .toList();
        if (events.size() < 2) return;
        if (anomalyRepository.existsBySagaIdAndAnomalyType(sagaId, AnomalyType.OUT_OF_ORDER)) return;

        for (int i = 1; i < events.size(); i++) {
            SagaEvent previousEvent = events.get(i - 1);
            SagaEvent currentEvent = events.get(i);
            Instant prevCreated = previousEvent.getTimestamp();
            Instant currCreated = currentEvent.getTimestamp();
            Duration timeDiff = Duration.between(currCreated, prevCreated);
            
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



    public void detectInvalidTransition(WorkflowSpec workflow, String sagaId, String previousStep,
            String currentStep, String condition, String eventId, Instant eventTimestamp) {
        if (workflow == null || workflow.getTransitions() == null) return;

        if (previousStep == null) return;
        // Only a declared-and-contradicted transition is an anomaly. UNKNOWN means the
        // spec has nothing to say about this step: a gap in the specification, not a
        // fault in the execution.
        if (workflow.judgeTransition(previousStep, currentStep, condition)
                != WorkflowSpec.TransitionVerdict.INVALID) return;

        {
            String expected = workflow.getTransitions().stream()
                    .filter(t -> t.getFrom().equals(previousStep))
                    .map(t -> t.getTo() + " (on: " + t.getCondition() + ")")
                    .collect(Collectors.joining(", "));

            createAnomaly(sagaId, AnomalyType.INVALID_TRANSITION,
                    String.format("Invalid transition: %s -> %s (condition: %s). Expected from %s: [%s]",
                            previousStep, currentStep, condition, previousStep,
                            expected.isEmpty() ? "none defined" : expected),
                    details()
                            .observedValue(String.format("%s -> %s on %s", previousStep, currentStep, condition))
                            .expectedValue(expected.isEmpty() ? "no transition defined from " + previousStep : expected)
                            .thresholdSource("workflow:" + workflow.getName() + ".transitions")
                            .evidenceEventIds(eventId)
                            .measuredAt(eventTimestamp));
        }
    }

    public void detectRetryBurst(WorkflowSpec workflow, String sagaId, List<SagaEvent> events) {
        int burstThreshold = detectionProperties.getRetryBurstThreshold();
        Duration burstWindow = detectionProperties.getRetryBurstWindow();
        if (events.size() < burstThreshold) return;
        if (anomalyRepository.existsBySagaIdAndAnomalyType(sagaId, AnomalyType.RETRY_BURST)) return;

        // No PAYMENT_RETRY fallback: a workflow that declares no retry event does not
        // have retries as far as this platform can tell, and borrowing the order saga's
        // retry event name would attribute another workflow's events to a step it has no
        // relationship with.
        Set<String> retryEventTypes = workflow != null ? workflow.retryEventTypes() : Set.of();
        if (retryEventTypes.isEmpty()) return;

        List<SagaEvent> retryEvents = events.stream()
                .filter(e -> retryEventTypes.contains(e.getEventType()))
                .collect(Collectors.toList());
        if (retryEvents.size() < burstThreshold) return;

        for (int i = 0; i <= retryEvents.size() - burstThreshold; i++) {
            SagaEvent first = retryEvents.get(i);
            SagaEvent last = retryEvents.get(i + burstThreshold - 1);

            Duration gap = Duration.between(first.getTimestamp(), last.getTimestamp());
            if (gap.compareTo(burstWindow) <= 0) {
                List<SagaEvent> burstEvents = retryEvents.subList(i, i + burstThreshold);
                createAnomaly(sagaId, AnomalyType.RETRY_BURST,
                        String.format("%d retry events in %ds — possible retry storm%s",
                                burstThreshold, gap.getSeconds(),
                                retryPolicyNote(workflow, first.getEventType(), retryEvents.size())),
                        details()
                                .observedValue(String.format("%d retry events in %dms%s",
                                        burstEvents.size(), gap.toMillis(),
                                        retryPolicyNote(workflow, first.getEventType(), retryEvents.size())))
                                .expectedValue(String.format("fewer than %d retry events within %ds",
                                        burstThreshold, burstWindow.toSeconds()))
                                .thresholdSource("config:anomaly.detection.retry-burst")
                                .evidenceEvents(burstEvents)
                                .measuredAt(last.getTimestamp()));
                return;
            }
        }
    }

    /**
     * States how the observed retry count compares with the step's declared attempt
     * budget.
     *
     * <p>The engine-side burst threshold and the service's Resilience4j
     * {@code maxAttempts} were chosen to match each other, so a firing RETRY_BURST does
     * not by itself show that anything exceeded its policy. Recording both numbers in
     * the evidence makes that visible to whoever reads the finding, instead of leaving
     * "retry storm" to imply a violation that was never measured.
     */
    private String retryPolicyNote(WorkflowSpec workflow, String retryEventType, int observedRetries) {
        if (workflow == null) return "";

        Optional<Integer> declared = workflow.classifyEvent(retryEventType)
                .map(WorkflowSpec.EventRole::stepId)
                .flatMap(workflow::declaredMaxAttempts);
        if (declared.isEmpty()) return "";

        int allowedRetries = Math.max(0, declared.get() - 1);
        return String.format(" [observed %d retries; declared policy allows %d — %s policy]",
                observedRetries, allowedRetries,
                observedRetries > allowedRetries ? "exceeds" : "within");
    }

    public void detectDuplicateEvent(String sagaId, String eventId, String eventType) {
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

    /**
     * Two contradictory terminal outcomes observed for one saga.
     *
     * <p>Was two hard-coded event sets naming the order saga's success and failure
     * events. The question is now asked of the workflow: which observations imply a
     * terminal status, and did this saga produce both a success and a non-success one.
     */
    public void detectMultiTerminalConflict(WorkflowSpec workflow, String sagaId, List<SagaEvent> events) {
        if (workflow == null) return;
        if (anomalyRepository.existsBySagaIdAndAnomalyType(sagaId, AnomalyType.MULTI_TERMINAL_CONFLICT)) return;

        List<SagaEvent> succeeded = terminalEvents(workflow, events, SagaStatus.COMPLETED);
        List<SagaEvent> notSucceeded = new ArrayList<>(
                terminalEvents(workflow, events, SagaStatus.FAILED));
        notSucceeded.addAll(terminalEvents(workflow, events, SagaStatus.COMPENSATED));

        if (succeeded.isEmpty() || notSucceeded.isEmpty()) return;

        List<SagaEvent> terminalEvidence = new ArrayList<>(succeeded);
        terminalEvidence.addAll(notSucceeded);
        terminalEvidence.sort(Comparator.comparing(SagaEvent::getTimestamp));

        String successTypes = distinctTypes(succeeded);
        String failTypes = distinctTypes(notSucceeded);
        createAnomaly(sagaId, AnomalyType.MULTI_TERMINAL_CONFLICT,
                String.format("Both success [%s] and non-success [%s] terminal events present in same saga",
                        successTypes, failTypes),
                details()
                        .observedValue(String.format("success=[%s], non-success=[%s]", successTypes, failTypes))
                        .expectedValue("only one terminal outcome per saga")
                        .thresholdSource("workflow:" + workflow.getName() + ".terminal-states")
                        .evidenceEvents(terminalEvidence)
                        .measuredAt(lastTimestamp(terminalEvidence)));
    }

    /** Events whose lifecycle role puts the saga into the given terminal status. */
    private List<SagaEvent> terminalEvents(WorkflowSpec workflow, List<SagaEvent> events,
            SagaStatus terminal) {
        return events.stream()
                .filter(e -> workflow.classifyEvent(e.getEventType())
                        .flatMap(workflow::statusAfter)
                        .filter(status -> status == terminal)
                        .isPresent())
                .toList();
    }

    public void detectDuplicateCompensation(WorkflowSpec workflow, String sagaId, List<SagaEvent> events) {
        if (workflow == null) return;
        List<SagaEvent> compStartEvents = compensationEvents(
                workflow, events, WorkflowSpec.EventRole.Role.STARTED);
        long startCount = compStartEvents.size();
        if (startCount > 1 && !anomalyRepository.existsBySagaIdAndAnomalyType(
                sagaId, AnomalyType.DUPLICATE_COMPENSATION)) {
            String types = distinctTypes(compStartEvents);
            createAnomaly(sagaId, AnomalyType.DUPLICATE_COMPENSATION,
                    String.format("Compensation started %d times (%s) - duplicate compensation",
                            startCount, types),
                    details()
                            .observedValue(String.format("%s start count=%d", types, startCount))
                            .expectedValue("compensation starts once per compensation attempt")
                            .thresholdSource("workflow:" + workflow.getName() + ".compensation.cardinality")
                            .evidenceEvents(compStartEvents)
                            .measuredAt(lastTimestamp(compStartEvents)));
        }
    }

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

    


    public void detectMissingTraceContext(String sagaId, String eventType, String eventId, String payload,
            Instant eventTimestamp, List<SagaEvent> sagaEvents) {
        if (anomalyRepository.existsBySagaIdAndAnomalyType(sagaId, AnomalyType.MISSING_TRACE_CONTEXT)) return;
        boolean hasTraceId = false;
        if (payload != null && !payload.isBlank()) {
            try {
                JsonNode node = objectMapper.readTree(payload);
                hasTraceId = node.has("traceId") && !node.get("traceId").isNull()
                        && !node.get("traceId").asText().isBlank();
            } catch (Exception e) {
                
            }
        }
        if (!hasTraceId) {
            boolean sagaHasAnyTrace = sagaEvents.stream()
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


    @Scheduled(fixedRateString = "${anomaly.detection.schedule-rate-ms:15000}")
    public void detectStuckSagas() {
        List<SagaInstance> runningSagas = sagaInstanceRepository.findByStatusIn(
                RUNNING_STATUSES, detectionBatch());

        Instant now = Instant.now();

        for (SagaInstance saga : runningSagas) {
            WorkflowSpec workflow = specFor(saga);
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

        
        Instant recentCutoff = Instant.now().minus(detectionProperties.getCompletedSagaLookback());
        List<SagaInstance> activeSagas = new ArrayList<>(
                sagaInstanceRepository.findByStatusIn(IN_FLIGHT_STATUSES, detectionBatch()));
        activeSagas.addAll(sagaInstanceRepository.findByStatusInAndEndTimeAfter(
                SETTLED_STATUSES, recentCutoff, detectionBatch()));

        Map<String, List<SagaEvent>> eventsBySaga = eventsFor(activeSagas, AnomalyType.STEP_TIMEOUT);

        Map<String, WorkflowSpec> specBySaga = activeSagas.stream()
                .filter(saga -> specFor(saga) != null)
                .collect(Collectors.toMap(SagaInstance::getSagaId, this::specFor, (a, b) -> a));

        for (Map.Entry<String, List<SagaEvent>> entry : eventsBySaga.entrySet()) {
            if (entry.getValue().size() < 2) continue;
            WorkflowSpec workflow = specBySaga.get(entry.getKey());
            if (workflow == null) continue;
            evaluateStepTimeouts(entry.getKey(), entry.getValue(), workflow);
        }
    }

    /**
     * Loads the events for a batch of sagas in one query, dropping the ones that
     * already carry {@code alreadyDetected}.
     *
     * <p>The scheduler loops used to issue two queries per saga per pass — an exists
     * check and an event fetch — so a pass cost O(batch) round trips. This makes it
     * two, regardless of batch size.
     */
    private Map<String, List<SagaEvent>> eventsFor(List<SagaInstance> sagas, AnomalyType alreadyDetected) {
        if (sagas.isEmpty()) return Map.of();

        List<String> sagaIds = sagas.stream().map(SagaInstance::getSagaId).distinct().toList();
        Set<String> skip = Set.copyOf(
                anomalyRepository.findSagaIdsWithAnomalyType(alreadyDetected, sagaIds));

        List<String> pending = sagaIds.stream().filter(id -> !skip.contains(id)).toList();
        if (pending.isEmpty()) return Map.of();

        return sagaEventRepository.findBySagaIdInOrderBySagaIdAscTimestampAsc(pending).stream()
                .collect(Collectors.groupingBy(SagaEvent::getSagaId,
                        LinkedHashMap::new, Collectors.toList()));
    }

    /**
     * Runs the configured step-timeout implementation for one saga.
     *
     * <p>Package-visible and repository-free apart from {@code createAnomaly} so the
     * legacy and projection measurements can be compared directly in tests.
     */
    void evaluateStepTimeouts(String sagaId, List<SagaEvent> events, WorkflowSpec workflow) {
        DetectionProperties.StepTimeoutMode mode = detectionProperties.getStepTimeoutMode();

        StepTimeoutFinding legacy = (mode == DetectionProperties.StepTimeoutMode.LEGACY
                || mode == DetectionProperties.StepTimeoutMode.SHADOW)
                ? findStepTimeoutByAdjacency(events, workflow)
                : null;

        StepTimeoutFinding projected = (mode == DetectionProperties.StepTimeoutMode.PROJECTION
                || mode == DetectionProperties.StepTimeoutMode.SHADOW)
                ? findStepTimeoutByProjection(sagaId, events, workflow)
                : null;

        if (mode == DetectionProperties.StepTimeoutMode.SHADOW) {
            logDivergence(sagaId, legacy, projected);
        }

        StepTimeoutFinding authoritative =
                mode == DetectionProperties.StepTimeoutMode.PROJECTION ? projected : legacy;
        if (authoritative != null) {
            createAnomaly(sagaId, AnomalyType.STEP_TIMEOUT, authoritative.description(),
                    authoritative.details());
        }
    }

    /**
     * Legacy measurement: the gap between an event and its positional predecessor.
     *
     * <p>Kept intact while SHADOW mode gathers comparison data. Its known weakness is
     * that any intervening observation — a retry event, or an event mapping to no step —
     * becomes the measurement's start point, so the reported duration of a step depends
     * on observations that are not part of that step.
     */
    private StepTimeoutFinding findStepTimeoutByAdjacency(List<SagaEvent> events, WorkflowSpec workflow) {
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
                return new StepTimeoutFinding(stepName, stepDurationMs, expectedTimeoutMs,
                        String.format("Step '%s' took %dms (timeout: %dms). Previous event: %s",
                                stepName, stepDurationMs, expectedTimeoutMs,
                                previousEvent.getEventType()),
                        details()
                                .observedValue(String.format("%s took %dms", stepName, stepDurationMs))
                                .expectedValue(String.format("<= %dms", expectedTimeoutMs))
                                .thresholdSource("workflow:" + stepName + ".timeout")
                                .evidenceEvents(previousEvent, currentEvent)
                                .measuredAt(currentEvent.getTimestamp()));
            }
        }
        return null;
    }

    /**
     * Projection measurement: an operation's duration from its start reference to its
     * terminal observation, with retries folded into the same operation.
     *
     * <p>Operations whose start could not be established are skipped rather than
     * measured against a substitute boundary — absence of evidence is reported as
     * absence, not as a fast step.
     */
    private StepTimeoutFinding findStepTimeoutByProjection(String sagaId, List<SagaEvent> events,
            WorkflowSpec workflow) {
        ExecutionProjection projection = executionProjectionService.project(sagaId, events, workflow);

        for (ExecutionProjection.Operation op : projection.measurableOperations()) {
            long durationMs = op.duration().toMillis();
            int expectedTimeoutMs = workflow.getStepTimeoutSeconds(op.stepId()) * 1000;
            if (durationMs <= expectedTimeoutMs) continue;

            String provenance = op.startProvenance().name();
            return new StepTimeoutFinding(op.stepId(), durationMs, expectedTimeoutMs,
                    String.format("Operation '%s' took %dms across %d attempt(s) (timeout: %dms), "
                                    + "measured from %s",
                            op.stepId(), durationMs, op.attemptCount(), expectedTimeoutMs, provenance),
                    details()
                            .observedValue(String.format("%s took %dms over %d attempt(s); start=%s",
                                    op.stepId(), durationMs, op.attemptCount(), provenance))
                            .expectedValue(String.format("<= %dms", expectedTimeoutMs))
                            .thresholdSource("workflow:" + op.stepId() + ".timeout")
                            .evidenceEventIds(op.evidenceEventIds().toArray(new String[0]))
                            .measuredAt(op.endedAt()));
        }
        return null;
    }

    private void logDivergence(String sagaId, StepTimeoutFinding legacy, StepTimeoutFinding projected) {
        boolean legacyFired = legacy != null;
        boolean projectedFired = projected != null;

        if (!legacyFired && !projectedFired) return;

        if (legacyFired != projectedFired) {
            log.warn("STEP_TIMEOUT shadow divergence saga={}: legacy={} projection={}",
                    sagaId,
                    legacyFired ? legacy.summary() : "no-finding",
                    projectedFired ? projected.summary() : "no-finding");
        } else if (!legacy.stepId().equals(projected.stepId())
                || legacy.durationMs() != projected.durationMs()) {
            log.warn("STEP_TIMEOUT shadow divergence saga={}: legacy={} projection={}",
                    sagaId, legacy.summary(), projected.summary());
        } else {
            log.debug("STEP_TIMEOUT shadow agreement saga={}: {}", sagaId, legacy.summary());
        }
    }

    private record StepTimeoutFinding(String stepId, long durationMs, int thresholdMs,
            String description, AnomalyDetails details) {
        String summary() {
            return String.format("%s=%dms(>%dms)", stepId, durationMs, thresholdMs);
        }
    }

    @Scheduled(fixedRateString = "${anomaly.detection.schedule-rate-ms:15000}")
    public void detectMissingTerminalEvents() {
        // One cutoff bounds the query for every workflow, so it has to be the most
        // permissive step timeout in the registry; a per-saga check would need a
        // per-workflow query and buys nothing here.
        int maxTimeout = maxStepTimeoutAcrossWorkflows()
                * detectionProperties.getMissingTerminalTimeoutMultiplier();

        Instant cutoff = Instant.now().minus(maxTimeout, ChronoUnit.SECONDS);

        List<SagaInstance> stuckSagas = sagaInstanceRepository.findByStatusInAndLastUpdatedBefore(
                List.of(SagaStatus.STARTED, SagaStatus.IN_PROGRESS), cutoff, detectionBatch());

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

        
        
        List<SagaInstance> compensatingSagas = sagaInstanceRepository.findByStatusInAndLastUpdatedBefore(
                List.of(SagaStatus.COMPENSATING), cutoff, detectionBatch());

        for (SagaInstance saga : compensatingSagas) {
            if (saga.getLastUpdated().isAfter(cutoff)) continue; 
            if (anomalyRepository.existsBySagaIdAndAnomalyType(
                    saga.getSagaId(), AnomalyType.COMPENSATION_MISSING)) continue;

            WorkflowSpec workflow = specFor(saga);
            if (workflow == null) continue;

            List<SagaEvent> events = sagaEventRepository
                    .findBySagaIdOrderByTimestampAsc(saga.getSagaId());
            List<SagaEvent> failureEvents = eventsWithRole(workflow, events, WorkflowSpec.EventRole.Role.FAILED);
            boolean hasFailure = !failureEvents.isEmpty();
            boolean hasCompStart = !compensationEvents(
                    workflow, events, WorkflowSpec.EventRole.Role.STARTED).isEmpty();

            if (hasFailure && !hasCompStart) {
                String failureTypes = distinctTypes(failureEvents);
                createAnomaly(saga.getSagaId(), AnomalyType.COMPENSATION_MISSING,
                        String.format("Failure detected (%s) but no compensation started", failureTypes),
                        details()
                                .observedValue(failureTypes + " without any compensation start event")
                                .expectedValue("a compensation step starts after a step failure")
                                .thresholdSource("workflow:" + workflow.getName() + ".compensation")
                                .evidenceEvents(failureEvents)
                                .measuredAt(Instant.now()));
            }
        }
    }

    @Scheduled(fixedRateString = "${anomaly.detection.schedule-rate-ms:15000}")
    public void detectMissingCompensationAck() {
        Instant cutoff = Instant.now().minus(detectionProperties.getCompensationAckCutoff());

        List<SagaInstance> compensatingSagas = sagaInstanceRepository.findByStatusInAndLastUpdatedBefore(
                List.of(SagaStatus.COMPENSATING), cutoff, detectionBatch());

        for (SagaInstance saga : compensatingSagas) {
            if (anomalyRepository.existsBySagaIdAndAnomalyType(
                    saga.getSagaId(), AnomalyType.MISSING_COMPENSATION_ACK)) continue;

            WorkflowSpec workflow = specFor(saga);
            if (workflow == null) continue;

            List<SagaEvent> events = sagaEventRepository
                    .findBySagaIdOrderByTimestampAsc(saga.getSagaId());
            List<SagaEvent> compStartEvents = compensationEvents(
                    workflow, events, WorkflowSpec.EventRole.Role.STARTED);
            boolean hasCompStart = !compStartEvents.isEmpty();
            boolean hasCompDone = !compensationEvents(
                    workflow, events, WorkflowSpec.EventRole.Role.COMPLETED).isEmpty();

            if (hasCompStart && !hasCompDone) {
                String startedTypes = distinctTypes(compStartEvents);
                createAnomaly(saga.getSagaId(), AnomalyType.MISSING_COMPENSATION_ACK,
                        String.format("Compensation started (%s) but never acknowledged", startedTypes),
                        details()
                                .observedValue(startedTypes + " without a compensation completion event")
                                .expectedValue("the compensation step reports completion")
                                .thresholdSource("workflow:" + workflow.getName() + ".compensation.ack")
                                .evidenceEvents(compStartEvents)
                                .measuredAt(Instant.now()));
            }
        }
    }

    @Scheduled(fixedRateString = "${anomaly.detection.schedule-rate-ms:15000}")
    public void detectDelayedCompensation() {
        
        
        Instant recentCutoff = Instant.now().minus(detectionProperties.getCompensationRecentLookback());

        
        List<SagaInstance> compensatedSagas = sagaInstanceRepository.findByStatusInAndLastUpdatedAfter(
                List.of(SagaStatus.COMPENSATED, SagaStatus.COMPENSATING), recentCutoff, detectionBatch());

        for (SagaInstance saga : compensatedSagas) {
            if (anomalyRepository.existsBySagaIdAndAnomalyType(
                    saga.getSagaId(), AnomalyType.DELAYED_COMPENSATION)) continue;

            WorkflowSpec workflow = specFor(saga);
            if (workflow == null) continue;

            List<SagaEvent> events = sagaEventRepository
                    .findBySagaIdOrderByTimestampAsc(saga.getSagaId());

            
            SagaEvent failureEvent = eventsWithRole(
                    workflow, events, WorkflowSpec.EventRole.Role.FAILED).stream()
                    .findFirst().orElse(null);
            SagaEvent compensationStartEvent = compensationEvents(
                    workflow, events, WorkflowSpec.EventRole.Role.STARTED).stream()
                    .findFirst().orElse(null);

            if (failureEvent == null || compensationStartEvent == null) continue;

            // The budget is the compensation step's own declared timeout, looked up from
            // the step the start event belongs to, rather than a fixed REFUND_PAYMENT.
            int compensationTimeoutSeconds = workflow
                    .classifyEvent(compensationStartEvent.getEventType())
                    .map(role -> workflow.getStepTimeoutSeconds(role.stepId()))
                    .orElse(detectionProperties.getDefaultStepTimeoutSeconds());
            int compensationThresholdMs = (int) (compensationTimeoutSeconds
                    * detectionProperties.getDelayedCompensationMultiplier() * 1000);

            long gapMs = Duration.between(failureEvent.getTimestamp(),
                    compensationStartEvent.getTimestamp()).toMillis();
            if (gapMs > compensationThresholdMs) {
                createAnomaly(saga.getSagaId(), AnomalyType.DELAYED_COMPENSATION,
                        String.format("Compensation delayed: %dms gap between %s and %s (threshold: %dms)",
                                gapMs, failureEvent.getEventType(),
                                compensationStartEvent.getEventType(), compensationThresholdMs),
                        details()
                                .observedValue(String.format("%dms gap from %s to %s",
                                        gapMs, failureEvent.getEventType(),
                                        compensationStartEvent.getEventType()))
                                .expectedValue(String.format("<= %dms", compensationThresholdMs))
                                .thresholdSource("config:anomaly.detection.delayed-compensation-multiplier")
                                .evidenceEvents(failureEvent, compensationStartEvent)
                                .measuredAt(compensationStartEvent.getTimestamp()));
            }
        }
    }

    private int statusRank(SagaStatus status) {
        return switch (status) {
            case STARTED -> 1;
            case IN_PROGRESS -> 2;
            case COMPENSATING -> 3;
            case FAILED -> 4;               
            case COMPENSATED -> 5;           
            case COMPLETED -> 6;             
        };
    }


    // ── Spec resolution and role-based event lookup ──────────────────────────
    //
    // The detectors below used to name the order saga's events directly
    // ("INVENTORY_FAILED", "REFUND_PAYMENT", "PAYMENT_REFUNDED"). They now ask the
    // saga's own workflow which of its events play those roles, so the same detector
    // works for a workflow whose compensation is releasing a reservation or reversing
    // a transfer.

    /** The specification governing this saga, or null when none is registered. */
    private WorkflowSpec specFor(SagaInstance saga) {
        return workflowRegistry.byType(saga.getWorkflowType()).orElse(null);
    }

    /** Events playing the given lifecycle role on any forward step. */
    private List<SagaEvent> eventsWithRole(WorkflowSpec workflow, List<SagaEvent> events,
            WorkflowSpec.EventRole.Role role) {
        return events.stream()
                .filter(e -> workflow.classifyEvent(e.getEventType())
                        .filter(r -> r.role() == role && !workflow.isCompensationStep(r.stepId()))
                        .isPresent())
                .toList();
    }

    /** Events playing the given lifecycle role on any compensation step. */
    private List<SagaEvent> compensationEvents(WorkflowSpec workflow, List<SagaEvent> events,
            WorkflowSpec.EventRole.Role role) {
        return events.stream()
                .filter(e -> workflow.classifyEvent(e.getEventType())
                        .filter(r -> r.role() == role && workflow.isCompensationStep(r.stepId()))
                        .isPresent())
                .toList();
    }

    private static String distinctTypes(List<SagaEvent> events) {
        return events.stream()
                .map(SagaEvent::getEventType)
                .distinct()
                .collect(Collectors.joining(", "));
    }

    /**
     * The longest step timeout any registered workflow declares.
     *
     * <p>Used only where one cutoff has to bound a query across every workflow at once;
     * taking the maximum keeps the query from excluding sagas of the slowest workflow.
     */
    private int maxStepTimeoutAcrossWorkflows() {
        int fallback = detectionProperties.getDefaultStepTimeoutSeconds();
        return workflowRegistry.workflowNames().stream()
                .map(name -> workflowRegistry.byType(name).orElse(null))
                .filter(Objects::nonNull)
                .filter(spec -> spec.getSteps() != null)
                .flatMap(spec -> spec.getSteps().stream()
                        .map(step -> spec.getStepTimeoutSeconds(step.getId())))
                .mapToInt(Integer::intValue)
                .max()
                .orElse(fallback);
    }

    /**
     * Row cap and ordering for one scheduler pass.
     *
     * <p>A pass must cost what the recent work costs, not what the accumulated history
     * costs. Newest-first ordering means that when a backlog exceeds the cap, the
     * sagas most likely to still be actionable are the ones examined.
     */
    private Pageable detectionBatch() {
        return PageRequest.of(0, detectionProperties.getDetectionBatchSize(),
                Sort.by(Sort.Direction.DESC, "lastUpdated"));
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
