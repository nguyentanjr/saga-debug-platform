package com.example.correlation.service;

import com.example.correlation.domain.*;
import com.example.correlation.execution.ExecutionProjection;
import com.example.correlation.execution.ExecutionProjectionService;
import com.example.correlation.domain.rca.CandidateStatus;
import com.example.correlation.domain.rca.CandidateAssessment;
import com.example.correlation.domain.rca.Evidence;
import com.example.correlation.domain.rca.RcaCandidate;
import com.example.correlation.domain.rca.RcaStatus;
import com.example.correlation.repository.AnomalyRepository;
import com.example.correlation.repository.RcaResultRepository;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.example.correlation.service.alert.AlertEvent;
import com.example.correlation.service.alert.AlertSink;
import com.example.correlation.service.rca.AnomalySignatureRule;
import com.example.correlation.service.rca.RcaSelector;
import com.example.correlation.service.rca.RuleRegistry;
import com.example.correlation.workflow.WorkflowRegistry;
import com.example.correlation.workflow.WorkflowSpec;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.OptionalInt;
import java.util.stream.Collectors;

@Service
public class RcaService {

    private static final Logger log = LoggerFactory.getLogger(RcaService.class);

    private final SagaInstanceRepository sagaInstanceRepository;
    private final SagaEventRepository sagaEventRepository;
    private final AnomalyRepository anomalyRepository;
    private final RcaResultRepository rcaResultRepository;
    private final WorkflowRegistry workflowRegistry;
    private final ObjectMapper objectMapper;
    private final RcaSelector rcaSelector;
    private final RuleRegistry ruleRegistry;
    private final DetectionProperties detectionProperties;
    private final AlertSink alertSink;
    private final ExecutionProjectionService executionProjectionService;
    private final RcaService self;

    public RcaService(SagaInstanceRepository sagaInstanceRepository,
            SagaEventRepository sagaEventRepository,
            AnomalyRepository anomalyRepository,
            RcaResultRepository rcaResultRepository,
            WorkflowRegistry workflowRegistry,
            ObjectMapper objectMapper,
            RcaSelector rcaSelector,
            RuleRegistry ruleRegistry,
            DetectionProperties detectionProperties,
            AlertSink alertSink,
            ExecutionProjectionService executionProjectionService,
            @Lazy RcaService self) {
        this.sagaInstanceRepository = sagaInstanceRepository;
        this.sagaEventRepository = sagaEventRepository;
        this.anomalyRepository = anomalyRepository;
        this.rcaResultRepository = rcaResultRepository;
        this.workflowRegistry = workflowRegistry;
        this.objectMapper = objectMapper;
        this.rcaSelector = rcaSelector;
        this.ruleRegistry = ruleRegistry;
        this.detectionProperties = detectionProperties;
        this.alertSink = alertSink;
        this.executionProjectionService = executionProjectionService;
        this.self = self;
    }

    @Transactional
    public RcaResult analyze(String sagaId) {
        // Locks the saga for this transaction so two concurrent analyses cannot both
        // delete the previous result and both insert a new one.
        SagaInstance saga = sagaInstanceRepository.findByIdForUpdate(sagaId).orElse(null);
        if (saga == null) {
            log.warn("Saga {} not found for RCA", sagaId);
            return null;
        }

        
        rcaResultRepository.deleteAllBySagaId(sagaId);

        List<SagaEvent> events = sagaEventRepository.findBySagaIdOrderByTimestampAsc(sagaId);
        List<Anomaly> anomalies = anomalyRepository.findBySagaId(sagaId);

        
        WorkflowSpec workflow = workflowRegistry.byType(saga.getWorkflowType()).orElse(null);

        List<CandidateAssessment> candidates = new ArrayList<>();
        // Declared rules first: R03, R04, R07, R09, R12, R13, R15, R19, R22 live in
        // rules/*.yaml, because a set of anomaly types is their entire condition.
        candidates.addAll(evaluateDeclaredRules(anomalies));

        // Procedural rules: each needs something a signature match cannot express —
        // payload content, saga status, timestamp arithmetic, or a missing event.
        addIfPresent(candidates, ruleCircuitBreakerOpen(workflow, events));
        addIfPresent(candidates, ruleTimeoutRetryStorm(workflow, anomalies, events));
        addIfPresent(candidates, ruleServiceCrash(saga, anomalies, events));
        addIfPresent(candidates, ruleTransientServiceFailure(saga, anomalies));
        addIfPresent(candidates, ruleCompensationStepFailure(workflow, saga, events, anomalies));
        addIfPresent(candidates, ruleForwardStepFailure(workflow, saga, events));
        addIfPresent(candidates, ruleOutOfOrderDelivery(saga, anomalies));

        
        
        addIfPresent(candidates, ruleStuckUnknownCause(saga, anomalies));
        addIfPresent(candidates, ruleObservabilityGap(anomalies));

        // Locate each candidate in the execution before ranking, so ordering reflects
        // where the execution diverged rather than how the rules happen to be named.
        ExecutionProjection projection =
                executionProjectionService.project(sagaId, events, workflow);
        anchorCandidates(candidates, events, projection, workflow);

        RcaSelector.SelectionResult selection =
                rcaSelector.select(candidates, anomalies, saga, executionOrder(projection, workflow));
        RcaStatus rcaStatus = selection.getRcaStatus();
        List<RcaCandidate> evidenceCandidates = evidenceCandidates(selection);
        // Order already established by the selector, which knows where each candidate
        // sits in the execution. Re-sorting here on severity and rule id would throw
        // that away and put the alphabetically-first rule back on top.

        
        String outcome = determineOutcome(saga, anomalies, rcaStatus);

        
        List<String> evidence = new ArrayList<>();
        List<String> suggestions = new ArrayList<>();

        appendEvidenceGroup(evidence, suggestions, "Strong evidence",
                candidatesWithStatus(selection, CandidateStatus.STRONG_EVIDENCE));
        appendEvidenceGroup(evidence, suggestions, "Supporting evidence",
                candidatesWithStatus(selection, CandidateStatus.SUPPORTING_EVIDENCE));
        appendEvidenceGroup(evidence, suggestions, "Weak evidence",
                candidatesWithStatus(selection, CandidateStatus.WEAK_EVIDENCE));
        
        suggestions = suggestions.stream().distinct().collect(Collectors.toCollection(ArrayList::new));

        
        if (evidenceCandidates.isEmpty()) {
            if ("NORMAL".equals(outcome)) {
                evidence.add("Saga completed successfully with " + events.size() + " events");
                if (saga.getStartTime() != null && saga.getEndTime() != null) {
                    long ms = Duration.between(saga.getStartTime(), saga.getEndTime()).toMillis();
                    evidence.add("Total duration: " + ms + "ms");
                }
            } else {
                evidence.addAll(selection.getMissingEvidence());
                suggestions.add("Review saga events manually");
            }
        }

        

        
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

        
        try {
            RcaResult result = new RcaResult();
            result.setSagaId(sagaId);
            result.setRootCause(null);
            result.setOutcome(outcome);
            result.setSeverity(!evidenceCandidates.isEmpty()
                    ? highestSeverity(evidenceCandidates)
                    : fallbackSeverity(outcome, selection));
            result.setConfidence(null);
            result.setRuleId(null);
            result.setEvidence(objectMapper.writeValueAsString(evidence));
            result.setSuggestions(objectMapper.writeValueAsString(suggestions));
            result.setRcaStatus(rcaStatus);
            result.setPrimaryRootCauses(List.of());
            result.setCandidates(selection.getCandidates());
            result.setMissingEvidence(selection.getMissingEvidence());

            
            if (!evidenceCandidates.isEmpty()) {
                List<String> evidenceLabels = evidenceCandidates.stream()
                        .map(cr -> cr.getRootCause() + " (" + cr.getRuleId() + ", " + cr.getStatus() + ")")
                        .collect(Collectors.toList());
                result.setContributingCauses(objectMapper.writeValueAsString(evidenceLabels));
            }

            rcaResultRepository.save(result);
            emitAlert(result, evidence);

            log.info("RCA saga={}: rcaStatus={}, outcome={}, severity={}, evidenceCandidates={}",
                    sagaId, rcaStatus, outcome, result.getSeverity(),
                    evidenceCandidates.stream().map(RcaCandidate::getRootCause).collect(Collectors.toList()));
            return result;
        } catch (Exception e) {
            log.error("Failed to save RCA result for saga {}: {}", sagaId, e.getMessage());
            return null;
        }
    }

    /**
     * Ties each candidate to the step its earliest piece of evidence belongs to.
     *
     * <p>Evidence already carries the event ids the rule relied on; the projection says
     * which operation each of those events belongs to. Taking the earliest gives the
     * point in the execution where this candidate's story starts, which is what makes
     * "which of these came first" answerable at all.
     *
     * <p>Anomaly-derived evidence often carries no event ids, in which case the
     * anomaly's own measurement time still positions it in time even though no step can
     * be named. Candidates that cannot be placed keep a null anchor and sort last rather
     * than being given a fabricated position.
     */
    private void anchorCandidates(List<CandidateAssessment> candidates,
            List<SagaEvent> events,
            ExecutionProjection projection,
            WorkflowSpec workflow) {

        Map<String, SagaEvent> eventsById = events.stream()
                .filter(e -> e.getEventId() != null)
                .collect(Collectors.toMap(SagaEvent::getEventId, e -> e, (a, b) -> a));

        for (CandidateAssessment candidate : candidates) {
            SagaEvent earliest = candidate.getEvidence().stream()
                    .flatMap(evidence -> evidence.eventIds().stream())
                    .map(eventsById::get)
                    .filter(Objects::nonNull)
                    .filter(e -> e.getTimestamp() != null)
                    .min(Comparator.comparing(SagaEvent::getTimestamp))
                    .orElse(null);

            if (earliest == null) continue;

            candidate.setAnchorAt(earliest.getTimestamp());
            if (workflow != null) {
                workflow.classifyEvent(earliest.getEventType())
                        .map(WorkflowSpec.EventRole::stepId)
                        .filter(stepId -> projection.operation(stepId).isPresent())
                        .ifPresent(candidate::setAnchorStepId);
            }
        }
    }

    /**
     * Execution position and dependency direction.
     *
     * <p>Dependency is answered from what was observed where the telemetry supports it,
     * and from the specification otherwise. The distinction matters: the specification
     * says which steps <em>can</em> depend on which, while the causal links say which
     * ones <em>did</em> in this run. A step that the spec places downstream but that no
     * observed chain connects to is not evidence of anything here.
     */
    private RcaSelector.ExecutionOrder executionOrder(ExecutionProjection projection,
            WorkflowSpec workflow) {
        List<String> stepOrder = projection.operations().stream()
                .map(ExecutionProjection.Operation::stepId)
                .toList();

        Map<String, String> observedCause = new LinkedHashMap<>();
        for (ExecutionProjection.Operation op : projection.operations()) {
            if (op.startProvenance() != ExecutionProjection.StartProvenance.OBSERVED_CAUSAL_LINK) {
                continue;
            }
            projection.operations().stream()
                    .filter(other -> op.startEventId() != null
                            && op.startEventId().equals(other.endEventId()))
                    .findFirst()
                    .ifPresent(cause -> observedCause.put(op.stepId(), cause.stepId()));
        }

        return new RcaSelector.ExecutionOrder() {
            @Override
            public OptionalInt positionOf(String stepId) {
                int index = stepOrder.indexOf(stepId);
                return index < 0 ? OptionalInt.empty() : OptionalInt.of(index);
            }

            @Override
            public boolean isUpstream(String candidate, String step) {
                if (walkObservedCauses(step).contains(candidate)) {
                    return true;
                }
                return workflow != null && workflow.isUpstreamOf(candidate, step);
            }

            private Set<String> walkObservedCauses(String step) {
                Set<String> seen = new LinkedHashSet<>();
                String cursor = observedCause.get(step);
                while (cursor != null && seen.add(cursor)) {
                    cursor = observedCause.get(cursor);
                }
                return seen;
            }
        };
    }

    public List<RcaResult> getResults(String sagaId) {
        return rcaResultRepository.findBySagaIdOrderByCreatedAtDesc(sagaId);
    }

    /**
     * Not transactional, and deliberately so: analysis of each saga runs in its own
     * transaction via {@code self}. Wrapping the whole sweep in one transaction meant a
     * single failing saga rolled back every result the pass had already produced, and
     * held one long-lived transaction open across the entire batch.
     */
    @Scheduled(fixedRateString = "${rca.auto-analysis.fixed-rate-ms:30000}")
    public void autoAnalyzeNewlyDetectedAnomalies() {
        Instant cutoff = Instant.now().minus(2, ChronoUnit.MINUTES);
        List<String> sagaIds = anomalyRepository.findSagaIdsWithAnomaliesSince(cutoff);

        for (String sagaId : sagaIds) {
            Instant latestAnomalyAt = anomalyRepository.findLatestDetectedAtBySagaId(sagaId);
            if (latestAnomalyAt == null) {
                continue;
            }

            List<RcaResult> existing = rcaResultRepository.findBySagaIdOrderByCreatedAtDesc(sagaId);
            boolean needsAnalysis = existing.isEmpty()
                    || existing.get(0).getCreatedAt() == null
                    || existing.get(0).getCreatedAt().isBefore(latestAnomalyAt);

            if (needsAnalysis) {
                try {
                    // Through the proxy, so @Transactional applies per saga.
                    RcaResult result = self.analyze(sagaId);
                    if (result != null) {
                        log.info("Auto-RCA triggered for saga {} with rcaStatus={} rootCause={}",
                                sagaId, result.getRcaStatus(), result.getRootCause());
                    }
                } catch (Exception e) {
                    log.error("Auto-RCA failed for saga {}: {}", sagaId, e.getMessage());
                }
            }
        }
    }

    private void emitAlert(RcaResult result, List<String> evidenceSummary) {
        if (result.getRcaStatus() != RcaStatus.EVIDENCE_AVAILABLE) {
            return;
        }

        List<String> rootCauses = evidenceRootCauses(result.getCandidates());
        if (rootCauses.isEmpty() && result.getRootCause() != null && !result.getRootCause().isBlank()) {
            rootCauses = List.of(result.getRootCause());
        }
        if (rootCauses.isEmpty()) {
            return;
        }

        AlertEvent alert = new AlertEvent(
                Instant.now(),
                result.getSagaId(),
                result.getRcaStatus().name(),
                rootCauses,
                result.getSeverity(),
                evidenceSummary != null ? List.copyOf(evidenceSummary) : List.of());

        // Deferred to commit. Emitting inline announced a diagnosis that the enclosing
        // transaction could still roll back, leaving an alert on disk for a result that
        // was never stored.
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    writeAlert(alert);
                }
            });
        } else {
            writeAlert(alert);
        }
    }

    private void writeAlert(AlertEvent alert) {
        try {
            alertSink.emit(alert);
        } catch (Exception e) {
            log.error("Failed to emit RCA alert for saga {}: {}", alert.sagaId(), e.getMessage());
        }
    }

    /** Events playing the given lifecycle role on any compensation step of this workflow. */
    private List<SagaEvent> compensationEvents(WorkflowSpec workflow, List<SagaEvent> events,
            WorkflowSpec.EventRole.Role role) {
        if (workflow == null) return List.of();
        return events.stream()
                .filter(e -> workflow.classifyEvent(e.getEventType())
                        .filter(r -> r.role() == role && workflow.isCompensationStep(r.stepId()))
                        .isPresent())
                .collect(Collectors.toList());
    }

    private static String eventTypes(List<SagaEvent> events) {
        return events.stream().map(SagaEvent::getEventType).distinct().collect(Collectors.joining(", "));
    }

    /**
     * Runs every declared anomaly-signature rule against this saga's anomalies.
     *
     * <p>Order of the returned candidates carries no meaning: {@code RcaSelector} ranks
     * them by evidence tier and by where they sit in the execution, so which file a rule
     * was declared in cannot influence which diagnosis wins. That was not true of the
     * previous implementation, where the order of {@code addIfPresent} calls decided
     * ties.
     */
    private List<CandidateAssessment> evaluateDeclaredRules(List<Anomaly> anomalies) {
        List<CandidateAssessment> matched = new ArrayList<>();

        for (AnomalySignatureRule rule : ruleRegistry.rules()) {
            rule.firstMatch(anomalies).ifPresent(anomaly -> {
                CandidateAssessment candidate = candidate(
                        rule.getRootCause(), rule.getSeverity(), rule.getId(), rule.getEvidence());

                // The matched anomaly first: it carries the event ids that let
                // anchorCandidates() place this candidate in the execution.
                candidate.getEvidence().add(evidence(anomaly));
                rule.getEvidenceNotes().forEach(note -> candidate.getEvidence().add(evidence(note)));
                candidate.getSuggestions().addAll(rule.getSuggestions());

                matched.add(candidate);
            });
        }
        return matched;
    }

    private void addIfPresent(List<CandidateAssessment> list, CandidateAssessment candidate) {
        if (candidate != null) list.add(candidate);
    }

    private String determineOutcome(SagaInstance saga, List<Anomaly> anomalies, RcaStatus rcaStatus) {
        boolean hasAnomalies = !anomalies.isEmpty();
        boolean hasRca = rcaStatus == RcaStatus.EVIDENCE_AVAILABLE;
        boolean isTerminal = saga.getStatus() == SagaStatus.COMPLETED
                || saga.getStatus() == SagaStatus.COMPENSATED
                || saga.getStatus() == SagaStatus.FAILED;

        if (saga.getStatus() == SagaStatus.COMPLETED && !hasAnomalies) return "NORMAL";
        if (saga.getStatus() == SagaStatus.COMPLETED && hasAnomalies) return "SELF_HEALED";
        if (saga.getStatus() == SagaStatus.COMPENSATED) return "COMPENSATED";
        
        
        if (saga.getStatus() == SagaStatus.FAILED && !hasAnomalies && !hasRca) return "NORMAL";
        if (saga.getStatus() == SagaStatus.FAILED) return "FAILED";
        if (saga.getStatus() == SagaStatus.COMPENSATING) return "INCONSISTENT";
        
        
        if (hasAnomalies) return "STILL_STUCK";
        return "IN_PROGRESS";
    }


    
    
    
    private CandidateAssessment ruleCircuitBreakerOpen(WorkflowSpec workflow, List<SagaEvent> events) {
        if (workflow == null) return null;
        for (SagaEvent event : events) {
            boolean isFailure = workflow.classifyEvent(event.getEventType())
                    .filter(role -> role.role() == WorkflowSpec.EventRole.Role.FAILED)
                    .isPresent();
            if (!isFailure) continue;

            String payload = event.getPayload() != null ? event.getPayload().toLowerCase(Locale.ROOT) : "";
            if (payload.contains("circuit breaker") || payload.contains("retry exhausted")) {
                CandidateAssessment rr = candidate("CIRCUIT_BREAKER_OPEN", "CRITICAL", "R01",
                        CandidateStatus.STRONG_EVIDENCE);
                rr.getEvidence().add(Evidence.ofEvent(
                        event.getEventType() + " reports circuit breaker / retry exhaustion",
                        event.getPayload(), "the step succeeds or reports a business failure",
                        event.getEventId()));
                rr.getEvidence().add(Evidence.of("Payload: " + event.getPayload(), null));
                // The signature is a substring of a message the failing service writes,
                // not a reading of breaker state. It establishes that retries were
                // exhausted; whether a breaker ever opened is not shown by this evidence.
                rr.setStatusReason("Signature matched in the failure payload; breaker state itself was not observed.");
                rr.getSuggestions().add("Check downstream service health");
                rr.getSuggestions().add("Review Resilience4j metrics at /actuator/circuitbreakers");
                return rr;
            }
        }
        return null;
    }

    
    
    
    
    
    
    
    private CandidateAssessment ruleTimeoutRetryStorm(WorkflowSpec workflow, List<Anomaly> anomalies,
            List<SagaEvent> events) {
        Set<String> retryEventTypes = workflow != null ? workflow.retryEventTypes() : Set.of();
        for (Anomaly a : anomalies) {
            if (a.getAnomalyType() == AnomalyType.RETRY_BURST) {
                List<SagaEvent> timeoutRetryEvents = events.stream()
                        .filter(e -> retryEventTypes.contains(e.getEventType()))
                        .filter(e -> payloadIndicatesTimeout(e.getPayload()))
                        .collect(Collectors.toList());
                boolean hasTimeoutEvidence = !timeoutRetryEvents.isEmpty();
                CandidateAssessment rr = candidate("TIMEOUT_RETRY_STORM", "CRITICAL", "R02",
                        hasTimeoutEvidence ? CandidateStatus.STRONG_EVIDENCE : CandidateStatus.WEAK_EVIDENCE);
                rr.getEvidence().add(evidence(a));
                if (hasTimeoutEvidence) {
                    rr.getEvidence().add(Evidence.of(
                            "Retry payloads report timeout exceptions",
                            timeoutRetryEvents.stream()
                                    .map(SagaEvent::getPayload)
                                    .filter(Objects::nonNull)
                                    .limit(3)
                                    .collect(Collectors.joining("; ")),
                            "retry burst should not be caused by timeout exceptions",
                            null,
                            timeoutRetryEvents.stream()
                                    .map(SagaEvent::getEventId)
                                    .filter(Objects::nonNull)
                                    .collect(Collectors.toList())));
                } else {
                    rr.setStatusReason("Retry events do not record timeout exception class.");
                }
                rr.getSuggestions().add("Reduce retry count or increase backoff");
                rr.getSuggestions().add("Check service logs for timeout root cause");
                return rr;
            }
        }
        return null;
    }

    private boolean payloadIndicatesTimeout(String payload) {
        if (payload == null) return false;
        String normalized = payload.toLowerCase(Locale.ROOT);
        return normalized.contains("timeout")
                || normalized.contains("timed out")
                || normalized.contains("timeoutexception");
    }

    
    

    
    
    

    
    
    
    
    
    
    
    
    private CandidateAssessment ruleServiceCrash(SagaInstance saga, List<Anomaly> anomalies,
            List<SagaEvent> events) {
        boolean hasStuck = anomalies.stream()
                .anyMatch(a -> a.getAnomalyType() == AnomalyType.SAGA_STUCK);
        boolean hasMissingTerminal = anomalies.stream()
                .anyMatch(a -> a.getAnomalyType() == AnomalyType.MISSING_TERMINAL_EVENT);
        boolean hasStepTimeout = anomalies.stream()
                .anyMatch(a -> a.getAnomalyType() == AnomalyType.STEP_TIMEOUT);

        if (hasStuck && hasMissingTerminal && !hasStepTimeout) {
            boolean isRecovered = saga.getStatus() == SagaStatus.COMPLETED
                    || saga.getStatus() == SagaStatus.COMPENSATED
                    || saga.getStatus() == SagaStatus.FAILED;

            // Resolved from the saga's own workflow: R05's threshold is the slowest step
            // of the workflow this execution belongs to, not of whichever workflow
            // happened to be loaded first.
            var workflow = workflowRegistry.byType(saga.getWorkflowType()).orElse(null);
            
            int maxTimeout = detectionProperties.getDefaultStepTimeoutSeconds();
            if (workflow != null && workflow.getSteps() != null) {
                maxTimeout = workflow.getSteps().stream()
                        .mapToInt(s -> workflow.getStepTimeoutSeconds(s.getId()))
                        .max().orElse(detectionProperties.getDefaultStepTimeoutSeconds());
            }
            int serviceCrashTimeoutSeconds = maxTimeout * detectionProperties.getServiceCrashTimeoutMultiplier();

            if (saga.getLastUpdated() != null) {
                
                
                
                Instant stuckMeasurePoint = anomalies.stream()
                        .filter(a -> a.getAnomalyType() == AnomalyType.SAGA_STUCK
                                || a.getAnomalyType() == AnomalyType.MISSING_TERMINAL_EVENT)
                        .map(Anomaly::getDetectedAt)
                        .filter(Objects::nonNull)
                        .max(Instant::compareTo)
                        .orElse(Instant.now());
                Instant stuckStart = events.stream()
                        .map(SagaEvent::getTimestamp)
                        .filter(Objects::nonNull)
                        .filter(timestamp -> !timestamp.isAfter(stuckMeasurePoint))
                        .max(Instant::compareTo)
                        .orElse(saga.getLastUpdated());
                long stuckSeconds = Duration.between(stuckStart, stuckMeasurePoint).getSeconds();

                if (stuckSeconds > serviceCrashTimeoutSeconds) {
                    
                    
                    CandidateAssessment rr = candidate("SERVICE_CRASH",
                            isRecovered ? "MEDIUM" : "CRITICAL", "R05",
                            CandidateStatus.SUPPORTING_EVIDENCE);
                    rr.getEvidence().add(evidence("Saga stuck for " + stuckSeconds + "s (>"
                            + serviceCrashTimeoutSeconds + "s threshold)"));
                    rr.getEvidence().add(evidence("Both SAGA_STUCK and MISSING_TERMINAL_EVENT anomalies present"));
                    if (isRecovered) {
                        rr.getEvidence().add(evidence("Service recovered → saga now " + saga.getStatus()));
                    }
                    rr.getSuggestions().add("Check if downstream service container is running");
                    rr.getSuggestions().add("Verify docker container health and restart policy");
                    return rr;
                }
            }
        }
        return null;
    }

    
    
    
    
    
    
    
    
    
    
    
    
    
    private CandidateAssessment ruleTransientServiceFailure(SagaInstance saga, List<Anomaly> anomalies) {
        boolean hasStuck = anomalies.stream()
                .anyMatch(a -> a.getAnomalyType() == AnomalyType.SAGA_STUCK);
        boolean hasMissingTerminal = anomalies.stream()
                .anyMatch(a -> a.getAnomalyType() == AnomalyType.MISSING_TERMINAL_EVENT);
        boolean isTerminal = saga.getStatus() == SagaStatus.COMPLETED
                || saga.getStatus() == SagaStatus.COMPENSATED
                || saga.getStatus() == SagaStatus.FAILED;
        boolean hasStepTimeoutBeforeRecovery = hasStepTimeoutBeforeRecovery(saga, anomalies);

        
        
        if (hasStuck && !hasMissingTerminal && !hasStepTimeoutBeforeRecovery && isTerminal) {
            CandidateAssessment rr = candidate("TRANSIENT_SERVICE_FAILURE", "LOW", "R06",
                    CandidateStatus.STRONG_EVIDENCE);
            rr.getEvidence().add(evidence("Saga was previously stuck (SAGA_STUCK anomaly detected)"));
            rr.getEvidence().add(evidence("Saga has since recovered → final status: " + saga.getStatus()));
            if (saga.getStartTime() != null && saga.getEndTime() != null) {
                long ms = Duration.between(saga.getStartTime(), saga.getEndTime()).toMillis();
                rr.getEvidence().add(evidence("Total duration (including stuck period): " + ms + "ms"));
            }
            rr.getSuggestions().add("Service self-healed — review uptime to prevent recurrence");
            return rr;
        }
        return null;
    }

    private boolean hasStepTimeoutBeforeRecovery(SagaInstance saga, List<Anomaly> anomalies) {
        if (saga.getEndTime() == null) {
            return anomalies.stream().anyMatch(a -> a.getAnomalyType() == AnomalyType.STEP_TIMEOUT);
        }
        return anomalies.stream()
                .filter(a -> a.getAnomalyType() == AnomalyType.STEP_TIMEOUT)
                .map(Anomaly::getMeasuredAt)
                .filter(Objects::nonNull)
                .anyMatch(measuredAt -> measuredAt.isBefore(saga.getEndTime()));
    }

    
    
    
    
    

    
    
    

    
    
    
    
    private CandidateAssessment ruleCompensationStepFailure(WorkflowSpec workflow, SagaInstance saga,
            List<SagaEvent> events, List<Anomaly> anomalies) {
        List<SagaEvent> started = compensationEvents(workflow, events, WorkflowSpec.EventRole.Role.STARTED);
        boolean completed = !compensationEvents(workflow, events, WorkflowSpec.EventRole.Role.COMPLETED).isEmpty();
        boolean hasMissingCompAck = anomalies.stream()
                .anyMatch(a -> a.getAnomalyType() == AnomalyType.MISSING_COMPENSATION_ACK);

        if (!started.isEmpty() && !completed
                && (saga.getStatus() == SagaStatus.COMPENSATING || hasMissingCompAck)) {
            CandidateAssessment rr = candidate("COMPENSATION_STEP_FAILURE", "HIGH", "R10",
                    CandidateStatus.STRONG_EVIDENCE);
            String startedTypes = eventTypes(started);
            rr.getEvidence().add(evidence(
                    "Compensation started (" + startedTypes + ") but never acknowledged"));
            rr.getEvidence().add(evidence("Saga status: " + saga.getStatus()));
            // Fires as soon as the saga enters COMPENSATING, which is the same event that
            // starts the compensation, so a compensation still within its budget looks
            // identical to one that failed. MISSING_COMPENSATION_ACK is the signal that
            // waited for a deadline; without it this is a hypothesis, not a failure.
            if (!hasMissingCompAck) {
                rr.setStatus(CandidateStatus.WEAK_EVIDENCE);
                rr.setStatusReason("No acknowledgement deadline has elapsed yet; the compensation may still be in flight.");
            }
            rr.getSuggestions().add("Check the compensating service's logs");
            rr.getSuggestions().add("Verify broker connectivity for the compensation topic");
            return rr;
        }
        return null;
    }

    
    
    
    
    private CandidateAssessment ruleForwardStepFailure(WorkflowSpec workflow, SagaInstance saga,
            List<SagaEvent> events) {
        List<SagaEvent> started = compensationEvents(workflow, events, WorkflowSpec.EventRole.Role.STARTED);
        List<SagaEvent> completed = compensationEvents(workflow, events, WorkflowSpec.EventRole.Role.COMPLETED);

        if (saga.getStatus() == SagaStatus.COMPENSATED && !started.isEmpty() && !completed.isEmpty()) {
            CandidateAssessment rr = candidate("FORWARD_STEP_FAILURE", "LOW", "R11",
                    CandidateStatus.STRONG_EVIDENCE);
            rr.getEvidence().add(evidence("Compensation completed: "
                    + eventTypes(started) + " -> " + eventTypes(completed)));
            rr.getEvidence().add(evidence("Saga status: COMPENSATED"));
            if (saga.getStartTime() != null && saga.getEndTime() != null) {
                long ms = Duration.between(saga.getStartTime(), saga.getEndTime()).toMillis();
                rr.getEvidence().add(evidence("Total duration (including compensation): " + ms + "ms"));
            }
            rr.getSuggestions().add("Saga rolled back correctly");
            rr.getSuggestions().add("Review why the forward step failed to prevent recurrence");
            return rr;
        }
        return null;
    }

    
    

    
    

    
    
    
    
    private CandidateAssessment ruleOutOfOrderDelivery(SagaInstance saga, List<Anomaly> anomalies) {
        boolean isTerminal = saga.getStatus() == SagaStatus.COMPLETED
                || saga.getStatus() == SagaStatus.COMPENSATED;
        for (Anomaly a : anomalies) {
            if (a.getAnomalyType() == AnomalyType.OUT_OF_ORDER) {
                CandidateAssessment rr = candidate("OUT_OF_ORDER_DELIVERY",
                        isTerminal ? "LOW" : "MEDIUM", "R14",
                        CandidateStatus.STRONG_EVIDENCE);
                rr.getEvidence().add(evidence(a));
                if (isTerminal) {
                    rr.getEvidence().add(evidence("Saga completed despite out-of-order events"));
                } else {
                    rr.getEvidence().add(evidence("Saga still in " + saga.getStatus() + " status"));
                }
                rr.getSuggestions().add("Check Kafka partition key assignment");
                return rr;
            }
        }
        return null;
    }

    
    
    
    
    

    
    

    
    
    
    
    
    private CandidateAssessment ruleObservabilityGap(List<Anomaly> anomalies) {
        if (!anomalies.isEmpty()) {
            CandidateAssessment rr = candidate("OBSERVABILITY_GAP", "LOW", "R20",
                    CandidateStatus.WEAK_EVIDENCE);
            rr.getEvidence().add(evidence("Anomalies detected but insufficient evidence to determine root cause"));
            rr.getEvidence().add(evidence("Anomaly types: " + anomalies.stream()
                    .map(a -> a.getAnomalyType().name())
                    .distinct()
                    .collect(Collectors.joining(", "))));
            rr.getSuggestions().add("Add more telemetry sources for better RCA");
            rr.getSuggestions().add("Check if Jaeger/logs provide additional context");
            return rr;
        }
        return null;
    }

    
    
    
    
    
    private CandidateAssessment ruleStuckUnknownCause(SagaInstance saga, List<Anomaly> anomalies) {
        boolean hasStuck = anomalies.stream()
                .anyMatch(a -> a.getAnomalyType() == AnomalyType.SAGA_STUCK);
        boolean isNonTerminal = saga.getStatus() != SagaStatus.COMPLETED
                && saga.getStatus() != SagaStatus.COMPENSATED
                && saga.getStatus() != SagaStatus.FAILED;

        if (hasStuck && isNonTerminal) {
            CandidateAssessment rr = candidate("STUCK_UNKNOWN_CAUSE", "HIGH", "R21",
                    CandidateStatus.WEAK_EVIDENCE);
            rr.getEvidence().add(evidence("Saga stuck in " + saga.getStatus() + " status"));
            rr.getEvidence().add(evidence("No specific root cause identified — insufficient evidence"));
            rr.getSuggestions().add("Check all downstream service health");
            rr.getSuggestions().add("Verify Kafka consumer group offsets");
            return rr;
        }
        return null;
    }

    
    
    
    
    

    
    
    
    private CandidateAssessment candidate(String rootCause, String severity, String ruleId,
            CandidateStatus tier) {
        CandidateAssessment candidate = CandidateAssessment.builder()
                .rootCause(rootCause)
                .severity(severity)
                .ruleId(ruleId)
                .status(tier)
                .build();
        if (tier == CandidateStatus.WEAK_EVIDENCE) {
            candidate.setStatusReason(
                    "Weak evidence only; treat " + rootCause + " as a fallback hypothesis.");
        }
        return candidate;
    }

    private Evidence evidence(String fact) {
        return Evidence.of(fact, null);
    }

    private Evidence evidence(Anomaly anomaly) {
        if (anomaly == null) return null;
        return Evidence.of(
                anomaly.getDescription(),
                anomaly.getObservedValue(),
                anomaly.getExpectedValue(),
                anomaly.getId(),
                anomaly.getEvidenceEventIds());
    }

    private List<RcaCandidate> candidatesWithStatus(RcaSelector.SelectionResult selection, CandidateStatus status) {
        return selection.getCandidates().stream()
                .filter(candidate -> candidate.getStatus() == status)
                .collect(Collectors.toList());
    }

    private List<RcaCandidate> evidenceCandidates(RcaSelector.SelectionResult selection) {
        return selection.getCandidates().stream()
                .filter(candidate -> isEvidenceStatus(candidate.getStatus()))
                .collect(Collectors.toCollection(ArrayList::new));
    }

    private boolean isEvidenceStatus(CandidateStatus status) {
        return status == CandidateStatus.STRONG_EVIDENCE
                || status == CandidateStatus.SUPPORTING_EVIDENCE
                || status == CandidateStatus.WEAK_EVIDENCE;
    }

    private void appendEvidenceGroup(List<String> evidence,
            List<String> suggestions,
            String groupLabel,
            List<RcaCandidate> groupCandidates) {
        if (groupCandidates == null || groupCandidates.isEmpty()) {
            return;
        }
        for (RcaCandidate candidate : groupCandidates) {
            List<String> facts = evidenceFacts(candidate);
            String factSummary = facts.isEmpty() ? "No structured evidence facts recorded" : String.join("; ", facts);
            String position = candidate.getAnchorStepId() != null
                    ? " @" + candidate.getAnchorStepId()
                    : "";
            String precededBy = candidate.getPrecededBy().isEmpty()
                    ? ""
                    : " (downstream of " + String.join(", ", candidate.getPrecededBy()) + ")";
            evidence.add(String.format("[%s] %s (%s)%s%s: %s",
                    groupLabel, candidate.getRootCause(), candidate.getRuleId(),
                    position, precededBy, factSummary));
            suggestions.addAll(candidate.getSuggestions());
        }
    }

    private int statusRank(CandidateStatus status) {
        if (status == CandidateStatus.STRONG_EVIDENCE) return 0;
        if (status == CandidateStatus.SUPPORTING_EVIDENCE) return 1;
        if (status == CandidateStatus.WEAK_EVIDENCE) return 2;
        if (status == CandidateStatus.RULED_OUT) return 3;
        return 4;
    }

    private List<String> evidenceFacts(RcaCandidate candidate) {
        return candidate.getEvidence().stream()
                .map(Evidence::fact)
                .filter(fact -> fact != null && !fact.isBlank())
                .collect(Collectors.toList());
    }

    private List<String> evidenceRootCauses(List<RcaCandidate> candidates) {
        if (candidates == null) {
            return List.of();
        }
        return candidates.stream()
                .filter(candidate -> isEvidenceStatus(candidate.getStatus()))
                .map(RcaCandidate::getRootCause)
                .filter(root -> root != null && !root.isBlank())
                .distinct()
                .collect(Collectors.toList());
    }

    private String highestSeverity(List<RcaCandidate> candidates) {
        return candidates.stream()
                .map(RcaCandidate::getSeverity)
                .filter(Objects::nonNull)
                .max(Comparator.comparingInt(this::severityRank))
                .orElse("LOW");
    }

    private int severityRank(String severity) {
        if ("CRITICAL".equals(severity)) return 4;
        if ("HIGH".equals(severity)) return 3;
        if ("MEDIUM".equals(severity)) return 2;
        if ("LOW".equals(severity)) return 1;
        return 0;
    }

    private String fallbackSeverity(String outcome, RcaSelector.SelectionResult selection) {
        if ("NORMAL".equals(outcome)) return "NONE";
        return highestSeverity(selection.getCandidates());
    }

    private String extractTraceId(String payloadJson) {
        if (payloadJson == null || payloadJson.isBlank()) return null;
        try {
            var node = objectMapper.readTree(payloadJson);
            if (node.has("traceId") && !node.get("traceId").isNull()) {
                return node.get("traceId").asText();
            }
        } catch (Exception e) {
            
        }
        return null;
    }
}
