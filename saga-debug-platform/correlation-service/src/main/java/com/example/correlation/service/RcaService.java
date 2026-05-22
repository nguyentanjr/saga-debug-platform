package com.example.correlation.service;

import com.example.correlation.domain.*;
import com.example.correlation.domain.rca.CandidateStatus;
import com.example.correlation.domain.rca.CandidateAssessment;
import com.example.correlation.domain.rca.Directness;
import com.example.correlation.domain.rca.Evidence;
import com.example.correlation.domain.rca.RcaCandidate;
import com.example.correlation.domain.rca.RcaStatus;
import com.example.correlation.repository.AnomalyRepository;
import com.example.correlation.repository.RcaResultRepository;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.example.correlation.service.alert.AlertEvent;
import com.example.correlation.service.alert.AlertSink;
import com.example.correlation.service.rca.RcaSelector;
import com.example.correlation.service.rca.SelectionResult;
import com.example.correlation.workflow.WorkflowLoader;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class RcaService {

    private static final Logger log = LoggerFactory.getLogger(RcaService.class);

    private final SagaInstanceRepository sagaInstanceRepository;
    private final SagaEventRepository sagaEventRepository;
    private final AnomalyRepository anomalyRepository;
    private final RcaResultRepository rcaResultRepository;
    private final WorkflowLoader workflowLoader;
    private final ObjectMapper objectMapper;
    private final RcaSelector rcaSelector;
    private final DetectionProperties detectionProperties;
    private final AlertSink alertSink;

    public RcaService(SagaInstanceRepository sagaInstanceRepository,
            SagaEventRepository sagaEventRepository,
            AnomalyRepository anomalyRepository,
            RcaResultRepository rcaResultRepository,
            WorkflowLoader workflowLoader,
            ObjectMapper objectMapper,
            RcaSelector rcaSelector,
            DetectionProperties detectionProperties,
            AlertSink alertSink) {
        this.sagaInstanceRepository = sagaInstanceRepository;
        this.sagaEventRepository = sagaEventRepository;
        this.anomalyRepository = anomalyRepository;
        this.rcaResultRepository = rcaResultRepository;
        this.workflowLoader = workflowLoader;
        this.objectMapper = objectMapper;
        this.rcaSelector = rcaSelector;
        this.detectionProperties = detectionProperties;
        this.alertSink = alertSink;
    }

    @Transactional
    public RcaResult analyze(String sagaId) {
        SagaInstance saga = sagaInstanceRepository.findById(sagaId).orElse(null);
        if (saga == null) {
            log.warn("Saga {} not found for RCA", sagaId);
            return null;
        }

        
        rcaResultRepository.deleteAllBySagaId(sagaId);

        List<SagaEvent> events = sagaEventRepository.findBySagaIdOrderByTimestampAsc(sagaId);
        List<Anomaly> anomalies = anomalyRepository.findBySagaId(sagaId);

        
        List<CandidateAssessment> candidates = new ArrayList<>();
        addIfPresent(candidates, ruleCircuitBreakerOpen(events));
        addIfPresent(candidates, ruleTimeoutRetryStorm(anomalies, events));
        addIfPresent(candidates, ruleMultiTerminalConflict(anomalies));
        addIfPresent(candidates, ruleTerminalStateOverwrite(anomalies));
        addIfPresent(candidates, ruleServiceCrash(saga, anomalies, events));
        addIfPresent(candidates, ruleTransientServiceFailure(saga, anomalies));
        addIfPresent(candidates, ruleDownstreamTimeout(anomalies));
        addIfPresent(candidates, ruleCompensationNotTriggered(anomalies));
        addIfPresent(candidates, ruleCompensationStepFailure(saga, events, anomalies));
        addIfPresent(candidates, ruleForwardStepFailure(saga, events));
        addIfPresent(candidates, ruleInvalidTransition(anomalies));
        addIfPresent(candidates, ruleDuplicateDelivery(anomalies));
        addIfPresent(candidates, ruleOutOfOrderDelivery(saga, anomalies));
        addIfPresent(candidates, ruleWrongSagaCorrelationKey(anomalies));
        addIfPresent(candidates, ruleMissingTracePropagation(anomalies));
        addIfPresent(candidates, ruleDelayedCompensation(anomalies));

        
        
        addIfPresent(candidates, ruleStuckUnknownCause(saga, anomalies));
        addIfPresent(candidates, ruleObservabilityGap(anomalies));

        SelectionResult selection = rcaSelector.select(candidates, anomalies, saga);
        RcaStatus rcaStatus = selection.getRcaStatus();
        List<RcaCandidate> evidenceCandidates = evidenceCandidates(selection);
        evidenceCandidates.sort(this::compareCandidateStrength);

        
        String outcome = determineOutcome(saga, anomalies, rcaStatus);

        
        List<String> evidence = new ArrayList<>();
        List<String> suggestions = new ArrayList<>();

        appendEvidenceGroup(evidence, suggestions, "Strong evidence",
                candidatesWithStatus(selection, CandidateStatus.STRONG_EVIDENCE));
        appendEvidenceGroup(evidence, suggestions, "Supporting evidence",
                candidatesWithStatus(selection, CandidateStatus.SUPPORTING_EVIDENCE));
        appendEvidenceGroup(evidence, suggestions, "Weak evidence",
                candidatesWithStatus(selection, CandidateStatus.WEAK_EVIDENCE));
        
        appendEvidenceGroup(evidence, suggestions, "Strong evidence",
                candidatesWithStatus(selection, CandidateStatus.SELECTED));
        appendEvidenceGroup(evidence, suggestions, "Supporting evidence",
                candidatesWithStatus(selection, CandidateStatus.CONTRIBUTING));
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

    public List<RcaResult> getResults(String sagaId) {
        return rcaResultRepository.findBySagaIdOrderByCreatedAtDesc(sagaId);
    }

    @Scheduled(fixedRateString = "${rca.auto-analysis.fixed-rate-ms:30000}")
    @Transactional
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
                    RcaResult result = analyze(sagaId);
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
        if (result.getRcaStatus() != RcaStatus.EVIDENCE_AVAILABLE
                && result.getRcaStatus() != RcaStatus.SELECTED) {
            return;
        }

        List<String> rootCauses = evidenceRootCauses(result.getCandidates());
        if (rootCauses.isEmpty() && result.getRootCause() != null && !result.getRootCause().isBlank()) {
            rootCauses = List.of(result.getRootCause());
        }
        if (rootCauses.isEmpty()) {
            return;
        }

        try {
            alertSink.emit(new AlertEvent(
                    Instant.now(),
                    result.getSagaId(),
                    result.getRcaStatus().name(),
                    rootCauses,
                    result.getSeverity(),
                    evidenceSummary != null ? List.copyOf(evidenceSummary) : List.of()));
        } catch (Exception e) {
            log.error("Failed to emit RCA alert for saga {}: {}", result.getSagaId(), e.getMessage());
        }
    }

    private void addIfPresent(List<CandidateAssessment> list, CandidateAssessment candidate) {
        if (candidate != null) list.add(candidate);
    }

    private String determineOutcome(SagaInstance saga, List<Anomaly> anomalies, RcaStatus rcaStatus) {
        boolean hasAnomalies = !anomalies.isEmpty();
        boolean hasRca = rcaStatus == RcaStatus.SELECTED || rcaStatus == RcaStatus.EVIDENCE_AVAILABLE;
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


    
    
    
    private CandidateAssessment ruleCircuitBreakerOpen(List<SagaEvent> events) {
        for (SagaEvent event : events) {
            if ("PAYMENT_FAILED".equals(event.getEventType())) {
                String payload = event.getPayload() != null ? event.getPayload().toLowerCase() : "";
                if (payload.contains("circuit breaker") || payload.contains("retry exhausted")) {
                    CandidateAssessment rr = candidate("CIRCUIT_BREAKER_OPEN", "CRITICAL", "R01",
                            Directness.DIRECT_SIGNATURE);
                    rr.getEvidence().add(Evidence.ofEvent("Payment failed with circuit breaker/retry exhaustion",
                            event.getPayload(), "payment succeeds or reports business failure",
                            event.getEventId()));
                    rr.getEvidence().add(Evidence.of("Payload: " + event.getPayload(), null));
                    rr.getSuggestions().add("Check downstream service health");
                    rr.getSuggestions().add("Review Resilience4j metrics at /actuator/circuitbreakers");
                    return rr;
                }
            }
        }
        return null;
    }

    
    
    
    
    
    
    
    private CandidateAssessment ruleTimeoutRetryStorm(List<Anomaly> anomalies, List<SagaEvent> events) {
        for (Anomaly a : anomalies) {
            if (a.getAnomalyType() == AnomalyType.RETRY_BURST) {
                List<SagaEvent> timeoutRetryEvents = events.stream()
                        .filter(e -> "PAYMENT_RETRY".equals(e.getEventType()))
                        .filter(e -> payloadIndicatesTimeout(e.getPayload()))
                        .collect(Collectors.toList());
                boolean hasTimeoutEvidence = !timeoutRetryEvents.isEmpty();
                CandidateAssessment rr = candidate("TIMEOUT_RETRY_STORM", "CRITICAL", "R02",
                        hasTimeoutEvidence ? Directness.DIRECT_SIGNATURE : Directness.HEURISTIC_INFERENCE);
                rr.getEvidence().add(evidence(a));
                if (hasTimeoutEvidence) {
                    rr.getEvidence().add(Evidence.of(
                            "PAYMENT_RETRY payloads report timeout exceptions",
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

    
    
    private CandidateAssessment ruleMultiTerminalConflict(List<Anomaly> anomalies) {
        for (Anomaly a : anomalies) {
            if (a.getAnomalyType() == AnomalyType.MULTI_TERMINAL_CONFLICT) {
                CandidateAssessment rr = candidate("MULTI_TERMINAL_CONFLICT", "CRITICAL", "R03",
                        Directness.DIRECT_SIGNATURE);
                rr.getEvidence().add(evidence(a));
                rr.getSuggestions().add("Enforce final-state immutability");
                rr.getSuggestions().add("Check for duplicate terminal event delivery");
                return rr;
            }
        }
        return null;
    }

    
    
    
    private CandidateAssessment ruleTerminalStateOverwrite(List<Anomaly> anomalies) {
        for (Anomaly a : anomalies) {
            if (a.getAnomalyType() == AnomalyType.FORBIDDEN_POST_TERMINAL
                    || a.getAnomalyType() == AnomalyType.STATE_REGRESSION) {
                CandidateAssessment rr = candidate("TERMINAL_STATE_OVERWRITE", "HIGH", "R04",
                        Directness.DIRECT_SIGNATURE);
                rr.getEvidence().add(evidence(a));
                rr.getSuggestions().add("Implement compare-and-set for saga state updates");
                rr.getSuggestions().add("Reject events arriving after terminal state");
                return rr;
            }
        }
        return null;
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

            var workflow = workflowLoader.getOrderSagaWorkflow();
            
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
                            Directness.CORROBORATED_INFERENCE);
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
                    Directness.DIRECT_SIGNATURE);
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

    
    
    
    
    
    private CandidateAssessment ruleDownstreamTimeout(List<Anomaly> anomalies) {
        for (Anomaly a : anomalies) {
            if (a.getAnomalyType() == AnomalyType.STEP_TIMEOUT) {
                CandidateAssessment rr = candidate("DOWNSTREAM_TIMEOUT", "HIGH", "R07",
                        Directness.THRESHOLD_VIOLATION);
                rr.getEvidence().add(evidence(a));
                rr.getSuggestions().add("Tune step timeout thresholds in workflow spec");
                rr.getSuggestions().add("Check downstream service performance");
                return rr;
            }
        }
        return null;
    }

    
    
    
    private CandidateAssessment ruleCompensationNotTriggered(List<Anomaly> anomalies) {
        for (Anomaly a : anomalies) {
            if (a.getAnomalyType() == AnomalyType.COMPENSATION_MISSING) {
                CandidateAssessment rr = candidate("COMPENSATION_NOT_TRIGGERED", "HIGH", "R09",
                        Directness.DIRECT_SIGNATURE);
                rr.getEvidence().add(evidence(a));
                rr.getSuggestions().add("Check if orchestrator (order-service) is running");
                rr.getSuggestions().add("Verify compensation trigger logic");
                return rr;
            }
        }
        return null;
    }

    
    
    
    
    private CandidateAssessment ruleCompensationStepFailure(SagaInstance saga, List<SagaEvent> events,
            List<Anomaly> anomalies) {
        boolean hasRefundCommand = events.stream()
                .anyMatch(e -> "REFUND_PAYMENT".equals(e.getEventType()));
        boolean hasRefundCompleted = events.stream()
                .anyMatch(e -> "PAYMENT_REFUNDED".equals(e.getEventType()));
        boolean hasMissingCompAck = anomalies.stream()
                .anyMatch(a -> a.getAnomalyType() == AnomalyType.MISSING_COMPENSATION_ACK);

        if (hasRefundCommand && !hasRefundCompleted
                && (saga.getStatus() == SagaStatus.COMPENSATING || hasMissingCompAck)) {
            CandidateAssessment rr = candidate("COMPENSATION_STEP_FAILURE", "HIGH", "R10",
                    Directness.DIRECT_SIGNATURE);
            rr.getEvidence().add(evidence("Compensation started (REFUND_PAYMENT) but no PAYMENT_REFUNDED received"));
            rr.getEvidence().add(evidence("Saga stuck in COMPENSATING status"));
            rr.getSuggestions().add("Check payment-service logs for refund errors");
            rr.getSuggestions().add("Verify Kafka connectivity for refund delivery");
            return rr;
        }
        return null;
    }

    
    
    
    
    private CandidateAssessment ruleForwardStepFailure(SagaInstance saga, List<SagaEvent> events) {
        boolean hasRefundCommand = events.stream()
                .anyMatch(e -> "REFUND_PAYMENT".equals(e.getEventType()));
        boolean hasRefundCompleted = events.stream()
                .anyMatch(e -> "PAYMENT_REFUNDED".equals(e.getEventType()));

        if (saga.getStatus() == SagaStatus.COMPENSATED && hasRefundCommand && hasRefundCompleted) {
            CandidateAssessment rr = candidate("FORWARD_STEP_FAILURE", "LOW", "R11",
                    Directness.DIRECT_SIGNATURE);
            rr.getEvidence().add(evidence("Compensation completed: REFUND_PAYMENT → PAYMENT_REFUNDED"));
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

    
    
    private CandidateAssessment ruleInvalidTransition(List<Anomaly> anomalies) {
        for (Anomaly a : anomalies) {
            if (a.getAnomalyType() == AnomalyType.INVALID_TRANSITION) {
                CandidateAssessment rr = candidate("INVALID_TRANSITION", "MEDIUM", "R12",
                        Directness.DIRECT_SIGNATURE);
                rr.getEvidence().add(evidence(a));
                rr.getSuggestions().add("Verify event routing in upstream services");
                rr.getSuggestions().add("Review workflow spec transitions");
                return rr;
            }
        }
        return null;
    }

    
    
    private CandidateAssessment ruleDuplicateDelivery(List<Anomaly> anomalies) {
        for (Anomaly a : anomalies) {
            if (a.getAnomalyType() == AnomalyType.EXACT_DUPLICATE_EVENT
                    || a.getAnomalyType() == AnomalyType.DUPLICATE_COMPENSATION) {
                CandidateAssessment rr = candidate("DUPLICATE_DELIVERY", "LOW", "R13",
                        Directness.DIRECT_SIGNATURE);
                rr.getEvidence().add(evidence(a));
                rr.getSuggestions().add("Verify producer idempotency configuration");
                rr.getSuggestions().add("Check consumer deduplication mechanisms");
                return rr;
            }
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
                        Directness.THRESHOLD_VIOLATION);
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

    
    
    
    
    
    private CandidateAssessment ruleWrongSagaCorrelationKey(List<Anomaly> anomalies) {
        for (Anomaly a : anomalies) {
            if (a.getAnomalyType() == AnomalyType.ORPHAN_EVENT) {
                CandidateAssessment rr = candidate("WRONG_SAGA_CORRELATION_KEY", "HIGH", "R15",
                        Directness.DIRECT_SIGNATURE);
                rr.getEvidence().add(evidence(a));
                rr.getSuggestions().add("Verify sagaId propagation across services");
                rr.getSuggestions().add("Check Kafka header saga-id configuration");
                return rr;
            }
        }
        return null;
    }

    
    
    private CandidateAssessment ruleMissingTracePropagation(List<Anomaly> anomalies) {
        for (Anomaly a : anomalies) {
            if (a.getAnomalyType() == AnomalyType.MISSING_TRACE_CONTEXT) {
                CandidateAssessment rr = candidate("MISSING_TRACE_PROPAGATION", "MEDIUM", "R19",
                        Directness.DIRECT_SIGNATURE);
                rr.getEvidence().add(evidence(a));
                rr.getEvidence().add(evidence("Events missing traceId cannot be correlated in Jaeger"));
                rr.getSuggestions().add("Verify trace context propagation in service configuration");
                rr.getSuggestions().add("Check OpenTelemetry agent or Spring Sleuth setup");
                rr.getSuggestions().add("Ensure Kafka headers include traceparent/traceId");
                return rr;
            }
        }
        return null;
    }

    
    
    
    
    
    private CandidateAssessment ruleObservabilityGap(List<Anomaly> anomalies) {
        if (!anomalies.isEmpty()) {
            CandidateAssessment rr = candidate("OBSERVABILITY_GAP", "LOW", "R20",
                    Directness.HEURISTIC_INFERENCE);
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
                    Directness.HEURISTIC_INFERENCE);
            rr.getEvidence().add(evidence("Saga stuck in " + saga.getStatus() + " status"));
            rr.getEvidence().add(evidence("No specific root cause identified — insufficient evidence"));
            rr.getSuggestions().add("Check all downstream service health");
            rr.getSuggestions().add("Verify Kafka consumer group offsets");
            return rr;
        }
        return null;
    }

    
    
    
    
    
    private CandidateAssessment ruleDelayedCompensation(List<Anomaly> anomalies) {
        for (Anomaly a : anomalies) {
            if (a.getAnomalyType() == AnomalyType.DELAYED_COMPENSATION) {
                CandidateAssessment rr = candidate("DELAYED_COMPENSATION", "MEDIUM", "R22",
                        Directness.THRESHOLD_VIOLATION);
                rr.getEvidence().add(evidence(a));
                rr.getSuggestions().add("Review orchestrator compensation trigger latency");
                rr.getSuggestions().add("Check message broker throughput during compensation");
                return rr;
            }
        }
        return null;
    }

    
    
    
    private CandidateAssessment candidate(String rootCause, String severity, String ruleId,
            Directness directness) {
        return CandidateAssessment.builder()
                .rootCause(rootCause)
                .severity(severity)
                .ruleId(ruleId)
                .directness(directness)
                .build();
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

    private List<RcaCandidate> candidatesWithStatus(SelectionResult selection, CandidateStatus status) {
        return selection.getCandidates().stream()
                .filter(candidate -> candidate.getStatus() == status)
                .collect(Collectors.toList());
    }

    private List<RcaCandidate> evidenceCandidates(SelectionResult selection) {
        return selection.getCandidates().stream()
                .filter(candidate -> isEvidenceStatus(candidate.getStatus()))
                .collect(Collectors.toCollection(ArrayList::new));
    }

    private boolean isEvidenceStatus(CandidateStatus status) {
        return status == CandidateStatus.STRONG_EVIDENCE
                || status == CandidateStatus.SUPPORTING_EVIDENCE
                || status == CandidateStatus.WEAK_EVIDENCE
                || status == CandidateStatus.SELECTED
                || status == CandidateStatus.CONTRIBUTING;
    }

    private void appendEvidenceGroup(List<String> evidence,
            List<String> suggestions,
            String groupLabel,
            List<RcaCandidate> groupCandidates) {
        if (groupCandidates == null || groupCandidates.isEmpty()) {
            return;
        }
        groupCandidates.sort(this::compareCandidateStrength);
        for (RcaCandidate candidate : groupCandidates) {
            List<String> facts = evidenceFacts(candidate);
            String factSummary = facts.isEmpty() ? "No structured evidence facts recorded" : String.join("; ", facts);
            evidence.add(String.format("[%s] %s (%s): %s",
                    groupLabel, candidate.getRootCause(), candidate.getRuleId(), factSummary));
            suggestions.addAll(candidate.getSuggestions());
        }
    }

    private int compareCandidateStrength(RcaCandidate first, RcaCandidate second) {
        int status = Integer.compare(statusRank(first.getStatus()), statusRank(second.getStatus()));
        if (status != 0) return status;
        int directness = Integer.compare(enumRank(first.getDirectness()), enumRank(second.getDirectness()));
        if (directness != 0) return directness;
        int severity = Integer.compare(severityRank(second.getSeverity()), severityRank(first.getSeverity()));
        if (severity != 0) return severity;
        return Comparator.nullsLast(String::compareTo).compare(first.getRuleId(), second.getRuleId());
    }

    private int statusRank(CandidateStatus status) {
        if (status == CandidateStatus.STRONG_EVIDENCE || status == CandidateStatus.SELECTED) return 0;
        if (status == CandidateStatus.SUPPORTING_EVIDENCE || status == CandidateStatus.CONTRIBUTING) return 1;
        if (status == CandidateStatus.WEAK_EVIDENCE || status == CandidateStatus.POSSIBLE_BUT_UNSUPPORTED) return 2;
        if (status == CandidateStatus.RULED_OUT) return 3;
        return 4;
    }

    private int enumRank(Enum<?> value) {
        return value != null ? value.ordinal() : Integer.MAX_VALUE;
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

    private String fallbackSeverity(String outcome, SelectionResult selection) {
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
