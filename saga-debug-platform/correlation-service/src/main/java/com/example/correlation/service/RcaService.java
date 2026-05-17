package com.example.correlation.service;

import com.example.correlation.domain.*;
import com.example.correlation.domain.rca.CandidateStatus;
import com.example.correlation.domain.rca.CandidateAssessment;
import com.example.correlation.domain.rca.Directness;
import com.example.correlation.domain.rca.Evidence;
import com.example.correlation.domain.rca.EvidenceSource;
import com.example.correlation.domain.rca.RcaCandidate;
import com.example.correlation.domain.rca.RcaStatus;
import com.example.correlation.domain.rca.Specificity;
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

        // Bug 1 fix: upsert semantics — remove previous RCA results to prevent duplicates
        rcaResultRepository.deleteAllBySagaId(sagaId);

        List<SagaEvent> events = sagaEventRepository.findBySagaIdOrderByTimestampAsc(sagaId);
        List<Anomaly> anomalies = anomalyRepository.findBySagaId(sagaId);

        // Collect all matching candidates (multi-rule matching).
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

        // Bug 4 fix: R20 and R21 are BOTH fallback-only — only use when no other rule matched.
        // Previously R21 was in the main loop and always preempted R20.
        addIfPresent(candidates, ruleStuckUnknownCause(saga, anomalies));
        addIfPresent(candidates, ruleObservabilityGap(anomalies));

        SelectionResult selection = rcaSelector.select(candidates, anomalies, saga);
        RcaStatus rcaStatus = selection.getRcaStatus();
        List<RcaCandidate> selectedCandidates = candidatesWithStatus(selection, CandidateStatus.SELECTED);
        selectedCandidates.sort(this::compareCandidateStrength);
        List<RcaCandidate> contributing = candidatesWithStatus(selection, CandidateStatus.CONTRIBUTING);
        RcaCandidate primary = selectedCandidates.isEmpty() ? null : selectedCandidates.get(0);

        // Determine outcome — now aware of RCA result
        String outcome = determineOutcome(saga, anomalies, rcaStatus);

        // Build merged evidence and suggestions
        List<String> evidence = new ArrayList<>();
        List<String> suggestions = new ArrayList<>();

        for (RcaCandidate selected : selectedCandidates) {
            evidence.addAll(evidenceFacts(selected));
            suggestions.addAll(selected.getSuggestions());
        }

        // Append contributing causes
        for (RcaCandidate cr : contributing) {
            evidence.add(String.format("[Contributing: %s (%s, CONTRIBUTING)] %s",
                    cr.getRootCause(), cr.getRuleId(),
                    String.join("; ", evidenceFacts(cr))));
        }

        // Fallback: no rule matched
        if (selectedCandidates.isEmpty()) {
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

        // Numeric confidence is intentionally not persisted in Phase 3+.

        // Add trace IDs for Jaeger link
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

        String selectedRootCause = primary != null ? primary.getRootCause() : null;

        // Save RCA result
        try {
            RcaResult result = new RcaResult();
            result.setSagaId(sagaId);
            result.setRootCause(selectedRootCause);
            result.setOutcome(outcome);
            result.setSeverity(primary != null ? primary.getSeverity() : fallbackSeverity(outcome, selection));
            result.setConfidence(null);
            result.setRuleId(primary != null ? primary.getRuleId() : null);
            result.setEvidence(objectMapper.writeValueAsString(evidence));
            result.setSuggestions(objectMapper.writeValueAsString(suggestions));
            result.setRcaStatus(rcaStatus);
            result.setPrimaryRootCauses(selection.getPrimaryRootCauses());
            result.setCandidates(selection.getCandidates());
            result.setMissingEvidence(selection.getMissingEvidence());

            // Legacy JSON compatibility: no percentage/confidence in labels.
            if (!contributing.isEmpty()) {
                List<String> contributingLabels = contributing.stream()
                        .map(cr -> cr.getRootCause() + " (" + cr.getRuleId() + ", CONTRIBUTING)")
                        .collect(Collectors.toList());
                result.setContributingCauses(objectMapper.writeValueAsString(contributingLabels));
            }

            rcaResultRepository.save(result);
            emitAlert(result, evidence);

            log.info("RCA saga={}: rootCause={}, rcaStatus={}, outcome={}, severity={}, contributing={}",
                    sagaId, result.getRootCause(), rcaStatus, outcome, result.getSeverity(),
                    contributing.stream().map(RcaCandidate::getRootCause).collect(Collectors.toList()));
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
        if (result.getRcaStatus() != RcaStatus.SELECTED || result.getRootCause() == null) {
            return;
        }

        try {
            List<String> primaryRootCauses = result.getPrimaryRootCauses();
            if (primaryRootCauses == null || primaryRootCauses.isEmpty()) {
                primaryRootCauses = List.of(result.getRootCause());
            }

            alertSink.emit(new AlertEvent(
                    Instant.now(),
                    result.getSagaId(),
                    result.getRcaStatus().name(),
                    primaryRootCauses,
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
        boolean hasRca = rcaStatus == RcaStatus.SELECTED;
        boolean isTerminal = saga.getStatus() == SagaStatus.COMPLETED
                || saga.getStatus() == SagaStatus.COMPENSATED
                || saga.getStatus() == SagaStatus.FAILED;

        if (saga.getStatus() == SagaStatus.COMPLETED && !hasAnomalies) return "NORMAL";
        if (saga.getStatus() == SagaStatus.COMPLETED && hasAnomalies) return "SELF_HEALED";
        if (saga.getStatus() == SagaStatus.COMPENSATED) return "COMPENSATED";
        // FIX: FAILED without anomalies AND without RCA → business rejection (NORMAL)
        // FAILED with RCA (e.g. circuit breaker) or anomalies → FAILED
        if (saga.getStatus() == SagaStatus.FAILED && !hasAnomalies && !hasRca) return "NORMAL";
        if (saga.getStatus() == SagaStatus.FAILED) return "FAILED";
        if (saga.getStatus() == SagaStatus.COMPENSATING) return "INCONSISTENT";
        // Bug 8 fix: remaining statuses are STARTED/IN_PROGRESS (always non-terminal here).
        // A running saga without anomalies is IN_PROGRESS, not NORMAL.
        if (hasAnomalies) return "STILL_STUCK";
        return "IN_PROGRESS";
    }


    // R01 — STRONG: payload contains literal "circuit breaker" / "retry exhausted".
    // Payment service writes that exact phrase only when CB triggers fallback.
    // Label is direct quote of the failure reason.
    private CandidateAssessment ruleCircuitBreakerOpen(List<SagaEvent> events) {
        for (SagaEvent event : events) {
            if ("PAYMENT_FAILED".equals(event.getEventType())) {
                String payload = event.getPayload() != null ? event.getPayload().toLowerCase() : "";
                if (payload.contains("circuit breaker") || payload.contains("retry exhausted")) {
                    CandidateAssessment rr = candidate("CIRCUIT_BREAKER_OPEN", "CRITICAL", "R01",
                            Directness.DIRECT_SIGNATURE, Specificity.LABEL_IS_SIGNATURE);
                    rr.getEvidence().add(Evidence.ofEvent("Payment failed with circuit breaker/retry exhaustion",
                            event.getPayload(), "payment succeeds or reports business failure",
                            EvidenceSource.EVENT_PAYLOAD, event.getEventId()));
                    rr.getEvidence().add(Evidence.of("Payload: " + event.getPayload(),
                            EvidenceSource.EVENT_PAYLOAD, null));
                    rr.getSuggestions().add("Check downstream service health");
                    rr.getSuggestions().add("Review Resilience4j metrics at /actuator/circuitbreakers");
                    return rr;
                }
            }
        }
        return null;
    }

    // R02 — WEAK: RETRY_BURST anomaly is reliable (3+ retries in 60s), but the
    // rule's label asserts "TIMEOUT" specifically caused the retries. The
    // RETRY_BURST detector counts attempts regardless of exception type, so
    // the retries could have been driven by NPE, validation error, or any
    // other RuntimeException — not necessarily a timeout. The named cause
    // is heuristic, hence WEAK. (Renaming to RETRY_STORM would justify
    // promoting this to STRONG; tracked as separate cleanup.)
    private CandidateAssessment ruleTimeoutRetryStorm(List<Anomaly> anomalies, List<SagaEvent> events) {
        for (Anomaly a : anomalies) {
            if (a.getAnomalyType() == AnomalyType.RETRY_BURST) {
                List<SagaEvent> timeoutRetryEvents = events.stream()
                        .filter(e -> "PAYMENT_RETRY".equals(e.getEventType()))
                        .filter(e -> payloadIndicatesTimeout(e.getPayload()))
                        .collect(Collectors.toList());
                boolean hasTimeoutEvidence = !timeoutRetryEvents.isEmpty();
                CandidateAssessment rr = candidate("TIMEOUT_RETRY_STORM", "CRITICAL", "R02",
                        hasTimeoutEvidence ? Directness.DIRECT_SIGNATURE : Directness.HEURISTIC_INFERENCE,
                        hasTimeoutEvidence ? Specificity.LABEL_IS_SIGNATURE : Specificity.LABEL_HEURISTIC);
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
                            EvidenceSource.EVENT_PAYLOAD,
                            null,
                            timeoutRetryEvents.stream()
                                    .map(SagaEvent::getEventId)
                                    .filter(Objects::nonNull)
                                    .collect(Collectors.toList())));
                } else {
                    rr.setMissingForStronger("Retry events do not record timeout exception class.");
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

    // R03 — STRONG: anomaly is detected from the event log directly (saga has
    // multiple conflicting terminal events). The data IS the bug; label = signature.
    private CandidateAssessment ruleMultiTerminalConflict(List<Anomaly> anomalies) {
        for (Anomaly a : anomalies) {
            if (a.getAnomalyType() == AnomalyType.MULTI_TERMINAL_CONFLICT) {
                CandidateAssessment rr = candidate("MULTI_TERMINAL_CONFLICT", "CRITICAL", "R03",
                        Directness.DIRECT_SIGNATURE, Specificity.LABEL_IS_SIGNATURE);
                rr.getEvidence().add(evidence(a));
                rr.getSuggestions().add("Enforce final-state immutability");
                rr.getSuggestions().add("Check for duplicate terminal event delivery");
                return rr;
            }
        }
        return null;
    }

    // R04 — STRONG: FORBIDDEN_POST_TERMINAL / STATE_REGRESSION anomalies come
    // from deterministic state-machine checks against terminal status.
    // Label = signature.
    private CandidateAssessment ruleTerminalStateOverwrite(List<Anomaly> anomalies) {
        for (Anomaly a : anomalies) {
            if (a.getAnomalyType() == AnomalyType.FORBIDDEN_POST_TERMINAL
                    || a.getAnomalyType() == AnomalyType.STATE_REGRESSION) {
                CandidateAssessment rr = candidate("TERMINAL_STATE_OVERWRITE", "HIGH", "R04",
                        Directness.DIRECT_SIGNATURE, Specificity.LABEL_IS_SIGNATURE);
                rr.getEvidence().add(evidence(a));
                rr.getSuggestions().add("Implement compare-and-set for saga state updates");
                rr.getSuggestions().add("Reject events arriving after terminal state");
                return rr;
            }
        }
        return null;
    }

    // R05 — MEDIUM: two corroborating anomalies (SAGA_STUCK + MISSING_TERMINAL_EVENT)
    // plus a 3× timeout threshold is a specific signature, but "service crash"
    // is one of several causes that fit this evidence (deadlock, OOM, blocked DB,
    // network partition, stuck consumer thread). One inferential hop from the
    // signature to the named cause.
    //   Bug 2 fix: also fires for recovered sagas with adjusted severity.
    //   Bug 5 fix: null guard for getSteps(). Bug 6 fix: use anomaly detectedAt.
    //   TODO (magic-number): the "× 3" threshold needs justification or ablation.
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
            // Bug 5 fix: null guard for workflow.getSteps()
            int maxTimeout = detectionProperties.getDefaultStepTimeoutSeconds();
            if (workflow != null && workflow.getSteps() != null) {
                maxTimeout = workflow.getSteps().stream()
                        .mapToInt(s -> workflow.getStepTimeoutSeconds(s.getId()))
                        .max().orElse(detectionProperties.getDefaultStepTimeoutSeconds());
            }
            int serviceCrashTimeoutSeconds = maxTimeout * detectionProperties.getServiceCrashTimeoutMultiplier();

            if (saga.getLastUpdated() != null) {
                // Use the latest corroborating stuck anomaly instead of Instant.now().
                // SAGA_STUCK marks the first threshold breach; MISSING_TERMINAL_EVENT
                // proves the saga stayed stuck long enough to classify service crash.
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
                    // affects severity (operational urgency), not signature
                    // specificity. The cause label is equally inferred either way.
                    CandidateAssessment rr = candidate("SERVICE_CRASH",
                            isRecovered ? "MEDIUM" : "CRITICAL", "R05",
                            Directness.CORROBORATED_INFERENCE, Specificity.LABEL_INFERS_FROM_SIGNATURE);
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

    // R06 — STRONG: label is literally descriptive of the observed sequence —
    // saga went stuck, then reached a terminal status without MISSING_TERMINAL.
    // If STEP_TIMEOUT was already observed before recovery, R07 is the more
    // specific threshold violation. A STEP_TIMEOUT measured on the terminal
    // recovery event is a side-effect of the transient outage and should not
    // mask the recovered-stuck signature.
    // The events themselves prove "transient failure". Severity is LOW because
    // operational impact is small (it self-healed), but confidence in the
    // diagnosis is high.
    //   Recovery-aware: only fires for stuck sagas that recovered WITHOUT
    //   MISSING_TERMINAL_EVENT. If both SAGA_STUCK and MISSING_TERMINAL_EVENT
    //   exist, R05 handles it — even if saga later recovered.
    //   Bug 7 fix: added FAILED to isTerminal check for consistency.
    private CandidateAssessment ruleTransientServiceFailure(SagaInstance saga, List<Anomaly> anomalies) {
        boolean hasStuck = anomalies.stream()
                .anyMatch(a -> a.getAnomalyType() == AnomalyType.SAGA_STUCK);
        boolean hasMissingTerminal = anomalies.stream()
                .anyMatch(a -> a.getAnomalyType() == AnomalyType.MISSING_TERMINAL_EVENT);
        boolean isTerminal = saga.getStatus() == SagaStatus.COMPLETED
                || saga.getStatus() == SagaStatus.COMPENSATED
                || saga.getStatus() == SagaStatus.FAILED;
        boolean hasStepTimeoutBeforeRecovery = hasStepTimeoutBeforeRecovery(saga, anomalies);

        // Only fire for brief transient failures (stuck but no MISSING_TERMINAL_EVENT).
        // If hasMissingTerminal is true, R05 covers this case (even after recovery).
        if (hasStuck && !hasMissingTerminal && !hasStepTimeoutBeforeRecovery && isTerminal) {
            CandidateAssessment rr = candidate("TRANSIENT_SERVICE_FAILURE", "LOW", "R06",
                    Directness.DIRECT_SIGNATURE, Specificity.LABEL_IS_SIGNATURE);
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

    // R07 — WEAK: STEP_TIMEOUT detection is reliable (step exceeded its
    // configured timeout), but the rule's label asserts "DOWNSTREAM" caused
    // the slowness. The same signature could be produced by local GC pause,
    // network jitter on the producer side, or a wrong timeout config on the
    // step itself. The named cause cannot be proved from the signature alone.
    private CandidateAssessment ruleDownstreamTimeout(List<Anomaly> anomalies) {
        for (Anomaly a : anomalies) {
            if (a.getAnomalyType() == AnomalyType.STEP_TIMEOUT) {
                CandidateAssessment rr = candidate("DOWNSTREAM_TIMEOUT", "HIGH", "R07",
                        Directness.THRESHOLD_VIOLATION, Specificity.LABEL_HEURISTIC);
                rr.getEvidence().add(evidence(a));
                rr.getSuggestions().add("Tune step timeout thresholds in workflow spec");
                rr.getSuggestions().add("Check downstream service performance");
                return rr;
            }
        }
        return null;
    }

    // R09 — STRONG: COMPENSATION_MISSING anomaly is a deterministic
    // workflow-state check — saga reached a state where compensation should
    // have started but didn't. Label = signature.
    private CandidateAssessment ruleCompensationNotTriggered(List<Anomaly> anomalies) {
        for (Anomaly a : anomalies) {
            if (a.getAnomalyType() == AnomalyType.COMPENSATION_MISSING) {
                CandidateAssessment rr = candidate("COMPENSATION_NOT_TRIGGERED", "HIGH", "R09",
                        Directness.DIRECT_SIGNATURE, Specificity.LABEL_IS_SIGNATURE);
                rr.getEvidence().add(evidence(a));
                rr.getSuggestions().add("Check if orchestrator (order-service) is running");
                rr.getSuggestions().add("Verify compensation trigger logic");
                return rr;
            }
        }
        return null;
    }

    // R10 — STRONG: three corroborating facts — REFUND_PAYMENT command was
    // emitted, no PAYMENT_REFUNDED ack arrived, and saga is in COMPENSATING
    // (or MISSING_COMPENSATION_ACK fired). Label is descriptive of those
    // observable facts.
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
                    Directness.DIRECT_SIGNATURE, Specificity.LABEL_IS_SIGNATURE);
            rr.getEvidence().add(evidence("Compensation started (REFUND_PAYMENT) but no PAYMENT_REFUNDED received"));
            rr.getEvidence().add(evidence("Saga stuck in COMPENSATING status"));
            rr.getSuggestions().add("Check payment-service logs for refund errors");
            rr.getSuggestions().add("Verify Kafka connectivity for refund delivery");
            return rr;
        }
        return null;
    }

    // R11 — STRONG: saga reached COMPENSATED with both REFUND_PAYMENT and
    // PAYMENT_REFUNDED in the event log. Direct event-log evidence proves the
    // compensation succeeded; label is descriptive. Severity LOW because
    // compensation = recovery, but confidence in the diagnosis is high.
    private CandidateAssessment ruleForwardStepFailure(SagaInstance saga, List<SagaEvent> events) {
        boolean hasRefundCommand = events.stream()
                .anyMatch(e -> "REFUND_PAYMENT".equals(e.getEventType()));
        boolean hasRefundCompleted = events.stream()
                .anyMatch(e -> "PAYMENT_REFUNDED".equals(e.getEventType()));

        if (saga.getStatus() == SagaStatus.COMPENSATED && hasRefundCommand && hasRefundCompleted) {
            CandidateAssessment rr = candidate("FORWARD_STEP_FAILURE", "LOW", "R11",
                    Directness.DIRECT_SIGNATURE, Specificity.LABEL_IS_SIGNATURE);
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

    // R12 — STRONG: INVALID_TRANSITION anomaly fires from direct workflow-spec
    // violation. Label = signature.
    private CandidateAssessment ruleInvalidTransition(List<Anomaly> anomalies) {
        for (Anomaly a : anomalies) {
            if (a.getAnomalyType() == AnomalyType.INVALID_TRANSITION) {
                CandidateAssessment rr = candidate("INVALID_TRANSITION", "MEDIUM", "R12",
                        Directness.DIRECT_SIGNATURE, Specificity.LABEL_IS_SIGNATURE);
                rr.getEvidence().add(evidence(a));
                rr.getSuggestions().add("Verify event routing in upstream services");
                rr.getSuggestions().add("Review workflow spec transitions");
                return rr;
            }
        }
        return null;
    }

    // R13 — STRONG: anomalies fire from exact eventId match in DB —
    // deterministic, direct evidence. Label = signature.
    private CandidateAssessment ruleDuplicateDelivery(List<Anomaly> anomalies) {
        for (Anomaly a : anomalies) {
            if (a.getAnomalyType() == AnomalyType.EXACT_DUPLICATE_EVENT
                    || a.getAnomalyType() == AnomalyType.DUPLICATE_COMPENSATION) {
                CandidateAssessment rr = candidate("DUPLICATE_DELIVERY", "LOW", "R13",
                        Directness.DIRECT_SIGNATURE, Specificity.LABEL_IS_SIGNATURE);
                rr.getEvidence().add(evidence(a));
                rr.getSuggestions().add("Verify producer idempotency configuration");
                rr.getSuggestions().add("Check consumer deduplication mechanisms");
                return rr;
            }
        }
        return null;
    }

    // R14 — MEDIUM: OUT_OF_ORDER detection compares event timestamps to
    // workflow-expected ordering. Detection is reliable, but vulnerable to
    // clock skew across services (false-positive risk). One inferential hop:
    // observed ordering anomaly may or may not be a real delivery problem.
    private CandidateAssessment ruleOutOfOrderDelivery(SagaInstance saga, List<Anomaly> anomalies) {
        boolean isTerminal = saga.getStatus() == SagaStatus.COMPLETED
                || saga.getStatus() == SagaStatus.COMPENSATED;
        for (Anomaly a : anomalies) {
            if (a.getAnomalyType() == AnomalyType.OUT_OF_ORDER) {
                CandidateAssessment rr = candidate("OUT_OF_ORDER_DELIVERY",
                        isTerminal ? "LOW" : "MEDIUM", "R14",
                        Directness.THRESHOLD_VIOLATION, Specificity.LABEL_INFERS_FROM_SIGNATURE);
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

    // R15 — STRONG: ORPHAN_EVENT = event references unknown sagaId. Detected
    // by direct membership check against registered sagas. In our setup,
    // sagas are not deleted, so the only plausible cause of a missing-sagaId
    // event is wrong correlation-key wiring on the producer side. Label =
    // signature for our system.
    private CandidateAssessment ruleWrongSagaCorrelationKey(List<Anomaly> anomalies) {
        for (Anomaly a : anomalies) {
            if (a.getAnomalyType() == AnomalyType.ORPHAN_EVENT) {
                CandidateAssessment rr = candidate("WRONG_SAGA_CORRELATION_KEY", "HIGH", "R15",
                        Directness.DIRECT_SIGNATURE, Specificity.LABEL_NARROWS_SIGNATURE);
                rr.getEvidence().add(evidence(a));
                rr.getSuggestions().add("Verify sagaId propagation across services");
                rr.getSuggestions().add("Check Kafka header saga-id configuration");
                return rr;
            }
        }
        return null;
    }

    // R19 — STRONG: MISSING_TRACE_CONTEXT anomaly is a direct field-level
    // check (event payload's traceId == null). Label = signature exactly.
    private CandidateAssessment ruleMissingTracePropagation(List<Anomaly> anomalies) {
        for (Anomaly a : anomalies) {
            if (a.getAnomalyType() == AnomalyType.MISSING_TRACE_CONTEXT) {
                CandidateAssessment rr = candidate("MISSING_TRACE_PROPAGATION", "MEDIUM", "R19",
                        Directness.DIRECT_SIGNATURE, Specificity.LABEL_IS_SIGNATURE);
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

    // R20 — FALLBACK: by design, fires only when no other rule matched.
    // Not a confidence claim — represents "anomaly seen but no specific
    // cause identified". The 0.30 value is intentionally below the
    // contributing-cause filter threshold so this never appears as a
    // contributing cause when a real primary exists.
    private CandidateAssessment ruleObservabilityGap(List<Anomaly> anomalies) {
        if (!anomalies.isEmpty()) {
            CandidateAssessment rr = candidate("OBSERVABILITY_GAP", "LOW", "R20",
                    Directness.HEURISTIC_INFERENCE, Specificity.LABEL_HEURISTIC);
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

    // R21 — FALLBACK: by design, fires only when no other rule matched.
    // Saga is currently stuck and non-terminal; no specific cause identified.
    // Note: severity is HIGH (operational urgency — saga is stuck right now)
    // even though confidence is FALLBACK (low diagnostic certainty).
    // Severity and confidence measure different things.
    private CandidateAssessment ruleStuckUnknownCause(SagaInstance saga, List<Anomaly> anomalies) {
        boolean hasStuck = anomalies.stream()
                .anyMatch(a -> a.getAnomalyType() == AnomalyType.SAGA_STUCK);
        boolean isNonTerminal = saga.getStatus() != SagaStatus.COMPLETED
                && saga.getStatus() != SagaStatus.COMPENSATED
                && saga.getStatus() != SagaStatus.FAILED;

        if (hasStuck && isNonTerminal) {
            CandidateAssessment rr = candidate("STUCK_UNKNOWN_CAUSE", "HIGH", "R21",
                    Directness.HEURISTIC_INFERENCE, Specificity.LABEL_HEURISTIC);
            rr.getEvidence().add(evidence("Saga stuck in " + saga.getStatus() + " status"));
            rr.getEvidence().add(evidence("No specific root cause identified — insufficient evidence"));
            rr.getSuggestions().add("Check all downstream service health");
            rr.getSuggestions().add("Verify Kafka consumer group offsets");
            return rr;
        }
        return null;
    }

    // R22 — MEDIUM: DELAYED_COMPENSATION anomaly is reliably detected, but
    // the underlying threshold (compensation × 1.5) is currently an
    // unjustified magic number. Once the threshold is defended via ablation
    // or reasoning, this rule can move to STRONG.
    //   TODO (magic-number): defend or remove the × 1.5 threshold.
    private CandidateAssessment ruleDelayedCompensation(List<Anomaly> anomalies) {
        for (Anomaly a : anomalies) {
            if (a.getAnomalyType() == AnomalyType.DELAYED_COMPENSATION) {
                CandidateAssessment rr = candidate("DELAYED_COMPENSATION", "MEDIUM", "R22",
                        Directness.THRESHOLD_VIOLATION, Specificity.LABEL_INFERS_FROM_SIGNATURE);
                rr.getEvidence().add(evidence(a));
                rr.getSuggestions().add("Review orchestrator compensation trigger latency");
                rr.getSuggestions().add("Check message broker throughput during compensation");
                return rr;
            }
        }
        return null;
    }

    // =========================================================================
    // Utility
    // =========================================================================
    private CandidateAssessment candidate(String rootCause, String severity, String ruleId,
            Directness directness, Specificity specificity) {
        return CandidateAssessment.builder()
                .rootCause(rootCause)
                .severity(severity)
                .ruleId(ruleId)
                .directness(directness)
                .specificity(specificity)
                .build();
    }

    private Evidence evidence(String fact) {
        return Evidence.of(fact, EvidenceSource.CODE_CONSTANT, null);
    }

    private Evidence evidence(Anomaly anomaly) {
        if (anomaly == null) return null;
        return Evidence.of(
                anomaly.getDescription(),
                anomaly.getObservedValue(),
                anomaly.getExpectedValue(),
                sourceFrom(anomaly.getThresholdSource()),
                anomaly.getId(),
                anomaly.getEvidenceEventIds());
    }

    private EvidenceSource sourceFrom(String thresholdSource) {
        if (thresholdSource == null) return EvidenceSource.CODE_CONSTANT;
        if (thresholdSource.startsWith("workflow:")) return EvidenceSource.WORKFLOW_SPEC;
        if (thresholdSource.startsWith("config:")) return EvidenceSource.CONFIG;
        if (thresholdSource.startsWith("state:") || thresholdSource.startsWith("db:")) {
            return EvidenceSource.STATE_MACHINE;
        }
        return EvidenceSource.CODE_CONSTANT;
    }

    private List<RcaCandidate> candidatesWithStatus(SelectionResult selection, CandidateStatus status) {
        return selection.getCandidates().stream()
                .filter(candidate -> candidate.getStatus() == status)
                .collect(Collectors.toList());
    }

    private int compareCandidateStrength(RcaCandidate first, RcaCandidate second) {
        int directness = Integer.compare(enumRank(first.getDirectness()), enumRank(second.getDirectness()));
        if (directness != 0) return directness;
        int specificity = Integer.compare(enumRank(first.getSpecificity()), enumRank(second.getSpecificity()));
        if (specificity != 0) return specificity;
        return Comparator.nullsLast(String::compareTo).compare(first.getRuleId(), second.getRuleId());
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

    private String fallbackSeverity(String outcome, SelectionResult selection) {
        if ("NORMAL".equals(outcome)) return "NONE";
        return selection.getCandidates().stream()
                .map(RcaCandidate::getSeverity)
                .filter(Objects::nonNull)
                .filter(severity -> "HIGH".equals(severity) || "CRITICAL".equals(severity))
                .findFirst()
                .orElse("LOW");
    }

    private String extractTraceId(String payloadJson) {
        if (payloadJson == null || payloadJson.isBlank()) return null;
        try {
            var node = objectMapper.readTree(payloadJson);
            if (node.has("traceId") && !node.get("traceId").isNull()) {
                return node.get("traceId").asText();
            }
        } catch (Exception e) {
            // Not valid JSON or no traceId
        }
        return null;
    }
}
