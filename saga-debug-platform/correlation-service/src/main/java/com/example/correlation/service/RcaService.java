package com.example.correlation.service;

import com.example.correlation.domain.*;
import com.example.correlation.repository.AnomalyRepository;
import com.example.correlation.repository.RcaResultRepository;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.example.correlation.service.ranking.RcaRankingScorer;
import com.example.correlation.workflow.WorkflowLoader;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
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
    private final RcaRankingScorer rankingScorer;

    public RcaService(SagaInstanceRepository sagaInstanceRepository,
            SagaEventRepository sagaEventRepository,
            AnomalyRepository anomalyRepository,
            RcaResultRepository rcaResultRepository,
            WorkflowLoader workflowLoader,
            ObjectMapper objectMapper,
            RcaRankingScorer rankingScorer) {
        this.sagaInstanceRepository = sagaInstanceRepository;
        this.sagaEventRepository = sagaEventRepository;
        this.anomalyRepository = anomalyRepository;
        this.rcaResultRepository = rcaResultRepository;
        this.workflowLoader = workflowLoader;
        this.objectMapper = objectMapper;
        this.rankingScorer = rankingScorer;
    }

    /**
     * Compute the ranking score for a rule using the active
     * {@link RcaRankingScorer}. Extracted so the comparator at the
     * sort site is short and so unit tests can verify per-rule scores.
     */
    private double scoreOf(RuleResult rr) {
        return rankingScorer.score(rr.confidence, rr.severity, rr.evidence.size());
    }


    private static class RuleResult {
        final String rootCause;
        final String severity;
        final double confidence;
        final String ruleId;
        final List<String> evidence;
        final List<String> suggestions;

        RuleResult(String rootCause, String severity, double confidence, String ruleId,
                   List<String> evidence, List<String> suggestions) {
            this.rootCause = rootCause;
            this.severity = severity;
            this.confidence = confidence;
            this.ruleId = ruleId;
            this.evidence = evidence != null ? evidence : new ArrayList<>();
            this.suggestions = suggestions != null ? suggestions : new ArrayList<>();
        }

        static RuleResult of(String rootCause, String severity, double confidence, String ruleId) {
            return new RuleResult(rootCause, severity, confidence, ruleId,
                    new ArrayList<>(), new ArrayList<>());
        }
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

        // Collect ALL matching rules (multi-rule matching)
        List<RuleResult> allResults = new ArrayList<>();
        addIfPresent(allResults, ruleCircuitBreakerOpen(events));
        addIfPresent(allResults, ruleTimeoutRetryStorm(anomalies));
        addIfPresent(allResults, ruleMultiTerminalConflict(anomalies));
        addIfPresent(allResults, ruleTerminalStateOverwrite(anomalies));
        addIfPresent(allResults, ruleServiceCrash(saga, anomalies, events));
        addIfPresent(allResults, ruleTransientServiceFailure(saga, anomalies));
        addIfPresent(allResults, ruleDownstreamTimeout(anomalies));
        addIfPresent(allResults, ruleDownstreamSlowResponse(anomalies));
        addIfPresent(allResults, ruleCompensationNotTriggered(anomalies));
        addIfPresent(allResults, ruleCompensationStepFailure(saga, events, anomalies));
        addIfPresent(allResults, ruleForwardStepFailure(saga, events));
        addIfPresent(allResults, ruleInvalidTransition(anomalies));
        addIfPresent(allResults, ruleDuplicateDelivery(anomalies));
        addIfPresent(allResults, ruleOutOfOrderDelivery(saga, anomalies));
        addIfPresent(allResults, ruleWrongSagaCorrelationKey(anomalies));
        addIfPresent(allResults, ruleBrokerDelayQueueing(anomalies));
        addIfPresent(allResults, ruleConsumerLagBacklog(anomalies));
        addIfPresent(allResults, ruleSchemaMismatch(anomalies));
        addIfPresent(allResults, ruleMissingTracePropagation(anomalies));
        addIfPresent(allResults, ruleDelayedCompensation(anomalies));

        // Bug 4 fix: R20 and R21 are BOTH fallback-only — only use when no other rule matched.
        // Previously R21 was in the main loop and always preempted R20.
        if (allResults.isEmpty()) {
            addIfPresent(allResults, ruleStuckUnknownCause(saga, anomalies));
            if (allResults.isEmpty()) {
                addIfPresent(allResults, ruleObservabilityGap(anomalies));
            }
        }

        // Rank rules by the active multi-criteria score (RcaRankingScorer).
        // The default profile is "confidence-only" (w_c=1, w_s=0, w_e=0) which
        // reproduces the original "sort by confidence" behaviour exactly. Other
        // profiles (confidence-heavy / equal-weight / severity-heavy /
        // evidence-heavy) are opt-in via Spring profiles or property overrides
        // for the ablation harness; see RankingProperties JavaDoc.
        allResults.sort((a, b) -> Double.compare(scoreOf(b), scoreOf(a)));

        RuleResult primary = allResults.isEmpty() ? null : allResults.get(0);
        // Filter out low-confidence fallback rules (R20, R21) from contributing
        // causes when a strong primary exists.
        List<RuleResult> contributing = allResults.size() > 1
                ? allResults.subList(1, allResults.size()).stream()
                    .filter(r -> r.confidence > 0.30 || primary.confidence <= 0.30)
                    .collect(java.util.stream.Collectors.toList())
                : List.of();

        // Determine outcome — now aware of RCA result
        String outcome = determineOutcome(saga, anomalies, primary);

        // Build merged evidence and suggestions
        List<String> evidence = new ArrayList<>();
        List<String> suggestions = new ArrayList<>();

        if (primary != null) {
            evidence.addAll(primary.evidence);
            suggestions.addAll(primary.suggestions);
        }

        // Append contributing causes
        for (RuleResult cr : contributing) {
            evidence.add(String.format("[Contributing: %s (%.0f%% confidence, %s)] %s",
                    cr.rootCause, cr.confidence * 100, cr.ruleId,
                    String.join("; ", cr.evidence)));
        }

        // Fallback: no rule matched
        if (primary == null) {
            if ("NORMAL".equals(outcome)) {
                evidence.add("Saga completed successfully with " + events.size() + " events");
                if (saga.getStartTime() != null && saga.getEndTime() != null) {
                    long ms = Duration.between(saga.getStartTime(), saga.getEndTime()).toMillis();
                    evidence.add("Total duration: " + ms + "ms");
                }
            } else {
                evidence.add("No matching RCA rules. Saga status: " + saga.getStatus());
                suggestions.add("Review saga events manually");
            }
        }

        // The final confidence equals the primary rule's tier value (Confidence.STRONG
        // / MEDIUM / WEAK / FALLBACK). The previous "boost" — adding 0.03 per distinct
        // co-firing anomaly type, capped at +0.10 — has been removed. The "+0.03 per
        // anomaly" weight was never justified, and it violated the tier system: a
        // STRONG rule (0.90) could end up at 0.97, claiming higher precision than the
        // tier definitions support. Co-firing anomalies are still surfaced as
        // contributing causes (see `contributing` above), which is the correct way to
        // express that multiple signals corroborate the primary diagnosis.
        double confidence = primary != null ? primary.confidence : 1.0;

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

        // Save RCA result
        try {
            RcaResult result = new RcaResult();
            result.setSagaId(sagaId);
            result.setRootCause(primary != null ? primary.rootCause : null);
            result.setOutcome(outcome);
            result.setSeverity(primary != null ? primary.severity : ("NORMAL".equals(outcome) ? "NONE" : "LOW"));
            result.setConfidence(confidence);
            result.setRuleId(primary != null ? primary.ruleId : null);
            result.setEvidence(objectMapper.writeValueAsString(evidence));
            result.setSuggestions(objectMapper.writeValueAsString(suggestions));

            // Store contributing causes in evidence JSON for thesis analysis
            if (!contributing.isEmpty()) {
                List<String> contributingLabels = contributing.stream()
                        .map(cr -> cr.rootCause + " (" + cr.ruleId + ", " +
                                String.format("%.0f%%", cr.confidence * 100) + ")")
                        .collect(Collectors.toList());
                result.setContributingCauses(objectMapper.writeValueAsString(contributingLabels));
            }

            rcaResultRepository.save(result);

            log.info("RCA saga={}: rootCause={}, outcome={}, severity={}, confidence={}, contributing={}",
                    sagaId, result.getRootCause(), outcome, result.getSeverity(), confidence,
                    contributing.stream().map(cr -> cr.rootCause).collect(Collectors.toList()));
            return result;
        } catch (Exception e) {
            log.error("Failed to save RCA result for saga {}: {}", sagaId, e.getMessage());
            return null;
        }
    }

    public List<RcaResult> getResults(String sagaId) {
        return rcaResultRepository.findBySagaIdOrderByCreatedAtDesc(sagaId);
    }

    private void addIfPresent(List<RuleResult> list, RuleResult rr) {
        if (rr != null) list.add(rr);
    }

    private String determineOutcome(SagaInstance saga, List<Anomaly> anomalies, RuleResult rr) {
        boolean hasAnomalies = !anomalies.isEmpty();
        boolean hasRca = rr != null;
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
    private RuleResult ruleCircuitBreakerOpen(List<SagaEvent> events) {
        for (SagaEvent event : events) {
            if ("PAYMENT_FAILED".equals(event.getEventType())) {
                String payload = event.getPayload() != null ? event.getPayload().toLowerCase() : "";
                if (payload.contains("circuit breaker") || payload.contains("retry exhausted")) {
                    RuleResult rr = RuleResult.of("CIRCUIT_BREAKER_OPEN", "CRITICAL",
                            Confidence.STRONG.value, "R01");
                    rr.evidence.add("Payment failed with circuit breaker/retry exhaustion");
                    rr.evidence.add("Payload: " + event.getPayload());
                    rr.suggestions.add("Check downstream service health");
                    rr.suggestions.add("Review Resilience4j metrics at /actuator/circuitbreakers");
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
    private RuleResult ruleTimeoutRetryStorm(List<Anomaly> anomalies) {
        for (Anomaly a : anomalies) {
            if (a.getAnomalyType() == AnomalyType.RETRY_BURST) {
                RuleResult rr = RuleResult.of("TIMEOUT_RETRY_STORM", "CRITICAL",
                        Confidence.WEAK.value, "R02");
                rr.evidence.add(a.getDescription());
                rr.suggestions.add("Reduce retry count or increase backoff");
                rr.suggestions.add("Check service logs for timeout root cause");
                return rr;
            }
        }
        return null;
    }

    // R03 — STRONG: anomaly is detected from the event log directly (saga has
    // multiple conflicting terminal events). The data IS the bug; label = signature.
    private RuleResult ruleMultiTerminalConflict(List<Anomaly> anomalies) {
        for (Anomaly a : anomalies) {
            if (a.getAnomalyType() == AnomalyType.MULTI_TERMINAL_CONFLICT) {
                RuleResult rr = RuleResult.of("MULTI_TERMINAL_CONFLICT", "CRITICAL",
                        Confidence.STRONG.value, "R03");
                rr.evidence.add(a.getDescription());
                rr.suggestions.add("Enforce final-state immutability");
                rr.suggestions.add("Check for duplicate terminal event delivery");
                return rr;
            }
        }
        return null;
    }

    // R04 — STRONG: FORBIDDEN_POST_TERMINAL / STATE_REGRESSION anomalies come
    // from deterministic state-machine checks against terminal status.
    // Label = signature.
    private RuleResult ruleTerminalStateOverwrite(List<Anomaly> anomalies) {
        for (Anomaly a : anomalies) {
            if (a.getAnomalyType() == AnomalyType.FORBIDDEN_POST_TERMINAL
                    || a.getAnomalyType() == AnomalyType.STATE_REGRESSION) {
                RuleResult rr = RuleResult.of("TERMINAL_STATE_OVERWRITE", "HIGH",
                        Confidence.STRONG.value, "R04");
                rr.evidence.add(a.getDescription());
                rr.suggestions.add("Implement compare-and-set for saga state updates");
                rr.suggestions.add("Reject events arriving after terminal state");
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
    private RuleResult ruleServiceCrash(SagaInstance saga, List<Anomaly> anomalies,
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
            int maxTimeout = 60; // default
            if (workflow != null && workflow.getSteps() != null) {
                maxTimeout = workflow.getSteps().stream()
                        .mapToInt(s -> workflow.getStepTimeoutSeconds(s.getId()))
                        .max().orElse(60);
            }

            if (saga.getLastUpdated() != null) {
                // Bug 6 fix: use SAGA_STUCK anomaly's detectedAt for accurate measurement
                // instead of Instant.now() which depends on when analyze() is called
                Instant stuckMeasurePoint = anomalies.stream()
                        .filter(a -> a.getAnomalyType() == AnomalyType.SAGA_STUCK)
                        .map(Anomaly::getDetectedAt)
                        .max(Instant::compareTo)
                        .orElse(Instant.now());
                long stuckSeconds = Duration.between(saga.getLastUpdated(), stuckMeasurePoint).getSeconds();

                if (stuckSeconds > maxTimeout * 3) {
                    // Both branches use Confidence.MEDIUM — the "recovered" status
                    // affects severity (operational urgency), not signature
                    // specificity. The cause label is equally inferred either way.
                    RuleResult rr = RuleResult.of("SERVICE_CRASH",
                            isRecovered ? "MEDIUM" : "CRITICAL",
                            Confidence.MEDIUM.value, "R05");
                    rr.evidence.add("Saga stuck for " + stuckSeconds + "s (>" + (maxTimeout * 3) + "s threshold)");
                    rr.evidence.add("Both SAGA_STUCK and MISSING_TERMINAL_EVENT anomalies present");
                    if (isRecovered) {
                        rr.evidence.add("Service recovered → saga now " + saga.getStatus());
                    }
                    rr.suggestions.add("Check if downstream service container is running");
                    rr.suggestions.add("Verify docker container health and restart policy");
                    return rr;
                }
            }
        }
        return null;
    }

    // R06 — STRONG: label is literally descriptive of the observed sequence —
    // saga went stuck, then reached a terminal status without MISSING_TERMINAL.
    // The events themselves prove "transient failure". Severity is LOW because
    // operational impact is small (it self-healed), but confidence in the
    // diagnosis is high.
    //   Recovery-aware: only fires for stuck sagas that recovered WITHOUT
    //   MISSING_TERMINAL_EVENT. If both SAGA_STUCK and MISSING_TERMINAL_EVENT
    //   exist, R05 handles it — even if saga later recovered.
    //   Bug 7 fix: added FAILED to isTerminal check for consistency.
    private RuleResult ruleTransientServiceFailure(SagaInstance saga, List<Anomaly> anomalies) {
        boolean hasStuck = anomalies.stream()
                .anyMatch(a -> a.getAnomalyType() == AnomalyType.SAGA_STUCK);
        boolean hasMissingTerminal = anomalies.stream()
                .anyMatch(a -> a.getAnomalyType() == AnomalyType.MISSING_TERMINAL_EVENT);
        boolean isTerminal = saga.getStatus() == SagaStatus.COMPLETED
                || saga.getStatus() == SagaStatus.COMPENSATED
                || saga.getStatus() == SagaStatus.FAILED;

        // Only fire for brief transient failures (stuck but no MISSING_TERMINAL_EVENT).
        // If hasMissingTerminal is true, R05 covers this case (even after recovery).
        if (hasStuck && !hasMissingTerminal && isTerminal) {
            RuleResult rr = RuleResult.of("TRANSIENT_SERVICE_FAILURE", "LOW",
                    Confidence.STRONG.value, "R06");
            rr.evidence.add("Saga was previously stuck (SAGA_STUCK anomaly detected)");
            rr.evidence.add("Saga has since recovered → final status: " + saga.getStatus());
            if (saga.getStartTime() != null && saga.getEndTime() != null) {
                long ms = Duration.between(saga.getStartTime(), saga.getEndTime()).toMillis();
                rr.evidence.add("Total duration (including stuck period): " + ms + "ms");
            }
            rr.suggestions.add("Service self-healed — review uptime to prevent recurrence");
            return rr;
        }
        return null;
    }

    // R07 — WEAK: STEP_TIMEOUT detection is reliable (step exceeded its
    // configured timeout), but the rule's label asserts "DOWNSTREAM" caused
    // the slowness. The same signature could be produced by local GC pause,
    // network jitter on the producer side, or a wrong timeout config on the
    // step itself. The named cause cannot be proved from the signature alone.
    private RuleResult ruleDownstreamTimeout(List<Anomaly> anomalies) {
        for (Anomaly a : anomalies) {
            if (a.getAnomalyType() == AnomalyType.STEP_TIMEOUT) {
                RuleResult rr = RuleResult.of("DOWNSTREAM_TIMEOUT", "HIGH",
                        Confidence.WEAK.value, "R07");
                rr.evidence.add(a.getDescription());
                rr.suggestions.add("Tune step timeout thresholds in workflow spec");
                rr.suggestions.add("Check downstream service performance");
                return rr;
            }
        }
        return null;
    }

    // R08 — MEDIUM
    private RuleResult ruleDownstreamSlowResponse(List<Anomaly> anomalies) {
        // Placeholder — needs StepDurationMetric baseline (Phase 4)
        return null;
    }

    // R09 — STRONG: COMPENSATION_MISSING anomaly is a deterministic
    // workflow-state check — saga reached a state where compensation should
    // have started but didn't. Label = signature.
    private RuleResult ruleCompensationNotTriggered(List<Anomaly> anomalies) {
        for (Anomaly a : anomalies) {
            if (a.getAnomalyType() == AnomalyType.COMPENSATION_MISSING) {
                RuleResult rr = RuleResult.of("COMPENSATION_NOT_TRIGGERED", "HIGH",
                        Confidence.STRONG.value, "R09");
                rr.evidence.add(a.getDescription());
                rr.suggestions.add("Check if orchestrator (order-service) is running");
                rr.suggestions.add("Verify compensation trigger logic");
                return rr;
            }
        }
        return null;
    }

    // R10 — STRONG: three corroborating facts — REFUND_PAYMENT command was
    // emitted, no PAYMENT_REFUNDED ack arrived, and saga is in COMPENSATING
    // (or MISSING_COMPENSATION_ACK fired). Label is descriptive of those
    // observable facts.
    private RuleResult ruleCompensationStepFailure(SagaInstance saga, List<SagaEvent> events,
            List<Anomaly> anomalies) {
        boolean hasRefundCommand = events.stream()
                .anyMatch(e -> "REFUND_PAYMENT".equals(e.getEventType()));
        boolean hasRefundCompleted = events.stream()
                .anyMatch(e -> "PAYMENT_REFUNDED".equals(e.getEventType()));
        boolean hasMissingCompAck = anomalies.stream()
                .anyMatch(a -> a.getAnomalyType() == AnomalyType.MISSING_COMPENSATION_ACK);

        if (hasRefundCommand && !hasRefundCompleted
                && (saga.getStatus() == SagaStatus.COMPENSATING || hasMissingCompAck)) {
            RuleResult rr = RuleResult.of("COMPENSATION_STEP_FAILURE", "HIGH",
                    Confidence.STRONG.value, "R10");
            rr.evidence.add("Compensation started (REFUND_PAYMENT) but no PAYMENT_REFUNDED received");
            rr.evidence.add("Saga stuck in COMPENSATING status");
            rr.suggestions.add("Check payment-service logs for refund errors");
            rr.suggestions.add("Verify Kafka connectivity for refund delivery");
            return rr;
        }
        return null;
    }

    // R11 — STRONG: saga reached COMPENSATED with both REFUND_PAYMENT and
    // PAYMENT_REFUNDED in the event log. Direct event-log evidence proves the
    // compensation succeeded; label is descriptive. Severity LOW because
    // compensation = recovery, but confidence in the diagnosis is high.
    private RuleResult ruleForwardStepFailure(SagaInstance saga, List<SagaEvent> events) {
        boolean hasRefundCommand = events.stream()
                .anyMatch(e -> "REFUND_PAYMENT".equals(e.getEventType()));
        boolean hasRefundCompleted = events.stream()
                .anyMatch(e -> "PAYMENT_REFUNDED".equals(e.getEventType()));

        if (saga.getStatus() == SagaStatus.COMPENSATED && hasRefundCommand && hasRefundCompleted) {
            RuleResult rr = RuleResult.of("FORWARD_STEP_FAILURE", "LOW",
                    Confidence.STRONG.value, "R11");
            rr.evidence.add("Compensation completed: REFUND_PAYMENT → PAYMENT_REFUNDED");
            rr.evidence.add("Saga status: COMPENSATED");
            if (saga.getStartTime() != null && saga.getEndTime() != null) {
                long ms = Duration.between(saga.getStartTime(), saga.getEndTime()).toMillis();
                rr.evidence.add("Total duration (including compensation): " + ms + "ms");
            }
            rr.suggestions.add("Saga rolled back correctly");
            rr.suggestions.add("Review why the forward step failed to prevent recurrence");
            return rr;
        }
        return null;
    }

    // R12 — STRONG: INVALID_TRANSITION anomaly fires from direct workflow-spec
    // violation. Label = signature.
    private RuleResult ruleInvalidTransition(List<Anomaly> anomalies) {
        for (Anomaly a : anomalies) {
            if (a.getAnomalyType() == AnomalyType.INVALID_TRANSITION) {
                RuleResult rr = RuleResult.of("INVALID_TRANSITION", "MEDIUM",
                        Confidence.STRONG.value, "R12");
                rr.evidence.add(a.getDescription());
                rr.suggestions.add("Verify event routing in upstream services");
                rr.suggestions.add("Review workflow spec transitions");
                return rr;
            }
        }
        return null;
    }

    // R13 — STRONG: anomalies fire from exact eventId match in DB —
    // deterministic, direct evidence. Label = signature.
    private RuleResult ruleDuplicateDelivery(List<Anomaly> anomalies) {
        for (Anomaly a : anomalies) {
            if (a.getAnomalyType() == AnomalyType.EXACT_DUPLICATE_EVENT
                    || a.getAnomalyType() == AnomalyType.DUPLICATE_COMPENSATION) {
                RuleResult rr = RuleResult.of("DUPLICATE_DELIVERY", "LOW",
                        Confidence.STRONG.value, "R13");
                rr.evidence.add(a.getDescription());
                rr.suggestions.add("Verify producer idempotency configuration");
                rr.suggestions.add("Check consumer deduplication mechanisms");
                return rr;
            }
        }
        return null;
    }

    // R14 — MEDIUM: OUT_OF_ORDER detection compares event timestamps to
    // workflow-expected ordering. Detection is reliable, but vulnerable to
    // clock skew across services (false-positive risk). One inferential hop:
    // observed ordering anomaly may or may not be a real delivery problem.
    private RuleResult ruleOutOfOrderDelivery(SagaInstance saga, List<Anomaly> anomalies) {
        boolean isTerminal = saga.getStatus() == SagaStatus.COMPLETED
                || saga.getStatus() == SagaStatus.COMPENSATED;
        for (Anomaly a : anomalies) {
            if (a.getAnomalyType() == AnomalyType.OUT_OF_ORDER) {
                RuleResult rr = RuleResult.of("OUT_OF_ORDER_DELIVERY",
                        isTerminal ? "LOW" : "MEDIUM",
                        Confidence.MEDIUM.value, "R14");
                rr.evidence.add(a.getDescription());
                if (isTerminal) {
                    rr.evidence.add("Saga completed despite out-of-order events");
                } else {
                    rr.evidence.add("Saga still in " + saga.getStatus() + " status");
                }
                rr.suggestions.add("Check Kafka partition key assignment");
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
    private RuleResult ruleWrongSagaCorrelationKey(List<Anomaly> anomalies) {
        for (Anomaly a : anomalies) {
            if (a.getAnomalyType() == AnomalyType.ORPHAN_EVENT) {
                RuleResult rr = RuleResult.of("WRONG_SAGA_CORRELATION_KEY", "HIGH",
                        Confidence.STRONG.value, "R15");
                rr.evidence.add(a.getDescription());
                rr.suggestions.add("Verify sagaId propagation across services");
                rr.suggestions.add("Check Kafka header saga-id configuration");
                return rr;
            }
        }
        return null;
    }

    // R16 — DEAD CODE: references AnomalyType.CONSUMER_LAG_HIGH, but no
    // detector currently emits that anomaly type. Rule cannot fire. Kept as
    // a stub for future KafkaMetricsService integration. Tier deferred until
    // detector exists; would be MEDIUM (consumer lag has multiple causes:
    // slow consumer, broker congestion, partition imbalance).
    private RuleResult ruleBrokerDelayQueueing(List<Anomaly> anomalies) {
        for (Anomaly a : anomalies) {
            if (a.getAnomalyType() == AnomalyType.CONSUMER_LAG_HIGH) {
                RuleResult rr = RuleResult.of("BROKER_DELAY_QUEUEING", "HIGH",
                        Confidence.MEDIUM.value, "R16");
                rr.evidence.add(a.getDescription());
                rr.suggestions.add("Check Kafka broker health");
                rr.suggestions.add("Monitor consumer group offsets");
                return rr;
            }
        }
        return null;
    }

    // R17 — HIGH (needs KafkaMetricsService)
    private RuleResult ruleConsumerLagBacklog(List<Anomaly> anomalies) {
        // Placeholder — enhanced in Phase 4
        return null;
    }

    // R18 — HIGH (needs DLQ)
    private RuleResult ruleSchemaMismatch(List<Anomaly> anomalies) {
        // Placeholder — enhanced in Phase 4 with DLQ listener
        return null;
    }

    // R19 — STRONG: MISSING_TRACE_CONTEXT anomaly is a direct field-level
    // check (event payload's traceId == null). Label = signature exactly.
    private RuleResult ruleMissingTracePropagation(List<Anomaly> anomalies) {
        for (Anomaly a : anomalies) {
            if (a.getAnomalyType() == AnomalyType.MISSING_TRACE_CONTEXT) {
                RuleResult rr = RuleResult.of("MISSING_TRACE_PROPAGATION", "MEDIUM",
                        Confidence.STRONG.value, "R19");
                rr.evidence.add(a.getDescription());
                rr.evidence.add("Events missing traceId cannot be correlated in Jaeger");
                rr.suggestions.add("Verify trace context propagation in service configuration");
                rr.suggestions.add("Check OpenTelemetry agent or Spring Sleuth setup");
                rr.suggestions.add("Ensure Kafka headers include traceparent/traceId");
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
    private RuleResult ruleObservabilityGap(List<Anomaly> anomalies) {
        if (!anomalies.isEmpty()) {
            RuleResult rr = RuleResult.of("OBSERVABILITY_GAP", "LOW",
                    Confidence.FALLBACK.value, "R20");
            rr.evidence.add("Anomalies detected but insufficient evidence to determine root cause");
            rr.evidence.add("Anomaly types: " + anomalies.stream()
                    .map(a -> a.getAnomalyType().name())
                    .distinct()
                    .collect(Collectors.joining(", ")));
            rr.suggestions.add("Add more telemetry sources for better RCA");
            rr.suggestions.add("Check if Jaeger/logs provide additional context");
            return rr;
        }
        return null;
    }

    // R21 — FALLBACK: by design, fires only when no other rule matched.
    // Saga is currently stuck and non-terminal; no specific cause identified.
    // Note: severity is HIGH (operational urgency — saga is stuck right now)
    // even though confidence is FALLBACK (low diagnostic certainty).
    // Severity and confidence measure different things.
    private RuleResult ruleStuckUnknownCause(SagaInstance saga, List<Anomaly> anomalies) {
        boolean hasStuck = anomalies.stream()
                .anyMatch(a -> a.getAnomalyType() == AnomalyType.SAGA_STUCK);
        boolean isNonTerminal = saga.getStatus() != SagaStatus.COMPLETED
                && saga.getStatus() != SagaStatus.COMPENSATED
                && saga.getStatus() != SagaStatus.FAILED;

        if (hasStuck && isNonTerminal) {
            RuleResult rr = RuleResult.of("STUCK_UNKNOWN_CAUSE", "HIGH",
                    Confidence.FALLBACK.value, "R21");
            rr.evidence.add("Saga stuck in " + saga.getStatus() + " status");
            rr.evidence.add("No specific root cause identified — insufficient evidence");
            rr.suggestions.add("Check all downstream service health");
            rr.suggestions.add("Verify Kafka consumer group offsets");
            return rr;
        }
        return null;
    }

    // R22 — MEDIUM: DELAYED_COMPENSATION anomaly is reliably detected, but
    // the underlying threshold (compensation × 1.5) is currently an
    // unjustified magic number. Once the threshold is defended via ablation
    // or reasoning, this rule can move to STRONG.
    //   TODO (magic-number): defend or remove the × 1.5 threshold.
    private RuleResult ruleDelayedCompensation(List<Anomaly> anomalies) {
        for (Anomaly a : anomalies) {
            if (a.getAnomalyType() == AnomalyType.DELAYED_COMPENSATION) {
                RuleResult rr = RuleResult.of("DELAYED_COMPENSATION", "MEDIUM",
                        Confidence.MEDIUM.value, "R22");
                rr.evidence.add(a.getDescription());
                rr.suggestions.add("Review orchestrator compensation trigger latency");
                rr.suggestions.add("Check message broker throughput during compensation");
                return rr;
            }
        }
        return null;
    }

    // =========================================================================
    // Utility
    // =========================================================================
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
