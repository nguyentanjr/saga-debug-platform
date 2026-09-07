package com.example.correlation.service.rca;

import com.example.correlation.domain.Anomaly;
import com.example.correlation.domain.SagaInstance;
import com.example.correlation.domain.SagaStatus;
import com.example.correlation.domain.rca.CandidateAssessment;
import com.example.correlation.domain.rca.CandidateStatus;
import com.example.correlation.domain.rca.ClaimSubject;
import com.example.correlation.domain.rca.RcaCandidate;
import com.example.correlation.domain.rca.RcaStatus;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class RcaSelector {

    private static final Set<String> FALLBACK_RULES = Set.of("R20", "R21");

    public SelectionResult select(List<CandidateAssessment> input,
            List<Anomaly> anomalies,
            SagaInstance saga) {
        return select(input, anomalies, saga, ExecutionOrder.unknown());
    }


    /**
     * @param order where each candidate's anchor sits in the execution, used to order
     *              candidates by when the execution first diverged instead of by rule id
     */
    public SelectionResult select(List<CandidateAssessment> input,
            List<Anomaly> anomalies,
            SagaInstance saga,
            ExecutionOrder order) {
        List<CandidateAssessment> candidates = input != null ? new ArrayList<>(input) : new ArrayList<>();
        List<Anomaly> anomalyList = anomalies != null ? anomalies : List.of();

        applyExclusions(candidates);
        recordPrecedence(candidates, order);
        candidates.sort(comparator(order));

        List<CandidateAssessment> evidenceCandidates = candidates.stream()
                .filter(c -> c.getStatus() != CandidateStatus.RULED_OUT)
                .collect(Collectors.toList());

        List<RcaCandidate> ranked = candidates.stream()
                .map(RcaCandidate::from)
                .collect(Collectors.toList());

        SelectionResult result = new SelectionResult();
        result.setRcaStatus(resolveStatus(evidenceCandidates, anomalyList, saga));
        result.setCandidates(ranked);
        result.setPrimary(promote(ranked));
        result.setMissingEvidence(buildMissingEvidence(candidates, result.getRcaStatus()));
        return result;
    }

    /**
     * The one candidate this selection is willing to publish as the root cause.
     *
     * <p>The ranking above already answers "which of these should a reader look at
     * first"; this only reads that answer off the front of the list. Nothing is
     * re-scored, no threshold is relaxed, and no candidate is promoted that the rules did
     * not already assess as {@link CandidateStatus#STRONG_EVIDENCE}.
     *
     * <p>Restricted to that one tier on purpose. SUPPORTING_EVIDENCE corroborates a
     * finding rather than establishing one, WEAK_EVIDENCE is explicitly a hypothesis, and
     * RULED_OUT has been withdrawn — publishing any of them as <em>the</em> root cause
     * would state more than the evidence does. Nothing here changes which candidates
     * exist or what status they carry; a saga with no strong candidate produces no
     * promotion, exactly as before.
     *
     * <p>The fallback rules R20/R21 cannot arrive here: they are assessed WEAK_EVIDENCE
     * by the rules that raise them, and {@link #applyExclusions} withdraws them outright
     * once anything specific is available.
     *
     * <h2>Why evidence tier is not the only question</h2>
     * A root cause answers "what did the observed system do wrong", so a candidate can
     * only be promoted if that is what it is a claim about. A finding whose subject is
     * this platform's own reception — which records it read first, which partitions it
     * was behind on — is strong evidence of something real and still not an answer to
     * that question. {@code OUT_OF_ORDER_DELIVERY (R14)} is the case this was written
     * for: on backlog ingestion it anchors at the first step of the run, so the ranking's
     * execution-position key put it ahead of an unacknowledged compensation and a
     * downstream timeout, and it was published as the cause of runs whose observed
     * systems had done nothing wrong.
     *
     * <p>Nothing is suppressed by this. Such a candidate is ranked exactly where it was,
     * keeps its status, severity, anchor and evidence, and is still returned in
     * {@code candidates}; it is passed over here and here only. See {@link ClaimSubject}.
     *
     * @return the first strongly-evidenced candidate in ranked order whose claim is about
     *         the observed system, or null when the selection has none — which is a
     *         refusal to name a cause, not a failure
     */
    private RcaCandidate promote(List<RcaCandidate> ranked) {
        return ranked.stream()
                .filter(candidate -> candidate.getStatus() == CandidateStatus.STRONG_EVIDENCE)
                .filter(candidate -> candidate.getSubject() == ClaimSubject.OBSERVED_SYSTEM)
                .findFirst()
                .orElse(null);
    }


    /**
     * Where a candidate sits in the execution, and which steps precede which.
     *
     * <p>Supplied by the caller because only it can build the execution projection; the
     * selector stays free of persistence and of workflow lookups.
     */
    public interface ExecutionOrder {
        /** Position of a step in this execution, or empty when it was never observed. */
        OptionalInt positionOf(String stepId);

        /** Whether {@code candidate} is on the dependency path leading into {@code step}. */
        boolean isUpstream(String candidate, String step);

        static ExecutionOrder unknown() {
            return new ExecutionOrder() {
                @Override public OptionalInt positionOf(String stepId) { return OptionalInt.empty(); }
                @Override public boolean isUpstream(String candidate, String step) { return false; }
            };
        }
    }


    /**
     * Records, for each candidate, which other candidates are anchored upstream of it.
     *
     * <p>Deliberately not phrased as causation. Being upstream and earlier is a reason
     * for a reader to look at that candidate first; it does not establish that it
     * produced this one, and the platform has no evidence that would.
     */
    private void recordPrecedence(List<CandidateAssessment> candidates, ExecutionOrder order) {
        for (CandidateAssessment candidate : candidates) {
            String step = candidate.getAnchorStepId();
            if (step == null) continue;

            List<String> upstream = candidates.stream()
                    .filter(other -> other != candidate)
                    .filter(other -> other.getStatus() != CandidateStatus.RULED_OUT)
                    .filter(other -> other.getAnchorStepId() != null)
                    .filter(other -> order.isUpstream(other.getAnchorStepId(), step))
                    .map(CandidateAssessment::getRuleId)
                    .filter(Objects::nonNull)
                    .distinct()
                    .sorted()
                    .collect(Collectors.toList());

            if (upstream.isEmpty()) continue;
            candidate.setPrecededBy(upstream);

            String note = "Anchored at " + step + ", downstream of " + upstream
                    + "; ordering only, no causal link established.";
            candidate.setStatusReason(candidate.getStatusReason() == null
                    ? note
                    : candidate.getStatusReason() + " " + note);
        }
    }


    /**
     * Evidence tier first, then position in the execution, then severity.
     *
     * <p>The previous ordering fell through from severity to a lexicographic comparison
     * of rule ids, so whichever candidate happened to be named "R07" outranked "R10".
     * Execution position is at least a property of the failure being diagnosed. Rule id
     * remains last purely to keep the order deterministic.
     */
    private Comparator<CandidateAssessment> comparator(ExecutionOrder order) {
        return Comparator
                .comparingInt((CandidateAssessment c) -> statusRank(c.getStatus()))
                .thenComparingInt(c -> anchorPosition(c, order))
                .thenComparing(c -> c.getAnchorAt(),
                        Comparator.nullsLast(Comparator.naturalOrder()))
                .thenComparingInt(c -> -severityRank(c.getSeverity()))
                .thenComparing(CandidateAssessment::getRuleId,
                        Comparator.nullsLast(Comparator.naturalOrder()));
    }

    private int anchorPosition(CandidateAssessment candidate, ExecutionOrder order) {
        if (candidate.getAnchorStepId() == null) return Integer.MAX_VALUE;
        return order.positionOf(candidate.getAnchorStepId()).orElse(Integer.MAX_VALUE);
    }

    private static int statusRank(CandidateStatus status) {
        if (status == CandidateStatus.STRONG_EVIDENCE) return 0;
        if (status == CandidateStatus.SUPPORTING_EVIDENCE) return 1;
        if (status == CandidateStatus.WEAK_EVIDENCE) return 2;
        if (status == CandidateStatus.RULED_OUT) return 3;
        return 4;
    }

    private static int severityRank(String severity) {
        if ("CRITICAL".equals(severity)) return 4;
        if ("HIGH".equals(severity)) return 3;
        if ("MEDIUM".equals(severity)) return 2;
        if ("LOW".equals(severity)) return 1;
        return 0;
    }


    private void applyExclusions(List<CandidateAssessment> candidates) {
        boolean hasSpecificEvidence = candidates.stream()
                .anyMatch(c -> !FALLBACK_RULES.contains(c.getRuleId()));
        if (!hasSpecificEvidence) {
            return;
        }
        for (CandidateAssessment candidate : candidates) {
            if (FALLBACK_RULES.contains(candidate.getRuleId())) {
                candidate.setStatus(CandidateStatus.RULED_OUT);
                candidate.setStatusReason("Specific evidence candidates are already available; "
                        + candidate.getRootCause() + " is fallback-only.");
            }
        }
    }

    private RcaStatus resolveStatus(List<CandidateAssessment> evidenceCandidates,
            List<Anomaly> anomalies,
            SagaInstance saga) {
        boolean hasActionableEvidence = evidenceCandidates.stream()
                .anyMatch(candidate -> candidate.getStatus() == CandidateStatus.STRONG_EVIDENCE
                        || candidate.getStatus() == CandidateStatus.SUPPORTING_EVIDENCE);
        if (hasActionableEvidence) {
            return RcaStatus.EVIDENCE_AVAILABLE;
        }
        boolean hasAnomalies = anomalies != null && !anomalies.isEmpty();
        if (!hasAnomalies && saga != null
                && (saga.getStatus() == SagaStatus.COMPLETED || saga.getStatus() == SagaStatus.FAILED)) {
            return RcaStatus.NO_DIAGNOSIS_NEEDED;
        }
        return RcaStatus.INSUFFICIENT_EVIDENCE;
    }

    private List<String> buildMissingEvidence(List<CandidateAssessment> candidates, RcaStatus status) {
        if (status != RcaStatus.INSUFFICIENT_EVIDENCE) {
            return List.of();
        }
        List<String> missing = candidates.stream()
                .filter(c -> c.getStatus() == CandidateStatus.WEAK_EVIDENCE)
                .map(CandidateAssessment::getStatusReason)
                .filter(reason -> reason != null && !reason.isBlank())
                .distinct()
                .collect(Collectors.toCollection(ArrayList::new));
        if (missing.isEmpty()) {
            missing.add("No RCA candidate could be built from the available evidence.");
        }
        return missing;
    }
    
    public static class SelectionResult {
        private RcaStatus rcaStatus;
        private List<RcaCandidate> candidates = new ArrayList<>();
        private List<String> missingEvidence = new ArrayList<>();

        /**
         * The candidate this selection promotes to the reported root cause, or null.
         *
         * <p>One of the objects in {@link #candidates}, not a copy, so the promoted cause
         * and the candidate list can never disagree about a rule's status or evidence.
         */
        private RcaCandidate primary;

        public RcaStatus getRcaStatus() { return rcaStatus; }
        public void setRcaStatus(RcaStatus rcaStatus) { this.rcaStatus = rcaStatus; }

        public RcaCandidate getPrimary() { return primary; }
        public void setPrimary(RcaCandidate primary) { this.primary = primary; }

        public List<RcaCandidate> getCandidates() { return candidates; }
        public void setCandidates(List<RcaCandidate> candidates) {
            this.candidates = candidates != null ? new ArrayList<>(candidates) : new ArrayList<>();
        }

        public List<String> getMissingEvidence() { return missingEvidence; }
        public void setMissingEvidence(List<String> missingEvidence) {
            this.missingEvidence = missingEvidence != null ? new ArrayList<>(missingEvidence) : new ArrayList<>();
        }
    }
}
