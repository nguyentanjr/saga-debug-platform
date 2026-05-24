package com.example.correlation.service.rca;

import com.example.correlation.domain.Anomaly;
import com.example.correlation.domain.SagaInstance;
import com.example.correlation.domain.SagaStatus;
import com.example.correlation.domain.rca.CandidateAssessment;
import com.example.correlation.domain.rca.CandidateStatus;
import com.example.correlation.domain.rca.Directness;
import com.example.correlation.domain.rca.RcaCandidate;
import com.example.correlation.domain.rca.RcaStatus;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class RcaSelector {

    private static final List<ExclusionRule> EXCLUSIONS = List.of(
            new ExclusionRule("R20",
                    (candidate, context) -> context.hasAnyNonFallbackCandidate(),
                    "Specific evidence candidates are already available; OBSERVABILITY_GAP is fallback-only."),
            new ExclusionRule("R21",
                    (candidate, context) -> context.hasAnyNonFallbackCandidate(),
                    "Specific evidence candidates are already available; STUCK_UNKNOWN_CAUSE is fallback-only.")
    );

    public SelectionResult select(List<CandidateAssessment> input,
            List<Anomaly> anomalies,
            SagaInstance saga) {
        List<CandidateAssessment> candidates = input != null ? new ArrayList<>(input) : new ArrayList<>();
        List<Anomaly> anomalyList = anomalies != null ? anomalies : List.of();
        SelectionContext context = new SelectionContext(candidates, anomalyList);

        applyExclusions(candidates, context);

        List<CandidateAssessment> evidenceCandidates = candidates.stream()
                .filter(c -> c.getStatus() != CandidateStatus.RULED_OUT)
                .collect(Collectors.toCollection(ArrayList::new));
        for (CandidateAssessment candidate : evidenceCandidates) {
            markEvidenceGroup(candidate);
        }

        SelectionResult result = new SelectionResult();
        result.setRcaStatus(resolveStatus(evidenceCandidates, anomalyList, saga));
        result.setPrimaryRootCauses(List.of());
        result.setCandidates(candidates.stream().map(RcaCandidate::from).collect(Collectors.toList()));
        result.setMissingEvidence(buildMissingEvidence(candidates, result.getRcaStatus()));
        return result;
    }

    private void applyExclusions(List<CandidateAssessment> candidates, SelectionContext context) {
        for (CandidateAssessment candidate : candidates) {
            for (ExclusionRule exclusion : EXCLUSIONS) {
                if (exclusion.appliesTo(candidate, context)) {
                    candidate.setStatus(CandidateStatus.RULED_OUT);
                    candidate.setStatusReason(exclusion.getReason());
                    break;
                }
            }
        }
    }

    private void markEvidenceGroup(CandidateAssessment candidate) {
        if (candidate.getStatus() != null) {
            return;
        }
        Directness directness = candidate.getDirectness();
        if (directness == Directness.DIRECT_SIGNATURE || directness == Directness.THRESHOLD_VIOLATION) {
            candidate.setStatus(CandidateStatus.STRONG_EVIDENCE);
            candidate.setStatusReason("Strong evidence from " + directness + ".");
        } else if (directness == Directness.CORROBORATED_INFERENCE) {
            candidate.setStatus(CandidateStatus.SUPPORTING_EVIDENCE);
            candidate.setStatusReason("Supporting evidence from corroborated signals.");
        } else {
            candidate.setStatus(CandidateStatus.WEAK_EVIDENCE);
            candidate.setStatusReason(defaultMissingEvidence(candidate));
        }
    }

    private String defaultMissingEvidence(CandidateAssessment candidate) {
        if (candidate.getStatusReason() != null && !candidate.getStatusReason().isBlank()) {
            return candidate.getStatusReason();
        }
        return "Weak evidence only; treat " + candidate.getRootCause() + " as a fallback hypothesis.";
    }

    private RcaStatus resolveStatus(List<CandidateAssessment> evidenceCandidates,
            List<Anomaly> anomalies,
            SagaInstance saga) {
        boolean hasActionableEvidence = evidenceCandidates.stream()
                .anyMatch(candidate -> candidate.getStatus() == CandidateStatus.STRONG_EVIDENCE
                        || candidate.getStatus() == CandidateStatus.SUPPORTING_EVIDENCE
                        || candidate.getStatus() == CandidateStatus.SELECTED
                        || candidate.getStatus() == CandidateStatus.CONTRIBUTING);
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
                .filter(c -> c.getStatus() == CandidateStatus.WEAK_EVIDENCE
                        || c.getStatus() == CandidateStatus.POSSIBLE_BUT_UNSUPPORTED)
                .map(CandidateAssessment::getStatusReason)
                .filter(reason -> reason != null && !reason.isBlank())
                .distinct()
                .collect(Collectors.toCollection(ArrayList::new));
        if (missing.isEmpty()) {
            missing.add("No RCA candidate could be built from the available evidence.");
        }
        return missing;
    }
}
