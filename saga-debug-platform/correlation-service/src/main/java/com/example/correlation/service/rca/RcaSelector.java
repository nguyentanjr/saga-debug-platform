package com.example.correlation.service.rca;

import com.example.correlation.domain.Anomaly;
import com.example.correlation.domain.AnomalyType;
import com.example.correlation.domain.SagaInstance;
import com.example.correlation.domain.SagaStatus;
import com.example.correlation.domain.rca.CandidateAssessment;
import com.example.correlation.domain.rca.CandidateStatus;
import com.example.correlation.domain.rca.Directness;
import com.example.correlation.domain.rca.Evidence;
import com.example.correlation.domain.rca.RcaCandidate;
import com.example.correlation.domain.rca.RcaStatus;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class RcaSelector {

    private static final List<ExclusionRule> EXCLUSIONS = List.of(
            new ExclusionRule("R05",
                    (candidate, context) -> context.hasAnomaly(AnomalyType.STEP_TIMEOUT),
                    "STEP_TIMEOUT anomaly is present; R07 covers this case with threshold-violation evidence."),
            new ExclusionRule("R06",
                    (candidate, context) -> context.hasAnomaly(AnomalyType.MISSING_TERMINAL_EVENT),
                    "MISSING_TERMINAL_EVENT anomaly is present; R05 handles this case."),
            new ExclusionRule("R20",
                    (candidate, context) -> context.hasAnyNonFallbackCandidate(),
                    "Other rules with stronger directness fired; OBSERVABILITY_GAP is fallback-only."),
            new ExclusionRule("R21",
                    (candidate, context) -> context.hasAnyNonFallbackCandidate(),
                    "Other rules with stronger directness fired; STUCK_UNKNOWN_CAUSE is fallback-only.")
    );

    public SelectionResult select(List<CandidateAssessment> input,
            List<Anomaly> anomalies,
            SagaInstance saga) {
        List<CandidateAssessment> candidates = input != null ? new ArrayList<>(input) : new ArrayList<>();
        List<Anomaly> anomalyList = anomalies != null ? anomalies : List.of();
        SelectionContext context = new SelectionContext(candidates, anomalyList);

        applyExclusions(candidates, context);

        List<CandidateAssessment> remaining = candidates.stream()
                .filter(c -> c.getStatus() != CandidateStatus.RULED_OUT)
                .collect(Collectors.toCollection(ArrayList::new));

        List<CandidateAssessment> selected = new ArrayList<>();
        if (!remaining.isEmpty()) {
            Directness topTier = remaining.stream()
                    .map(CandidateAssessment::getDirectness)
                    .min(Comparator.comparingInt(Directness::ordinal))
                    .orElse(Directness.HEURISTIC_INFERENCE);

            if (topTier == Directness.DIRECT_SIGNATURE
                    || topTier == Directness.THRESHOLD_VIOLATION
                    || topTier == Directness.CORROBORATED_INFERENCE) {
                for (CandidateAssessment candidate : remaining) {
                    if (candidate.getDirectness() == topTier) {
                        markSelected(candidate, "Selected from top evidence tier " + topTier + ".");
                        selected.add(candidate);
                    }
                }
                markLowerTierCandidates(remaining, selected, topTier);
            } else {
                remaining.forEach(c -> markPossible(c, defaultMissingEvidence(c)));
            }
        }

        List<CandidateAssessment> orderedSelected = selected.stream()
                .sorted(this::compareCandidateStrength)
                .collect(Collectors.toList());

        SelectionResult result = new SelectionResult();
        result.setRcaStatus(resolveStatus(selected, anomalyList, saga));
        result.setPrimaryRootCauses(orderedSelected.stream()
                .map(CandidateAssessment::getRootCause)
                .distinct()
                .collect(Collectors.toList()));
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

    private void markLowerTierCandidates(List<CandidateAssessment> remaining,
            List<CandidateAssessment> selected,
            Directness topTier) {
        for (CandidateAssessment candidate : remaining) {
            if (candidate.getDirectness() == topTier || candidate.getStatus() != null) {
                continue;
            }
            if (hasDisjointEvidence(candidate, selected)) {
                candidate.setStatus(CandidateStatus.CONTRIBUTING);
                candidate.setStatusReason("Independent evidence below selected tier.");
            } else {
                markPossible(candidate, defaultMissingEvidence(candidate));
            }
        }
    }

    private boolean hasDisjointEvidence(CandidateAssessment candidate, List<CandidateAssessment> selected) {
        Set<String> candidateFingerprint = fingerprint(candidate);
        if (candidateFingerprint.isEmpty()) {
            return false;
        }
        for (CandidateAssessment selectedCandidate : selected) {
            Set<String> selectedFingerprint = fingerprint(selectedCandidate);
            if (!selectedFingerprint.isEmpty()
                    && selectedFingerprint.stream().anyMatch(candidateFingerprint::contains)) {
                return false;
            }
        }
        return true;
    }

    private Set<String> fingerprint(CandidateAssessment candidate) {
        Set<String> keys = new LinkedHashSet<>();
        for (Evidence evidence : candidate.getEvidence()) {
            if (evidence.anomalyId() != null) {
                keys.add("a:" + evidence.anomalyId());
            }
            for (String eventId : evidence.eventIds()) {
                keys.add("e:" + eventId);
            }
        }
        if (keys.isEmpty()) {
            candidate.getEvidence().stream()
                    .map(Evidence::fact)
                    .filter(fact -> fact != null && !fact.isBlank())
                    .forEach(fact -> keys.add("f:" + fact));
        }
        return keys;
    }

    private int compareCandidateStrength(CandidateAssessment first, CandidateAssessment second) {
        int directness = Integer.compare(enumRank(first.getDirectness()), enumRank(second.getDirectness()));
        if (directness != 0) return directness;
        return Comparator.nullsLast(String::compareTo).compare(first.getRuleId(), second.getRuleId());
    }

    private int enumRank(Enum<?> value) {
        return value != null ? value.ordinal() : Integer.MAX_VALUE;
    }

    private void markSelected(CandidateAssessment candidate, String reason) {
        candidate.setStatus(CandidateStatus.SELECTED);
        candidate.setStatusReason(reason);
    }

    private void markPossible(CandidateAssessment candidate, String reason) {
        candidate.setStatus(CandidateStatus.POSSIBLE_BUT_UNSUPPORTED);
        candidate.setStatusReason(reason);
    }

    private String defaultMissingEvidence(CandidateAssessment candidate) {
        if (candidate.getStatusReason() != null && !candidate.getStatusReason().isBlank()) {
            return candidate.getStatusReason();
        }
        return "Evidence is not specific enough to promote " + candidate.getRootCause() + ".";
    }

    private RcaStatus resolveStatus(List<CandidateAssessment> selected,
            List<Anomaly> anomalies,
            SagaInstance saga) {
        if (!selected.isEmpty()) {
            return RcaStatus.SELECTED;
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
                .filter(c -> c.getStatus() == CandidateStatus.POSSIBLE_BUT_UNSUPPORTED)
                .map(CandidateAssessment::getStatusReason)
                .filter(reason -> reason != null && !reason.isBlank())
                .distinct()
                .collect(Collectors.toCollection(ArrayList::new));
        if (missing.isEmpty()) {
            missing.add("No candidate had enough specific evidence to select a root cause.");
        }
        return missing;
    }
}
