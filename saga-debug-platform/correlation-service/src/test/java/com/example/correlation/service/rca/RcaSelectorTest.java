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
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class RcaSelectorTest {

    private final RcaSelector selector = new RcaSelector();

    @Test
    void singleDirectSignature_isSelected() {
        CandidateAssessment direct = candidate("R03", "MULTI_TERMINAL_CONFLICT",
                Directness.DIRECT_SIGNATURE, 1L);

        SelectionResult result = selector.select(List.of(direct), List.of(anomaly(1L, AnomalyType.MULTI_TERMINAL_CONFLICT)), saga(SagaStatus.FAILED));

        assertThat(result.getRcaStatus()).isEqualTo(RcaStatus.SELECTED);
        assertThat(result.getPrimaryRootCauses()).containsExactly("MULTI_TERMINAL_CONFLICT");
        assertThat(find(result, "R03").getStatus()).isEqualTo(CandidateStatus.SELECTED);
    }

    @Test
    void singleThresholdViolation_isSelected() {
        CandidateAssessment threshold = candidate("R07", "DOWNSTREAM_TIMEOUT",
                Directness.THRESHOLD_VIOLATION, 7L);

        SelectionResult result = selector.select(List.of(threshold), List.of(anomaly(7L, AnomalyType.STEP_TIMEOUT)), saga(SagaStatus.IN_PROGRESS));

        assertThat(result.getRcaStatus()).isEqualTo(RcaStatus.SELECTED);
        assertThat(result.getPrimaryRootCauses()).containsExactly("DOWNSTREAM_TIMEOUT");
        assertThat(find(result, "R07").getStatus()).isEqualTo(CandidateStatus.SELECTED);
    }

    @Test
    void sameTierDisjointEvidence_allSelectedAsMultiCause() {
        CandidateAssessment first = candidate("R03", "MULTI_TERMINAL_CONFLICT",
                Directness.DIRECT_SIGNATURE, 1L);
        CandidateAssessment second = candidate("R12", "INVALID_TRANSITION",
                Directness.DIRECT_SIGNATURE, 2L);

        SelectionResult result = selector.select(List.of(first, second),
                List.of(anomaly(1L, AnomalyType.MULTI_TERMINAL_CONFLICT),
                        anomaly(2L, AnomalyType.INVALID_TRANSITION)),
                saga(SagaStatus.FAILED));

        assertThat(result.getRcaStatus()).isEqualTo(RcaStatus.SELECTED);
        assertThat(result.getPrimaryRootCauses())
                .containsExactlyInAnyOrder("MULTI_TERMINAL_CONFLICT", "INVALID_TRANSITION");
        assertThat(find(result, "R03").getStatus()).isEqualTo(CandidateStatus.SELECTED);
        assertThat(find(result, "R12").getStatus()).isEqualTo(CandidateStatus.SELECTED);
    }

    @Test
    void crossTierIndependentEvidence_lowerTierIsContributingNotSelected() {
        CandidateAssessment direct = candidate("R03", "MULTI_TERMINAL_CONFLICT",
                Directness.DIRECT_SIGNATURE, 1L);
        CandidateAssessment threshold = candidate("R07", "DOWNSTREAM_TIMEOUT",
                Directness.THRESHOLD_VIOLATION, 2L);

        SelectionResult result = selector.select(List.of(direct, threshold),
                List.of(anomaly(1L, AnomalyType.MULTI_TERMINAL_CONFLICT),
                        anomaly(2L, AnomalyType.STEP_TIMEOUT)),
                saga(SagaStatus.FAILED));

        assertThat(result.getPrimaryRootCauses()).containsExactly("MULTI_TERMINAL_CONFLICT");
        assertThat(find(result, "R03").getStatus()).isEqualTo(CandidateStatus.SELECTED);
        assertThat(find(result, "R07").getStatus()).isEqualTo(CandidateStatus.CONTRIBUTING);
    }

    @Test
    void heuristicOnly_abstainsWithPossibleCandidate() {
        CandidateAssessment heuristic = candidateBuilder("R02", "TIMEOUT_RETRY_STORM",
                Directness.HEURISTIC_INFERENCE, 2L)
                .statusReason("Retry events do not record exception class")
                .build();

        SelectionResult result = selector.select(List.of(heuristic),
                List.of(anomaly(2L, AnomalyType.RETRY_BURST)),
                saga(SagaStatus.IN_PROGRESS));

        assertThat(result.getRcaStatus()).isEqualTo(RcaStatus.INSUFFICIENT_EVIDENCE);
        assertThat(result.getPrimaryRootCauses()).isEmpty();
        assertThat(find(result, "R02").getStatus()).isEqualTo(CandidateStatus.POSSIBLE_BUT_UNSUPPORTED);
        assertThat(result.getMissingEvidence()).contains("Retry events do not record exception class");
    }

    @Test
    void exclusionRule_rulesOutServiceCrashWhenStepTimeoutPresent() {
        CandidateAssessment r05 = candidate("R05", "SERVICE_CRASH",
                Directness.CORROBORATED_INFERENCE, 5L);
        CandidateAssessment r07 = candidate("R07", "DOWNSTREAM_TIMEOUT",
                Directness.THRESHOLD_VIOLATION, 7L);

        SelectionResult result = selector.select(List.of(r05, r07),
                List.of(anomaly(7L, AnomalyType.STEP_TIMEOUT)),
                saga(SagaStatus.IN_PROGRESS));

        assertThat(find(result, "R05").getStatus()).isEqualTo(CandidateStatus.RULED_OUT);
        assertThat(find(result, "R05").getStatusReason()).contains("STEP_TIMEOUT");
        assertThat(find(result, "R07").getStatus()).isEqualTo(CandidateStatus.SELECTED);
    }

    @Test
    void sameTierSameEvidence_allSelected() {
        CandidateAssessment first = candidate("R03", "MULTI_TERMINAL_CONFLICT",
                Directness.DIRECT_SIGNATURE, 1L);
        CandidateAssessment second = candidate("R03B", "TERMINAL_CONFLICT_ALT",
                Directness.DIRECT_SIGNATURE, 1L);

        SelectionResult result = selector.select(List.of(first, second),
                List.of(anomaly(1L, AnomalyType.MULTI_TERMINAL_CONFLICT)),
                saga(SagaStatus.FAILED));

        assertThat(result.getPrimaryRootCauses())
                .containsExactlyInAnyOrder("MULTI_TERMINAL_CONFLICT", "TERMINAL_CONFLICT_ALT");
        assertThat(find(result, "R03").getStatus()).isEqualTo(CandidateStatus.SELECTED);
        assertThat(find(result, "R03B").getStatus()).isEqualTo(CandidateStatus.SELECTED);
    }

    @Test
    void completedWithoutAnomalies_needsNoDiagnosis() {
        SelectionResult result = selector.select(List.of(), List.of(), saga(SagaStatus.COMPLETED));

        assertThat(result.getRcaStatus()).isEqualTo(RcaStatus.NO_DIAGNOSIS_NEEDED);
        assertThat(result.getPrimaryRootCauses()).isEmpty();
        assertThat(result.getCandidates()).isEmpty();
    }

    @Test
    void failedBusinessRejectionWithoutAnomalies_needsNoDiagnosis() {
        SelectionResult result = selector.select(List.of(), List.of(), saga(SagaStatus.FAILED));

        assertThat(result.getRcaStatus()).isEqualTo(RcaStatus.NO_DIAGNOSIS_NEEDED);
        assertThat(result.getPrimaryRootCauses()).isEmpty();
    }

    private CandidateAssessment candidate(String ruleId, String rootCause,
            Directness directness, Long anomalyId) {
        return candidateBuilder(ruleId, rootCause, directness, anomalyId).build();
    }

    private CandidateAssessment.Builder candidateBuilder(String ruleId, String rootCause,
            Directness directness, Long anomalyId) {
        return CandidateAssessment.builder()
                .ruleId(ruleId)
                .rootCause(rootCause)
                .severity("HIGH")
                .directness(directness)
                .evidence(Evidence.of("fact-" + anomalyId, anomalyId));
    }

    private Anomaly anomaly(Long id, AnomalyType type) {
        Anomaly anomaly = new Anomaly();
        anomaly.setId(id);
        anomaly.setAnomalyType(type);
        anomaly.setSagaId("saga-1");
        return anomaly;
    }

    private SagaInstance saga(SagaStatus status) {
        SagaInstance saga = new SagaInstance();
        saga.setSagaId("saga-1");
        saga.setStatus(status);
        return saga;
    }

    private RcaCandidate find(SelectionResult result, String ruleId) {
        return result.getCandidates().stream()
                .filter(c -> ruleId.equals(c.getRuleId()))
                .findFirst()
                .orElseThrow();
    }
}
