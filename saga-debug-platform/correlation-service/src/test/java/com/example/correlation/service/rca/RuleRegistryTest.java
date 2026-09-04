package com.example.correlation.service.rca;

import com.example.correlation.domain.Anomaly;
import com.example.correlation.domain.AnomalyType;
import com.example.correlation.domain.rca.CandidateStatus;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers the declarative half of the rule engine: what {@code rules/*.yaml} actually
 * loads, and what a loaded rule does when it meets a set of anomalies.
 *
 * <p>The registry is loaded here exactly as the application loads it — from the
 * classpath, not from a fixture — so these assertions fail if the shipped YAML stops
 * parsing, loses a rule, or changes a severity. That is the point: the rules are
 * configuration now, and configuration that nothing checks is configuration that drifts.
 */
class RuleRegistryTest {

    private static RuleRegistry loaded() {
        RuleRegistry registry = new RuleRegistry();
        registry.load();
        return registry;
    }

    // ── what ships ────────────────────────────────────────────────────────────

    @Test
    void loadsTheNineDeclaredRules() {
        assertThat(loaded().ruleIds())
                .containsExactlyInAnyOrder("R03", "R04", "R07", "R09", "R12", "R13", "R15", "R19", "R22");
    }

    @Test
    void everyLoadedRuleIsWellFormed() {
        assertThat(loaded().rules())
                .allSatisfy(rule -> assertThat(rule.validationError()).isEmpty());
    }

    @Test
    void declaredRulesKeepTheirSeverityAndTier() {
        assertThat(rule("R03").getSeverity()).isEqualTo("CRITICAL");
        assertThat(rule("R04").getSeverity()).isEqualTo("HIGH");
        assertThat(rule("R12").getSeverity()).isEqualTo("MEDIUM");
        assertThat(rule("R13").getSeverity()).isEqualTo("LOW");

        // None of the nine is a fallback, so all of them assert strong evidence.
        assertThat(loaded().rules())
                .allSatisfy(r -> assertThat(r.getEvidence()).isEqualTo(CandidateStatus.STRONG_EVIDENCE));
    }

    @Test
    void everyRuleCarriesSomethingActionable() {
        assertThat(loaded().rules()).allSatisfy(r -> assertThat(r.getSuggestions()).isNotEmpty());
    }

    // ── matching ──────────────────────────────────────────────────────────────

    @Test
    void firesOnItsDeclaredAnomalyType() {
        Optional<Anomaly> match = rule("R07").firstMatch(List.of(anomaly(1L, AnomalyType.STEP_TIMEOUT)));

        assertThat(match).isPresent();
        assertThat(match.get().getAnomalyType()).isEqualTo(AnomalyType.STEP_TIMEOUT);
    }

    @Test
    void staysSilentOnAnUnrelatedAnomaly() {
        assertThat(rule("R07").firstMatch(List.of(anomaly(1L, AnomalyType.ORPHAN_EVENT)))).isEmpty();
    }

    @Test
    void staysSilentWhenThereAreNoAnomalies() {
        assertThat(rule("R07").firstMatch(List.of())).isEmpty();
        assertThat(rule("R07").firstMatch(null)).isEmpty();
    }

    /**
     * R04 and R13 each name two anomaly types as alternative signatures of one cause.
     * Either alone has to be enough, or the second signature is decoration.
     */
    @Test
    void eitherAlternativeSignatureFiresTheRule() {
        assertThat(rule("R04").firstMatch(List.of(anomaly(1L, AnomalyType.FORBIDDEN_POST_TERMINAL)))).isPresent();
        assertThat(rule("R04").firstMatch(List.of(anomaly(2L, AnomalyType.STATE_REGRESSION)))).isPresent();

        assertThat(rule("R13").firstMatch(List.of(anomaly(3L, AnomalyType.EXACT_DUPLICATE_EVENT)))).isPresent();
        assertThat(rule("R13").firstMatch(List.of(anomaly(4L, AnomalyType.DUPLICATE_COMPENSATION)))).isPresent();
    }

    /**
     * The matched anomaly is what positions the candidate in the execution, so a rule
     * must quote the earliest one it matches. Returning a later anomaly would place the
     * diagnosis after the point its story actually starts, and ranking is ordered by
     * exactly that position.
     */
    @Test
    void quotesTheEarliestMatchingAnomalyNotJustAnyOfThem() {
        List<Anomaly> anomalies = List.of(
                anomaly(1L, AnomalyType.ORPHAN_EVENT),
                anomaly(2L, AnomalyType.EXACT_DUPLICATE_EVENT),
                anomaly(3L, AnomalyType.DUPLICATE_COMPENSATION));

        assertThat(rule("R13").firstMatch(anomalies)).get()
                .extracting(Anomaly::getId).isEqualTo(2L);
    }

    @Test
    void toleratesANullEntryAmongTheAnomalies() {
        List<Anomaly> withNull = java.util.Arrays.asList(null, anomaly(9L, AnomalyType.STEP_TIMEOUT));

        assertThat(rule("R07").firstMatch(withNull)).get()
                .extracting(Anomaly::getId).isEqualTo(9L);
    }

    // ── rejection of malformed declarations ───────────────────────────────────

    @Test
    void aRuleWithNoTriggerIsRejectedRatherThanLoadedAndSilent() {
        AnomalySignatureRule empty = new AnomalySignatureRule();
        empty.setId("RXX");
        empty.setRootCause("SOMETHING");
        empty.setSeverity("LOW");

        assertThat(empty.validationError()).hasValueSatisfying(
                reason -> assertThat(reason).contains("can never fire"));
    }

    @Test
    void aRuleWithNoRootCauseIsRejected() {
        AnomalySignatureRule noCause = new AnomalySignatureRule();
        noCause.setId("RXX");
        noCause.setSeverity("LOW");
        noCause.setMatchesAnyAnomaly(List.of(AnomalyType.SAGA_STUCK));

        assertThat(noCause.validationError()).hasValue("no rootCause");
    }

    @Test
    void aRuleWithNoIdIsRejected() {
        AnomalySignatureRule noId = new AnomalySignatureRule();
        noId.setRootCause("SOMETHING");
        noId.setSeverity("LOW");
        noId.setMatchesAnyAnomaly(List.of(AnomalyType.SAGA_STUCK));

        assertThat(noId.validationError()).hasValue("no id");
    }

    @Test
    void repeatedAnomalyTypesInOneRuleAreCollapsed() {
        AnomalySignatureRule rule = new AnomalySignatureRule();
        rule.setMatchesAnyAnomaly(List.of(
                AnomalyType.SAGA_STUCK, AnomalyType.SAGA_STUCK, AnomalyType.STEP_TIMEOUT));

        assertThat(rule.getMatchesAnyAnomaly())
                .containsExactly(AnomalyType.SAGA_STUCK, AnomalyType.STEP_TIMEOUT);
    }

    // ── the boundary this design draws ────────────────────────────────────────

    /**
     * The rules that stay in Java must not reappear here. If one ever does, its condition
     * was reduced to an anomaly type and its real logic — payload content, saga status,
     * timestamp arithmetic — was quietly dropped.
     */
    @Test
    void proceduralRulesAreNotDeclaredHere() {
        assertThat(loaded().ruleIds())
                .doesNotContain("R01", "R02", "R05", "R06", "R10", "R11", "R14", "R20", "R21");
    }

    // ── fixtures ──────────────────────────────────────────────────────────────

    private static AnomalySignatureRule rule(String id) {
        return loaded().rules().stream()
                .filter(r -> id.equals(r.getId()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("rule " + id + " is not declared"));
    }

    private static Anomaly anomaly(Long id, AnomalyType type) {
        Anomaly anomaly = new Anomaly();
        anomaly.setId(id);
        anomaly.setSagaId("saga-1");
        anomaly.setAnomalyType(type);
        anomaly.setDescription(type.name() + " detected");
        return anomaly;
    }
}
