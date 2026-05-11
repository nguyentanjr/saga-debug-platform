package com.example.correlation.service.ranking;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Unit tests for {@link RcaRankingScorer}.
 *
 * <p>These tests verify the two non-trivial decisions in the scorer:
 * the severity ordinal mapping and the evidence soft-cap normalization.
 * They also document the score-formula behaviour under each named profile
 * so future ablation runs can be sanity-checked against expected values.
 */
class RcaRankingScorerTest {

    /** Helper: build a scorer with the given weights for a one-off calculation. */
    private static RcaRankingScorer scorerWith(double wc, double ws, double we) {
        RankingProperties p = new RankingProperties();
        p.setConfidenceWeight(wc);
        p.setSeverityWeight(ws);
        p.setEvidenceWeight(we);
        p.setProfileName("test");
        return new RcaRankingScorer(p);
    }

    @Test
    void severityNormalization_followsLikertOrdinals() {
        assertThat(RcaRankingScorer.normalizeSeverity("CRITICAL")).isEqualTo(1.00);
        assertThat(RcaRankingScorer.normalizeSeverity("HIGH")).isEqualTo(0.75);
        assertThat(RcaRankingScorer.normalizeSeverity("MEDIUM")).isEqualTo(0.50);
        assertThat(RcaRankingScorer.normalizeSeverity("LOW")).isEqualTo(0.25);
        assertThat(RcaRankingScorer.normalizeSeverity("NONE")).isEqualTo(0.00);
    }

    @Test
    void severityNormalization_unknownDefaultsToMedium() {
        assertThat(RcaRankingScorer.normalizeSeverity(null)).isEqualTo(0.50);
        assertThat(RcaRankingScorer.normalizeSeverity("WHATEVER")).isEqualTo(0.50);
    }

    @Test
    void evidenceNormalization_softCapsAtFive() {
        assertThat(RcaRankingScorer.normalizeEvidence(0)).isEqualTo(0.0);
        assertThat(RcaRankingScorer.normalizeEvidence(1)).isEqualTo(0.2);
        assertThat(RcaRankingScorer.normalizeEvidence(3)).isEqualTo(0.6);
        assertThat(RcaRankingScorer.normalizeEvidence(5)).isEqualTo(1.0);
        assertThat(RcaRankingScorer.normalizeEvidence(10)).isEqualTo(1.0);
        assertThat(RcaRankingScorer.normalizeEvidence(-3)).isEqualTo(0.0);
    }

    @Test
    void confidenceOnlyProfile_reproducesLegacyBehaviour() {
        // Under the default profile (w_c=1, others=0) the score must equal
        // the rule's confidence exactly — this is the behaviour-preservation
        // sanity check that justifies adding the scaffold.
        RcaRankingScorer s = scorerWith(1.0, 0.0, 0.0);
        assertThat(s.score(0.90, "CRITICAL", 5)).isCloseTo(0.90, within(1e-9));
        assertThat(s.score(0.50, "LOW", 1)).isCloseTo(0.50, within(1e-9));
        assertThat(s.score(0.30, "NONE", 0)).isCloseTo(0.30, within(1e-9));
    }

    @Test
    void confidenceHeavyProfile_breaksTiesWithSeverityAndEvidence() {
        // Two STRONG rules tied at confidence=0.90 should be ordered by
        // severity then evidence. This is the main reason confidence-heavy
        // exists: same-tier rules need a deterministic tie-breaker.
        RcaRankingScorer s = scorerWith(0.60, 0.25, 0.15);
        double a = s.score(0.90, "CRITICAL", 4);   // strong + critical + 4 evidence
        double b = s.score(0.90, "MEDIUM", 2);     // strong + medium + 2 evidence
        assertThat(a).isGreaterThan(b);
    }

    @Test
    void severityHeavyProfile_canPromoteLowerConfidenceCriticalRule() {
        // A WEAK rule with CRITICAL severity should beat a STRONG rule with
        // LOW severity under severity-heavy weights. This documents the
        // effect of the profile, so later ablation results have an explicit
        // expected mechanism to compare against.
        RcaRankingScorer s = scorerWith(0.30, 0.50, 0.20);
        double weakCritical = s.score(0.50, "CRITICAL", 3);
        double strongLow = s.score(0.90, "LOW", 3);
        assertThat(weakCritical).isGreaterThan(strongLow);
    }

    @Test
    void evidenceHeavyProfile_rewardsCorroboratedRules() {
        RcaRankingScorer s = scorerWith(0.30, 0.20, 0.50);
        double sparse = s.score(0.75, "MEDIUM", 1);
        double rich = s.score(0.75, "MEDIUM", 5);
        assertThat(rich).isGreaterThan(sparse);
        // The advantage of "rich" should equal we * (5/5 - 1/5) = 0.50 * 0.8 = 0.40
        assertThat(rich - sparse).isCloseTo(0.40, within(1e-9));
    }

    @Test
    void weightsAreLinear() {
        // Score is a linear combination — verify the obvious algebraic identity.
        RcaRankingScorer s = scorerWith(0.5, 0.3, 0.2);
        double expected = 0.5 * 0.90 + 0.3 * 1.00 + 0.2 * 0.60;
        assertThat(s.score(0.90, "CRITICAL", 3)).isCloseTo(expected, within(1e-9));
    }
}
