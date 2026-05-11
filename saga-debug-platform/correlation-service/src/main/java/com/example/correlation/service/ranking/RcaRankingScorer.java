package com.example.correlation.service.ranking;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;

/**
 * Computes a multi-criteria ranking score for an RCA rule result.
 *
 * <p>This is pure logic — no Spring beans, no DB access — so it can be
 * unit-tested directly. Inputs are the three rule attributes
 * (confidence, severity, evidence count) and a {@link RankingProperties}
 * instance carrying the weights.
 *
 * <p>This class is intentionally NOT in the {@code com.example.correlation.service}
 * package, to avoid coupling between the scorer and the package-private
 * {@code RuleResult} type. {@link com.example.correlation.service.RcaService}
 * adapts {@code RuleResult} to the scorer's primitive inputs at the call site.
 *
 * <h3>Severity normalization</h3>
 * <p>Severities are mapped to evenly-spaced ordinals in [0, 1]:
 * <pre>
 *   CRITICAL = 1.00
 *   HIGH     = 0.75
 *   MEDIUM   = 0.50
 *   LOW      = 0.25
 *   NONE     = 0.00
 *   (any other string defaults to MEDIUM = 0.50)
 * </pre>
 * <p>Even spacing is a Likert-scale convention: ordinal levels treated as
 * having equal psychological/operational distance. This is defensible as a
 * standard qualitative-ranking choice and does not require ablation.
 *
 * <h3>Evidence normalization</h3>
 * <p>Evidence is normalized via a soft cap:
 * <pre>
 *   evidence_norm = min(1.0, evidenceCount / 5.0)
 * </pre>
 * <p>The cap of 5 reflects empirical observation that most rules emit 1–4
 * evidence strings; verbose rules with 6+ entries do not deserve more
 * weight than a rule that emits the same set of facts more concisely.
 * Above 5, additional entries are usually elaboration, not new evidence.
 */
@Component
public class RcaRankingScorer {

    private static final Logger log = LoggerFactory.getLogger(RcaRankingScorer.class);

    /** Soft cap on evidence count before normalization. See class JavaDoc. */
    public static final int EVIDENCE_CAP = 5;

    private final RankingProperties weights;

    public RcaRankingScorer(RankingProperties weights) {
        this.weights = weights;
    }

    @PostConstruct
    void logActiveProfile() {
        log.info("RCA ranking active: {}", weights);
        if (Math.abs(weights.weightSum() - 1.0) > 0.001) {
            log.warn("RCA ranking weights do not sum to 1.0 (sum={}). " +
                    "Scores will still order consistently but absolute values " +
                    "are harder to interpret. Check rca.ranking.* properties.",
                    weights.weightSum());
        }
    }

    /**
     * Compute the score for a rule with the given attributes.
     *
     * @param confidence    rule's tier confidence (0.30 / 0.50 / 0.75 / 0.90)
     * @param severityLabel one of CRITICAL/HIGH/MEDIUM/LOW/NONE
     * @param evidenceCount number of evidence strings the rule emitted
     * @return weighted score; higher = ranked first
     */
    public double score(double confidence, String severityLabel, int evidenceCount) {
        double sevNorm = normalizeSeverity(severityLabel);
        double evNorm = normalizeEvidence(evidenceCount);
        return weights.getConfidenceWeight() * confidence
                + weights.getSeverityWeight() * sevNorm
                + weights.getEvidenceWeight() * evNorm;
    }

    static double normalizeSeverity(String severity) {
        if (severity == null) return 0.5;
        switch (severity) {
            case "CRITICAL": return 1.00;
            case "HIGH":     return 0.75;
            case "MEDIUM":   return 0.50;
            case "LOW":      return 0.25;
            case "NONE":     return 0.00;
            default:         return 0.50;
        }
    }

    static double normalizeEvidence(int count) {
        if (count <= 0) return 0.0;
        return Math.min(1.0, count / (double) EVIDENCE_CAP);
    }

    /** Active weights — exposed for logging and audit-trail purposes. */
    public RankingProperties getWeights() {
        return weights;
    }
}
