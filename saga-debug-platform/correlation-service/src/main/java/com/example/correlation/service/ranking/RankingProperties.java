package com.example.correlation.service.ranking;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Externalized weights for RCA rule ranking.
 *
 * <p>The RCA service collects all matching rules for a saga and ranks them
 * by a multi-criteria score; the highest-scoring rule becomes the primary
 * root cause and the rest become contributing causes. The weights of the
 * individual criteria are read from configuration so they can be ablated
 * (Group C in the defensibility ladder) without recompiling.
 *
 * <p><b>Score formula</b>:
 * <pre>
 *   score = confidenceWeight * confidence
 *         + severityWeight   * severity_normalized
 *         + evidenceWeight   * evidence_normalized
 * </pre>
 *
 * <p><b>Weight constraint</b>: the three weights should sum to 1.0. This is
 * a convention, not a hard requirement — sums other than 1.0 still produce
 * a consistent ordering, but the absolute score values become harder to
 * interpret. {@link RcaRankingScorer} does not enforce or normalize the
 * sum; profiles are expected to sum to 1.0 by construction.
 *
 * <p><b>Default profile</b> is {@code confidence-only} (1.0 / 0.0 / 0.0):
 * this reproduces the original "sort by confidence" behaviour exactly, so
 * adding this scaffold is behavior-preserving by default. Other profiles
 * (in {@code application-rca-*.yml}) are opt-in via Spring profiles or
 * property overrides during ablation runs.
 *
 * <p>Properties:
 * <ul>
 *   <li>{@code rca.ranking.confidence-weight} — weight on rule's tier confidence</li>
 *   <li>{@code rca.ranking.severity-weight} — weight on operational severity</li>
 *   <li>{@code rca.ranking.evidence-weight} — weight on corroborating evidence count</li>
 *   <li>{@code rca.ranking.profile-name} — human-readable label, logged for audit</li>
 * </ul>
 */
@ConfigurationProperties(prefix = "rca.ranking")
public class RankingProperties {

    /** Weight on the rule's confidence tier value (0.30/0.50/0.75/0.90). */
    private double confidenceWeight = 1.0;

    /** Weight on the rule's severity, normalized to [0, 1]. */
    private double severityWeight = 0.0;

    /**
     * Weight on the rule's evidence count, normalized to [0, 1] via soft
     * cap at 5 entries (more is not better past that point).
     */
    private double evidenceWeight = 0.0;

    /** Human-readable profile label, logged at startup for audit trail. */
    private String profileName = "confidence-only";

    public double getConfidenceWeight() { return confidenceWeight; }
    public void setConfidenceWeight(double confidenceWeight) { this.confidenceWeight = confidenceWeight; }

    public double getSeverityWeight() { return severityWeight; }
    public void setSeverityWeight(double severityWeight) { this.severityWeight = severityWeight; }

    public double getEvidenceWeight() { return evidenceWeight; }
    public void setEvidenceWeight(double evidenceWeight) { this.evidenceWeight = evidenceWeight; }

    public String getProfileName() { return profileName; }
    public void setProfileName(String profileName) { this.profileName = profileName; }

    /**
     * @return sum of the three weights — should be ~1.0 for interpretable scores.
     */
    public double weightSum() {
        return confidenceWeight + severityWeight + evidenceWeight;
    }

    @Override
    public String toString() {
        return String.format(
                "RankingProperties[profile=%s, w_c=%.3f, w_s=%.3f, w_e=%.3f, sum=%.3f]",
                profileName, confidenceWeight, severityWeight, evidenceWeight, weightSum());
    }
}
