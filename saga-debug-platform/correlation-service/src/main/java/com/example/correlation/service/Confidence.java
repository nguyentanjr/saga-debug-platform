package com.example.correlation.service;

/**
 * RCA rule confidence tiers.
 *
 * <p>Replaces the previous practice of assigning each rule its own hardcoded
 * confidence value (0.70, 0.75, 0.80, 0.85, 0.90, 0.92, 0.95, ...). Those
 * 17 distinct values implied a level of precision the rule logic cannot
 * actually defend.
 *
 * <p>Each tier is justified by the <b>specificity of the rule's triggering
 * signature</b>: how directly the evidence supports the named root cause.
 *
 * <ul>
 *   <li><b>STRONG (0.90)</b> — The label literally describes what was
 *       detected. Direct evidence — in the event payload, in the workflow
 *       state machine, or in the event log itself. Hard to mis-fire.
 *       Examples: substring match in failure reason, anomaly types whose
 *       name is the rule's name, conjunction of three observable facts.</li>
 *
 *   <li><b>MEDIUM (0.75)</b> — Signature is reliable, but the label adds
 *       <i>one inferential hop</i>. A different (less likely) cause could
 *       fit the same evidence. Examples: SERVICE_CRASH from stuck+missing
 *       (could also be deadlock or OOM); OUT_OF_ORDER (can false-positive
 *       under clock skew).</li>
 *
 *   <li><b>WEAK (0.50)</b> — Signature is real, but the label specifies a
 *       <i>cause the signature cannot prove</i>. The named root cause is a
 *       heuristic. Example: TIMEOUT_RETRY_STORM (label asserts the retries
 *       were caused by timeouts, but RETRY_BURST anomaly fires for any
 *       repeated failure including non-timeout exceptions).</li>
 *
 *   <li><b>FALLBACK (0.30)</b> — Reserved for rules that fire only when no
 *       other rule matched. Not a confidence claim — by design. Used by
 *       R20 (OBSERVABILITY_GAP) and R21 (STUCK_UNKNOWN_CAUSE) only.</li>
 * </ul>
 *
 * <p><b>Why these specific values</b>: 0.15–0.20 spacing makes tiers
 * clearly distinguishable in ranking without claiming false precision.
 * The 0.30 / 0.50 / 0.75 / 0.90 ladder also satisfies the existing
 * contributing-cause filter ({@code confidence > 0.30}) which excludes
 * fallback rules from contributing-cause aggregation.
 *
 * <p>The exact values are themselves defensible only as "tier markers";
 * they are not claimed to be optimally tuned. Tuning the values is an
 * ablation-study question (Group C in the defensibility ladder).
 */
public enum Confidence {

    /** Direct evidence; label = signature. */
    STRONG(0.90),

    /** Specific signature, one inferential hop to the label. */
    MEDIUM(0.75),

    /** Real signature, label specifies an unprovable cause. */
    WEAK(0.50),

    /** No specific cause identified — observability/unknown class. */
    FALLBACK(0.30);

    public final double value;

    Confidence(double value) {
        this.value = value;
    }
}
