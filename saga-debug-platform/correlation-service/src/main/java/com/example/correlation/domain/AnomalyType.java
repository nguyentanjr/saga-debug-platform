package com.example.correlation.domain;

/**
 * What kind of thing a detector found.
 *
 * <h2>Why some of these carry a property</h2>
 * A finding is a claim about an execution, and claims have different shapes. Most assert
 * that something <em>happened</em> and was wrong: an event arrived after a terminal state,
 * two terminal events conflicted, a step took longer than its declared timeout. Those stay
 * true no matter what else is later observed — the event really did arrive.
 *
 * <p>Others assert that something is <em>missing</em>. "No start event was seen", "the
 * compensation was never acknowledged", "this saga is stuck". An absence is only ever true
 * of the observations in hand, so a claim of absence is falsified the moment the missing
 * observations turn up — which is exactly what happens when two executions turn out to be
 * one and are folded together.
 *
 * <p>That distinction is not about this project's demo domain; it is about what kind of
 * statement a finding is. It lives here rather than as a list inside the merge code so that
 * a new detector declares the shape of its own claim, instead of a merge somewhere else
 * having to be taught about it.
 */
public enum AnomalyType {

    SAGA_STUCK(Claim.ABSENCE),
    STEP_TIMEOUT(Claim.OCCURRENCE),
    MISSING_TERMINAL_EVENT(Claim.ABSENCE),

    OUT_OF_ORDER(Claim.OCCURRENCE),
    STATE_REGRESSION(Claim.OCCURRENCE),
    FORBIDDEN_POST_TERMINAL(Claim.OCCURRENCE),

    /**
     * "Two observations could each be this run's last, and nothing in hand says which."
     *
     * <p>An absence claim, and the only one that is about the evidence rather than about
     * the observed system: what is missing is anything that would order the two — a clock
     * that separates them, or a dependency between their steps. A third observation, or
     * the same events re-read from a source that stamps them properly, falsifies it.
     *
     * <p>It exists so that "this run's outcome cannot be determined" is something the
     * platform can say. The alternative is to pick one of the two readings and present it
     * as the run's outcome, which is the observer inventing a fact.
     */
    INDETERMINATE_ORDER(Claim.ABSENCE),

    EXACT_DUPLICATE_EVENT(Claim.OCCURRENCE),
    DUPLICATE_COMPENSATION(Claim.OCCURRENCE),

    INVALID_TRANSITION(Claim.OCCURRENCE),
    MULTI_TERMINAL_CONFLICT(Claim.OCCURRENCE),

    COMPENSATION_MISSING(Claim.ABSENCE),
    MISSING_COMPENSATION_ACK(Claim.ABSENCE),

    /** "No event that starts a workflow was seen before this one." */
    ORPHAN_EVENT(Claim.ABSENCE),

    /** "The events of this execution carry no trace, though its siblings do." */
    MISSING_TRACE_CONTEXT(Claim.ABSENCE),

    RETRY_BURST(Claim.OCCURRENCE),

    DELAYED_COMPENSATION(Claim.OCCURRENCE);

    /** What kind of statement a finding of this type makes. */
    public enum Claim {
        /** Something observed happened and should not have. Later observations cannot undo it. */
        OCCURRENCE,
        /** Something expected was not observed. More observations can falsify it. */
        ABSENCE
    }

    private final Claim claim;

    AnomalyType(Claim claim) {
        this.claim = claim;
    }

    public Claim claim() {
        return claim;
    }

    /**
     * Whether a finding of this type can be falsified by observations arriving later.
     *
     * <p>Used when two executions are folded into one: every absence claim made about
     * either half was made against half the evidence, so it is retracted and left to the
     * detectors to raise again if it is still true of the whole.
     */
    public boolean assertsAbsence() {
        return claim == Claim.ABSENCE;
    }

    /**
     * Every type whose claim later observations can falsify.
     *
     * <p>Derived from the types rather than listed by each caller, so a detector added
     * later is covered wherever findings are withdrawn without that code being taught
     * about it. Two places withdraw them today: folding two executions into one, and
     * resolving an execution's workflow after the fact.
     */
    public static java.util.Set<AnomalyType> absenceClaims() {
        return java.util.Arrays.stream(values())
                .filter(AnomalyType::assertsAbsence)
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
    }
}
