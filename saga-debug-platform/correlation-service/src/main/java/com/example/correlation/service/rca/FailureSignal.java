package com.example.correlation.service.rca;

/**
 * What a source system's failure message can be read as saying.
 *
 * <p>The categories are the platform's; the wording that signals each one is the observed
 * system's, and therefore declared. This split is the point of the type. A previous
 * version of R01 matched the literal substrings {@code "circuit breaker"} and
 * {@code "retry exhausted"} against the whole payload, which made one Java library's
 * phrasing into the platform's semantics for every Saga system it might ever watch. A Go
 * or .NET system reports the same two situations in different words, and the core has no
 * business knowing any of them in advance (I7).
 *
 * <p>Both categories below are <em>mechanisms</em>, not root causes. They say how the call
 * failed, not why the system was in a state where it would.
 */
public enum FailureSignal {

    /**
     * The failing call was refused by a circuit breaker that was open at the time.
     *
     * <p>This requires the source to report breaker <em>state</em>. A message the source
     * writes on every failure — whatever words it happens to contain — is not a reading of
     * breaker state, and measuring SC06 showed why the difference matters: across three
     * runs of one scenario the breaker was genuinely closed in the first and open in the
     * other two, while a fixed prefix in the payload read identically in all three.
     */
    BREAKER_REJECTED_CALL,

    /**
     * The source exhausted the retry policy it declares and gave up.
     *
     * <p>Weaker than {@link #BREAKER_REJECTED_CALL} and not a substitute for it: retries
     * run out whether or not a breaker was ever involved. It is the claim that survives
     * when the failure message says nothing about breaker state, and it exists so that
     * such a run yields the conclusion its evidence supports rather than no conclusion
     * at all.
     */
    RETRY_EXHAUSTED
}
