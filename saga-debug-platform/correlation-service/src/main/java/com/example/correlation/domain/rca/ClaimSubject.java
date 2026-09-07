package com.example.correlation.domain.rca;

/**
 * Who a candidate's claim is about.
 *
 * <h2>Why a diagnosis needs this</h2>
 * A root cause is an answer to "what did the observed system do wrong". Not every finding
 * this platform makes is an answer to that question. Some are about the platform's own
 * reading — which records it happened to read first, which partitions it was behind on,
 * how much of the run it had in hand when a detector ran. Those are worth recording, and
 * an operator reading a diagnosis is entitled to see them, but publishing one as
 * <em>the</em> root cause states that the observed system failed when what actually
 * happened is that the observer read something in an order of its own making.
 *
 * <p>That distinction had no representation here. Every candidate was implicitly a claim
 * about the observed system, so once promotion existed
 * ({@link com.example.correlation.service.rca.RcaSelector} reading the front of the
 * ranked list) a finding about the observer's reception order could be, and was, reported
 * as the run's cause: {@code OUT_OF_ORDER_DELIVERY (R14)} outranked an unacknowledged
 * compensation and a downstream timeout on real runs of an unmodified third-party system,
 * and advised checking Kafka partition keys for a run whose partition keys were fine.
 *
 * <h2>What this does and does not decide</h2>
 * It decides one thing: eligibility for promotion. It is not a tier, not a severity, and
 * not a retraction. A candidate marked {@link #OBSERVATION_PROCESS} keeps its evidence,
 * its severity, its anchor, its status and its place in the ranking; it is simply not the
 * kind of statement that can answer "what did the observed system do wrong".
 *
 * <p>Deliberately declared per rule rather than derived from an anomaly type or from a
 * rule's subject matter. "This finding is about transport" is not the same claim as "this
 * finding is about my own reading of transport": a source that really did publish one
 * event twice is a fact about that source, and {@code DUPLICATE_DELIVERY (R13)} stays
 * promotable. Only a rule that knows its own subject is the observation process says so.
 *
 * @see com.example.correlation.domain.AnomalyType.Claim for the orthogonal question of
 *      whether later observations can falsify a finding
 */
public enum ClaimSubject {

    /**
     * The observed system did this. The default, and what a root cause must be.
     */
    OBSERVED_SYSTEM,

    /**
     * This platform's own reception or reading did this.
     *
     * <p>Legitimate evidence — it stays in {@code candidates} with everything else it
     * carries — but never an operational cause of the run it was observed on.
     */
    OBSERVATION_PROCESS
}
