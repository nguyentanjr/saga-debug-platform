package com.example.correlation.domain;

/**
 * On what grounds an observation was filed under its execution.
 *
 * <h2>Why this is recorded at all</h2>
 * Every finding this platform produces — a step timing out, a compensation missing, an
 * event arriving after a terminal state — is a statement about a <em>sequence</em>. The
 * sequence is only as sound as the decision that put those observations together. That
 * decision was previously invisible: a diagnosis read the same whether the correlation
 * came from an identifier the producer stamped on every message or from a link the
 * platform worked out for itself.
 *
 * <p>The platform already refuses to present an inferred step boundary as an observed one
 * ({@code StartProvenance}) and already grades its root-cause evidence rather than scoring
 * it. Correlation was the remaining place where something derived could be reported as
 * something seen. This closes it.
 *
 * <h2>The ladder</h2>
 * In descending order of directness. The first rung that applies is the one used; there is
 * no rung for a guess. When none applies the observation is refused as not correlatable,
 * because a wrong correlation does not surface as an error — it surfaces as a diagnosis
 * that quietly makes no sense.
 *
 * <p>What is deliberately absent: joining two executions because they share a business
 * key, because their timestamps are close, or because they carry the same event type.
 * None of those is evidence; each is a coincidence that a busy system produces constantly.
 */
public enum CorrelationBasis {

    /**
     * The source declared where its correlation key lives and this observation carried it.
     *
     * <p>The strongest and most common case: a saga id, an order number, a message key —
     * something the producer itself put there. Systems built around an orchestrator are
     * always here.
     */
    DECLARED_KEY,

    /**
     * No business identifier, but the observation carried propagated trace context.
     *
     * <p>Weaker than a declared key because a trace is a request-scoped notion rather than
     * a business one: one trace can span two executions, and a long-running saga can span
     * several traces. It is still evidence the producer emitted, not something the platform
     * invented, and for a system whose services propagate W3C context without sharing any
     * business id it is the difference between being observable and not.
     *
     * <p>This is the rung that makes trace propagation worth recommending to a system
     * being onboarded — a deployment flag rather than a code change — without the core
     * requiring it of anyone.
     */
    PROPAGATED_CONTEXT,

    /**
     * Filed under an identifier this one was already known to name.
     *
     * <p>The link came from an observation that carried both identifiers at once, and the
     * event that carried it is recorded. Systems whose services each know a different id
     * live here.
     */
    ALIAS_LINK
}
