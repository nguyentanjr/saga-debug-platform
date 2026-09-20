package com.example.correlation.execution;

import com.example.correlation.domain.SagaEvent;
import com.example.correlation.domain.SagaStatus;
import com.example.correlation.workflow.WorkflowSpec;
import com.example.correlation.workflow.WorkflowSpec.EventRole;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * One execution's observations placed in the order the workflow can prove they happened,
 * and the status that whole body of evidence implies.
 *
 * <h2>The two orders</h2>
 * An observer of a distributed system meets two different orders and must never confuse
 * them. <em>Processing order</em> is the order this platform read the events: a property of
 * topics, partitions, consumer threads and retries — of the observer. <em>Business order</em>
 * is the order the work happened: a property of the observed system. Only the second says
 * anything about the run.
 *
 * <p>Status used to be folded in processing order, one event at a time, over the status
 * stored from the last one. Because {@link WorkflowSpec#statusAfter} answers absolutely —
 * {@code INVENTORY_RESERVED} means COMPLETED whatever came before it — folding in
 * processing order makes the answer "whatever the most recently <em>read</em> event
 * implies". A run whose completion was read before its middle step therefore rested at
 * IN_PROGRESS, its {@code endTime} was the instant of whichever event happened to arrive
 * last, and the observer reported {@code STATE_REGRESSION} and
 * {@code FORBIDDEN_POST_TERMINAL} against a system that had done nothing wrong. Every one
 * of those findings described the observer's own read order.
 *
 * <p>So the status is reconstructed from all the evidence in hand, every time, rather than
 * advanced by whichever event arrived. Re-reading the run's events is what makes the answer
 * independent of the order they were read in; there is no cheaper way to be order-invariant
 * than to look at everything. Executions hold a handful of events, and the caller has
 * already loaded them for the detectors.
 *
 * <h2>What decides the order, and what refuses to</h2>
 * Event time first. Where two observations carry the same instant — a producer stamping a
 * burst identically, or a source with no clock of its own falling back to delivery time —
 * the workflow's own dependency graph decides: work at an upstream step happened before
 * work at a downstream one, and within one step a start happened before its completion.
 * That is the workflow proving an order, not the platform assuming one.
 *
 * <p>Where neither says anything — same instant, two steps the graph does not connect —
 * this class does not pick. It reports the contested observations and states no outcome.
 * Sorting still produces some sequence, because a sequence is needed to walk, but nothing
 * is <em>concluded</em> from a pair it cannot order.
 *
 * <h2>Why not {@link ExecutionProjection}</h2>
 * That projection answers a different question — what each step did and how long it took —
 * and to answer it collapses every step to a single operation, discarding the interleaving
 * between steps. Which status-bearing observation is last, and whether the run had already
 * ended when a given event happened, are questions about exactly that interleaving. Adding
 * saga-level status semantics to a step-level timing structure would make the answer to one
 * question depend on the other, so the ordering lives here. Both are built from the same
 * (events, spec) pair and neither touches persistence.
 */
public final class EvidenceTimeline {

    private final WorkflowSpec spec;
    private final List<SagaEvent> ordered;
    private final List<Bearing> bearings;
    private final SagaStatus status;
    private final SagaEvent decidingEvent;
    private final List<SagaEvent> contested;
    private final Regression regression;

    /**
     * An observation that provably followed an ending and reported something lower.
     *
     * <p>Reported only for a provably ordered pair whose earlier half is <em>terminal</em>.
     * A pair whose order is contested cannot evidence a regression: it would be the
     * observer's tie-break showing up as a fault in the observed system, which is the class
     * of bug this file exists to remove. Nor can a pair of non-terminal statuses, for the
     * reasons on {@link #firstRegression}: a running saga moves down the lifecycle ranking
     * legitimately, and whether two such observations even sat next to each other depended
     * on how much of the run had arrived.
     *
     * @param from the ending that was contradicted; always terminal
     */
    public record Regression(SagaEvent earlier, SagaStatus from, SagaEvent later, SagaStatus to) {
    }

    /**
     * An ending, together with an observation the evidence places after it.
     *
     * <p>Reported only for a provably ordered pair, for the reason on {@link Regression}:
     * a pair the evidence does not order would put the observer's own tie-break into a
     * finding against the observed system.
     *
     * @param endedAs the status {@code terminal} established; always terminal
     */
    public record PostTerminal(SagaEvent terminal, SagaStatus endedAs, SagaEvent late) {
    }

    /** An observation together with what it says the run's status is. */
    private record Bearing(SagaEvent event, SagaStatus status) {
    }

    /** What the timeline can prove about the order of two observations. */
    private enum Relation { BEFORE, AFTER, UNDETERMINED }

    private EvidenceTimeline(WorkflowSpec spec, List<SagaEvent> ordered, List<Bearing> bearings,
            SagaStatus status, SagaEvent decidingEvent, List<SagaEvent> contested,
            Regression regression) {
        this.spec = spec;
        this.ordered = ordered;
        this.bearings = bearings;
        this.status = status;
        this.decidingEvent = decidingEvent;
        this.contested = contested;
        this.regression = regression;
    }

    /**
     * @param spec   the workflow this run belongs to, or null while it is still unknown.
     *               Without one nothing can be classified, so the timeline reports only
     *               that the run has been seen — never a guess at where it got to
     * @param events every observation of the run in hand, in any order
     */
    public static EvidenceTimeline of(WorkflowSpec spec, List<SagaEvent> events) {
        List<SagaEvent> given = events == null ? List.of() : events;
        if (spec == null) {
            // A run with no specification has no derivable status. IN_PROGRESS here is not
            // a reading of its evidence; it says only that the run exists and that nothing
            // has been able to judge it.
            return new EvidenceTimeline(null, List.copyOf(given), List.of(),
                    given.isEmpty() ? SagaStatus.STARTED : SagaStatus.IN_PROGRESS,
                    null, List.of(), null);
        }

        List<SagaEvent> ordered = new ArrayList<>(given);
        ordered.sort(businessOrder(spec));

        List<Bearing> bearings = new ArrayList<>();
        for (SagaEvent event : ordered) {
            statusOf(spec, event).ifPresent(status -> bearings.add(new Bearing(event, status)));
        }

        if (bearings.isEmpty()) {
            return new EvidenceTimeline(spec, List.copyOf(ordered), List.of(),
                    ordered.isEmpty() ? SagaStatus.STARTED : SagaStatus.IN_PROGRESS,
                    null, List.of(), null);
        }

        // The run's status is what the last status-bearing observation says — but "last"
        // has to be provable. Every observation the timeline cannot order against that one
        // is an equally good candidate for the last position.
        Bearing last = bearings.get(bearings.size() - 1);
        List<Bearing> contenders = new ArrayList<>();
        for (Bearing bearing : bearings) {
            if (bearing == last
                    || relate(spec, bearing.event(), last.event()) == Relation.UNDETERMINED) {
                contenders.add(bearing);
            }
        }

        Set<SagaStatus> claimed = new LinkedHashSet<>();
        contenders.forEach(bearing -> claimed.add(bearing.status()));

        SagaStatus status;
        SagaEvent deciding;
        List<SagaEvent> contestedEvents;
        if (claimed.size() == 1) {
            status = last.status();
            deciding = last.event();
            contestedEvents = List.of();
        } else {
            // The evidence does not say which of these came last, so it does not say how
            // the run ended. Fall back to what is settled before the contest and claim no
            // outcome: an observer that picks here reports one of two mutually exclusive
            // endings as fact.
            status = beforeTheContest(bearings, contenders);
            deciding = null;
            contestedEvents = contenders.stream().map(Bearing::event).toList();
        }

        return new EvidenceTimeline(spec, List.copyOf(ordered), List.copyOf(bearings),
                status, deciding, contestedEvents, firstRegression(spec, bearings));
    }

    /**
     * The status established by the observations the contest does not reach.
     *
     * <p>Never terminal. A terminal reading before a contested position is superseded by
     * whatever the contest resolves to, so it is no more assertable than the contest is.
     */
    private static SagaStatus beforeTheContest(List<Bearing> bearings, List<Bearing> contenders) {
        SagaStatus settled = null;
        for (Bearing bearing : bearings) {
            if (contenders.contains(bearing)) break;
            settled = bearing.status();
        }
        return settled == null || settled.isTerminal() ? SagaStatus.IN_PROGRESS : settled;
    }

    /**
     * The first observation that provably followed an ending, and contradicted it.
     *
     * <h2>Why a drop in rank is not on its own enough</h2>
     * Two things had to be true for this to be a claim about the observed system, and only
     * one of them was checked. The pair has to be provably ordered — it was. But the pair
     * also has to be one the ranking can say something about, and between two
     * <em>non-terminal</em> statuses it cannot.
     *
     * <p>{@link SagaStatus#progressRank} orders the lifecycle, and a run legitimately moves
     * down it while still running. The case that exposed this has a workflow whose stock and
     * payment steps are declared concurrent: payment failed, moving the run to COMPENSATING,
     * and the stock arm — which had been in flight the whole time and owed nothing to the
     * payment arm — then reported success, which reads as IN_PROGRESS. Nothing went
     * backwards. Two independent pieces of work were compared on a scale that does not
     * order them.
     *
     * <p>Worse, whether that comparison happened at all depended on what had arrived. The
     * finding was raised on a three-event prefix and, being an
     * {@link com.example.correlation.domain.AnomalyType.Claim#OCCURRENCE} claim, was never
     * revisited when the fourth event turned up and separated the pair. The same run read
     * live reported {@code TERMINAL_STATE_OVERWRITE}; read as a backlog it reported the
     * compensation failure that was actually there.
     *
     * <h2>What is established, and why it survives more evidence</h2>
     * A terminal status is absolute: the specification says the run is over. An observation
     * provably after one, reporting something lower, is the run contradicting its own
     * ending — which is precisely what {@code TERMINAL_STATE_OVERWRITE} (R04), the rule this
     * finding feeds, is named for. A drop between two non-terminal statuses was never that.
     *
     * <p>Requiring the earlier status to be terminal is also what makes the claim safe to
     * make on partial evidence, which is the only kind this platform ever has. The
     * predicate is monotone: no later observation can make a terminal status non-terminal,
     * nor un-order a pair {@link #relate} has ordered. Inserting a further observation
     * between the two leaves it standing either way — every status ranked at or above a
     * terminal one is itself terminal, so the inserted bearing either drops from the
     * ending itself or becomes a new ending that the later observation still contradicts.
     * So the answer is the same whether the events arrive together or one at a time, which
     * is the invariant this whole class exists to hold.
     *
     * <p>What is given up is a non-terminal backwards move along a dependency edge. Nothing
     * observed on any of the systems validated so far produced one, and establishing it
     * would need evidence that the steps in between were not merely unobserved — evidence
     * an observer of a partial stream does not have.
     */
    private static Regression firstRegression(WorkflowSpec spec, List<Bearing> bearings) {
        for (int i = 1; i < bearings.size(); i++) {
            Bearing earlier = bearings.get(i - 1);
            Bearing later = bearings.get(i);
            if (!earlier.status().isTerminal()) continue;
            if (relate(spec, earlier.event(), later.event()) != Relation.BEFORE) continue;
            if (later.status().progressRank() < earlier.status().progressRank()) {
                return new Regression(earlier.event(), earlier.status(),
                        later.event(), later.status());
            }
        }
        return null;
    }

    /**
     * A total order to walk the run in.
     *
     * <p>Every key is a scalar, deliberately. The dependency relation itself is not
     * transitive — two steps on unrelated branches are mutually un-upstream — and a
     * comparator built on an intransitive relation is undefined behaviour in a sort, not
     * merely imprecise. {@link WorkflowSpec#upstreamStepCount} is its transitive rendering
     * and agrees with it wherever it has an opinion.
     *
     * <p>The event id comes last as a tie-break for determinism only. Nothing is concluded
     * from it: {@link #relate} reports such a pair as undetermined, and every claim this
     * class makes is checked against that rather than against the sort.
     */
    private static Comparator<SagaEvent> businessOrder(WorkflowSpec spec) {
        return Comparator
                .comparing(SagaEvent::getTimestamp, Comparator.nullsLast(Comparator.naturalOrder()))
                .thenComparingInt(event -> spec.classifyEvent(event.getEventType())
                        .map(role -> spec.upstreamStepCount(role.stepId()))
                        .orElse(Integer.MAX_VALUE))
                .thenComparingInt(event -> spec.classifyEvent(event.getEventType())
                        .map(role -> lifecycleRank(role.role()))
                        .orElse(Integer.MAX_VALUE))
                .thenComparing(SagaEvent::getEventId,
                        Comparator.nullsLast(Comparator.naturalOrder()));
    }

    /**
     * What can be proved about the order of two observations, as opposed to what the sort
     * had to decide in order to produce a sequence at all.
     */
    private static Relation relate(WorkflowSpec spec, SagaEvent left, SagaEvent right) {
        if (left == right) return Relation.UNDETERMINED;

        Instant leftAt = left.getTimestamp();
        Instant rightAt = right.getTimestamp();
        if (leftAt != null && rightAt != null && !leftAt.equals(rightAt)) {
            return leftAt.isBefore(rightAt) ? Relation.BEFORE : Relation.AFTER;
        }

        // Same instant, or an observation with no clock at all. The clock has nothing left
        // to say about this pair; the workflow might.
        Optional<EventRole> leftRole = spec.classifyEvent(left.getEventType());
        Optional<EventRole> rightRole = spec.classifyEvent(right.getEventType());
        if (leftRole.isEmpty() || rightRole.isEmpty()) return Relation.UNDETERMINED;

        String leftStep = leftRole.get().stepId();
        String rightStep = rightRole.get().stepId();
        if (!Objects.equals(leftStep, rightStep)) {
            if (spec.isUpstreamOf(leftStep, rightStep)) return Relation.BEFORE;
            if (spec.isUpstreamOf(rightStep, leftStep)) return Relation.AFTER;
            return Relation.UNDETERMINED;
        }

        int leftRank = lifecycleRank(leftRole.get().role());
        int rightRank = lifecycleRank(rightRole.get().role());
        if (leftRank == rightRank) return Relation.UNDETERMINED;
        return leftRank < rightRank ? Relation.BEFORE : Relation.AFTER;
    }

    /** Where in one step's life an observation falls. */
    private static int lifecycleRank(EventRole.Role role) {
        return switch (role) {
            case STARTED -> 0;
            case RETRY -> 1;
            case COMPLETED, FAILED -> 2;
        };
    }

    private static Optional<SagaStatus> statusOf(WorkflowSpec spec, SagaEvent event) {
        return spec.classifyEvent(event.getEventType()).flatMap(spec::statusAfter);
    }

    // ── what the caller asks ──────────────────────────────────────────────────

    /** The status the whole body of evidence implies, independent of read order. */
    public SagaStatus status() {
        return status;
    }

    /** The run's observations in business order. */
    public List<SagaEvent> inBusinessOrder() {
        return ordered;
    }

    /** The earliest instant any observation of this run carries. */
    public Optional<Instant> startedAt() {
        return ordered.stream()
                .map(SagaEvent::getTimestamp)
                .filter(Objects::nonNull)
                .min(Comparator.naturalOrder());
    }

    /** The latest instant any observation of this run carries. */
    public Optional<Instant> lastEventAt() {
        return ordered.stream()
                .map(SagaEvent::getTimestamp)
                .filter(Objects::nonNull)
                .max(Comparator.naturalOrder());
    }

    /**
     * When the run ended, or empty while it has not.
     *
     * <p>The instant of the observation the terminal status rests on — not of whichever
     * event was being processed when the platform worked it out.
     */
    public Optional<Instant> endedAt() {
        return status.isTerminal() && decidingEvent != null
                ? Optional.ofNullable(decidingEvent.getTimestamp())
                : Optional.empty();
    }

    /**
     * The observations whose relative order the evidence does not settle, when that
     * indecision is what stops the run's outcome from being stated. Empty otherwise.
     */
    public List<SagaEvent> contestedOutcome() {
        return contested;
    }

    public Optional<Regression> regression() {
        return Optional.ofNullable(regression);
    }

    /**
     * The ending, and the observation the evidence puts after it, if there provably is
     * such a pair.
     *
     * <p>The question {@code FORBIDDEN_POST_TERMINAL} means to ask. Asking it of the
     * stored status instead answered "had the platform already concluded the run was over
     * by the time it got round to this event", which is true of every event read after a
     * completion — including the ones that happened before it.
     *
     * <p>Asked of the whole reconstruction, and not of whichever observation is in hand.
     * The arriving event is half of the pair only when it happens to be the <em>later</em>
     * half, so scoping the question to it left every run whose terminal event arrived last
     * unexamined: the same three observations raised the finding on one delivery order and
     * stayed silent on another. That is the shape of the FP-2 defect — a claim decided on a
     * prefix and never revisited — reached through a different predicate, and it breaks I9
     * for a finding I9 does not exempt.
     */
    public Optional<PostTerminal> postTerminal() {
        return bearings.stream()
                .filter(bearing -> bearing.status().isTerminal())
                .flatMap(ended -> ordered.stream()
                        .filter(late -> relate(spec, ended.event(), late) == Relation.BEFORE)
                        .findFirst()
                        .map(late -> new PostTerminal(ended.event(), ended.status(), late))
                        .stream())
                .findFirst();
    }
}
