package com.example.correlation.service;

import com.example.correlation.domain.Anomaly;
import com.example.correlation.domain.AnomalyType;
import com.example.correlation.domain.CorrelationBasis;
import com.example.correlation.domain.ExecutionKey;
import com.example.correlation.domain.IngestedEvent;
import com.example.correlation.domain.RcaResult;
import com.example.correlation.domain.SagaEvent;
import com.example.correlation.domain.SagaInstance;
import com.example.correlation.domain.SagaStatus;
import com.example.correlation.domain.rca.RcaCandidate;
import com.example.correlation.execution.ExecutionProjectionService;
import com.example.correlation.repository.AnomalyRepository;
import com.example.correlation.repository.RcaResultRepository;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.example.correlation.service.rca.FailureSignalRegistry;
import com.example.correlation.service.rca.RcaSelector;
import com.example.correlation.service.rca.RuleRegistry;
import com.example.correlation.workflow.WorkflowRegistry;
import com.example.correlation.workflow.WorkflowSpec;
import com.example.correlation.workflow.WorkflowSpec.TransitionJudgment;
import com.example.correlation.workflow.WorkflowSpec.TransitionVerdict;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Whether an observed pair of steps is a transition the workflow forbids, or a transition
 * the platform has not finished watching.
 *
 * <h2>The defect these were written against</h2>
 * {@code judgeTransition} asked whether the two steps were <em>adjacent</em> in the
 * declared graph, and answered INVALID whenever they were not. Adjacency is a property of
 * the specification; what was observed is a property of the evidence. Conflating them made
 * every gap in delivery into an accusation:
 *
 * <pre>
 *   observed:  ORDER_CREATED, INVENTORY_RESERVED
 *   declared:  CREATE_ORDER -> PROCESS_PAYMENT -> RESERVE_INVENTORY
 *   answered:  INVALID_TRANSITION
 * </pre>
 *
 * <p>The run had gone exactly where the graph says. The platform had not yet read
 * {@code PAYMENT_COMPLETED} — a partition it was behind on, a retry in flight. Measured
 * live on this code before the change: injecting those two events raised a standing
 * {@code INVALID_TRANSITION}, and because the finding is an occurrence claim it was never
 * withdrawn when the middle event arrived.
 *
 * <p>The distinction is not a nicety. A wrong transition says the observed system violated
 * its own workflow; a missing observation says the observer is behind. Only the first is a
 * finding, and only reachability tells them apart.
 *
 * <h2>A second false positive found on the way</h2>
 * A declared edge's condition names the outcome of the step it <em>leaves</em>. The caller
 * passed the outcome of the step it arrives at, so a loan assessment reporting APPROVED
 * was judged against the edge for SUCCESS — what disbursement reports — and every healthy
 * run of a workflow with named outcomes was condemned for the transition it correctly made.
 */
class TransitionEvidenceTest {

    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
    private static final String SOURCE = ExecutionKey.DEFAULT_SOURCE;
    private static final ExecutionKey RUN = ExecutionKey.of(SOURCE, "run-1");

    private Map<String, SagaInstance> instances;
    private List<SagaEvent> events;
    private List<Anomaly> anomalies;

    private WorkflowRegistry registry;
    private WorkflowSpec orderSaga;
    private WorkflowSpec loanApproval;
    private CorrelationService correlationService;
    private SagaInstanceRepository sagaInstanceRepository;
    private SagaEventRepository sagaEventRepository;
    private AnomalyRepository anomalyRepository;

    @BeforeEach
    void setUp() {
        instances = new LinkedHashMap<>();
        events = new ArrayList<>();
        anomalies = new ArrayList<>();

        sagaInstanceRepository = mock(SagaInstanceRepository.class);
        when(sagaInstanceRepository.findById(anyString()))
                .thenAnswer(call -> Optional.ofNullable(instances.get(call.getArgument(0))));
        when(sagaInstanceRepository.findByIdForUpdate(anyString()))
                .thenAnswer(call -> Optional.ofNullable(instances.get(call.getArgument(0))));
        when(sagaInstanceRepository.save(any(SagaInstance.class))).thenAnswer(call -> {
            SagaInstance instance = call.getArgument(0);
            instances.put(instance.getSagaId(), instance);
            return instance;
        });

        sagaEventRepository = mock(SagaEventRepository.class);
        when(sagaEventRepository.findBySourceNameAndEventId(anyString(), anyString()))
                .thenAnswer(call -> events.stream()
                        .filter(e -> e.getEventId().equals(call.getArgument(1)))
                        .findFirst());
        when(sagaEventRepository.findBySagaIdOrderByTimestampAsc(anyString()))
                .thenAnswer(call -> events.stream()
                        .filter(e -> e.getSagaId().equals(call.getArgument(0)))
                        .sorted(Comparator.comparing(SagaEvent::getTimestamp))
                        .toList());
        when(sagaEventRepository.save(any(SagaEvent.class))).thenAnswer(call -> {
            events.add(call.getArgument(0));
            return call.getArgument(0);
        });

        anomalyRepository = mock(AnomalyRepository.class);
        when(anomalyRepository.save(any(Anomaly.class))).thenAnswer(call -> {
            Anomaly anomaly = call.getArgument(0);
            if (anomaly.getId() == null) {
                anomaly.setId((long) anomalies.size() + 1);
                anomalies.add(anomaly);
            }
            return anomaly;
        });
        when(anomalyRepository.findStandingBySagaId(anyString())).thenAnswer(call ->
                anomalies.stream().filter(a -> a.getSagaId().equals(call.getArgument(0))
                        && !a.isRetracted()).toList());
        when(anomalyRepository.existsBySagaIdAndAnomalyType(anyString(), any()))
                .thenAnswer(call -> anomalies.stream()
                        .anyMatch(a -> a.getSagaId().equals(call.getArgument(0))
                                && a.getAnomalyType() == call.getArgument(1) && !a.isRetracted()));

        registry = new WorkflowRegistry();
        registry.load();
        orderSaga = registry.byType("ORDER_SAGA").orElseThrow();
        loanApproval = registry.byType("LOAN_APPROVAL").orElseThrow();

        // The real detector, not a mock: the question is whether an INVALID_TRANSITION row
        // is written, and a mock can only report that the detector was consulted.
        AnomalyDetectionService anomalyDetectionService = new AnomalyDetectionService(
                sagaInstanceRepository, sagaEventRepository, anomalyRepository,
                registry, new ObjectMapper(), new DetectionProperties(),
                new ExecutionProjectionService());

        correlationService = new CorrelationService(
                sagaInstanceRepository, sagaEventRepository, anomalyDetectionService,
                new DeduplicationCache(), registry, new ObjectMapper());
    }

    // ── 1. a missing intermediate observation ─────────────────────────────────

    /**
     * A and C observed, B not. The graph declares A → B → C, so what was seen is exactly
     * what the graph allows; the only thing established is that B has not been read yet.
     */
    @Test
    void aTargetReachableThroughAnUnobservedStepIsNotJudged() {
        TransitionJudgment judgment = orderSaga.explainTransition(
                "CREATE_ORDER", "RESERVE_INVENTORY", WorkflowSpec.SUCCESS_OUTCOME);

        assertThat(judgment.verdict()).isEqualTo(TransitionVerdict.UNKNOWN);
        assertThat(judgment.stepsInBetween()).containsExactly("PROCESS_PAYMENT");
    }

    /** And no finding is filed for it. */
    @Test
    void aMissingIntermediateObservationRaisesNoInvalidTransition() {
        observe("e1", "ORDER_CREATED", 0);
        observe("e3", "INVENTORY_RESERVED", 2);

        assertThat(raised()).doesNotContain(AnomalyType.INVALID_TRANSITION);
    }

    /** Nor when several steps in a row are missing. */
    @Test
    void severalMissingStepsInARowAreStillJustMissing() {
        assertThat(loanApproval.explainTransition(
                "SUBMIT_APPLICATION", "DISBURSE_FUNDS", WorkflowSpec.SUCCESS_OUTCOME))
                .extracting(TransitionJudgment::verdict)
                .isEqualTo(TransitionVerdict.UNKNOWN);

        observe("l1", "APPLICATION_SUBMITTED", 0);
        observe("l4", "FUNDS_DISBURSED", 3);

        assertThat(raised()).doesNotContain(AnomalyType.INVALID_TRANSITION);
    }

    // ── 2. the intermediate observation turns up afterwards ───────────────────

    /**
     * A, then C, then B. Once B is in hand the reconstruction judges the pairs the run
     * really made — A→B and B→C, both declared — and never retrospectively invents a
     * finding out of the window in which C had no predecessor but A.
     */
    @Test
    void anIntermediateEventArrivingLateCompletesTheRunWithoutAFinding() {
        observe("e1", "ORDER_CREATED", 0);
        observe("e3", "INVENTORY_RESERVED", 2);
        observe("e2", "PAYMENT_COMPLETED", 1);

        assertThat(raised()).doesNotContain(AnomalyType.INVALID_TRANSITION);
        assertThat(run().getStatus()).isEqualTo(SagaStatus.COMPLETED);
        assertThat(run().getEndTime()).isEqualTo(T0.plusSeconds(2));
    }

    // ── 3. a transition the workflow really forbids ───────────────────────────

    /**
     * The assessment reported APPROVED, which the graph routes to disbursement, and no path
     * from disbursement reaches human review. Nothing about delivery order or unobserved
     * steps can explain that, so it is a claim about the observed system.
     */
    @Test
    void anOutcomeRoutedSomewhereItCannotReachIsInvalid() {
        assertThat(loanApproval.judgeTransition("ASSESS_RISK", "HUMAN_REVIEW", "APPROVED"))
                .isEqualTo(TransitionVerdict.INVALID);

        observe("l1", "APPLICATION_SUBMITTED", 0);
        observe("l2", "ASSESSMENT_APPROVED", 1);
        observe("l3", "REVIEW_APPROVED", 2);

        assertThat(raised()).contains(AnomalyType.INVALID_TRANSITION);
    }

    /** Going backwards is forbidden too: nothing downstream of a step reaches its start. */
    @Test
    void aRunMovingBackwardsThroughItsGraphIsInvalid() {
        assertThat(orderSaga.judgeTransition(
                "PROCESS_PAYMENT", "CREATE_ORDER", WorkflowSpec.SUCCESS_OUTCOME))
                .isEqualTo(TransitionVerdict.INVALID);
    }

    /**
     * And an outcome the graph declares to end the run has no legitimate successor at all,
     * so a step following it contradicts the specification rather than merely being far
     * from it.
     */
    @Test
    void aStepFollowingAnOutcomeThatEndsTheRunIsInvalid() {
        assertThat(orderSaga.judgeTransition(
                "RESERVE_INVENTORY", "CREATE_ORDER", WorkflowSpec.SUCCESS_OUTCOME))
                .isEqualTo(TransitionVerdict.INVALID);
    }

    // ── 4. a declared, adjacent transition ────────────────────────────────────

    @Test
    void aDeclaredTransitionIsValid() {
        assertThat(orderSaga.judgeTransition(
                "CREATE_ORDER", "PROCESS_PAYMENT", WorkflowSpec.SUCCESS_OUTCOME))
                .isEqualTo(TransitionVerdict.VALID);
        assertThat(orderSaga.judgeTransition(
                "PROCESS_PAYMENT", "RESERVE_INVENTORY", WorkflowSpec.SUCCESS_OUTCOME))
                .isEqualTo(TransitionVerdict.VALID);
    }

    @Test
    void aHealthyRunRaisesNoTransitionFinding() {
        observeHappyPath();

        assertThat(raised()).doesNotContain(AnomalyType.INVALID_TRANSITION);
        assertThat(run().getStatus()).isEqualTo(SagaStatus.COMPLETED);
    }

    /**
     * The second false positive: a workflow whose steps have named outcomes. The edge out of
     * the assessment is declared on APPROVED — the assessment's own outcome — so that is the
     * condition the run has to be judged against.
     */
    @Test
    void aHealthyRunOfAWorkflowWithNamedOutcomesRaisesNoTransitionFinding() {
        observe("l1", "APPLICATION_SUBMITTED", 0);
        observe("l2", "ASSESSMENT_APPROVED", 1);
        observe("l3", "FUNDS_DISBURSED", 2);

        assertThat(raised()).doesNotContain(AnomalyType.INVALID_TRANSITION);
        assertThat(run().getStatus()).isEqualTo(SagaStatus.COMPLETED);
    }

    /** Including the branch that routes through a human. */
    @Test
    void theBranchingRunOfThatWorkflowRaisesNoTransitionFindingEither() {
        observe("l1", "APPLICATION_SUBMITTED", 0);
        observe("l2", "ASSESSMENT_FLAGGED", 1);
        observe("l3", "REVIEW_APPROVED", 2);
        observe("l4", "FUNDS_DISBURSED", 3);

        assertThat(raised()).doesNotContain(AnomalyType.INVALID_TRANSITION);
    }

    // ── 5. redelivery ─────────────────────────────────────────────────────────

    /** A replayed event is folded in nowhere, so it cannot change a verdict either. */
    @Test
    void redeliveryDoesNotChangeTheVerdict() {
        observe("e1", "ORDER_CREATED", 0);
        observe("e3", "INVENTORY_RESERVED", 2);
        assertThat(raised()).doesNotContain(AnomalyType.INVALID_TRANSITION);

        observe("e3", "INVENTORY_RESERVED", 2);
        observe("e1", "ORDER_CREATED", 0);

        assertThat(raised()).doesNotContain(AnomalyType.INVALID_TRANSITION);
        assertThat(events).hasSize(2);
    }

    /** And replaying a run that really is invalid does not multiply the finding. */
    @Test
    void redeliveryDoesNotDuplicateATrueFinding() {
        observe("l1", "APPLICATION_SUBMITTED", 0);
        observe("l2", "ASSESSMENT_APPROVED", 1);
        observe("l3", "REVIEW_APPROVED", 2);
        observe("l3", "REVIEW_APPROVED", 2);

        assertThat(raised().stream().filter(t -> t == AnomalyType.INVALID_TRANSITION).count())
                .isEqualTo(1);
    }

    // ── 6. delivery order must not turn UNKNOWN into INVALID ──────────────────

    /**
     * Every delivery order of one healthy run. A verdict that changed with arrival order
     * would be the observer's own read sequence deciding whether the observed system is at
     * fault, which is the whole class of bug this file exists to remove.
     */
    @Test
    void noDeliveryOrderOfAHealthyRunProducesAFinding() {
        List<String[]> happyPath = List.of(
                new String[]{"e1", "ORDER_CREATED", "0"},
                new String[]{"e2", "PAYMENT_COMPLETED", "1"},
                new String[]{"e3", "INVENTORY_RESERVED", "2"});

        for (List<String[]> order : permutations(happyPath)) {
            setUp();
            order.forEach(e -> observe(e[0], e[1], Long.parseLong(e[2])));
            assertThat(raised())
                    .as("delivery order %s", order.stream().map(e -> e[1]).toList())
                    .doesNotContain(AnomalyType.INVALID_TRANSITION);
        }
    }

    /** The same for a run whose outcome really is invalid: it is found in every order. */
    @Test
    void aTrulyInvalidRunIsFoundInEveryDeliveryOrder() {
        List<String[]> invalidRun = List.of(
                new String[]{"l1", "APPLICATION_SUBMITTED", "0"},
                new String[]{"l2", "ASSESSMENT_APPROVED", "1"},
                new String[]{"l3", "REVIEW_APPROVED", "2"});

        for (List<String[]> order : permutations(invalidRun)) {
            setUp();
            order.forEach(e -> observe(e[0], e[1], Long.parseLong(e[2])));
            assertThat(raised())
                    .as("delivery order %s", order.stream().map(e -> e[1]).toList())
                    .contains(AnomalyType.INVALID_TRANSITION);
        }
    }

    // ── 7. compensation ───────────────────────────────────────────────────────

    /**
     * A failure routes to the COMPENSATING marker, which is a phase rather than an ending:
     * the work done in it is the compensation steps. Reading the marker as "nothing may
     * follow" would make every ordinary compensation a forbidden transition.
     */
    @Test
    void aFailureHandingOffToItsCompensationIsValid() {
        assertThat(orderSaga.judgeTransition(
                "RESERVE_INVENTORY", "REFUND_PAYMENT", WorkflowSpec.FAILED_OUTCOME))
                .isEqualTo(TransitionVerdict.VALID);
        assertThat(loanApproval.judgeTransition(
                "DISBURSE_FUNDS", "REVERSE_DISBURSEMENT", WorkflowSpec.FAILED_OUTCOME))
                .isEqualTo(TransitionVerdict.VALID);
    }

    /** A compensation reachable only through steps that were not observed is not judged. */
    @Test
    void aCompensationReachedThroughUnobservedStepsIsNotJudged() {
        assertThat(orderSaga.judgeTransition(
                "CREATE_ORDER", "REFUND_PAYMENT", WorkflowSpec.SUCCESS_OUTCOME))
                .isEqualTo(TransitionVerdict.UNKNOWN);
    }

    /** A whole compensating run, delivered backwards, raises no transition finding. */
    @Test
    void aCompensatingRunReadBackwardsRaisesNoTransitionFinding() {
        observe("c5", "PAYMENT_REFUNDED", 4);
        observe("c4", "REFUND_PAYMENT", 3);
        observe("c3", "INVENTORY_FAILED", 2);
        observe("c2", "PAYMENT_COMPLETED", 1);
        observe("c1", "ORDER_CREATED", 0);

        assertThat(raised()).doesNotContain(AnomalyType.INVALID_TRANSITION);
        assertThat(run().getStatus()).isEqualTo(SagaStatus.COMPENSATED);
    }

    // ── 8. terminal state semantics are untouched ─────────────────────────────

    /**
     * An event that really happened after the run ended is still reported as such. Making
     * the transition check abstain more often must not soften a different finding that
     * rests on different evidence.
     */
    @Test
    void anEventAfterATerminalStateIsStillReported() {
        observeHappyPath();
        observe("e9", "INVENTORY_FAILED", 9);

        assertThat(raised()).contains(AnomalyType.FORBIDDEN_POST_TERMINAL);
    }

    /** And one that merely arrived late still is not. */
    @Test
    void anEventThatOnlyArrivedLateIsStillNotReported() {
        observe("e1", "ORDER_CREATED", 0);
        observe("e3", "INVENTORY_RESERVED", 2);
        observe("e2", "PAYMENT_COMPLETED", 1);

        assertThat(raised()).doesNotContain(AnomalyType.FORBIDDEN_POST_TERMINAL);
    }

    // ── 9. F3: the post-terminal question is asked of the run, not of the arrival ──

    /**
     * The F3 counterexample. One run, three observations whose business order the clock
     * settles outright — {@code PAYMENT_FAILED} at t20 provably follows the ending at t10 —
     * and yet the finding used to depend on which of them was read last:
     *
     * <pre>
     *   arrival [e1, e2, e3] -> FORBIDDEN_POST_TERMINAL raised
     *   arrival [e1, e3, e2] -> silence
     * </pre>
     *
     * <p>{@code EvidenceTimeline} was order-independent throughout; the question put to it
     * was not. It was asked once, of the event in hand, so the terminal observation that
     * arrives <em>after</em> the late one was never matched against what it had already
     * ended. Same evidence, two verdicts — I9, and unlike {@code OUT_OF_ORDER} this finding
     * has no claim on delivery order to exempt it.
     */
    @Test
    void aPostTerminalEventIsFoundInEveryDeliveryOrder() {
        List<String[]> run = List.of(
                new String[]{"e1", "ORDER_CREATED", "0"},
                new String[]{"e2", "INVENTORY_RESERVED", "10"},
                new String[]{"e3", "PAYMENT_FAILED", "20"});

        for (List<String[]> order : permutations(run)) {
            setUp();
            order.forEach(e -> observe(e[0], e[1], Long.parseLong(e[2])));
            assertThat(raised())
                    .as("delivery order %s", order.stream().map(e -> e[1]).toList())
                    .contains(AnomalyType.FORBIDDEN_POST_TERMINAL);
        }
    }

    /**
     * And the other half of the same invariant: asking the question of the whole run must
     * not make it easier to answer yes. A run where nothing happened after the ending stays
     * silent in all six orders — including the three where the ending is read before the
     * observations the clock puts ahead of it.
     */
    @Test
    void noDeliveryOrderOfARunWithNoPostTerminalEventRaisesOne() {
        List<String[]> run = List.of(
                new String[]{"e1", "ORDER_CREATED", "0"},
                new String[]{"e2", "PAYMENT_COMPLETED", "1"},
                new String[]{"e3", "INVENTORY_RESERVED", "2"});

        for (List<String[]> order : permutations(run)) {
            setUp();
            order.forEach(e -> observe(e[0], e[1], Long.parseLong(e[2])));
            assertThat(raised())
                    .as("delivery order %s", order.stream().map(e -> e[1]).toList())
                    .doesNotContain(AnomalyType.FORBIDDEN_POST_TERMINAL);
        }
    }

    /**
     * What F3 buys, and what it does not, when more than one pair qualifies.
     *
     * <p>Five observations with three provably post-terminal pairs — (e3,e4), (e3,e5) and
     * (e4,e5). The finding's <em>existence</em> is now a property of the evidence: it is
     * raised in all 120 delivery orders. That is the invariant the fix was for.
     */
    @Test
    void aPostTerminalFindingIsRaisedInEveryOrderWhenSeveralPairsQualify() {
        assertThat(postTerminalShapesOverEveryOrderOf(severalQualifyingPairs()))
                .as("orders that raised no finding")
                .doesNotContainKey(NO_FINDING);
    }

    /** F9: the cited pair and its business time must also be independent of delivery. */
    @Test
    void everyDeliveryOrderCitesTheSamePostTerminalPair() {
        assertThat(postTerminalShapesOverEveryOrderOf(severalQualifyingPairs()))
                .containsOnlyKeys("[e3, e4] @" + T0.plusSeconds(3));
    }

    @Test
    void aLateEarlierEndingRevisesTheSameOccurrenceAndItsWholeExplanation() {
        observe("e1", "ORDER_CREATED", 0);
        observe("e4", "PAYMENT_FAILED", 3);
        observe("e5", "INVENTORY_FAILED", 4);
        Anomaly finding = postTerminalFinding();
        Long id = finding.getId();
        assertThat(finding.getEvidenceEventIds()).containsExactly("e4", "e5");
        finding.setDetectedAt(T0); // Earlier analysis/detection, without a wall-clock sleep.

        observe("e3", "INVENTORY_RESERVED", 2);

        assertThat(postTerminalFinding()).isSameAs(finding);
        assertThat(finding.getId()).isEqualTo(id);
        assertThat(finding.getEvidenceEventIds()).containsExactly("e3", "e4");
        assertThat(finding.getMeasuredAt()).isEqualTo(T0.plusSeconds(3));
        assertThat(finding.getDescription()).isEqualTo(
                "Event 'PAYMENT_FAILED' at 2026-01-01T00:00:03Z happened after "
                        + "'INVENTORY_RESERVED' ended the saga as COMPLETED at 2026-01-01T00:00:02Z");
        assertThat(finding.getObservedValue()).isEqualTo("PAYMENT_FAILED after terminal status COMPLETED");
        assertThat(finding.getExpectedValue()).isEqualTo("no events after terminal saga status");
        assertThat(finding.getThresholdSource()).isEqualTo("code:terminal-status-guard");
        assertThat(finding.getDetectedAt()).isAfter(T0);
        assertThat(finding.isRetracted()).isFalse();
    }

    @Test
    void anEarlierPostTerminalObservationRevisesTheMeasurementToo() {
        observe("e1", "ORDER_CREATED", 0);
        observe("e3", "INVENTORY_RESERVED", 2);
        observe("e5", "INVENTORY_FAILED", 4);
        Anomaly finding = postTerminalFinding();
        assertThat(finding.getEvidenceEventIds()).containsExactly("e3", "e5");

        observe("e4", "PAYMENT_FAILED", 3);

        assertThat(postTerminalFinding()).isSameAs(finding);
        assertThat(finding.getEvidenceEventIds()).containsExactly("e3", "e4");
        assertThat(finding.getMeasuredAt()).isEqualTo(T0.plusSeconds(3));
    }

    @Test
    void anUnchangedPairAndADuplicateDoNotRewriteOrRedetectTheFinding() {
        observe("e1", "ORDER_CREATED", 0);
        observe("e3", "INVENTORY_RESERVED", 2);
        observe("e4", "PAYMENT_FAILED", 3);
        Anomaly finding = postTerminalFinding();
        Instant detected = finding.getDetectedAt();
        clearInvocations(anomalyRepository);

        observe("e2", "PAYMENT_COMPLETED", 1); // More evidence, same canonical pair.
        observe("e4", "PAYMENT_FAILED", 3); // Duplicate observation.

        assertThat(postTerminalFinding()).isSameAs(finding);
        assertThat(finding.getDetectedAt()).isEqualTo(detected);
        assertThat(events).hasSize(4);
        verify(anomalyRepository, never()).save(finding);
    }

    @Test
    void tiedEndingsChooseADeterministicCitationWithoutClaimingOrderBetweenThem() {
        List<String[]> run = List.of(
                new String[]{"e1", "ORDER_CREATED", "0"},
                new String[]{"e3b", "INVENTORY_RESERVED", "2"},
                new String[]{"e3a", "INVENTORY_RESERVED", "2"},
                new String[]{"e4", "PAYMENT_FAILED", "3"});
        assertThat(postTerminalShapesOverEveryOrderOf(run))
                .containsOnlyKeys("[e3a, e4] @" + T0.plusSeconds(3));

        assertThat(postTerminalShapesOverEveryOrderOf(run.subList(0, 3)))
                .containsOnlyKeys(NO_FINDING);
    }

    /**
     * Holds the competing evidence fixed to isolate F9 from other detectors' citations.
     * Both R03 and R04 are STRONG; an old (e4,e5) citation incorrectly anchors R04 at
     * payment, before the competing inventory finding. The canonical pair anchors it at
     * inventory, where R03's higher severity wins.
     */
    @Test
    void thePostTerminalCitationGivesTheSameRcaAnchorRankingAndPrimaryInEveryOrder() {
        RuleRegistry rules = new RuleRegistry();
        rules.load();
        for (List<String[]> order : permutations(severalQualifyingPairs())) {
            setUp();
            order.forEach(e -> observe(e[0], e[1], Long.parseLong(e[2])));
            Anomaly conflict = new Anomaly();
            conflict.setSagaId(RUN.canonical());
            conflict.setAnomalyType(AnomalyType.MULTI_TERMINAL_CONFLICT);
            conflict.setDescription("COMPLETED and FAILED terminal observations");
            conflict.setEvidenceEventIds(List.of("e3", "e5"));
            conflict.setMeasuredAt(T0.plusSeconds(4));
            when(anomalyRepository.findStandingBySagaId(RUN.canonical()))
                    .thenReturn(List.of(postTerminalFinding(), conflict));
            RcaService rca = rcaService(rules, mock(RcaResultRepository.class), null);

            RcaResult result = rca.analyze(RUN.canonical());

            RcaCandidate candidate = result.getCandidates().stream()
                    .filter(c -> "R04".equals(c.getRuleId())).findFirst().orElseThrow();
            assertThat(candidate.getAnchorAt()).isEqualTo(T0.plusSeconds(2));
            assertThat(candidate.getAnchorStepId()).isEqualTo("RESERVE_INVENTORY");
            assertThat(result.getCandidates().stream().map(RcaCandidate::getRuleId).toList())
                    .containsSubsequence("R03", "R04");
            assertThat(result.getRuleId()).isEqualTo("R03");
        }
    }

    @Test
    void revisedEvidenceMakesTheExistingAutoRcaSweepReanalyzeButAnUnchangedPairDoesNot() {
        observe("e1", "ORDER_CREATED", 0);
        observe("e4", "PAYMENT_FAILED", 3);
        observe("e5", "INVENTORY_FAILED", 4);
        Anomaly finding = postTerminalFinding();
        finding.setDetectedAt(T0);
        RcaResult previous = new RcaResult();
        previous.setCreatedAt(T0.plusSeconds(10));
        RcaResultRepository results = mock(RcaResultRepository.class);
        when(results.findBySagaIdOrderByCreatedAtDesc(RUN.canonical())).thenReturn(List.of(previous));
        when(anomalyRepository.findSagaIdsWithAnomaliesSince(any())).thenAnswer(call ->
                finding.getDetectedAt().isBefore(call.getArgument(0)) ? List.of() : List.of(RUN.canonical()));
        when(anomalyRepository.findLatestDetectedAtBySagaId(RUN.canonical()))
                .thenAnswer(call -> finding.getDetectedAt());
        RcaService analysis = mock(RcaService.class);
        RcaService sweep = rcaService(new RuleRegistry(), results, analysis);

        sweep.autoAnalyzeNewlyDetectedAnomalies();
        verify(analysis, never()).analyze(anyString());
        observe("e3", "INVENTORY_RESERVED", 2);
        sweep.autoAnalyzeNewlyDetectedAnomalies();
        verify(analysis).analyze(RUN.canonical());

        previous.setCreatedAt(finding.getDetectedAt().plusSeconds(1));
        clearInvocations(analysis);
        observe("e2", "PAYMENT_COMPLETED", 1);
        sweep.autoAnalyzeNewlyDetectedAnomalies();
        verify(analysis, never()).analyze(anyString());
    }

    private RcaService rcaService(RuleRegistry rules, RcaResultRepository results, RcaService self) {
        return new RcaService(sagaInstanceRepository, sagaEventRepository, anomalyRepository,
                results, registry, new ObjectMapper(), new RcaSelector(), rules,
                new DetectionProperties(), alert -> { }, new ExecutionProjectionService(),
                new FailureSignalRegistry(""), self);
    }

    private Anomaly postTerminalFinding() {
        List<Anomaly> findings = anomalies.stream()
                .filter(a -> a.getAnomalyType() == AnomalyType.FORBIDDEN_POST_TERMINAL).toList();
        assertThat(findings).hasSize(1);
        return findings.get(0);
    }

    private static final String NO_FINDING = "NONE";

    private static List<String[]> severalQualifyingPairs() {
        return List.of(
                new String[]{"e1", "ORDER_CREATED", "0"},
                new String[]{"e2", "PAYMENT_COMPLETED", "1"},
                new String[]{"e3", "INVENTORY_RESERVED", "2"},
                new String[]{"e4", "PAYMENT_FAILED", "3"},
                new String[]{"e5", "INVENTORY_FAILED", "4"});
    }

    /** Every delivery order of {@code run}, against what its post-terminal finding cites. */
    private Map<String, List<String>> postTerminalShapesOverEveryOrderOf(List<String[]> run) {
        Map<String, List<String>> shapes = new LinkedHashMap<>();
        for (List<String[]> order : permutations(run)) {
            setUp();
            order.forEach(e -> observe(e[0], e[1], Long.parseLong(e[2])));
            String shape = anomalies.stream()
                    .filter(a -> a.getAnomalyType() == AnomalyType.FORBIDDEN_POST_TERMINAL)
                    .findFirst()
                    .map(a -> a.getEvidenceEventIds() + " @" + a.getMeasuredAt())
                    .orElse(NO_FINDING);
            shapes.computeIfAbsent(shape, key -> new ArrayList<>())
                    .add(order.stream().map(e -> e[0]).toList().toString());
        }
        return shapes;
    }

    // ── the specification saying nothing is still its own answer ──────────────

    /** A step the graph does not mention cannot be judged against it. */
    @Test
    void aStepTheGraphDoesNotMentionIsNotJudged() {
        assertThat(orderSaga.judgeTransition(
                "A_STEP_NO_SPEC_DECLARES", "PROCESS_PAYMENT", WorkflowSpec.SUCCESS_OUTCOME))
                .isEqualTo(TransitionVerdict.UNKNOWN);
    }

    /** Nor are two observations of one step's own lifecycle a transition at all. */
    @Test
    void aStepHandingOffToItselfIsNotATransition() {
        assertThat(orderSaga.judgeTransition(
                "RESERVE_INVENTORY", "RESERVE_INVENTORY", WorkflowSpec.SUCCESS_OUTCOME))
                .isEqualTo(TransitionVerdict.UNKNOWN);
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private SagaInstance run() {
        return instances.get(RUN.canonical());
    }

    private List<AnomalyType> raised() {
        return anomalies.stream().map(Anomaly::getAnomalyType).toList();
    }

    private void observeHappyPath() {
        observe("e1", "ORDER_CREATED", 0);
        observe("e2", "PAYMENT_COMPLETED", 1);
        observe("e3", "INVENTORY_RESERVED", 2);
    }

    private static <T> List<List<T>> permutations(List<T> items) {
        if (items.isEmpty()) return List.of(List.of());
        List<List<T>> all = new ArrayList<>();
        for (int i = 0; i < items.size(); i++) {
            List<T> rest = new ArrayList<>(items);
            T head = rest.remove(i);
            for (List<T> tail : permutations(rest)) {
                List<T> one = new ArrayList<>();
                one.add(head);
                one.addAll(tail);
                all.add(one);
            }
        }
        return all;
    }

    private void observe(String eventId, String eventType, long secondsIn) {
        correlationService.processEvent(new IngestedEvent(
                new IngestedEvent.Identity(SOURCE, RUN, eventId, null,
                        CorrelationBasis.DECLARED_KEY),
                null,
                eventType,
                "UNKNOWN",
                "a-service",
                T0.plusSeconds(secondsIn),
                "{}",
                Instant.now(),
                IngestedEvent.Causality.NONE));
    }
}
