package com.example.correlation.execution;

import com.example.correlation.domain.SagaEvent;
import com.example.correlation.domain.SagaStatus;
import com.example.correlation.execution.ExecutionProjection.Operation;
import com.example.correlation.execution.ExecutionProjection.StartProvenance;
import com.example.correlation.workflow.WorkflowSpec;
import com.example.correlation.workflow.WorkflowSpecTestFixture;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers a workflow whose steps genuinely run in parallel.
 *
 * <p>Two shapes are exercised: a fan-out, where one completion enables two steps, and a
 * fan-in, where a step waits for both. Both used to be mishandled — the fan-out by
 * picking whichever successor appeared first in the YAML, the fan-in by leaving the step
 * behind it unmeasurable.
 */
class ParallelExecutionTest {

    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    private final ExecutionProjectionService service = new ExecutionProjectionService();
    private final WorkflowSpec spec =
            WorkflowSpecTestFixture.load("workflows/parallel-checkout-workflow.yaml");

    // ── Fan-out ───────────────────────────────────────────────────────────────

    @Test
    void aFanOutCompletionMeansTheExecutionContinues() {
        // OPEN_CART has two SUCCESS successors. Both mean "more work follows", so the
        // status they imply is unambiguous even though the target is not.
        assertThat(statusAfter("CART_OPENED")).contains(SagaStatus.IN_PROGRESS);
    }

    @Test
    void bothArmsOfTheFanOutAreMeasuredFromTheSameCompletion() {
        ExecutionProjection projection = project(
                event("e1", "CART_OPENED", T0),
                event("e2", "FRAUD_CHECK_PASSED", T0.plusSeconds(8)),
                event("e3", "SEATS_RESERVED", T0.plusSeconds(3)));

        Operation fraud = projection.operation("RUN_FRAUD_CHECK").orElseThrow();
        Operation seats = projection.operation("RESERVE_SEATS").orElseThrow();

        // Neither arm's duration is affected by the other finishing first or last.
        assertThat(fraud.duration().toSeconds()).isEqualTo(8);
        assertThat(seats.duration().toSeconds()).isEqualTo(3);
        assertThat(fraud.startEventId()).isEqualTo("e1");
        assertThat(seats.startEventId()).isEqualTo("e1");
    }

    @Test
    void oneArmFinishingLateDoesNotChangeTheOtherArmsDuration() {
        Operation fastRun = project(
                event("e1", "CART_OPENED", T0),
                event("e2", "FRAUD_CHECK_PASSED", T0.plusSeconds(8)),
                event("e3", "SEATS_RESERVED", T0.plusSeconds(3)))
                .operation("RUN_FRAUD_CHECK").orElseThrow();

        Operation slowSiblingRun = project(
                event("e1", "CART_OPENED", T0),
                event("e2", "FRAUD_CHECK_PASSED", T0.plusSeconds(8)),
                event("e3", "SEATS_RESERVED", T0.plusSeconds(120)))
                .operation("RUN_FRAUD_CHECK").orElseThrow();

        assertThat(slowSiblingRun.duration()).isEqualTo(fastRun.duration());
    }

    // ── Fan-in ────────────────────────────────────────────────────────────────

    @Test
    void aJoinIsMeasuredFromTheLastArmToFinish() {
        Operation settle = project(
                event("e1", "CART_OPENED", T0),
                event("e2", "SEATS_RESERVED", T0.plusSeconds(3)),
                event("e3", "FRAUD_CHECK_PASSED", T0.plusSeconds(8)),
                event("e4", "ORDER_SETTLED", T0.plusSeconds(11)))
                .operation("SETTLE_ORDER").orElseThrow();

        // Settlement could not begin before the slower arm finished at +8s, so it took
        // 3s — not the 11s since the cart opened, and not the 8s since seats were held.
        assertThat(settle.startProvenance()).isEqualTo(StartProvenance.INFERRED_FROM_JOIN);
        assertThat(settle.startEventId()).isEqualTo("e3");
        assertThat(settle.duration().toSeconds()).isEqualTo(3);
    }

    @Test
    void aJoinWithAnUnobservedArmHasNoDuration() {
        // Only one of the two arms was seen. Measuring from it would report a duration
        // that silently assumes the missing arm finished earlier.
        Operation settle = project(
                event("e1", "CART_OPENED", T0),
                event("e2", "FRAUD_CHECK_PASSED", T0.plusSeconds(8)),
                event("e4", "ORDER_SETTLED", T0.plusSeconds(11)))
                .operation("SETTLE_ORDER").orElseThrow();

        assertThat(settle.startProvenance()).isEqualTo(StartProvenance.NO_START_REFERENCE);
        assertThat(settle.duration()).isNull();
    }

    @Test
    void bothArmsCountAsUpstreamOfTheJoin() {
        assertThat(spec.isUpstreamOf("RUN_FRAUD_CHECK", "SETTLE_ORDER")).isTrue();
        assertThat(spec.isUpstreamOf("RESERVE_SEATS", "SETTLE_ORDER")).isTrue();

        // Reachable through either arm, so still upstream.
        assertThat(spec.isUpstreamOf("OPEN_CART", "SETTLE_ORDER")).isTrue();

        // Siblings on separate arms are ordered in time but neither depends on the other.
        assertThat(spec.isUpstreamOf("RESERVE_SEATS", "RUN_FRAUD_CHECK")).isFalse();
        assertThat(spec.isUpstreamOf("RUN_FRAUD_CHECK", "RESERVE_SEATS")).isFalse();
    }

    @Test
    void independentArmsMakeTheirSuccessorAJoin() {
        // Neither arm can reach the other, so both have to run before settlement can.
        assertThat(spec.isJoin("SETTLE_ORDER")).isTrue();
        assertThat(spec.predecessorSteps("SETTLE_ORDER"))
                .containsExactlyInAnyOrder("RUN_FRAUD_CHECK", "RESERVE_SEATS");

        // A step with one predecessor is never a join.
        assertThat(spec.isJoin("RUN_FRAUD_CHECK")).isFalse();
    }

    // ── Ambiguity is reported, not resolved ───────────────────────────────────

    @Test
    void aStepWhoseSuccessorsDisagreeAboutTheOutcomeYieldsNoStatus() {
        // SETTLE_ORDER -> COMPLETED is the only SUCCESS target, so this workflow is
        // unambiguous. Add a second target implying a different status and the spec can
        // no longer say what success means.
        WorkflowSpec ambiguous = WorkflowSpecTestFixture.load(
                "workflows/parallel-checkout-workflow.yaml");
        WorkflowSpec.TransitionSpec extra = new WorkflowSpec.TransitionSpec();
        extra.setFrom("SETTLE_ORDER");
        extra.setTo("RUN_FRAUD_CHECK");
        extra.setCondition("SUCCESS");

        List<WorkflowSpec.TransitionSpec> transitions =
                new ArrayList<>(ambiguous.getTransitions());
        transitions.add(extra);
        ambiguous.setTransitions(transitions);

        assertThat(ambiguous.classifyEvent("ORDER_SETTLED").flatMap(ambiguous::statusAfter))
                .isEmpty();
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private java.util.Optional<SagaStatus> statusAfter(String eventType) {
        return spec.classifyEvent(eventType).flatMap(spec::statusAfter);
    }

    private ExecutionProjection project(SagaEvent... events) {
        return service.project("saga-p1", List.of(events), spec);
    }

    private static SagaEvent event(String eventId, String eventType, Instant timestamp) {
        SagaEvent event = new SagaEvent();
        event.setEventId(eventId);
        event.setSagaId("saga-p1");
        event.setEventType(eventType);
        event.setServiceName("test");
        event.setTimestamp(timestamp);
        event.setPayload("{}");
        event.setReceivedAt(timestamp);
        return event;
    }
}
