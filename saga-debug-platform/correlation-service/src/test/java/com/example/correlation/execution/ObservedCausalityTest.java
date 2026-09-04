package com.example.correlation.execution;

import com.example.correlation.domain.SagaEvent;
import com.example.correlation.execution.ExecutionProjection.Operation;
import com.example.correlation.execution.ExecutionProjection.StartProvenance;
import com.example.correlation.workflow.WorkflowSpec;
import com.example.correlation.workflow.WorkflowSpecTestFixture;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers what changes once producers report which event caused which.
 *
 * <p>Everything the platform knew about dependency previously came from the workflow
 * specification: step B follows step A because the spec declares that edge. That is a
 * statement about the design. It cannot tell "B happened because A finished" apart from
 * "B happened after A finished", and on a run where the spec and reality disagree it
 * follows the spec.
 *
 * <p>A reported {@code causationId} is a statement about the run. Where one exists the
 * projection uses it and says so; where none exists nothing changes, so an unmodified
 * producer keeps working.
 */
class ObservedCausalityTest {

    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    private final ExecutionProjectionService service = new ExecutionProjectionService();
    private final WorkflowSpec orderSaga = WorkflowSpecTestFixture.orderSaga();
    private final WorkflowSpec parallel =
            WorkflowSpecTestFixture.load("workflows/parallel-checkout-workflow.yaml");

    @Test
    void areportedCauseIsPreferredOverTheInferredPredecessor() {
        Operation payment = project(orderSaga,
                event("e1", "ORDER_CREATED", T0),
                causedBy(event("e2", "PAYMENT_COMPLETED", T0.plusSeconds(4)), "e1"))
                .operation("PROCESS_PAYMENT").orElseThrow();

        assertThat(payment.startProvenance()).isEqualTo(StartProvenance.OBSERVED_CAUSAL_LINK);
        assertThat(payment.startEventId()).isEqualTo("e1");
        assertThat(payment.duration().toSeconds()).isEqualTo(4);
    }

    @Test
    void withoutAReportedCauseTheInferenceIsUsedAndLabelledAsSuch() {
        Operation payment = project(orderSaga,
                event("e1", "ORDER_CREATED", T0),
                event("e2", "PAYMENT_COMPLETED", T0.plusSeconds(4)))
                .operation("PROCESS_PAYMENT").orElseThrow();

        assertThat(payment.startProvenance()).isEqualTo(StartProvenance.INFERRED_FROM_PREDECESSOR);
        assertThat(payment.duration().toSeconds()).isEqualTo(4);
    }

    @Test
    void parentSpanIdIsUsedWhenNoDirectCauseIsReported() {
        SagaEvent cause = event("e1", "ORDER_CREATED", T0);
        cause.setSpanId("aaaa1111");

        SagaEvent effect = event("e2", "PAYMENT_COMPLETED", T0.plusSeconds(4));
        effect.setParentSpanId("aaaa1111");

        Operation payment = project(orderSaga, cause, effect)
                .operation("PROCESS_PAYMENT").orElseThrow();

        assertThat(payment.startProvenance()).isEqualTo(StartProvenance.OBSERVED_CAUSAL_LINK);
        assertThat(payment.startEventId()).isEqualTo("e1");
    }

    @Test
    void aCauseOutsideThisExecutionResolvesToNothingRatherThanToAGuess() {
        Operation payment = project(orderSaga,
                event("e1", "ORDER_CREATED", T0),
                causedBy(event("e2", "PAYMENT_COMPLETED", T0.plusSeconds(4)), "from-another-saga"))
                .operation("PROCESS_PAYMENT").orElseThrow();

        // The reference could not be resolved, so the specification is used and the
        // provenance says so instead of claiming an observed link.
        assertThat(payment.startProvenance()).isEqualTo(StartProvenance.INFERRED_FROM_PREDECESSOR);
    }

    /**
     * The case the specification cannot decide on its own: which arm of a fan-out
     * actually triggered the work.
     */
    @Test
    void areportedCauseIdentifiesWhichArmTriggeredTheJoin() {
        ExecutionProjection projection = project(parallel,
                event("e1", "CART_OPENED", T0),
                event("e2", "SEATS_RESERVED", T0.plusSeconds(3)),
                event("e3", "FRAUD_CHECK_PASSED", T0.plusSeconds(8)),
                causedBy(event("e4", "ORDER_SETTLED", T0.plusSeconds(11)), "e2"));

        Operation settle = projection.operation("SETTLE_ORDER").orElseThrow();

        // Inferring from the join would anchor this at the later arm (+8s). The producer
        // says it was the seat reservation, so the reported link wins and the duration
        // reflects what happened rather than what the graph implies.
        assertThat(settle.startProvenance()).isEqualTo(StartProvenance.OBSERVED_CAUSAL_LINK);
        assertThat(settle.startEventId()).isEqualTo("e2");
        assertThat(settle.duration().toSeconds()).isEqualTo(8);
    }

    @Test
    void anEventCitingItselfIsIgnored() {
        Operation payment = project(orderSaga,
                event("e1", "ORDER_CREATED", T0),
                causedBy(event("e2", "PAYMENT_COMPLETED", T0.plusSeconds(4)), "e2"))
                .operation("PROCESS_PAYMENT").orElseThrow();

        assertThat(payment.startProvenance()).isEqualTo(StartProvenance.INFERRED_FROM_PREDECESSOR);
    }

    @Test
    void aMixOfReportedAndUnreportedLinksIsHandledPerOperation() {
        ExecutionProjection projection = project(orderSaga,
                event("e1", "ORDER_CREATED", T0),
                causedBy(event("e2", "PAYMENT_COMPLETED", T0.plusSeconds(4)), "e1"),
                event("e3", "INVENTORY_RESERVED", T0.plusSeconds(6)));

        assertThat(projection.operation("PROCESS_PAYMENT").orElseThrow().startProvenance())
                .isEqualTo(StartProvenance.OBSERVED_CAUSAL_LINK);
        assertThat(projection.operation("RESERVE_INVENTORY").orElseThrow().startProvenance())
                .isEqualTo(StartProvenance.INFERRED_FROM_PREDECESSOR);
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private ExecutionProjection project(WorkflowSpec spec, SagaEvent... events) {
        return service.project("saga-c1", List.of(events), spec);
    }

    private static SagaEvent causedBy(SagaEvent event, String causationId) {
        event.setCausationId(causationId);
        return event;
    }

    private static SagaEvent event(String eventId, String eventType, Instant timestamp) {
        SagaEvent event = new SagaEvent();
        event.setEventId(eventId);
        event.setSagaId("saga-c1");
        event.setEventType(eventType);
        event.setServiceName("test");
        event.setTimestamp(timestamp);
        event.setPayload("{}");
        event.setReceivedAt(timestamp);
        return event;
    }
}
