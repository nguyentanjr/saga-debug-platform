package com.example.correlation.execution;

import com.example.correlation.domain.SagaEvent;
import com.example.correlation.domain.SagaStatus;
import com.example.correlation.workflow.WorkflowRegistry;
import com.example.correlation.workflow.WorkflowSpec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * F28: an event cannot have happened before the event that caused it.
 *
 * <p>Measured on the ablation run of 2026-10-04 (SC01 run 1, saga 2fadc104): the causation
 * ids say ORDER_CREATED caused PAYMENT_COMPLETED, which caused INVENTORY_RESERVED, but the
 * timestamps the services stamped themselves were 22.517s, 22.559s and 21.787s — the
 * reservation 0.77s before its own cause. Ordered by clock alone, the reservation (which
 * completes the run) came first, so the observer reported FORBIDDEN_POST_TERMINAL and
 * STATE_REGRESSION and promoted TERMINAL_STATE_OVERWRITE against a healthy saga.
 */
class CausalOrderTest {

    private static final Instant T = Instant.parse("2026-10-03T19:33:21Z");
    private WorkflowSpec workflow;

    @BeforeEach
    void load() {
        WorkflowRegistry registry = new WorkflowRegistry();
        registry.load();
        workflow = registry.byType("ORDER_SAGA").orElseThrow();
    }

    /** The three events of the measured run, with the stamps the services gave them. */
    private static List<SagaEvent> measuredRun() {
        return List.of(
                event("order", "ORDER_CREATED", null, 1517),
                event("payment", "PAYMENT_COMPLETED", "order", 1559),
                event("inventory", "INVENTORY_RESERVED", "payment", 787));
    }

    @Test
    void aReservationStampedBeforeItsCauseStillCompletesTheRun() {
        EvidenceTimeline timeline = EvidenceTimeline.of(workflow, measuredRun());

        assertThat(timeline.status()).isEqualTo(SagaStatus.COMPLETED);
        assertThat(timeline.postTerminal()).isEmpty();
        assertThat(timeline.regression()).isEmpty();
        assertThat(timeline.inBusinessOrder()).extracting(SagaEvent::getEventId)
                .containsExactly("order", "payment", "inventory");
    }

    /** Whatever order the events arrive in: the answer is a function of the evidence. */
    @Test
    void theAnswerDoesNotDependOnArrivalOrder() {
        List<SagaEvent> shuffled = new ArrayList<>(measuredRun());
        for (int i = 0; i < 6; i++) {
            Collections.rotate(shuffled, 1);
            if (i % 2 == 1) Collections.swap(shuffled, 0, 1);
            EvidenceTimeline timeline = EvidenceTimeline.of(workflow, shuffled);
            assertThat(timeline.status()).isEqualTo(SagaStatus.COMPLETED);
            assertThat(timeline.postTerminal()).isEmpty();
        }
    }

    /** The run cannot end before it started, nor last a negative time. */
    @Test
    void theRunEndsNoEarlierThanTheCauseOfItsEnding() {
        EvidenceTimeline timeline = EvidenceTimeline.of(workflow, measuredRun());

        Instant start = timeline.startedAt().orElseThrow();
        Instant end = timeline.endedAt().orElseThrow();
        assertThat(start).isEqualTo(T.plusMillis(1517));
        assertThat(end).isAfterOrEqualTo(start);
    }

    /** Without declared causation the clock is all there is: nothing changes for such sources. */
    @Test
    void withoutCausationTheClockStillDecides() {
        List<SagaEvent> unlinked = List.of(
                event("order", "ORDER_CREATED", null, 1517),
                event("payment", "PAYMENT_COMPLETED", null, 1559),
                event("inventory", "INVENTORY_RESERVED", null, 787));

        EvidenceTimeline timeline = EvidenceTimeline.of(workflow, unlinked);

        assertThat(timeline.inBusinessOrder()).extracting(SagaEvent::getEventId)
                .containsExactly("inventory", "order", "payment");
    }

    /**
     * A genuine late event is still reported: a payment caused by the order but stamped after
     * the reservation that ended the run, with no causal tie to that reservation.
     */
    @Test
    void causationDoesNotHideAnEventThatReallyCameAfterTheEnding() {
        List<SagaEvent> events = List.of(
                event("order", "ORDER_CREATED", null, 0),
                event("payment", "PAYMENT_COMPLETED", "order", 100),
                event("inventory", "INVENTORY_RESERVED", "payment", 200),
                event("late", "PAYMENT_COMPLETED", "order", 900));

        EvidenceTimeline timeline = EvidenceTimeline.of(workflow, events);

        assertThat(timeline.postTerminal()).isPresent();
        assertThat(timeline.postTerminal().get().late().getEventId()).isEqualTo("late");
    }

    /** A causation that points outside the events in hand, or at itself, orders nothing. */
    @Test
    void danglingOrSelfCausationIsIgnored() {
        List<SagaEvent> events = List.of(
                event("order", "ORDER_CREATED", "order", 1517),
                event("payment", "PAYMENT_COMPLETED", "elsewhere", 1559),
                event("inventory", "INVENTORY_RESERVED", null, 1600));

        EvidenceTimeline timeline = EvidenceTimeline.of(workflow, events);

        assertThat(timeline.status()).isEqualTo(SagaStatus.COMPLETED);
        assertThat(timeline.inBusinessOrder()).extracting(SagaEvent::getEventId)
                .containsExactly("order", "payment", "inventory");
    }

    private static SagaEvent event(String id, String type, String causedBy, long millis) {
        SagaEvent e = new SagaEvent();
        e.setSagaId("run");
        e.setEventId(id);
        e.setEventType(type);
        e.setCausationId(causedBy);
        e.setTimestamp(T.plusMillis(millis));
        e.setReceivedAt(T.plusMillis(millis));
        return e;
    }
}
