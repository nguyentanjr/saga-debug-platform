package com.example.saga.events;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers the helper every producer uses to answer "what caused this".
 *
 * <p>Each producer used to copy the saga id by hand and nothing else, so the stream
 * carried correlation but no causation: a consumer could group events into one execution
 * yet had to infer which of them led to which. Recording the link at the point of
 * production is what makes that inference unnecessary.
 */
class CausalChainTest {

    @Test
    void aResponseCarriesTheCorrelationKeyAndTheCauseItResponded() {
        OrderCreatedEvent cause = new OrderCreatedEvent();
        cause.setSagaId(UUID.randomUUID());
        cause.setSpanId("aaaa1111");

        PaymentCompletedEvent response = new PaymentCompletedEvent();
        EventTraceHelper.inResponseTo(response, cause);

        assertThat(response.getSagaId()).isEqualTo(cause.getSagaId());
        assertThat(response.getCausationId()).isEqualTo(cause.getEventId());
        assertThat(response.getParentSpanId()).isEqualTo("aaaa1111");
    }

    @Test
    void theChainSurvivesAcrossSeveralHops() {
        OrderCreatedEvent created = new OrderCreatedEvent();
        created.setSagaId(UUID.randomUUID());

        PaymentCompletedEvent paid = EventTraceHelper.inResponseTo(new PaymentCompletedEvent(), created);
        InventoryFailedEvent stockFailed = EventTraceHelper.inResponseTo(new InventoryFailedEvent(), paid);
        RefundPaymentCommand refund = EventTraceHelper.inResponseTo(new RefundPaymentCommand(), stockFailed);

        // Each hop names its immediate cause, so the whole chain can be walked back from
        // the last event without consulting a workflow specification.
        assertThat(refund.getCausationId()).isEqualTo(stockFailed.getEventId());
        assertThat(stockFailed.getCausationId()).isEqualTo(paid.getEventId());
        assertThat(paid.getCausationId()).isEqualTo(created.getEventId());
        assertThat(created.getCausationId()).isNull();

        assertThat(refund.getSagaId()).isEqualTo(created.getSagaId());
    }

    @Test
    void anEventThatStartsAnExecutionNamesNoCause() {
        OrderCreatedEvent created = new OrderCreatedEvent();

        assertThat(created.getCausationId()).isNull();
        assertThat(created.getParentSpanId()).isNull();
        assertThat(created.getEventId()).isNotBlank();
    }

    @Test
    void respondingToNothingLeavesTheEventUnchanged() {
        PaymentCompletedEvent event = new PaymentCompletedEvent();
        UUID sagaId = UUID.randomUUID();
        event.setSagaId(sagaId);

        EventTraceHelper.inResponseTo(event, null);

        assertThat(event.getSagaId()).isEqualTo(sagaId);
        assertThat(event.getCausationId()).isNull();
    }

    @Test
    void everyEventHasADistinctIdentitySoACauseCanBeNamed() {
        assertThat(new OrderCreatedEvent().getEventId())
                .isNotEqualTo(new OrderCreatedEvent().getEventId());
    }
}
