package com.example.correlation.messaging;

import com.example.correlation.domain.IngestedEvent;
import com.example.correlation.ingestion.EventIngestionService;
import com.example.correlation.ingestion.SourceMappingRegistry;
import com.example.correlation.service.CorrelationService;
import com.example.correlation.service.DeduplicationCache;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.dao.DataAccessResourceFailureException;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Covers how the consumer treats an event it failed to store.
 *
 * <p>The behaviour under test is the one that used to lose events: the cache recorded
 * an id before persistence was attempted and the listener returned normally on failure,
 * so the offset advanced and the redelivery was rejected by the cache.
 */
class SagaEventConsumerTest {

    private static final String TOPIC = "payment-completed";
    private static final Instant EVENT_TIME = Instant.parse("2026-01-01T00:00:00Z");

    private CorrelationService correlationService;
    private DeduplicationCache deduplicationCache;
    private SagaEventConsumer consumer;

    @BeforeEach
    void setUp() {
        correlationService = mock(CorrelationService.class);
        deduplicationCache = new DeduplicationCache();
        consumer = new SagaEventConsumer(new EventIngestionService(
                correlationService,
                new ObjectMapper(),
                new com.example.correlation.ingestion.SourceCompletenessCheck(),
                deduplicationCache,
                new EventTimeResolver(),
                loadedMappings(),
                new com.example.correlation.ingestion.PassthroughAliasResolver(),
                new com.example.correlation.ingestion.InlineTransactionManager()));
    }

    private static SourceMappingRegistry loadedMappings() {
        SourceMappingRegistry registry = new SourceMappingRegistry();
        registry.load();
        return registry;
    }

    /**
     * The deduplication key is namespaced by source, not the bare event id. Two observed
     * systems both numbering their events from one would otherwise have the second
     * system's events discarded as duplicates of the first system's.
     */
    private static String dedupKey(String eventId) {
        return com.example.correlation.domain.ExecutionKey.DEFAULT_SOURCE
                + com.example.correlation.domain.ExecutionKey.SEPARATOR + eventId;
    }

    @Test
    void marksTheEventOnlyAfterItHasBeenProcessed() {
        consumer.handleEvent(record("e1"));

        IngestedEvent ingested = captured();
        assertThat(ingested.eventId()).isEqualTo("e1");
        assertThat(ingested.executionKey().rawId()).isEqualTo("saga-1");
        assertThat(ingested.eventType()).isEqualTo("PAYMENT_COMPLETED");
        assertThat(ingested.eventTime()).isEqualTo(EVENT_TIME);
        // Not declared in this envelope and no longer guessed from the topic name.
        assertThat(ingested.stepName()).isEqualTo("UNKNOWN");
        assertThat(ingested.serviceName()).isEqualTo("unknown");
        assertThat(deduplicationCache.seen(dedupKey("e1"))).isTrue();
    }

    @Test
    void aFailedEventIsNotMarkedAsProcessed() {
        doThrow(new DataAccessResourceFailureException("database down"))
                .when(correlationService).processEvent(any(IngestedEvent.class));

        assertThatThrownBy(() -> consumer.handleEvent(record("e1")))
                .isInstanceOf(DataAccessResourceFailureException.class);

        assertThat(deduplicationCache.seen(dedupKey("e1"))).isFalse();
    }

    @Test
    void redeliveryAfterAFailureIsProcessedAgain() {
        doThrow(new DataAccessResourceFailureException("database down"))
                .when(correlationService).processEvent(any(IngestedEvent.class));

        assertThatThrownBy(() -> consumer.handleEvent(record("e1")))
                .isInstanceOf(DataAccessResourceFailureException.class);

        // Database recovers; the container redelivers the same record.
        org.mockito.Mockito.reset(correlationService);
        consumer.handleEvent(record("e1"));

        verify(correlationService, times(1)).processEvent(any(IngestedEvent.class));
        assertThat(deduplicationCache.seen(dedupKey("e1"))).isTrue();
    }

    @Test
    void aRedeliveryOfAnAlreadyStoredEventIsSkipped() {
        consumer.handleEvent(record("e1"));
        consumer.handleEvent(record("e1"));

        verify(correlationService, times(1)).processEvent(any(IngestedEvent.class));
    }

    @Test
    void theEventDescribesItselfRatherThanBeingTypedByItsTopic() {
        // Arrives on a topic this consumer has no mapping for; everything needed is in
        // the envelope. Previously the event would have been typed "parcel-dispatched"
        // after the topic and attributed to service "unknown".
        String value = "{\"eventId\":\"n1\","
                + "\"eventType\":\"PARCEL_DISPATCHED\","
                + "\"stepName\":\"DISPATCH_PARCEL\","
                + "\"serviceName\":\"courier-service\","
                + "\"sagaId\":\"saga-9\","
                + "\"timestamp\":\"2026-01-01T00:00:00Z\"}";
        consumer.handleEvent(new ConsumerRecord<>("parcel-dispatched", 0, 0L, "key", value));

        IngestedEvent ingested = captured();
        assertThat(ingested.eventId()).isEqualTo("n1");
        assertThat(ingested.executionKey().rawId()).isEqualTo("saga-9");
        assertThat(ingested.eventType()).isEqualTo("PARCEL_DISPATCHED");
        assertThat(ingested.stepName()).isEqualTo("DISPATCH_PARCEL");
        assertThat(ingested.serviceName()).isEqualTo("courier-service");
    }

    @Test
    void nothingIsInferredFromTheTopicName() {
        // Three Java maps used to translate a topic name into an event type, a step and a
        // service. They were the last place the platform assumed it knew a particular
        // system's naming, and a topic they did not list produced an event typed after
        // the topic and attributed to service "unknown" regardless of its contents.
        //
        // An envelope that does not describe itself now reports what is missing instead
        // of being completed from a table of one deployment's topic names. Where a source
        // genuinely cannot self-describe, a mapping under ingestion/*.yaml says where its
        // fields live — declaration rather than code.
        String value = "{\"eventId\":\"o1\",\"sagaId\":\"saga-8\","
                + "\"timestamp\":\"2026-01-01T00:00:00Z\"}";
        consumer.handleEvent(new ConsumerRecord<>(TOPIC, 0, 0L, "key", value));

        IngestedEvent ingested = captured();
        assertThat(ingested.eventId()).isEqualTo("o1");
        assertThat(ingested.executionKey().rawId()).isEqualTo("saga-8");
        assertThat(ingested.stepName()).isEqualTo("UNKNOWN");
        assertThat(ingested.serviceName()).isEqualTo("unknown");
    }

    @Test
    void anUnreadableRecordIsDroppedWithoutFailingTheListener() {
        ConsumerRecord<String, String> broken =
                new ConsumerRecord<>(TOPIC, 0, 0L, "key", "{not json");

        assertThatCode(() -> consumer.handleEvent(broken)).doesNotThrowAnyException();
        verify(correlationService, never()).processEvent(any(IngestedEvent.class));
    }

    @Test
    void aRecordWithNoCorrelationKeyIsDroppedWithoutFailingTheListener() {
        ConsumerRecord<String, String> uncorrelated = new ConsumerRecord<>(
                TOPIC, 0, 0L, "key",
                "{\"eventId\":\"e9\",\"timestamp\":\"2026-01-01T00:00:00Z\"}");

        assertThatCode(() -> consumer.handleEvent(uncorrelated)).doesNotThrowAnyException();
        verify(correlationService, never()).processEvent(any(IngestedEvent.class));
    }

    @Test
    void causalMetadataOnTheEnvelopeIsCarriedThrough() {
        String value = "{\"eventId\":\"c2\","
                + "\"eventType\":\"PAYMENT_COMPLETED\","
                + "\"sagaId\":\"saga-1\","
                + "\"causationId\":\"c1\","
                + "\"spanId\":\"bbbb2222\","
                + "\"parentSpanId\":\"aaaa1111\","
                + "\"timestamp\":\"2026-01-01T00:00:00Z\"}";
        consumer.handleEvent(new ConsumerRecord<>(TOPIC, 0, 0L, "key", value));

        IngestedEvent ingested = captured();
        assertThat(ingested.causationId()).isEqualTo("c1");
        assertThat(ingested.spanId()).isEqualTo("bbbb2222");
        assertThat(ingested.parentSpanId()).isEqualTo("aaaa1111");
        assertThat(ingested.hasCausalLink()).isTrue();
    }

    @Test
    void anEnvelopeWithoutCausalMetadataStillCorrelates() {
        consumer.handleEvent(record("e1"));

        IngestedEvent ingested = captured();
        assertThat(ingested.causationId()).isNull();
        assertThat(ingested.parentSpanId()).isNull();
        assertThat(ingested.hasCausalLink()).isFalse();
    }

    private IngestedEvent captured() {
        ArgumentCaptor<IngestedEvent> captor = ArgumentCaptor.forClass(IngestedEvent.class);
        verify(correlationService).processEvent(captor.capture());
        return captor.getValue();
    }

    private static ConsumerRecord<String, String> record(String eventId) {
        String value = "{\"eventId\":\"" + eventId + "\","
                + "\"eventType\":\"PAYMENT_COMPLETED\","
                + "\"sagaId\":\"saga-1\","
                + "\"timestamp\":\"2026-01-01T00:00:00Z\"}";
        // Transport time deliberately differs from the event's own timestamp.
        return new ConsumerRecord<>(TOPIC, 0, 0L,
                Instant.parse("2026-01-01T00:05:00Z").toEpochMilli(),
                org.apache.kafka.common.record.TimestampType.CREATE_TIME,
                0, 0, "key", value, new org.apache.kafka.common.header.internals.RecordHeaders(), null);
    }
}
