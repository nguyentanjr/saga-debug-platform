package com.example.correlation.ingestion;

import com.example.correlation.domain.IngestedEvent;
import com.example.correlation.messaging.EventTimeResolver;
import com.example.correlation.messaging.SagaEventConsumer;
import com.example.correlation.service.CorrelationService;
import com.example.correlation.service.DeduplicationCache;
import com.example.correlation.service.EventSchemaValidator;
import com.example.correlation.workflow.WorkflowRegistry;
import com.example.correlation.workflow.WorkflowSpec;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Reads an unmodified third-party system that publishes through the transactional outbox
 * pattern: <a href="https://github.com/semotpan/saga-orchestration">semotpan/saga-orchestration</a>,
 * a public reference implementation of saga orchestration for hotel reservations.
 *
 * <h2>What this test exists to prove, and what it exposed</h2>
 * {@link ForeignSystemIngestionTest} already showed that a system with its own field
 * names and its own vocabulary can be read. It shared one assumption with the platform,
 * though: that everything worth reading is somewhere <em>inside the document</em>.
 *
 * <p>This system breaks that. Debezium's {@code EventRouter} publishes the business
 * payload as the record value and puts the saga id in the record <b>key</b> and the
 * identity and type in <b>headers</b>. A room-booking result arrives as literally
 * {@code {"status":"BOOKED"}} — a body with no correlation key, no identity and no
 * timestamp in it at all.
 *
 * <p>Before source mappings could name the transport, this system could not be ingested
 * however its payload was mapped: every observation was rejected as not correlatable.
 * That is the limitation these tests were written against, and the {@code key} and
 * {@code header:NAME} forms are the answer to it.
 *
 * <h2>Cost of the integration</h2>
 * Three YAML files and no Java: two ingestion mappings and one workflow specification,
 * all under {@code src/test/resources}. Nothing in the observed project was changed.
 */
class OutboxPatternIngestionTest {

    private static final String ROOM_TOPIC = "room-booking.outbox.events";
    private static final String PAYMENT_TOPIC = "payment.outbox.events";

    /** The saga id, which this system carries as the Kafka record key. */
    private static final String SAGA_ID = "3fa85f64-5717-4562-b3fc-2c963f66afa6";

    private CorrelationService correlationService;
    private SagaEventConsumer consumer;

    @BeforeEach
    void setUp() {
        correlationService = mock(CorrelationService.class);

        SourceMappingRegistry registry = new SourceMappingRegistry();
        registry.load();

        consumer = new SagaEventConsumer(new EventIngestionService(
                correlationService,
                new ObjectMapper(),
                new EventSchemaValidator(),
                new DeduplicationCache(),
                new EventTimeResolver(),
                registry));
    }

    // ── the mappings are found ────────────────────────────────────────────────

    @Test
    void bothOutboxTopicsHaveTheirOwnMapping() {
        SourceMappingRegistry registry = new SourceMappingRegistry();
        registry.load();

        assertThat(registry.forTopic(ROOM_TOPIC).getName()).isEqualTo("hotel-saga-room-booking");
        assertThat(registry.forTopic(PAYMENT_TOPIC).getName()).isEqualTo("hotel-saga-payment");
    }

    // ── the point: fields outside the body ────────────────────────────────────

    @Test
    void theCorrelationKeyIsReadFromTheRecordKey() {
        // The body carries the outcome and nothing else. Without reading the key there is
        // no execution to attach this observation to.
        consumer.handleEvent(roomRecord("""
                {"status":"BOOKED"}""", "evt-room-1"));

        assertThat(captured().sagaId()).isEqualTo(SAGA_ID);
    }

    @Test
    void theIdentityIsReadFromAHeader() {
        consumer.handleEvent(roomRecord("""
                {"status":"BOOKED"}""", "evt-room-1"));

        // Not the partition-offset fallback: the outbox row's own id, so the same event
        // republished is recognised as the same event.
        assertThat(captured().eventId()).isEqualTo("evt-room-1");
    }

    @Test
    void theOutcomeIsReadFromTheBodyAndTranslated() {
        consumer.handleEvent(roomRecord("""
                {"status":"BOOKED"}""", "evt-room-1"));

        assertThat(captured().eventType()).isEqualTo("ROOM_BOOKED");
    }

    /**
     * The header says {@code RoomUpdated} for a booking and for a rejection alike, so
     * taking the type from there would collapse two different outcomes into one. Meaning
     * comes from the body; identity comes from the header. Each field is read from
     * wherever it actually is.
     */
    @Test
    void aRejectionIsDistinguishedFromABookingEvenThoughTheHeaderTypeIsIdentical() {
        consumer.handleEvent(roomRecord("""
                {"status":"REJECTED"}""", "evt-room-2"));

        assertThat(captured().eventType()).isEqualTo("ROOM_REJECTED");
    }

    // ── two sources, one field name, two meanings ─────────────────────────────

    /**
     * {@code status} is CANCELLED on both topics and means different things: the room
     * booking was released, or the payment was reversed. One shared mapping could not
     * translate it two ways, which is why each topic has its own.
     */
    @Test
    void theSameStatusValueOnTwoTopicsTranslatesToTwoDifferentEvents() {
        consumer.handleEvent(roomRecord("""
                {"status":"CANCELLED"}""", "evt-room-3"));
        assertThat(captured().eventType()).isEqualTo("ROOM_CANCELLED");

        setUp();
        consumer.handleEvent(paymentRecord("""
                {"status":"CANCELLED"}""", "evt-pay-3"));
        assertThat(captured().eventType()).isEqualTo("PAYMENT_REVERSED");
    }

    @Test
    void thePaymentTopicIsReadWithItsOwnVocabulary() {
        consumer.handleEvent(paymentRecord("""
                {"status":"FAILED"}""", "evt-pay-1"));

        IngestedEvent ingested = captured();
        assertThat(ingested.sagaId()).isEqualTo(SAGA_ID);
        assertThat(ingested.eventType()).isEqualTo("PAYMENT_REJECTED");
    }

    // ── nothing is guessed at ─────────────────────────────────────────────────

    @Test
    void anObservationWithNoKeyIsNotIngested() {
        // No key, and this source declares no payload path for the correlation key, so
        // there is nothing to attach it to and no field is invented in its place.
        ConsumerRecord<String, String> keyless = new ConsumerRecord<>(
                ROOM_TOPIC, 0, 0L, 0L, TimestampType.CREATE_TIME, 0, 0,
                null, """
                {"status":"BOOKED"}""", headers("evt-x"), Optional.empty());

        consumer.handleEvent(keyless);

        verify(correlationService, never()).processEvent(any(IngestedEvent.class));
    }

    // ── the workflow specification matches the observed vocabulary ────────────

    /**
     * The mappings and the specification have to agree, or observations classify into no
     * step and the engine reports an execution it cannot judge. Checking it here means a
     * rename on either side fails a test rather than producing quiet nonsense.
     */
    @Test
    void theWorkflowSpecificationRecognisesEveryMappedEventType() {
        WorkflowRegistry registry = new WorkflowRegistry();
        registry.load();
        WorkflowSpec spec = registry.byType("ROOM_RESERVATION").orElseThrow();

        assertThat(spec.classifyEvent("ROOM_BOOKED")).isPresent();
        assertThat(spec.classifyEvent("ROOM_REJECTED")).isPresent();
        assertThat(spec.classifyEvent("ROOM_CANCELLED")).isPresent();
        assertThat(spec.classifyEvent("PAYMENT_REQUESTED")).isPresent();
        assertThat(spec.classifyEvent("PAYMENT_TAKEN")).isPresent();
        assertThat(spec.classifyEvent("PAYMENT_REJECTED")).isPresent();
    }

    @Test
    void aRejectedPaymentRoutesToCompensationNotToOutrightFailure() {
        WorkflowRegistry registry = new WorkflowRegistry();
        registry.load();
        WorkflowSpec spec = registry.byType("ROOM_RESERVATION").orElseThrow();

        // A held room has to be released, which is the whole reason this saga has a
        // compensation step; a rejected booking has nothing to undo.
        assertThat(spec.classifyEvent("PAYMENT_REJECTED").flatMap(spec::statusAfter))
                .contains(com.example.correlation.domain.SagaStatus.COMPENSATING);
        assertThat(spec.classifyEvent("ROOM_REJECTED").flatMap(spec::statusAfter))
                .contains(com.example.correlation.domain.SagaStatus.FAILED);
    }

    @Test
    void releasingTheRoomResolvesTheSagaAsCompensated() {
        WorkflowRegistry registry = new WorkflowRegistry();
        registry.load();
        WorkflowSpec spec = registry.byType("ROOM_RESERVATION").orElseThrow();

        assertThat(spec.classifyEvent("ROOM_CANCELLED").flatMap(spec::statusAfter))
                .contains(com.example.correlation.domain.SagaStatus.COMPENSATED);
    }

    // ── fixtures ──────────────────────────────────────────────────────────────

    private ConsumerRecord<String, String> roomRecord(String body, String eventId) {
        return record(ROOM_TOPIC, body, eventId, "RoomUpdated");
    }

    private ConsumerRecord<String, String> paymentRecord(String body, String eventId) {
        return record(PAYMENT_TOPIC, body, eventId, "PaymentUpdated");
    }

    /** A record shaped the way Debezium's EventRouter publishes one. */
    private ConsumerRecord<String, String> record(String topic, String body,
            String eventId, String headerType) {
        var headers = new org.apache.kafka.common.header.internals.RecordHeaders();
        headers.add(new RecordHeader("id", eventId.getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader("eventType", headerType.getBytes(StandardCharsets.UTF_8)));

        return new ConsumerRecord<>(topic, 0, 0L, System.currentTimeMillis(),
                TimestampType.CREATE_TIME, 0, 0, SAGA_ID, body, headers, Optional.empty());
    }

    private static org.apache.kafka.common.header.Headers headers(String eventId) {
        var headers = new org.apache.kafka.common.header.internals.RecordHeaders();
        headers.add(new RecordHeader("id", eventId.getBytes(StandardCharsets.UTF_8)));
        return headers;
    }

    private IngestedEvent captured() {
        ArgumentCaptor<IngestedEvent> captor = ArgumentCaptor.forClass(IngestedEvent.class);
        verify(correlationService).processEvent(captor.capture());
        return captor.getValue();
    }
}
