package com.example.correlation.ingestion;

import com.example.correlation.domain.IngestedEvent;
import com.example.correlation.messaging.EventTimeResolver;
import com.example.correlation.messaging.SagaEventConsumer;
import com.example.correlation.service.CorrelationService;
import com.example.correlation.service.DeduplicationCache;
import com.example.correlation.workflow.WorkflowRegistry;
import com.example.correlation.workflow.WorkflowSpec;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

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
 * YAML and no Java: four ingestion mappings and one workflow specification, read from
 * {@code declarations/} — the files the observer loads when it watches this system. They
 * used to be copies under {@code src/test/resources}, and the copies drifted: the workflow
 * lost its {@code started} events while the live one kept them, so these tests checked a
 * specification nobody ran (ROADMAP L13). Nothing in the observed project was changed.
 */
class OutboxPatternIngestionTest {

    private static final String ROOM_TOPIC = "room-booking.outbox.events";
    private static final String PAYMENT_TOPIC = "payment.outbox.events";
    private static final List<String> ALL_TOPICS = List.of(ROOM_TOPIC, PAYMENT_TOPIC,
            "room-booking.inbox.events", "payment.inbox.events");

    /** The saga id, which this system carries as the Kafka record key. */
    private static final String SAGA_ID = "3fa85f64-5717-4562-b3fc-2c963f66afa6";

    private CorrelationService correlationService;
    private SagaEventConsumer consumer;

    @BeforeEach
    void setUp() {
        correlationService = mock(CorrelationService.class);

        SourceMappingRegistry registry = declaredMappings();

        consumer = new SagaEventConsumer(new EventIngestionService(
                correlationService,
                new ObjectMapper(),
                new com.example.correlation.ingestion.SourceCompletenessCheck(),
                new DeduplicationCache(),
                new EventTimeResolver(),
                registry,
                new PassthroughAliasResolver(),
                new com.example.correlation.ingestion.InlineTransactionManager()));
    }

    // ── the mappings are found ────────────────────────────────────────────────

    @Test
    void bothOutboxTopicsHaveTheirOwnMapping() {
        SourceMappingRegistry registry = declaredMappings();

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

        assertThat(captured().executionKey().rawId()).isEqualTo(SAGA_ID);
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
        assertThat(ingested.executionKey().rawId()).isEqualTo(SAGA_ID);
        assertThat(ingested.eventType()).isEqualTo("PAYMENT_REJECTED");
    }

    /**
     * The observed system declares a {@code COMPLETED} payment state that its code never
     * reaches: {@code Payment.paymentStatus()} returns only REQUESTED, FAILED or
     * CANCELLED, and the orchestrator maps REQUESTED to SUCCEEDED. So the event meaning
     * "the payment step succeeded" is named REQUESTED.
     *
     * <p>Worth a test of its own because a specification written from that project's
     * README or from its enum — rather than from what its code emits — would declare
     * {@code completed: PAYMENT_TAKEN} and then wait forever for an event that is never
     * published. Every successful reservation would look unfinished. This is the gap
     * between a design and a run that the platform exists to surface, met here in the
     * platform's own integration work.
     */
    @Test
    void successIsSignalledByRequestedBecauseCompletedIsUnreachable() {
        consumer.handleEvent(paymentRecord("""
                {"status":"REQUESTED"}""", "evt-pay-2"));

        assertThat(captured().eventType()).isEqualTo("PAYMENT_ACCEPTED");
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
        WorkflowSpec spec = roomReservation();
        // Read from the mappings rather than listed here, so a name added or renamed on
        // either side is checked without anyone remembering to update this test.
        Set<String> mapped = new TreeSet<>();
        SourceMappingRegistry mappings = declaredMappings();
        for (String topic : ALL_TOPICS) {
            mapped.addAll(mappings.forTopic(topic).getEventTypes().values());
        }

        assertThat(mapped).contains("BOOK_ROOM_REQUESTED", "ROOM_BOOKED");
        assertThat(mapped).filteredOn(type -> !MAPPED_BUT_UNREACHABLE.contains(type))
                .allSatisfy(type -> assertThat(spec.classifyEvent(type)).as(type).isPresent());
    }

    /**
     * Mapped so that nothing arriving on these topics is left untranslated, and absent
     * from the workflow because the observed system never sends them: its orchestrator's
     * {@code goBack()} cancels only the step before the failing one, and payment is the
     * last step, so no payment is ever cancelled. Declaring them would describe a
     * compensation this system does not have. Pinned so that the day one of them joins the
     * workflow, this list is revisited rather than silently outgrown.
     */
    private static final Set<String> MAPPED_BUT_UNREACHABLE =
            Set.of("REFUND_PAYMENT_REQUESTED", "PAYMENT_REVERSED");

    @Test
    void theMappedButUnreachableNamesAreNotPartOfTheWorkflow() {
        WorkflowSpec spec = roomReservation();

        assertThat(MAPPED_BUT_UNREACHABLE)
                .allSatisfy(type -> assertThat(spec.classifyEvent(type)).as(type).isEmpty());
    }

    @Test
    void aRejectedPaymentRoutesToCompensationNotToOutrightFailure() {
        WorkflowSpec spec = roomReservation();

        // A held room has to be released, which is the whole reason this saga has a
        // compensation step; a rejected booking has nothing to undo.
        assertThat(spec.classifyEvent("PAYMENT_REJECTED").flatMap(spec::statusAfter))
                .contains(com.example.correlation.domain.SagaStatus.COMPENSATING);
        assertThat(spec.classifyEvent("ROOM_REJECTED").flatMap(spec::statusAfter))
                .contains(com.example.correlation.domain.SagaStatus.FAILED);
    }

    @Test
    void aPaymentAcceptedCompletesTheSaga() {
        WorkflowSpec spec = roomReservation();

        assertThat(spec.classifyEvent("PAYMENT_ACCEPTED").flatMap(spec::statusAfter))
                .contains(com.example.correlation.domain.SagaStatus.COMPLETED);
    }

    @Test
    void releasingTheRoomResolvesTheSagaAsCompensated() {
        WorkflowSpec spec = roomReservation();

        assertThat(spec.classifyEvent("ROOM_CANCELLED").flatMap(spec::statusAfter))
                .contains(com.example.correlation.domain.SagaStatus.COMPENSATED);
    }

    // ── fixtures ──────────────────────────────────────────────────────────────

    /** The repository's declarations directory, whether surefire runs from the module or the root. */
    private static String declarationsDirectory() {
        Path here = Path.of("").toAbsolutePath();
        Path root = Files.exists(here.resolve("declarations")) ? here : here.getParent();
        return root.resolve("declarations").toString();
    }

    /** Package-private: MultiMappingSystemIdentityTest reads the same files. */
    static SourceMappingRegistry declaredMappings() {
        SourceMappingRegistry registry = new SourceMappingRegistry();
        ReflectionTestUtils.setField(registry, "declarationsDirectory", declarationsDirectory());
        registry.load();
        return registry;
    }

    /** Its payment-service shares a name with the demo's; the declaration says whose (V21). */
    @org.junit.jupiter.api.Test
    void theHotelWorkflowDeclaresTheDeploymentItsServicesRunIn() {
        org.assertj.core.api.Assertions.assertThat(roomReservation().getRuntimeScope()).isEqualTo("hotel-real");
    }

    private static WorkflowSpec roomReservation() {
        WorkflowRegistry registry = new WorkflowRegistry();
        ReflectionTestUtils.setField(registry, "declarationsDirectory", declarationsDirectory());
        registry.load();
        return registry.byType("ROOM_RESERVATION").orElseThrow();
    }

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
