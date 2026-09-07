package com.example.correlation.ingestion;

import com.example.correlation.domain.IngestedEvent;
import com.example.correlation.messaging.EventTimeResolver;
import com.example.correlation.service.CorrelationService;
import com.example.correlation.service.DeduplicationCache;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * The shapes a real system publishes that a single dotted path into the body cannot read.
 *
 * <p>Each of these was a system the platform could not observe without asking its owners
 * to change what they emit — which is the one thing this platform is not allowed to ask
 * for. They are written as mappings rather than as Java because that is the claim: the
 * cost of reading a new system is a YAML file.
 */
class SourceMappingCoverageTest {

    private static final Instant TRANSPORT_TIME = Instant.parse("2026-01-01T12:00:00Z");

    private final CorrelationService correlationService = mock(CorrelationService.class);

    // ── one field, more than one place ────────────────────────────────────────

    /**
     * A system that moved a field between versions of its events and kept publishing
     * both. Declaring one path means choosing which half of the traffic to lose;
     * declaring both reads all of it.
     */
    @Test
    void aFieldIsFoundWhicheverOfItsDeclaredPlacesThisEventUsedIt() {
        String mapping = """
                name: versioned
                topicPattern: 'orders'
                fields:
                  eventId: id
                  executionId: [orderId, data.orderId]
                """;

        assertThat(ingest(mapping, "orders",
                """
                {"id":"e1","orderId":"ORD-1"}""").executionKey().rawId()).isEqualTo("ORD-1");

        assertThat(ingest(mapping, "orders",
                """
                {"id":"e2","data":{"orderId":"ORD-2"}}""").executionKey().rawId()).isEqualTo("ORD-2");
    }

    /** Order is the declaration's, so a source can say which place it prefers. */
    @Test
    void theFirstDeclaredPlaceThatHasAValueWins() {
        IngestedEvent ingested = ingest("""
                name: versioned
                topicPattern: 'orders'
                fields:
                  eventId: id
                  executionId: [orderId, data.orderId]
                """, "orders", """
                {"id":"e3","orderId":"TOP","data":{"orderId":"NESTED"}}""");

        assertThat(ingested.executionKey().rawId()).isEqualTo("TOP");
    }

    // ── the body is a string, not a document ──────────────────────────────────

    /**
     * The ordinary transactional outbox: the event body lives in a {@code text} column,
     * so Debezium publishes it as a JSON string and a path into it used to stop at the
     * quotation mark. Every such system was unreadable however it was mapped.
     */
    @Test
    void aBodyEmbeddedAsAJsonStringIsRead() {
        IngestedEvent ingested = ingest("""
                name: outbox
                topicPattern: 'outbox\\.events'
                fields:
                  eventId: id
                  executionId: payload.orderId
                  eventType: payload.status
                """, "outbox.events", """
                {"id":"e4","payload":"{\\"orderId\\":\\"ORD-9\\",\\"status\\":\\"SHIPPED\\"}"}""");

        assertThat(ingested.executionKey().rawId()).isEqualTo("ORD-9");
        assertThat(ingested.eventType()).isEqualTo("SHIPPED");
    }

    /**
     * Text that is only text stays text. The unwrap is attempted only where the mapping
     * asked to descend, so a string field read directly is never reinterpreted.
     */
    @Test
    void aStringFieldReadDirectlyIsNotReinterpretedAsADocument() {
        IngestedEvent ingested = ingest("""
                name: outbox
                topicPattern: 'outbox\\.events'
                fields:
                  eventId: id
                  executionId: note
                """, "outbox.events", """
                {"id":"e5","note":"{not really json"}""");

        assertThat(ingested.executionKey().rawId()).isEqualTo("{not really json");
    }

    // ── the type is the topic ─────────────────────────────────────────────────

    /**
     * One topic per event type, nothing in the body naming it. Common enough that the
     * fallback existed already — but it handed the raw topic name to the specification
     * untranslated, so it matched no declared step and the run could not be judged.
     */
    @Test
    void anEventTypeCarriedOnlyByTheTopicNameIsReadAndTranslated() {
        IngestedEvent ingested = ingest("""
                name: topic-per-type
                topicPattern: 'shop\\..*'
                fields:
                  eventId: id
                  executionId: orderId
                  eventType: source
                eventTypes:
                  "shop.order.placed": ORDER_CREATED
                """, "shop.order.placed", """
                {"id":"e6","orderId":"ORD-3"}""");

        assertThat(ingested.eventType()).isEqualTo("ORDER_CREATED");
    }

    /** The same translation applies when nothing is declared and the fallback is used. */
    @Test
    void theUndeclaredFallbackToTheSourceNameIsTranslatedToo() {
        IngestedEvent ingested = ingest("""
                name: topic-per-type
                topicPattern: 'shop\\..*'
                fields:
                  eventId: id
                  executionId: orderId
                  eventType: null
                eventTypes:
                  "shop.order.placed": ORDER_CREATED
                """, "shop.order.placed", """
                {"id":"e7","orderId":"ORD-4"}""");

        assertThat(ingested.eventType()).isEqualTo("ORDER_CREATED");
    }

    // ── a vocabulary that keeps growing ───────────────────────────────────────

    /**
     * A source that versions its event names would otherwise need a line per version,
     * and a new version published on a Friday would be an unrecognised event until
     * someone noticed and edited the file.
     */
    @Test
    void aVersionedEventNameIsTranslatedByPattern() {
        String mapping = """
                name: versioned-types
                topicPattern: 'orders'
                fields:
                  eventId: id
                  executionId: orderId
                  eventType: type
                eventTypes:
                  "order\\\\.created(\\\\.v[0-9]+)?": ORDER_CREATED
                """;

        assertThat(ingest(mapping, "orders", """
                {"id":"e8","orderId":"O","type":"order.created"}""").eventType())
                .isEqualTo("ORDER_CREATED");
        assertThat(ingest(mapping, "orders", """
                {"id":"e9","orderId":"O","type":"order.created.v3"}""").eventType())
                .isEqualTo("ORDER_CREATED");
    }

    /**
     * An exact key is never reinterpreted as a pattern. A source whose type names contain
     * regex metacharacters — a dot is one, and dots are everywhere in event names — must
     * keep meaning what it says.
     */
    @Test
    void anExactMatchWinsOverAPatternThatWouldAlsoMatch() {
        IngestedEvent ingested = ingest("""
                name: versioned-types
                topicPattern: 'orders'
                fields:
                  eventId: id
                  executionId: orderId
                  eventType: type
                eventTypes:
                  "order.created.v2": ORDER_CREATED_V2
                  "order\\\\.created.*": ORDER_CREATED
                """, "orders", """
                {"id":"e10","orderId":"O","type":"order.created.v2"}""");

        assertThat(ingested.eventType()).isEqualTo("ORDER_CREATED_V2");
    }

    // ── fields that are not in the body at all ────────────────────────────────

    /**
     * The event time in a header. The clock used to be the one field that could only be
     * read from inside the document, so a system that put it in a header had every
     * duration measured against broker arrival instead.
     */
    @Test
    void theEventTimeIsReadFromAHeader() {
        IngestedEvent ingested = ingest("""
                name: header-clock
                topicPattern: 'orders'
                fields:
                  eventId: id
                  executionId: orderId
                  eventTime: header:occurred-at
                """, "orders", """
                {"id":"e11","orderId":"O"}""",
                Map.of("occurred-at", "2026-01-01T09:30:00Z"));

        assertThat(ingested.eventTime()).isEqualTo(Instant.parse("2026-01-01T09:30:00Z"));
    }

    /** An epoch in a header is an epoch, whichever unit it is in. */
    @Test
    void anEpochEventTimeIsReadFromAHeader() {
        IngestedEvent ingested = ingest("""
                name: header-clock
                topicPattern: 'orders'
                fields:
                  eventId: id
                  executionId: orderId
                  eventTime: header:ts
                """, "orders", """
                {"id":"e12","orderId":"O"}""",
                Map.of("ts", "1767258000000"));

        assertThat(ingested.eventTime()).isEqualTo(Instant.ofEpochMilli(1767258000000L));
    }

    /**
     * The business key an operator searches by. It used to be read by looking for a
     * payload field named {@code orderId} — the name of this platform's own demo domain,
     * so every other system's saga list showed a blank column.
     */
    @Test
    void theBusinessKeyIsReadFromWhereTheSourceKeepsIt() {
        IngestedEvent ingested = ingest("""
                name: hotel
                topicPattern: 'bookings'
                fields:
                  eventId: id
                  executionId: sagaRef
                  businessKey: reservation.reference
                """, "bookings", """
                {"id":"e13","sagaRef":"S-1","reservation":{"reference":"BK-2026-001"}}""");

        assertThat(ingested.businessKey()).isEqualTo("BK-2026-001");
    }

    // ── a second identifier for the same run ──────────────────────────────────

    /**
     * A source whose services do not share one id declares the second one it carries.
     * What is done with it is {@link ExecutionAliasResolver}'s; that it is read from the
     * source's own path, like everything else, is this test's.
     */
    @Test
    void aSecondIdentifierIsReadFromWhereTheSourceDeclaresIt() {
        SourceMapping mapping = parse("""
                name: split-ids
                topicPattern: 'payments'
                fields:
                  eventId: id
                  executionId: paymentId
                  aliasKey: context.orderNumber
                """);

        RawObservation observation = new RawObservation("payments", "{}", TRANSPORT_TIME,
                null, null, Map.of());
        com.fasterxml.jackson.databind.JsonNode payload = payload("""
                {"id":"e15","paymentId":"PAY-7","context":{"orderNumber":"ORD-7"}}""");

        assertThat(mapping.executionId(payload, observation)).contains("PAY-7");
        assertThat(mapping.aliasKey(payload, observation)).contains("ORD-7");
    }

    /** A source declaring none carries none, rather than something guessed at. */
    @Test
    void aSourceThatDeclaresNoSecondIdentifierHasNone() {
        SourceMapping mapping = parse("""
                name: single-id
                topicPattern: 'payments'
                fields:
                  executionId: paymentId
                """);

        assertThat(mapping.aliasKey(payload("""
                {"paymentId":"PAY-7","orderId":"ORD-7"}"""), null)).isEmpty();
    }

    // ── nothing is invented ───────────────────────────────────────────────────

    /**
     * None of the forms above is a licence to guess. When every declared place is empty
     * the observation is still refused, because a wrong correlation key does not surface
     * as an error — it surfaces as a diagnosis that quietly makes no sense.
     */
    @Test
    void anObservationMatchingNoneOfTheDeclaredPlacesIsStillRefused() {
        ingestRaw("""
                name: versioned
                topicPattern: 'orders'
                fields:
                  eventId: id
                  executionId: [orderId, data.orderId]
                """, "orders", """
                {"id":"e14","somethingElse":"X"}""", Map.of());

        verify(correlationService, never()).processEvent(any(IngestedEvent.class));
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private IngestedEvent ingest(String mappingYaml, String sourceRef, String payload) {
        return ingest(mappingYaml, sourceRef, payload, Map.of());
    }

    private IngestedEvent ingest(String mappingYaml, String sourceRef, String payload,
            Map<String, String> headers) {
        ingestRaw(mappingYaml, sourceRef, payload, headers);

        ArgumentCaptor<IngestedEvent> captor = ArgumentCaptor.forClass(IngestedEvent.class);
        verify(correlationService).processEvent(captor.capture());
        return captor.getValue();
    }

    private void ingestRaw(String mappingYaml, String sourceRef, String payload,
            Map<String, String> headers) {
        // Several of these cases ingest twice to show one mapping reading two shapes,
        // so each ingestion is verified against a clean slate.
        org.mockito.Mockito.reset(correlationService);

        SourceMapping mapping = parse(mappingYaml);

        SourceMappingRegistry registry = new SourceMappingRegistry() {
            @Override
            public SourceMapping forTopic(String topic) {
                return mapping.matches(topic) ? mapping : SourceMapping.identity();
            }
        };

        new EventIngestionService(
                correlationService,
                new ObjectMapper(),
                new SourceCompletenessCheck(),
                new DeduplicationCache(),
                new EventTimeResolver(),
                registry,
                new PassthroughAliasResolver(),
                new com.example.correlation.ingestion.InlineTransactionManager())
                .ingest(new RawObservation(sourceRef, payload, TRANSPORT_TIME,
                        "fallback-id", null, headers));
    }

    private static com.fasterxml.jackson.databind.JsonNode payload(String json) {
        try {
            return new ObjectMapper().readTree(json);
        } catch (Exception e) {
            throw new IllegalArgumentException("unreadable test payload", e);
        }
    }

    private static SourceMapping parse(String yaml) {
        try {
            return new ObjectMapper(new YAMLFactory()).readValue(yaml, SourceMapping.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("unreadable test mapping", e);
        }
    }
}
