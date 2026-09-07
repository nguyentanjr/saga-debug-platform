package com.example.correlation.ingestion;

import com.example.correlation.domain.CorrelationBasis;
import com.example.correlation.domain.IngestedEvent;
import com.example.correlation.messaging.EventTimeResolver;
import com.example.correlation.service.CorrelationService;
import com.example.correlation.service.DeduplicationCache;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Two observed systems in one deployment, and the identifiers they happen to share.
 *
 * <h2>What went wrong before</h2>
 * Executions were keyed by the raw identifier a source published. Two systems are two id
 * spaces, so {@code ORD-A} in one and {@code ORD-A} in another were filed as one run. The
 * failure was not a crash or a rejected event — it was a fabricated execution whose events
 * interleaved two companies' systems, after which the detectors correctly reported a
 * terminal saga resuming, a state regression, an invalid transition and a step that took
 * twelve hours. Four high-severity findings, every one of them manufactured by the
 * observer and attributed to the observed.
 *
 * <p>The same held for event ids, more quietly: a globally unique {@code event_id} meant
 * the second system to publish an event called {@code 1} had it silently skipped as a
 * duplicate of the first system's.
 *
 * <p>These tests hold the boundary at the place it is decided, before anything is stored.
 */
class SourceScopedIdentityTest {

    private static final Instant TRANSPORT_TIME = Instant.parse("2026-01-01T12:00:00Z");

    /** Two unrelated systems that both number orders {@code ORD-A} and events {@code 1}. */
    private static final String SHOP = """
            name: shop
            topicPattern: 'shop\\.events'
            fields:
              eventId: id
              executionId: orderNo
              eventType: kind
            """;

    private static final String PARTNER = """
            name: partner
            topicPattern: 'partner\\.events'
            fields:
              eventId: id
              executionId: orderNo
              eventType: kind
            """;

    private CorrelationService correlationService;
    private DeduplicationCache deduplicationCache;
    private EventIngestionService ingestion;

    @BeforeEach
    void setUp() {
        correlationService = mock(CorrelationService.class);
        deduplicationCache = new DeduplicationCache();
        ingestion = new EventIngestionService(
                correlationService,
                new ObjectMapper(),
                new SourceCompletenessCheck(),
                deduplicationCache,
                new EventTimeResolver(),
                registryOf(SHOP, PARTNER),
                new PassthroughAliasResolver(),
                new com.example.correlation.ingestion.InlineTransactionManager());
    }

    // ── the same identifier in two systems ────────────────────────────────────

    @Test
    void twoSourcesUsingTheSameExecutionIdAreTwoExecutions() {
        ingest("shop.events", """
                {"id":"s1","orderNo":"ORD-A","kind":"ORDER_CREATED"}""");
        ingest("partner.events", """
                {"id":"p1","orderNo":"ORD-A","kind":"ORDER_CREATED"}""");

        List<IngestedEvent> ingested = allIngested();
        assertThat(ingested).hasSize(2);
        assertThat(ingested.get(0).sagaId()).isEqualTo("shop::ORD-A");
        assertThat(ingested.get(1).sagaId()).isEqualTo("partner::ORD-A");
        assertThat(ingested.get(0).sagaId()).isNotEqualTo(ingested.get(1).sagaId());
    }

    /**
     * Both halves of the identity are kept: the raw id is what the system published and
     * what an operator recognises, and it is never rewritten to make it unique.
     */
    @Test
    void theRawIdentifierIsPreservedAlongsideTheNamespace() {
        ingest("shop.events", """
                {"id":"s1","orderNo":"ORD-A","kind":"ORDER_CREATED"}""");

        IngestedEvent ingested = allIngested().get(0);
        assertThat(ingested.executionKey().source()).isEqualTo("shop");
        assertThat(ingested.executionKey().rawId()).isEqualTo("ORD-A");
        assertThat(ingested.sourceName()).isEqualTo("shop");
    }

    /**
     * An event id is unique only within the system that issued it. Deduplicating on the
     * bare value discarded the second system's events as redeliveries of the first's — an
     * accepted-looking outcome that lost data.
     */
    @Test
    void twoSourcesUsingTheSameEventIdAreTwoObservations() {
        ingest("shop.events", """
                {"id":"1","orderNo":"ORD-A","kind":"ORDER_CREATED"}""");
        ingest("partner.events", """
                {"id":"1","orderNo":"ORD-B","kind":"ORDER_CREATED"}""");

        assertThat(allIngested()).hasSize(2);
    }

    /**
     * A system whose run has already finished must not be disturbed by another system's
     * event that happens to carry the same string. The two never meet: they are different
     * executions before anything is loaded, so no status is recomputed and no detector
     * sees a terminal saga resuming.
     */
    @Test
    void anEventFromAnotherSourceNeverReachesAnAlreadyTerminalExecution() {
        ingest("shop.events", """
                {"id":"s9","orderNo":"ORD-DONE","kind":"ORDER_COMPLETED"}""");
        ingest("partner.events", """
                {"id":"p9","orderNo":"ORD-DONE","kind":"INVENTORY_FAILED"}""");

        List<IngestedEvent> ingested = allIngested();
        assertThat(ingested.get(0).sagaId()).isEqualTo("shop::ORD-DONE");
        assertThat(ingested.get(1).sagaId()).isEqualTo("partner::ORD-DONE");
    }

    // ── replay and ordering ───────────────────────────────────────────────────

    /** A redelivery of the same observation from the same source is still one observation. */
    @Test
    void aReplayFromTheSameSourceIsStillADuplicate() {
        ingest("shop.events", """
                {"id":"s1","orderNo":"ORD-A","kind":"ORDER_CREATED"}""");
        IngestionResult second = ingest("shop.events", """
                {"id":"s1","orderNo":"ORD-A","kind":"ORDER_CREATED"}""");

        assertThat(second.outcome()).isEqualTo(IngestionResult.Outcome.DUPLICATE);
        assertThat(allIngested()).hasSize(1);
    }

    /**
     * Arrival order is not identity. Observations are filed by their key and carry the
     * time the producer reported, so a later-published event read first still belongs to
     * its own execution and keeps its own timestamp — the out-of-order detectors judge the
     * sequence afterwards, on event time.
     */
    @Test
    void observationsArrivingOutOfOrderKeepTheirOwnIdentityAndTime() {
        ingest("shop.events", """
                {"id":"s2","orderNo":"ORD-A","kind":"PAYMENT_COMPLETED",
                 "timestamp":"2026-01-01T10:00:05Z"}""");
        ingest("shop.events", """
                {"id":"s1","orderNo":"ORD-A","kind":"ORDER_CREATED",
                 "timestamp":"2026-01-01T10:00:00Z"}""");

        List<IngestedEvent> ingested = allIngested();
        assertThat(ingested).hasSize(2);
        assertThat(ingested.get(0).sagaId()).isEqualTo(ingested.get(1).sagaId());
        assertThat(ingested.get(0).eventTime()).isAfter(ingested.get(1).eventTime());
    }

    // ── the correlation ladder ────────────────────────────────────────────────

    @Test
    void aDeclaredCorrelationKeyIsRecordedAsSuch() {
        ingest("shop.events", """
                {"id":"s1","orderNo":"ORD-A","kind":"ORDER_CREATED"}""");

        assertThat(allIngested().get(0).correlationBasis())
                .isEqualTo(CorrelationBasis.DECLARED_KEY);
    }

    /**
     * The second rung. A system whose services share no business identifier can still be
     * observed if it propagates trace context — which an auto-instrumentation agent
     * provides as a deployment flag, not a code change. Recommended, never required.
     */
    @Test
    void propagatedTraceContextCorrelatesWhenNoBusinessKeyExists() {
        IngestionResult result = ingest("shop.events", """
                {"id":"s1","kind":"ORDER_CREATED",
                 "traceId":"4bf92f3577b34da6a3ce929d0e0e4736"}""");

        assertThat(result.isAccepted()).isTrue();
        IngestedEvent ingested = allIngested().get(0);
        assertThat(ingested.executionKey().rawId())
                .isEqualTo("4bf92f3577b34da6a3ce929d0e0e4736");
        assertThat(ingested.correlationBasis()).isEqualTo(CorrelationBasis.PROPAGATED_CONTEXT);
    }

    /** A declared key outranks trace context: business identity beats request identity. */
    @Test
    void aDeclaredKeyIsPreferredOverTraceContext() {
        ingest("shop.events", """
                {"id":"s1","orderNo":"ORD-A","kind":"ORDER_CREATED",
                 "traceId":"4bf92f3577b34da6a3ce929d0e0e4736"}""");

        IngestedEvent ingested = allIngested().get(0);
        assertThat(ingested.executionKey().rawId()).isEqualTo("ORD-A");
        assertThat(ingested.correlationBasis()).isEqualTo(CorrelationBasis.DECLARED_KEY);
    }

    /**
     * There is no rung below trace context. Nothing here falls back on a business key, a
     * timestamp window or an event type, because none of those is evidence — and a wrong
     * correlation does not surface as an error, it surfaces as a diagnosis that quietly
     * makes no sense.
     */
    @Test
    void anObservationWithNothingOnTheLadderIsRefusedRatherThanGuessedAt() {
        IngestionResult result = ingest("shop.events", """
                {"id":"s1","kind":"ORDER_CREATED","customer":"acme"}""");

        assertThat(result.outcome()).isEqualTo(IngestionResult.Outcome.NOT_CORRELATABLE);
        verify(correlationService, never()).processEvent(any(IngestedEvent.class));
    }

    /** The refusal says what would have made it work, in the source's own terms. */
    @Test
    void theRefusalNamesBothRungsThatWereTried() {
        IngestionResult result = ingest("shop.events", """
                {"id":"s1","kind":"ORDER_CREATED"}""");

        assertThat(result.reason()).contains("orderNo").contains("trace context");
    }

    // ── an unmapped source is a namespace too ─────────────────────────────────

    /**
     * A source matching no mapping falls back to the identity mapping, and that fallback is
     * a namespace like any other — so an unmapped producer cannot land inside a mapped
     * system's execution by using the same identifier.
     */
    @Test
    void anUnmappedSourceGetsTheDefaultNamespace() {
        ingest("something.else", """
                {"eventId":"x1","sagaId":"ORD-A","eventType":"ORDER_CREATED"}""");

        assertThat(allIngested().get(0).sagaId()).isEqualTo("default::ORD-A");
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private IngestionResult ingest(String sourceRef, String payload) {
        return ingestion.ingest(new RawObservation(
                sourceRef, payload, TRANSPORT_TIME, "fallback", null, Map.of()));
    }

    private List<IngestedEvent> allIngested() {
        ArgumentCaptor<IngestedEvent> captor = ArgumentCaptor.forClass(IngestedEvent.class);
        verify(correlationService, org.mockito.Mockito.atLeast(0)).processEvent(captor.capture());
        return captor.getAllValues();
    }

    private static SourceMappingRegistry registryOf(String... mappingYaml) {
        List<SourceMapping> mappings = new ArrayList<>();
        for (String yaml : mappingYaml) {
            mappings.add(parse(yaml));
        }
        return new SourceMappingRegistry() {
            @Override
            public SourceMapping forTopic(String topic) {
                return mappings.stream()
                        .filter(mapping -> mapping.matches(topic))
                        .findFirst()
                        .orElseGet(SourceMapping::identity);
            }
        };
    }

    private static SourceMapping parse(String yaml) {
        try {
            return new ObjectMapper(new YAMLFactory()).readValue(yaml, SourceMapping.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("unreadable test mapping", e);
        }
    }
}
