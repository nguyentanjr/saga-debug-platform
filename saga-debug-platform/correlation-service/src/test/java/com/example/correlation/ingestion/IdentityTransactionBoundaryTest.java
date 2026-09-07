package com.example.correlation.ingestion;

import com.example.correlation.domain.IngestedEvent;
import com.example.correlation.messaging.EventTimeResolver;
import com.example.correlation.service.CorrelationService;
import com.example.correlation.service.DeduplicationCache;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.springframework.dao.DataAccessResourceFailureException;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Identity and the observation that establishes it commit together, or not at all.
 *
 * <h2>The limitation these were written against</h2>
 * Alias resolution ran in {@code REQUIRES_NEW} — a transaction of its own, opened before
 * the one that stores the observation. The justification was that an alias is a standing
 * fact about two identifiers whether or not this particular event is stored.
 *
 * <p>That justification does not survive contact with what an alias row actually contains.
 * It cites the event that carried both identifiers, in {@code linkedByEventId}. A row
 * saying "these two are one run, see event e3" while e3 was never stored is not a fact, it
 * is a dangling citation — and worse, a fold performed on that basis moves observations,
 * re-points findings and withdraws absence claims, all on evidence the database does not
 * hold. A transient failure storing e3 left exactly that state behind.
 *
 * <h2>What atomicity does and does not buy</h2>
 * It makes the observer's own data consistent. It does not promote an inference to a fact:
 * the alias still records its source, its evidence event and the
 * {@link com.example.correlation.domain.CorrelationBasis} it was arrived at on, and a link
 * is still only ever made from an event that carried both identifiers.
 */
class IdentityTransactionBoundaryTest {

    private static final String MAPPING = """
            name: split-ids
            topicPattern: 'split\\.events'
            fields:
              eventId: id
              executionId: orderNo
              aliasKey: linkedPaymentRef
            """;

    private static final String SOURCE = "split.events";
    private static final Instant TRANSPORT_TIME = Instant.parse("2026-01-01T00:00:00Z");

    private CorrelationService correlationService;
    private DeduplicationCache deduplication;
    private ExecutionAliasResolver aliasResolver;
    private InlineTransactionManager transactions;
    private EventIngestionService ingestion;

    @BeforeEach
    void setUp() {
        correlationService = mock(CorrelationService.class);
        deduplication = new DeduplicationCache();
        aliasResolver = new PassthroughAliasResolver();
        transactions = new InlineTransactionManager();

        SourceMapping mapping = parse(MAPPING);
        SourceMappingRegistry registry = new SourceMappingRegistry() {
            @Override
            public SourceMapping forTopic(String topic) {
                return mapping.matches(topic) ? mapping : SourceMapping.identity();
            }
        };

        ingestion = new EventIngestionService(
                correlationService,
                new ObjectMapper(),
                new SourceCompletenessCheck(),
                deduplication,
                new EventTimeResolver(),
                registry,
                aliasResolver,
                transactions);
    }

    // ── the boundary ──────────────────────────────────────────────────────────

    /**
     * Identity is decided inside the same unit of work that stores the observation, and
     * before it — an event's {@code saga_id} is its identity, so there is nothing to write
     * until the key is known. The order is a consequence of the data model, not of the
     * transaction.
     */
    @Test
    void identityIsDecidedInsideTheUnitOfWorkThatStoresTheObservation() {
        ingest("""
                {"id":"e1","orderNo":"ORD-1","linkedPaymentRef":"PAY-9"}""");

        InOrder order = inOrder(correlationService);
        order.verify(correlationService).processEvent(any(IngestedEvent.class));

        assertThat(transactions.hasCommitted()).isTrue();
        assertThat(transactions.hasRolledBack()).isFalse();
    }

    /**
     * A transient failure storing the observation rolls the identity decision back with it.
     * Before, the alias had already committed in its own transaction and survived.
     */
    @Test
    void aFailureStoringTheObservationRollsTheIdentityDecisionBack() {
        doThrow(new DataAccessResourceFailureException("database down"))
                .when(correlationService).processEvent(any(IngestedEvent.class));

        assertThatThrownBy(() -> ingest("""
                {"id":"e2","orderNo":"ORD-2","linkedPaymentRef":"PAY-8"}"""))
                .isInstanceOf(DataAccessResourceFailureException.class);

        assertThat(transactions.hasRolledBack()).isTrue();
        assertThat(transactions.hasCommitted()).isFalse();
    }

    // ── idempotency across the boundary ───────────────────────────────────────

    /**
     * The failure must not be remembered as success. Marking the event as seen before the
     * commit would make the transport's redelivery — the whole recovery mechanism — look
     * like a duplicate and drop it.
     */
    @Test
    void anEventIsNotRememberedAsSeenUntilItsTransactionCommits() {
        doThrow(new DataAccessResourceFailureException("database down"))
                .when(correlationService).processEvent(any(IngestedEvent.class));

        String event = """
                {"id":"e3","orderNo":"ORD-3","linkedPaymentRef":"PAY-7"}""";
        assertThatThrownBy(() -> ingest(event))
                .isInstanceOf(DataAccessResourceFailureException.class);

        // The redelivery the transport is about to make.
        org.mockito.Mockito.reset(correlationService);
        IngestionResult retry = ingestion.ingest(observation(event));

        assertThat(retry.isAccepted()).isTrue();
        verify(correlationService).processEvent(any(IngestedEvent.class));
    }

    /** A redelivery after a successful commit is recognised rather than stored twice. */
    @Test
    void aRedeliveryAfterACommitIsRecognised() {
        String event = """
                {"id":"e4","orderNo":"ORD-4","linkedPaymentRef":"PAY-6"}""";

        assertThat(ingest(event).isAccepted()).isTrue();
        assertThat(ingestion.ingest(observation(event)).outcome())
                .isEqualTo(IngestionResult.Outcome.DUPLICATE);

        verify(correlationService, times(1)).processEvent(any(IngestedEvent.class));
    }

    /**
     * A rejection is not a failure: an observation that cannot be identified never reaches
     * the transaction at all, so no unit of work is opened for it.
     */
    @Test
    void anUnidentifiableObservationOpensNoTransaction() {
        IngestionResult result = ingestion.ingest(observation("""
                {"orderNo":"ORD-5"}"""));

        assertThat(result.outcome()).isEqualTo(IngestionResult.Outcome.NOT_CORRELATABLE);
        assertThat(transactions.hasCommitted()).isFalse();
        assertThat(transactions.hasRolledBack()).isFalse();
        verify(correlationService, never()).processEvent(any(IngestedEvent.class));
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private IngestionResult ingest(String body) {
        return ingestion.ingest(observation(body));
    }

    private static RawObservation observation(String body) {
        return new RawObservation(SOURCE, body, TRANSPORT_TIME, null, null, Map.of());
    }

    private static SourceMapping parse(String yaml) {
        try {
            return new ObjectMapper(new YAMLFactory()).readValue(yaml, SourceMapping.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("unreadable test mapping", e);
        }
    }
}
