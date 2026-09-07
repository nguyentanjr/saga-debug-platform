package com.example.correlation.service;

import com.example.correlation.domain.Anomaly;
import com.example.correlation.domain.AnomalyType;
import com.example.correlation.domain.CorrelationBasis;
import com.example.correlation.domain.ExecutionKey;
import com.example.correlation.domain.IngestedEvent;
import com.example.correlation.domain.RcaResult;
import com.example.correlation.domain.SagaEvent;
import com.example.correlation.domain.SagaInstance;
import com.example.correlation.domain.SagaStatus;
import com.example.correlation.execution.ExecutionProjectionService;
import com.example.correlation.repository.AnomalyRepository;
import com.example.correlation.repository.RcaResultRepository;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.example.correlation.service.alert.AlertSink;
import com.example.correlation.service.rca.RcaSelector;
import com.example.correlation.service.rca.RuleRegistry;
import com.example.correlation.workflow.WorkflowRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Telling this platform's re-reading apart from the observed system's re-sending.
 *
 * <h2>What went wrong before</h2>
 * The duplicate detector sat behind a database lookup that only runs when the in-memory
 * deduplication cache misses — and that cache misses exactly when the observer restarts or
 * its consumer offsets are reset. So it fired on the one occasion nothing had happened in
 * the observed system, and stayed silent on the occasion something had: a genuine
 * duplicate arriving inside a single run hit the cache and was skipped without a word.
 *
 * <p>Validation against three unrelated saga systems showed the consequence. Resetting the
 * offsets to re-read a topic produced EXACT_DUPLICATE_EVENT against every execution on it,
 * citing real outbox ids, and once RCA promotion landed those findings were promoted:
 * three of four sagas reported {@code DUPLICATE_DELIVERY (R13)} as their root cause, for a
 * replay the observer had performed on itself.
 *
 * <p>What separates the two cases is which record on the transport each observation came
 * in on. Kafka names one by topic-partition-offset; the consumer already computed it and
 * then dropped it. Re-reading presents the same record again; a source that published an
 * event twice produced two. That is now recorded with the event and compared — not
 * inferred from timing, and not from the payload looking familiar.
 */
class ObserverReplayTest {

    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
    private static final ExecutionKey RUN = ExecutionKey.of("shop", "ORD-1");

    private Map<String, SagaInstance> instances;
    private List<SagaEvent> events;
    private AnomalyDetectionService anomalyDetectionService;
    private CorrelationService correlationService;

    @BeforeEach
    void setUp() {
        instances = new LinkedHashMap<>();
        events = new ArrayList<>();

        SagaInstanceRepository sagaInstanceRepository = mock(SagaInstanceRepository.class);
        when(sagaInstanceRepository.findById(anyString()))
                .thenAnswer(call -> Optional.ofNullable(instances.get(call.getArgument(0))));
        when(sagaInstanceRepository.save(any(SagaInstance.class))).thenAnswer(call -> {
            SagaInstance instance = call.getArgument(0);
            instances.put(instance.getSagaId(), instance);
            return instance;
        });

        SagaEventRepository sagaEventRepository = mock(SagaEventRepository.class);
        when(sagaEventRepository.findBySourceNameAndEventId(anyString(), anyString()))
                .thenAnswer(call -> events.stream()
                        .filter(e -> e.getSourceName().equals(call.getArgument(0)))
                        .filter(e -> e.getEventId().equals(call.getArgument(1)))
                        .findFirst());
        when(sagaEventRepository.findBySagaIdOrderByTimestampAsc(anyString()))
                .thenAnswer(call -> events.stream()
                        .filter(e -> e.getSagaId().equals(call.getArgument(0)))
                        .sorted(Comparator.comparing(SagaEvent::getTimestamp))
                        .toList());
        when(sagaEventRepository.save(any(SagaEvent.class))).thenAnswer(call -> {
            events.add(call.getArgument(0));
            return call.getArgument(0);
        });

        anomalyDetectionService = mock(AnomalyDetectionService.class);

        WorkflowRegistry registry = new WorkflowRegistry();
        registry.load();

        correlationService = new CorrelationService(
                sagaInstanceRepository, sagaEventRepository, anomalyDetectionService,
                new DeduplicationCache(), registry, new ObjectMapper());
    }

    // ══ the distinction itself ════════════════════════════════════════════════

    /**
     * The headline. The same record read a second time — an offset reset, a restart, a
     * reprocess — is this platform reading, not the source sending.
     */
    @Test
    void aReReadOfTheSameRecordIsNotReportedAgainstTheSource() {
        correlationService.processEvent(observation("e1", "ORDER_CREATED", "orders-0-17"));
        correlationService.processEvent(observation("e1", "ORDER_CREATED", "orders-0-17"));

        verify(anomalyDetectionService, never())
                .detectDuplicateEvent(anyString(), anyString(), anyString(), any(), any());
        verify(anomalyDetectionService, never())
                .detectDuplicateEvent(anyString(), anyString(), anyString());
    }

    /**
     * And the case the detector exists for is untouched: one event id, two records, which
     * only a source that published it twice can produce.
     */
    @Test
    void aGenuineSourceDuplicateIsStillReported() {
        correlationService.processEvent(observation("e1", "ORDER_CREATED", "orders-0-17"));
        correlationService.processEvent(observation("e1", "ORDER_CREATED", "orders-0-42"));

        verify(anomalyDetectionService).detectDuplicateEvent(
                RUN.canonical(), "e1", "ORDER_CREATED", "orders-0-17", "orders-0-42");
    }

    /**
     * A replay stores nothing a second time. That was already true and is the reason the
     * finding was wrong rather than harmful: dedup was working, only its explanation was.
     */
    @Test
    void aReplayDoesNotStoreTheEventTwice() {
        correlationService.processEvent(observation("e1", "ORDER_CREATED", "orders-0-17"));
        correlationService.processEvent(observation("e2", "PAYMENT_COMPLETED", "payments-0-3"));

        List<SagaEvent> afterFirstPass = List.copyOf(events);

        // The whole run re-read, every record at the position it already occupied.
        correlationService.processEvent(observation("e1", "ORDER_CREATED", "orders-0-17"));
        correlationService.processEvent(observation("e2", "PAYMENT_COMPLETED", "payments-0-3"));

        assertThat(events).hasSize(2).containsExactlyElementsOf(afterFirstPass);
    }

    /** The stored event remembers which record established it, which is what makes the above work. */
    @Test
    void theRecordThatCarriedAnEventIsStoredWithIt() {
        correlationService.processEvent(observation("e1", "ORDER_CREATED", "orders-0-17"));

        assertThat(events).singleElement()
                .extracting(SagaEvent::getTransportRef).isEqualTo("orders-0-17");
    }

    /**
     * A transport that names no record leaves the platform unable to tell the two apart,
     * and it says so by keeping its previous behaviour rather than assuming replay. HTTP
     * ingestion is the real instance: a second POST of one event id genuinely is a second
     * delivery, and there is no offset for it to have been re-read from.
     */
    @Test
    void anObservationWithNoTransportIdentityKeepsItsPreviousMeaning() {
        correlationService.processEvent(observation("e1", "ORDER_CREATED", null));
        correlationService.processEvent(observation("e1", "ORDER_CREATED", null));

        verify(anomalyDetectionService).detectDuplicateEvent(
                RUN.canonical(), "e1", "ORDER_CREATED", null, null);
    }

    /** Nor is a half-known pair treated as a match: one reference is not two. */
    @Test
    void aStoredEventWithNoRecordIsNotAssumedToBeTheSameRecord() {
        correlationService.processEvent(observation("e1", "ORDER_CREATED", null));
        correlationService.processEvent(observation("e1", "ORDER_CREATED", "orders-0-17"));

        verify(anomalyDetectionService).detectDuplicateEvent(
                RUN.canonical(), "e1", "ORDER_CREATED", null, "orders-0-17");
    }

    // ══ the fast path in front of it ══════════════════════════════════════════

    /**
     * The deduplication cache still short-circuits a redelivery of the same record without
     * a database round trip, which is the whole reason it exists.
     */
    @Test
    void theCacheStillSkipsARedeliveryOfTheSameRecord() {
        DeduplicationCache cache = new DeduplicationCache();
        cache.markProcessed("shop::e1", "orders-0-17");

        assertThat(cache.seen("shop::e1")).isTrue();
        assertThat(cache.seenOnDifferentRecord("shop::e1", "orders-0-17")).isFalse();
    }

    /**
     * But it no longer swallows a second record silently. This is the case that used to
     * disappear: a source publishing one event twice inside a single observer run never
     * reached the database lookup, so the genuine duplicate went unreported while the
     * observer's own re-reads were reported instead.
     */
    @Test
    void theCacheNoLongerSwallowsASecondRecordCarryingTheSameEvent() {
        DeduplicationCache cache = new DeduplicationCache();
        cache.markProcessed("shop::e1", "orders-0-17");

        assertThat(cache.seenOnDifferentRecord("shop::e1", "orders-0-42")).isTrue();
    }

    /** An entry recorded without a record cannot claim two records differ. */
    @Test
    void theCacheDoesNotClaimADifferenceItCannotSee() {
        DeduplicationCache cache = new DeduplicationCache();
        cache.markProcessed("shop::e1");

        assertThat(cache.seen("shop::e1")).isTrue();
        assertThat(cache.seenOnDifferentRecord("shop::e1", "orders-0-42")).isFalse();
        assertThat(cache.seenOnDifferentRecord("shop::e1", null)).isFalse();
    }

    // ══ what reaches RCA ══════════════════════════════════════════════════════

    /**
     * The consequence F2 was reported for. A replayed run raises no duplicate finding, so
     * there is no R13 candidate for promotion to pick up, and RCA reports what the
     * execution actually shows instead of a duplicate the observer caused.
     */
    @Test
    void aReplayLeavesRcaWithNoDuplicateCauseToPromote() {
        RcaResult result = analyseWith(standingAnomalies());

        assertThat(result.getRootCause()).isNull();
        assertThat(result.getRuleId()).isNull();
        assertThat(result.getCandidates())
                .noneSatisfy(candidate -> assertThat(candidate.getRuleId()).isEqualTo("R13"));
    }

    /**
     * And a genuine duplicate still reaches RCA and is still promotable, so this fix
     * removed a false finding rather than the rule that reports the true one. R13 is not
     * hidden, down-ranked or reclassified — it is simply not raised for a replay.
     */
    @Test
    void aGenuineDuplicateStillReachesRcaAndIsPromotable() {
        RcaResult result = analyseWith(standingAnomalies(AnomalyType.EXACT_DUPLICATE_EVENT));

        assertThat(result.getRootCause()).isEqualTo("DUPLICATE_DELIVERY");
        assertThat(result.getRuleId()).isEqualTo("R13");
    }

    /**
     * Retraction still governs what RCA may reason from. A withdrawn duplicate finding is
     * invisible to the analysis exactly as any other withdrawn finding is — this change
     * did not add a second route into RCA that bypasses it.
     */
    @Test
    void aWithdrawnDuplicateFindingStaysOutOfTheDiagnosis() {
        Anomaly withdrawn = anomaly(AnomalyType.EXACT_DUPLICATE_EVENT);
        withdrawn.setRetractedAt(T0.plusSeconds(60));
        withdrawn.setRetractionReason("the records turned out to be one record read twice");

        RcaResult result = analyseWith(List.of());

        assertThat(result.getRootCause()).isNull();
        assertThat(result.getRuleId()).isNull();
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private IngestedEvent observation(String eventId, String eventType, String transportRef) {
        return new IngestedEvent(
                new IngestedEvent.Identity("shop", RUN, eventId, null,
                        CorrelationBasis.DECLARED_KEY),
                null, eventType, "UNKNOWN", "unknown", T0, "{}", T0,
                IngestedEvent.Causality.NONE, transportRef);
    }

    private List<Anomaly> standingAnomalies(AnomalyType... types) {
        List<Anomaly> standing = new ArrayList<>();
        for (AnomalyType type : types) {
            standing.add(anomaly(type));
        }
        return standing;
    }

    private Anomaly anomaly(AnomalyType type) {
        Anomaly anomaly = new Anomaly();
        anomaly.setId((long) (type.ordinal() + 1));
        anomaly.setAnomalyType(type);
        anomaly.setSagaId(RUN.canonical());
        anomaly.setDetectedAt(T0);
        return anomaly;
    }

    /**
     * Runs the real RCA pipeline — the shipped rule registry, the real selector and the
     * promotion added for F1 — over the findings a run is left with.
     */
    private RcaResult analyseWith(List<Anomaly> standing) {
        SagaInstance saga = new SagaInstance();
        saga.setSagaId(RUN.canonical());
        saga.setStatus(SagaStatus.COMPLETED);
        saga.setStartTime(T0);

        SagaInstanceRepository sagaInstances = mock(SagaInstanceRepository.class);
        when(sagaInstances.findByIdForUpdate(anyString())).thenReturn(Optional.of(saga));

        SagaEventRepository sagaEvents = mock(SagaEventRepository.class);
        when(sagaEvents.findBySagaIdOrderByTimestampAsc(anyString())).thenReturn(List.of());

        AnomalyRepository anomalies = mock(AnomalyRepository.class);
        when(anomalies.findStandingBySagaId(eq(RUN.canonical()))).thenReturn(standing);

        RcaResultRepository results = mock(RcaResultRepository.class);
        when(results.save(any(RcaResult.class))).thenAnswer(call -> call.getArgument(0));

        RuleRegistry rules = new RuleRegistry();
        rules.load();
        WorkflowRegistry workflows = new WorkflowRegistry();
        workflows.load();

        return new RcaService(sagaInstances, sagaEvents, anomalies, results, workflows,
                new ObjectMapper(), new RcaSelector(), rules, new DetectionProperties(),
                mock(AlertSink.class), new ExecutionProjectionService(), null)
                .analyze(RUN.canonical());
    }
}
