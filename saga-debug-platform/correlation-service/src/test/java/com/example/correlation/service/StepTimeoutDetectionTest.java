package com.example.correlation.service;

import com.example.correlation.domain.Anomaly;
import com.example.correlation.domain.AnomalyType;
import com.example.correlation.domain.EventTimeSource;
import com.example.correlation.domain.SagaEvent;
import com.example.correlation.execution.ExecutionProjectionService;
import com.example.correlation.repository.AnomalyRepository;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.example.correlation.workflow.WorkflowRegistry;
import com.example.correlation.workflow.WorkflowSpec;
import com.example.correlation.workflow.WorkflowSpecTestFixture;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Compares the two step-timeout measurements on the same event streams.
 *
 * <p>These tests pin down where the adjacency measurement reports the wrong answer.
 * They are written to keep failing for LEGACY on purpose: the legacy assertions
 * document current behaviour so the switch to PROJECTION has a documented delta,
 * rather than asserting that the legacy behaviour is correct.
 */
class StepTimeoutDetectionTest {

    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    private final WorkflowSpec spec = WorkflowSpecTestFixture.orderSaga();

    // PROCESS_PAYMENT declares a 10s timeout in the workflow spec.
    private final List<SagaEvent> slowPayment = List.of(
            event("e1", "ORDER_CREATED", T0),
            event("e2", "PAYMENT_COMPLETED", T0.plusSeconds(12)),
            event("e3", "INVENTORY_RESERVED", T0.plusSeconds(13)));

    @Test
    void bothMeasurementsAgreeOnAPlainSlowStep() {
        assertThat(findingStep(DetectionProperties.StepTimeoutMode.LEGACY, slowPayment))
                .isEqualTo("PROCESS_PAYMENT");
        assertThat(findingStep(DetectionProperties.StepTimeoutMode.PROJECTION, slowPayment))
                .isEqualTo("PROCESS_PAYMENT");
    }

    /**
     * An event belonging to no declared step is enough to hide the timeout, because
     * the adjacency measurement restarts from whatever event precedes the one it is
     * measuring. The execution being observed is unchanged.
     */
    @Test
    void unrelatedObservationHidesTheTimeoutFromTheLegacyMeasurement() {
        List<SagaEvent> withNoise = new ArrayList<>(slowPayment);
        withNoise.add(event("e-noise", "AUDIT_PING", T0.plusSeconds(11)));

        assertThat(findingStep(DetectionProperties.StepTimeoutMode.LEGACY, withNoise)).isNull();
        assertThat(findingStep(DetectionProperties.StepTimeoutMode.PROJECTION, withNoise))
                .isEqualTo("PROCESS_PAYMENT");
    }

    /**
     * The same defect on the production path: PAYMENT_RETRY is a real event on a
     * subscribed topic, and it is not listed in {@code validEventTypes}, so each retry
     * resets the adjacency measurement. A payment step that took 11s is measured as
     * the 2s between the last retry and the failure.
     */
    @Test
    void retriesHideTheTimeoutFromTheLegacyMeasurement() {
        List<SagaEvent> retriedPayment = List.of(
                event("e1", "ORDER_CREATED", T0),
                event("e2", "PAYMENT_RETRY", T0.plusSeconds(3)),
                event("e3", "PAYMENT_RETRY", T0.plusSeconds(6)),
                event("e4", "PAYMENT_RETRY", T0.plusSeconds(9)),
                event("e5", "PAYMENT_FAILED", T0.plusSeconds(11)));

        assertThat(findingStep(DetectionProperties.StepTimeoutMode.LEGACY, retriedPayment)).isNull();

        Anomaly projected = capture(DetectionProperties.StepTimeoutMode.PROJECTION, retriedPayment);
        assertThat(projected).isNotNull();
        assertThat(projected.getObservedValue())
                .contains("PROCESS_PAYMENT took 11000ms")
                .contains("over 4 attempt(s)");
    }

    @Test
    void shadowModeKeepsLegacyAuthoritative() {
        List<SagaEvent> withNoise = new ArrayList<>(slowPayment);
        withNoise.add(event("e-noise", "AUDIT_PING", T0.plusSeconds(11)));

        // Legacy finds nothing here, so shadow mode must also write nothing:
        // running the projection alongside must not change what is persisted.
        assertThat(findingStep(DetectionProperties.StepTimeoutMode.SHADOW, withNoise)).isNull();
    }

    @Test
    void projectionRecordsMeasurementProvenance() {
        Anomaly anomaly = capture(DetectionProperties.StepTimeoutMode.PROJECTION, slowPayment);

        assertThat(anomaly.getObservedValue()).contains("start=INFERRED_FROM_PREDECESSOR");
        assertThat(anomaly.getThresholdSource()).isEqualTo("workflow:PROCESS_PAYMENT.timeout");
        assertThat(anomaly.getEvidenceEventIds()).contains("e1", "e2");
    }

    @Test
    void projectionDoesNotInventADurationForAnUnanchoredOperation() {
        List<SagaEvent> orphan = List.of(
                event("e1", "INVENTORY_RESERVED", T0.plusSeconds(3600)));

        AnomalyRepository repository = mock(AnomalyRepository.class);
        service(DetectionProperties.StepTimeoutMode.PROJECTION, repository)
                .evaluateStepTimeouts("saga-1", orphan, spec);

        verifyNoInteractions(repository);
    }

    // ── F12: which clocks a duration was measured between ────────────────────

    /**
     * Both ends declared by the source: the finding says so, and claims no more — two
     * services' declared times can still disagree, but neither end is transport time.
     */
    @Test
    void aDurationBetweenTwoDeclaredTimesSaysSo() {
        List<SagaEvent> declared = withSources(EventTimeSource.DECLARED, EventTimeSource.DECLARED);

        for (DetectionProperties.StepTimeoutMode mode : List.of(
                DetectionProperties.StepTimeoutMode.LEGACY, DetectionProperties.StepTimeoutMode.PROJECTION)) {
            assertThat(capture(mode, declared).getObservedValue()).as(mode.name())
                    .contains("clocks=DECLARED->DECLARED");
        }
    }

    /**
     * One end declared and one taken from the transport: the interval spans two different
     * clocks and includes whatever separates them. Before F12 it read exactly like the
     * case above.
     */
    @Test
    void aDurationAcrossADeclaredAndATransportTimeIsMarkedMixed() {
        List<SagaEvent> mixed = withSources(EventTimeSource.DECLARED, EventTimeSource.TRANSPORT);

        for (DetectionProperties.StepTimeoutMode mode : List.of(
                DetectionProperties.StepTimeoutMode.LEGACY, DetectionProperties.StepTimeoutMode.PROJECTION)) {
            Anomaly anomaly = capture(mode, mixed);
            assertThat(anomaly.getObservedValue()).as(mode.name())
                    .contains("clocks=DECLARED->TRANSPORT (mixed)");
            assertThat(anomaly.getDescription()).as(mode.name()).contains("different clocks");
        }
    }

    /** Rows written before V17 carry no origin, and the finding does not invent one. */
    @Test
    void aDurationOverTimesOfUnrecordedOriginSaysSo() {
        List<SagaEvent> legacy = withSources(null, EventTimeSource.DECLARED);

        for (DetectionProperties.StepTimeoutMode mode : List.of(
                DetectionProperties.StepTimeoutMode.LEGACY, DetectionProperties.StepTimeoutMode.PROJECTION)) {
            assertThat(capture(mode, legacy).getObservedValue()).as(mode.name())
                    .contains("clocks=unrecorded");
        }
    }

    /** slowPayment, with the origin of the two timestamps a PROCESS_PAYMENT duration spans. */
    private List<SagaEvent> withSources(EventTimeSource start, EventTimeSource end) {
        SagaEvent created = event("e1", "ORDER_CREATED", T0);
        created.setEventTimeSource(start);
        SagaEvent paid = event("e2", "PAYMENT_COMPLETED", T0.plusSeconds(12));
        paid.setEventTimeSource(end);
        SagaEvent reserved = event("e3", "INVENTORY_RESERVED", T0.plusSeconds(13));
        reserved.setEventTimeSource(EventTimeSource.DECLARED);
        return List.of(created, paid, reserved);
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private String findingStep(DetectionProperties.StepTimeoutMode mode, List<SagaEvent> events) {
        Anomaly anomaly = capture(mode, events);
        if (anomaly == null) return null;
        // thresholdSource is "workflow:<STEP>.timeout"
        String source = anomaly.getThresholdSource();
        return source.substring("workflow:".length(), source.indexOf(".timeout"));
    }

    private Anomaly capture(DetectionProperties.StepTimeoutMode mode, List<SagaEvent> events) {
        List<Anomaly> saved = new ArrayList<>();
        AnomalyRepository repository = mock(AnomalyRepository.class);
        when(repository.save(any(Anomaly.class))).thenAnswer(invocation -> {
            saved.add(invocation.getArgument(0));
            return invocation.getArgument(0);
        });

        // The production caller passes findBySagaIdOrderByTimestampAsc(...), so the
        // legacy adjacency walk always sees events in event-time order. Reproduce that
        // here; otherwise the comparison would measure list order rather than the two
        // measurement strategies.
        List<SagaEvent> ordered = events.stream()
                .sorted(java.util.Comparator.comparing(SagaEvent::getTimestamp))
                .toList();

        service(mode, repository).evaluateStepTimeouts("saga-1", ordered, spec);

        assertThat(saved).hasSizeLessThanOrEqualTo(1);
        if (saved.isEmpty()) return null;

        Anomaly anomaly = saved.get(0);
        assertThat(anomaly.getAnomalyType()).isEqualTo(AnomalyType.STEP_TIMEOUT);
        return anomaly;
    }

    private AnomalyDetectionService service(DetectionProperties.StepTimeoutMode mode,
            AnomalyRepository anomalyRepository) {
        DetectionProperties properties = new DetectionProperties();
        properties.setStepTimeoutMode(mode);

        WorkflowRegistry registry = mock(WorkflowRegistry.class);
        org.mockito.Mockito.lenient().when(registry.byType(org.mockito.ArgumentMatchers.anyString()))
                .thenReturn(java.util.Optional.of(spec));
        org.mockito.Mockito.lenient().when(registry.workflowNames())
                .thenReturn(java.util.List.of(spec.getName()));

        return new AnomalyDetectionService(
                mock(SagaInstanceRepository.class),
                mock(SagaEventRepository.class),
                anomalyRepository,
                registry,
                new ObjectMapper(),
                properties,
                new ExecutionProjectionService());
    }

    private static SagaEvent event(String eventId, String eventType, Instant timestamp) {
        SagaEvent event = new SagaEvent();
        event.setEventId(eventId);
        event.setSagaId("saga-1");
        event.setEventType(eventType);
        event.setServiceName("test");
        event.setTimestamp(timestamp);
        event.setPayload("{}");
        event.setReceivedAt(timestamp);
        return event;
    }
}
