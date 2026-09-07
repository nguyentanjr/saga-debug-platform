package com.example.correlation.service;

import com.example.correlation.domain.Anomaly;
import com.example.correlation.domain.AnomalyType;
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

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * What a withdrawn finding is allowed to go on influencing.
 *
 * <h2>The mechanism, and the hole in it</h2>
 * A claim of absence — "no start event was seen", "this saga is stuck", "the compensation
 * was never acknowledged" — is only ever true of the observations in hand, so the platform
 * withdraws one when later observations falsify it. The row stays, stamped with when and
 * why, and {@code existsBySagaIdAndAnomalyType} stops counting it so the detectors are free
 * to raise it again if it still holds.
 *
 * <p>Root-cause analysis read the anomalies straight back with {@code findBySagaId}, which
 * returns every row including the withdrawn ones. So a diagnosis was built on findings the
 * platform had already said it no longer stood behind: a run whose SAGA_STUCK was withdrawn
 * the moment its late events arrived was still diagnosed SERVICE_CRASH, with "Saga stuck
 * for Ns" offered as evidence. Withdrawing a claim and then citing it is worse than never
 * withdrawing it, because the report reads as current.
 *
 * <p>That is the same confusion as everywhere else in this audit, one layer up: a gap in
 * the platform's own evidence being read as evidence about the observed system.
 */
class RetractedEvidenceTest {

    private static final String SAGA = "default::run-1";

    private List<Anomaly> anomalies;
    private List<RcaResult> saved;
    private RcaService rcaService;

    @BeforeEach
    void setUp() {
        anomalies = new ArrayList<>();
        saved = new ArrayList<>();

        SagaInstance saga = new SagaInstance();
        saga.setSagaId(SAGA);
        saga.setWorkflowType("ORDER_SAGA");
        saga.setStatus(SagaStatus.IN_PROGRESS);
        saga.setStartTime(Instant.now().minus(Duration.ofMinutes(30)));
        saga.setLastUpdated(Instant.now().minus(Duration.ofMinutes(30)));

        SagaInstanceRepository sagaInstanceRepository = mock(SagaInstanceRepository.class);
        when(sagaInstanceRepository.findById(anyString())).thenReturn(Optional.of(saga));
        when(sagaInstanceRepository.findByIdForUpdate(anyString())).thenReturn(Optional.of(saga));

        SagaEventRepository sagaEventRepository = mock(SagaEventRepository.class);
        when(sagaEventRepository.findBySagaIdOrderByTimestampAsc(anyString()))
                .thenReturn(List.of(event("e1", "ORDER_CREATED", 0), event("e2", "PAYMENT_COMPLETED", 1)));

        AnomalyRepository anomalyRepository = mock(AnomalyRepository.class);
        when(anomalyRepository.findBySagaId(anyString())).thenAnswer(call -> List.copyOf(anomalies));
        when(anomalyRepository.findStandingBySagaId(anyString())).thenAnswer(call ->
                anomalies.stream().filter(a -> a.getRetractedAt() == null).toList());

        RcaResultRepository rcaResultRepository = mock(RcaResultRepository.class);
        when(rcaResultRepository.save(any(RcaResult.class))).thenAnswer(call -> {
            saved.add(call.getArgument(0));
            return call.getArgument(0);
        });

        WorkflowRegistry registry = new WorkflowRegistry();
        registry.load();
        RuleRegistry ruleRegistry = new RuleRegistry();
        ruleRegistry.load();

        AlertSink alertSink = alert -> { };

        rcaService = new RcaService(sagaInstanceRepository, sagaEventRepository,
                anomalyRepository, rcaResultRepository, registry, new ObjectMapper(),
                new RcaSelector(), ruleRegistry, new DetectionProperties(), alertSink,
                new ExecutionProjectionService(), null);
    }

    /**
     * A run that looked stuck, then turned out not to be. Both findings were withdrawn when
     * the late events arrived, so neither may appear in the diagnosis.
     */
    @Test
    void aWithdrawnFindingIsNotEvidenceForADiagnosis() {
        anomalies.add(withdrawn(AnomalyType.SAGA_STUCK));
        anomalies.add(withdrawn(AnomalyType.MISSING_TERMINAL_EVENT));

        RcaResult result = rcaService.analyze(SAGA);

        assertThat(result).isNotNull();
        assertThat(result.getEvidence()).doesNotContain("SAGA_STUCK");
        assertThat(result.getEvidence()).doesNotContain("MISSING_TERMINAL_EVENT");
        assertThat(String.valueOf(result.getRootCause())).doesNotContain("SERVICE_CRASH");
    }

    /** A finding that still stands is still evidence, exactly as before. */
    @Test
    void aStandingFindingIsStillEvidence() {
        anomalies.add(standing(AnomalyType.SAGA_STUCK));
        anomalies.add(standing(AnomalyType.MISSING_TERMINAL_EVENT));

        RcaResult result = rcaService.analyze(SAGA);

        assertThat(result).isNotNull();
        assertThat(result.getEvidence()).contains("SAGA_STUCK");
    }

    /** A mix reports only the half that still stands. */
    @Test
    void onlyTheStandingHalfOfAMixedSetIsUsed() {
        anomalies.add(standing(AnomalyType.OUT_OF_ORDER));
        anomalies.add(withdrawn(AnomalyType.SAGA_STUCK));

        RcaResult result = rcaService.analyze(SAGA);

        assertThat(result).isNotNull();
        assertThat(result.getEvidence()).contains("OUT_OF_ORDER");
        assertThat(result.getEvidence()).doesNotContain("SAGA_STUCK");
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private static Anomaly standing(AnomalyType type) {
        Anomaly anomaly = new Anomaly();
        anomaly.setSagaId(SAGA);
        anomaly.setAnomalyType(type);
        anomaly.setDescription(type + " observed");
        anomaly.setDetectedAt(Instant.now().minus(Duration.ofMinutes(5)));
        anomaly.setMeasuredAt(Instant.now().minus(Duration.ofMinutes(5)));
        return anomaly;
    }

    private static Anomaly withdrawn(AnomalyType type) {
        Anomaly anomaly = standing(type);
        anomaly.setRetractedAt(Instant.now());
        anomaly.setRetractionReason("the late events arrived");
        return anomaly;
    }

    private static SagaEvent event(String eventId, String eventType, long secondsIn) {
        SagaEvent event = new SagaEvent();
        event.setEventId(eventId);
        event.setSagaId(SAGA);
        event.setEventType(eventType);
        event.setTimestamp(Instant.parse("2026-01-01T00:00:00Z").plusSeconds(secondsIn));
        event.setReceivedAt(event.getTimestamp());
        return event;
    }
}
