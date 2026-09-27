package com.example.correlation.service.rca;

import com.example.correlation.domain.Anomaly;
import com.example.correlation.domain.AnomalyType;
import com.example.correlation.domain.RcaResult;
import com.example.correlation.domain.SagaEvent;
import com.example.correlation.domain.SagaInstance;
import com.example.correlation.domain.SagaStatus;
import com.example.correlation.domain.rca.RcaCandidate;
import com.example.correlation.execution.ExecutionProjectionService;
import com.example.correlation.repository.AnomalyRepository;
import com.example.correlation.repository.RcaResultRepository;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.example.correlation.service.DetectionProperties;
import com.example.correlation.service.RcaService;
import com.example.correlation.workflow.WorkflowRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * F15: R05 measures how long a run has been silent, and must do it on one clock.
 *
 * <p>It subtracted the source's timestamp of the last event from the platform's
 * {@code detectedAt}. A source clock running behind added its lag to the silence; one
 * running ahead subtracted it, down to a negative number. SAGA_STUCK already measures on
 * {@code receivedAt} for exactly this reason. ORDER_SAGA's slowest step is 10s, so R05's
 * threshold is 30s.
 */
class SilenceClockTest {

    private static final Instant HEARD = Instant.parse("2026-01-01T12:00:00Z");

    @Test
    void aSourceClockRunningBehindDoesNotInflateTheSilence() {
        // Heard 20s before the findings; stamped ten minutes earlier by a lagging clock.
        RcaResult result = analyze(HEARD.minusSeconds(600), HEARD, HEARD.plusSeconds(20));

        assertThat(result.getCandidates()).extracting(RcaCandidate::getRuleId).doesNotContain("R05");
    }

    @Test
    void aSourceClockRunningAheadDoesNotHideTheSilence() {
        // Heard 60s before the findings; stamped ten minutes later by a clock running ahead.
        RcaResult result = analyze(HEARD.plusSeconds(600), HEARD, HEARD.plusSeconds(60));

        assertThat(result.getCandidates()).filteredOn(c -> "R05".equals(c.getRuleId()))
                .singleElement()
                .satisfies(c -> assertThat(c.getEvidence()).anyMatch(e -> e.fact().contains("60s")));
    }

    private static RcaResult analyze(Instant stamped, Instant received, Instant detected) {
        SagaEvent event = new SagaEvent();
        event.setSagaId("saga-1");
        event.setSourceName("default");
        event.setEventId("e-1");
        event.setEventType("ORDER_CREATED");
        event.setPayload("{}");
        event.setTimestamp(stamped);
        event.setReceivedAt(received);

        SagaInstance saga = new SagaInstance();
        saga.setSagaId("saga-1");
        saga.setWorkflowType("ORDER_SAGA");
        saga.setStatus(SagaStatus.IN_PROGRESS);
        saga.setStartTime(stamped);
        saga.setLastUpdated(stamped);

        SagaInstanceRepository sagas = mock(SagaInstanceRepository.class);
        when(sagas.findByIdForUpdate(anyString())).thenReturn(Optional.of(saga));
        SagaEventRepository eventRepo = mock(SagaEventRepository.class);
        when(eventRepo.findBySagaIdOrderByTimestampAsc(anyString())).thenReturn(List.of(event));
        AnomalyRepository anomalies = mock(AnomalyRepository.class);
        when(anomalies.findStandingBySagaId(anyString())).thenReturn(List.of(
                anomaly(1L, AnomalyType.SAGA_STUCK, detected),
                anomaly(2L, AnomalyType.MISSING_TERMINAL_EVENT, detected)));
        RcaResultRepository results = mock(RcaResultRepository.class);
        when(results.save(any(RcaResult.class))).thenAnswer(call -> call.getArgument(0));

        WorkflowRegistry workflows = new WorkflowRegistry();
        workflows.load();
        RuleRegistry rules = new RuleRegistry();
        rules.load();

        return new RcaService(sagas, eventRepo, anomalies, results, workflows, new ObjectMapper(),
                new RcaSelector(), rules, new DetectionProperties(), alert -> { },
                new ExecutionProjectionService(), new FailureSignalRegistry(""), null)
                .analyze("saga-1");
    }

    private static Anomaly anomaly(Long id, AnomalyType type, Instant detectedAt) {
        Anomaly anomaly = new Anomaly();
        anomaly.setId(id);
        anomaly.setSagaId("saga-1");
        anomaly.setAnomalyType(type);
        anomaly.setDescription(type.name());
        anomaly.setDetectedAt(detectedAt);
        return anomaly;
    }
}
