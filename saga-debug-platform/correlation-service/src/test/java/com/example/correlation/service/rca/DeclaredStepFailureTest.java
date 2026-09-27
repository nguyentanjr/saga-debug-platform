package com.example.correlation.service.rca;

import com.example.correlation.domain.RcaResult;
import com.example.correlation.domain.SagaEvent;
import com.example.correlation.domain.SagaInstance;
import com.example.correlation.domain.SagaStatus;
import com.example.correlation.domain.rca.RcaCandidate;
import com.example.correlation.domain.rca.RcaStatus;
import com.example.correlation.execution.ExecutionProjectionService;
import com.example.correlation.repository.AnomalyRepository;
import com.example.correlation.repository.RcaResultRepository;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.example.correlation.service.DetectionProperties;
import com.example.correlation.service.RcaService;
import com.example.correlation.service.alert.AlertSink;
import com.example.correlation.workflow.WorkflowRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * R24: a FAILED run whose failure is the step's own declared failed event, carrying no
 * reason the source's declaration can read.
 *
 * <p>Found on a live run of an unmodified third-party saga (hotel reservation, 2026-09-21):
 * {@code ROOM_REJECTED} carries only {@code {"status":"REJECTED"}}, so R01/R23 had nothing
 * to read, no other rule looked at the failed event, and the result said "Saga completed
 * successfully with 2 events" about a run whose status was FAILED.
 */
class DeclaredStepFailureTest {

    private static final FailureSignalRegistry NO_DECLARATIONS = new FailureSignalRegistry("");

    @Test
    void aDeclaredFailedEventWithoutReadableReasonIsStatedAsTheStepsFailure() {
        RcaResult result = analyze(SagaStatus.FAILED, NO_DECLARATIONS,
                event("e-1", "ORDER_CREATED", "{}", 0),
                event("e-2", "PAYMENT_FAILED", "{\"status\":\"REJECTED\"}", 1));

        assertThat(result.getRcaStatus()).isEqualTo(RcaStatus.EVIDENCE_AVAILABLE);
        assertThat(result.getRootCause()).isEqualTo("STEP_REPORTED_FAILURE");
        assertThat(result.getRuleId()).isEqualTo("R24");
        assertThat(result.getOutcome()).isEqualTo("FAILED");
        assertThat(result.getEvidence()).contains("PAYMENT_FAILED").doesNotContain("completed successfully");
    }

    /**
     * When the source's own wording is readable, R01/R23 say more than R24 about the same
     * event. R24 must not be built at all, so SC06's promoted label cannot come to depend on
     * how the selector breaks a tie between two claims on one event.
     */
    @Test
    void aReadableDeclaredSignalLeavesNoRoomForR24() {
        RcaResult result = analyze(SagaStatus.FAILED, repositoryDeclarations(),
                event("e-1", "ORDER_CREATED", "{}", 0),
                event("e-2", "PAYMENT_FAILED", "{\"reason\":\"Circuit breaker / retry exhausted: "
                        + "CircuitBreaker 'paymentProcessing' is OPEN and does not permit further calls\"}", 1));

        assertThat(result.getRuleId()).isEqualTo("R01");
        assertThat(result.getCandidates()).extracting(RcaCandidate::getRuleId).doesNotContain("R24");
    }

    /**
     * No declared failed event observed: nothing to state, and the selector's existing
     * NO_DIAGNOSIS_NEEDED for a FAILED run without findings is kept. What changes is that
     * the result no longer reports a FAILED run as a successful one.
     */
    @Test
    void aFailedRunWithoutEvidenceIsNotReportedAsSuccessful() {
        RcaResult result = analyze(SagaStatus.FAILED, NO_DECLARATIONS,
                event("e-1", "ORDER_CREATED", "{}", 0));

        assertThat(result.getRootCause()).isNull();
        assertThat(result.getRcaStatus()).isEqualTo(RcaStatus.NO_DIAGNOSIS_NEEDED);
        assertThat(result.getOutcome()).isEqualTo("FAILED");
        assertThat(result.getEvidence()).doesNotContain("completed successfully");
    }

    /** A failed event on a run that has not ended FAILED is not this claim. */
    @Test
    void aFailedEventOnARunStillInProgressDoesNotFire() {
        RcaResult result = analyze(SagaStatus.IN_PROGRESS, NO_DECLARATIONS,
                event("e-1", "ORDER_CREATED", "{}", 0),
                event("e-2", "PAYMENT_FAILED", "{\"status\":\"REJECTED\"}", 1));

        assertThat(result.getCandidates()).extracting(RcaCandidate::getRuleId).doesNotContain("R24");
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private static RcaResult analyze(SagaStatus status, FailureSignalRegistry failureSignals,
            SagaEvent... events) {
        SagaInstance saga = new SagaInstance();
        saga.setSagaId("saga-1");
        saga.setWorkflowType("ORDER_SAGA");
        saga.setStatus(status);
        saga.setStartTime(events[0].getTimestamp());

        SagaInstanceRepository sagas = mock(SagaInstanceRepository.class);
        when(sagas.findByIdForUpdate(anyString())).thenReturn(Optional.of(saga));
        SagaEventRepository eventRepo = mock(SagaEventRepository.class);
        when(eventRepo.findBySagaIdOrderByTimestampAsc(anyString())).thenReturn(List.of(events));
        AnomalyRepository anomalies = mock(AnomalyRepository.class);
        when(anomalies.findStandingBySagaId(anyString())).thenReturn(List.of());
        RcaResultRepository results = mock(RcaResultRepository.class);
        when(results.save(any(RcaResult.class))).thenAnswer(call -> call.getArgument(0));

        WorkflowRegistry workflows = new WorkflowRegistry();
        workflows.load();
        RuleRegistry rules = new RuleRegistry();
        rules.load();

        return new RcaService(sagas, eventRepo, anomalies, results, workflows, new ObjectMapper(),
                new RcaSelector(), rules, new DetectionProperties(), mock(AlertSink.class),
                new ExecutionProjectionService(), failureSignals, null).analyze("saga-1");
    }

    /** The declarations production reads, as Sc06ReplayTest does, not a copy. */
    private static FailureSignalRegistry repositoryDeclarations() {
        Path here = Path.of("").toAbsolutePath();
        Path root = Files.exists(here.resolve("declarations")) ? here : here.getParent();
        FailureSignalRegistry registry = new FailureSignalRegistry(root.resolve("declarations").toString());
        registry.load();
        return registry;
    }

    private static SagaEvent event(String id, String type, String payload, int second) {
        SagaEvent event = new SagaEvent();
        event.setSagaId("saga-1");
        event.setSourceName("default");
        event.setEventId(id);
        event.setEventType(type);
        event.setPayload(payload);
        event.setTimestamp(Instant.parse("2026-01-01T00:00:00Z").plusSeconds(second));
        return event;
    }
}
