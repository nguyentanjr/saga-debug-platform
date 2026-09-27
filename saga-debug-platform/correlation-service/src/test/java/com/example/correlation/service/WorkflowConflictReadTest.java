package com.example.correlation.service;

import com.example.correlation.controller.SagaController;
import com.example.correlation.domain.*;
import com.example.correlation.domain.rca.RcaStatus;
import com.example.correlation.execution.ExecutionLookup;
import com.example.correlation.execution.ExecutionProjectionService;
import com.example.correlation.repository.*;
import com.example.correlation.service.alert.AlertSink;
import com.example.correlation.service.rca.*;
import com.example.correlation.workflow.WorkflowRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class WorkflowConflictReadTest {
    private final SagaInstanceRepository instances = mock(SagaInstanceRepository.class);
    private final SagaEventRepository events = mock(SagaEventRepository.class);
    private final AnomalyRepository anomalies = mock(AnomalyRepository.class);
    private final RcaResultRepository results = mock(RcaResultRepository.class);
    private final WorkflowRegistry workflows = new WorkflowRegistry();
    private final ObjectMapper json = new ObjectMapper().findAndRegisterModules();
    private final AlertSink alerts = mock(AlertSink.class);
    private final SagaInstance saga = conflicted();

    private static SagaInstance conflicted() {
        SagaInstance saga = new SagaInstance();
        saga.setSagaId("default::conflict");
        saga.setWorkflowType("ORDER_SAGA");
        saga.recordWorkflowConflict("LOAN_APPROVAL");
        saga.setStatus(SagaStatus.IN_PROGRESS);
        saga.setStartTime(Instant.parse("2026-01-01T00:00:00Z"));
        saga.setLastUpdated(saga.getStartTime());
        return saga;
    }

    @Test
    void analysisRefusesEvenWithOldStrongFindingsAndDoesNotAlert() {
        when(instances.findByIdForUpdate(saga.getSagaId())).thenReturn(Optional.of(saga));
        RcaResult result = rca().analyze(saga.getSagaId());
        assertRefusal(result);
        verify(results).save(result);
        verifyNoInteractions(anomalies, events, alerts);
    }

    @Test
    void readingRcaDoesNotServeOrMutateAPreConflictDiagnosis() {
        when(instances.findById(saga.getSagaId())).thenReturn(Optional.of(saga));
        assertRefusal(rca().getResults(saga.getSagaId()).get(0));
        verifyNoInteractions(results, alerts);
    }

    @Test
    void detailKeepsObservedEventsAndQualifiesHistoricalFindings() throws Exception {
        ExecutionLookup lookup = mock(ExecutionLookup.class);
        when(lookup.resolve(saga.getSagaId())).thenReturn(Optional.of(saga.getSagaId()));
        when(instances.findById(saga.getSagaId())).thenReturn(Optional.of(saga));
        SagaEvent observation = new SagaEvent();
        when(events.findBySagaIdOrderByTimestampAsc(saga.getSagaId())).thenReturn(List.of(observation));
        Anomaly old = new Anomaly();
        old.setSagaId(saga.getSagaId());
        old.setAnomalyType(AnomalyType.INVALID_TRANSITION);
        when(anomalies.findBySagaId(saga.getSagaId())).thenReturn(List.of(old));
        RcaResult stale = new RcaResult();
        stale.setRootCause("INVALID_TRANSITION");
        when(results.findBySagaIdOrderByCreatedAtDesc(saga.getSagaId())).thenReturn(List.of(stale));
        SagaController controller = new SagaController(instances, events, anomalies, results,
                mock(ExecutionAliasRepository.class), lookup);
        var detail = controller.getSagaDetail(saga.getSagaId()).getBody();
        assertThat(detail.getEvents()).containsExactly(observation);
        assertThat(detail.getAnomalies()).containsExactly(old);
        assertThat(old.getRetractedAt()).isNull();
        assertThat(old.getWorkflowConflicts()).containsExactly("LOAN_APPROVAL", "ORDER_SAGA");
        assertRefusal(detail.getRcaResults().get(0));
        assertThat(json.readTree(json.writeValueAsString(detail)).at("/workflowConflicts/0").asText())
                .isEqualTo("LOAN_APPROVAL");
        when(instances.findAll(any(Pageable.class))).thenReturn(new PageImpl<>(List.of(saga)));
        assertThat(controller.listSagas(null, null, null, 0, 20).getBody().getContent().get(0)
                .getWorkflowConflicts()).containsExactly("LOAN_APPROVAL", "ORDER_SAGA");
        verify(results, never()).save(any());
    }

    @Test
    void scheduledDetectorsDoNotTreatAConflictAsSourceFailure() {
        workflows.load();
        when(instances.findByStatusIn(any(), any())).thenReturn(List.of(saga));
        when(instances.findByStatusInAndLastUpdatedBefore(any(), any(), any())).thenReturn(List.of(saga));
        when(instances.findByStatusInAndLastUpdatedAfter(any(), any(), any())).thenReturn(List.of(saga));
        when(events.countBySagaId(saga.getSagaId())).thenReturn(2L);
        SagaEvent last = new SagaEvent();
        last.setTimestamp(saga.getStartTime());
        last.setReceivedAt(saga.getStartTime());
        last.setEventType("ORDER_CREATED");
        when(events.findTopBySagaIdOrderByTimestampDesc(saga.getSagaId())).thenReturn(Optional.of(last));
        AnomalyDetectionService detector = new AnomalyDetectionService(instances, events, anomalies,
                workflows, json, new DetectionProperties(), mock(ExecutionProjectionService.class));
        detector.detectStuckSagas();
        detector.detectMissingTerminalEvents();
        detector.detectStepTimeouts();
        detector.detectCompensationMissing();
        detector.detectMissingCompensationAck();
        detector.detectDelayedCompensation();
        verify(anomalies, never()).save(any());
    }

    @Test
    void dashboardDoesNotCountHistoricalConflictDiagnosesAsCurrentEvidence() {
        Anomaly old = new Anomaly();
        old.setSagaId(saga.getSagaId());
        RcaResult stale = new RcaResult();
        stale.setSagaId(saga.getSagaId());
        stale.setRcaStatus(RcaStatus.EVIDENCE_AVAILABLE);
        when(anomalies.findByDetectedAtAfter(any())).thenReturn(List.of(old));
        when(results.findByCreatedAtAfter(any())).thenReturn(List.of(stale));
        when(instances.findAllById(any())).thenReturn(List.of(saga));
        when(instances.countUncontestedByStatusSince(eq("IN_PROGRESS"), any())).thenReturn(1L);
        when(instances.countByStatusAndStartTimeAfter(any(), any())).thenReturn(2L);
        var summary = new DashboardMetricsService(instances, anomalies, results).summarize(24);
        assertThat(summary.activeSagas()).isEqualTo(1);
        assertThat(summary.totalAnomalies()).isZero();
        assertThat(summary.rcaSelected()).isZero();
        assertThat(summary.topRootCauses()).isEmpty();
    }

    private RcaService rca() {
        return new RcaService(instances, events, anomalies, results, workflows, json,
                new RcaSelector(), mock(RuleRegistry.class), new DetectionProperties(), alerts,
                mock(ExecutionProjectionService.class), new FailureSignalRegistry(""), null);
    }

    private void assertRefusal(RcaResult result) {
        assertThat(result.getRcaStatus()).isEqualTo(RcaStatus.INSUFFICIENT_EVIDENCE);
        assertThat(result.getOutcome()).isEqualTo("UNDETERMINED");
        assertThat(result.getRootCause()).isNull();
        assertThat(result.getCandidates()).isEmpty();
        assertThat(result.getMissingEvidence()).anySatisfy(reason ->
                assertThat(reason).contains("LOAN_APPROVAL", "ORDER_SAGA"));
    }
}
