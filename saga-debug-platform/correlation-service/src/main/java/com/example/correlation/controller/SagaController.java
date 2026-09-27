package com.example.correlation.controller;

import com.example.correlation.domain.*;
import com.example.correlation.dto.SagaDetailResponse;
import com.example.correlation.execution.ExecutionLookup;
import com.example.correlation.dto.SagaListItem;
import com.example.correlation.repository.AnomalyRepository;
import com.example.correlation.repository.RcaResultRepository;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/sagas")
public class SagaController {

    private final SagaInstanceRepository sagaInstanceRepository;
    private final SagaEventRepository sagaEventRepository;
    private final AnomalyRepository anomalyRepository;
    private final RcaResultRepository rcaResultRepository;
    private final com.example.correlation.repository.ExecutionAliasRepository aliasRepository;
    private final ExecutionLookup executionLookup;

    public SagaController(SagaInstanceRepository sagaInstanceRepository,
                          SagaEventRepository sagaEventRepository,
                          AnomalyRepository anomalyRepository,
                          RcaResultRepository rcaResultRepository,
                          com.example.correlation.repository.ExecutionAliasRepository aliasRepository,
                          ExecutionLookup executionLookup) {
        this.sagaInstanceRepository = sagaInstanceRepository;
        this.sagaEventRepository = sagaEventRepository;
        this.anomalyRepository = anomalyRepository;
        this.rcaResultRepository = rcaResultRepository;
        this.aliasRepository = aliasRepository;
        this.executionLookup = executionLookup;
    }

    @GetMapping
    public ResponseEntity<Page<SagaListItem>> listSagas(
            @RequestParam(value = "status", required = false) String status,
            @RequestParam(value = "startTime", required = false) Instant startTime,
            @RequestParam(value = "endTime", required = false) Instant endTime,
            @RequestParam(value = "page", defaultValue = "0") int page,
            @RequestParam(value = "size", defaultValue = "20") int size) {

        PageRequest pageRequest = PageRequest.of(page, size, Sort.by(Sort.Direction.DESC, "startTime"));

        Page<SagaInstance> sagas;
        if (status != null && startTime != null && endTime != null) {
            sagas = sagaInstanceRepository.findByStatusAndStartTimeBetween(
                    SagaStatus.valueOf(status), startTime, endTime, pageRequest);
        } else if (status != null) {
            sagas = sagaInstanceRepository.findByStatus(SagaStatus.valueOf(status), pageRequest);
        } else if (startTime != null && endTime != null) {
            sagas = sagaInstanceRepository.findByStartTimeBetween(startTime, endTime, pageRequest);
        } else {
            sagas = sagaInstanceRepository.findAll(pageRequest);
        }

        
        List<String> sagaIds = sagas.getContent().stream()
                .map(SagaInstance::getSagaId)
                .collect(Collectors.toList());
        Map<String, Long> eventCounts = sagaIds.isEmpty() ? Map.of()
                : sagaEventRepository.countBySagaIdIn(sagaIds).stream()
                    .collect(Collectors.toMap(
                            r -> (String) r[0],
                            r -> (Long) r[1]));

        Page<SagaListItem> result = sagas.map(saga -> {
            SagaListItem item = new SagaListItem();
            item.setSagaId(saga.getSagaId());
            item.setWorkflowType(saga.getWorkflowType());
            item.setWorkflowConflicts(saga.getWorkflowConflicts());
            item.setStatus(saga.getStatus());
            item.setStartTime(saga.getStartTime());
            item.setEndTime(saga.getEndTime());
            if (saga.getStartTime() != null && saga.getEndTime() != null) {
                item.setDurationMs(Duration.between(saga.getStartTime(), saga.getEndTime()).toMillis());
            }
            item.setEventCount(eventCounts.getOrDefault(saga.getSagaId(), 0L));
            return item;
        });

        return ResponseEntity.ok(result);
    }

    /**
     * One execution, addressed either by its stored key or by the identifier the observed
     * system issued.
     *
     * <p>The second form is what an operator and the evaluation harness actually hold:
     * they know the id the source published, not the namespace this platform files it
     * under. Resolution is exact rather than a search, and refuses rather than picks when
     * one raw id names runs in two observed systems. See {@link ExecutionLookup}.
     */
    @GetMapping("/{sagaId}")
    public ResponseEntity<SagaDetailResponse> getSagaDetail(@PathVariable("sagaId") String sagaId) {
        String executionId = executionLookup.resolve(sagaId).orElse(null);
        if (executionId == null) {
            return ResponseEntity.notFound().build();
        }
        SagaInstance saga = sagaInstanceRepository.findById(executionId).orElse(null);
        if (saga == null) {
            return ResponseEntity.notFound().build();
        }
        // Everything below reads the run under the key it is actually stored by, never
        // under the string the caller happened to use to ask for it.
        List<SagaEvent> events = sagaEventRepository.findBySagaIdOrderByTimestampAsc(executionId);
        List<Anomaly> anomalies = anomalyRepository.findBySagaId(executionId);
        List<RcaResult> rcaResults =
                rcaResultRepository.findBySagaIdOrderByCreatedAtDesc(executionId);

        SagaDetailResponse response = new SagaDetailResponse();
        response.setSagaId(saga.getSagaId());
        response.setSourceName(saga.getSourceName());
        // Which identifiers this run answers to, and what event linked each one. An
        // execution assembled from services that each knew a different id has to be able
        // to say so rather than presenting itself as one shared id.
        response.setIdentities(aliasRepository.findByExecutionId(executionId).stream()
                .map(alias -> new SagaDetailResponse.IdentityLink(
                        alias.getAliasKey(), alias.getSourceName(), alias.getLinkedByEventId()))
                .toList());
        response.setWorkflowType(saga.getWorkflowType());
        response.setWorkflowConflicts(saga.getWorkflowConflicts());
        response.setStatus(saga.getStatus());
        response.setStartTime(saga.getStartTime());
        response.setEndTime(saga.getEndTime());
        if (saga.getStartTime() != null && saga.getEndTime() != null) {
            response.setDurationMs(Duration.between(saga.getStartTime(), saga.getEndTime()).toMillis());
        }
        response.setEvents(events);
        if (saga.hasWorkflowConflict()) {
            anomalies.forEach(anomaly -> anomaly.setWorkflowConflicts(saga.getWorkflowConflicts()));
            rcaResults = List.of(RcaResult.workflowConflict(saga));
        }
        response.setAnomalies(anomalies);
        response.setRcaResults(rcaResults);

        return ResponseEntity.ok(response);
    }
}
