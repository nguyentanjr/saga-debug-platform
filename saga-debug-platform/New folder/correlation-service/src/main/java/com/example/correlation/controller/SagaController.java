package com.example.correlation.controller;

import com.example.correlation.domain.*;
import com.example.correlation.dto.SagaDetailResponse;
import com.example.correlation.dto.SagaListItem;
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

@RestController
@RequestMapping("/api/sagas")
public class SagaController {

    private final SagaInstanceRepository sagaInstanceRepository;
    private final SagaEventRepository sagaEventRepository;
    private final RcaResultRepository rcaResultRepository;

    public SagaController(SagaInstanceRepository sagaInstanceRepository,
                          SagaEventRepository sagaEventRepository,
                          RcaResultRepository rcaResultRepository) {
        this.sagaInstanceRepository = sagaInstanceRepository;
        this.sagaEventRepository = sagaEventRepository;
        this.rcaResultRepository = rcaResultRepository;
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

        Page<SagaListItem> result = sagas.map(saga -> {
            SagaListItem item = new SagaListItem();
            item.setSagaId(saga.getSagaId());
            item.setWorkflowType(saga.getWorkflowType());
            item.setStatus(saga.getStatus());
            item.setStartTime(saga.getStartTime());
            item.setEndTime(saga.getEndTime());
            if (saga.getStartTime() != null && saga.getEndTime() != null) {
                item.setDurationMs(Duration.between(saga.getStartTime(), saga.getEndTime()).toMillis());
            }
            item.setEventCount(sagaEventRepository.countBySagaId(saga.getSagaId()));
            return item;
        });

        return ResponseEntity.ok(result);
    }

    @GetMapping("/{sagaId}")
    public ResponseEntity<SagaDetailResponse> getSagaDetail(@PathVariable("sagaId") String sagaId) {
        SagaInstance saga = sagaInstanceRepository.findById(sagaId).orElse(null);
        if (saga == null) {
            return ResponseEntity.notFound().build();
        }

        List<SagaEvent> events = sagaEventRepository.findBySagaIdOrderByTimestampAsc(sagaId);
        List<RcaResult> rcaResults = rcaResultRepository.findBySagaIdOrderByCreatedAtDesc(sagaId);

        SagaDetailResponse response = new SagaDetailResponse();
        response.setSagaId(saga.getSagaId());
        response.setWorkflowType(saga.getWorkflowType());
        response.setStatus(saga.getStatus());
        response.setStartTime(saga.getStartTime());
        response.setEndTime(saga.getEndTime());
        if (saga.getStartTime() != null && saga.getEndTime() != null) {
            response.setDurationMs(Duration.between(saga.getStartTime(), saga.getEndTime()).toMillis());
        }
        response.setEvents(events);
        response.setRcaResults(rcaResults);

        return ResponseEntity.ok(response);
    }
}
