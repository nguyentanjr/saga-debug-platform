package com.example.correlation.service;

import com.example.correlation.domain.*;
import com.example.correlation.repository.RcaResultRepository;
import com.example.correlation.repository.SagaEventRepository;
import com.example.correlation.repository.SagaInstanceRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Simple Root Cause Analysis Service.
 * Checks saga status and events to determine root cause.
 */
@Service
public class RcaService {

    private static final Logger log = LoggerFactory.getLogger(RcaService.class);
    private static final int STUCK_TIMEOUT_SECONDS = 60;

    private final SagaInstanceRepository sagaInstanceRepository;
    private final SagaEventRepository sagaEventRepository;
    private final RcaResultRepository rcaResultRepository;
    private final ObjectMapper objectMapper;

    public RcaService(SagaInstanceRepository sagaInstanceRepository,
                      SagaEventRepository sagaEventRepository,
                      RcaResultRepository rcaResultRepository,
                      ObjectMapper objectMapper) {
        this.sagaInstanceRepository = sagaInstanceRepository;
        this.sagaEventRepository = sagaEventRepository;
        this.rcaResultRepository = rcaResultRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public RcaResult analyze(String sagaId) {
        SagaInstance saga = sagaInstanceRepository.findById(sagaId).orElse(null);
        if (saga == null) {
            log.warn("Saga {} not found for RCA", sagaId);
            return null;
        }

        List<SagaEvent> events = sagaEventRepository.findBySagaIdOrderByTimestampAsc(sagaId);

        String rootCause = null;
        String severity = null;
        List<String> evidence = new ArrayList<>();
        List<String> suggestions = new ArrayList<>();

        // Rule 1: Check if saga is stuck (non-terminal + last event too old)
        if (rootCause == null && !isTerminal(saga.getStatus())) {
            SagaEvent lastEvent = events.isEmpty() ? null : events.get(events.size() - 1);
            if (lastEvent != null && lastEvent.getTimestamp()
                    .plus(STUCK_TIMEOUT_SECONDS, ChronoUnit.SECONDS).isBefore(Instant.now())) {
                rootCause = "STUCK_SAGA";
                severity = "HIGH";
                evidence.add("No event since " + lastEvent.getTimestamp()
                        + " (last: " + lastEvent.getEventType() + ")");
                evidence.add("Saga status: " + saga.getStatus());
                suggestions.add("Check if consuming service is running");
            }
        }

        // Rule 2: Check compensation status
        if (rootCause == null) {
            boolean hasRefund = events.stream()
                    .anyMatch(e -> "REFUND_PAYMENT".equals(e.getEventType()));
            if (hasRefund) {
                boolean refundDone = events.stream()
                        .anyMatch(e -> "PAYMENT_REFUNDED".equals(e.getEventType()));
                if (saga.getStatus() == SagaStatus.COMPENSATED && refundDone) {
                    rootCause = "COMPENSATED";
                    severity = "LOW";
                    evidence.add("Saga rolled back successfully");
                    suggestions.add("Review why forward step failed");
                } else if (saga.getStatus() == SagaStatus.COMPENSATING) {
                    rootCause = "COMPENSATION_IN_PROGRESS";
                    severity = "HIGH";
                    evidence.add("Saga stuck in COMPENSATING");
                    suggestions.add("Check payment-service refund processing");
                }
            }
        }

        // Fallback
        if (rootCause == null) {
            if (saga.getStatus() == SagaStatus.COMPLETED) {
                rootCause = "HEALTHY";
                severity = "NONE";
                evidence.add("Saga completed with " + events.size() + " events");
                if (saga.getStartTime() != null && saga.getEndTime() != null) {
                    evidence.add("Duration: " + Duration.between(
                            saga.getStartTime(), saga.getEndTime()).toMillis() + "ms");
                }
            } else {
                rootCause = "UNKNOWN";
                severity = "LOW";
                evidence.add("Saga status: " + saga.getStatus());
                suggestions.add("Review events manually");
            }
        }

        // Save result
        try {
            RcaResult result = new RcaResult();
            result.setSagaId(sagaId);
            result.setRootCause(rootCause);
            result.setSeverity(severity);
            result.setEvidence(objectMapper.writeValueAsString(evidence));
            result.setSuggestions(objectMapper.writeValueAsString(suggestions));
            rcaResultRepository.save(result);
            log.info("RCA for saga {}: {}/{}", sagaId, rootCause, severity);
            return result;
        } catch (Exception e) {
            log.error("Failed to save RCA: {}", e.getMessage());
            return null;
        }
    }

    public List<RcaResult> getResults(String sagaId) {
        return rcaResultRepository.findBySagaIdOrderByCreatedAtDesc(sagaId);
    }

    private boolean isTerminal(SagaStatus status) {
        return status == SagaStatus.COMPLETED
                || status == SagaStatus.FAILED
                || status == SagaStatus.COMPENSATED;
    }
}
