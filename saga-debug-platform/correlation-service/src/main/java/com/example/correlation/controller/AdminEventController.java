package com.example.correlation.controller;

import com.example.correlation.service.CorrelationService;
import com.example.correlation.service.RcaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;





@RestController
@RequestMapping("/api/admin")
public class AdminEventController {

    private static final Logger log = LoggerFactory.getLogger(AdminEventController.class);

    private final CorrelationService correlationService;
    private final RcaService rcaService;

    public AdminEventController(CorrelationService correlationService, RcaService rcaService) {
        this.correlationService = correlationService;
        this.rcaService = rcaService;
    }

    



    @PostMapping("/inject-event")
    public ResponseEntity<Map<String, String>> injectEvent(@RequestBody Map<String, String> body) {
        String sagaId = body.getOrDefault("sagaId", UUID.randomUUID().toString());
        String eventType = body.get("eventType");
        String stepName = body.getOrDefault("stepName", "UNKNOWN");
        String serviceName = body.getOrDefault("serviceName", "test-inject");
        String payload = body.getOrDefault("payload", "{}");
        String eventId = body.getOrDefault("eventId",
                "inject-" + UUID.randomUUID().toString().substring(0, 8));
        String timestampStr = body.get("timestamp");

        if (eventType == null) {
            return ResponseEntity.badRequest().body(Map.of("error", "eventType is required"));
        }

        Instant timestamp = timestampStr != null
                ? Instant.parse(timestampStr)
                : Instant.now();

        correlationService.processEvent(new com.example.correlation.domain.IngestedEvent(
                eventId, sagaId, eventType, stepName, serviceName,
                timestamp, payload, Instant.now(),
                body.get("causationId"), body.get("spanId"), body.get("parentSpanId")));

        log.info("Injected event: sagaId={}, eventType={}", sagaId, eventType);

        return ResponseEntity.ok(Map.of(
                "status", "injected",
                "sagaId", sagaId,
                "eventId", eventId,
                "eventType", eventType
        ));
    }

    


    @PostMapping("/analyze/{sagaId}")
    public ResponseEntity<?> triggerRca(@PathVariable("sagaId") String sagaId) {
        var result = rcaService.analyze(sagaId);
        if (result == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(result);
    }
}
