package com.example.correlation.ingestion;

import com.example.correlation.domain.IngestedEvent;
import com.example.correlation.messaging.EventTimeResolver;
import com.example.correlation.service.CorrelationService;
import com.example.correlation.service.DeduplicationCache;
import com.example.correlation.service.EventSchemaValidator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;

/**
 * Turns an observation from any source into a correlated event.
 *
 * <p>The single place where mapping, schema checks, deduplication and the decision to
 * accept or reject live. Transports are adapters around it and hold no ingestion logic of
 * their own, so adding one is writing an adapter rather than reimplementing this.
 *
 * <p>The distinction that matters to every caller: a rejection is returned, a transient
 * failure is thrown. Returning normally on a database outage is what used to lose events —
 * the transport read it as success and moved its cursor past them.
 */
@Service
public class EventIngestionService {

    private static final Logger log = LoggerFactory.getLogger(EventIngestionService.class);

    private final CorrelationService correlationService;
    private final ObjectMapper objectMapper;
    private final EventSchemaValidator schemaValidator;
    private final DeduplicationCache deduplicationCache;
    private final EventTimeResolver eventTimeResolver;
    private final SourceMappingRegistry sourceMappings;

    public EventIngestionService(CorrelationService correlationService,
            ObjectMapper objectMapper,
            EventSchemaValidator schemaValidator,
            DeduplicationCache deduplicationCache,
            EventTimeResolver eventTimeResolver,
            SourceMappingRegistry sourceMappings) {
        this.correlationService = correlationService;
        this.objectMapper = objectMapper;
        this.schemaValidator = schemaValidator;
        this.deduplicationCache = deduplicationCache;
        this.eventTimeResolver = eventTimeResolver;
        this.sourceMappings = sourceMappings;
    }

    /**
     * @throws RuntimeException when storing failed for a reason that may not recur; the
     *                          observation is deliberately left unmarked so the transport
     *                          can present it again
     */
    public IngestionResult ingest(RawObservation raw) {
        SourceMapping mapping = sourceMappings.forTopic(raw.sourceRef());

        JsonNode payload;
        try {
            payload = objectMapper.readTree(raw.payload());
        } catch (Exception e) {
            log.error("Discarding unreadable observation from {}: {}",
                    raw.sourceRef(), e.getMessage());
            return IngestionResult.unreadable(e.getMessage());
        }

        List<String> violations = schemaValidator.validate(payload);
        if (!violations.isEmpty()) {
            log.warn("Observation from {} has schema violations: {}. Processing anyway.",
                    raw.sourceRef(), violations);
        }

        String executionId = mapping.executionId(payload)
                .orElse(raw.correlationHint());
        if (executionId == null) {
            String reason = "no value at '" + mapping.getFields().getExecutionId()
                    + "' and no correlation hint";
            log.error("Cannot correlate observation from {}: {}. Declare the correlation "
                    + "key for this source under ingestion/*.yaml.", raw.sourceRef(), reason);
            return IngestionResult.notCorrelatable(reason);
        }

        String eventId = mapping.eventId(payload).orElse(raw.fallbackEventId());
        if (eventId == null) {
            String reason = "no value at '" + mapping.getFields().getEventId()
                    + "' and the transport offered no identity";
            log.error("Cannot identify observation from {}: {}", raw.sourceRef(), reason);
            return IngestionResult.notCorrelatable(reason);
        }

        if (deduplicationCache.seen(eventId)) {
            log.debug("Skipping already-processed event {} from {}", eventId, raw.sourceRef());
            return IngestionResult.duplicate(eventId);
        }

        EventTimeResolver.Resolved resolved = eventTimeResolver.resolve(
                payload, raw.transportTime(), mapping.getFields().getEventTime());
        if (!resolved.isObserved()) {
            log.warn("Event {} from {} has no usable timestamp at '{}'; using transport time {}",
                    eventId, raw.sourceRef(), mapping.getFields().getEventTime(),
                    raw.transportTime());
        }

        IngestedEvent observation = new IngestedEvent(
                eventId,
                executionId,
                mapping.eventType(payload).orElse(raw.sourceRef()),
                mapping.stepName(payload).orElse("UNKNOWN"),
                mapping.serviceName(payload).orElse("unknown"),
                resolved.eventTime(),
                raw.payload(),
                Instant.now(),
                mapping.causationId(payload).orElse(null),
                mapping.spanId(payload).orElse(null),
                mapping.parentSpanId(payload).orElse(null));

        try {
            correlationService.processEvent(observation);
        } catch (RuntimeException e) {
            log.error("Failed to process event {} from {}: {}",
                    eventId, raw.sourceRef(), e.getMessage(), e);
            throw e;
        }

        // Marked only now: the transaction inside processEvent has committed.
        deduplicationCache.markProcessed(eventId);
        return IngestionResult.accepted(eventId);
    }
}
