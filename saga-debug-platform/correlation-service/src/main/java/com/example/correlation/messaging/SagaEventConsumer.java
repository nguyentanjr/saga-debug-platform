package com.example.correlation.messaging;

import com.example.correlation.service.CorrelationService;
import com.example.correlation.service.DeduplicationCache;
import com.example.correlation.service.EventSchemaValidator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;

@Component
public class SagaEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(SagaEventConsumer.class);

    private final CorrelationService correlationService;
    private final ObjectMapper objectMapper;
    private final EventSchemaValidator schemaValidator;
    private final DeduplicationCache deduplicationCache;

    private static final Map<String, String> TOPIC_EVENT_MAP = Map.ofEntries(
            Map.entry("order-created",      "ORDER_CREATED"),
            Map.entry("payment-completed",  "PAYMENT_COMPLETED"),
            Map.entry("payment-failed",     "PAYMENT_FAILED"),
            Map.entry("payment-retry",      "PAYMENT_RETRY"),
            Map.entry("inventory-reserved", "INVENTORY_RESERVED"),
            Map.entry("inventory-failed",   "INVENTORY_FAILED"),
            Map.entry("refund-payment",     "REFUND_PAYMENT"),
            Map.entry("payment-refunded",   "PAYMENT_REFUNDED"));

    private static final Map<String, String> TOPIC_STEP_MAP = Map.ofEntries(
            Map.entry("order-created",      "CREATE_ORDER"),
            Map.entry("payment-completed",  "PROCESS_PAYMENT"),
            Map.entry("payment-failed",     "PROCESS_PAYMENT"),
            Map.entry("payment-retry",      "PROCESS_PAYMENT"),
            Map.entry("inventory-reserved", "RESERVE_INVENTORY"),
            Map.entry("inventory-failed",   "RESERVE_INVENTORY"),
            Map.entry("refund-payment",     "REFUND_PAYMENT"),
            Map.entry("payment-refunded",   "REFUND_PAYMENT"));

    private static final Map<String, String> TOPIC_SERVICE_MAP = Map.ofEntries(
            Map.entry("order-created",      "order-service"),
            Map.entry("payment-completed",  "payment-service"),
            Map.entry("payment-failed",     "payment-service"),
            Map.entry("payment-retry",      "payment-service"),
            Map.entry("inventory-reserved", "inventory-service"),
            Map.entry("inventory-failed",   "inventory-service"),
            Map.entry("refund-payment",     "order-service"),
            Map.entry("payment-refunded",   "payment-service"));

    public SagaEventConsumer(CorrelationService correlationService, ObjectMapper objectMapper,
            EventSchemaValidator schemaValidator, DeduplicationCache deduplicationCache) {
        this.correlationService = correlationService;
        this.objectMapper = objectMapper;
        this.schemaValidator = schemaValidator;
        this.deduplicationCache = deduplicationCache;
    }

    @KafkaListener(topics = {
            "order-created", "payment-completed", "payment-failed", "payment-retry",
            "inventory-reserved", "inventory-failed",
            "refund-payment", "payment-refunded"
    }, groupId = "correlation-engine-group")
    public void handleEvent(ConsumerRecord<String, String> record) {
        Instant receivedAt = Instant.now();
        try {
            String topic = record.topic();
            String eventType = TOPIC_EVENT_MAP.getOrDefault(topic, topic);
            String stepName = TOPIC_STEP_MAP.getOrDefault(topic, "UNKNOWN");
            String serviceName = TOPIC_SERVICE_MAP.getOrDefault(topic, "unknown");

            JsonNode payload = objectMapper.readTree(record.value());

            List<String> violations = schemaValidator.validate(payload);
            if (!violations.isEmpty()) {
                log.warn("Event from topic {} has schema violations: {}. Processing anyway.", topic, violations);
            }
            schemaValidator.logWarnings(payload, topic);

            String sagaId = extractSagaId(payload, record);
            if (sagaId == null) {
                log.warn("No sagaId found in event from topic {}. Using orderId as fallback.", topic);
                sagaId = extractField(payload, "orderId");
            }

            if (sagaId == null) {
                log.error("Cannot correlate event from topic {}: no sagaId or orderId", topic);
                return;
            }

            String eventId = extractField(payload, "eventId");
            if (eventId == null) {
                eventId = topic + "-" + record.partition() + "-" + record.offset();
            }

            
            
            if (deduplicationCache.isDuplicate(eventId)) {
                log.debug("Fast-path dedup in consumer: eventId={}, topic={}", eventId, topic);
                return;
            }

            Instant eventTimestamp = Instant.ofEpochMilli(record.timestamp());

            correlationService.processEvent(
                    eventId, sagaId, eventType, stepName,
                    serviceName, eventTimestamp, record.value(), receivedAt);

        } catch (Exception e) {
            log.error("Failed to process event from topic {}: {}", record.topic(), e.getMessage(), e);
        }
    }

    private String extractSagaId(JsonNode payload, ConsumerRecord<String, String> record) {
        String fromBody = extractField(payload, "sagaId");
        if (fromBody != null)
            return fromBody;

        Header header = record.headers().lastHeader("saga-id");
        if (header != null) {
            return new String(header.value(), StandardCharsets.UTF_8);
        }
        return null;
    }

    private String extractField(JsonNode payload, String fieldName) {
        if (payload.has(fieldName) && !payload.get(fieldName).isNull()) {
            return payload.get(fieldName).asText();
        }
        return null;
    }
}
