package com.example.correlation.messaging;

import com.example.correlation.service.CorrelationService;
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
import java.util.Map;

/**
 * Consumes business events from Kafka topics and maps them to generic saga operations.
 *
 * Mapping:
 *   Kafka Topic           → eventType            → stepName            → serviceName
 *   order-created         → ORDER_CREATED        → CREATE_ORDER        → order-service
 *   payment-completed     → PAYMENT_COMPLETED    → PROCESS_PAYMENT     → payment-service
 *   payment-failed        → PAYMENT_FAILED       → PROCESS_PAYMENT     → payment-service
 *   inventory-reserved    → INVENTORY_RESERVED   → RESERVE_INVENTORY   → inventory-service
 *   inventory-failed      → INVENTORY_FAILED     → RESERVE_INVENTORY   → inventory-service
 *   refund-payment        → REFUND_PAYMENT       → REFUND_PAYMENT      → order-service
 *   payment-refunded      → PAYMENT_REFUNDED     → REFUND_PAYMENT      → payment-service
 */
@Component
public class SagaEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(SagaEventConsumer.class);

    private final CorrelationService correlationService;
    private final ObjectMapper objectMapper;

    private static final Map<String, String> TOPIC_EVENT_MAP = Map.of(
            "order-created", "ORDER_CREATED",
            "payment-completed", "PAYMENT_COMPLETED",
            "payment-failed", "PAYMENT_FAILED",
            "inventory-reserved", "INVENTORY_RESERVED",
            "inventory-failed", "INVENTORY_FAILED",
            "refund-payment", "REFUND_PAYMENT",
            "payment-refunded", "PAYMENT_REFUNDED");

    private static final Map<String, String> TOPIC_STEP_MAP = Map.of(
            "order-created", "CREATE_ORDER",
            "payment-completed", "PROCESS_PAYMENT",
            "payment-failed", "PROCESS_PAYMENT",
            "inventory-reserved", "RESERVE_INVENTORY",
            "inventory-failed", "RESERVE_INVENTORY",
            "refund-payment", "REFUND_PAYMENT",
            "payment-refunded", "REFUND_PAYMENT");

    private static final Map<String, String> TOPIC_SERVICE_MAP = Map.of(
            "order-created", "order-service",
            "payment-completed", "payment-service",
            "payment-failed", "payment-service",
            "inventory-reserved", "inventory-service",
            "inventory-failed", "inventory-service",
            "refund-payment", "order-service",
            "payment-refunded", "payment-service");

    public SagaEventConsumer(CorrelationService correlationService, ObjectMapper objectMapper) {
        this.correlationService = correlationService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = {
            "order-created", "payment-completed", "payment-failed",
            "inventory-reserved", "inventory-failed",
            "refund-payment", "payment-refunded"
    }, groupId = "correlation-engine-group")
    public void handleEvent(ConsumerRecord<String, String> record) {
        try {
            String topic = record.topic();
            String eventType = TOPIC_EVENT_MAP.getOrDefault(topic, topic);
            String stepName = TOPIC_STEP_MAP.getOrDefault(topic, "UNKNOWN");
            String serviceName = TOPIC_SERVICE_MAP.getOrDefault(topic, "unknown");

            JsonNode payload = objectMapper.readTree(record.value());

            // Extract sagaId
            String sagaId = extractField(payload, "sagaId");
            if (sagaId == null) {
                Header header = record.headers().lastHeader("saga-id");
                if (header != null) {
                    sagaId = new String(header.value(), StandardCharsets.UTF_8);
                }
            }
            if (sagaId == null) {
                sagaId = extractField(payload, "orderId");
            }
            if (sagaId == null) {
                log.error("Cannot correlate event from topic {}: no sagaId", topic);
                return;
            }

            String eventId = extractField(payload, "eventId");
            if (eventId == null) {
                eventId = topic + "-" + record.partition() + "-" + record.offset();
            }
            Instant eventTimestamp = Instant.ofEpochMilli(record.timestamp());

            correlationService.processEvent(
                    eventId, sagaId, eventType, stepName,
                    serviceName, eventTimestamp, record.value());

        } catch (Exception e) {
            log.error("Failed to process event from topic {}: {}", record.topic(), e.getMessage(), e);
        }
    }

    private String extractField(JsonNode payload, String fieldName) {
        if (payload.has(fieldName) && !payload.get(fieldName).isNull()) {
            return payload.get(fieldName).asText();
        }
        return null;
    }
}
