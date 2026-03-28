package com.example.inventory.messaging.producer;

import com.example.saga.events.BaseEvent;
import com.example.saga.events.InventoryFailedEvent;
import com.example.saga.events.InventoryReservedEvent;
import io.opentelemetry.api.trace.Span;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class InventoryEventProducer {

    private static final Logger log = LoggerFactory.getLogger(InventoryEventProducer.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public InventoryEventProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishInventoryReserved(InventoryReservedEvent event) {
        enrichTraceContext(event);
        ProducerRecord<String, Object> record = new ProducerRecord<>("inventory-reserved", event.getOrderId().toString(), event);
        addStandardHeaders(record, event);
        kafkaTemplate.send(record)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish InventoryReserved for {}: {}", event.getOrderId(), ex.getMessage());
                    } else {
                        log.info("Published InventoryReserved for {} [sagaId={}, eventId={}]",
                                event.getOrderId(), event.getSagaId(), event.getEventId());
                    }
                });
    }

    public void publishInventoryFailed(InventoryFailedEvent event) {
        enrichTraceContext(event);
        ProducerRecord<String, Object> record = new ProducerRecord<>("inventory-failed", event.getOrderId().toString(), event);
        addStandardHeaders(record, event);
        kafkaTemplate.send(record)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish InventoryFailed for {}: {}", event.getOrderId(), ex.getMessage());
                    } else {
                        log.info("Published InventoryFailed for {} [sagaId={}]", event.getOrderId(), event.getSagaId());
                    }
                });
    }

    private void enrichTraceContext(BaseEvent event) {
        Span span = Span.current();
        if (span.getSpanContext().isValid()) {
            event.setTraceId(span.getSpanContext().getTraceId());
            event.setSpanId(span.getSpanContext().getSpanId());
        }
    }

    private void addStandardHeaders(ProducerRecord<String, Object> record, BaseEvent event) {
        if (event.getSagaId() != null) {
            record.headers().add("saga-id", event.getSagaId().toString().getBytes());
        }
        record.headers().add("event-type", event.getEventType().getBytes());
    }
}
