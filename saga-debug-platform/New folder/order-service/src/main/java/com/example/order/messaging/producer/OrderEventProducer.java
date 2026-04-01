package com.example.order.messaging.producer;

import com.example.saga.events.BaseEvent;
import com.example.saga.events.OrderCreatedEvent;
import com.example.saga.events.RefundPaymentCommand;
import io.opentelemetry.api.trace.Span;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class OrderEventProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventProducer.class);
    private static final String ORDER_CREATED_TOPIC = "order-created";
    private static final String REFUND_PAYMENT_TOPIC = "refund-payment";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public OrderEventProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishOrderCreated(OrderCreatedEvent event) {
        enrichTraceContext(event);
        ProducerRecord<String, Object> record = new ProducerRecord<>(ORDER_CREATED_TOPIC, event.getOrderId().toString(), event);
        addStandardHeaders(record, event);
        kafkaTemplate.send(record)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish OrderCreated for {}: {}", event.getOrderId(), ex.getMessage());
                    } else {
                        log.info("Published OrderCreated for {} [sagaId={}, eventId={}]",
                                event.getOrderId(), event.getSagaId(), event.getEventId());
                    }
                });
    }

    public void publishRefundPayment(RefundPaymentCommand command) {
        enrichTraceContext(command);
        ProducerRecord<String, Object> record = new ProducerRecord<>(REFUND_PAYMENT_TOPIC, command.getOrderId().toString(), command);
        addStandardHeaders(record, command);
        kafkaTemplate.send(record)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish RefundPayment for {}: {}", command.getOrderId(), ex.getMessage());
                    } else {
                        log.info("Published RefundPayment for {} [sagaId={}]", command.getOrderId(), command.getSagaId());
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
