package com.example.payment.messaging.producer;

import com.example.saga.events.BaseEvent;
import com.example.saga.events.PaymentCompletedEvent;
import com.example.saga.events.PaymentFailedEvent;
import com.example.saga.events.PaymentRefundedEvent;
import com.example.saga.events.RetryAttemptedEvent;
import io.opentelemetry.api.trace.Span;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class PaymentEventProducer {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventProducer.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public PaymentEventProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishPaymentCompleted(PaymentCompletedEvent event) {
        enrichTraceContext(event);
        ProducerRecord<String, Object> record = new ProducerRecord<>("payment-completed", event.getOrderId().toString(), event);
        addStandardHeaders(record, event);
        kafkaTemplate.send(record)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish PaymentCompleted for {}: {}", event.getOrderId(), ex.getMessage());
                    } else {
                        log.info("Published PaymentCompleted for {} [sagaId={}, eventId={}]",
                                event.getOrderId(), event.getSagaId(), event.getEventId());
                    }
                });
    }

    public void publishPaymentFailed(PaymentFailedEvent event) {
        enrichTraceContext(event);
        ProducerRecord<String, Object> record = new ProducerRecord<>("payment-failed", event.getOrderId().toString(), event);
        addStandardHeaders(record, event);
        kafkaTemplate.send(record)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish PaymentFailed for {}: {}", event.getOrderId(), ex.getMessage());
                    } else {
                        log.info("Published PaymentFailed for {} [sagaId={}]", event.getOrderId(), event.getSagaId());
                    }
                });
    }

    public void publishPaymentRefunded(PaymentRefundedEvent event) {
        enrichTraceContext(event);
        ProducerRecord<String, Object> record = new ProducerRecord<>("payment-refunded", event.getOrderId().toString(), event);
        addStandardHeaders(record, event);
        kafkaTemplate.send(record)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish PaymentRefunded for {}: {}", event.getOrderId(), ex.getMessage());
                    } else {
                        log.info("Published PaymentRefunded for {} [sagaId={}]", event.getOrderId(), event.getSagaId());
                    }
                });
    }

    public void publishRetryAttempted(RetryAttemptedEvent event) {
        enrichTraceContext(event);
        String key = event.getOrderId() != null
                ? event.getOrderId().toString()
                : event.getSagaId().toString();
        ProducerRecord<String, Object> record =
                new ProducerRecord<>("payment-retry", key, event);
        addStandardHeaders(record, event);
        kafkaTemplate.send(record)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish PaymentRetry for saga {}: {}",
                                event.getSagaId(), ex.getMessage());
                    } else {
                        log.info("Published PaymentRetry for saga {} [attempt={}]",
                                event.getSagaId(), event.getAttemptNumber());
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
