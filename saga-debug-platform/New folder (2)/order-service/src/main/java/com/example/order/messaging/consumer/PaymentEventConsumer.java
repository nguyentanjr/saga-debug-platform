package com.example.order.messaging.consumer;

import com.example.saga.events.PaymentCompletedEvent;
import com.example.saga.events.PaymentFailedEvent;
import com.example.saga.events.PaymentRefundedEvent;
import com.example.order.service.OrderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PaymentEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventConsumer.class);

    private final OrderService orderService;

    public PaymentEventConsumer(OrderService orderService) {
        this.orderService = orderService;
    }

    @KafkaListener(topics = "payment-completed", groupId = "order-service-group")
    public void handlePaymentCompleted(PaymentCompletedEvent event) {
        log.info("Received PaymentCompletedEvent for order {} [sagaId={}]", event.getOrderId(), event.getSagaId());
        orderService.markPaymentCompleted(event.getOrderId());
    }

    @KafkaListener(topics = "payment-failed", groupId = "order-service-group")
    public void handlePaymentFailed(PaymentFailedEvent event) {
        log.info("Received PaymentFailedEvent for order {} [sagaId={}]", event.getOrderId(), event.getSagaId());
        orderService.markPaymentFailed(event.getOrderId());
    }

    @KafkaListener(topics = "payment-refunded", groupId = "order-service-group")
    public void handlePaymentRefunded(PaymentRefundedEvent event) {
        log.info("Received PaymentRefundedEvent for order {} [sagaId={}]", event.getOrderId(), event.getSagaId());
        orderService.markRefunded(event.getOrderId());
    }
}
