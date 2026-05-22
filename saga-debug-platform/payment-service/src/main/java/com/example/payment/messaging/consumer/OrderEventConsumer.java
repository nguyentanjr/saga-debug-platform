package com.example.payment.messaging.consumer;

import com.example.saga.events.OrderCreatedEvent;
import com.example.payment.service.PaymentService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);

    private final PaymentService paymentService;

    public OrderEventConsumer(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    @KafkaListener(topics = "order-created", groupId = "payment-service-group")
    public void handleOrderCreated(OrderCreatedEvent event) {
        log.info("Received OrderCreatedEvent for order {} [sagaId={}]", event.getOrderId(), event.getSagaId());
        
        
        
        
        MDC.put("sagaId", event.getSagaId().toString());
        MDC.put("orderId", event.getOrderId().toString());
        try {
            paymentService.handleOrderCreated(event);
        } finally {
            MDC.remove("sagaId");
            MDC.remove("orderId");
        }
    }
}
