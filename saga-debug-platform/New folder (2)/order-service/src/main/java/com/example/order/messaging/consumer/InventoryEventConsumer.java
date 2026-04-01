package com.example.order.messaging.consumer;

import com.example.saga.events.InventoryFailedEvent;
import com.example.saga.events.InventoryReservedEvent;
import com.example.order.service.OrderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class InventoryEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(InventoryEventConsumer.class);

    private final OrderService orderService;

    public InventoryEventConsumer(OrderService orderService) {
        this.orderService = orderService;
    }

    @KafkaListener(topics = "inventory-reserved", groupId = "order-service-group")
    public void handleInventoryReserved(InventoryReservedEvent event) {
        log.info("Received InventoryReservedEvent for order {} [sagaId={}]", event.getOrderId(), event.getSagaId());
        orderService.markInventoryReserved(event.getOrderId());
    }

    @KafkaListener(topics = "inventory-failed", groupId = "order-service-group")
    public void handleInventoryFailed(InventoryFailedEvent event) {
        log.info("Received InventoryFailedEvent for order {} [sagaId={}]", event.getOrderId(), event.getSagaId());
        orderService.markInventoryFailed(event.getOrderId(), event.getSagaId());
    }
}
