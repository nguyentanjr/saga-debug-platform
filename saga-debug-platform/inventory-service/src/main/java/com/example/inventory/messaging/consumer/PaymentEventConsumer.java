package com.example.inventory.messaging.consumer;

import com.example.saga.events.PaymentCompletedEvent;
import com.example.inventory.admin.CrashSwitch;
import com.example.inventory.service.InventoryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PaymentEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventConsumer.class);

    private final InventoryService inventoryService;
    private final CrashSwitch crashSwitch;

    public PaymentEventConsumer(InventoryService inventoryService, CrashSwitch crashSwitch) {
        this.inventoryService = inventoryService;
        this.crashSwitch = crashSwitch;
    }

    @KafkaListener(topics = "payment-completed", groupId = "inventory-service-group")
    public void handlePaymentCompleted(PaymentCompletedEvent event) {
        log.info("Received PaymentCompletedEvent for order {}", event.getOrderId());
        crashSwitch.crashIfArmed();
        inventoryService.handlePaymentCompleted(event);
    }
}

