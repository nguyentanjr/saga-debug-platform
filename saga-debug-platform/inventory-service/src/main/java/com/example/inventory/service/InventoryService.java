package com.example.inventory.service;

import com.example.inventory.domain.Inventory;
import com.example.inventory.domain.InventoryReservation;
import com.example.inventory.domain.InventoryReservationStatus;
import com.example.inventory.dto.InventoryResponse;
import com.example.inventory.exception.NotFoundException;
import com.example.saga.events.InventoryFailedEvent;
import com.example.saga.events.InventoryReservedEvent;
import com.example.saga.events.PaymentCompletedEvent;
import com.example.inventory.messaging.producer.InventoryEventProducer;
import com.example.inventory.repository.InventoryRepository;
import com.example.inventory.repository.InventoryReservationRepository;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Service
public class InventoryService {

    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);

    private final InventoryRepository inventoryRepository;
    private final InventoryReservationRepository reservationRepository;
    private final InventoryEventProducer inventoryEventProducer;

    public InventoryService(InventoryRepository inventoryRepository,
                            InventoryReservationRepository reservationRepository,
                            InventoryEventProducer inventoryEventProducer) {
        this.inventoryRepository = inventoryRepository;
        this.reservationRepository = reservationRepository;
        this.inventoryEventProducer = inventoryEventProducer;
    }

    @Transactional
    @WithSpan("saga.inventory.reserve")
    @CircuitBreaker(name = "inventoryProcessing", fallbackMethod = "handlePaymentCompletedFallback")
    @Retry(name = "inventoryProcessing")
    public void handlePaymentCompleted(PaymentCompletedEvent event) {
        Span.current().setAttribute("order.id", event.getOrderId().toString());
        if (event.getSagaId() != null) {
            Span.current().setAttribute("saga.id", event.getSagaId().toString());
        }
        Span.current().setAttribute("amount", event.getAmount().toString());
        Span.current().setAttribute("product.id", event.getProductId());
        Span.current().setAttribute("quantity", event.getQuantity());

        
        if (reservationRepository.findByOrderId(event.getOrderId()).isPresent()) {
            log.info("Inventory reservation already exists for order {}. Skipping.", event.getOrderId());
            return;
        }

        
        String productId = event.getProductId();
        int quantity = event.getQuantity();

        Inventory inventory = inventoryRepository.findByProductId(productId)
                .orElse(null);

        if (inventory == null || inventory.getAvailableQuantity() < quantity) {
            InventoryReservation reservation = new InventoryReservation();
            reservation.setId(UUID.randomUUID());
            reservation.setOrderId(event.getOrderId());
            reservation.setProductId(productId);
            reservation.setQuantity(quantity);
            reservation.setStatus(InventoryReservationStatus.FAILED);
            reservation.setCreatedAt(Instant.now());
            reservationRepository.save(reservation);

            InventoryFailedEvent failedEvent = new InventoryFailedEvent();
            failedEvent.setOrderId(event.getOrderId());
            failedEvent.setSagaId(event.getSagaId());
            failedEvent.setProductId(productId);
            failedEvent.setQuantity(quantity);
            failedEvent.setReason("Not enough inventory");
            inventoryEventProducer.publishInventoryFailed(failedEvent);

            log.info("Inventory reservation FAILED for order {} (product={}, requested={}, available={})",
                    event.getOrderId(), productId, quantity,
                    inventory != null ? inventory.getAvailableQuantity() : 0);
            return;
        }

        inventory.setAvailableQuantity(inventory.getAvailableQuantity() - quantity);
        inventoryRepository.save(inventory);

        InventoryReservation reservation = new InventoryReservation();
        reservation.setId(UUID.randomUUID());
        reservation.setOrderId(event.getOrderId());
        reservation.setProductId(productId);
        reservation.setQuantity(quantity);
        reservation.setStatus(InventoryReservationStatus.RESERVED);
        reservation.setCreatedAt(Instant.now());
        reservationRepository.save(reservation);

        InventoryReservedEvent reservedEvent = new InventoryReservedEvent();
        reservedEvent.setOrderId(event.getOrderId());
        reservedEvent.setSagaId(event.getSagaId());
        reservedEvent.setProductId(productId);
        reservedEvent.setQuantity(quantity);
        inventoryEventProducer.publishInventoryReserved(reservedEvent);

        log.info("Reserved inventory for order {} (product={}, qty={})", event.getOrderId(), productId, quantity);
    }

    public InventoryResponse getInventory(String productId) {
        Inventory inventory = inventoryRepository.findByProductId(productId)
                .orElseThrow(() -> new NotFoundException("Inventory not found for product " + productId));
        return new InventoryResponse(inventory.getProductId(), inventory.getAvailableQuantity());
    }

    


    public void handlePaymentCompletedFallback(PaymentCompletedEvent event, Throwable t) {
        log.error("Circuit breaker / retry fallback for order {}: {}", event.getOrderId(), t.getMessage());

        InventoryFailedEvent failedEvent = new InventoryFailedEvent();
        failedEvent.setOrderId(event.getOrderId());
        failedEvent.setSagaId(event.getSagaId());
        failedEvent.setProductId(event.getProductId());
        failedEvent.setQuantity(event.getQuantity());
        failedEvent.setReason("Circuit breaker open: " + t.getMessage());
        inventoryEventProducer.publishInventoryFailed(failedEvent);
    }
}
