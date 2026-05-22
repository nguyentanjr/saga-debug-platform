package com.example.order.service;

import com.example.order.domain.Order;
import com.example.order.domain.OrderStatus;
import com.example.order.dto.CreateOrderRequest;
import com.example.order.dto.OrderResponse;
import com.example.order.exception.NotFoundException;
import com.example.saga.events.OrderCreatedEvent;
import com.example.saga.events.RefundPaymentCommand;
import com.example.order.messaging.producer.OrderEventProducer;
import com.example.order.repository.OrderRepository;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository orderRepository;
    private final OrderEventProducer orderEventProducer;

    public OrderService(OrderRepository orderRepository, OrderEventProducer orderEventProducer) {
        this.orderRepository = orderRepository;
        this.orderEventProducer = orderEventProducer;
    }

    @WithSpan("saga.order.create")
    public OrderResponse createOrder(CreateOrderRequest request) {

        UUID id = UUID.randomUUID();
        UUID sagaId = UUID.randomUUID();
        Instant now = Instant.now();

        Span.current().setAttribute("order.id", id.toString());
        Span.current().setAttribute("saga.id", sagaId.toString());
        Span.current().setAttribute("customer.id", request.getCustomerId());
        Span.current().setAttribute("product.id", request.getProductId());
        Span.current().setAttribute("quantity", request.getQuantity());
        Span.current().setAttribute("amount", request.getAmount().toString());

        Order order = new Order();
        order.setId(id);
        order.setSagaId(sagaId);
        order.setCustomerId(request.getCustomerId());
        order.setProductId(request.getProductId());
        order.setQuantity(request.getQuantity());
        order.setAmount(request.getAmount());
        order.setStatus(OrderStatus.CREATED);
        order.setCreatedAt(now);
        order.setUpdatedAt(now);

        orderRepository.save(order);
        log.info("Order {} created with status {}", id, order.getStatus());

        OrderCreatedEvent event = new OrderCreatedEvent();
        event.setOrderId(id);
        event.setSagaId(sagaId);
        event.setCustomerId(order.getCustomerId());
        event.setProductId(order.getProductId());
        event.setQuantity(order.getQuantity());
        event.setAmount(order.getAmount());

        orderEventProducer.publishOrderCreated(event);

        return toResponse(order);
    }

    public OrderResponse getOrder(UUID id) {
        Order order = orderRepository.findById(id)
                .orElseThrow(() -> new NotFoundException("Order not found: " + id));
        return toResponse(order);
    }

    @WithSpan("saga.order.markPaymentCompleted")
    @Transactional
    public void markPaymentCompleted(UUID orderId) {
        Span.current().setAttribute("order.id", orderId.toString());
        Span.current().setAttribute("saga.step", "PROCESS_PAYMENT");
        orderRepository.findById(orderId).ifPresent(order -> {
            if (isFinalStatus(order.getStatus())) {
                log.info("Skipping: order {} already in final status {}", orderId, order.getStatus());
                return;
            }
            order.setStatus(OrderStatus.PAYMENT_COMPLETED);
            order.setUpdatedAt(Instant.now());
            orderRepository.save(order);
            log.info("Order {} updated to PAYMENT_COMPLETED", orderId);
        });
    }

    @WithSpan("saga.order.markPaymentFailed")
    @Transactional
    public void markPaymentFailed(UUID orderId) {
        Span.current().setAttribute("order.id", orderId.toString());
        Span.current().setAttribute("saga.step", "PROCESS_PAYMENT");
        Span.current().setAttribute("saga.step.status", "FAILED");
        orderRepository.findById(orderId).ifPresent(order -> {
            if (isFinalStatus(order.getStatus())) {
                log.info("Skipping: order {} already in final status {}", orderId, order.getStatus());
                return;
            }
            order.setStatus(OrderStatus.FAILED);
            order.setUpdatedAt(Instant.now());
            orderRepository.save(order);
            log.info("Order {} updated to FAILED (payment failed)", orderId);
        });
    }

    @WithSpan("saga.order.markInventoryReserved")
    @Transactional
    public void markInventoryReserved(UUID orderId) {
        Span.current().setAttribute("order.id", orderId.toString());
        Span.current().setAttribute("saga.step", "RESERVE_INVENTORY");
        orderRepository.findById(orderId).ifPresent(order -> {
            if (isFinalStatus(order.getStatus())) {
                log.info("Skipping: order {} already in final status {}", orderId, order.getStatus());
                return;
            }
            order.setStatus(OrderStatus.COMPLETED);
            order.setUpdatedAt(Instant.now());
            orderRepository.save(order);
            log.info("Order {} updated to COMPLETED", orderId);
        });
    }

    @WithSpan("saga.order.markInventoryFailed")
    @Transactional
    public void markInventoryFailed(UUID orderId, UUID sagaId) {
        Span.current().setAttribute("order.id", orderId.toString());
        if (sagaId != null) Span.current().setAttribute("saga.id", sagaId.toString());
        Span.current().setAttribute("saga.step", "RESERVE_INVENTORY");
        Span.current().setAttribute("saga.step.status", "FAILED");
        Span.current().setAttribute("saga.compensation", "REFUND_PAYMENT");
        orderRepository.findById(orderId).ifPresent(order -> {
            if (isFinalStatus(order.getStatus())) {
                log.info("Skipping: order {} already in final status {}", orderId, order.getStatus());
                return;
            }
            
            order.setStatus(OrderStatus.REFUNDING);
            order.setUpdatedAt(Instant.now());
            orderRepository.save(order);
            log.info("Order {} updated to REFUNDING, triggering payment refund", orderId);

            RefundPaymentCommand refundCommand = new RefundPaymentCommand();
            refundCommand.setOrderId(orderId);
            refundCommand.setSagaId(sagaId);
            refundCommand.setReason("Inventory reservation failed");
            orderEventProducer.publishRefundPayment(refundCommand);
        });
    }

    @WithSpan("saga.order.markRefunded")
    @Transactional
    public void markRefunded(UUID orderId) {
        Span.current().setAttribute("order.id", orderId.toString());
        Span.current().setAttribute("saga.step", "REFUND_PAYMENT");
        orderRepository.findById(orderId).ifPresent(order -> {
            if (order.getStatus() != OrderStatus.REFUNDING) {
                log.info("Skipping refund completion: order {} not in REFUNDING status (current: {})",
                        orderId, order.getStatus());
                return;
            }
            order.setStatus(OrderStatus.FAILED);
            order.setUpdatedAt(Instant.now());
            orderRepository.save(order);
            log.info("Order {} updated to FAILED (refund completed)", orderId);
        });
    }

    private boolean isFinalStatus(OrderStatus status) {
        return status == OrderStatus.COMPLETED || status == OrderStatus.FAILED;
    }

    private OrderResponse toResponse(Order order) {
        OrderResponse dto = new OrderResponse();
        dto.setId(order.getId());
        dto.setSagaId(order.getSagaId());
        dto.setCustomerId(order.getCustomerId());
        dto.setProductId(order.getProductId());
        dto.setQuantity(order.getQuantity());
        dto.setAmount(order.getAmount());
        dto.setStatus(order.getStatus());
        dto.setCreatedAt(order.getCreatedAt());
        dto.setUpdatedAt(order.getUpdatedAt());
        return dto;
    }
}
