package com.example.payment.service;

import com.example.payment.domain.Payment;
import com.example.payment.domain.PaymentStatus;
import com.example.payment.dto.PaymentResponse;
import com.example.payment.exception.NotFoundException;
import com.example.saga.events.OrderCreatedEvent;
import com.example.saga.events.PaymentCompletedEvent;
import com.example.saga.events.PaymentRefundedEvent;
import com.example.saga.events.RefundPaymentCommand;
import com.example.payment.messaging.producer.PaymentEventProducer;
import com.example.payment.repository.PaymentRepository;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final PaymentRepository paymentRepository;
    private final PaymentEventProducer paymentEventProducer;

    public PaymentService(PaymentRepository paymentRepository, PaymentEventProducer paymentEventProducer) {
        this.paymentRepository = paymentRepository;
        this.paymentEventProducer = paymentEventProducer;
    }

    @WithSpan("saga.payment.process_created")
    @Transactional
    @CircuitBreaker(name = "paymentProcessing", fallbackMethod = "handleOrderCreatedFallback")
    @Retry(name = "paymentProcessing")
    public void handleOrderCreated(OrderCreatedEvent event) {

        Span.current().setAttribute("order.id", event.getOrderId().toString());
        Span.current().setAttribute("saga.id", event.getSagaId().toString());
        Span.current().setAttribute("product.id", event.getProductId());
        Span.current().setAttribute("quantity", event.getQuantity());
        Span.current().setAttribute("amount", event.getAmount().toString());

        
        if (paymentRepository.findByOrderId(event.getOrderId()).isPresent()) {
            log.info("Payment already exists for order {}. Skipping.", event.getOrderId());
            return;
        }

        Payment payment = new Payment();
        payment.setId(UUID.randomUUID());
        payment.setOrderId(event.getOrderId());
        payment.setAmount(event.getAmount());
        payment.setStatus(PaymentStatus.PENDING);
        payment.setCreatedAt(Instant.now());

        paymentRepository.save(payment);

        
        payment.setStatus(PaymentStatus.COMPLETED);
        paymentRepository.save(payment);

        PaymentCompletedEvent completedEvent = new PaymentCompletedEvent();
        completedEvent.setOrderId(payment.getOrderId());
        completedEvent.setAmount(payment.getAmount());
        
        completedEvent.setSagaId(event.getSagaId());
        completedEvent.setProductId(event.getProductId());
        completedEvent.setQuantity(event.getQuantity());
        paymentEventProducer.publishPaymentCompleted(completedEvent);

        log.info("Processed payment for order {} with status {}", payment.getOrderId(), payment.getStatus());
    }

    @WithSpan("saga.payment.refund")
    @Transactional
    public void handleRefundPayment(RefundPaymentCommand command) {
        Span.current().setAttribute("order.id", command.getOrderId().toString());
        Span.current().setAttribute("saga.step", "REFUND_PAYMENT");
        Span.current().setAttribute("refund.reason", command.getReason());
        Payment payment = paymentRepository.findByOrderId(command.getOrderId())
                .orElse(null);

        if (payment == null) {
            log.warn("No payment found for order {}. Publishing refunded event anyway.", command.getOrderId());
            PaymentRefundedEvent refundedEvent = new PaymentRefundedEvent();
            refundedEvent.setOrderId(command.getOrderId());
            refundedEvent.setSagaId(command.getSagaId());
            refundedEvent.setAmount(java.math.BigDecimal.ZERO);
            paymentEventProducer.publishPaymentRefunded(refundedEvent);
            return;
        }

        if (payment.getStatus() == PaymentStatus.REFUNDED) {
            log.info("Payment for order {} already refunded. Skipping.", command.getOrderId());
            return;
        }

        payment.setStatus(PaymentStatus.REFUNDED);
        paymentRepository.save(payment);

        PaymentRefundedEvent refundedEvent = new PaymentRefundedEvent();
        refundedEvent.setOrderId(payment.getOrderId());
        refundedEvent.setSagaId(command.getSagaId());
        
        refundedEvent.setAmount(payment.getAmount());
        paymentEventProducer.publishPaymentRefunded(refundedEvent);

        log.info("Refunded payment for order {} (amount: {}, reason: {})",
                payment.getOrderId(), payment.getAmount(), command.getReason());
    }

    public PaymentResponse getPayment(UUID id) {
        Payment payment = paymentRepository.findById(id)
                .orElseThrow(() -> new NotFoundException("Payment not found: " + id));
        return toResponse(payment);
    }

    private PaymentResponse toResponse(Payment payment) {
        PaymentResponse dto = new PaymentResponse();
        dto.setId(payment.getId());
        dto.setOrderId(payment.getOrderId());
        dto.setAmount(payment.getAmount());
        dto.setStatus(payment.getStatus());
        dto.setCreatedAt(payment.getCreatedAt());
        return dto;
    }

    


    private void handleOrderCreatedFallback(OrderCreatedEvent event, Throwable t) {
        log.error("Payment processing failed for order {} after retries: {}", event.getOrderId(), t.getMessage());

        com.example.saga.events.PaymentFailedEvent failedEvent = new com.example.saga.events.PaymentFailedEvent();
        failedEvent.setOrderId(event.getOrderId());
        failedEvent.setSagaId(event.getSagaId());
        failedEvent.setReason("Circuit breaker / retry exhausted: " + t.getMessage());
        paymentEventProducer.publishPaymentFailed(failedEvent);
    }
}
