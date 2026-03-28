package com.example.payment.messaging.consumer;

import com.example.saga.events.RefundPaymentCommand;
import com.example.payment.service.PaymentService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class RefundPaymentConsumer {

    private static final Logger log = LoggerFactory.getLogger(RefundPaymentConsumer.class);

    private final PaymentService paymentService;

    public RefundPaymentConsumer(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    @KafkaListener(topics = "refund-payment", groupId = "payment-service-group")
    public void handleRefundPayment(RefundPaymentCommand command) {
        log.info("Received RefundPaymentCommand for order {} [sagaId={}] (reason: {})",
                command.getOrderId(), command.getSagaId(), command.getReason());
        paymentService.handleRefundPayment(command);
    }
}
