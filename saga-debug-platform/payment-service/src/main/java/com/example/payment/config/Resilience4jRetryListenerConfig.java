package com.example.payment.config;

import com.example.payment.messaging.producer.PaymentEventProducer;
import com.example.saga.events.RetryAttemptedEvent;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.UUID;











@Configuration
public class Resilience4jRetryListenerConfig {

    private static final Logger log = LoggerFactory.getLogger(Resilience4jRetryListenerConfig.class);
    private static final String RETRY_INSTANCE = "paymentProcessing";

    @Bean
    public ApplicationRunner registerPaymentRetryListener(
            RetryRegistry retryRegistry,
            PaymentEventProducer producer) {
        return args -> {
            Retry retry = retryRegistry.retry(RETRY_INSTANCE);
            retry.getEventPublisher().onRetry(event -> {
                String sagaIdStr = MDC.get("sagaId");
                String orderIdStr = MDC.get("orderId");
                if (sagaIdStr == null) {
                    log.warn("Retry attempt #{} for '{}' but sagaId missing in MDC — PAYMENT_RETRY NOT emitted",
                            event.getNumberOfRetryAttempts(), RETRY_INSTANCE);
                    return;
                }

                RetryAttemptedEvent retryEvent = new RetryAttemptedEvent();
                try {
                    retryEvent.setSagaId(UUID.fromString(sagaIdStr));
                    if (orderIdStr != null) retryEvent.setOrderId(UUID.fromString(orderIdStr));
                } catch (IllegalArgumentException ex) {
                    log.warn("MDC sagaId/orderId not a valid UUID; skipping PAYMENT_RETRY emit ({}, {})",
                            sagaIdStr, orderIdStr);
                    return;
                }
                retryEvent.setAttemptNumber(event.getNumberOfRetryAttempts());
                Throwable t = event.getLastThrowable();
                retryEvent.setLastException(t != null
                        ? t.getClass().getSimpleName() + ": " + t.getMessage()
                        : null);

                producer.publishRetryAttempted(retryEvent);
                log.info("Emitted PAYMENT_RETRY for saga={}, attempt={}",
                        sagaIdStr, retryEvent.getAttemptNumber());
            });
            log.info("Resilience4j retry listener registered for instance '{}'", RETRY_INSTANCE);
        };
    }
}
