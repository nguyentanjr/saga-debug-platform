package com.example.saga.events;

import java.util.UUID;





public class RetryAttemptedEvent extends BaseEvent {

    private UUID orderId;
    private int attemptNumber;
    private String lastException;

    public RetryAttemptedEvent() {
        super("PAYMENT_RETRY", "PROCESS_PAYMENT", "payment-service");
    }

    public UUID getOrderId() { return orderId; }
    public void setOrderId(UUID orderId) { this.orderId = orderId; }

    public int getAttemptNumber() { return attemptNumber; }
    public void setAttemptNumber(int attemptNumber) { this.attemptNumber = attemptNumber; }

    public String getLastException() { return lastException; }
    public void setLastException(String lastException) { this.lastException = lastException; }
}
