package com.example.saga.events;

import java.math.BigDecimal;
import java.util.UUID;

public class PaymentFailedEvent extends BaseEvent {

    private UUID orderId;
    private BigDecimal amount;
    private String reason;

    public PaymentFailedEvent() {
        super("PAYMENT_FAILED", "PROCESS_PAYMENT", "payment-service");
    }

    public UUID getOrderId() { return orderId; }
    public void setOrderId(UUID orderId) { this.orderId = orderId; }

    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }

    public String getReason() { return reason; }
    public void setReason(String reason) { this.reason = reason; }
}
