package com.example.saga.events;

import java.math.BigDecimal;
import java.util.UUID;

public class PaymentRefundedEvent extends BaseEvent {

    private UUID orderId;
    private BigDecimal amount;

    public PaymentRefundedEvent() {
        super("PAYMENT_REFUNDED", "REFUND_PAYMENT", "payment-service");
    }

    public UUID getOrderId() { return orderId; }
    public void setOrderId(UUID orderId) { this.orderId = orderId; }

    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }
}
