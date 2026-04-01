package com.example.saga.events;

import java.util.UUID;

public class RefundPaymentCommand extends BaseEvent {

    private UUID orderId;
    private String reason;

    public RefundPaymentCommand() {
        super("REFUND_PAYMENT", "REFUND_PAYMENT", "order-service");
    }

    public UUID getOrderId() { return orderId; }
    public void setOrderId(UUID orderId) { this.orderId = orderId; }

    public String getReason() { return reason; }
    public void setReason(String reason) { this.reason = reason; }
}
