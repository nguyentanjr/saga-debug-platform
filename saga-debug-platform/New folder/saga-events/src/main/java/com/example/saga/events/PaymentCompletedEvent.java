package com.example.saga.events;

import java.math.BigDecimal;
import java.util.UUID;

public class PaymentCompletedEvent extends BaseEvent {

    private UUID orderId;
    private BigDecimal amount;
    private String productId;
    private int quantity;

    public PaymentCompletedEvent() {
        super("PAYMENT_COMPLETED", "PROCESS_PAYMENT", "payment-service");
    }

    public UUID getOrderId() { return orderId; }
    public void setOrderId(UUID orderId) { this.orderId = orderId; }

    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }

    public String getProductId() { return productId; }
    public void setProductId(String productId) { this.productId = productId; }

    public int getQuantity() { return quantity; }
    public void setQuantity(int quantity) { this.quantity = quantity; }
}
