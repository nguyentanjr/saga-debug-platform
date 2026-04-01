package com.example.saga.events;

import java.math.BigDecimal;
import java.util.UUID;

public class OrderCreatedEvent extends BaseEvent {

    private UUID orderId;
    private String customerId;
    private String productId;
    private int quantity;
    private BigDecimal amount;

    public OrderCreatedEvent() {
        super("ORDER_CREATED", "CREATE_ORDER", "order-service");
    }

    public UUID getOrderId() { return orderId; }
    public void setOrderId(UUID orderId) { this.orderId = orderId; }

    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }

    public String getProductId() { return productId; }
    public void setProductId(String productId) { this.productId = productId; }

    public int getQuantity() { return quantity; }
    public void setQuantity(int quantity) { this.quantity = quantity; }

    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }
}
