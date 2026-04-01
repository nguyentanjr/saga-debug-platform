package com.example.saga.events;

import java.util.UUID;

public class InventoryReservedEvent extends BaseEvent {

    private UUID orderId;
    private String productId;
    private int quantity;

    public InventoryReservedEvent() {
        super("INVENTORY_RESERVED", "RESERVE_INVENTORY", "inventory-service");
    }

    public UUID getOrderId() { return orderId; }
    public void setOrderId(UUID orderId) { this.orderId = orderId; }

    public String getProductId() { return productId; }
    public void setProductId(String productId) { this.productId = productId; }

    public int getQuantity() { return quantity; }
    public void setQuantity(int quantity) { this.quantity = quantity; }
}
