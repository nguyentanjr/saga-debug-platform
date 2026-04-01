package com.example.order.domain;

public enum OrderStatus {
    CREATED,
    PAYMENT_COMPLETED,
    PAYMENT_FAILED,
    INVENTORY_RESERVED,
    INVENTORY_FAILED,
    REFUNDING,
    COMPLETED,
    FAILED
}

