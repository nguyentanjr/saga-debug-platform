package com.example.inventory.domain;

import jakarta.persistence.*;

import java.util.UUID;

@Entity
@Table(name = "inventory")
public class Inventory {

    @Id
    private UUID id;

    @Column(nullable = false, unique = true)
    private String productId;

    @Column(nullable = false)
    private int availableQuantity;

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public int getAvailableQuantity() {
        return availableQuantity;
    }

    public void setAvailableQuantity(int availableQuantity) {
        this.availableQuantity = availableQuantity;
    }
}

