CREATE TABLE IF NOT EXISTS inventory (
    id                 UUID PRIMARY KEY,
    product_id         VARCHAR(64) NOT NULL UNIQUE,
    available_quantity INT NOT NULL
);

CREATE TABLE IF NOT EXISTS inventory_reservation (
    id         UUID PRIMARY KEY,
    order_id   UUID NOT NULL,
    product_id VARCHAR(64) NOT NULL,
    quantity   INT NOT NULL,
    status     VARCHAR(32) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_inventory_reservation_order_id
    ON inventory_reservation(order_id);

-- Seed one product for demo
INSERT INTO inventory (id, product_id, available_quantity)
VALUES (gen_random_uuid(), 'PRODUCT-1', 100)
ON CONFLICT (product_id) DO NOTHING;

