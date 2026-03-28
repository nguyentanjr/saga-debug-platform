CREATE TABLE IF NOT EXISTS orders (
    id          UUID PRIMARY KEY,
    customer_id VARCHAR(64) NOT NULL,
    product_id  VARCHAR(64) NOT NULL,
    quantity    INT NOT NULL,
    amount      NUMERIC(10,2) NOT NULL,
    status      VARCHAR(32) NOT NULL,
    created_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_orders_id ON orders(id);

