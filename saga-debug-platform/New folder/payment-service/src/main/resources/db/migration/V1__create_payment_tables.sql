CREATE TABLE IF NOT EXISTS payments (
    id         UUID PRIMARY KEY,
    order_id   UUID NOT NULL,
    amount     NUMERIC(10,2) NOT NULL,
    status     VARCHAR(32) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_payments_order_id ON payments(order_id);

