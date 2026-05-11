-- Add new columns to rca_results
ALTER TABLE rca_results ADD COLUMN outcome VARCHAR(30) NOT NULL DEFAULT 'NORMAL';
ALTER TABLE rca_results ADD COLUMN confidence DOUBLE PRECISION NOT NULL DEFAULT 1.0;
ALTER TABLE rca_results ADD COLUMN rule_id VARCHAR(50);
ALTER TABLE rca_results ALTER COLUMN root_cause DROP NOT NULL;

-- Add order_id to saga_instances for multi-key correlation
ALTER TABLE saga_instances ADD COLUMN order_id VARCHAR(100);
CREATE INDEX idx_saga_instances_order_id ON saga_instances(order_id);
