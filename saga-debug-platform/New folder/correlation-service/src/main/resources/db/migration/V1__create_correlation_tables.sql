CREATE TABLE saga_instances (
    saga_id VARCHAR(36) PRIMARY KEY,
    workflow_type VARCHAR(50) NOT NULL DEFAULT 'ORDER_SAGA',
    status VARCHAR(20) NOT NULL,
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ,
    last_updated TIMESTAMPTZ NOT NULL
);

CREATE TABLE saga_events (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(100) NOT NULL UNIQUE,
    saga_id VARCHAR(36) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    step_name VARCHAR(50),
    service_name VARCHAR(50),
    timestamp TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL
);

CREATE TABLE rca_results (
    id BIGSERIAL PRIMARY KEY,
    saga_id VARCHAR(36) NOT NULL,
    root_cause VARCHAR(100) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    evidence JSONB NOT NULL,
    suggestions JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes
CREATE INDEX idx_saga_instances_status ON saga_instances(status);
CREATE INDEX idx_saga_events_saga_id ON saga_events(saga_id);
CREATE INDEX idx_rca_results_saga_id ON rca_results(saga_id);
