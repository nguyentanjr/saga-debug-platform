ALTER TABLE anomalies
    ADD COLUMN observed_value TEXT,
    ADD COLUMN expected_value TEXT,
    ADD COLUMN threshold_source VARCHAR(120),
    ADD COLUMN evidence_event_ids JSONB,
    ADD COLUMN measured_at TIMESTAMPTZ;
