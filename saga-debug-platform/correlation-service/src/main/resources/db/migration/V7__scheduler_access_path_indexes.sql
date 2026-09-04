-- Indexes for the access paths the scheduled detectors and RCA actually use.
--
-- V1 created single-column indexes on saga_id and on timestamp separately. Every
-- hot query filters on saga_id AND orders by time, or filters on status AND a time
-- bound, so those indexes force a sort or a wider scan than necessary. The composite
-- indexes below match the queries as written.

-- findBySagaIdOrderByTimestampAsc / findTopBySagaIdOrderByTimestampDesc
CREATE INDEX IF NOT EXISTS idx_saga_events_saga_id_timestamp
    ON saga_events (saga_id, timestamp);

-- findBySagaIdOrderByReceivedAtAsc (out-of-order detection reads arrival order)
CREATE INDEX IF NOT EXISTS idx_saga_events_saga_id_received_at
    ON saga_events (saga_id, received_at);

-- existsBySagaIdAndAnomalyType, called once per saga per scheduler pass
CREATE INDEX IF NOT EXISTS idx_anomalies_saga_id_type
    ON anomalies (saga_id, anomaly_type);

-- findByDetectedAtAfter and findSagaIdsWithAnomaliesSince (dashboard + auto-RCA)
CREATE INDEX IF NOT EXISTS idx_anomalies_detected_at
    ON anomalies (detected_at DESC);

-- Scheduled detectors select by status with a recency bound. Without these the
-- planner has only idx_saga_instances_status and must filter the time column.
CREATE INDEX IF NOT EXISTS idx_saga_instances_status_last_updated
    ON saga_instances (status, last_updated DESC);

CREATE INDEX IF NOT EXISTS idx_saga_instances_status_end_time
    ON saga_instances (status, end_time DESC);

-- findBySagaIdOrderByCreatedAtDesc
CREATE INDEX IF NOT EXISTS idx_rca_results_saga_id_created_at
    ON rca_results (saga_id, created_at DESC);

-- findByCreatedAtAfter (dashboard summary window)
CREATE INDEX IF NOT EXISTS idx_rca_results_created_at_desc
    ON rca_results (created_at DESC);
