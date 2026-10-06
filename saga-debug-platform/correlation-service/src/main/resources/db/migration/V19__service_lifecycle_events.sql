-- What the container runtime said about the observed system's processes.
--
-- Until now every diagnosis rested on the saga's own events, and a stopped service emits
-- none: silence after a crash, a slow consumer and a dropped network all look the same from
-- there (see R05/R06). The runtime knows which of these happened. One row is one fact it
-- reported: the service's process exited (DOWN) or was started (UP), and when.
--
-- Read by R23 only. Nothing else consults it, so an empty table -- the collector disabled,
-- or a deployment with no runtime to ask -- leaves every diagnosis exactly as before.
CREATE TABLE IF NOT EXISTS service_lifecycle_events (
    id            BIGSERIAL PRIMARY KEY,
    service_name  VARCHAR(255) NOT NULL,
    state         VARCHAR(16)  NOT NULL,
    occurred_at   TIMESTAMP WITH TIME ZONE NOT NULL,
    received_at   TIMESTAMP WITH TIME ZONE NOT NULL,
    source        VARCHAR(64)  NOT NULL,
    detail        VARCHAR(512),
    -- The runtime's own identity for the report, so a replay after a reconnect (the
    -- collector asks for events since its last one) is recorded once.
    external_id   VARCHAR(255) NOT NULL,
    CONSTRAINT uq_service_lifecycle_source_external UNIQUE (source, external_id),
    CONSTRAINT ck_service_lifecycle_state CHECK (state IN ('DOWN', 'UP'))
);

-- R23 asks: what happened to these services up to the end of this run's wait.
CREATE INDEX IF NOT EXISTS idx_service_lifecycle_service_time
    ON service_lifecycle_events (service_name, occurred_at);
