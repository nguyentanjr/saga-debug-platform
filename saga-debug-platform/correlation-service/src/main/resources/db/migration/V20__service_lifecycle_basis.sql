-- How a lifecycle row's state was established, because the two ways do not support the
-- same claim:
--   PROCESS  the runtime managing the process reported it exiting or starting (Docker die,
--            systemd stopped, a Kubernetes container terminated). The process state is known.
--   PROBE    something outside could or could not reach the service (a health check, up==0).
--            A failed probe is equally what a network partition looks like.
-- R23 names a crash at STRONG_EVIDENCE only on PROCESS; on PROBE it says no more than
-- SUPPORTING_EVIDENCE.
--
-- Existing rows all came from the Docker collector (die/start), so PROCESS is what they are.
ALTER TABLE service_lifecycle_events
    ADD COLUMN basis VARCHAR(16) NOT NULL DEFAULT 'PROCESS';

ALTER TABLE service_lifecycle_events
    ADD CONSTRAINT ck_service_lifecycle_basis CHECK (basis IN ('PROCESS', 'PROBE'));
