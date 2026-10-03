-- Which deployment a lifecycle fact belongs to.
--
-- A service name alone is not an identity once the platform watches more than one system:
-- the demo saga and the hotel saga both run a "payment-service", in different Compose
-- projects, so a restart of one would be read as the other's crash. The same reason
-- execution keys are scoped by source (ADR 0002).
--
-- For Docker this is the Compose project label; a pushed report may give its own. Null when
-- the source said nothing. A workflow that declares runtimeScope only reads its own scope;
-- one that does not refuses to use a service name that more than one scope reported.
ALTER TABLE service_lifecycle_events
    ADD COLUMN scope VARCHAR(255);
