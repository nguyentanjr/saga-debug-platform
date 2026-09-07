-- Identifier columns sized for this platform's own demo, not for the systems it observes.

-- saga_id was VARCHAR(36): the width of a UUID, which is what the three demo services
-- happen to emit. Every correlation key from an observed system had to fit that. A real
-- one often does not -- "ORDER-2026-NORTHERN-REGION-0000012345678" is 40 characters -- and
-- what happened then was not a clear refusal but a 500 from a constraint violation deep
-- inside the write, after the mapping had read the event correctly.

-- The platform asking a system to shorten its order numbers is the same demand as asking
-- it to rename its fields, in a less obvious place. 200 matches execution_aliases, which
-- was written without that assumption.
ALTER TABLE saga_instances    ALTER COLUMN saga_id  TYPE VARCHAR(200);
ALTER TABLE saga_events       ALTER COLUMN saga_id  TYPE VARCHAR(200);
ALTER TABLE anomalies         ALTER COLUMN saga_id  TYPE VARCHAR(200);
ALTER TABLE rca_results       ALTER COLUMN saga_id  TYPE VARCHAR(200);

-- Event ids are the source's own too: an outbox row id, a message id, a partition-offset
-- pair. 100 was generous for the demo and arbitrary for anyone else.
ALTER TABLE saga_events       ALTER COLUMN event_id TYPE VARCHAR(200);

-- The business identifier an operator searches by, which is the least constrained value
-- in the system: whatever the observed domain calls its reference.
ALTER TABLE saga_instances    ALTER COLUMN order_id TYPE VARCHAR(200);
