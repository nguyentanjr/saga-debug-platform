-- Identity is the pair (source, id), not the id.

-- Executions were keyed by the raw identifier a source published, on the unstated
-- assumption that identifiers are unique across everything the platform watches. Two
-- observed systems are two id spaces. ORD-1 in one and ORD-1 in another are different runs
-- that share a string, and filing them together produced one fabricated execution whose
-- events interleaved two systems -- after which the detectors correctly reported a
-- terminal saga resuming, a state regression, and a step that took twelve hours. Every one
-- of those findings was manufactured by the observer and attributed to the observed.

-- The same held for event ids: a global UNIQUE meant the second system to publish an event
-- called "1" had it silently discarded as a duplicate of the first system's.

-- saga_id now stores source::rawId. A single column stays the primary key, so every query,
-- path variable and dashboard link keeps working, and uniqueness becomes a property of the
-- value rather than of a constraint someone must remember to write. source_name is stored
-- beside it and a CHECK ties the two together, so the invariant holds for writers that
-- never went through ExecutionKey.

-- Namespacing lengthens every key by the source name plus a separator.
ALTER TABLE saga_instances ALTER COLUMN saga_id  TYPE VARCHAR(300);
ALTER TABLE saga_events    ALTER COLUMN saga_id  TYPE VARCHAR(300);
ALTER TABLE saga_events    ALTER COLUMN event_id TYPE VARCHAR(300);
ALTER TABLE anomalies      ALTER COLUMN saga_id  TYPE VARCHAR(300);
ALTER TABLE rca_results    ALTER COLUMN saga_id  TYPE VARCHAR(300);

-- Everything already stored came in before mappings existed or through the identity
-- mapping, which is the default source.
ALTER TABLE saga_instances ADD COLUMN source_name VARCHAR(120);
ALTER TABLE saga_events    ADD COLUMN source_name VARCHAR(120);

UPDATE saga_instances SET source_name = 'default' WHERE source_name IS NULL;
UPDATE saga_events    SET source_name = 'default' WHERE source_name IS NULL;

-- Existing keys are rewritten into their namespaced form. Guarded so the migration is
-- idempotent against a database that somehow already holds namespaced values.
UPDATE saga_instances SET saga_id = 'default::' || saga_id WHERE saga_id NOT LIKE '%::%';
UPDATE saga_events    SET saga_id = 'default::' || saga_id WHERE saga_id NOT LIKE '%::%';
UPDATE anomalies      SET saga_id = 'default::' || saga_id WHERE saga_id NOT LIKE '%::%';
UPDATE rca_results    SET saga_id = 'default::' || saga_id WHERE saga_id NOT LIKE '%::%';

ALTER TABLE saga_instances ALTER COLUMN source_name SET NOT NULL;
ALTER TABLE saga_events    ALTER COLUMN source_name SET NOT NULL;

-- The invariant, enforced where it cannot be forgotten: the key carries its own namespace,
-- and the namespace is the source recorded beside it.
ALTER TABLE saga_instances
    ADD CONSTRAINT saga_instances_saga_id_is_namespaced
    CHECK (saga_id LIKE source_name || '::%');
ALTER TABLE saga_events
    ADD CONSTRAINT saga_events_saga_id_is_namespaced
    CHECK (saga_id LIKE source_name || '::%');

-- An event id identifies an observation only within the system that issued it.
ALTER TABLE saga_events DROP CONSTRAINT IF EXISTS saga_events_event_id_key;
ALTER TABLE saga_events
    ADD CONSTRAINT saga_events_source_event_id_key UNIQUE (source_name, event_id);

-- "Every execution of this observed system", which is how a multi-system deployment reads
-- one system at a time.
CREATE INDEX IF NOT EXISTS idx_saga_instances_source ON saga_instances (source_name);
CREATE INDEX IF NOT EXISTS idx_saga_events_source    ON saga_events (source_name);

-- On what grounds each observation was filed under its execution: a declared key,
-- propagated trace context, or a link from an event that carried two identifiers. Every
-- finding downstream is a statement about a sequence, and this says how sound the sequence
-- is. Nullable for rows written before it was recorded.
ALTER TABLE saga_events ADD COLUMN correlation_basis VARCHAR(32);

-- Aliases are scoped too, and for the same reason: an alias table keyed on a bare
-- identifier is a cross-system merge waiting to happen. Two systems both using PAY-9 would
-- have resolved to each other's executions, and the platform would have folded two
-- unrelated runs into one and then diagnosed the result.
ALTER TABLE execution_aliases ADD COLUMN source_name VARCHAR(120);
UPDATE execution_aliases SET source_name = 'default' WHERE source_name IS NULL;
ALTER TABLE execution_aliases ALTER COLUMN source_name SET NOT NULL;

UPDATE execution_aliases SET execution_id = 'default::' || execution_id
    WHERE execution_id NOT LIKE '%::%';

ALTER TABLE execution_aliases ALTER COLUMN execution_id TYPE VARCHAR(300);
ALTER TABLE execution_aliases DROP CONSTRAINT execution_aliases_pkey;
ALTER TABLE execution_aliases ADD PRIMARY KEY (source_name, alias_key);
