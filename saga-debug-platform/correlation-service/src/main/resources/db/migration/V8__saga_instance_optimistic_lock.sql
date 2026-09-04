-- Optimistic locking column for saga_instances.
--
-- processEvent reads a saga, derives its next status, and writes it back. With more
-- than one writer — the Kafka consumer and the admin inject endpoint today, several
-- consumer threads once listener concurrency is raised — two of those read-modify-write
-- cycles can interleave and one update is silently lost.
--
-- Hibernate increments this column on every update and fails the second writer, which
-- the consumer surfaces as a retryable error rather than a lost event.
ALTER TABLE saga_instances ADD COLUMN version BIGINT NOT NULL DEFAULT 0;
