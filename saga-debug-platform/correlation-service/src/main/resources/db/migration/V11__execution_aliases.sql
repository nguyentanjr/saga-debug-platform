-- Identifiers that name the same execution.

-- The platform could correlate observations only when every service publishing them
-- agreed on one identifier. Systems built around a saga orchestrator do agree; systems
-- that were not -- which is most of the ones worth attaching a debugger to -- do not.
-- The order service knows an order number, the payment service knows a payment id, and
-- exactly one event carries both.

-- Without this table those events fell into two unrelated executions, and the platform
-- reported two half-finished sagas instead of one whole one. The remedy on offer was to
-- ask the observed system to propagate a shared id: a change to code this platform is
-- only supposed to watch.

-- Each row is an observed fact, not an inference: some event carried both identifiers,
-- and linked_by_event_id says which. Nothing here is derived from timing or from
-- similarity, so a wrong join cannot be produced by a coincidence.
CREATE TABLE execution_aliases (
    alias_key          VARCHAR(200) PRIMARY KEY,
    execution_id       VARCHAR(200) NOT NULL,
    linked_by_event_id VARCHAR(200),
    observed_at        TIMESTAMP    NOT NULL
);

-- Merging two chains that were built independently walks every alias of the losing id.
CREATE INDEX IF NOT EXISTS idx_execution_aliases_execution
    ON execution_aliases (execution_id);
