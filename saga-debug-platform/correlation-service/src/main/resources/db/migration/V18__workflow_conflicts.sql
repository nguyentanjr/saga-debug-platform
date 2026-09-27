-- Positive declarations that disagree must survive restart and alias merge (I8).
-- The previously selected workflow is retained for inspection, not as an authority.
-- Old rows/writers default to no recorded conflict; do not invent historical evidence.
ALTER TABLE saga_instances ADD COLUMN workflow_conflicts JSONB NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE saga_instances ADD CONSTRAINT chk_workflow_conflicts_array
    CHECK (jsonb_typeof(workflow_conflicts) = 'array' AND jsonb_array_length(workflow_conflicts) <> 1);
