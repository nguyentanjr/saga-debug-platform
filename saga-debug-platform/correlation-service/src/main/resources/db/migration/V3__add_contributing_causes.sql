-- Add contributing_causes column for multi-rule matching support
ALTER TABLE rca_results ADD COLUMN contributing_causes JSONB;
