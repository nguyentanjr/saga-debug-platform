ALTER TABLE rca_results
    ALTER COLUMN confidence DROP NOT NULL;

ALTER TABLE rca_results
    ADD COLUMN rca_status VARCHAR(40),
    ADD COLUMN primary_root_causes JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN candidates JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN missing_evidence JSONB NOT NULL DEFAULT '[]'::jsonb;

UPDATE rca_results
SET rca_status = CASE
        WHEN root_cause IS NOT NULL THEN 'SELECTED'
        WHEN outcome = 'NORMAL' THEN 'NO_DIAGNOSIS_NEEDED'
        ELSE 'INSUFFICIENT_EVIDENCE'
    END,
    primary_root_causes = CASE
        WHEN root_cause IS NOT NULL THEN jsonb_build_array(root_cause)
        ELSE '[]'::jsonb
    END;

ALTER TABLE rca_results
    ALTER COLUMN rca_status SET NOT NULL;
