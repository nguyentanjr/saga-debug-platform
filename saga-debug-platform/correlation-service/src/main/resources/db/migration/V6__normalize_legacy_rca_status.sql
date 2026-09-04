-- Score-free model dropped the legacy single-primary states.
-- Normalize any historical rows so Hibernate can still read them after
-- SELECTED / CONTRIBUTING were removed from the RcaStatus / CandidateStatus enums.

-- 1. rca_status column (enum stored as text)
UPDATE rca_results
SET rca_status = 'EVIDENCE_AVAILABLE'
WHERE rca_status = 'SELECTED';

-- 2. candidates JSONB — rewrite the per-candidate "status" enum values.
--    Cover both spaced and compact JSON renderings to be safe.
UPDATE rca_results
SET candidates = replace(replace(replace(replace(
        candidates::text,
        '"status": "SELECTED"',     '"status": "STRONG_EVIDENCE"'),
        '"status":"SELECTED"',      '"status":"STRONG_EVIDENCE"'),
        '"status": "CONTRIBUTING"', '"status": "SUPPORTING_EVIDENCE"'),
        '"status":"CONTRIBUTING"',  '"status":"SUPPORTING_EVIDENCE"')::jsonb
WHERE candidates::text LIKE '%"SELECTED"%'
   OR candidates::text LIKE '%"CONTRIBUTING"%';
