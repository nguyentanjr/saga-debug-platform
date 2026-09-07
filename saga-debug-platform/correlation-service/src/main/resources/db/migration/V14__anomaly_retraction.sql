-- Findings the platform has withdrawn, and why.

-- An anomaly is a claim about an execution. Claims of the form "X was never observed"
-- are only ever true of the observations in hand, so folding two executions into one can
-- falsify them: the start event that was missing from a fragment is present in the whole.

-- Before this, a merge left those findings attached to the merged run, still asserting an
-- absence that the merge had just disproved. The alternative on offer was to delete them,
-- which is worse: a debugging platform that silently removes findings it has already
-- reported cannot be trusted about the ones it keeps.

-- Retraction is the third option. The row stays, carrying what was claimed and when, plus
-- the reason it no longer holds. The scheduled detectors raise the finding again on their
-- next pass if it is still true of the whole execution -- so a retraction that turns out
-- to have been premature corrects itself.
ALTER TABLE anomalies
    ADD COLUMN retracted_at     TIMESTAMPTZ,
    ADD COLUMN retraction_reason TEXT;

-- Every read that asks "does this execution already have a finding of this type" has to
-- skip retracted ones, or a retracted finding would suppress the re-detection that is
-- supposed to confirm or deny it.
CREATE INDEX IF NOT EXISTS idx_anomalies_saga_active
    ON anomalies (saga_id, anomaly_type) WHERE retracted_at IS NULL;
