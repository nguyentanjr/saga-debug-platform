package com.example.correlation.domain.rca;

/**
 * Evidence tier assigned to an RCA candidate after ranking.
 * STRONG / SUPPORTING / WEAK express how trustworthy the candidate is;
 * RULED_OUT marks fallback rules suppressed because specific evidence exists.
 */
public enum CandidateStatus {
    STRONG_EVIDENCE,
    SUPPORTING_EVIDENCE,
    WEAK_EVIDENCE,
    RULED_OUT
}
