package com.example.correlation.domain.rca;

/**
 * Overall conclusion of one RCA pass for a saga.
 * EVIDENCE_AVAILABLE   — at least one STRONG/SUPPORTING candidate exists.
 * INSUFFICIENT_EVIDENCE — anomalies present but only weak hypotheses.
 * NO_DIAGNOSIS_NEEDED  — no anomalies; saga is healthy.
 */
public enum RcaStatus {
    EVIDENCE_AVAILABLE,
    INSUFFICIENT_EVIDENCE,
    NO_DIAGNOSIS_NEEDED
}
