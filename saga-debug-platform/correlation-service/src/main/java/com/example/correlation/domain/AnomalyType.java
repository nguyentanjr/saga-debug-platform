package com.example.correlation.domain;

public enum AnomalyType {
    // Execution
    SAGA_STUCK,
    STEP_TIMEOUT,
    MISSING_TERMINAL_EVENT,

    // Ordering
    OUT_OF_ORDER,
    STATE_REGRESSION,
    FORBIDDEN_POST_TERMINAL,

    // Duplication
    EXACT_DUPLICATE_EVENT,
    DUPLICATE_COMPENSATION,

    // State transition
    INVALID_TRANSITION,
    MULTI_TERMINAL_CONFLICT,

    // Compensation
    COMPENSATION_MISSING,
    MISSING_COMPENSATION_ACK,

    // Correlation
    ORPHAN_EVENT,
    MISSING_TRACE_CONTEXT,

    // Messaging
    RETRY_BURST,
    CONSUMER_LAG_HIGH,  // Placeholder — no detector implemented. R16 checks but never fires.

    // Compensation timing
    DELAYED_COMPENSATION
}
