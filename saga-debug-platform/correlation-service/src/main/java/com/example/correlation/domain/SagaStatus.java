package com.example.correlation.domain;

public enum SagaStatus {
    STARTED,
    IN_PROGRESS,
    COMPLETED,
    FAILED,
    COMPENSATING,
    COMPENSATED;

    /**
     * How far through its life a run in this status has got.
     *
     * <p>A run's status only ever moves up this ordering. A move down it is a claim about
     * the observed system — the thing {@code STATE_REGRESSION} reports — so the ordering
     * has to be a property of the status itself rather than a private table inside one
     * detector, which is where it used to live even though the finding it raised already
     * cited {@code code:SagaStatus.rank} as its source.
     *
     * <p>{@code FAILED} ranks below the two settled outcomes because a failed step can
     * still be compensated; {@code COMPLETED} is the highest because nothing follows it.
     */
    public int progressRank() {
        return switch (this) {
            case STARTED -> 1;
            case IN_PROGRESS -> 2;
            case COMPENSATING -> 3;
            case FAILED -> 4;
            case COMPENSATED -> 5;
            case COMPLETED -> 6;
        };
    }

    /** Whether the run is over: no further work is expected under this status. */
    public boolean isTerminal() {
        return this == COMPLETED || this == FAILED || this == COMPENSATED;
    }
}
