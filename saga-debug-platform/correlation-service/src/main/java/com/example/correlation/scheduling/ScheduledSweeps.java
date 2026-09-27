package com.example.correlation.scheduling;

import java.time.Duration;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.example.correlation.service.AnomalyDetectionService;
import com.example.correlation.service.RcaService;
import com.example.correlation.service.RetentionService;

/**
 * Everything in this service that runs on a clock rather than on an arriving event.
 *
 * <h2>Why the schedule lives here and not on the detectors</h2>
 * A detector answers "what does the evidence say about this run"; it should not also
 * decide how often anyone asks, or whether this particular process is the one allowed to
 * ask. Those are deployment questions, and keeping them out of the detectors is what lets
 * a test call {@code detectStuckSagas()} directly and get an answer without a database
 * lease, a scheduler, or a second instance existing.
 *
 * <p>Each sweep is wrapped in {@link SweepLease}, so running two instances does not run
 * two of every pass. The reason that matters is not the wasted work: the sweeps are
 * check-then-act over shared rows, so a duplicated pass can fabricate a duplicate finding,
 * or re-raise one that the other pass has just retracted. See {@link SweepLease}.
 *
 * <h2>Choosing a lease</h2>
 * Each lease is set well above what its sweep takes, because an overrun past the lease is
 * exactly the window in which a second instance can join. They are deliberately not
 * configurable: a lease is a statement about how long the code takes, not an operational
 * preference, and a deployment that needs to tune one has a sweep that is too slow.
 */
@Component
public class ScheduledSweeps {

    /**
     * Detection passes are bounded by {@code anomaly.detection.batch-size} and run in
     * seconds; two minutes is a wide margin over a slow one on a loaded database.
     */
    private static final Duration DETECTION_LEASE = Duration.ofMinutes(2);

    /**
     * RCA analyses each saga in its own transaction and can have a long backlog to work
     * through after a restart, so its lease is longer than the detectors'.
     */
    private static final Duration RCA_LEASE = Duration.ofMinutes(5);

    /**
     * Retention deletes in batches, children first, inside one transaction; on a database
     * with a large backlog that is the slowest thing this service does.
     */
    private static final Duration RETENTION_LEASE = Duration.ofMinutes(30);

    private final SweepLease lease;
    private final AnomalyDetectionService detection;
    private final RcaService rca;
    /**
     * Retention is opt-in ({@code retention.enabled}), so on most deployments there is no
     * bean to call. Asking for it lazily keeps that choice where it already was — in one
     * property — instead of making every other sweep depend on it being switched on.
     */
    private final ObjectProvider<RetentionService> retention;

    public ScheduledSweeps(SweepLease lease, AnomalyDetectionService detection,
                           RcaService rca, ObjectProvider<RetentionService> retention) {
        this.lease = lease;
        this.detection = detection;
        this.rca = rca;
        this.retention = retention;
    }

    @Scheduled(fixedRateString = "${anomaly.detection.schedule-rate-ms:15000}")
    public void detectStuckSagas() {
        lease.runExclusively("detect-stuck-sagas", DETECTION_LEASE, detection::detectStuckSagas);
    }

    @Scheduled(fixedRateString = "${anomaly.detection.schedule-rate-ms:15000}")
    public void detectStepTimeouts() {
        lease.runExclusively("detect-step-timeouts", DETECTION_LEASE, detection::detectStepTimeouts);
    }

    @Scheduled(fixedRateString = "${anomaly.detection.schedule-rate-ms:15000}")
    public void detectMissingTerminalEvents() {
        lease.runExclusively("detect-missing-terminal-events", DETECTION_LEASE,
                detection::detectMissingTerminalEvents);
    }

    @Scheduled(fixedRateString = "${anomaly.detection.schedule-rate-ms:15000}")
    public void detectCompensationMissing() {
        lease.runExclusively("detect-compensation-missing", DETECTION_LEASE,
                detection::detectCompensationMissing);
    }

    @Scheduled(fixedRateString = "${anomaly.detection.schedule-rate-ms:15000}")
    public void detectMissingCompensationAck() {
        lease.runExclusively("detect-missing-compensation-ack", DETECTION_LEASE,
                detection::detectMissingCompensationAck);
    }

    @Scheduled(fixedRateString = "${anomaly.detection.schedule-rate-ms:15000}")
    public void detectDelayedCompensation() {
        lease.runExclusively("detect-delayed-compensation", DETECTION_LEASE,
                detection::detectDelayedCompensation);
    }

    @Scheduled(fixedRateString = "${rca.auto-analysis.fixed-rate-ms:30000}")
    public void autoAnalyzeNewlyDetectedAnomalies() {
        lease.runExclusively("auto-analyze-anomalies", RCA_LEASE,
                rca::autoAnalyzeNewlyDetectedAnomalies);
    }

    @Scheduled(fixedRateString = "${rca.auto-analysis.fixed-rate-ms:30000}")
    public void autoAnalyzeEndedRuns() {
        lease.runExclusively("auto-analyze-ended-runs", RCA_LEASE,
                rca::autoAnalyzeEndedRuns);
    }

    /**
     * Does nothing at all when retention is switched off, including taking a lease: an
     * absent purge is not a purge that lost a race, and the table should not say it was.
     *
     * <p>{@code purgeExpiredSagas} carries {@code @Transactional}, and calling it from
     * here goes through the proxy, so the four deletes still commit or roll back together.
     */
    @Scheduled(cron = "${retention.schedule-cron:0 30 3 * * *}")
    public void purgeExpiredSagas() {
        RetentionService enabled = retention.getIfAvailable();
        if (enabled == null) return;
        lease.runExclusively("purge-expired-sagas", RETENTION_LEASE, enabled::purgeExpiredSagas);
    }
}
