package com.example.correlation.scheduling;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * The right to run one scheduled sweep, held by one instance at a time.
 *
 * <h2>Why the sweeps need this at all</h2>
 * Every sweep in this service is a read-then-write over the same rows: find the runs that
 * look stuck, ask whether a finding already exists, write one if it does not. That
 * existence check is check-then-act. Two instances sweeping at the same moment can both
 * read "no finding yet" and both write one, and since ADR 0006 the race has a worse shape
 * as well: one pass can raise a finding that the other pass has just retracted, so the
 * platform ends up reporting something it had already withdrawn — with the withdrawal
 * still on record. Neither outcome is a fact about the observed system; both are the
 * observer's own concurrency showing up as a fault in someone else's saga, which is the
 * class of bug this codebase exists to remove.
 *
 * <h2>What the lease establishes</h2>
 * Acquisition is one conditional UPDATE against {@code scheduler_lease}. The database
 * serialises the two statements, so exactly one of them reports a row changed, and the
 * other instance skips this tick. No instance has to agree with any other about anything.
 *
 * <p>The decision uses the <em>database</em> clock, never the caller's:
 * {@code lease_expires_at} is compared against, and computed from,
 * {@code EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)} inside the same statement. A host whose
 * clock is wrong gets a turn that is shorter or longer than it asked for; it never gets a
 * second copy of the sweep. Making exclusion depend on two hosts agreeing about the time
 * would put the observer's environment back into its conclusions, which is what
 * invariant I9 forbids for the observed system and is no more acceptable here.
 *
 * <h2>What it deliberately does not establish</h2>
 * The lease expires on its own, because a holder that is killed mid-sweep never releases;
 * expiry is what makes crash recovery and the normal path the same statement. The cost is
 * that a sweep which runs <em>longer</em> than its lease can be joined by a second
 * instance. That is why every caller passes a lease comfortably longer than the sweep it
 * guards, and why this class does not pretend otherwise: it bounds duplicate work, it does
 * not make it impossible. Fencing beyond that would need the sweeps themselves to carry a
 * token into every write, which is a much larger change than the race it would close.
 *
 * <p>Nothing here is a distributed transaction (invariant I6). The lease lives in the same
 * local database as the rows the sweeps touch; losing it is allowed to mean nothing more
 * than "skip this tick".
 */
@Component
public class SweepLease {

    private static final Logger log = LoggerFactory.getLogger(SweepLease.class);

    /**
     * Epoch seconds as the database sees them. Written out in every statement rather than
     * read once and passed in, so no value derived from the caller's clock can reach the
     * comparison.
     */
    private static final String DB_EPOCH_NOW = "CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) AS BIGINT)";

    private static final String TAKE_OVER = """
            UPDATE scheduler_lease
               SET holder = ?, token = ?, taken_at = CURRENT_TIMESTAMP,
                   lease_expires_at = %s + ?
             WHERE sweep = ? AND lease_expires_at <= %s
            """.formatted(DB_EPOCH_NOW, DB_EPOCH_NOW);

    private static final String TAKE_FIRST = """
            INSERT INTO scheduler_lease (sweep, holder, token, taken_at, lease_expires_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP, %s + ?)
            """.formatted(DB_EPOCH_NOW);

    private static final String RELEASE = """
            UPDATE scheduler_lease
               SET lease_expires_at = %s
             WHERE sweep = ? AND token = ?
            """.formatted(DB_EPOCH_NOW);

    private final JdbcTemplate jdbc;
    private final String holder;

    @Autowired
    public SweepLease(JdbcTemplate jdbc) {
        this(jdbc, ManagementFactory.getRuntimeMXBean().getName());
    }

    /** Names the holder explicitly, so a test can play two instances against each other. */
    SweepLease(JdbcTemplate jdbc, String holder) {
        this.jdbc = jdbc;
        this.holder = holder;
    }

    /**
     * Runs {@code work} if this instance can take the lease on {@code sweep}, and skips it
     * otherwise.
     *
     * @param lease how long the lease is held. Longer than the sweep can reasonably take:
     *              an overrun past this point lets another instance join in.
     * @return whether the work ran here
     */
    public boolean runExclusively(String sweep, Duration lease, Runnable work) {
        String token = UUID.randomUUID().toString();
        if (!take(sweep, token, lease)) {
            log.debug("Sweep {} skipped: held elsewhere", sweep);
            return false;
        }
        try {
            work.run();
            return true;
        } finally {
            release(sweep, token);
        }
    }

    private boolean take(String sweep, String token, Duration lease) {
        long seconds = Math.max(1, lease.toSeconds());
        if (jdbc.update(TAKE_OVER, holder, token, seconds, sweep) == 1) {
            return true;
        }
        // Either someone holds it, or this sweep has never run anywhere and has no row
        // yet. The insert tells the two apart: a duplicate key means the row exists, so
        // the update above failed for the first reason and this instance does not run.
        try {
            jdbc.update(TAKE_FIRST, sweep, holder, token, seconds);
            return true;
        } catch (DuplicateKeyException alreadyHeld) {
            return false;
        }
    }

    /**
     * Frees the lease so the next tick does not have to wait out the full duration.
     *
     * <p>Matched on token, not on holder: if this sweep overran its lease and another
     * instance has since taken over, the row belongs to that instance and must be left
     * alone. A failure to release is logged and swallowed — the lease expires by itself,
     * so the sweep is late, never stuck.
     */
    private void release(String sweep, String token) {
        try {
            jdbc.update(RELEASE, sweep, token);
        } catch (RuntimeException e) {
            log.warn("Could not release lease on sweep {}; it will expire on its own", sweep, e);
        }
    }
}
