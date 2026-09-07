package com.example.correlation.ingestion;

import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.SimpleTransactionStatus;

/**
 * A transaction manager for tests that have no database.
 *
 * <p>Runs the callback inline and records whether it was committed or rolled back. That is
 * enough for the tests that care about <em>ordering</em> — that identity is resolved before
 * the observation is written, that the deduplication mark happens only after the commit —
 * without a database to commit to.
 *
 * <p>What it deliberately does not model is atomicity: nothing here undoes the writes a
 * rolled-back callback made against a mock. Tests that need real rollback semantics need a
 * real transaction manager, and the ones asserting them say so.
 */
public class InlineTransactionManager implements PlatformTransactionManager {

    private boolean committed;
    private boolean rolledBack;

    @Override
    public TransactionStatus getTransaction(TransactionDefinition definition) {
        return new SimpleTransactionStatus();
    }

    @Override
    public void commit(TransactionStatus status) {
        committed = true;
    }

    @Override
    public void rollback(TransactionStatus status) {
        rolledBack = true;
    }

    public boolean hasCommitted() {
        return committed;
    }

    public boolean hasRolledBack() {
        return rolledBack;
    }
}
