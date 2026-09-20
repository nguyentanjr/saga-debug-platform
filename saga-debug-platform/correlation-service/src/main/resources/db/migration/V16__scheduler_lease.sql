-- Which instance is allowed to run a scheduled sweep right now.

-- Every sweep in this service is a read-then-write over the same rows: find sagas that
-- look stuck, decide whether a finding already exists, write one if not. The existence
-- check is check-then-act, so two instances sweeping at once can both decide a finding is
-- missing and both create it. Since ADR 0006 the same race runs the other way too: one
-- pass can raise a finding the other pass has just retracted, and the retraction carries
-- a reason while the duplicate does not. Nothing in the schema stopped either.

-- A row here is a lease on one sweep name: whoever holds it runs that sweep, everyone
-- else skips this tick. Taking the lease is a single conditional UPDATE, so the database
-- decides the winner and no instance has to agree with another about anything.

-- lease_expires_at is epoch seconds rather than a timestamp, and it is computed from the
-- database clock (EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)), never from the clock of the
-- instance taking the lease. That is the point: mutual exclusion must not depend on two
-- hosts agreeing about the time. A skewed host gets a shorter or longer turn; it never
-- gets a second copy of the sweep. taken_at is for operators reading the table and is not
-- read by the acquisition predicate.

-- A holder that crashes never releases, so the lease has to expire on its own. That is
-- why the predicate is an expiry comparison and not a boolean: the recovery path and the
-- normal path are the same statement.

CREATE TABLE scheduler_lease (
    sweep            VARCHAR(64)  PRIMARY KEY,
    holder           VARCHAR(128) NOT NULL,
    token            VARCHAR(36)  NOT NULL,
    taken_at         TIMESTAMPTZ  NOT NULL,
    lease_expires_at BIGINT       NOT NULL
);

COMMENT ON TABLE scheduler_lease IS
    'One row per scheduled sweep: who holds the right to run it and until when (epoch seconds, database clock).';
COMMENT ON COLUMN scheduler_lease.token IS
    'Identifies one acquisition. Release only frees a lease whose token still matches, so a holder that overran cannot free its successor.';
