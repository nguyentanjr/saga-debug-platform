-- Run on a disposable PostgreSQL connection, followed by V18 and the after script.
-- A temp table shadows the persistent table; rollback leaves the database untouched.
BEGIN;
CREATE TEMP TABLE saga_instances (saga_id TEXT PRIMARY KEY, workflow_type TEXT NOT NULL);
INSERT INTO saga_instances VALUES ('legacy', 'ORDER_SAGA');
