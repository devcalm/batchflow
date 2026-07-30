-- FR-6.2: skipped items are counted alongside the other step counters.
--
-- A separate file rather than an edit to 0001: that migration has been applied,
-- and sqlx records its checksum. Changing it in place makes every existing
-- database refuse to migrate.

-- `DEFAULT 0` is what makes this safe on a table that already has rows —
-- `NOT NULL` without one fails outright on a non-empty table.
ALTER TABLE step_execution
    ADD COLUMN skip_count BIGINT NOT NULL DEFAULT 0;

-- The counters check has to be replaced, not extended: a CHECK constraint is
-- one expression, so the new column joins it or it goes unguarded.
ALTER TABLE step_execution
    DROP CONSTRAINT step_execution_counts_non_negative;

ALTER TABLE step_execution
    ADD CONSTRAINT step_execution_counts_non_negative
        CHECK (read_count >= 0 AND write_count >= 0
               AND filter_count >= 0 AND skip_count >= 0);
