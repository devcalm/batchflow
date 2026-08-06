-- PROD-2 / ERR-1: when an execution ran, whether it is still alive, and why it
-- failed.
--
-- Before this the store could answer none of those. An operator asking "when
-- did last night's job run", "how long did it take", "is this STARTED row a
-- live process or a zombie" or "why did 4712 fail" had to go to the process's
-- own logs -- which are retained differently, are not joinable to an execution
-- id without a tracing subscriber, and are unavailable to anything querying the
-- store from another service.
--
-- The absence compounded: no `last_updated` meant no heartbeat, no heartbeat
-- meant no automatic reaper, so a crashed process needed a human running
-- `abandon_execution`. One migration unblocks all of it.

-- There is deliberately no `started_at`. It would equal `created_at` for every
-- row the engine produces: `start_execution` inserts a job execution that is
-- already `Started`, and a step execution is created and set `Started` in the
-- same breath. A second column carrying the same instant is a second column to
-- keep consistent across three backends, for no question it answers alone.
-- Duration is `ended_at - created_at`.

ALTER TABLE job_execution
    -- `now()` is the *database's* clock, which is the point: it is the one
    -- clock every process writing to this store agrees on. Deriving these from
    -- an application's clock would make durations across two replicas depend on
    -- how well their NTP is behaving.
    ADD COLUMN created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Terminal instant. NULL while running, which is also how "is this row
    -- finished?" is asked without parsing a status string.
    ADD COLUMN ended_at     TIMESTAMPTZ,
    -- The heartbeat. Maintained by the trigger below rather than by any
    -- statement, so it cannot be forgotten at a call site.
    ADD COLUMN last_updated TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Why it failed. Bounded by the writer, not here: an error chain includes a
    -- user error whose `Display` this project does not control.
    ADD COLUMN exit_message TEXT;

ALTER TABLE step_execution
    ADD COLUMN created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN ended_at     TIMESTAMPTZ,
    ADD COLUMN last_updated TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN exit_message TEXT;

-- A trigger rather than `SET last_updated = now()` in each UPDATE, for two
-- reasons. It cannot be forgotten when a statement is added; and the
-- per-chunk `UPDATE step_execution` is the heartbeat, so keeping it out of that
-- statement means the hottest write in the system did not have to change.
CREATE OR REPLACE FUNCTION batchflow_touch_last_updated()
    RETURNS TRIGGER AS $$
BEGIN
    NEW.last_updated := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER job_execution_touch
    BEFORE UPDATE ON job_execution
    FOR EACH ROW EXECUTE FUNCTION batchflow_touch_last_updated();

CREATE TRIGGER step_execution_touch
    BEFORE UPDATE ON step_execution
    FOR EACH ROW EXECUTE FUNCTION batchflow_touch_last_updated();

-- Retention (PROD-3) scans by age.
CREATE INDEX job_execution_by_created ON job_execution (created_at);

-- The reaper query: live executions ordered by how long they have been silent.
-- Partial, so it stays small however much history accumulates -- the rows it
-- indexes are the handful currently running.
CREATE INDEX job_execution_live ON job_execution (last_updated)
    WHERE status IN ('STARTING', 'STARTED');
