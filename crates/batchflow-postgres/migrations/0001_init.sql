-- BatchFlow metadata schema.
--
-- Three tables, joined by foreign key, matching the shape `InMemoryJobRepository`
-- was deliberately built to (step executions are flat and joined, never nested).

CREATE TABLE job_instance (
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_name   TEXT  NOT NULL,
    parameters JSONB NOT NULL,

    -- FR-4.2: (job_name, parameters) *is* the identity of an instance. Enforcing
    -- it here rather than in application code is what closes the check-then-act
    -- race two schedulers can otherwise both win.
    CONSTRAINT job_instance_identity UNIQUE (job_name, parameters)
);

CREATE TABLE job_execution (
    id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    instance_id       BIGINT NOT NULL REFERENCES job_instance (id),
    status            TEXT   NOT NULL,
    execution_context JSONB  NOT NULL
);

-- `last_execution` orders by id within an instance.
CREATE INDEX job_execution_by_instance ON job_execution (instance_id, id DESC);

CREATE TABLE step_execution (
    id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_execution_id  BIGINT NOT NULL REFERENCES job_execution (id),
    step_name         TEXT   NOT NULL,
    status            TEXT   NOT NULL,
    read_count        BIGINT NOT NULL,
    write_count       BIGINT NOT NULL,
    filter_count      BIGINT NOT NULL,
    execution_context JSONB  NOT NULL,

    -- Counters are `usize` in the engine; a negative one here means corruption
    -- rather than a legitimate value.
    CONSTRAINT step_execution_counts_non_negative
        CHECK (read_count >= 0 AND write_count >= 0 AND filter_count >= 0)
);

-- `step_executions` returns insertion order; `last_step_execution` walks the
-- same index backwards after joining to the instance.
CREATE INDEX step_execution_by_job_execution ON step_execution (job_execution_id, id);
CREATE INDEX step_execution_by_name ON step_execution (step_name, id DESC);
