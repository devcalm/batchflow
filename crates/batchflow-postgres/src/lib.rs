//! # BatchFlow Postgres
//!
//! Postgres metadata store for BatchFlow, and the first backend where the
//! chunk-loop transaction is real: `Tx` is a `sqlx` transaction, so a chunk's
//! rows, its counters and its bookmark commit together or not at all.
#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

mod classifier;

pub use classifier::PostgresClassifier;

use batchflow_core::{
    BatchError, BatchStatus, ExecutionContext, JobExecution, JobExecutionId, JobInstance,
    JobInstanceId, JobParameters, JobRepository, StepContribution, StepExecution, StepExecutionId,
    Timestamps,
};
use sqlx::types::time::OffsetDateTime;
use sqlx::{PgPool, Postgres, Transaction};
use std::time::SystemTime;

const STARTING: &str = "STARTING";
const STARTED: &str = "STARTED";
const COMPLETED: &str = "COMPLETED";
const FAILED: &str = "FAILED";
const STOPPED: &str = "STOPPED";
const ABANDONED: &str = "ABANDONED";

/// A durable [`JobRepository`] backed by Postgres.
///
/// `Tx` is a real `sqlx::Transaction`, so a step's rows, counters and bookmark
/// commit together (FR-2.4). Instance identity is enforced by a
/// `UNIQUE (job_name, parameters)` constraint rather than by the launcher, so
/// two schedulers cannot both win the check-then-act race.
#[derive(Debug, Clone)]
pub struct PostgresJobRepository {
    pool: PgPool,
}

impl PostgresJobRepository {
    /// Wraps an existing pool. Call [`migrate`](Self::migrate) before first use.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// The underlying pool, so a writer can enlist against the same database.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Apply the embedded schema migrations.
    ///
    /// # Errors
    ///
    /// [`BatchError::Repository`] if a migration fails or the applied history
    /// diverges from the embedded one.
    pub async fn migrate(&self) -> Result<(), BatchError> {
        sqlx::migrate!("./migrations")
            .run(&self.pool)
            .await
            .map_err(|e| BatchError::repository(format!("migration failed: {e}")))
    }
}

fn db(error: sqlx::Error) -> BatchError {
    BatchError::repository(error)
}

fn status_name(status: BatchStatus) -> Result<&'static str, BatchError> {
    Ok(match status {
        BatchStatus::Starting => STARTING,
        BatchStatus::Started => STARTED,
        BatchStatus::Completed => COMPLETED,
        BatchStatus::Failed => FAILED,
        BatchStatus::Stopped => STOPPED,
        BatchStatus::Abandoned => ABANDONED,
        // `BatchStatus` is `#[non_exhaustive]`, so unlike inside the core crate a
        // new variant compiles here and has to be caught at runtime.
        other => {
            return Err(BatchError::repository(format!(
                "status {other:?} has no stored representation"
            )));
        }
    })
}

fn status_from(name: &str) -> Result<BatchStatus, BatchError> {
    Ok(match name {
        STARTING => BatchStatus::Starting,
        STARTED => BatchStatus::Started,
        COMPLETED => BatchStatus::Completed,
        FAILED => BatchStatus::Failed,
        STOPPED => BatchStatus::Stopped,
        ABANDONED => BatchStatus::Abandoned,
        other => return Err(BatchError::repository(format!("unknown status '{other}'"))),
    })
}

/// Whether a status is terminal, and therefore fixes `ended_at`.
fn is_terminal(status: BatchStatus) -> bool {
    matches!(
        status,
        BatchStatus::Completed
            | BatchStatus::Failed
            | BatchStatus::Stopped
            | BatchStatus::Abandoned
    )
}

/// Rebuilds the three instants a row carries.
///
/// `created_at` and `last_updated` are `NOT NULL`, so they are always present;
/// `ended_at` is NULL until a terminal status is written.
fn timestamps(
    created_at: OffsetDateTime,
    ended_at: Option<OffsetDateTime>,
    last_updated: OffsetDateTime,
) -> Timestamps {
    Timestamps::new(
        Some(created_at.into()),
        ended_at.map(SystemTime::from),
        Some(last_updated.into()),
    )
}

fn count(value: i64) -> Result<usize, BatchError> {
    usize::try_from(value)
        .map_err(|_| BatchError::repository(format!("negative counter {value} in step_execution")))
}

/// A counter on its way *into* the database.
///
/// `try_from` rather than `as`, mirroring [`count`] on the read path: `as`
/// reinterprets a `usize` above `i64::MAX` as negative, which the
/// `step_execution_counts_non_negative` constraint would then reject with a
/// message claiming corruption. Unreachable in practice — that many items is
/// not a batch — but the constraint's meaning should stay true.
fn stored(value: usize) -> Result<i64, BatchError> {
    i64::try_from(value)
        .map_err(|_| BatchError::repository(format!("counter {value} does not fit in i64")))
}

/// The one `UPDATE step_execution`, against a pool or a transaction.
///
/// Generic over [`sqlx::Executor`] rather than written twice: the transactional
/// and non-transactional paths differ only in where the statement runs, and two
/// copies is two chances for the chunk-commit path and the terminal-status path
/// to drift apart.
async fn write_step_execution<'e, E>(
    executor: E,
    step_execution: &StepExecution,
) -> Result<(), BatchError>
where
    E: sqlx::Executor<'e, Database = Postgres>,
{
    // `last_updated` is absent on purpose: the `step_execution_touch` trigger
    // maintains it. This is the per-chunk write, so keeping the heartbeat out
    // of it means the hottest statement in the system carries nothing extra and
    // cannot forget to.
    //
    // `COALESCE(ended_at, now())` fixes the terminal instant the first time it
    // is reached, so a later write cannot move it.
    let affected = sqlx::query!(
        "UPDATE step_execution
                SET status = $2, read_count = $3, write_count = $4,
                    filter_count = $5, skip_count = $6, execution_context = $7,
                    ended_at = CASE WHEN $8 THEN COALESCE(ended_at, now()) ELSE ended_at END,
                    exit_message = $9
              WHERE id = $1",
        step_execution.id().get(),
        status_name(step_execution.status())?,
        stored(step_execution.read_count())?,
        stored(step_execution.write_count())?,
        stored(step_execution.filter_count())?,
        stored(step_execution.skip_count())?,
        json(step_execution.execution_context())?,
        is_terminal(step_execution.status()),
        step_execution.exit_message(),
    )
    .execute(executor)
    .await
    .map_err(db)?
    .rows_affected();

    if affected == 0 {
        return Err(BatchError::repository(format!(
            "unknown step execution {:?}",
            step_execution.id()
        )));
    }
    Ok(())
}

fn json<T: serde::Serialize>(value: &T) -> Result<serde_json::Value, BatchError> {
    serde_json::to_value(value).map_err(BatchError::repository)
}

fn from_json<T: serde::de::DeserializeOwned>(value: serde_json::Value) -> Result<T, BatchError> {
    serde_json::from_value(value).map_err(BatchError::repository)
}

#[allow(clippy::too_many_arguments)]
fn execution(
    id: i64,
    instance_id: i64,
    status: &str,
    context: serde_json::Value,
    created_at: OffsetDateTime,
    ended_at: Option<OffsetDateTime>,
    last_updated: OffsetDateTime,
    exit_message: Option<String>,
) -> Result<JobExecution, BatchError> {
    let mut execution = JobExecution::new(JobExecutionId::new(id), JobInstanceId::new(instance_id));
    execution.set_status(status_from(status)?);
    execution.set_execution_context(from_json::<ExecutionContext>(context)?);
    execution.set_timestamps(timestamps(created_at, ended_at, last_updated));
    execution.set_exit_message(exit_message);
    Ok(execution)
}

/// Rebuilds the counters from a row.
///
/// They are private and fold-only on `StepExecution`, so they are restored
/// through a contribution rather than assigned.
fn counters(read: i64, write: i64, filter: i64, skip: i64) -> Result<StepContribution, BatchError> {
    let mut counters = StepContribution::new();
    counters.increment_read(count(read)?);
    counters.increment_write(count(write)?);
    counters.increment_filter(count(filter)?);
    counters.increment_skip(count(skip)?);
    Ok(counters)
}

#[allow(clippy::too_many_arguments)]
fn step(
    id: i64,
    job_execution_id: i64,
    step_name: String,
    status: &str,
    counters: StepContribution,
    context: serde_json::Value,
    created_at: OffsetDateTime,
    ended_at: Option<OffsetDateTime>,
    last_updated: OffsetDateTime,
    exit_message: Option<String>,
) -> Result<StepExecution, BatchError> {
    let mut step = StepExecution::new(
        StepExecutionId::new(id),
        JobExecutionId::new(job_execution_id),
        step_name,
    );
    step.set_status(status_from(status)?);
    step.set_execution_context(from_json::<ExecutionContext>(context)?);
    step.apply(&counters);
    step.set_timestamps(timestamps(created_at, ended_at, last_updated));
    step.set_exit_message(exit_message);

    Ok(step)
}

impl JobRepository for PostgresJobRepository {
    type Tx = Transaction<'static, Postgres>;

    async fn begin(&self) -> Result<Self::Tx, BatchError> {
        self.pool.begin().await.map_err(db)
    }

    async fn commit(&self, tx: Self::Tx) -> Result<(), BatchError> {
        tx.commit().await.map_err(db)
    }

    async fn rollback(&self, tx: Self::Tx) -> Result<(), BatchError> {
        tx.rollback().await.map_err(db)
    }

    async fn update_step_execution_in(
        &self,
        tx: &mut Self::Tx,
        step_execution: &StepExecution,
    ) -> Result<(), BatchError> {
        write_step_execution(&mut **tx, step_execution).await
    }

    async fn find_or_create_instance(
        &self,
        job_name: &str,
        parameters: &JobParameters,
    ) -> Result<JobInstance, BatchError> {
        // One statement, so the lookup and the insert cannot race: the unique
        // constraint decides the winner and the loser reads the winner's row.
        let row = sqlx::query!(
            "INSERT INTO job_instance (job_name, parameters)
                  VALUES ($1, $2)
             ON CONFLICT ON CONSTRAINT job_instance_identity
             DO UPDATE SET job_name = EXCLUDED.job_name
               RETURNING id, job_name, parameters",
            job_name,
            json(parameters)?,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(db)?;

        Ok(JobInstance::new(
            JobInstanceId::new(row.id),
            row.job_name,
            from_json::<JobParameters>(row.parameters)?,
        ))
    }

    async fn find_instance(
        &self,
        job_name: &str,
        parameters: &JobParameters,
    ) -> Result<Option<JobInstance>, BatchError> {
        let row = sqlx::query!(
            "SELECT id, job_name, parameters
               FROM job_instance
              WHERE job_name = $1 AND parameters = $2",
            job_name,
            json(parameters)?,
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(db)?;

        row.map(|row| {
            Ok(JobInstance::new(
                JobInstanceId::new(row.id),
                row.job_name,
                from_json::<JobParameters>(row.parameters)?,
            ))
        })
        .transpose()
    }

    async fn create_execution(
        &self,
        instance_id: JobInstanceId,
    ) -> Result<JobExecution, BatchError> {
        let row = sqlx::query!(
            "INSERT INTO job_execution (instance_id, status, execution_context)
                  VALUES ($1, $2, $3)
               RETURNING id, created_at, last_updated",
            instance_id.get(),
            STARTING,
            json(&ExecutionContext::new())?,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(db)?;

        let mut execution = JobExecution::new(JobExecutionId::new(row.id), instance_id);
        execution.set_timestamps(timestamps(row.created_at, None, row.last_updated));
        Ok(execution)
    }

    /// The gate and the insert in one transaction, serialised by a row lock on
    /// the instance.
    ///
    /// `SELECT … FOR UPDATE` on `job_instance` is what makes this atomic: a
    /// second launcher racing the same instance blocks on that lock until the
    /// first has committed its execution, and then reads it and is refused.
    /// The lock is per instance, so unrelated jobs never contend.
    ///
    /// Chosen over a partial unique index on live executions because that would
    /// also constrain [`create_execution`](JobRepository::create_execution),
    /// which is deliberately unconditional — a primitive that mints a row is
    /// worth keeping. The index remains available as later hardening if this
    /// method is ever bypassed.
    async fn start_execution(
        &self,
        job_name: &str,
        instance_id: JobInstanceId,
    ) -> Result<JobExecution, BatchError> {
        let mut tx = self.pool.begin().await.map_err(db)?;

        // Taken before anything is read, so the whole decision below sees a
        // state no concurrent launcher can change under it.
        let locked = sqlx::query!(
            "SELECT id FROM job_instance WHERE id = $1 FOR UPDATE",
            instance_id.get(),
        )
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;

        if locked.is_none() {
            return Err(BatchError::repository(format!(
                "unknown instance {instance_id:?}"
            )));
        }

        // Same statement `last_execution` runs, against the transaction rather
        // than the pool — so it shares the prepared-query cache entry.
        let last = sqlx::query!(
            "SELECT id, instance_id, status, execution_context,
                      created_at, ended_at, last_updated, exit_message
               FROM job_execution
              WHERE instance_id = $1
              ORDER BY id DESC
              LIMIT 1",
            instance_id.get(),
        )
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;

        if let Some(row) = last {
            match status_from(&row.status)? {
                BatchStatus::Completed => {
                    return Err(BatchError::JobInstanceAlreadyComplete {
                        job_name: job_name.to_owned(),
                        instance_id,
                    });
                }
                BatchStatus::Starting | BatchStatus::Started => {
                    return Err(BatchError::JobExecutionAlreadyRunning {
                        job_name: job_name.to_owned(),
                        execution_id: JobExecutionId::new(row.id),
                    });
                }
                // Terminal but unsuccessful — the restart door.
                BatchStatus::Failed | BatchStatus::Stopped | BatchStatus::Abandoned => {}
                other => {
                    return Err(BatchError::repository(format!(
                        "status {other:?} has no launch rule"
                    )));
                }
            }
        }

        // `STARTED`, not `STARTING`: the row has to hold the instance the
        // moment it becomes visible, which is when this transaction commits.
        let row = sqlx::query!(
            "INSERT INTO job_execution (instance_id, status, execution_context)
                  VALUES ($1, $2, $3)
               RETURNING id, created_at, last_updated",
            instance_id.get(),
            STARTED,
            json(&ExecutionContext::new())?,
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;

        tx.commit().await.map_err(db)?;

        let mut execution = JobExecution::new(JobExecutionId::new(row.id), instance_id);
        execution.set_status(BatchStatus::Started);
        execution.set_timestamps(timestamps(row.created_at, None, row.last_updated));
        Ok(execution)
    }

    async fn update_execution(&self, execution: &JobExecution) -> Result<(), BatchError> {
        let affected = sqlx::query!(
            "UPDATE job_execution
                SET status = $2, execution_context = $3,
                    ended_at = CASE WHEN $4 THEN COALESCE(ended_at, now()) ELSE ended_at END,
                    exit_message = $5
              WHERE id = $1",
            execution.id().get(),
            status_name(execution.status())?,
            json(execution.execution_context())?,
            is_terminal(execution.status()),
            execution.exit_message(),
        )
        .execute(&self.pool)
        .await
        .map_err(db)?
        .rows_affected();

        if affected == 0 {
            return Err(BatchError::repository(format!(
                "unknown execution {:?}",
                execution.id()
            )));
        }
        Ok(())
    }

    async fn last_execution(
        &self,
        instance_id: JobInstanceId,
    ) -> Result<Option<JobExecution>, BatchError> {
        let row = sqlx::query!(
            "SELECT id, instance_id, status, execution_context,
                      created_at, ended_at, last_updated, exit_message
               FROM job_execution
              WHERE instance_id = $1
              ORDER BY id DESC
              LIMIT 1",
            instance_id.get(),
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(db)?;

        row.map(|row| {
            execution(
                row.id,
                row.instance_id,
                &row.status,
                row.execution_context,
                row.created_at,
                row.ended_at,
                row.last_updated,
                row.exit_message,
            )
        })
        .transpose()
    }

    async fn executions(
        &self,
        instance_id: JobInstanceId,
    ) -> Result<Vec<JobExecution>, BatchError> {
        let rows = sqlx::query!(
            "SELECT id, instance_id, status, execution_context,
                      created_at, ended_at, last_updated, exit_message
               FROM job_execution
              WHERE instance_id = $1
              ORDER BY id",
            instance_id.get(),
        )
        .fetch_all(&self.pool)
        .await
        .map_err(db)?;

        rows.into_iter()
            .map(|row| {
                execution(
                    row.id,
                    row.instance_id,
                    &row.status,
                    row.execution_context,
                    row.created_at,
                    row.ended_at,
                    row.last_updated,
                    row.exit_message,
                )
            })
            .collect()
    }

    async fn abandon_execution(&self, execution_id: JobExecutionId) -> Result<(), BatchError> {
        // `FOR UPDATE` inside one statement, so the status that gates the update
        // is the status the update sees — check-then-act across two round trips
        // would let a concurrent commit slip between them.
        let row = sqlx::query!(
            r#"WITH locked AS (
                   SELECT id, status FROM job_execution WHERE id = $1 FOR UPDATE
               ), updated AS (
                   UPDATE job_execution
                      SET status = $2, ended_at = COALESCE(ended_at, now())
                    WHERE id = (SELECT id FROM locked WHERE status <> $3)
                RETURNING id
               )
               SELECT (SELECT status FROM locked)  AS "status?",
                      (SELECT id     FROM updated) AS "updated?""#,
            execution_id.get(),
            ABANDONED,
            COMPLETED,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(db)?;

        match (row.status, row.updated) {
            (None, _) => Err(BatchError::repository(format!(
                "unknown execution {execution_id:?}"
            ))),
            (Some(status), None) => Err(BatchError::CannotAbandon {
                execution_id,
                status: status_from(&status)?,
            }),
            (Some(_), Some(_)) => Ok(()),
        }
    }

    async fn create_step_execution(
        &self,
        job_execution_id: JobExecutionId,
        step_name: &str,
    ) -> Result<StepExecution, BatchError> {
        let row = sqlx::query!(
            "INSERT INTO step_execution
                    (job_execution_id, step_name, status,
                     read_count, write_count, filter_count, skip_count,
                     execution_context)
                  VALUES ($1, $2, $3, 0, 0, 0, 0, $4)
               RETURNING id, created_at, last_updated",
            job_execution_id.get(),
            step_name,
            STARTING,
            json(&ExecutionContext::new())?,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(db)?;

        let mut step =
            StepExecution::new(StepExecutionId::new(row.id), job_execution_id, step_name);
        step.set_timestamps(timestamps(row.created_at, None, row.last_updated));
        Ok(step)
    }

    async fn update_step_execution(
        &self,
        step_execution: &StepExecution,
    ) -> Result<(), BatchError> {
        write_step_execution(&self.pool, step_execution).await
    }

    async fn last_step_execution(
        &self,
        instance_id: JobInstanceId,
        step_name: &str,
    ) -> Result<Option<StepExecution>, BatchError> {
        let row = sqlx::query!(
            "SELECT s.id, s.job_execution_id, s.step_name, s.status,
s.read_count, s.write_count, s.filter_count, s.skip_count,
                    s.execution_context, s.created_at, s.ended_at, s.last_updated,
                    s.exit_message
               FROM step_execution s
               JOIN job_execution e ON e.id = s.job_execution_id
              WHERE e.instance_id = $1 AND s.step_name = $2
              ORDER BY s.id DESC
              LIMIT 1",
            instance_id.get(),
            step_name,
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(db)?;

        row.map(|row| {
            step(
                row.id,
                row.job_execution_id,
                row.step_name,
                &row.status,
                counters(
                    row.read_count,
                    row.write_count,
                    row.filter_count,
                    row.skip_count,
                )?,
                row.execution_context,
                row.created_at,
                row.ended_at,
                row.last_updated,
                row.exit_message,
            )
        })
        .transpose()
    }

    async fn step_executions(
        &self,
        job_execution_id: JobExecutionId,
    ) -> Result<Vec<StepExecution>, BatchError> {
        let rows = sqlx::query!(
            "SELECT id, job_execution_id, step_name, status,
                    read_count, write_count, filter_count, skip_count,
                    execution_context, created_at, ended_at, last_updated,
                    exit_message
               FROM step_execution
              WHERE job_execution_id = $1
              ORDER BY id",
            job_execution_id.get(),
        )
        .fetch_all(&self.pool)
        .await
        .map_err(db)?;

        rows.into_iter()
            .map(|row| {
                step(
                    row.id,
                    row.job_execution_id,
                    row.step_name,
                    &row.status,
                    counters(
                        row.read_count,
                        row.write_count,
                        row.filter_count,
                        row.skip_count,
                    )?,
                    row.execution_context,
                    row.created_at,
                    row.ended_at,
                    row.last_updated,
                    row.exit_message,
                )
            })
            .collect()
    }
}
