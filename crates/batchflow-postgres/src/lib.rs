//! # BatchFlow Postgres
//!
//! Postgres metadata store for BatchFlow, and the first backend where the
//! chunk-loop transaction is real: `Tx` is a `sqlx` transaction, so a chunk's
//! rows, its counters and its bookmark commit together or not at all.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod classifier;

pub use classifier::PostgresClassifier;

use batchflow_core::{
    BatchError, BatchStatus, ExecutionContext, JobExecution, JobExecutionId, JobInstance,
    JobInstanceId, JobParameters, JobRepository, StepContribution, StepExecution, StepExecutionId,
};
use sqlx::{PgPool, Postgres, Transaction};

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

fn count(value: i64) -> Result<usize, BatchError> {
    usize::try_from(value)
        .map_err(|_| BatchError::repository(format!("negative counter {value} in step_execution")))
}

fn json<T: serde::Serialize>(value: &T) -> Result<serde_json::Value, BatchError> {
    serde_json::to_value(value).map_err(BatchError::repository)
}

fn from_json<T: serde::de::DeserializeOwned>(value: serde_json::Value) -> Result<T, BatchError> {
    serde_json::from_value(value).map_err(BatchError::repository)
}

fn execution(
    id: i64,
    instance_id: i64,
    status: &str,
    context: serde_json::Value,
) -> Result<JobExecution, BatchError> {
    let mut execution = JobExecution::new(JobExecutionId::new(id), JobInstanceId::new(instance_id));
    execution.set_status(status_from(status)?);
    execution.set_execution_context(from_json::<ExecutionContext>(context)?);
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

fn step(
    id: i64,
    job_execution_id: i64,
    step_name: String,
    status: &str,
    counters: StepContribution,
    context: serde_json::Value,
) -> Result<StepExecution, BatchError> {
    let mut step = StepExecution::new(
        StepExecutionId::new(id),
        JobExecutionId::new(job_execution_id),
        step_name,
    );
    step.set_status(status_from(status)?);
    step.set_execution_context(from_json::<ExecutionContext>(context)?);
    step.apply(&counters);

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
        let affected = sqlx::query!(
            "UPDATE step_execution
                SET status = $2, read_count = $3, write_count = $4,
                    filter_count = $5, skip_count = $6, execution_context = $7
              WHERE id = $1",
            step_execution.id().get(),
            status_name(step_execution.status())?,
            step_execution.read_count() as i64,
            step_execution.write_count() as i64,
            step_execution.filter_count() as i64,
            step_execution.skip_count() as i64,
            json(step_execution.execution_context())?,
        )
        .execute(&mut **tx)
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
               RETURNING id",
            instance_id.get(),
            STARTING,
            json(&ExecutionContext::new())?,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(db)?;

        Ok(JobExecution::new(JobExecutionId::new(row.id), instance_id))
    }

    async fn update_execution(&self, execution: &JobExecution) -> Result<(), BatchError> {
        let affected = sqlx::query!(
            "UPDATE job_execution SET status = $2, execution_context = $3 WHERE id = $1",
            execution.id().get(),
            status_name(execution.status())?,
            json(execution.execution_context())?,
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
            "SELECT id, instance_id, status, execution_context
               FROM job_execution
              WHERE instance_id = $1
              ORDER BY id DESC
              LIMIT 1",
            instance_id.get(),
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(db)?;

        row.map(|row| execution(row.id, row.instance_id, &row.status, row.execution_context))
            .transpose()
    }

    async fn abandon_execution(&self, execution_id: JobExecutionId) -> Result<(), BatchError> {
        // `FOR UPDATE` inside one statement, so the status that gates the update
        // is the status the update sees — check-then-act across two round trips
        // would let a concurrent commit slip between them.
        let row = sqlx::query!(
            r#"WITH locked AS (
                   SELECT id, status FROM job_execution WHERE id = $1 FOR UPDATE
               ), updated AS (
                   UPDATE job_execution SET status = $2
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
               RETURNING id",
            job_execution_id.get(),
            step_name,
            STARTING,
            json(&ExecutionContext::new())?,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(db)?;

        Ok(StepExecution::new(
            StepExecutionId::new(row.id),
            job_execution_id,
            step_name,
        ))
    }

    async fn update_step_execution(
        &self,
        step_execution: &StepExecution,
    ) -> Result<(), BatchError> {
        let affected = sqlx::query!(
            "UPDATE step_execution
                SET status = $2, read_count = $3, write_count = $4,
                    filter_count = $5, skip_count = $6, execution_context = $7
              WHERE id = $1",
            step_execution.id().get(),
            status_name(step_execution.status())?,
            step_execution.read_count() as i64,
            step_execution.write_count() as i64,
            step_execution.filter_count() as i64,
            step_execution.skip_count() as i64,
            json(step_execution.execution_context())?,
        )
        .execute(&self.pool)
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

    async fn last_step_execution(
        &self,
        instance_id: JobInstanceId,
        step_name: &str,
    ) -> Result<Option<StepExecution>, BatchError> {
        let row = sqlx::query!(
            "SELECT s.id, s.job_execution_id, s.step_name, s.status,
                    s.read_count, s.write_count, s.filter_count, s.skip_count,
                    s.execution_context
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
                    execution_context
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
                )
            })
            .collect()
    }
}
