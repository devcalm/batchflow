//! Phase 10 end to end: a whole job, driven by `JobLauncher`, recovering from
//! errors a real Postgres actually raised, classified by `PostgresClassifier`.
//!
//! The pieces are each tested elsewhere — the policy in `batchflow-core`
//! against fakes, the SQLSTATE mapping in `classifier.rs` against real codes.
//! Nothing until now joined them up, so nothing until now could catch a seam:
//! a classifier the step never consults, a retry that reuses a poisoned
//! transaction, a `skip_count` that is counted but never persisted.

use batchflow_core::{
    BatchError, BatchStatus, ChunkStep, ContextValue, ExecutionContext, FaultTolerance,
    ItemProcessor, ItemReader, Job, JobLauncher, JobParameter, JobParameters, JobRepository,
    RetryPolicy, TransactionalWriter,
};
use batchflow_postgres::{PostgresClassifier, PostgresJobRepository};
use sqlx::{PgPool, Postgres, Transaction};
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use testcontainers_modules::postgres::Postgres as PostgresImage;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

type PgTx = Transaction<'static, Postgres>;

const POSITION: &str = "position";

/// The container handle must outlive the test; dropping it stops the database.
async fn start() -> (ContainerAsync<PostgresImage>, PostgresJobRepository) {
    let container = PostgresImage::default().start().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let pool = PgPool::connect(&format!(
        "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
    ))
    .await
    .unwrap();

    sqlx::query("CREATE TABLE items (value BIGINT NOT NULL)")
        .execute(&pool)
        .await
        .unwrap();

    let repository = PostgresJobRepository::new(pool);
    repository.migrate().await.unwrap();
    (container, repository)
}

fn params(date: &str) -> JobParameters {
    JobParameters::new().with("date", JobParameter::String(date.into()))
}

fn nz(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).unwrap()
}

async fn item_values(pool: &PgPool) -> Vec<i64> {
    sqlx::query_scalar::<_, i64>("SELECT value FROM items ORDER BY value")
        .fetch_all(pool)
        .await
        .unwrap()
}

struct Identity;

impl ItemProcessor for Identity {
    type In = i64;
    type Out = i64;

    async fn process(&mut self, item: i64) -> Result<Option<i64>, BatchError> {
        Ok(Some(item))
    }
}

// ---- retry ----

/// Reads a fixed list, recording its position so the step is restartable.
struct Rows {
    items: Vec<i64>,
    pos: usize,
}

impl ItemReader for Rows {
    type Item = i64;

    async fn read(&mut self) -> Result<Option<i64>, BatchError> {
        let item = self.items.get(self.pos).copied();
        if item.is_some() {
            self.pos += 1;
        }
        Ok(item)
    }

    async fn open(&mut self, context: &ExecutionContext) -> Result<(), BatchError> {
        if let Some(position) = context.get_long(POSITION)? {
            self.pos = usize::try_from(position)
                .map_err(|_| BatchError::read(format!("negative bookmark {position}")))?;
        }
        Ok(())
    }

    fn update(&self, context: &mut ExecutionContext) {
        context.put(POSITION, ContextValue::Long(self.pos as i64));
    }
}

/// Inserts into `items` inside the step's transaction and *then*, on its first
/// `contended` attempts, touches a row another connection holds locked with
/// `NOWAIT` — which Postgres answers with `55P03 lock_not_available`.
///
/// A real transient error, raised by the database, inside the real transaction.
/// A hand-rolled `BatchError::write("deadlock")` would exercise the same code
/// path while proving nothing about what Postgres emits or what a rolled-back
/// transaction does next.
///
/// The order matters: failing *after* the inserts leaves rows pending in the
/// doomed transaction, so a retry that failed to discard them would duplicate
/// every item. Probing the lock first would make the test unable to see that at
/// all — there would be nothing to roll back.
struct ContendedItemTable {
    contended: usize,
    attempts: Arc<AtomicUsize>,
}

impl TransactionalWriter<PgTx> for ContendedItemTable {
    type Item = i64;

    async fn write(&mut self, tx: &mut PgTx, items: &[i64]) -> Result<(), BatchError> {
        self.attempts.fetch_add(1, Ordering::SeqCst);

        for item in items {
            sqlx::query("INSERT INTO items (value) VALUES ($1)")
                .bind(item)
                .execute(&mut **tx)
                .await
                .map_err(BatchError::write)?;
        }

        if self.contended > 0 {
            self.contended -= 1;
            sqlx::query("SELECT id FROM lock_target WHERE id = 1 FOR UPDATE NOWAIT")
                .fetch_one(&mut **tx)
                .await
                .map_err(BatchError::write)?;
        }

        Ok(())
    }
}

/// US-4, end to end: a transient database error is retried in a fresh
/// transaction and the job completes with every row written exactly once.
#[tokio::test]
async fn a_transient_database_error_is_retried_and_the_job_completes() {
    let (_container, repository) = start().await;
    let pool = repository.pool().clone();

    sqlx::query("CREATE TABLE lock_target (id BIGINT PRIMARY KEY)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO lock_target (id) VALUES (1)")
        .execute(&pool)
        .await
        .unwrap();

    // Held open for the whole run, on its own connection, so the writer's first
    // attempt genuinely cannot take the lock.
    let mut holder = pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM lock_target WHERE id = 1 FOR UPDATE")
        .fetch_one(&mut *holder)
        .await
        .unwrap();

    let attempts = Arc::new(AtomicUsize::new(0));
    let step = ChunkStep::new(
        "load",
        Rows {
            items: vec![1, 2, 3, 4],
            pos: 0,
        },
        Identity,
        ContendedItemTable {
            contended: 1,
            attempts: Arc::clone(&attempts),
        },
        nz(2),
    )
    .with_fault_tolerance(
        FaultTolerance::new()
            .classifier(PostgresClassifier)
            .retry(RetryPolicy::attempts(NonZeroU32::new(3).unwrap())),
    );

    let launcher = JobLauncher::new(repository);
    let mut job = Job::<PgTx>::new("nightly", vec![Box::new(step)]);

    launcher.run(&mut job, &params("2026-07-29")).await.unwrap();

    // Four rows, each exactly once. The first attempt at chunk one *did* insert
    // 1 and 2 before failing; they are absent from this list because that
    // transaction was discarded, and present exactly once because the retry
    // re-inserted them. A retry that reused or leaked the failed transaction
    // would show `[1, 1, 2, 2, 3, 4]`.
    assert_eq!(item_values(&pool).await, vec![1, 2, 3, 4]);

    // Two chunks plus one retried attempt. Without the retry the run would have
    // failed; without a *fresh* transaction the second attempt would have hit
    // `25P02` on an aborted one.
    assert_eq!(attempts.load(Ordering::SeqCst), 3);

    let instance = launcher
        .repository()
        .find_instance("nightly", &params("2026-07-29"))
        .await
        .unwrap()
        .unwrap();
    let step_execution = launcher
        .repository()
        .last_step_execution(instance.id(), "load")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(step_execution.status(), BatchStatus::Completed);
    assert_eq!(step_execution.read_count(), 4);
    assert_eq!(step_execution.write_count(), 4);
    assert_eq!(
        step_execution.skip_count(),
        0,
        "a retried item is not a skipped one"
    );
}

// ---- skip ----

/// Reads rows out of a staging table, casting text to a number — so a row
/// holding `'oops'` fails with `22P02 invalid_text_representation`, which is
/// exactly what a malformed input row looks like in practice (US-3).
///
/// Advances `pos` *before* it can fail, so a skipped row is not re-read
/// forever. That contract is the reader's, not the engine's.
struct StagingRows {
    ids: Vec<i64>,
    pos: usize,
    pool: PgPool,
}

impl ItemReader for StagingRows {
    type Item = i64;

    async fn read(&mut self) -> Result<Option<i64>, BatchError> {
        let Some(id) = self.ids.get(self.pos).copied() else {
            return Ok(None);
        };
        self.pos += 1;

        sqlx::query_scalar::<_, i64>("SELECT value::BIGINT FROM staging WHERE id = $1")
            .bind(id)
            .fetch_one(&self.pool)
            .await
            .map_err(BatchError::read)
            .map(Some)
    }

    fn update(&self, context: &mut ExecutionContext) {
        context.put(POSITION, ContextValue::Long(self.pos as i64));
    }
}

struct ItemTable;

impl TransactionalWriter<PgTx> for ItemTable {
    type Item = i64;

    async fn write(&mut self, tx: &mut PgTx, items: &[i64]) -> Result<(), BatchError> {
        for item in items {
            sqlx::query("INSERT INTO items (value) VALUES ($1)")
                .bind(item)
                .execute(&mut **tx)
                .await
                .map_err(BatchError::write)?;
        }
        Ok(())
    }
}

/// US-3, end to end: a malformed row is skipped, the job completes, and the
/// skip is durable in the metadata store.
#[tokio::test]
async fn a_malformed_row_is_skipped_and_counted_in_the_metadata_store() {
    let (_container, repository) = start().await;
    let pool = repository.pool().clone();

    sqlx::query("CREATE TABLE staging (id BIGINT PRIMARY KEY, value TEXT NOT NULL)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO staging (id, value) VALUES (1,'10'),(2,'oops'),(3,'30'),(4,'40')")
        .execute(&pool)
        .await
        .unwrap();

    let step = ChunkStep::new(
        "load",
        StagingRows {
            ids: vec![1, 2, 3, 4],
            pos: 0,
            pool: pool.clone(),
        },
        Identity,
        ItemTable,
        nz(2),
    )
    .with_fault_tolerance(
        FaultTolerance::new()
            .classifier(PostgresClassifier)
            .skip_limit(1),
    );

    let launcher = JobLauncher::new(repository);
    let mut job = Job::<PgTx>::new("nightly", vec![Box::new(step)]);

    launcher.run(&mut job, &params("2026-07-29")).await.unwrap();

    // The bad row is gone; every good row survives. A skip that dropped the
    // rest of its chunk would lose 10 as well.
    assert_eq!(item_values(&pool).await, vec![10, 30, 40]);

    let instance = launcher
        .repository()
        .find_instance("nightly", &params("2026-07-29"))
        .await
        .unwrap()
        .unwrap();
    let step_execution = launcher
        .repository()
        .last_step_execution(instance.id(), "load")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(step_execution.status(), BatchStatus::Completed);
    assert_eq!(step_execution.skip_count(), 1);
    assert_eq!(step_execution.read_count(), 3, "the bad row was not read");
    assert_eq!(step_execution.write_count(), 3);
    assert_eq!(
        step_execution.filter_count(),
        0,
        "a skip must not be recorded as a filter"
    );
}

/// Past the limit the job fails, and it fails with `SkipLimitExceeded` rather
/// than the bare item error — the signal an operator needs to tell "one odd
/// row" from "this input is wrong".
#[tokio::test]
async fn exceeding_the_skip_limit_fails_the_job() {
    let (_container, repository) = start().await;
    let pool = repository.pool().clone();

    sqlx::query("CREATE TABLE staging (id BIGINT PRIMARY KEY, value TEXT NOT NULL)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO staging (id, value) VALUES (1,'10'),(2,'oops'),(3,'nope'),(4,'40')")
        .execute(&pool)
        .await
        .unwrap();

    let step = ChunkStep::new(
        "load",
        StagingRows {
            ids: vec![1, 2, 3, 4],
            pos: 0,
            pool: pool.clone(),
        },
        Identity,
        ItemTable,
        nz(2),
    )
    .with_fault_tolerance(
        FaultTolerance::new()
            .classifier(PostgresClassifier)
            .skip_limit(1),
    );

    let launcher = JobLauncher::new(repository);
    let mut job = Job::<PgTx>::new("nightly", vec![Box::new(step)]);

    let result = launcher.run(&mut job, &params("2026-07-29")).await;

    assert!(matches!(
        result,
        Err(BatchError::SkipLimitExceeded { limit: 1, .. })
    ));

    let instance = launcher
        .repository()
        .find_instance("nightly", &params("2026-07-29"))
        .await
        .unwrap()
        .unwrap();
    let step_execution = launcher
        .repository()
        .last_step_execution(instance.id(), "load")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(step_execution.status(), BatchStatus::Failed);
}
