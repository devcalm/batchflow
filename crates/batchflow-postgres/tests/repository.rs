//! Integration tests against a real Postgres, per ADR-007: the transaction
//! abstraction is validated here and nowhere else.

use batchflow_core::{
    BatchError, BatchStatus, ChunkStep, ContextValue, ExecutionContext, ItemProcessor, ItemReader,
    Job, JobLauncher, JobParameter, JobParameters, JobRepository, TransactionalWriter,
};
use batchflow_postgres::PostgresJobRepository;
use sqlx::{PgPool, Postgres, Transaction};
use std::num::NonZeroUsize;
use testcontainers_modules::postgres::Postgres as PostgresImage;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

type PgTx = Transaction<'static, Postgres>;

/// The container handle must be kept alive for the length of the test; dropping
/// it stops the database.
async fn start() -> (ContainerAsync<PostgresImage>, PostgresJobRepository) {
    let container = PostgresImage::default().start().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let pool = PgPool::connect(&format!(
        "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
    ))
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

const POSITION: &str = "position";

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
                .map_err(|_| BatchError::Read(format!("negative bookmark {position}")))?;
        }
        Ok(())
    }

    fn update(&self, context: &mut ExecutionContext) {
        context.put(POSITION, ContextValue::Long(self.pos as i64));
    }
}

struct Identity;

impl ItemProcessor for Identity {
    type In = i64;
    type Out = i64;

    async fn process(&mut self, item: i64) -> Result<Option<i64>, BatchError> {
        Ok(Some(item))
    }
}

/// Inserts into `items` **inside the step's transaction**, then fails once it
/// has written more than `ok_writes` chunks — so the failing chunk's rows exist
/// in the transaction at the moment it is rolled back.
struct ItemTable {
    ok_writes: usize,
    writes: usize,
}

impl TransactionalWriter<PgTx> for ItemTable {
    type Item = i64;

    async fn write(&mut self, tx: &mut PgTx, items: &[i64]) -> Result<(), BatchError> {
        for item in items {
            sqlx::query("INSERT INTO items (value) VALUES ($1)")
                .bind(item)
                .execute(&mut **tx)
                .await
                .map_err(|e| BatchError::Write(e.to_string()))?;
        }

        self.writes += 1;
        if self.writes > self.ok_writes {
            return Err(BatchError::Write("boom".into()));
        }
        Ok(())
    }
}

async fn item_values(pool: &PgPool) -> Vec<i64> {
    sqlx::query_scalar::<_, i64>("SELECT value FROM items ORDER BY value")
        .fetch_all(pool)
        .await
        .unwrap()
}

/// The reason Phase 11 exists, and the one thing no in-memory fake can show:
/// a chunk's rows and its metadata are committed by the same transaction, so a
/// chunk that fails leaves neither behind.
#[tokio::test]
async fn a_failed_chunk_rolls_back_its_rows_and_its_counters() {
    let (_container, repository) = start().await;
    sqlx::query("CREATE TABLE items (value BIGINT NOT NULL)")
        .execute(repository.pool())
        .await
        .unwrap();

    let launcher = JobLauncher::new(repository);
    let step = ChunkStep::new(
        "load",
        Rows {
            items: vec![1, 2, 3, 4],
            pos: 0,
        },
        Identity,
        ItemTable {
            ok_writes: 1,
            writes: 0,
        },
        nz(2),
    );
    let mut job: Job<PgTx> = Job::new("nightly", vec![Box::new(step)]);

    assert!(launcher.run(&mut job, &params("2026-07-29")).await.is_err());

    // The second chunk inserted 3 and 4 and *then* failed. Both inserts must be
    // gone: this is a real ROLLBACK, not the engine declining to write.
    assert_eq!(item_values(launcher.repository().pool()).await, vec![1, 2]);

    let instance = launcher
        .repository()
        .find_instance("nightly", &params("2026-07-29"))
        .await
        .unwrap()
        .unwrap();
    let execution = launcher
        .repository()
        .last_execution(instance.id())
        .await
        .unwrap()
        .unwrap();
    let steps = launcher
        .repository()
        .step_executions(execution.id())
        .await
        .unwrap();

    assert_eq!(steps[0].status(), BatchStatus::Failed);
    assert_eq!(steps[0].read_count(), 2, "counters describe committed work");
    assert_eq!(
        steps[0].execution_context().get_long(POSITION).unwrap(),
        Some(2),
        "the bookmark commits with the rows it describes"
    );
}

/// FR-5.3 against a real database: restarting the failed run above must insert
/// only the rows the first attempt did not.
#[tokio::test]
async fn a_restart_inserts_each_row_exactly_once() {
    let (_container, repository) = start().await;
    sqlx::query("CREATE TABLE items (value BIGINT NOT NULL)")
        .execute(repository.pool())
        .await
        .unwrap();

    let launcher = JobLauncher::new(repository);
    let job_of = |ok_writes| {
        let step = ChunkStep::new(
            "load",
            Rows {
                items: vec![1, 2, 3, 4],
                pos: 0,
            },
            Identity,
            ItemTable {
                ok_writes,
                writes: 0,
            },
            nz(2),
        );
        Job::<PgTx>::new("nightly", vec![Box::new(step)])
    };

    let mut first = job_of(1);
    assert!(
        launcher
            .run(&mut first, &params("2026-07-29"))
            .await
            .is_err()
    );
    assert_eq!(item_values(launcher.repository().pool()).await, vec![1, 2]);

    let mut second = job_of(usize::MAX);
    launcher
        .run(&mut second, &params("2026-07-29"))
        .await
        .unwrap();

    assert_eq!(
        item_values(launcher.repository().pool()).await,
        vec![1, 2, 3, 4],
        "no row may be inserted twice"
    );
}

#[tokio::test]
async fn identical_parameters_resolve_to_the_same_instance() {
    let (_container, repository) = start().await;

    let first = repository
        .find_or_create_instance("nightly", &params("2026-07-29"))
        .await
        .unwrap();
    let again = repository
        .find_or_create_instance("nightly", &params("2026-07-29"))
        .await
        .unwrap();
    let other = repository
        .find_or_create_instance("nightly", &params("2026-07-30"))
        .await
        .unwrap();

    assert_eq!(first.id(), again.id());
    assert_ne!(first.id(), other.id());
    assert_eq!(first.parameters(), &params("2026-07-29"));
}

#[tokio::test]
async fn a_completed_execution_cannot_be_abandoned() {
    let (_container, repository) = start().await;

    let instance = repository
        .find_or_create_instance("nightly", &params("2026-07-29"))
        .await
        .unwrap();
    let mut execution = repository.create_execution(instance.id()).await.unwrap();
    execution.set_status(BatchStatus::Completed);
    repository.update_execution(&execution).await.unwrap();

    let result = repository.abandon_execution(execution.id()).await;
    assert!(matches!(result, Err(BatchError::CannotAbandon { .. })));

    let reloaded = repository
        .last_execution(instance.id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reloaded.status(), BatchStatus::Completed);
}

#[tokio::test]
async fn abandoning_a_started_execution_succeeds_and_an_unknown_one_errors() {
    let (_container, repository) = start().await;

    let instance = repository
        .find_or_create_instance("nightly", &params("2026-07-29"))
        .await
        .unwrap();
    let mut execution = repository.create_execution(instance.id()).await.unwrap();
    execution.set_status(BatchStatus::Started);
    repository.update_execution(&execution).await.unwrap();

    repository.abandon_execution(execution.id()).await.unwrap();
    assert_eq!(
        repository
            .last_execution(instance.id())
            .await
            .unwrap()
            .unwrap()
            .status(),
        BatchStatus::Abandoned
    );

    let unknown = batchflow_core::JobExecutionId::new(999_999);
    assert!(matches!(
        repository.abandon_execution(unknown).await,
        Err(BatchError::Repository(_))
    ));
}

/// `step_executions` returns insertion order and `last_step_execution` is scoped
/// to the instance — both are contracts Phase 9's restart depends on.
#[tokio::test]
async fn step_execution_ordering_and_scoping_match_the_contract() {
    let (_container, repository) = start().await;

    let instance = repository
        .find_or_create_instance("nightly", &params("2026-07-29"))
        .await
        .unwrap();
    let other = repository
        .find_or_create_instance("nightly", &params("2026-07-30"))
        .await
        .unwrap();

    let execution = repository.create_execution(instance.id()).await.unwrap();
    repository
        .create_step_execution(execution.id(), "first")
        .await
        .unwrap();
    repository
        .create_step_execution(execution.id(), "second")
        .await
        .unwrap();

    let other_execution = repository.create_execution(other.id()).await.unwrap();
    repository
        .create_step_execution(other_execution.id(), "first")
        .await
        .unwrap();

    let steps = repository.step_executions(execution.id()).await.unwrap();
    let names: Vec<&str> = steps.iter().map(|s| s.step_name()).collect();
    assert_eq!(names, ["first", "second"]);

    let last = repository
        .last_step_execution(instance.id(), "first")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(last.job_execution_id(), execution.id());

    assert!(
        repository
            .last_step_execution(instance.id(), "never-ran")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn migrations_are_idempotent() {
    let (_container, repository) = start().await;
    repository.migrate().await.unwrap();
}
