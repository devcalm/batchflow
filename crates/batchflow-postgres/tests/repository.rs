//! Integration tests against a real Postgres, per ADR-007: the transaction
//! abstraction is validated here and nowhere else.

use batchflow_core::{
    BatchError, BatchStatus, ChunkStep, ContextValue, ExecutionContext, ItemProcessor, ItemReader,
    Job, JobLauncher, JobParameter, JobParameters, JobRepository, StepContribution,
    TransactionalWriter,
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
                .map_err(|_| BatchError::read(format!("negative bookmark {position}")))?;
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
                .map_err(BatchError::write)?;
        }

        self.writes += 1;
        if self.writes > self.ok_writes {
            return Err(BatchError::write("boom"));
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

/// FR-6.2: `skip_count` survives a round trip through the database.
///
/// The column arrived in migration 0002, so this also proves the migration
/// applies on top of 0001 rather than only in a freshly-created schema.
#[tokio::test]
async fn skip_count_round_trips_through_the_database() {
    let (_container, repository) = start().await;

    let instance = repository
        .find_or_create_instance("nightly", &params("2026-07-29"))
        .await
        .unwrap();
    let execution = repository.create_execution(instance.id()).await.unwrap();
    let mut step = repository
        .create_step_execution(execution.id(), "load")
        .await
        .unwrap();

    // A fresh row must read back as zero, not as NULL or a default that the
    // engine would fold into a wrong total.
    assert_eq!(step.skip_count(), 0);

    let mut contribution = StepContribution::new();
    contribution.increment_read(10);
    contribution.increment_write(7);
    contribution.increment_filter(1);
    contribution.increment_skip(2);
    step.apply(&contribution);
    repository.update_step_execution(&step).await.unwrap();

    // Read back through both query paths — they are separate SELECTs, and a
    // column added to one and missed in the other is exactly the bug a single
    // assertion would hide.
    let latest = repository
        .last_step_execution(instance.id(), "load")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(latest.skip_count(), 2);
    assert_eq!(latest.filter_count(), 1, "skips did not land in filters");

    let listed = repository.step_executions(execution.id()).await.unwrap();
    assert_eq!(listed[0].skip_count(), 2);
}

/// The `CHECK` constraint covers the new column too — a negative skip count is
/// corruption, and the database refuses it rather than the engine discovering
/// it later as a `usize` the size of the universe.
#[tokio::test]
async fn a_negative_skip_count_is_rejected_by_the_database() {
    let (_container, repository) = start().await;

    let instance = repository
        .find_or_create_instance("nightly", &params("2026-07-29"))
        .await
        .unwrap();
    let execution = repository.create_execution(instance.id()).await.unwrap();
    let step = repository
        .create_step_execution(execution.id(), "load")
        .await
        .unwrap();

    let result = sqlx::query("UPDATE step_execution SET skip_count = -1 WHERE id = $1")
        .bind(step.id().get())
        .execute(repository.pool())
        .await;

    assert!(result.is_err(), "the CHECK constraint must reject this");
}

/// `executions` and `last_execution` are separate `ORDER BY` clauses, so a
/// stray `DESC` in one of them is invisible to any test that checks only one.
#[tokio::test]
async fn executions_lists_every_attempt_oldest_first() {
    let (_container, repository) = start().await;
    let instance = repository
        .find_or_create_instance("nightly", &params("2026-08-05"))
        .await
        .unwrap();

    let first = repository.create_execution(instance.id()).await.unwrap();
    let second = repository.create_execution(instance.id()).await.unwrap();
    let third = repository.create_execution(instance.id()).await.unwrap();

    let all = repository.executions(instance.id()).await.unwrap();

    assert_eq!(
        all.iter().map(|e| e.id()).collect::<Vec<_>>(),
        vec![first.id(), second.id(), third.id()]
    );
    assert_eq!(
        all.last().unwrap().id(),
        repository
            .last_execution(instance.id())
            .await
            .unwrap()
            .unwrap()
            .id()
    );
}

/// The gap the method closes: a superseded attempt stays reachable, with the
/// status and bookmark it died holding.
#[tokio::test]
async fn executions_still_reaches_a_superseded_attempt() {
    let (_container, repository) = start().await;
    let instance = repository
        .find_or_create_instance("nightly", &params("2026-08-05"))
        .await
        .unwrap();

    let mut failed = repository.create_execution(instance.id()).await.unwrap();
    failed.set_status(BatchStatus::Failed);
    let mut context = ExecutionContext::new();
    context.put(POSITION, ContextValue::Long(7));
    failed.set_execution_context(context);
    repository.update_execution(&failed).await.unwrap();

    repository.create_execution(instance.id()).await.unwrap();

    let all = repository.executions(instance.id()).await.unwrap();

    assert_eq!(all.len(), 2);
    assert_eq!(all[0].status(), BatchStatus::Failed);
    assert_eq!(
        all[0].execution_context().get_long(POSITION).unwrap(),
        Some(7)
    );
    assert_eq!(all[1].status(), BatchStatus::Starting);
}

#[tokio::test]
async fn executions_are_scoped_to_their_instance() {
    let (_container, repository) = start().await;
    let nightly = repository
        .find_or_create_instance("nightly", &params("2026-08-05"))
        .await
        .unwrap();
    let hourly = repository
        .find_or_create_instance("hourly", &params("2026-08-05"))
        .await
        .unwrap();

    let nightly_exec = repository.create_execution(nightly.id()).await.unwrap();
    repository.create_execution(hourly.id()).await.unwrap();

    let all = repository.executions(nightly.id()).await.unwrap();

    assert_eq!(all.len(), 1);
    assert_eq!(all[0].id(), nightly_exec.id());
    assert!(
        repository
            .executions(hourly.id())
            .await
            .unwrap()
            .iter()
            .all(|e| e.id() != nightly_exec.id())
    );
}
