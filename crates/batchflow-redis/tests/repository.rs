//! What Redis promises beyond the shared contract.
//!
//! The 32 contract cases live in `conformance.rs`. These are the properties
//! that are *not* part of `JobRepository`'s promise — rollback, and the
//! atomicity of resolve-or-create — so they belong to this backend and are
//! asserted here.

use batchflow_core::{BatchStatus, JobParameter, JobParameters, JobRepository, StepContribution};
use batchflow_redis::RedisJobRepository;
use testcontainers_modules::redis::Redis;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

async fn start() -> (ContainerAsync<Redis>, RedisJobRepository) {
    let container = Redis::default().start().await.unwrap();
    let port = container.get_host_port_ipv4(6379).await.unwrap();
    let repository = RedisJobRepository::connect(&format!("redis://127.0.0.1:{port}/"))
        .await
        .unwrap();
    (container, repository)
}

fn params(date: &str) -> JobParameters {
    JobParameters::new().with("date", JobParameter::String(date.into()))
}

/// Redis's rollback is stronger than a compensating write and weaker than a
/// database's: the pipeline buffers client-side, so a discarded chunk was never
/// sent. Nothing to undo because nothing happened.
#[tokio::test]
async fn a_rolled_back_transaction_was_never_sent() {
    let (_container, repository) = start().await;
    let instance = repository
        .find_or_create_instance("nightly", &params("2026-08-05"))
        .await
        .unwrap();
    let execution = repository.create_execution(instance.id()).await.unwrap();
    let mut step = repository
        .create_step_execution(execution.id(), "load")
        .await
        .unwrap();

    let mut contribution = StepContribution::new();
    contribution.increment_read(99);
    step.apply(&contribution);
    step.set_status(BatchStatus::Completed);

    let mut tx = repository.begin().await.unwrap();
    repository
        .update_step_execution_in(&mut tx, &step)
        .await
        .unwrap();
    repository.rollback(tx).await.unwrap();

    let reloaded = repository
        .step_executions(execution.id())
        .await
        .unwrap()
        .into_iter()
        .find(|s| s.id() == step.id())
        .unwrap();

    assert_eq!(
        reloaded.read_count(),
        0,
        "a rolled-back chunk must not count"
    );
    assert_eq!(reloaded.status(), BatchStatus::Starting);
}

/// Every queued command lands, or none does. The second write is what makes
/// this more than a restatement of the single-write case.
#[tokio::test]
async fn a_committed_transaction_applies_every_queued_command() {
    let (_container, repository) = start().await;
    let instance = repository
        .find_or_create_instance("nightly", &params("2026-08-05"))
        .await
        .unwrap();
    let execution = repository.create_execution(instance.id()).await.unwrap();

    let mut extract = repository
        .create_step_execution(execution.id(), "extract")
        .await
        .unwrap();
    let mut load = repository
        .create_step_execution(execution.id(), "load")
        .await
        .unwrap();

    let mut contribution = StepContribution::new();
    contribution.increment_read(3);
    extract.apply(&contribution);
    load.apply(&contribution);

    let mut tx = repository.begin().await.unwrap();
    repository
        .update_step_execution_in(&mut tx, &extract)
        .await
        .unwrap();
    repository
        .update_step_execution_in(&mut tx, &load)
        .await
        .unwrap();
    repository.commit(tx).await.unwrap();

    let counts: Vec<usize> = repository
        .step_executions(execution.id())
        .await
        .unwrap()
        .iter()
        .map(batchflow_core::StepExecution::read_count)
        .collect();

    assert_eq!(counts, vec![3, 3]);
}

/// FR-4.2 under contention. Redis has no unique constraint to lean on, so
/// resolve-or-create is a Lua script; check-then-act across two commands is the
/// race two schedulers firing at once would both win.
#[tokio::test]
async fn concurrent_resolution_yields_exactly_one_instance() {
    let (_container, repository) = start().await;

    let mut tasks = Vec::new();
    for _ in 0..16 {
        let repository = repository.clone();
        tasks.push(tokio::spawn(async move {
            repository
                .find_or_create_instance("nightly", &params("2026-08-05"))
                .await
                .unwrap()
                .id()
        }));
    }

    let mut ids = Vec::new();
    for task in tasks {
        ids.push(task.await.unwrap());
    }

    let first = ids[0];
    assert!(
        ids.iter().all(|id| *id == first),
        "16 concurrent resolutions produced more than one instance: {ids:?}"
    );
}
