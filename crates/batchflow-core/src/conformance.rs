//! A shared test suite every [`JobRepository`] implementation must pass.
//!
//! The trait is the framework's main extension point, and until this module
//! existed its contract was pinned by two unrelated test files that had already
//! drifted: the in-memory store checked twenty-one properties, PostgreSQL
//! checked seven, and `abandon_execution` — the escape hatch that releases a
//! crashed instance — was checked only against PostgreSQL.
//!
//! # Using it
//!
//! Enable the `conformance` feature as a **dev-dependency** and invoke the
//! macro once, passing an expression that yields a fresh, empty repository:
//!
//! ```ignore
//! batchflow_core::job_repository_conformance!(setup());
//! ```
//!
//! The expression is evaluated once per generated test and must produce a
//! future resolving to `(guard, repository)`. The guard is whatever must stay
//! alive for the repository to work — a container handle, a temporary
//! directory, or `()` if nothing does. Requires `tokio` with the `macros`
//! feature in the calling crate.
//!
//! # What is deliberately *not* asserted
//!
//! **Rollback.** Per ADR-007 a store with no transactions sets `Tx = ()` and
//! degrades to at-least-once; `InMemoryJobRepository` cannot discard anything.
//! Asserting that a rolled-back chunk vanishes would be asserting a promise the
//! trait does not make. That property is real, but it belongs to the backends
//! that offer it, and it is tested against PostgreSQL alone.
//!
//! **Id values.** In-memory ids come from one shared counter while PostgreSQL
//! uses a sequence per table, so the *same* correct behaviour produces
//! different numbers. Ids are opaque newtypes and the suite only ever compares
//! them for equality.

// Every function here is one named assertion; a doc comment would restate the
// name. The ones that carry rationale have it.
#![allow(missing_docs)]

use crate::{
    BatchError, BatchStatus, ContextValue, ExecutionContext, JobExecution, JobExecutionId,
    JobInstanceId, JobParameter, JobParameters, JobRepository, StepContribution, StepExecutionId,
};

fn params(pairs: &[(&str, &str)]) -> JobParameters {
    pairs
        .iter()
        .fold(JobParameters::new(), |acc, (key, value)| {
            acc.with(*key, JobParameter::String((*value).to_owned()))
        })
}

/// A fresh instance with one open execution — the state most cases start from.
async fn open_execution<R: JobRepository>(repository: &R, job: &str) -> JobExecution {
    let instance = repository
        .find_or_create_instance(job, &params(&[("date", "2026-08-05")]))
        .await
        .expect("create instance");

    repository
        .create_execution(instance.id())
        .await
        .expect("create execution")
}

// ---------------------------------------------------------------- identity

/// FR-4.2: the instance *is* `(job_name, parameters)`. This is what makes a
/// relaunch a restart rather than a second run.
pub async fn identical_parameters_resolve_to_the_same_instance<R: JobRepository>(repository: &R) {
    let first = repository
        .find_or_create_instance("nightly", &params(&[("date", "2026-08-05")]))
        .await
        .unwrap();
    let second = repository
        .find_or_create_instance("nightly", &params(&[("date", "2026-08-05")]))
        .await
        .unwrap();

    assert_eq!(first.id(), second.id());
}

pub async fn different_parameters_resolve_to_different_instances<R: JobRepository>(repository: &R) {
    let first = repository
        .find_or_create_instance("nightly", &params(&[("date", "2026-08-05")]))
        .await
        .unwrap();
    let second = repository
        .find_or_create_instance("nightly", &params(&[("date", "2026-08-06")]))
        .await
        .unwrap();

    assert_ne!(first.id(), second.id());
}

/// Identity is by *content*, not by insertion order — which is why
/// `JobParameters` is a `BTreeMap`. A backend keying on a serialized form must
/// serialize it in sorted order or a re-run silently becomes a new instance.
pub async fn parameter_order_does_not_affect_identity<R: JobRepository>(repository: &R) {
    let first = repository
        .find_or_create_instance("nightly", &params(&[("a", "1"), ("b", "2")]))
        .await
        .unwrap();
    let second = repository
        .find_or_create_instance("nightly", &params(&[("b", "2"), ("a", "1")]))
        .await
        .unwrap();

    assert_eq!(first.id(), second.id());
}

pub async fn the_same_parameters_under_different_jobs_differ<R: JobRepository>(repository: &R) {
    let nightly = repository
        .find_or_create_instance("nightly", &params(&[("date", "2026-08-05")]))
        .await
        .unwrap();
    let hourly = repository
        .find_or_create_instance("hourly", &params(&[("date", "2026-08-05")]))
        .await
        .unwrap();

    assert_ne!(nightly.id(), hourly.id());
}

pub async fn find_instance_returns_none_when_never_created<R: JobRepository>(repository: &R) {
    let found = repository
        .find_instance("nightly", &params(&[("date", "2026-08-05")]))
        .await
        .unwrap();

    assert!(found.is_none());
}

/// `find_instance` must not create. A caller asking "did this ever run?" that
/// silently created the instance would make the question unanswerable.
pub async fn find_instance_finds_an_existing_instance_without_creating<R: JobRepository>(
    repository: &R,
) {
    let created = repository
        .find_or_create_instance("nightly", &params(&[("date", "2026-08-05")]))
        .await
        .unwrap();

    let found = repository
        .find_instance("nightly", &params(&[("date", "2026-08-05")]))
        .await
        .unwrap()
        .expect("instance exists");

    assert_eq!(created.id(), found.id());
    assert_eq!(found.job_name(), "nightly");
}

// -------------------------------------------------------------- executions

/// The restart model in one assertion: one instance, many attempts.
pub async fn an_instance_can_have_several_distinct_executions<R: JobRepository>(repository: &R) {
    let instance = repository
        .find_or_create_instance("nightly", &params(&[("date", "2026-08-05")]))
        .await
        .unwrap();

    let first = repository.create_execution(instance.id()).await.unwrap();
    let second = repository.create_execution(instance.id()).await.unwrap();

    assert_ne!(first.id(), second.id());
    assert_eq!(first.instance_id(), instance.id());
    assert_eq!(second.instance_id(), instance.id());
}

pub async fn a_new_execution_starts_in_starting<R: JobRepository>(repository: &R) {
    let execution = open_execution(repository, "nightly").await;

    assert_eq!(execution.status(), BatchStatus::Starting);
    assert!(execution.execution_context().is_empty());
}

pub async fn create_execution_rejects_an_unknown_instance<R: JobRepository>(repository: &R) {
    let result = repository
        .create_execution(JobInstanceId::new(918_273))
        .await;

    assert!(result.is_err(), "an unknown instance must not open");
}

// ------------------------------------------------------- the launch gate

/// `start_execution` opens an execution that already holds the instance.
///
/// `Started`, not `Starting`: a row that exists but does not yet hold the
/// instance is a window the gate does not cover, and closing that window is
/// half the point of the method.
pub async fn start_execution_opens_a_started_execution<R: JobRepository>(repository: &R) {
    let instance = repository
        .find_or_create_instance("nightly", &params(&[("date", "2026-08-06")]))
        .await
        .unwrap();

    let execution = repository
        .start_execution("nightly", instance.id())
        .await
        .unwrap();

    assert_eq!(execution.status(), BatchStatus::Started);
    assert_eq!(execution.instance_id(), instance.id());

    // Persisted, not merely returned.
    let reloaded = repository
        .last_execution(instance.id())
        .await
        .unwrap()
        .expect("the launch must be durable");
    assert_eq!(reloaded.id(), execution.id());
    assert_eq!(reloaded.status(), BatchStatus::Started);
}

/// FR-4.4: a completed instance refuses a relaunch, and refuses it *without*
/// leaving a new execution behind.
pub async fn start_execution_refuses_a_completed_instance<R: JobRepository>(repository: &R) {
    let instance = repository
        .find_or_create_instance("nightly", &params(&[("date", "2026-08-06")]))
        .await
        .unwrap();

    let mut first = repository
        .start_execution("nightly", instance.id())
        .await
        .unwrap();
    first.set_status(BatchStatus::Completed);
    repository.update_execution(&first).await.unwrap();

    let refused = repository.start_execution("nightly", instance.id()).await;

    assert!(
        matches!(refused, Err(BatchError::JobInstanceAlreadyComplete { .. })),
        "expected JobInstanceAlreadyComplete, got {refused:?}"
    );

    // A refusal that inserted first would pass a status assertion alone.
    assert_eq!(
        repository.executions(instance.id()).await.unwrap().len(),
        1,
        "a refused launch must not leave an execution behind"
    );
}

/// The running-execution gate. An implementation that checks and then inserts
/// non-atomically still passes this one — the race case below is what catches
/// that — but an implementation with no check at all fails here.
pub async fn start_execution_refuses_a_live_execution<R: JobRepository>(repository: &R) {
    let instance = repository
        .find_or_create_instance("nightly", &params(&[("date", "2026-08-06")]))
        .await
        .unwrap();

    let running = repository
        .start_execution("nightly", instance.id())
        .await
        .unwrap();

    let refused = repository.start_execution("nightly", instance.id()).await;

    let Err(BatchError::JobExecutionAlreadyRunning { execution_id, .. }) = refused else {
        panic!("expected JobExecutionAlreadyRunning, got {refused:?}");
    };
    assert_eq!(
        execution_id,
        running.id(),
        "the refusal must name the execution actually holding the instance"
    );

    assert_eq!(repository.executions(instance.id()).await.unwrap().len(), 1);
}

/// The restart door, and the control for the two refusals above: a terminal
/// *unsuccessful* status must let the instance run again, or a failed job could
/// never be retried.
pub async fn start_execution_allows_a_terminal_unsuccessful_instance<R: JobRepository>(
    repository: &R,
) {
    let instance = repository
        .find_or_create_instance("nightly", &params(&[("date", "2026-08-06")]))
        .await
        .unwrap();

    for status in [
        BatchStatus::Failed,
        BatchStatus::Stopped,
        BatchStatus::Abandoned,
    ] {
        let mut attempt = repository
            .start_execution("nightly", instance.id())
            .await
            .unwrap_or_else(|error| panic!("a {status:?} instance must be launchable: {error:?}"));

        attempt.set_status(status);
        repository.update_execution(&attempt).await.unwrap();
    }

    // Three attempts, each opened after the previous reached a restartable
    // terminal status.
    assert_eq!(repository.executions(instance.id()).await.unwrap().len(), 3);
}

pub async fn start_execution_rejects_an_unknown_instance<R: JobRepository>(repository: &R) {
    let result = repository
        .start_execution("nightly", JobInstanceId::new(9999))
        .await;

    assert!(
        matches!(result, Err(BatchError::Repository(_))),
        "expected a repository error, got {result:?}"
    );
}

/// **The race.** Two launchers going for the same instance at the same moment:
/// exactly one may win.
///
/// This is the audit's CONC-1. The gate used to be a read, a decision and a
/// write in `JobLauncher`, which two processes could interleave — both read
/// "no live execution", both inserted, and one instance ran twice. For a
/// billing job that is a duplicated financial effect, and it happens exactly
/// when it is most likely to: two replicas of one `CronJob`.
///
/// A check-then-act implementation fails this by producing **two** executions.
/// `tokio::join!` interleaves the two futures at every `.await` inside them,
/// which is precisely where a non-atomic implementation can be split.
pub async fn only_one_of_two_concurrent_launches_wins<R: JobRepository>(repository: &R) {
    let instance = repository
        .find_or_create_instance("nightly", &params(&[("date", "2026-08-06")]))
        .await
        .unwrap();

    let (first, second) = tokio::join!(
        repository.start_execution("nightly", instance.id()),
        repository.start_execution("nightly", instance.id()),
    );

    let winners = usize::from(first.is_ok()) + usize::from(second.is_ok());
    assert_eq!(
        winners, 1,
        "exactly one launch may win; got first={first:?} second={second:?}"
    );

    // The store is the record: two rows here means both inserted, whatever the
    // return values claimed.
    assert_eq!(
        repository.executions(instance.id()).await.unwrap().len(),
        1,
        "a refused launch must not leave an execution behind"
    );

    // The loser is told why, rather than getting an opaque constraint error it
    // cannot act on.
    let loser = if first.is_err() { first } else { second };
    assert!(
        matches!(
            loser,
            Err(BatchError::JobExecutionAlreadyRunning { .. })
                | Err(BatchError::JobInstanceAlreadyComplete { .. })
        ),
        "the loser must get a launch refusal, got {loser:?}"
    );
}

pub async fn update_execution_persists_a_status_change<R: JobRepository>(repository: &R) {
    let mut execution = open_execution(repository, "nightly").await;
    execution.set_status(BatchStatus::Completed);
    repository.update_execution(&execution).await.unwrap();

    let reloaded = repository
        .last_execution(execution.instance_id())
        .await
        .unwrap()
        .expect("execution exists");

    assert_eq!(reloaded.status(), BatchStatus::Completed);
}

/// Pins *replace*, not append. A store that appended would still pass a test
/// that only reads the row back — the second entity is what exposes it.
pub async fn update_execution_replaces_rather_than_appending<R: JobRepository>(repository: &R) {
    let instance = repository
        .find_or_create_instance("nightly", &params(&[("date", "2026-08-05")]))
        .await
        .unwrap();

    let mut first = repository.create_execution(instance.id()).await.unwrap();
    let second = repository.create_execution(instance.id()).await.unwrap();

    first.set_status(BatchStatus::Failed);
    repository.update_execution(&first).await.unwrap();

    let all = repository.executions(instance.id()).await.unwrap();

    assert_eq!(all.len(), 2, "updating must not add a row");
    assert_eq!(
        repository
            .last_execution(instance.id())
            .await
            .unwrap()
            .unwrap()
            .id(),
        second.id(),
        "an update must not disturb ordering"
    );
}

pub async fn update_execution_rejects_an_unknown_execution<R: JobRepository>(repository: &R) {
    let execution = JobExecution::new(JobExecutionId::new(918_273), JobInstanceId::new(918_274));

    let result = repository.update_execution(&execution).await;

    assert!(
        result.is_err(),
        "updating an unknown execution must error rather than insert"
    );
}

/// The bookmark at job level: a step that died at item 900 leaves a context,
/// and restart depends on reading it back verbatim.
pub async fn execution_context_round_trips<R: JobRepository>(repository: &R) {
    let mut execution = open_execution(repository, "nightly").await;

    let mut context = ExecutionContext::new();
    context.put("position", ContextValue::Long(900));
    context.put("file", ContextValue::String("feed.csv".into()));
    context.put("done", ContextValue::Bool(false));
    execution.set_execution_context(context);

    repository.update_execution(&execution).await.unwrap();

    let reloaded = repository
        .last_execution(execution.instance_id())
        .await
        .unwrap()
        .unwrap();
    let context = reloaded.execution_context();

    assert_eq!(context.get_long("position").unwrap(), Some(900));
    assert_eq!(context.get_string("file").unwrap(), Some("feed.csv"));
    assert_eq!(context.get_bool("done").unwrap(), Some(false));
}

pub async fn last_execution_is_none_before_any_attempt<R: JobRepository>(repository: &R) {
    let instance = repository
        .find_or_create_instance("nightly", &params(&[("date", "2026-08-05")]))
        .await
        .unwrap();

    assert!(
        repository
            .last_execution(instance.id())
            .await
            .unwrap()
            .is_none()
    );
}

/// The FR-4.4 gate reads this, so "most recent" must mean most recent *for
/// this instance* rather than most recent overall.
pub async fn last_execution_is_scoped_to_its_instance<R: JobRepository>(repository: &R) {
    let nightly = repository
        .find_or_create_instance("nightly", &params(&[("date", "2026-08-05")]))
        .await
        .unwrap();
    let hourly = repository
        .find_or_create_instance("hourly", &params(&[("date", "2026-08-05")]))
        .await
        .unwrap();

    let nightly_execution = repository.create_execution(nightly.id()).await.unwrap();
    // Created last overall: an unscoped implementation returns this one.
    repository.create_execution(hourly.id()).await.unwrap();

    let found = repository
        .last_execution(nightly.id())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(found.id(), nightly_execution.id());
}

/// Oldest first, and the last element agrees with `last_execution`. The two are
/// separate queries in a SQL backend, so a stray `DESC` in one of them would
/// otherwise go unnoticed.
pub async fn executions_lists_every_attempt_oldest_first<R: JobRepository>(repository: &R) {
    let instance = repository
        .find_or_create_instance("nightly", &params(&[("date", "2026-08-05")]))
        .await
        .unwrap();

    let first = repository.create_execution(instance.id()).await.unwrap();
    let second = repository.create_execution(instance.id()).await.unwrap();
    let third = repository.create_execution(instance.id()).await.unwrap();

    let all = repository.executions(instance.id()).await.unwrap();

    assert_eq!(
        all.iter().map(JobExecution::id).collect::<Vec<_>>(),
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

/// The gap `executions` closes: once a second attempt exists, the first one's
/// record is unreachable through `last_execution`, and it is the one holding
/// the status and bookmark the failure left behind.
pub async fn executions_still_reaches_a_superseded_attempt<R: JobRepository>(repository: &R) {
    let instance = repository
        .find_or_create_instance("nightly", &params(&[("date", "2026-08-05")]))
        .await
        .unwrap();

    let mut failed = repository.create_execution(instance.id()).await.unwrap();
    failed.set_status(BatchStatus::Failed);
    let mut context = ExecutionContext::new();
    context.put("position", ContextValue::Long(7));
    failed.set_execution_context(context);
    repository.update_execution(&failed).await.unwrap();

    repository.create_execution(instance.id()).await.unwrap();

    let all = repository.executions(instance.id()).await.unwrap();

    assert_eq!(all.len(), 2);
    assert_eq!(all[0].status(), BatchStatus::Failed);
    assert_eq!(
        all[0].execution_context().get_long("position").unwrap(),
        Some(7)
    );
    assert_eq!(all[1].status(), BatchStatus::Starting);
}

pub async fn executions_are_scoped_to_their_instance<R: JobRepository>(repository: &R) {
    let nightly = repository
        .find_or_create_instance("nightly", &params(&[("date", "2026-08-05")]))
        .await
        .unwrap();
    let hourly = repository
        .find_or_create_instance("hourly", &params(&[("date", "2026-08-05")]))
        .await
        .unwrap();

    let nightly_execution = repository.create_execution(nightly.id()).await.unwrap();
    repository.create_execution(hourly.id()).await.unwrap();

    let all = repository.executions(nightly.id()).await.unwrap();

    assert_eq!(all.len(), 1);
    assert_eq!(all[0].id(), nightly_execution.id());
}

pub async fn executions_is_empty_before_any_attempt<R: JobRepository>(repository: &R) {
    let instance = repository
        .find_or_create_instance("nightly", &params(&[("date", "2026-08-05")]))
        .await
        .unwrap();

    assert!(
        repository
            .executions(instance.id())
            .await
            .unwrap()
            .is_empty()
    );
}

// ----------------------------------------------------------------- abandon

pub async fn abandoning_a_started_execution_releases_the_instance<R: JobRepository>(
    repository: &R,
) {
    let mut execution = open_execution(repository, "nightly").await;
    execution.set_status(BatchStatus::Started);
    repository.update_execution(&execution).await.unwrap();

    repository.abandon_execution(execution.id()).await.unwrap();

    let reloaded = repository
        .last_execution(execution.instance_id())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(reloaded.status(), BatchStatus::Abandoned);
}

/// Non-negotiable: `abandon_execution` writes the exact field the FR-4.4 gate
/// reads, so allowing it on a `Completed` execution would make an
/// already-finished instance relaunchable in two calls.
pub async fn a_completed_execution_cannot_be_abandoned<R: JobRepository>(repository: &R) {
    let mut execution = open_execution(repository, "nightly").await;
    execution.set_status(BatchStatus::Completed);
    repository.update_execution(&execution).await.unwrap();

    let result = repository.abandon_execution(execution.id()).await;

    assert!(
        matches!(result, Err(BatchError::CannotAbandon { .. })),
        "expected CannotAbandon, got {result:?}"
    );

    let reloaded = repository
        .last_execution(execution.instance_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reloaded.status(), BatchStatus::Completed);
}

/// Distinguishable from `CannotAbandon`: "no such execution" and "that one is
/// finished" call for different operator responses.
pub async fn abandoning_an_unknown_execution_errors<R: JobRepository>(repository: &R) {
    let result = repository
        .abandon_execution(JobExecutionId::new(918_273))
        .await;

    assert!(result.is_err(), "an unknown execution must not abandon");
}

// ----------------------------------------------------------- step executions

pub async fn create_step_execution_rejects_an_unknown_job_execution<R: JobRepository>(
    repository: &R,
) {
    let result = repository
        .create_step_execution(JobExecutionId::new(918_273), "load")
        .await;

    assert!(result.is_err(), "an unknown job execution must not open");
}

pub async fn update_step_execution_persists_counters_and_status<R: JobRepository>(repository: &R) {
    let execution = open_execution(repository, "nightly").await;
    let mut step = repository
        .create_step_execution(execution.id(), "load")
        .await
        .unwrap();

    let mut contribution = StepContribution::new();
    contribution.increment_read(10);
    contribution.increment_write(7);
    contribution.increment_filter(2);
    contribution.increment_skip(1);
    step.apply(&contribution);
    step.set_status(BatchStatus::Completed);
    repository.update_step_execution(&step).await.unwrap();

    let reloaded = repository
        .step_executions(execution.id())
        .await
        .unwrap()
        .into_iter()
        .find(|s| s.id() == step.id())
        .expect("step execution exists");

    assert_eq!(reloaded.status(), BatchStatus::Completed);
    assert_eq!(reloaded.read_count(), 10);
    assert_eq!(reloaded.write_count(), 7);
    assert_eq!(reloaded.filter_count(), 2);
    assert_eq!(reloaded.skip_count(), 1);
}

pub async fn update_step_execution_rejects_an_unknown_step_execution<R: JobRepository>(
    repository: &R,
) {
    let execution = open_execution(repository, "nightly").await;
    let mut step = repository
        .create_step_execution(execution.id(), "load")
        .await
        .unwrap();

    // Same shape, id that was never minted.
    let stolen = StepExecutionId::new(918_273);
    step.set_status(BatchStatus::Completed);
    let mut orphan = step.clone();
    orphan.set_status(BatchStatus::Failed);

    let result = repository
        .update_step_execution(&rekey(orphan, stolen))
        .await;

    assert!(
        result.is_err(),
        "updating an unknown step execution must error rather than insert"
    );
}

/// Restoring an id the repository never minted, which is the only way to build
/// an orphan without a public setter.
fn rekey(step: crate::StepExecution, id: StepExecutionId) -> crate::StepExecution {
    let mut rebuilt = crate::StepExecution::new(id, step.job_execution_id(), step.step_name());
    rebuilt.set_status(step.status());
    rebuilt
}

/// Phase 9b reads this to decide what to skip on a restart, and it is keyed on
/// the *instance* — the attempt that succeeded is by definition a different
/// execution from the one asking.
pub async fn last_step_execution_spans_attempts_of_one_instance<R: JobRepository>(repository: &R) {
    let instance = repository
        .find_or_create_instance("nightly", &params(&[("date", "2026-08-05")]))
        .await
        .unwrap();

    let first_attempt = repository.create_execution(instance.id()).await.unwrap();
    let mut step = repository
        .create_step_execution(first_attempt.id(), "load")
        .await
        .unwrap();
    step.set_status(BatchStatus::Completed);
    repository.update_step_execution(&step).await.unwrap();

    // A second attempt that has not run the step yet.
    repository.create_execution(instance.id()).await.unwrap();

    let found = repository
        .last_step_execution(instance.id(), "load")
        .await
        .unwrap()
        .expect("the first attempt's record is still reachable");

    assert_eq!(found.id(), step.id());
    assert_eq!(found.status(), BatchStatus::Completed);
}

pub async fn last_step_execution_is_scoped_to_the_step_name<R: JobRepository>(repository: &R) {
    let instance = repository
        .find_or_create_instance("nightly", &params(&[("date", "2026-08-05")]))
        .await
        .unwrap();
    let execution = repository.create_execution(instance.id()).await.unwrap();

    let extract = repository
        .create_step_execution(execution.id(), "extract")
        .await
        .unwrap();
    // Created later: an unscoped implementation returns this one.
    repository
        .create_step_execution(execution.id(), "load")
        .await
        .unwrap();

    let found = repository
        .last_step_execution(instance.id(), "extract")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(found.id(), extract.id());
}

pub async fn last_step_execution_is_none_when_the_step_never_ran<R: JobRepository>(repository: &R) {
    let instance = repository
        .find_or_create_instance("nightly", &params(&[("date", "2026-08-05")]))
        .await
        .unwrap();
    repository.create_execution(instance.id()).await.unwrap();

    assert!(
        repository
            .last_step_execution(instance.id(), "never-ran")
            .await
            .unwrap()
            .is_none()
    );
}

pub async fn step_executions_come_back_in_the_order_they_ran<R: JobRepository>(repository: &R) {
    let execution = open_execution(repository, "nightly").await;

    let extract = repository
        .create_step_execution(execution.id(), "extract")
        .await
        .unwrap();
    let transform = repository
        .create_step_execution(execution.id(), "transform")
        .await
        .unwrap();
    let load = repository
        .create_step_execution(execution.id(), "load")
        .await
        .unwrap();

    let all = repository.step_executions(execution.id()).await.unwrap();

    assert_eq!(
        all.iter().map(|s| s.id()).collect::<Vec<_>>(),
        vec![extract.id(), transform.id(), load.id()]
    );
}

pub async fn step_executions_are_scoped_to_their_job_execution<R: JobRepository>(repository: &R) {
    let instance = repository
        .find_or_create_instance("nightly", &params(&[("date", "2026-08-05")]))
        .await
        .unwrap();
    let first = repository.create_execution(instance.id()).await.unwrap();
    let second = repository.create_execution(instance.id()).await.unwrap();

    let mine = repository
        .create_step_execution(first.id(), "load")
        .await
        .unwrap();
    repository
        .create_step_execution(second.id(), "load")
        .await
        .unwrap();

    let all = repository.step_executions(first.id()).await.unwrap();

    assert_eq!(all.len(), 1);
    assert_eq!(all[0].id(), mine.id());
}

/// The step-level bookmark, which is the one `ItemReader::open` actually reads.
pub async fn step_execution_context_round_trips<R: JobRepository>(repository: &R) {
    let execution = open_execution(repository, "nightly").await;
    let mut step = repository
        .create_step_execution(execution.id(), "load")
        .await
        .unwrap();

    let mut context = ExecutionContext::new();
    context.put("position", ContextValue::Long(4_200));
    step.set_execution_context(context);
    repository.update_step_execution(&step).await.unwrap();

    let reloaded = repository
        .last_step_execution(
            repository
                .last_execution(execution.instance_id())
                .await
                .unwrap()
                .unwrap()
                .instance_id(),
            "load",
        )
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        reloaded.execution_context().get_long("position").unwrap(),
        Some(4_200)
    );
}

// ------------------------------------------------------------- transactions

/// The commit point: what `update_step_execution_in` writes inside a
/// transaction must survive the commit. Rollback is *not* asserted — see the
/// module docs.
pub async fn a_committed_transaction_persists_its_step_execution<R: JobRepository>(repository: &R) {
    let execution = open_execution(repository, "nightly").await;
    let mut step = repository
        .create_step_execution(execution.id(), "load")
        .await
        .unwrap();

    let mut contribution = StepContribution::new();
    contribution.increment_read(5);
    step.apply(&contribution);

    let mut tx = repository.begin().await.unwrap();
    repository
        .update_step_execution_in(&mut tx, &step)
        .await
        .unwrap();
    repository.commit(tx).await.unwrap();

    let reloaded = repository
        .step_executions(execution.id())
        .await
        .unwrap()
        .into_iter()
        .find(|s| s.id() == step.id())
        .expect("step execution exists");

    assert_eq!(reloaded.read_count(), 5);
}

/// Generates one `#[tokio::test]` per conformance case against `$setup`.
///
/// `$setup` is evaluated once per test and must yield `(guard, repository)`.
/// See the [module docs](self) for the full contract and for what the suite
/// deliberately does not assert.
///
/// **This macro is the suite's registry.** A case added to this module but not
/// listed here runs nowhere, so add both together.
#[macro_export]
macro_rules! job_repository_conformance {
    ($setup:expr) => {
        $crate::__conformance_cases! {
            $setup,
            identical_parameters_resolve_to_the_same_instance,
            different_parameters_resolve_to_different_instances,
            parameter_order_does_not_affect_identity,
            the_same_parameters_under_different_jobs_differ,
            find_instance_returns_none_when_never_created,
            find_instance_finds_an_existing_instance_without_creating,
            an_instance_can_have_several_distinct_executions,
            a_new_execution_starts_in_starting,
            create_execution_rejects_an_unknown_instance,
            start_execution_opens_a_started_execution,
            start_execution_refuses_a_completed_instance,
            start_execution_refuses_a_live_execution,
            start_execution_allows_a_terminal_unsuccessful_instance,
            start_execution_rejects_an_unknown_instance,
            only_one_of_two_concurrent_launches_wins,
            update_execution_persists_a_status_change,
            update_execution_replaces_rather_than_appending,
            update_execution_rejects_an_unknown_execution,
            execution_context_round_trips,
            last_execution_is_none_before_any_attempt,
            last_execution_is_scoped_to_its_instance,
            executions_lists_every_attempt_oldest_first,
            executions_still_reaches_a_superseded_attempt,
            executions_are_scoped_to_their_instance,
            executions_is_empty_before_any_attempt,
            abandoning_a_started_execution_releases_the_instance,
            a_completed_execution_cannot_be_abandoned,
            abandoning_an_unknown_execution_errors,
            create_step_execution_rejects_an_unknown_job_execution,
            update_step_execution_persists_counters_and_status,
            update_step_execution_rejects_an_unknown_step_execution,
            last_step_execution_spans_attempts_of_one_instance,
            last_step_execution_is_scoped_to_the_step_name,
            last_step_execution_is_none_when_the_step_never_ran,
            step_executions_come_back_in_the_order_they_ran,
            step_executions_are_scoped_to_their_job_execution,
            step_execution_context_round_trips,
            a_committed_transaction_persists_its_step_execution,
        }
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __conformance_cases {
    ($setup:expr, $($case:ident),* $(,)?) => {
        $(
            #[tokio::test]
            async fn $case() {
                let (_guard, repository) = $setup.await;
                $crate::conformance::$case(&repository).await;
            }
        )*
    };
}
