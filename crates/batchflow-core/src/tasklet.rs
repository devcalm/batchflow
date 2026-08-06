use crate::metrics::{
    ITEMS_FILTERED, ITEMS_READ, ITEMS_SKIPPED, ITEMS_WRITTEN, LABEL_JOB, LABEL_PHASE, LABEL_STEP,
};
use crate::{BatchError, ExecutionContext, Step, StepCommit, StepContribution, Unmanaged};
use ::metrics::{Counter, counter};
use async_trait::async_trait;
use std::future::Future;

/// The [`LABEL_PHASE`] value for skips a tasklet reported itself. A tasklet is
/// opaque, so there is no finer phase to attribute them to.
const PHASE_TASKLET: &str = "tasklet";

/// Whether a [`Tasklet`] has more work to do.
///
/// Returned rather than looped internally so that "more work" gets a commit
/// point: each `Continuable` pass ends in its own transaction, and the bookmark
/// the tasklet left in the [`ExecutionContext`] is durable with it. A tasklet
/// that loops inside a single `execute` commits once at the end, and a crash
/// before that loses all of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepeatStatus {
    /// Call `execute` again, in a fresh transaction.
    ///
    /// The tasklet owes progress. Nothing bounds this loop — there is no
    /// item count to bound it *with*, the way a skip limit bounds a
    /// non-advancing reader — so a tasklet that always returns `Continuable`
    /// runs until the process is killed.
    Continuable,
    /// The unit of work is complete; the step succeeds.
    Finished,
}

/// A step that is one unit of work rather than a chunk loop (FR-1.2).
///
/// Truncating a staging table, archiving a file, calling a stored procedure:
/// work with no items to read and therefore no commit interval to size. The
/// counterpart is [`ChunkStep`](crate::ChunkStep); both are wrapped into a
/// [`Step`] and are otherwise indistinguishable to a [`Job`](crate::Job).
///
/// Implementors that need the step's transaction implement
/// [`TransactionalTasklet`] instead; this trait is adapted to any `Tx` with
/// [`Unmanaged`], exactly as [`ItemWriter`](crate::ItemWriter) is.
///
/// ```
/// use batchflow_core::{
///     BatchError, ExecutionContext, RepeatStatus, StepContribution, Tasklet,
/// };
///
/// struct TruncateStaging;
///
/// impl Tasklet for TruncateStaging {
///     async fn execute(
///         &mut self,
///         _context: &mut ExecutionContext,
///         contribution: &mut StepContribution,
///     ) -> Result<RepeatStatus, BatchError> {
///         contribution.increment_write(1);
///         Ok(RepeatStatus::Finished)
///     }
/// }
/// ```
pub trait Tasklet: Send {
    /// Does the work, reporting what it did through `contribution`.
    ///
    /// `context` is the step's bookmark bag, restored from the previous attempt
    /// on a restart — which is what makes a `Continuable` tasklet resumable.
    fn execute(
        &mut self,
        context: &mut ExecutionContext,
        contribution: &mut StepContribution,
    ) -> impl Future<Output = Result<RepeatStatus, BatchError>> + Send;
}

/// A [`Tasklet`] that enlists in the step's transaction, so its writes commit
/// with the counters and the bookmark (FR-2.4).
pub trait TransactionalTasklet<Tx>: Send {
    /// Does the work inside the step's transaction.
    fn execute(
        &mut self,
        tx: &mut Tx,
        context: &mut ExecutionContext,
        contribution: &mut StepContribution,
    ) -> impl Future<Output = Result<RepeatStatus, BatchError>> + Send;
}

impl<T, Tx> TransactionalTasklet<Tx> for Unmanaged<T>
where
    T: Tasklet,
    Tx: Send,
{
    fn execute(
        &mut self,
        _tx: &mut Tx,
        context: &mut ExecutionContext,
        contribution: &mut StepContribution,
    ) -> impl Future<Output = Result<RepeatStatus, BatchError>> + Send {
        self.0.execute(context, contribution)
    }
}

/// Metric handles for one tasklet step, resolved once — same rule as
/// [`ChunkMetrics`](crate::ChunkStep): label values must be owned, so building
/// these per pass would allocate on every commit.
struct TaskletMetrics {
    read: Counter,
    written: Counter,
    filtered: Counter,
    skipped: Counter,
}

impl TaskletMetrics {
    fn new(job: &str, step: &str) -> Self {
        let (job, step) = (job.to_owned(), step.to_owned());

        Self {
            read: counter!(ITEMS_READ, LABEL_JOB => job.clone(), LABEL_STEP => step.clone()),
            written: counter!(ITEMS_WRITTEN, LABEL_JOB => job.clone(), LABEL_STEP => step.clone()),
            filtered: counter!(ITEMS_FILTERED, LABEL_JOB => job.clone(), LABEL_STEP => step.clone()),
            skipped: counter!(ITEMS_SKIPPED, LABEL_JOB => job, LABEL_STEP => step, LABEL_PHASE => PHASE_TASKLET),
        }
    }

    /// Called only after the pass has committed, so these can never describe
    /// work that rolled back — and `sum(batchflow_items_written_total)` still
    /// reconciles with `sum(write_count)` for a job containing tasklets.
    fn record(&self, contribution: &StepContribution) {
        self.read.increment(contribution.read_count() as u64);
        self.written.increment(contribution.write_count() as u64);
        self.filtered.increment(contribution.filter_count() as u64);
        self.skipped.increment(contribution.skip_count() as u64);
    }
}

/// Wraps a [`Tasklet`] into a [`Step`], giving it a name and a commit point.
///
/// One transaction per `execute` call: `begin` → `execute` → `commit`, repeated
/// while the tasklet returns [`RepeatStatus::Continuable`].
///
/// **No retry or skip.** [`FaultTolerance`](crate::FaultTolerance) is a
/// [`ChunkStep`](crate::ChunkStep) feature and deliberately does not apply here.
/// Skip is meaningless with no items to drop, and retry would re-call `execute`
/// on a tasklet that has already mutated its own state — the "your code must be
/// idempotent" obligation this framework avoids imposing on processors, which it
/// would have to impose here instead. A tasklet that wants to retry owns that
/// decision, where it can see what it already did.
pub struct TaskletStep<T> {
    name: String,
    tasklet: T,
}

impl<T> TaskletStep<T> {
    /// Names a tasklet so a restart can find its previous attempt.
    #[must_use]
    pub fn new(name: impl Into<String>, tasklet: T) -> Self {
        Self {
            name: name.into(),
            tasklet,
        }
    }

    /// Inherent, so `step.name()` resolves without inferring `Tx` — the same
    /// reason [`ChunkStep`](crate::ChunkStep) has one.
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Hand-written so the tasklet needs no `Debug` bound of its own.
impl<T> std::fmt::Debug for TaskletStep<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskletStep")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<T, Tx> Step<Tx> for TaskletStep<T>
where
    T: TransactionalTasklet<Tx> + Send,
    Tx: Send,
{
    fn name(&self) -> &str {
        &self.name
    }

    async fn run(
        &mut self,
        context: &mut ExecutionContext,
        commit: &mut dyn StepCommit<Tx>,
    ) -> Result<(), BatchError> {
        let metrics = TaskletMetrics::new(commit.identity().job_name, &self.name);

        loop {
            let mut tx = commit.begin().await?;

            // Fresh per pass, so an errored pass contributes nothing: the
            // deltas are discarded with the transaction rather than folded.
            let mut contribution = StepContribution::new();

            let status = match self
                .tasklet
                .execute(&mut tx, context, &mut contribution)
                .await
            {
                Ok(status) => status,
                Err(error) => {
                    // Same shape as the chunk loop: a rollback that itself
                    // fails leaves the transaction in an unknown state, and the
                    // error the caller wants is the one that caused it.
                    if let Err(rollback_error) = commit.rollback(tx).await {
                        tracing::error!(
                            error = %rollback_error,
                            "rollback failed; the transaction is in an unknown state"
                        );
                        return Err(error.with_cleanup(Err(rollback_error)));
                    }
                    return Err(error);
                }
            };

            commit.commit(tx, &contribution, context).await?;
            metrics.record(&contribution);

            if status == RepeatStatus::Finished {
                return Ok(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{
        CountingTasklet, FailingTasklet, RecordingCommit, block_on, counter, recorded,
    };
    use crate::{ContextValue, StepContribution};

    #[tokio::test]
    async fn a_finished_tasklet_commits_once() {
        let mut step = TaskletStep::new("truncate", Unmanaged(CountingTasklet::new(1)));
        let mut context = ExecutionContext::new();
        let mut commit = RecordingCommit::new();

        step.run(&mut context, &mut commit).await.unwrap();

        assert_eq!(step.name(), "truncate");
        assert_eq!(commit.begins, 1);
        assert_eq!(commit.commits, 1);
        assert_eq!(commit.rollbacks, 0);
        assert_eq!(commit.total.write_count(), 1);
    }

    /// The whole point of `RepeatStatus`: three passes are three transactions,
    /// not one. A tasklet looping internally would show `commits == 1`.
    #[tokio::test]
    async fn a_continuable_tasklet_commits_once_per_pass() {
        let mut step = TaskletStep::new("archive", Unmanaged(CountingTasklet::new(3)));
        let mut context = ExecutionContext::new();
        let mut commit = RecordingCommit::new();

        step.run(&mut context, &mut commit).await.unwrap();

        assert_eq!(commit.begins, 3);
        assert_eq!(commit.commits, 3);
        assert_eq!(commit.total.write_count(), 3);
    }

    /// Each pass's bookmark is committed with it, which is what a restart reads.
    #[tokio::test]
    async fn each_pass_commits_the_bookmark_it_left() {
        let mut step = TaskletStep::new("archive", Unmanaged(CountingTasklet::new(3)));
        let mut context = ExecutionContext::new();
        let mut commit = RecordingCommit::new();

        step.run(&mut context, &mut commit).await.unwrap();

        assert_eq!(
            commit.context.get_long(CountingTasklet::DONE).unwrap(),
            Some(3)
        );
    }

    /// A restart hands the previous attempt's context back, and the tasklet
    /// picks up from it rather than redoing the passes that committed.
    #[tokio::test]
    async fn a_tasklet_resumes_from_its_bookmark() {
        let mut context = ExecutionContext::new();
        context.put(CountingTasklet::DONE, ContextValue::Long(2));

        let mut step = TaskletStep::new("archive", Unmanaged(CountingTasklet::new(3)));
        let mut commit = RecordingCommit::new();

        step.run(&mut context, &mut commit).await.unwrap();

        // One pass left, not three.
        assert_eq!(commit.begins, 1);
        assert_eq!(commit.total.write_count(), 1);
    }

    /// A failed pass rolls back and contributes nothing — the counters it
    /// incremented before failing go with the transaction.
    #[tokio::test]
    async fn a_failed_pass_rolls_back_and_contributes_nothing() {
        let mut step = TaskletStep::new("doomed", Unmanaged(FailingTasklet));
        let mut context = ExecutionContext::new();
        let mut commit = RecordingCommit::new();

        assert!(step.run(&mut context, &mut commit).await.is_err());

        assert_eq!(commit.begins, 1);
        assert_eq!(commit.commits, 0);
        assert_eq!(commit.rollbacks, 1);
        assert_eq!(commit.total, StepContribution::new());
    }

    /// A rollback that fails must not bury the failure it was cleaning up
    /// after: the tasklet's error stays the cause (ADR-009).
    #[tokio::test]
    async fn a_failed_rollback_keeps_the_original_error_as_the_cause() {
        let mut step = TaskletStep::new("doomed", Unmanaged(FailingTasklet));
        let mut context = ExecutionContext::new();
        let mut commit = RecordingCommit::failing_rollback();

        let error = step.run(&mut context, &mut commit).await.unwrap_err();

        let BatchError::CleanupFailed { cause, .. } = error else {
            panic!("expected CleanupFailed, got {error}");
        };
        assert!(cause.to_string().contains("tasklet failed"));
    }

    /// Reconciliation (Phase 12): a tasklet's committed counters reach the same
    /// metrics the chunk loop feeds, or `sum(items_written_total)` stops
    /// agreeing with `sum(write_count)` for any job containing one.
    #[test]
    fn committed_counters_reach_the_item_metrics() {
        let snapshot = recorded(|| {
            block_on(async {
                let mut step = TaskletStep::new("archive", Unmanaged(CountingTasklet::new(2)));
                let mut context = ExecutionContext::new();
                let mut commit = RecordingCommit::new();
                step.run(&mut context, &mut commit).await.unwrap();
            });
        });

        assert_eq!(counter(&snapshot, ITEMS_WRITTEN, None), Some(2));
        // A tasklet's skips get their own phase: it is opaque, so there is no
        // read/process/write distinction to attribute them to.
        assert_eq!(
            counter(&snapshot, ITEMS_SKIPPED, Some(PHASE_TASKLET)),
            Some(0)
        );
    }

    /// The negative half of the rule: a pass that rolled back publishes
    /// nothing. A counter is monotonic and cannot be taken back.
    ///
    /// `Some(0)`, not `None`: hoisting the handles *registers* the series at
    /// step start, which is Phase 12's deliberate choice — a job that has never
    /// written must read zero rather than be missing from the scrape. The
    /// `FailingTasklet` increments by 7 before failing, so a fold that ignored
    /// the rollback would show 7 here.
    #[test]
    fn a_rolled_back_pass_publishes_no_items() {
        let snapshot = recorded(|| {
            block_on(async {
                let mut step = TaskletStep::new("doomed", Unmanaged(FailingTasklet));
                let mut context = ExecutionContext::new();
                let mut commit = RecordingCommit::new();
                assert!(step.run(&mut context, &mut commit).await.is_err());
            });
        });

        assert_eq!(counter(&snapshot, ITEMS_WRITTEN, None), Some(0));
    }
}
