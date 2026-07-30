//! Shared test doubles.
//!
//! Compiled only under `cfg(test)`, so nothing here reaches the public API or a
//! release build. Lives in its own module because several modules' tests need the
//! same fakes, and duplicating them is how test suites drift apart.

use crate::{
    BatchError, Classifier, ContextValue, ErrorAction, ExecutionContext, InMemoryJobRepository,
    ItemProcessor, ItemReader, ItemWriter, JobExecution, JobExecutionId, JobInstance,
    JobInstanceId, JobParameters, JobRepository, Step, StepCommit, StepContribution, StepExecution,
};
use async_trait::async_trait;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::{Arc, Mutex};

/// Shorthand for a chunk-size literal. A zero here is a bug in the test itself,
/// so panicking is the right response.
pub(crate) fn nz(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).expect("test chunk size must be non-zero")
}

/// Shorthand for a retry-attempt literal.
pub(crate) fn nz32(n: u32) -> NonZeroU32 {
    NonZeroU32::new(n).expect("test attempt count must be non-zero")
}

/// Classifies every error as transient.
///
/// No real policy looks like this — it exists so a test can drive the retry
/// path without needing a backend whose errors carry SQLSTATE codes.
pub(crate) struct RetryAll;

impl Classifier for RetryAll {
    fn classify(&self, _error: &BatchError) -> ErrorAction {
        ErrorAction::Retry
    }
}

/// Classifies every error as a bad item.
pub(crate) struct SkipAll;

impl Classifier for SkipAll {
    fn classify(&self, _error: &BatchError) -> ErrorAction {
        ErrorAction::Skip
    }
}

/// Errors on any item equal to `poison`, *having already advanced past it*.
///
/// The advance is what makes a read error skippable at all: a reader that
/// errors without moving on would be handed the same item forever.
pub(crate) struct PoisonReader {
    pub items: Vec<u32>,
    pub pos: usize,
    pub poison: u32,
}

impl PoisonReader {
    pub(crate) fn new(items: Vec<u32>, poison: u32) -> Self {
        Self {
            items,
            pos: 0,
            poison,
        }
    }
}

impl ItemReader for PoisonReader {
    type Item = u32;

    async fn read(&mut self) -> Result<Option<Self::Item>, BatchError> {
        let Some(item) = self.items.get(self.pos).copied() else {
            return Ok(None);
        };
        self.pos += 1;

        if item == self.poison {
            return Err(BatchError::read(format!("malformed row {item}")));
        }
        Ok(Some(item))
    }
}

/// Doubles every item except `poison`, which fails.
///
/// Never returns `None`, so a test can tell a skip from a filter without the
/// two counters shadowing each other.
pub(crate) struct PoisonProcessor {
    pub poison: u32,
}

impl ItemProcessor for PoisonProcessor {
    type In = u32;
    type Out = u32;

    async fn process(&mut self, item: u32) -> Result<Option<u32>, BatchError> {
        if item == self.poison {
            return Err(BatchError::process(format!("bad item {item}")));
        }
        Ok(Some(item * 2))
    }
}

/// Reads a fixed list of items, then reports exhaustion.
pub(crate) struct VecReader {
    pub items: Vec<u32>,
    pub pos: usize,
}

impl VecReader {
    pub(crate) fn new(items: Vec<u32>) -> Self {
        Self { items, pos: 0 }
    }
}

impl ItemReader for VecReader {
    type Item = u32;

    async fn read(&mut self) -> Result<Option<Self::Item>, BatchError> {
        let item = self.items.get(self.pos).copied();
        if item.is_some() {
            self.pos += 1;
        }
        Ok(item)
    }
}

/// Key `BookmarkReader` stores its position under.
pub(crate) const POSITION: &str = "position";

/// Like [`VecReader`], but restartable: it records its position and seeks back
/// to it on `open`.
///
/// `VecReader` stays plain on purpose, so the *default* (non-restartable)
/// `open`/`update` bodies keep getting exercised too.
pub(crate) struct BookmarkReader {
    pub items: Vec<u32>,
    pub pos: usize,
}

impl BookmarkReader {
    pub(crate) fn new(items: Vec<u32>) -> Self {
        Self { items, pos: 0 }
    }
}

impl ItemReader for BookmarkReader {
    type Item = u32;

    async fn read(&mut self) -> Result<Option<Self::Item>, BatchError> {
        let item = self.items.get(self.pos).copied();
        if item.is_some() {
            self.pos += 1;
        }
        Ok(item)
    }

    async fn open(&mut self, context: &ExecutionContext) -> Result<(), BatchError> {
        // The `?` is what makes a corrupt bookmark abort the run instead of
        // silently restarting it from zero.
        if let Some(position) = context.get_long(POSITION)? {
            // `try_from`, not `as`: a negative position is corrupt data, and
            // `as` would wrap it into a huge number instead of complaining.
            self.pos = usize::try_from(position)
                .map_err(|_| BatchError::read(format!("negative bookmark {position}")))?;
        }

        Ok(())
    }

    fn update(&self, context: &mut ExecutionContext) {
        // usize -> i64 only loses data above 2^63 items, which is not a batch.
        context.put(POSITION, ContextValue::Long(self.pos as i64));
    }
}

/// Yields `remaining_ok` items, then errors.
pub(crate) struct FailingReader {
    pub remaining_ok: usize,
}

impl ItemReader for FailingReader {
    type Item = u32;

    async fn read(&mut self) -> Result<Option<u32>, BatchError> {
        if self.remaining_ok == 0 {
            return Err(BatchError::read("boom"));
        }
        self.remaining_ok -= 1;
        Ok(Some(7))
    }
}

/// Doubles even items; filters odd ones out by returning `None`.
pub(crate) struct EvenDoubler;

impl ItemProcessor for EvenDoubler {
    type In = u32;
    type Out = u32;

    async fn process(&mut self, item: u32) -> Result<Option<u32>, BatchError> {
        if item % 2 == 0 {
            Ok(Some(item * 2))
        } else {
            Ok(None) // odd → filtered out
        }
    }
}

/// Accumulates everything written, so tests can assert on the exact output.
pub(crate) struct CollectingWriter {
    pub written: Vec<u32>,
}

impl CollectingWriter {
    pub(crate) fn new() -> Self {
        Self {
            written: Vec::new(),
        }
    }
}

impl ItemWriter for CollectingWriter {
    type Item = u32;

    async fn write(&mut self, items: &[u32]) -> Result<(), BatchError> {
        self.written.extend_from_slice(items);
        Ok(())
    }
}

/// Fails every write — pins the rule that counters are recorded only once the
/// write has succeeded.
pub(crate) struct FailingWriter;

impl ItemWriter for FailingWriter {
    type Item = u32;

    async fn write(&mut self, _items: &[u32]) -> Result<(), BatchError> {
        Err(BatchError::write("boom"))
    }
}

/// Succeeds for `ok_writes` chunks, then fails — lets tests observe the state a
/// partially-completed step leaves behind.
pub(crate) struct FlakyWriter {
    pub written: Vec<u32>,
    pub ok_writes: usize,
}

impl FlakyWriter {
    pub(crate) fn new(ok_writes: usize) -> Self {
        Self {
            written: Vec::new(),
            ok_writes,
        }
    }
}

impl ItemWriter for FlakyWriter {
    type Item = u32;

    async fn write(&mut self, items: &[u32]) -> Result<(), BatchError> {
        if self.ok_writes == 0 {
            return Err(BatchError::write("boom"));
        }
        self.ok_writes -= 1;
        self.written.extend_from_slice(items);
        Ok(())
    }
}

/// Fails its first `failures` writes, then succeeds.
///
/// The inverse of [`FlakyWriter`], which succeeds first: this one exercises
/// recovery, that one exercises partial progress.
pub(crate) struct TransientWriter {
    pub written: Vec<u32>,
    pub failures: usize,
}

impl TransientWriter {
    pub(crate) fn new(failures: usize) -> Self {
        Self {
            written: Vec::new(),
            failures,
        }
    }
}

impl ItemWriter for TransientWriter {
    type Item = u32;

    async fn write(&mut self, items: &[u32]) -> Result<(), BatchError> {
        if self.failures > 0 {
            self.failures -= 1;
            return Err(BatchError::write("deadlock"));
        }
        self.written.extend_from_slice(items);
        Ok(())
    }
}

/// A write destination that outlives the step writing to it, so a restart test
/// can see everything written across both attempts. A step-owned writer would
/// drop its evidence, making a duplicated item invisible.
#[derive(Clone, Default)]
pub(crate) struct SharedSink(Arc<Mutex<Vec<u32>>>);

impl SharedSink {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// A writer onto this sink that succeeds `ok_writes` times, then fails.
    pub(crate) fn writer(&self, ok_writes: usize) -> SharedWriter {
        SharedWriter {
            sink: Arc::clone(&self.0),
            ok_writes,
        }
    }

    pub(crate) fn written(&self) -> Vec<u32> {
        self.0.lock().expect("sink poisoned").clone()
    }
}

pub(crate) struct SharedWriter {
    sink: Arc<Mutex<Vec<u32>>>,
    ok_writes: usize,
}

impl ItemWriter for SharedWriter {
    type Item = u32;

    async fn write(&mut self, items: &[u32]) -> Result<(), BatchError> {
        if self.ok_writes == 0 {
            return Err(BatchError::write("boom"));
        }
        self.ok_writes -= 1;

        // Guard created and dropped in one statement: holding it across an
        // `.await` would make the future `!Send`.
        self.sink
            .lock()
            .expect("sink poisoned")
            .extend_from_slice(items);

        Ok(())
    }
}

/// An [`InMemoryJobRepository`] whose `commit` always fails.
///
/// It cannot roll anything back — no fake can — but it does let a test observe
/// what the engine does *around* a failed commit, which is our logic rather
/// than the backend's.
pub(crate) struct CommitFails(InMemoryJobRepository);

impl CommitFails {
    pub(crate) fn new() -> Self {
        Self(InMemoryJobRepository::default())
    }
}

impl JobRepository for CommitFails {
    type Tx = ();

    async fn begin(&self) -> Result<(), BatchError> {
        Ok(())
    }

    async fn commit(&self, _tx: ()) -> Result<(), BatchError> {
        Err(BatchError::repository("commit failed"))
    }

    async fn rollback(&self, tx: ()) -> Result<(), BatchError> {
        self.0.rollback(tx).await
    }

    async fn update_step_execution_in(
        &self,
        tx: &mut (),
        step_execution: &StepExecution,
    ) -> Result<(), BatchError> {
        self.0.update_step_execution_in(tx, step_execution).await
    }

    async fn find_or_create_instance(
        &self,
        job_name: &str,
        parameters: &JobParameters,
    ) -> Result<JobInstance, BatchError> {
        self.0.find_or_create_instance(job_name, parameters).await
    }

    async fn find_instance(
        &self,
        job_name: &str,
        parameters: &JobParameters,
    ) -> Result<Option<JobInstance>, BatchError> {
        self.0.find_instance(job_name, parameters).await
    }

    async fn create_execution(
        &self,
        instance_id: JobInstanceId,
    ) -> Result<JobExecution, BatchError> {
        self.0.create_execution(instance_id).await
    }

    async fn update_execution(&self, execution: &JobExecution) -> Result<(), BatchError> {
        self.0.update_execution(execution).await
    }

    async fn last_execution(
        &self,
        instance_id: JobInstanceId,
    ) -> Result<Option<JobExecution>, BatchError> {
        self.0.last_execution(instance_id).await
    }

    async fn abandon_execution(&self, execution_id: JobExecutionId) -> Result<(), BatchError> {
        self.0.abandon_execution(execution_id).await
    }

    async fn create_step_execution(
        &self,
        job_execution_id: JobExecutionId,
        step_name: &str,
    ) -> Result<StepExecution, BatchError> {
        self.0
            .create_step_execution(job_execution_id, step_name)
            .await
    }

    async fn update_step_execution(
        &self,
        step_execution: &StepExecution,
    ) -> Result<(), BatchError> {
        self.0.update_step_execution(step_execution).await
    }

    async fn last_step_execution(
        &self,
        instance_id: JobInstanceId,
        step_name: &str,
    ) -> Result<Option<StepExecution>, BatchError> {
        self.0.last_step_execution(instance_id, step_name).await
    }

    async fn step_executions(
        &self,
        job_execution_id: JobExecutionId,
    ) -> Result<Vec<StepExecution>, BatchError> {
        self.0.step_executions(job_execution_id).await
    }
}

/// A tasklet-style step that does no item I/O — stands in for "delete temp files,
/// send a report". Proves a `Job` can hold heterogeneous step types.
pub(crate) struct LogStep;

#[async_trait]
impl Step for LogStep {
    fn name(&self) -> &str {
        "log"
    }

    // A tasklet reads and writes nothing, so it has nothing to commit.
    async fn run(
        &mut self,
        _context: &mut ExecutionContext,
        _commit: &mut dyn StepCommit<()>,
    ) -> Result<(), BatchError> {
        Ok(())
    }
}

/// A step that always fails — lets tests drive the launcher's failure path and
/// assert that the step's *own* error reaches the caller unwrapped.
pub(crate) struct FailingStep;

#[async_trait]
impl Step for FailingStep {
    fn name(&self) -> &str {
        "failing"
    }

    async fn run(
        &mut self,
        _context: &mut ExecutionContext,
        _commit: &mut dyn StepCommit<()>,
    ) -> Result<(), BatchError> {
        Err(BatchError::process("boom"))
    }
}

/// What happened at a transaction boundary, in order.
///
/// Counters alone cannot tell "rolled back, then opened a fresh transaction"
/// from "opened two transactions and rolled one back at the end" — only the
/// sequence can.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CommitEvent {
    Begin,
    Commit,
    Rollback,
}

/// Accumulates what a step commits, so tests can assert on totals and on the
/// *number* of commit points without needing a repository.
#[derive(Default)]
pub(crate) struct RecordingCommit {
    pub total: StepContribution,
    pub begins: usize,
    pub commits: usize,
    pub rollbacks: usize,
    pub context: ExecutionContext,
    pub events: Vec<CommitEvent>,
    /// Fail this many `commit` calls before the first success.
    pub commit_failures: usize,
}

impl RecordingCommit {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn failing_commits(commit_failures: usize) -> Self {
        Self {
            commit_failures,
            ..Self::default()
        }
    }
}

#[async_trait]
impl StepCommit<()> for RecordingCommit {
    async fn begin(&mut self) -> Result<(), BatchError> {
        self.begins += 1;
        self.events.push(CommitEvent::Begin);
        Ok(())
    }

    async fn commit(
        &mut self,
        _tx: (),
        contribution: &StepContribution,
        context: &ExecutionContext,
    ) -> Result<(), BatchError> {
        // Recorded on entry, so a *failed* commit still shows in the sequence —
        // otherwise a test cannot see that no rollback followed it.
        self.events.push(CommitEvent::Commit);

        if self.commit_failures > 0 {
            self.commit_failures -= 1;
            return Err(BatchError::repository("serialization failure at commit"));
        }

        self.total.apply(contribution);
        self.commits += 1;
        self.context = context.clone();
        Ok(())
    }

    async fn rollback(&mut self, _tx: ()) -> Result<(), BatchError> {
        self.rollbacks += 1;
        self.events.push(CommitEvent::Rollback);
        Ok(())
    }
}
