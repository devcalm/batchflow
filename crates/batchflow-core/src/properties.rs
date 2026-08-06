//! Property tests for the chunk loop (FR-2, Phase 6).
//!
//! The example-based tests in [`chunk`](crate::ChunkStep) pin specific
//! behaviours; these pin the *invariants* that must hold for every input. The
//! distinction matters most for the commit interval, where every example-based
//! test necessarily picks one chunk size and one item count, and the bug worth
//! fearing is the one that only appears when `items % chunk_size == 1`.
//!
//! In its own module rather than in `chunk.rs` because these tests share a set
//! of parameterised fakes with each other and with nothing else — the same
//! reason `mod testing` exists, applied one level down.

use crate::chunk::{ChunkConfig, run_step};
use crate::testing::{POSITION, RecordingCommit, block_on};
use crate::{
    BatchError, ExecutionContext, FaultTolerance, ItemProcessor, ItemReader, ItemWriter,
    StepContribution, Unmanaged,
};
use proptest::prelude::*;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

/// Reads a list, records its position, and errors on every item divisible by
/// `poison_modulus` — *after* advancing past it, since a reader that errors
/// without moving on is handed the same item forever.
///
/// `poison_modulus` of 0 means "never fail", which is how the clean-run
/// properties reuse this one reader.
struct ParameterisedReader {
    items: Vec<u32>,
    pos: usize,
    poison_modulus: u32,
}

impl ParameterisedReader {
    fn new(items: Vec<u32>, poison_modulus: u32) -> Self {
        Self {
            items,
            pos: 0,
            poison_modulus,
        }
    }

    fn is_poison(&self, item: u32) -> bool {
        self.poison_modulus != 0 && item % self.poison_modulus == 0
    }
}

impl ItemReader for ParameterisedReader {
    type Item = u32;

    async fn read(&mut self) -> Result<Option<Self::Item>, BatchError> {
        let Some(item) = self.items.get(self.pos).copied() else {
            return Ok(None);
        };
        self.pos += 1;

        if self.is_poison(item) {
            return Err(BatchError::read(format!("malformed row {item}")));
        }
        Ok(Some(item))
    }

    async fn open(&mut self, context: &ExecutionContext) -> Result<(), BatchError> {
        if let Some(position) = context.get_long(POSITION)? {
            self.pos = usize::try_from(position)
                .map_err(|_| BatchError::read(format!("negative bookmark {position}")))?;
        }
        Ok(())
    }

    fn update(&self, context: &mut ExecutionContext) {
        context.put(POSITION, crate::ContextValue::Long(self.pos as i64));
    }
}

/// Doubles an item, filtering out every one divisible by `filter_modulus`.
///
/// A modulus of 0 filters nothing, so a single strategy covers "no filtering"
/// and "filters some" without a second processor type.
struct ParameterisedProcessor {
    filter_modulus: u32,
}

impl ItemProcessor for ParameterisedProcessor {
    type In = u32;
    type Out = u32;

    async fn process(&mut self, item: u32) -> Result<Option<u32>, BatchError> {
        if self.filter_modulus != 0 && item % self.filter_modulus == 0 {
            return Ok(None);
        }
        Ok(Some(item * 2))
    }
}

/// A destination that outlives the step, so the *order* items landed in is
/// observable — a step-owned writer takes its evidence with it.
#[derive(Clone, Default)]
struct Sink(Arc<Mutex<Vec<u32>>>);

impl Sink {
    fn taken(&self) -> Vec<u32> {
        self.0.lock().expect("sink poisoned").clone()
    }
}

impl ItemWriter for Sink {
    type Item = u32;

    async fn write(&mut self, items: &[u32]) -> Result<(), BatchError> {
        self.0
            .lock()
            .expect("sink poisoned")
            .extend_from_slice(items);
        Ok(())
    }
}

/// What one run of the chunk loop produced.
struct Run {
    total: StepContribution,
    commits: usize,
    written: Vec<u32>,
    context: ExecutionContext,
    outcome: Result<(), BatchError>,
}

/// Drives one step to completion. Synchronous so `proptest!` bodies — which are
/// plain `fn`s and cannot be `#[tokio::test]` — can call it directly.
fn run(items: Vec<u32>, chunk_size: usize, filter_modulus: u32, poison_modulus: u32) -> Run {
    let sink = Sink::default();
    let mut reader = ParameterisedReader::new(items, poison_modulus);
    let mut processor = ParameterisedProcessor { filter_modulus };
    let mut writer = Unmanaged(sink.clone());
    let mut commit = RecordingCommit::new();
    let mut context = ExecutionContext::new();

    // A skip limit no realistic input can reach, so a poisoned read is tolerated
    // rather than turning every property into an assertion about failure.
    let fault = FaultTolerance::new()
        .classifier(crate::testing::SkipAll)
        .skip_limit(usize::MAX);

    let outcome = {
        let config = ChunkConfig::new(
            NonZeroUsize::new(chunk_size).expect("chunk size strategy is 1..="),
            &fault,
            "property-job",
            "property-step",
        );

        block_on_result(run_step(
            &mut reader,
            &mut processor,
            &mut writer,
            &config,
            &mut context,
            &mut commit,
        ))
    };

    Run {
        total: commit.total,
        commits: commit.commits,
        written: sink.taken(),
        context: commit.context,
        outcome,
    }
}

/// [`block_on`](crate::testing::block_on) discards its future's output; these
/// properties need it.
fn block_on_result(future: impl Future<Output = Result<(), BatchError>>) -> Result<(), BatchError> {
    let mut captured = None;
    block_on(async {
        captured = Some(future.await);
    });
    captured.expect("block_on always polls to completion")
}

/// Distinct items, so a duplicate in the sink is a bug rather than an input.
fn items() -> impl Strategy<Value = Vec<u32>> {
    (0usize..=60).prop_map(|len| (1..=len as u32).collect())
}

proptest! {
    /// Every item read is either written or filtered. This is the invariant
    /// `filter_count` exists to make checkable: Phase 7 derived it as
    /// `read - written`, which cannot disagree with itself and so proved
    /// nothing.
    #[test]
    fn every_item_read_is_written_or_filtered(
        items in items(),
        chunk_size in 1usize..=16,
        filter_modulus in 0u32..=5,
    ) {
        let run = run(items, chunk_size, filter_modulus, 0);

        prop_assert!(run.outcome.is_ok());
        prop_assert_eq!(
            run.total.read_count(),
            run.total.write_count() + run.total.filter_count()
        );
    }

    /// A skipped read never reaches the processor, so it is counted once, in
    /// `skip_count`, and never in `read_count`. Together with the property
    /// above this makes the four counters a partition of the input.
    #[test]
    fn skips_and_reads_partition_the_input(
        items in items(),
        chunk_size in 1usize..=16,
        poison_modulus in 2u32..=5,
    ) {
        let total_items = items.len();
        let run = run(items, chunk_size, 0, poison_modulus);

        prop_assert!(run.outcome.is_ok());
        prop_assert_eq!(run.total.read_count() + run.total.skip_count(), total_items);
    }

    /// The commit interval is exactly that: `ceil(n / chunk_size)` transactions,
    /// and none at all for an empty input.
    ///
    /// The boundary this covers and no example test does is `n % chunk_size`
    /// over its whole range — an off-by-one in the partial final chunk shows up
    /// for some remainders and not others.
    #[test]
    fn the_commit_count_is_the_number_of_chunks(
        items in items(),
        chunk_size in 1usize..=16,
    ) {
        let total_items = items.len();
        let run = run(items, chunk_size, 0, 0);

        prop_assert!(run.outcome.is_ok());
        prop_assert_eq!(run.commits, total_items.div_ceil(chunk_size));
    }

    /// Chunk size is a performance knob, not a semantic one: the same input
    /// yields the same output, in the same order, whatever the commit interval.
    ///
    /// This is the property a user actually relies on when tuning, and it is the
    /// one that would break first if a chunk boundary ever dropped or reordered
    /// an item.
    #[test]
    fn chunk_size_does_not_change_the_result(
        items in items(),
        first in 1usize..=16,
        second in 1usize..=16,
        filter_modulus in 0u32..=5,
    ) {
        let a = run(items.clone(), first, filter_modulus, 0);
        let b = run(items, second, filter_modulus, 0);

        prop_assert_eq!(&a.written, &b.written);
        prop_assert_eq!(a.total, b.total);
    }

    /// What was written is exactly the processor's output for the items it kept,
    /// in input order — no duplicates, no gaps, no reordering across chunks.
    ///
    /// Asserting the exact vector rather than its length is what catches a
    /// resume at the wrong offset, which produces a duplicate-free but wrong
    /// result (the Phase 16 lesson).
    #[test]
    fn the_sink_holds_exactly_the_kept_items_in_order(
        items in items(),
        chunk_size in 1usize..=16,
        filter_modulus in 2u32..=5,
    ) {
        let expected: Vec<u32> = items
            .iter()
            .filter(|item| *item % filter_modulus != 0)
            .map(|item| item * 2)
            .collect();

        let run = run(items, chunk_size, filter_modulus, 0);

        prop_assert_eq!(run.written, expected);
    }

    /// The committed bookmark always describes the whole input once the step
    /// finishes, whatever the chunk size — so a restart of a completed step
    /// reads nothing rather than re-reading a final partial chunk.
    #[test]
    fn the_committed_bookmark_covers_the_whole_input(
        items in items().prop_filter("a bookmark needs a commit", |items| !items.is_empty()),
        chunk_size in 1usize..=16,
    ) {
        let total_items = items.len() as i64;
        let run = run(items, chunk_size, 0, 0);

        prop_assert!(run.outcome.is_ok());
        prop_assert_eq!(
            run.context.get_long(POSITION).unwrap(),
            Some(total_items)
        );
    }
}
