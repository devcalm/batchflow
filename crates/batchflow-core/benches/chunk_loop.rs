//! Chunk-loop overhead with no I/O whatsoever.
//!
//! The reader, processor, writer and commit point all do nothing, so every
//! nanosecond measured here belongs to BatchFlow. That is what makes P-1
//! checkable at all: "negligible vs. actual I/O" is a ratio, and this
//! benchmark is its denominator.

use batchflow_core::{
    BatchError, ChunkStep, ExecutionContext, ItemProcessor, ItemReader, ItemWriter, JobExecutionId,
    Step, StepCommit, StepContribution, StepExecutionId, StepIdentity, Unmanaged, async_trait,
};
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;
use std::num::NonZeroUsize;

/// Yields `0..count` and nothing else. Overrides neither `open` nor `update`,
/// so no bookmark work is measured.
struct BenchReader {
    next: u64,
    count: u64,
}

impl ItemReader for BenchReader {
    type Item = u64;

    async fn read(&mut self) -> Result<Option<u64>, BatchError> {
        if self.next == self.count {
            return Ok(None);
        }
        self.next += 1;
        Ok(Some(self.next))
    }
}

struct Passthrough;

impl ItemProcessor for Passthrough {
    type In = u64;
    type Out = u64;

    async fn process(&mut self, item: u64) -> Result<Option<u64>, BatchError> {
        Ok(Some(item))
    }
}

/// Discards the chunk. `black_box` keeps the optimiser from concluding that
/// the whole pipeline is unobservable and deleting it — without it this
/// benchmark measures an empty loop and reports a wonderful number.
struct NullWriter;

impl ItemWriter for NullWriter {
    type Item = u64;

    async fn write(&mut self, items: &[u64]) -> Result<(), BatchError> {
        black_box(items);
        Ok(())
    }
}

/// A commit point with no repository behind it: `begin`/`commit`/`rollback`
/// all succeed instantly. Isolates the chunk loop from storage, including
/// `InMemoryJobRepository`'s mutex.
struct NoOpCommit;

#[async_trait]
impl StepCommit<()> for NoOpCommit {
    fn identity(&self) -> StepIdentity<'_> {
        StepIdentity {
            job_name: "bench",
            step_name: "chunk_loop",
            job_execution_id: JobExecutionId::new(1),
            step_execution_id: StepExecutionId::new(1),
        }
    }

    async fn begin(&mut self) -> Result<(), BatchError> {
        Ok(())
    }

    async fn commit(
        &mut self,
        _tx: (),
        _contribution: &StepContribution,
        _context: &ExecutionContext,
    ) -> Result<(), BatchError> {
        Ok(())
    }

    async fn rollback(&mut self, _tx: ()) -> Result<(), BatchError> {
        Ok(())
    }
}

fn step(
    count: u64,
    chunk_size: usize,
) -> ChunkStep<BenchReader, Passthrough, Unmanaged<NullWriter>> {
    ChunkStep::new(
        "chunk_loop",
        BenchReader { next: 0, count },
        Passthrough,
        Unmanaged(NullWriter),
        NonZeroUsize::new(chunk_size).expect("chunk size must be non-zero"),
    )
}

/// Fixed item count, varying commit interval. This is the curve that turns
/// P-2 from a claim into something a user can read off a graph before picking
/// a chunk size.
fn commit_interval(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("commit_interval");

    // The sweep spans the region where per-chunk cost stops dominating: at 1
    // every item pays a full commit boundary, and by 10,000 the boundary has
    // amortised to nothing. Five points at ~100 samples each is the wall-clock
    // bill, which is why the item count is held at 10,000 rather than higher —
    // the shape of the curve is what is being measured, not the absolute time.
    const ITEMS: u64 = 10_000;
    const CHUNK_SIZES: &[usize] = &[1, 10, 100, 1000, 10_000];

    // The element is an **item**, so criterion prints items/sec and ns/item
    // directly. That is the figure `docs/Performance.md` quotes, and declaring
    // it here is what stops the document and the harness from drifting: before
    // this, the ns/item column was arithmetic done by hand outside the tool.
    group.throughput(Throughput::Elements(ITEMS));

    for &chunk_size in CHUNK_SIZES {
        group.bench_function(format!("{chunk_size}"), |b| {
            b.to_async(&runtime).iter_batched(
                // A reader is exhausted after one run, so each iteration needs
                // a fresh step. Built in `setup`, which criterion excludes
                // from the measurement.
                || step(ITEMS, chunk_size),
                |mut step| async move {
                    let mut context = ExecutionContext::new();
                    let mut commit = NoOpCommit;
                    step.run(&mut context, &mut commit)
                        .await
                        .expect("bench step failed")
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

criterion_group!(benches, commit_interval);
criterion_main!(benches);
