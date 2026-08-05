# BatchFlow performance

Phase 17. Every number here is measured; P-5 exists precisely to forbid quoting
one that is not. Where something is unmeasured this document says so rather
than estimating.

## What is measured

`crates/batchflow-core/benches/chunk_loop.rs` drives a `ChunkStep` through
`Step::run` with a reader that yields integers, a passthrough processor, a
writer that discards, and a `StepCommit` whose `begin`/`commit`/`rollback` all
return `Ok(())` immediately. There is **no repository in the measurement at
all** — not even `InMemoryJobRepository`, whose mutex would otherwise be
counted as framework cost.

So what is left is the chunk loop itself: the read loop, the process loop, the
counter bookkeeping, the retry scaffolding and the transaction boundary calls.
That is the denominator P-1 needs. "Chunk-loop overhead is negligible vs.
actual I/O" is a ratio, and this is the half of it we control.

## Setup

| | |
|---|---|
| Machine | Apple M1, 8 cores, arm64, macOS 26.5.2 |
| Toolchain | rustc 1.95.0, `bench` profile |
| Harness | criterion 0.7, 100 samples per point |
| Workload | 10,000 items per iteration, fresh reader each iteration |

## The commit-interval curve

10,000 items, varying the commit interval:

| `chunk_size` | chunks | time | ns / item |
|---|---|---|---|
| 1 | 10,000 | 1.057 ms | 105.7 |
| 10 | 1,000 | 144.8 µs | 14.5 |
| 100 | 100 | 49.8 µs | 5.0 |
| 1,000 | 10 | 37.6 µs | 3.8 |
| 10,000 | 1 | 36.8 µs | 3.7 |

A two-parameter least-squares fit of `t = items·a + chunks·b` over all five
points gives:

**a ≈ 3.9 ns per item · b ≈ 102 ns per chunk**

reproducing every measured point to within 6%. The residuals are not random —
per-item cost drifts slightly downward as chunks grow, which is what you would
expect from better locality and fewer loop-control branches — so treat the fit
as a good approximation rather than an exact decomposition.

## Choosing a commit interval

The curve flattens early. Going from 1 to 100 is a 21× improvement; from 100 to
1,000 is 1.32×; from 1,000 to 10,000 is 1.02×. **By a chunk size of roughly
1,000 the framework's own per-chunk cost has been amortised to nothing.**

The practical consequence is the opposite of what the curve first suggests:
because BatchFlow's per-chunk cost is only ~102 ns while a Postgres `COMMIT` is
on the order of 100 µs to 1 ms, **the framework is never the reason to raise
your chunk size — your database is.** Tune the commit interval against the
backend's per-transaction cost, the memory you are willing to hold (the loop
keeps up to `chunk_size` items live across two `Vec`s), and how much re-done
work you can tolerate after a crash. BatchFlow's contribution to that decision
is negligible above ~100.

## Allocation behaviour

`crates/batchflow-core/tests/allocations.rs` asserts P-4 rather than
illustrating it, because a benchmark only goes red if a human reads it. A
counting `#[global_allocator]` measures two runs that commit the *same* number
of chunks over a tenfold difference in items:

| workload | chunks | allocations |
|---|---|---|
| 1,000 items @ `chunk_size` 100 | 10 | 66 |
| 10,000 items @ `chunk_size` 1,000 | 10 | 66 |

Identical. Allocation is a function of the chunk count and nothing else, so
`assert_eq!` is the honest assertion and no tolerance is needed. Roughly 4 of
those 66 are per chunk — the two `Vec::with_capacity` calls plus two
`#[async_trait]` `Box::pin`s for `begin` and `commit` — and the remaining ~26
are fixed per step.

Note that `TransactionalWriter::write` costs nothing here: it is RPITIT rather
than boxed. That is ADR-002's "boxing cost scales with call frequency" showing
up as a measurement.

The test ships with a positive control (`the_counter_observes_per_item_allocation`)
that runs a deliberately allocating processor and requires the count to scale.
Without it, either a global counter polluted by the test harness or a
multi-threaded runtime — which would drive the future on a worker thread whose
thread-local counter is a different cell — would make the real assertion
compare zero to zero and pass forever.

## Scorecard

| | Status |
|---|---|
| **P-1** chunk-loop overhead negligible vs. I/O | **Holds, as a ratio.** At `chunk_size` 1,000 the framework spends 37.6 µs on 10,000 items; the ten Postgres round trips that work needs cost ~2–10 ms. Under 2%. |
| **P-2** commit interval amortises transaction cost | **Holds.** The curve above; flat by ~1,000. |
| **P-3** batched writer I/O | **Structural, not benchmarked.** `ItemWriter::write(&[Item])` is chunk-oriented by signature, so one round trip per commit interval is guaranteed by the API rather than by a measurement. |
| **P-4** no per-item heap allocation | **Holds exactly**, and is now a test. |
| **P-5** benchmark before claiming | **This document.** |

## What P-5 already prevented

Reading `chunk.rs` before measuring, the two per-chunk `Vec::with_capacity`
calls looked like an obvious optimisation: hoist the buffers out of the loop
and reuse them, which would mean changing `process_chunk` to stop consuming its
input by value — a breaking change to a signature, made just before the 0.1.0
API freeze.

Those allocations are inside the 102 ns per chunk. At `chunk_size` 1,000 that
is **0.1 ns per item**: about 2.6% of a framework overhead that is itself under
2% of a real job. The change would have broken the API for nothing. This is the
entire reason Phase 17 precedes Phase 18.

The same reasoning applies to anything else in the chunk loop. **There is no
outstanding optimisation work in the engine**, and inventing some would be
worse than saying so.

## Caveats

These numbers are a **lower bound on framework overhead**, for four reasons
worth stating plainly:

1. **The reader and processor are trivially inlinable.** Only the writer is
   `black_box`ed. A real `async fn read` that touches a file or a socket will
   not inline, and its state machine will be larger.
2. **No metrics recorder is installed.** `metrics::counter!` with no recorder
   is close to a null check; with `batchflow-metrics` installed those seven
   per-step handles and their per-chunk increments do real work. That cost is
   **not measured here.**
3. **No tracing subscriber is installed.** Spans with no subscriber are nearly
   free; under a configured `Layer` they are not.
4. **One machine, one architecture.** Apple M1, arm64. There are no x86-64
   numbers, and no numbers from a contended or memory-constrained host.

Points 2 and 3 are the interesting gap: the observability added in Phases 12
and 13 sits *inside* the loop measured here, and this benchmark configures it
away. Measuring the loop with a live recorder and subscriber is the obvious
next benchmark, and it is not yet written.

## Reproducing

```
cargo bench -p batchflow-core --bench chunk_loop
cargo test  -p batchflow-core --test allocations
```

The benchmark takes about a minute. The allocation test is instant and runs as
part of `cargo test --workspace`, so P-4 is enforced on every CI run while the
timing numbers above are refreshed by hand.
