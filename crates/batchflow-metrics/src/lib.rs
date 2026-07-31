//! Prometheus wiring for [`batchflow_core`]'s metrics.
//!
//! The engine emits through the [`metrics`] facade and records nothing until an
//! application installs a recorder. This crate is that recorder, plus the two
//! things a bare `PrometheusBuilder` gets wrong for batch workloads: histogram
//! buckets, and calling `describe` at the right moment.
//!
//! ```no_run
//! let handle = batchflow_metrics::install().expect("install the recorder once");
//!
//! // Serve this from whatever HTTP stack the application already has.
//! let scrape: String = handle.render();
//! ```
//!
//! Installing mutates process-global state, so it is never done on the
//! application's behalf — the same discipline as not calling a logger's `init`
//! from a library.
//!
//! # Versioning
//!
//! [`PrometheusHandle`], [`PrometheusBuilder`] and [`BuildError`] are
//! re-exported from `metrics-exporter-prometheus`, so that crate's major
//! version is part of this one's public API. For a crate whose whole purpose is
//! Prometheus integration that coupling is deliberate; it is worth being
//! explicit that a breaking release there is a breaking release here.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

use batchflow_core::metrics::{CHUNK_DURATION, STEP_DURATION};

pub use batchflow_core::metrics::describe;
pub use metrics_exporter_prometheus::{BuildError, PrometheusBuilder, PrometheusHandle};

/// Chunk latency: sub-millisecond to a minute.
const CHUNK_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
];

/// Step latency: a whole step runs for seconds to hours.
const STEP_BUCKETS: &[f64] = &[
    0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0, 600.0, 1800.0, 3600.0, 7200.0,
];

/// A [`PrometheusBuilder`] with buckets configured for the engine's histograms.
///
/// Without explicit buckets the exporter renders a histogram as a *summary*,
/// whose quantiles are computed per process and cannot be aggregated across
/// them — so `p99 chunk latency` over four workers becomes unanswerable.
/// Buckets are summable, so the same query works over any number of instances.
///
/// Use this when the application needs to customise the builder further; use
/// [`install`] when it does not.
pub fn builder() -> PrometheusBuilder {
    // The only error `set_buckets_for_metric` reports is an empty bucket list,
    // and both lists are literal constants — an assertion about this file, not
    // about anything a caller passed in.
    PrometheusBuilder::new()
        .set_buckets_for_metric(full(CHUNK_DURATION), CHUNK_BUCKETS)
        .expect("chunk buckets are a non-empty constant")
        .set_buckets_for_metric(full(STEP_DURATION), STEP_BUCKETS)
        .expect("step buckets are a non-empty constant")
}

fn full(name: &str) -> metrics_exporter_prometheus::Matcher {
    metrics_exporter_prometheus::Matcher::Full(name.to_owned())
}

/// Installs the Prometheus recorder for the whole process and describes every
/// engine metric.
///
/// Returns the handle to render scrapes from. Errors if a recorder is already
/// installed — a global can be set only once, so this is a startup call.
///
/// `describe` runs *after* installation on purpose: descriptions are stored in
/// the recorder, so describing before one exists writes them nowhere and the
/// scrape ships without `# HELP` lines.
pub fn install() -> Result<PrometheusHandle, BuildError> {
    let handle = builder().install_recorder()?;
    describe();
    Ok(handle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use batchflow_core::metrics::{CHUNKS_COMMITTED, ITEMS_READ, ITEMS_WRITTEN};
    use batchflow_core::{
        BatchError, ChunkStep, InMemoryJobRepository, ItemProcessor, ItemReader, ItemWriter, Job,
        JobLauncher, JobParameter, JobParameters, Unmanaged,
    };
    use std::num::NonZeroUsize;

    struct Counting {
        remaining: u32,
    }

    impl ItemReader for Counting {
        type Item = u32;

        async fn read(&mut self) -> Result<Option<u32>, BatchError> {
            if self.remaining == 0 {
                return Ok(None);
            }
            self.remaining -= 1;
            Ok(Some(self.remaining))
        }
    }

    struct Identity;

    impl ItemProcessor for Identity {
        type In = u32;
        type Out = u32;

        async fn process(&mut self, item: u32) -> Result<Option<u32>, BatchError> {
            Ok(Some(item))
        }
    }

    struct Discard;

    impl ItemWriter for Discard {
        type Item = u32;

        async fn write(&mut self, _items: &[u32]) -> Result<(), BatchError> {
            Ok(())
        }
    }

    /// Runs a real job under a recorder scoped to this thread and returns the
    /// scrape it produced.
    ///
    /// `build_recorder` + `with_local_recorder`, never `install`: a global
    /// recorder can be set once per process, so a test that installed one would
    /// make every other test in the binary order-dependent.
    fn scrape() -> String {
        let recorder = builder().build_recorder();
        let handle = recorder.handle();

        metrics::with_local_recorder(&recorder, || {
            describe();

            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let launcher = JobLauncher::new(InMemoryJobRepository::default());
                    let mut job = Job::builder("nightly")
                        .step(ChunkStep::new(
                            "load",
                            Counting { remaining: 5 },
                            Identity,
                            Unmanaged(Discard),
                            NonZeroUsize::new(2).unwrap(),
                        ))
                        .build();

                    let params = JobParameters::new()
                        .with("date", JobParameter::String("2026-07-31".into()));
                    launcher.run(&mut job, &params).await.unwrap();
                });
        });

        handle.render()
    }

    #[test]
    fn a_job_run_renders_labelled_engine_counters() {
        let scrape = scrape();

        assert!(
            scrape.contains(&format!("{ITEMS_READ}{{job=\"nightly\",step=\"load\"}} 5")),
            "expected read counter in:\n{scrape}"
        );
        assert!(
            scrape.contains(&format!(
                "{ITEMS_WRITTEN}{{job=\"nightly\",step=\"load\"}} 5"
            )),
            "expected write counter in:\n{scrape}"
        );
        // 5 items at chunk_size 2 is three commits: the transaction boundary,
        // not the item total.
        assert!(
            scrape.contains(&format!(
                "{CHUNKS_COMMITTED}{{job=\"nightly\",step=\"load\"}} 3"
            )),
            "expected commit count in:\n{scrape}"
        );
    }

    /// [`describe`] is what puts these lines in the scrape. Without it the
    /// numbers are still correct and completely undocumented.
    #[test]
    fn the_scrape_carries_help_and_type_lines() {
        let scrape = scrape();

        assert!(scrape.contains(&format!("# HELP {ITEMS_READ}")));
        assert!(scrape.contains(&format!("# TYPE {ITEMS_READ} counter")));
    }

    /// The buckets decision, asserted: `_bucket{le=...}` lines mean a real
    /// histogram. A summary would render `quantile=` instead, and quantiles
    /// cannot be aggregated across processes.
    #[test]
    fn durations_render_as_aggregatable_histograms_not_summaries() {
        let scrape = scrape();

        assert!(scrape.contains(&format!("# TYPE {CHUNK_DURATION} histogram")));
        assert!(scrape.contains(&format!("{CHUNK_DURATION}_bucket")));
        assert!(scrape.contains("le=\"0.001\""), "chunk buckets missing");
        assert!(scrape.contains("le=\"7200\""), "step buckets missing");
        assert!(
            !scrape.contains("quantile="),
            "a summary leaked in:\n{scrape}"
        );
    }
}
